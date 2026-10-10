"""
Coderhelm Log Analyzer Lambda

Triggered by EventBridge every 6 hours. For each team with an AWS connection:
1. AssumeRole into the customer's account
2. Run pre-built CloudWatch Logs Insights queries
3. Send error summaries to Anthropic Claude for analysis
4. Deduplicate and store recommendations in DynamoDB
5. Post new findings to the team's Teams channel, when turned on

No raw logs are stored — only error summaries and recommendations.
"""

import hashlib
import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from urllib.request import Request, urlopen
from urllib.error import HTTPError

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

AWS_INSIGHTS_TABLE = os.environ.get("AWS_INSIGHTS_TABLE_NAME", "coderhelm-prod-aws-insights")
SETTINGS_TABLE = os.environ.get("SETTINGS_TABLE_NAME", "coderhelm-prod-settings")
MODEL_ID = os.environ.get("MODEL_ID", "claude-sonnet-4-6")
CODERHELM_ACCOUNT_ID = os.environ["CODERHELM_ACCOUNT_ID"]
LOOKBACK_HOURS = int(os.environ.get("LOOKBACK_HOURS", "24"))
DASHBOARD_URL = os.environ.get("DASHBOARD_URL", "https://app.coderhelm.com")
ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_VERSION = "2023-06-01"

dynamodb = boto3.resource("dynamodb")
aws_insights_table = dynamodb.Table(AWS_INSIGHTS_TABLE)
settings_table = dynamodb.Table(SETTINGS_TABLE)
sts_client = boto3.client("sts")

# ── Pre-built Insights Queries ──────────────────────────────────

INSIGHTS_QUERIES = [
    {
        "name": "lambda_errors",
        "description": "Lambda function errors and timeouts",
        "log_group_pattern": "/aws/lambda/",
        "query": (
            "fields @timestamp, @message, @logStream, @logGroup "
            "| filter @message like /(?i)(error|exception|timeout|task timed out|out of memory|runtime exited)/ "
            "| filter @message not like /(?i)(info|debug|warn.*deprecat)/ "
            "| stats count() as error_count by @logGroup, @logStream "
            "| sort error_count desc "
            "| limit 20"
        ),
    },
    {
        "name": "api_gateway_5xx",
        "description": "API Gateway 5xx errors",
        "log_group_pattern": "api-gateway",
        "query": (
            "fields @timestamp, @message, @logGroup "
            "| filter @message like /\" 5\\d{2} \"/ "
            "| stats count() as error_count by @logGroup, @message "
            "| sort error_count desc "
            "| limit 20"
        ),
    },
    {
        "name": "ecs_crashes",
        "description": "ECS task crashes and OOM kills",
        "log_group_pattern": "/ecs/",
        "query": (
            "fields @timestamp, @message, @logStream, @logGroup "
            "| filter @message like /(?i)(oom|killed|signal|exit code [^0]|panic|fatal|segfault)/ "
            "| stats count() as crash_count by @logGroup, @logStream "
            "| sort crash_count desc "
            "| limit 20"
        ),
    },
    {
        "name": "general_errors",
        "description": "General application errors across all log groups",
        "log_group_pattern": None,  # matches any
        "query": (
            "fields @timestamp, @message, @logGroup "
            "| filter @message like /(?i)(ERROR|FATAL|CRITICAL|UnhandledPromiseRejection|Traceback)/ "
            "| filter @message not like /(?i)(healthcheck|ping|OPTIONS)/ "
            # Filter out common bot/scanner noise before it reaches the LLM
            "| filter @message not like /(?i)(Not found: \\/\\.env|Not found: \\/\\.git|Not found: \\/wp-|Not found: \\/admin|Not found: \\/phpMyAdmin|Not found: \\/\\.aws|Not found: \\/\\.DS_Store|Not found: \\/robots\\.txt|Not found: \\/favicon\\.ico|Not found: \\/sitemap)/ "
            "| stats count() as error_count by @logGroup, @message "
            "| sort error_count desc "
            "| limit 20"
        ),
    },
    {
        "name": "http_4xx_5xx",
        "description": "HTTP error responses from web services",
        "log_group_pattern": "/ecs/",
        "query": (
            "fields @timestamp, @message, @logGroup "
            "| filter @message like /(?i)(\\\"(GET|POST|PUT|DELETE|PATCH).*\\\" [45]\\d{2}|HTTP [45]\\d{2}|status[=: ]+[45]\\d{2})/ "
            # Exclude known scanner paths
            "| filter @message not like /(?i)(\\.env|\\.git|wp-admin|wp-login|phpMyAdmin|\\.aws|xmlrpc|wp-content|wp-includes)/ "
            "| stats count() as error_count by @logGroup, @message "
            "| sort error_count desc "
            "| limit 20"
        ),
    },
]

# ── Token / Secret Scrubbing ───────────────────────────────────

import re

# Patterns that match common secrets, tokens, keys, and credentials
SECRET_PATTERNS = [
    # AWS keys
    (re.compile(r"(?:AKIA|ASIA)[A-Z0-9]{16}"), "[AWS_ACCESS_KEY]"),
    (re.compile(r"(?<![A-Za-z0-9/+])[A-Za-z0-9/+=]{40}(?![A-Za-z0-9/+=])"), None),  # handled separately
    # AWS session tokens
    (re.compile(r"(?i)(?:aws[_-]?session[_-]?token|x-amz-security-token)\s*[:=]\s*\S+"), "[AWS_SESSION_TOKEN]"),
    # Generic API keys / tokens (hex or base64, 32+ chars)
    (re.compile(r"(?i)(?:api[_-]?key|auth[_-]?token|bearer|secret[_-]?key|access[_-]?token|private[_-]?key)\s*[:=]\s*['\"]?\S{20,}['\"]?"), "[REDACTED_CREDENTIAL]"),
    # Bearer tokens in headers
    (re.compile(r"(?i)Bearer\s+[A-Za-z0-9\-._~+/]+=*"), "[BEARER_TOKEN]"),
    # JWTs (3 base64 segments separated by dots)
    (re.compile(r"eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}"), "[JWT_TOKEN]"),
    # GitHub tokens
    # Includes "." and "-": GitHub's stateless installation tokens are
    # JWT-style (ghs_xxx.yyy.zzz, ~520 chars) — the old class stopped at the
    # first dot and leaked the rest.
    (re.compile(r"gh[pousr]_[A-Za-z0-9.\-_]{36,}"), "[GITHUB_TOKEN]"),
    # Slack tokens
    (re.compile(r"xox[baprs]-[A-Za-z0-9\-]{10,}"), "[SLACK_TOKEN]"),
    # Generic password fields
    (re.compile(r"(?i)(?:password|passwd|pwd)\s*[:=]\s*['\"]?\S+['\"]?"), "[REDACTED_PASSWORD]"),
    # Connection strings
    (re.compile(r"(?i)(?:mongodb|postgres|mysql|redis|amqp)://\S+@\S+"), "[REDACTED_CONNECTION_STRING]"),
    # Private keys
    (re.compile(r"-----BEGIN (?:RSA |EC |DSA )?PRIVATE KEY-----"), "[PRIVATE_KEY]"),
]


def scrub_secrets(text):
    """Remove tokens, secrets, and credentials from log text before analysis."""
    if not isinstance(text, str):
        return text
    for pattern, replacement in SECRET_PATTERNS:
        if replacement:
            text = pattern.sub(replacement, text)
        else:
            # AWS secret key heuristic — only replace near AWS-related context
            text = pattern.sub("[POSSIBLE_SECRET_KEY]", text)
    return text


def scrub_query_results(results):
    """Recursively scrub secrets from query result data."""
    if isinstance(results, str):
        return scrub_secrets(results)
    if isinstance(results, list):
        return [scrub_query_results(item) for item in results]
    if isinstance(results, dict):
        return {k: scrub_query_results(v) for k, v in results.items()}
    return results


# ── Main Handler ────────────────────────────────────────────────


def handler(event, context):
    """EventBridge scheduled handler — runs every 6 hours."""
    logger.info("Log analyzer started")

    # Scan all teams with AWS connections
    connections = scan_aws_connections()
    logger.info(f"Found {len(connections)} AWS connections")

    total_recs = 0
    for conn in connections:
        try:
            recs = analyze_connection(conn)
            total_recs += recs
        except Exception as e:
            logger.error(
                f"Failed to analyze connection {conn['team_id']}/{conn['account_id']}: {e}",
                exc_info=True,
            )
            # Keep the connection active so the next run retries; a transient
            # failure must not stop analysis until someone presses Test.
            record_connection_error(conn["team_id"], conn["account_id"], str(e))

    logger.info(f"Log analyzer complete — {total_recs} new recommendations")
    return {"statusCode": 200, "total_recommendations": total_recs}


def scan_aws_connections():
    """Scan settings table for all active AWS connections across all teams."""
    connections = []
    last_key = None

    while True:
        scan_kwargs = {
            "FilterExpression": "begins_with(sk, :prefix) AND #s = :active",
            "ExpressionAttributeNames": {"#s": "status"},
            "ExpressionAttributeValues": {
                ":prefix": "AWS_CONN#",
                ":active": "active",
            },
        }
        if last_key:
            scan_kwargs["ExclusiveStartKey"] = last_key

        resp = aws_insights_table.scan(**scan_kwargs)

        for item in resp.get("Items", []):
            connections.append(
                {
                    "team_id": item["pk"],
                    "account_id": item.get("account_id", ""),
                    "role_arn": item["role_arn"],
                    "external_id": item["external_id"],
                    "region": item.get("region", "us-east-1"),
                    "log_groups": item.get("log_groups", []),
                }
            )

        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break

    return connections


def get_team_api_key(team_id):
    """Load the team's Anthropic API key from DynamoDB settings."""
    try:
        resp = settings_table.get_item(
            Key={"pk": team_id, "sk": "SETTINGS#MODEL_PROVIDER"}
        )
        item = resp.get("Item")
        if not item:
            return None
        return item.get("api_key", "")
    except Exception as e:
        logger.warning(f"Failed to load API key for {team_id}: {e}")
        return None


def analyze_connection(conn):
    """Analyze a single AWS connection — AssumeRole, query logs, generate recommendations."""
    team_id = conn["team_id"]
    account_id = conn["account_id"]
    role_arn = conn["role_arn"]
    external_id = conn["external_id"]
    region = conn["region"]

    logger.info(f"Analyzing {team_id} / account {account_id}")

    # Load team's Anthropic API key
    api_key = get_team_api_key(team_id)
    if not api_key:
        logger.warning(f"No Anthropic API key configured for {team_id}, skipping analysis")
        return 0

    # AssumeRole with 15-minute session (minimum)
    assumed = sts_client.assume_role(
        RoleArn=role_arn,
        RoleSessionName="coderhelm-analyzer",
        ExternalId=external_id,
        DurationSeconds=900,
    )

    creds = assumed["Credentials"]
    cw_logs = boto3.client(
        "logs",
        region_name=region,
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretAccessKey"],
        aws_session_token=creds["SessionToken"],
    )

    # Get the log groups this team has configured (or discover them)
    log_groups = conn.get("log_groups", [])
    if not log_groups:
        log_groups = discover_log_groups(cw_logs)
        logger.info(f"Auto-discovered {len(log_groups)} log groups")

    # Run queries and collect results. A query that fails is not "no errors":
    # when any fails, nothing is auto-resolved this run.
    all_results = []
    any_query_failed = False
    for query_def in INSIGHTS_QUERIES:
        matching_groups = filter_log_groups(log_groups, query_def.get("log_group_pattern"))
        if not matching_groups:
            continue

        # Run query against matching log groups (batch of 50 max)
        for batch in chunk_list(matching_groups, 50):
            try:
                results = run_insights_query(cw_logs, batch, query_def["query"])
            except Exception as e:
                logger.warning(f"Query {query_def['name']} failed: {e}")
                results = None
            if results is None:
                any_query_failed = True
                continue
            if results:
                all_results.append(
                    {
                        "query_name": query_def["name"],
                        "description": query_def["description"],
                        "log_groups": batch,
                        "results": results,
                    }
                )

    existing = list_pending_recommendations(team_id, account_id)

    if not all_results:
        record_connection_success(team_id, account_id)
        if any_query_failed:
            logger.warning(f"{team_id}/{account_id}: queries failed and none returned rows, skipping resolution")
            return 0
        logger.info(f"No errors found for {team_id}/{account_id}")
        # Every query ran and found nothing: the findings are no longer erroring.
        resolve_stale_recommendations(team_id, existing, set())
        return 0

    # Reorganize results grouped by log group
    all_results = group_results_by_log_group(all_results)

    seen_ids = set()
    new_recs = []

    # Check for secrets/tokens in raw results and create advisory if found
    raw_text = json.dumps(all_results, default=str)
    secrets_found = 0
    secret_types = set()
    for pattern, replacement in SECRET_PATTERNS:
        matches = pattern.findall(raw_text)
        if matches and replacement:
            secrets_found += len(matches)
            secret_types.add(replacement.strip("[]"))

    if secrets_found > 0:
        types_str = ", ".join(sorted(secret_types))
        advisory_rec = {
            "title": f"Secrets detected in CloudWatch Logs ({secrets_found} instances)",
            "severity": "critical",
            "summary": (
                f"We detected {secrets_found} potential secrets or tokens in your CloudWatch Logs "
                f"(types: {types_str}). These were automatically scrubbed before AI analysis, "
                f"but they still exist in your logs and could be exposed."
            ),
            "suggested_action": (
                "1. Rotate any exposed credentials immediately. "
                "2. Add a CloudWatch Logs subscription filter or Lambda to strip secrets at ingestion. "
                "3. Update your application to avoid logging sensitive values — use environment variables "
                "and never pass secrets as command-line arguments or log them at any level. "
                "4. Consider using AWS Secrets Manager or Parameter Store for credential management."
            ),
            "source_log_group": "multiple",
            "error_pattern": f"secrets_in_logs_{account_id}",
        }
        rec_id, is_new = store_recommendation(team_id, account_id, advisory_rec, existing)
        if rec_id:
            seen_ids.add(rec_id)
        if is_new:
            new_recs.append(advisory_rec)

    # Send to Anthropic for analysis
    recommendations = analyze_with_anthropic(all_results, account_id, api_key, existing)

    # None = the analysis failed: we can't tell "no issues" from "API error",
    # so nothing is resolved.
    if recommendations is None:
        logger.warning(f"{team_id}/{account_id}: analysis failed, skipping resolution")
        notify_new_recommendations(team_id, account_id, new_recs)
        return len(new_recs)

    record_connection_success(team_id, account_id)

    for rec in recommendations:
        rec_id, is_new = store_recommendation(team_id, account_id, rec, existing)
        if rec_id:
            seen_ids.add(rec_id)
        if is_new:
            new_recs.append(rec)

    # Resolve findings that no longer appear — only after a complete run.
    if not any_query_failed:
        resolve_stale_recommendations(team_id, existing, seen_ids)

    notify_new_recommendations(team_id, account_id, new_recs)
    logger.info(f"{team_id}/{account_id}: {len(new_recs)} new recommendations")
    return len(new_recs)


def discover_log_groups(cw_logs):
    """Discover all log groups in the customer's account."""
    groups = []
    paginator = cw_logs.get_paginator("describe_log_groups")
    for page in paginator.paginate():
        for lg in page.get("logGroups", []):
            name = lg.get("logGroupName", "")
            groups.append(name)
        if len(groups) >= 500:
            break
    return groups


def filter_log_groups(log_groups, pattern):
    """Filter log groups by pattern."""
    if not pattern:
        return log_groups
    pattern = pattern.lower()
    return [g for g in log_groups if pattern in g.lower()]


def run_insights_query(cw_logs, log_groups, query_string):
    """Run a CloudWatch Logs Insights query and wait for results."""
    end_time = int(time.time())
    start_time = end_time - (LOOKBACK_HOURS * 3600)

    try:
        response = cw_logs.start_query(
            logGroupNames=log_groups,
            startTime=start_time,
            endTime=end_time,
            queryString=query_string,
        )
    except ClientError as e:
        logger.warning(f"StartQuery failed: {e}")
        return None

    query_id = response["queryId"]

    # Poll for results (max 30 seconds)
    for _ in range(30):
        time.sleep(1)
        result = cw_logs.get_query_results(queryId=query_id)
        status = result["status"]

        if status == "Complete":
            return format_query_results(result.get("results", []))
        elif status in ("Failed", "Cancelled", "Timeout"):
            logger.warning(f"Query {query_id} ended with status: {status}")
            return None

    # Timed out waiting — stop the query
    try:
        cw_logs.stop_query(queryId=query_id)
    except Exception:
        pass

    logger.warning(f"Query {query_id} did not finish in time")
    return None


def format_query_results(results):
    """Format Insights query results into readable lines."""
    lines = []
    for row in results[:20]:  # Cap at 20 rows
        fields = {f["field"]: f["value"] for f in row}
        lines.append(fields)
    return lines


def is_always_thinking_model(model_id):
    """Claude models that think on every request reject `temperature` and lead
    their response with thinking blocks (mirrors common::is_always_thinking_model)."""
    return model_id.startswith(("claude-opus-5", "claude-sonnet-5", "claude-fable-", "claude-mythos-"))


def build_request_body(prompt):
    body = {
        "model": MODEL_ID,
        "max_tokens": 16000,
        "messages": [{"role": "user", "content": prompt}],
    }
    if is_always_thinking_model(MODEL_ID):
        body["output_config"] = {"effort": "medium"}
    else:
        body["max_tokens"] = 4096
        body["temperature"] = 0.1
    return body


def response_text(response_data):
    """Concatenate the text blocks of a Messages API response (skips thinking)."""
    return "".join(
        block.get("text", "")
        for block in response_data.get("content", [])
        if block.get("type") == "text"
    )


def parse_recommendations(output_text):
    """Parse the model's JSON array (tolerates a fenced code block). None if invalid."""
    text = output_text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else ""
        if text.rstrip().endswith("```"):
            text = text.rstrip()[:-3]
    start, end = text.find("["), text.rfind("]")
    if start == -1 or end < start:
        return None
    try:
        recs = json.loads(text[start : end + 1])
    except ValueError:
        return None
    if not isinstance(recs, list):
        return None
    return [r for r in recs if isinstance(r, dict)][:10]


def analyze_with_anthropic(query_results, account_id, api_key, existing=None):
    """Send log error summaries to Anthropic Claude for analysis."""
    # Scrub any secrets/tokens from the data before sending to AI
    scrubbed_results = scrub_query_results(query_results)

    # Build context — NO raw log data, only aggregated summaries
    context = json.dumps(scrubbed_results, indent=2, default=str)

    # Truncate if too large (keep under 100K tokens)
    if len(context) > 50000:
        context = context[:50000] + "\n... (truncated)"

    known = [
        {
            "id": r["rec_id"],
            "title": r.get("title", ""),
            "source_log_group": r.get("source_log_group", ""),
            "error_pattern": r.get("error_pattern", "")[:300],
        }
        for r in (existing or [])
    ]
    known_json = json.dumps(known, indent=2)

    prompt = f"""You are a senior SRE analyzing CloudWatch Logs error summaries for AWS account {account_id}.

Below are aggregated error patterns from CloudWatch Logs Insights queries, grouped by log group. Each entry shows the error message and how many times it occurred.

<error_summaries>
{context}
</error_summaries>

Your job: identify REAL operational issues that need engineering attention. Think step-by-step:

1. For each error pattern, determine:
   - Is this an INTERNAL application error (code bug, resource exhaustion, misconfiguration)?
   - Or EXTERNAL noise (bot scanners, user typos, crawlers hitting nonexistent paths)?
   - What is the actual blast radius — is it affecting users or just generating log noise?

2. Group related errors into a single finding (e.g., multiple 502s from the same service = one finding)

3. Prioritize by impact: service outages > data integrity > performance > cleanup

Return a JSON array of recommendations. Each must have:
- "title": Short descriptive title (max 80 chars)
- "severity": "critical" | "warning" | "info"
- "summary": 2-3 sentence explanation. Be specific about what's broken and why.
- "suggested_action": Concrete steps — not generic advice. Reference actual service names, error codes, log groups.
- "source_log_group": The primary log group where this was detected
- "error_pattern": A representative error string for deduplication
- "existing_id": If this is the same underlying issue as one of the already-open findings below, that finding's "id"; otherwise null. Reuse an id only for the same issue, even if the wording differs.

Already-open findings for this account:
<open_findings>
{known_json}
</open_findings>

Severity guide:
- critical = service down/crashing, data loss risk, active security breach
- warning = degraded performance, recurring errors impacting users, resource pressure
- info = cleanup opportunities, optimization suggestions

Rules:
- Do NOT report bot/scanner noise (404s on /.env, /.git, /wp-admin, /robots.txt, /phpMyAdmin etc.) as application issues. These are automated internet scanners, not your application failing.
- Do NOT give generic advice like "review application logs" — be specific or don't include it.
- Quality over quantity — 3 high-signal findings beat 10 vague ones.
- Maximum 8 recommendations.

Return ONLY the JSON array, no surrounding text."""

    try:
        request_body = json.dumps(build_request_body(prompt)).encode("utf-8")

        req = Request(
            ANTHROPIC_API_URL,
            data=request_body,
            headers={
                "Content-Type": "application/json",
                "x-api-key": api_key,
                "anthropic-version": ANTHROPIC_VERSION,
            },
            method="POST",
        )

        with urlopen(req, timeout=120) as resp:
            response_data = json.loads(resp.read().decode("utf-8"))

        recommendations = parse_recommendations(response_text(response_data))
        if recommendations is None:
            logger.error("Anthropic response was not a JSON array")
        return recommendations

    except HTTPError as e:
        error_body = e.read().decode("utf-8") if e.fp else ""
        logger.error(f"Anthropic API error ({e.code}): {error_body}")
        return None
    except Exception as e:
        logger.error(f"Anthropic analysis failed: {e}", exc_info=True)
        return None


def list_pending_recommendations(team_id, account_id):
    """All pending findings for this account (paginated)."""
    items = []
    last_key = None
    while True:
        query_kwargs = {
            "KeyConditionExpression": "pk = :pk AND begins_with(sk, :prefix)",
            "FilterExpression": "#s = :pending AND source_account_id = :acct",
            "ExpressionAttributeNames": {"#s": "status"},
            "ExpressionAttributeValues": {
                ":pk": team_id,
                ":prefix": "REC#",
                ":pending": "pending",
                ":acct": account_id,
            },
        }
        if last_key:
            query_kwargs["ExclusiveStartKey"] = last_key
        result = aws_insights_table.query(**query_kwargs)
        for item in result.get("Items", []):
            item["rec_id"] = item["sk"][len("REC#"):]
            items.append(item)
        last_key = result.get("LastEvaluatedKey")
        if not last_key:
            break
    return items


def resolve_stale_recommendations(team_id, existing, seen_ids):
    """Mark open findings resolved when this complete run didn't see them."""
    now = datetime.now(timezone.utc).isoformat()
    resolved_count = 0
    for item in existing:
        if item["rec_id"] in seen_ids:
            continue
        try:
            aws_insights_table.update_item(
                Key={"pk": team_id, "sk": item["sk"]},
                UpdateExpression="SET #s = :s, resolved_at = :t, updated_at = :t",
                ConditionExpression="#s = :pending",
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={":s": "resolved", ":t": now, ":pending": "pending"},
            )
            resolved_count += 1
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") != "ConditionalCheckFailedException":
                logger.warning(f"Failed to resolve {item['sk']}: {e}")
    if resolved_count:
        logger.info(f"{team_id}: {resolved_count} recommendations resolved")


def error_hash_for(account_id, rec):
    raw = f"{account_id}:{rec.get('source_log_group', '')}:{rec.get('error_pattern', rec.get('title', ''))}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def match_existing(rec, existing, error_hash):
    """The open finding this recommendation is the same issue as, if any: the
    model's existing_id when it names an open finding, else the same hash."""
    by_id = {e["rec_id"]: e for e in existing}
    claimed = rec.get("existing_id")
    if isinstance(claimed, str) and claimed in by_id:
        return by_id[claimed]
    for e in existing:
        if e.get("error_hash") == error_hash:
            return e
    return None


def store_recommendation(team_id, account_id, rec, existing):
    """Store a finding, or refresh the open one it repeats.
    Returns (rec_id, is_new); rec_id is None when the write failed."""
    error_hash = error_hash_for(account_id, rec)
    now = datetime.now(timezone.utc).isoformat()
    # Open findings stay as long as they keep appearing.
    ttl_epoch = int((datetime.now(timezone.utc) + timedelta(days=7)).timestamp())

    match = match_existing(rec, existing, error_hash)
    if match:
        try:
            aws_insights_table.update_item(
                Key={"pk": team_id, "sk": match["sk"]},
                UpdateExpression="SET last_seen_at = :t, updated_at = :t, #ttl = :ttl ADD seen_count :one",
                ExpressionAttributeNames={"#ttl": "ttl"},
                ExpressionAttributeValues={":t": now, ":ttl": ttl_epoch, ":one": 1},
            )
        except Exception as e:
            logger.warning(f"Failed to refresh {match['sk']}: {e}")
        return match["rec_id"], False

    rec_id = ulid_now()
    try:
        aws_insights_table.put_item(
            Item={
                "pk": team_id,
                "sk": f"REC#{rec_id}",
                "status": "pending",
                "severity": rec.get("severity", "info"),
                "title": str(rec.get("title", "Untitled"))[:200],
                "summary": str(rec.get("summary", ""))[:2000],
                "suggested_action": str(rec.get("suggested_action", ""))[:2000],
                "source_log_group": str(rec.get("source_log_group", ""))[:500],
                "source_account_id": account_id,
                "error_pattern": str(rec.get("error_pattern", ""))[:500],
                "error_hash": error_hash,
                "created_at": now,
                "updated_at": now,
                "last_seen_at": now,
                "seen_count": 1,
                "ttl": ttl_epoch,
            }
        )
        existing.append({"rec_id": rec_id, "sk": f"REC#{rec_id}", "error_hash": error_hash})
        return rec_id, True
    except Exception as e:
        logger.error(f"Failed to store recommendation: {e}")
        return None, False


def record_connection_error(team_id, account_id, error_msg):
    """Note a failed analysis on the connection without disabling it."""
    try:
        now = datetime.now(timezone.utc).isoformat()
        aws_insights_table.update_item(
            Key={"pk": team_id, "sk": f"AWS_CONN#{account_id}"},
            UpdateExpression="SET last_error = :e, last_error_at = :t",
            ConditionExpression="attribute_exists(pk)",
            ExpressionAttributeValues={":e": error_msg[:500], ":t": now},
        )
    except Exception as e:
        logger.error(f"Failed to record connection error: {e}")


def record_connection_success(team_id, account_id):
    try:
        now = datetime.now(timezone.utc).isoformat()
        aws_insights_table.update_item(
            Key={"pk": team_id, "sk": f"AWS_CONN#{account_id}"},
            UpdateExpression="SET last_analyzed_at = :t REMOVE last_error, last_error_at",
            ConditionExpression="attribute_exists(pk)",
            ExpressionAttributeValues={":t": now},
        )
    except Exception as e:
        logger.warning(f"Failed to record connection success: {e}")


# ─── Teams notifications for new recommendations ──────────────────────────

REC_NOTIFY_SK = "REC_NOTIFY"
TEAM_CHANNEL_SK = "ALERT_NOTIFY"  # the team's alert channel (common::alert_notify)
WEBHOOK_HOST_SUFFIXES = (".powerplatform.com", ".logic.azure.com", ".webhook.office.com")
SEVERITY_ORDER = {"critical": 0, "warning": 1, "info": 2}
SEVERITY_ICON = {"critical": "🔴", "warning": "🟠", "info": "🔵"}


def valid_webhook_url(url):
    """https on a Microsoft webhook host (mirrors common::alert_notify)."""
    if not isinstance(url, str) or not url.startswith("https://") or len(url) > 1000:
        return False
    if any(c.isspace() for c in url):
        return False
    authority = url[len("https://"):].split("/", 1)[0].split("?", 1)[0].split("#", 1)[0]
    if "@" in authority:
        return False
    host = authority.split(":", 1)[0].lower()
    return host.endswith(WEBHOOK_HOST_SUFFIXES)


def pick_webhook(mode, own_url, team_url, team_enabled):
    """The channel new recommendations post to. Off unless the team turned it on."""
    if mode == "custom":
        return own_url if valid_webhook_url(own_url) else None
    if mode == "team":
        return team_url if team_enabled and valid_webhook_url(team_url) else None
    return None


def recommendations_card(account_id, recs, dashboard_url):
    """Adaptive Card (Teams Workflows webhook) listing new findings."""
    recs = sorted(recs, key=lambda r: SEVERITY_ORDER.get(r.get("severity"), 3))
    worst = recs[0].get("severity", "info") if recs else "info"
    style, color = {
        "critical": ("attention", "Attention"),
        "warning": ("warning", "Warning"),
    }.get(worst, ("accent", "Accent"))
    n = len(recs)
    body = [
        {
            "type": "Container",
            "bleed": True,
            "style": style,
            "items": [{
                "type": "TextBlock",
                "text": f"🔎 {n} new log finding{'s' if n != 1 else ''}",
                "weight": "Bolder", "size": "Large", "color": color, "wrap": True,
            }],
        },
        {
            "type": "TextBlock",
            "text": f"CloudWatch Logs · AWS account {account_id}",
            "isSubtle": True, "spacing": "Small", "wrap": True,
        },
    ]
    for r in recs[:5]:
        sev = r.get("severity", "info")
        items = [
            {
                "type": "TextBlock",
                "text": f"{SEVERITY_ICON.get(sev, '⚪')} {str(r.get('title', 'Finding'))[:150]}",
                "weight": "Bolder", "wrap": True,
            },
            {
                "type": "TextBlock",
                "text": str(r.get("summary", ""))[:400],
                "wrap": True, "spacing": "Small",
            },
        ]
        if r.get("source_log_group"):
            items.append({
                "type": "TextBlock",
                "text": str(r["source_log_group"])[:200],
                "isSubtle": True, "size": "Small", "spacing": "None", "wrap": True,
            })
        body.append({"type": "Container", "separator": True, "spacing": "Medium", "items": items})
    if n > 5:
        body.append({"type": "TextBlock", "text": f"+{n - 5} more", "isSubtle": True, "wrap": True})
    return {
        "type": "message",
        "attachments": [{
            "contentType": "application/vnd.microsoft.card.adaptive",
            "contentUrl": None,
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard",
                "version": "1.5",
                "msteams": {"width": "Full"},
                "body": body,
                "actions": [{
                    "type": "Action.OpenUrl",
                    "title": "Review findings",
                    "url": f"{dashboard_url}/settings/aws",
                }],
            },
        }],
    }


def notify_new_recommendations(team_id, account_id, recs):
    """Post one card for this run's new findings. Best effort."""
    if not recs:
        return
    try:
        own = settings_table.get_item(Key={"pk": team_id, "sk": REC_NOTIFY_SK}).get("Item") or {}
        mode = own.get("notify_mode", "off")
        if mode == "off":
            return
        team = settings_table.get_item(Key={"pk": team_id, "sk": TEAM_CHANNEL_SK}).get("Item") or {}
        url = pick_webhook(
            mode,
            own.get("teams_webhook_url", ""),
            team.get("teams_webhook_url", ""),
            bool(team.get("enabled", False)),
        )
        if not url:
            return
        card = recommendations_card(account_id, recs, DASHBOARD_URL)
        req = Request(
            url,
            data=json.dumps(card).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urlopen(req, timeout=10) as resp:
            if resp.status >= 300:
                logger.warning(f"Teams webhook returned {resp.status}")
    except HTTPError as e:
        logger.warning(f"Teams notification failed ({e.code})")
    except Exception as e:
        logger.warning(f"Teams notification failed: {e}")


def ulid_now():
    """Generate a time-sortable ULID-like ID."""
    import random
    import string

    # Timestamp component (milliseconds since epoch, base32)
    ts = int(time.time() * 1000)
    ts_chars = []
    alphabet = "0123456789abcdefghjkmnpqrstvwxyz"  # Crockford's Base32
    for _ in range(10):
        ts_chars.append(alphabet[ts & 0x1F])
        ts >>= 5
    ts_part = "".join(reversed(ts_chars))

    # Random component
    rand_part = "".join(random.choices(alphabet, k=16))

    return ts_part + rand_part


def group_results_by_log_group(all_results):
    """Reorganize query results so they are grouped by log group."""
    grouped = {}  # log_group_name -> list of {query_name, description, results}

    for entry in all_results:
        query_name = entry["query_name"]
        description = entry["description"]
        log_groups_in_batch = entry.get("log_groups", [])

        # Split results by @logGroup field if present
        per_group = {}  # log_group -> [rows]
        ungrouped = []
        for row in entry.get("results", []):
            lg = row.get("@logGroup")
            if lg:
                per_group.setdefault(lg, []).append(row)
            else:
                ungrouped.append(row)

        # Add grouped rows
        for lg, rows in per_group.items():
            grouped.setdefault(lg, []).append(
                {"query_name": query_name, "description": description, "results": rows}
            )

        # If no @logGroup field was returned, fall back to batch log groups
        if ungrouped:
            key = ", ".join(log_groups_in_batch[:3])
            if len(log_groups_in_batch) > 3:
                key += f" (+{len(log_groups_in_batch) - 3} more)"
            grouped.setdefault(key or "unknown", []).append(
                {"query_name": query_name, "description": description, "results": ungrouped}
            )

    # Convert to a list format for the prompt
    return [
        {"log_group": lg, "queries": queries}
        for lg, queries in sorted(grouped.items())
    ]


def chunk_list(lst, chunk_size):
    """Split a list into chunks."""
    for i in range(0, len(lst), chunk_size):
        yield lst[i : i + chunk_size]
