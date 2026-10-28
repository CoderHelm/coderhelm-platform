"""
Coderhelm Log Analyzer Lambda

Triggered by EventBridge every 6 hours. For each team with an AWS connection:
1. AssumeRole into the customer's account
2. Run pre-built CloudWatch Logs Insights queries, counted per log group
   and stream (@log, @logStream)
3. Send that evidence to Anthropic Claude, which writes the findings' words
4. Credit each finding to the log groups whose lines match it, check each
   group alone, guard the title against services the evidence doesn't show
   (attribution.py), then match each to the finding it already is, by the
   error signature of its lines (fingerprint.py, lifecycle.py), and store it
   in DynamoDB
5. Post new findings to the team's Teams channel, when turned on

Raw logs are not stored: only counts, times and up to three scrubbed sample
lines per finding, each with its log group and stream.
"""

import base64
import json
import logging
import os
import time
from datetime import datetime, timezone
from urllib.request import Request, urlopen
from urllib.error import HTTPError

import boto3
from botocore.exceptions import ClientError

import attribution
import fingerprint
import lifecycle
import usage

logger = logging.getLogger()
logger.setLevel(logging.INFO)

AWS_INSIGHTS_TABLE = os.environ.get("AWS_INSIGHTS_TABLE_NAME", "coderhelm-prod-aws-insights")
SETTINGS_TABLE = os.environ.get("SETTINGS_TABLE_NAME", "coderhelm-prod-settings")
MODEL_ID = os.environ.get("MODEL_ID", "claude-sonnet-4-6")
CODERHELM_ACCOUNT_ID = os.environ["CODERHELM_ACCOUNT_ID"]
LOOKBACK_HOURS = int(os.environ.get("LOOKBACK_HOURS", "24"))
# This deployment's dashboard (the deploy always sets it); never the hosted
# dashboard, which a self-hosted install's teams can't sign in to.
DASHBOARD_URL = os.environ["DASHBOARD_URL"].rstrip("/")
# Team usage counters (empty: usage isn't counted).
ANALYTICS_TABLE = os.environ.get("ANALYTICS_TABLE_NAME", "")
# Every model call asks the team's token limits (settings) first.
usage.SETTINGS_TABLE = SETTINGS_TABLE
# Outbound webhook events go on this queue (empty: none are sent). The
# worker sends them to the team's endpoints (common::webhooks).
WEBHOOK_QUEUE_URL = os.environ.get("WEBHOOK_QUEUE_URL", "")
# Must equal common::webhooks::catalog::API_VERSION (a Rust test checks).
WEBHOOK_API_VERSION = "2026-10-26"

dynamodb = boto3.resource("dynamodb")
dynamodb_client = boto3.client("dynamodb")
aws_insights_table = dynamodb.Table(AWS_INSIGHTS_TABLE)
settings_table = dynamodb.Table(SETTINGS_TABLE)


def _stored_model_provider():
    """The install's model provider from Settings, System, Models (or None)."""
    item = settings_table.get_item(Key={"pk": "SYSTEM", "sk": "MODELS"}).get("Item") or {}
    return json.loads(item["data"]) if item.get("data") else None


usage.RUNTIME_LOADER = _stored_model_provider
sts_client = boto3.client("sts")
kms_client = boto3.client("kms")
sqs_client = boto3.client("sqs")

# ── Pre-built Insights Queries ──────────────────────────────────

# Every query runs over many log groups at once, so each returns @log
# (account:log group) and @logStream and is grouped by them: a count always
# belongs to one log group (attribution.discovery_query). The filters only
# choose which lines are worth reading.
INSIGHTS_QUERIES = [
    {
        "name": "lambda_errors",
        "description": "Lambda function errors and timeouts",
        "log_group_pattern": "/aws/lambda/",
        "query": attribution.discovery_query([
            "filter @message like /(?i)(error|exception|timeout|task timed out|out of memory|runtime exited)/",
            "filter @message not like /(?i)(info|debug|warn.*deprecat)/",
        ]),
    },
    {
        "name": "api_gateway_5xx",
        "description": "API Gateway 5xx errors",
        "log_group_pattern": "api-gateway",
        "query": attribution.discovery_query([
            "filter @message like /\" 5\\d{2} \"/",
        ]),
    },
    {
        "name": "ecs_crashes",
        "description": "ECS task crashes and OOM kills",
        "log_group_pattern": "/ecs/",
        "query": attribution.discovery_query([
            "filter @message like /(?i)(oom|killed|signal|exit code [^0]|panic|fatal|segfault)/",
        ]),
    },
    {
        "name": "general_errors",
        "description": "General application errors across all log groups",
        "log_group_pattern": None,  # matches any
        "query": attribution.discovery_query([
            "filter @message like /(?i)(ERROR|FATAL|CRITICAL|UnhandledPromiseRejection|Traceback)/",
            "filter @message not like /(?i)(healthcheck|ping|OPTIONS)/",
            # Filter out common bot/scanner noise before it reaches the LLM
            "filter @message not like /(?i)(Not found: \\/\\.env|Not found: \\/\\.git|Not found: \\/wp-|Not found: \\/admin|Not found: \\/phpMyAdmin|Not found: \\/\\.aws|Not found: \\/\\.DS_Store|Not found: \\/robots\\.txt|Not found: \\/favicon\\.ico|Not found: \\/sitemap)/",
        ]),
    },
    {
        "name": "http_4xx_5xx",
        "description": "HTTP error responses from web services",
        "log_group_pattern": "/ecs/",
        "query": attribution.discovery_query([
            "filter @message like /(?i)(\\\"(GET|POST|PUT|DELETE|PATCH).*\\\" [45]\\d{2}|HTTP [45]\\d{2}|status[=: ]+[45]\\d{2})/",
            # Exclude known scanner paths
            "filter @message not like /(?i)(\\.env|\\.git|wp-admin|wp-login|phpMyAdmin|\\.aws|xmlrpc|wp-content|wp-includes)/",
        ]),
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


def logs_client_for(conn):
    """A CloudWatch Logs client in the team's account, through its log reader
    role (15-minute session, the minimum). Shared with repair_attribution."""
    assumed = sts_client.assume_role(
        RoleArn=conn["role_arn"],
        RoleSessionName="coderhelm-analyzer",
        ExternalId=conn["external_id"],
        DurationSeconds=900,
    )
    creds = assumed["Credentials"]
    return boto3.client(
        "logs",
        region_name=conn["region"],
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretAccessKey"],
        aws_session_token=creds["SessionToken"],
    )


def log_groups_for(conn, cw_logs):
    """The log groups the team configured, or every one in the account."""
    log_groups = conn.get("log_groups", [])
    if not log_groups:
        log_groups = discover_log_groups(cw_logs)
        logger.info(f"Auto-discovered {len(log_groups)} log groups")
    return list(log_groups)


def lookback_window(hours=None):
    """(start, end) in epoch seconds; every query in a run uses the same one."""
    end = int(time.time())
    return end - int(hours or LOOKBACK_HOURS) * 3600, end


def analyze_connection(conn):
    """Analyze a single AWS connection — AssumeRole, query logs, generate recommendations."""
    team_id = conn["team_id"]
    account_id = conn["account_id"]

    logger.info(f"Analyzing {team_id} / account {account_id}")

    # Load team's Anthropic API key
    # On Bedrock the Lambda's own role calls the model: no team key needed.
    api_key = "" if usage.is_bedrock() else get_team_api_key(team_id)
    if not api_key and not usage.is_bedrock():
        logger.warning(f"No Anthropic API key configured for {team_id}, skipping analysis")
        return 0

    cw_logs = logs_client_for(conn)
    log_groups = log_groups_for(conn, cw_logs)
    start, end = lookback_window()

    # Run queries and collect results. A query that fails is not "no errors":
    # when any fails, nothing is auto-resolved this run.
    batches = []
    any_query_failed = False
    for query_def in INSIGHTS_QUERIES:
        matching_groups = filter_log_groups(log_groups, query_def.get("log_group_pattern"))
        if not matching_groups:
            continue

        # Run query against matching log groups (batch of 50 max)
        for batch in chunk_list(matching_groups, 50):
            results = run_insights_query(cw_logs, batch, query_def["query"], start, end)
            if results is None:
                any_query_failed = True
                continue
            if results:
                batches.append(
                    {
                        "query_name": query_def["name"],
                        "description": query_def["description"],
                        "log_groups": batch,
                        "results": results,
                    }
                )

    # Every finding a sighting could be: open, acknowledged, being fixed,
    # ignored, or resolved recently (lifecycle.is_recent).
    existing = list_recent_findings(team_id, account_id)

    if not batches:
        record_connection_success(team_id, account_id)
        if any_query_failed:
            logger.warning(f"{team_id}/{account_id}: queries failed and none returned rows, skipping resolution")
            return 0
        logger.info(f"No errors found for {team_id}/{account_id}")
        # Every query ran and found nothing: the findings are no longer erroring.
        resolve_stale_recommendations(team_id, existing, set())
        return 0

    # One entry per log group and message, each with its own counts and times.
    evidence = attribution.build_evidence(batches, scrub_secrets)

    seen_ids = set()
    new_recs = []

    for advisory in secrets_advisories(batches, evidence, account_id):
        rec_id, is_new = store_finding(team_id, account_id, advisory, existing, None, (start, end))
        if rec_id:
            seen_ids.add(rec_id)
        if is_new or advisory.get("seen_again"):
            new_recs.append(advisory)

    # The model reads the attributed evidence and writes the words.
    recommendations = analyze_with_anthropic(evidence, account_id, api_key, existing, team_id=team_id)

    # None = the analysis failed: we can't tell "no issues" from "API error",
    # so nothing is resolved.
    if recommendations is None:
        logger.warning(f"{team_id}/{account_id}: analysis failed, skipping resolution")
        notify_new_recommendations(team_id, account_id, new_recs)
        return len(new_recs)

    record_connection_success(team_id, account_id)

    # Credit each finding to the log groups whose lines match it, checked one
    # group at a time; then make its words match that evidence.
    candidates, check_failed = attribute_recommendations(cw_logs, recommendations, evidence, start, end)
    if check_failed:
        any_query_failed = True
    attribution.guard_texts(
        candidates, log_groups, lambda prompt: ask_model(prompt, api_key, team_id)
    )

    for cand in candidates:
        rec_id, is_new = store_finding(team_id, account_id, cand, existing, cw_logs, (start, end))
        if rec_id:
            seen_ids.add(rec_id)
        # A resolved finding that came back is news; an ignored one is not.
        if is_new or cand.get("seen_again"):
            new_recs.append(cand)

    # Resolve findings that stopped appearing for the quiet period — only
    # after a complete run.
    if not any_query_failed:
        resolve_stale_recommendations(team_id, existing, seen_ids)

    notify_new_recommendations(team_id, account_id, new_recs)
    logger.info(f"{team_id}/{account_id}: {len(new_recs)} new recommendations")
    return len(new_recs)


def secrets_advisories(batches, evidence, account_id):
    """One finding per log group whose raw query rows (before scrubbing)
    carry secrets, counted from that group's rows; [] when there are none.
    Each is credited to the group its lines are in, never to "multiple"."""
    per_group = {}
    for b in batches:
        scope = tuple(b.get("log_groups", []))
        for row in b.get("results", []):
            account, group = attribution.split_log(row.get("@log"), scope)
            if not group:
                continue
            raw = str(row.get("@message", ""))
            found, types = 0, set()
            for pattern, replacement in SECRET_PATTERNS:
                if not replacement:
                    continue
                matches = pattern.findall(raw)
                if matches:
                    found += len(matches)
                    types.add(replacement.strip("[]"))
            if not found:
                continue
            g = per_group.setdefault(group, {
                "account": account, "found": 0, "types": set(), "lines": 0, "streams": {},
            })
            n = attribution.as_int(row.get("n") or 1) or 1
            g["found"] += found
            g["types"] |= types
            g["lines"] += n
            stream = str(row.get("@logStream", "") or "")
            if stream:
                g["streams"][stream] = g["streams"].get(stream, 0) + n
    out = []
    for group, g in sorted(per_group.items()):
        mine = [e for e in evidence if e["log_group"] == group]
        firsts = [e["first"] for e in mine if e["first"]]
        lasts = [e["last"] for e in mine if e["last"]]
        types_str = ", ".join(sorted(g["types"]))
        out.append({
            "id": f"secrets-{group}",
            "title": f"Secrets detected in CloudWatch Logs ({g['found']} instances)",
            "severity": "critical",
            "summary": (
                f"We detected {g['found']} potential secrets or tokens in {group} "
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
            "log_group": group,
            "account": g["account"] or account_id,
            "literal": f"secrets_in_logs_{account_id}",
            # One finding per group whatever the count or wording.
            "signature": "secrets | secrets in logs |",
            "stats": {
                "count": g["lines"],
                "first": min(firsts) if firsts else None,
                "last": max(lasts) if lasts else None,
                "streams": g["streams"],
                "samples": [],
            },
        })
    return out


def attribute_recommendations(cw_logs, recommendations, evidence, start, end, run=None):
    """One candidate per (model finding, log group with matching lines).

    The log group, account, count and times of each come from Insights
    (attribution.attribute over every group the cited evidence was searched
    in, then attribution.check_group on each group alone). A finding whose
    text matches nowhere is dropped. Returns (candidates, a query failed)."""
    run = run or attribution.run_query
    candidates, failed = [], False
    for i, rec in enumerate(recommendations):
        literal, cited = attribution.choose_literal(rec, evidence)
        if not literal:
            logger.info(f"Dropped '{str(rec.get('title', ''))[:60]}': no evidence line backs it")
            continue
        stats = attribution.attribute(
            cw_logs, attribution.scope_of(cited), literal, start, end, scrub_secrets, run
        )
        if stats is None:
            failed = True
            continue
        credited, check_failed = attribution.credited_groups(
            stats, cw_logs, literal, start, end, scrub_secrets, run
        )
        failed = failed or check_failed
        if not credited:
            logger.info(f"Dropped '{str(rec.get('title', ''))[:60]}': no log group has '{literal[:40]}' lines")
            continue
        for g in credited:
            candidates.append({
                "id": f"f{i}-{g['log_group']}",
                "title": str(rec.get("title", "Untitled")),
                "summary": str(rec.get("summary", "")),
                "suggested_action": str(rec.get("suggested_action", "")),
                "severity": rec.get("severity", "info"),
                "suspected_location": rec.get("suspected_location"),
                "existing_id": rec.get("existing_id"),
                "log_group": g["log_group"],
                "account": g["account"],
                "literal": literal,
                "stats": g,
                # This run's distinct lines of the error in this group, with
                # their counts: what its signature is worked out from.
                "lines": [
                    (e["message"], e["count"])
                    for e in evidence
                    if e["log_group"] == g["log_group"] and literal in e["message"]
                ],
                "others": [
                    {"log_group": o["log_group"], "lines": o["count"]}
                    for o in credited
                    if o is not g
                ],
            })
    return candidates, failed


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


def run_insights_query(cw_logs, log_groups, query_string, start=None, end=None):
    """Run a CloudWatch Logs Insights query; rows as dicts, None when it failed."""
    if start is None or end is None:
        start, end = lookback_window()
    return attribution.run_query(cw_logs, log_groups, query_string, start, end)


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



def ask_model(prompt, api_key, team_id=None):
    """One model call; its text, or None when it failed. Every attempt is
    counted in the team's usage (usage.call_messages)."""
    try:
        if usage.is_bedrock():
            response_data = usage.call_messages_bedrock(
                build_request_body(prompt), MODEL_ID,
                dynamodb_client, ANALYTICS_TABLE, team_id, usage.FEATURE_LOG_ANALYSIS,
            )
        else:
            response_data = usage.call_messages(
                api_key, build_request_body(prompt), MODEL_ID,
                dynamodb_client, ANALYTICS_TABLE, team_id, usage.FEATURE_LOG_ANALYSIS,
            )
        return response_text(response_data)
    except usage.LimitReached as e:
        logger.warning(f"Analysis skipped for {team_id}: {e}")
        return None
    except HTTPError as e:
        error_body = e.read().decode("utf-8") if e.fp else ""
        logger.error(f"Anthropic API error ({e.code}): {error_body}")
        return None
    except Exception as e:
        logger.error(f"Anthropic analysis failed: {e}", exc_info=True)
        return None


def analysis_prompt(evidence, account_id, existing=None):
    """The analysis prompt: attributed evidence in, words out. The model cites
    evidence ids; it never names the log group a finding is saved under."""
    context = json.dumps(attribution.evidence_for_prompt(evidence), indent=2, default=str)

    # Truncate if too large (keep under 100K tokens)
    if len(context) > 50000:
        context = context[:50000] + "\n... (truncated)"

    # Findings people act on first, then the latest; at most 60.
    ranked = sorted(existing or [], key=lambda r: (lifecycle.rank(r), r.get("last_seen_at") or ""))
    known = [
        {
            "id": r["rec_id"],
            "title": r.get("title", ""),
            "status": r.get("status", "pending"),
            "source_log_group": r.get("source_log_group", ""),
            "error_pattern": r.get("error_pattern", "")[:300],
        }
        for r in ranked[:60]
    ]
    known_json = json.dumps(known, indent=2)

    return f"""You are a senior SRE analyzing CloudWatch Logs error summaries for AWS account {account_id}.

Below is the evidence from CloudWatch Logs Insights. Each entry is one error message in ONE log group, with its id, the number of matching lines, the first and last time it was seen and the log streams it came from. These numbers are exact; do not change or reassign them.

<evidence>
{context}
</evidence>

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
- "suggested_action": Concrete steps — not generic advice. Reference actual error codes and the log groups of the evidence you cite.
- "evidence_ids": The ids of every evidence entry this finding is about. If the same error appears in several log groups, cite all of them; each log group becomes its own finding.
- "error_pattern": Text copied exactly from the cited evidence messages that every occurrence of this error shares, at most 80 characters. Leave out ids, hashes, timestamps and numbers that change between lines.
- "suspected_location": The file, function or module the error text points to (from a stack trace, logger name or path in the messages), or null when the logs don't show one. Never guess.
- "existing_id": If this is the same underlying issue as one of the known findings below (open, ignored or resolved), that finding's "id"; otherwise null. Reuse an id only for the same issue, even if the wording differs.

Name a service or log group in the title, summary or suggested action only when it is the log_group of evidence you cite. Never infer a service from another log group.

Known findings for this account:
<known_findings>
{known_json}
</known_findings>

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


def analyze_with_anthropic(evidence, account_id, api_key, existing=None, team_id=None):
    """The model's findings for this run's evidence (scrubbed when it was
    built), or None when the call or its JSON failed."""
    text = ask_model(analysis_prompt(evidence, account_id, existing), api_key, team_id)
    if text is None:
        return None
    recommendations = parse_recommendations(text)
    if recommendations is None:
        logger.error("Anthropic response was not a JSON array")
    return recommendations


def list_recent_findings(team_id, account_id, now=None):
    """Every finding of this account a new sighting could be (paginated):
    open, acknowledged, being fixed, ignored, or resolved within
    lifecycle.REOPEN_DAYS. Merged duplicates are left out."""
    items = []
    last_key = None
    while True:
        query_kwargs = {
            "KeyConditionExpression": "pk = :pk AND begins_with(sk, :prefix)",
            "FilterExpression": "source_account_id = :acct",
            "ExpressionAttributeValues": {
                ":pk": team_id,
                ":prefix": "REC#",
                ":acct": account_id,
            },
        }
        if last_key:
            query_kwargs["ExclusiveStartKey"] = last_key
        result = aws_insights_table.query(**query_kwargs)
        for item in result.get("Items", []):
            item["rec_id"] = item["sk"][len("REC#"):]
            if lifecycle.is_recent(item, now):
                items.append(item)
        last_key = result.get("LastEvaluatedKey")
        if not last_key:
            break
    return items


def resolve_stale_recommendations(team_id, existing, seen_ids, now=None):
    """Mark open findings resolved when this complete run didn't see them and
    no matching line came for lifecycle.QUIET_DAYS. One missed run never
    resolves a finding: the next sighting would have to make a new one."""
    now_dt = now or datetime.now(timezone.utc)
    now_iso = now_dt.isoformat()
    resolved_count = 0
    for item in lifecycle.stale(existing, seen_ids, now_dt):
        try:
            aws_insights_table.update_item(
                Key={"pk": team_id, "sk": item["sk"]},
                UpdateExpression="SET #s = :s, resolved_at = :t, updated_at = :t",
                ConditionExpression="#s = :pending",
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={":s": "resolved", ":t": now_iso, ":pending": "pending"},
            )
            resolved_count += 1
            item["status"], item["resolved_at"] = "resolved", now_iso
            emit_event(
                team_id,
                "log_finding.resolved",
                finding_event_data(
                    item["rec_id"],
                    item.get("source_account_id", ""),
                    item,
                    resolved_at=now_iso.replace("+00:00", "Z"),
                ),
            )
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") != "ConditionalCheckFailedException":
                logger.warning(f"Failed to resolve {item['sk']}: {e}")
    if resolved_count:
        logger.info(f"{team_id}: {resolved_count} recommendations resolved")


def finding_url(rec_id):
    return f"{DASHBOARD_URL.rstrip('/')}/logs/?finding={rec_id}"


def webhook_event(team_id, event_type, data):
    """The envelope common::webhooks::Event has, as the worker reads it."""
    now = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    return {
        "id": "evt_" + ulid_now().upper(),
        "type": event_type,
        "created_at": now,
        "team_id": team_id,
        "api_version": WEBHOOK_API_VERSION,
        "data": data,
    }


def emit_event(team_id, event_type, data, send=None):
    """Queue one event for the team's webhook endpoints. Never raises: the
    analysis matters more than the event."""
    if not WEBHOOK_QUEUE_URL or not team_id:
        return None
    event = webhook_event(team_id, event_type, data)
    body = json.dumps({"type": "webhook_emit", "team_id": team_id, "event": event})
    try:
        (send or sqs_client.send_message)(QueueUrl=WEBHOOK_QUEUE_URL, MessageBody=body)
        return event["id"]
    except Exception as e:  # noqa: BLE001
        logger.warning(f"Could not queue webhook event {event_type}: {type(e).__name__}")
        return None


def finding_event_data(rec_id, account_id, item, resolved_at=None):
    """`log_finding.created` / `log_finding.resolved` data (the catalog's fields)."""
    data = {
        "finding_id": rec_id,
        "account_id": account_id,
        "title": scrub_secrets(str(item.get("title", ""))),
        "severity": item.get("severity") if item.get("severity") in ("critical", "warning", "info") else "info",
    }
    if resolved_at is None:
        data["log_group"] = item.get("source_log_group") or None
    else:
        data["resolved_at"] = resolved_at
    data["url"] = finding_url(rec_id)
    return data


def error_hash_for(account_id, rec):
    """The identity findings had before error signatures (account, group and
    the model's error pattern). Still matched, so older findings are found;
    new findings use sighting_of's fingerprint."""
    return lifecycle.legacy_hash(
        account_id, rec.get("source_log_group", ""), rec.get("error_pattern", rec.get("title", ""))
    )


def sighting_of(account_id, cand):
    """What identifies a candidate: its error signatures, worked out from its
    lines (this run's evidence lines in its group, then its sample lines),
    never from the words the model wrote. Returns a dict for
    lifecycle.find_match plus the signature and fingerprint to store."""
    group = str(cand["log_group"])
    literal = str(cand.get("literal", ""))
    lines = list(cand.get("lines") or [])
    lines += [s.get("message", "") for s in cand.get("stats", {}).get("samples", []) if s.get("message")]
    if cand.get("signature"):
        sigs = [cand["signature"]]
    else:
        sigs = fingerprint.signatures(lines) or fingerprint.signatures([literal])
    main = sigs[0] if sigs else ""
    return {
        "log_group": group,
        "signature": main,
        "fingerprint": fingerprint.fingerprint(account_id, group, main) if main else "",
        "signature_hashes": [fingerprint.signature_hash(x) for x in sigs],
        "legacy_hash": error_hash_for(account_id, {"source_log_group": group, "error_pattern": literal}),
        "lines": [l[0] if isinstance(l, tuple) else l for l in lines],
        "existing_id": cand.get("existing_id"),
    }


def suspected_location(rec):
    """The model's code location, when it named one."""
    loc = rec.get("suspected_location")
    if not isinstance(loc, str):
        return ""
    loc = loc.strip()
    return "" if loc.lower() in ("", "null", "none", "unknown") else loc[:300]


def lines_since(cw_logs, group, literal, prev, window):
    """Matching lines after a stored finding's last_seen_at, for seen_count.
    None when that time is before this run's window (every line in it is
    new). 0 when the check can't run: an unanswered question never adds."""
    start, end = window
    prev_last = attribution.parse_ts(prev.get("last_seen_at"))
    if prev_last is None or prev_last.timestamp() < start:
        return None
    if cw_logs is None:
        return 0
    # Insights takes whole seconds; lines in the same second as the stored
    # last one are not counted again (a rare undercount, never a double count).
    checked = attribution.check_group(
        cw_logs, group, literal, int(prev_last.timestamp()) + 1, end, scrub_secrets
    )
    return checked["count"] if checked else 0


def store_finding(team_id, account_id, cand, existing, cw_logs=None, window=None, now=None):
    """Store a finding credited to one log group, or refresh the stored one it
    is. `cand` carries the evidence: log_group, account, literal, lines and
    checked stats (count, first, last, streams, samples). Returns (rec_id,
    is_new); rec_id is None when the write failed. Sets cand["seen_again"]
    when a resolved finding came back and was reopened.

    Identity is the error signature of the lines (sighting_of), matched
    against every recent finding (lifecycle.find_match), whatever its status:
    an open, acknowledged or fixing finding is refreshed, an ignored one is
    counted and stays ignored, a resolved one is reopened.

    Every number written comes from the stats: error_count = lines in this
    run's window, seen_count = matching lines over every run, first_seen_at
    and last_seen_at = the first and last line's own time, log_streams = the
    streams the lines are in, daily_lines = new lines per day."""
    window = window or lookback_window()
    group = str(cand["log_group"])
    literal = str(cand["literal"])
    stats = cand["stats"]
    now_dt = now or datetime.now(timezone.utc)
    now = now_dt.isoformat()
    today = now_dt.strftime("%Y-%m-%d")
    ttl_epoch = lifecycle.ttl_from(now_dt)
    samples = [dict(s) for s in stats.get("samples", [])][: attribution.MAX_SAMPLES]
    sighting = sighting_of(account_id, cand)
    # Chat cards name the group the lines are in.
    cand["source_log_group"] = group
    streams = lifecycle.streams_of(stats)
    others = lifecycle.others_of(cand)

    match = lifecycle.find_match(sighting, existing, now_dt)
    if match:
        new_lines = lines_since(cw_logs, group, literal, match, window)
        seen, first, last = attribution.merge_seen(match, stats, new_lines)
        added = stats["count"] if new_lines is None else new_lines
        hashes = fingerprint.merge_hashes(
            sorted(fingerprint.item_hashes(match)) if not match.get("signature_hashes")
            else match.get("signature_hashes"),
            sighting["signature_hashes"],
        )
        history = lifecycle.add_history(match.get("daily_lines"), today, added)
        values = {
            ":t": now,
            ":ttl": ttl_epoch,
            ":c": stats["count"],
            ":seen": seen,
            ":f": attribution.iso(first) or now,
            ":l": attribution.iso(last) or now,
            ":basis": "events",
            ":samples": samples,
            ":acct": cand.get("account") or account_id,
            ":sigs": hashes,
            ":streams": streams,
            ":others": others,
            ":hist": history,
            ":fp": sighting["fingerprint"],
            ":sig": sighting["signature"][:300],
        }
        sets = (
            "last_seen_at = :l, first_seen_at = :f, updated_at = :t, error_count = :c, "
            "seen_count = :seen, count_basis = :basis, samples = :samples, log_account_id = :acct, "
            "signature_hashes = :sigs, log_streams = :streams, also_in = :others, daily_lines = :hist, "
            "fingerprint = if_not_exists(fingerprint, :fp), #sig = if_not_exists(#sig, :sig)"
        )
        names = {"#sig": "signature"}
        reopened = match.get("status") == "resolved"
        removes = ""
        if reopened:
            # Seen again after it was resolved: the same finding, open again.
            values.update({
                ":open": "pending",
                ":was": match.get("resolved_at") or now,
                ":one": 1,
                ":zero": 0,
            })
            sets += (
                ", #s = :open, reopened_at = :t, last_resolved_at = :was, "
                "reopen_count = if_not_exists(reopen_count, :zero) + :one"
            )
            names["#s"] = "status"
            removes = " REMOVE resolved_at"
        if match.get("status") == "approved":
            # A finding with a plan has no TTL (the gateway removed it when
            # the plan was made); seeing it again keeps it so.
            refresh_fixing(team_id, match["sk"], sets + removes, values, names)
        else:
            try:
                aws_insights_table.update_item(
                    Key={"pk": team_id, "sk": match["sk"]},
                    # A finding being fixed has no TTL; seeing it again keeps it so.
                    UpdateExpression=f"SET {sets}, #ttl = :ttl{removes}",
                    ConditionExpression="attribute_not_exists(fix_ticket_id)",
                    ExpressionAttributeNames={**names, "#ttl": "ttl"},
                    ExpressionAttributeValues=values,
                )
            except ClientError as e:
                if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                    refresh_fixing(team_id, match["sk"], sets + removes, values, names)
                else:
                    logger.warning(f"Failed to refresh {match['sk']}: {e}")
            except Exception as e:
                logger.warning(f"Failed to refresh {match['sk']}: {e}")
        match.update({
            "last_seen_at": values[":l"], "first_seen_at": values[":f"],
            "seen_count": seen, "count_basis": "events", "signature_hashes": hashes,
            "daily_lines": history,
        })
        if reopened:
            match.update({"status": "pending", "reopened_at": now})
            match.pop("resolved_at", None)
            cand["seen_again"] = True
        return match["rec_id"], False

    first, last = stats.get("first"), stats.get("last")
    rec_id = ulid_now()
    error_hash = sighting["fingerprint"] or sighting["legacy_hash"]
    item = {
        "pk": team_id,
        "sk": f"REC#{rec_id}",
        "status": "pending",
        "severity": cand.get("severity", "info"),
        "title": str(cand.get("title", "Untitled"))[:200],
        "summary": str(cand.get("summary", ""))[:2000],
        "suggested_action": str(cand.get("suggested_action", ""))[:2000],
        "source_log_group": group[:500],
        "source_account_id": account_id,
        "log_account_id": cand.get("account") or account_id,
        "error_pattern": literal[:500],
        # The fix ticket id is made from error_hash (Finding::fingerprint),
        # so it is the signature fingerprint too.
        "error_hash": error_hash,
        "fingerprint": sighting["fingerprint"],
        "signature": sighting["signature"][:300],
        "signature_hashes": sighting["signature_hashes"][: fingerprint.MAX_SIGNATURES],
        "log_streams": streams,
        "also_in": others,
        "daily_lines": lifecycle.add_history([], today, stats["count"]),
        "created_at": now,
        "updated_at": now,
        "first_seen_at": attribution.iso(first) or now,
        "last_seen_at": attribution.iso(last) or now,
        "error_count": stats["count"],
        "seen_count": stats["count"],
        "count_basis": "events",
        "samples": samples,
        "ttl": ttl_epoch,
    }
    location = suspected_location(cand)
    if location:
        item["suspected_location"] = location
    try:
        aws_insights_table.put_item(Item=item)
        emit_event(team_id, "log_finding.created", finding_event_data(rec_id, account_id, item))
        existing.append({
            "rec_id": rec_id, "sk": f"REC#{rec_id}", "status": "pending", "error_hash": error_hash,
            "fingerprint": item["fingerprint"], "signature_hashes": list(item["signature_hashes"]),
            "error_pattern": item["error_pattern"], "created_at": now,
            "source_log_group": group, "count_basis": "events", "seen_count": stats["count"],
            "first_seen_at": item["first_seen_at"], "last_seen_at": item["last_seen_at"],
            "daily_lines": item["daily_lines"],
        })
        return rec_id, True
    except Exception as e:
        logger.error(f"Failed to store recommendation: {e}")
        return None, False


def refresh_fixing(team_id, sk, sets, values, names=None):
    """Refresh a finding that has a fix run, keeping it without a TTL."""
    values = {k: v for k, v in values.items() if k != ":ttl"}
    kw = {"ExpressionAttributeNames": names} if names else {}
    try:
        aws_insights_table.update_item(
            Key={"pk": team_id, "sk": sk},
            UpdateExpression=f"SET {sets}",
            ExpressionAttributeValues=values,
            **kw,
        )
    except Exception as e:
        logger.warning(f"Failed to refresh {sk}: {e}")


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


# ─── Chat notifications (Teams, Slack) for new recommendations ─────────────
#
# Mirrors common::notify in the services: a destination posts to Teams, Slack
# or both, each webhook is sealed with KMS (bound to the team), and a failure
# on one channel never stops the other.

REC_NOTIFY_SK = "REC_NOTIFY"
TEAM_CHANNEL_SK = "ALERT_NOTIFY"  # the team's alert channel (common::alert_notify)
WEBHOOK_HOST_SUFFIXES = (".powerplatform.com", ".logic.azure.com", ".webhook.office.com")
SLACK_PREFIX = "https://hooks.slack.com/services/"
SEVERITY_ORDER = {"critical": 0, "warning": 1, "info": 2}
SEVERITY_ICON = {"critical": "🔴", "warning": "🟠", "info": "🔵"}
CHANNELS = ("teams", "slack")


def valid_webhook_url(url):
    """https on a Microsoft webhook host (mirrors common::notify::teams)."""
    if not isinstance(url, str) or not url.startswith("https://") or len(url) > 1000:
        return False
    if any(c.isspace() for c in url):
        return False
    authority = url[len("https://"):].split("/", 1)[0].split("?", 1)[0].split("#", 1)[0]
    if "@" in authority:
        return False
    host = authority.split(":", 1)[0].lower()
    return host.endswith(WEBHOOK_HOST_SUFFIXES)


def valid_slack_url(url):
    """https://hooks.slack.com/services/<team>/<bot>/<secret> (mirrors common::notify::slack)."""
    if not isinstance(url, str) or len(url) > 500 or not url.startswith(SLACK_PREFIX):
        return False
    parts = url[len(SLACK_PREFIX):].split("/")
    return len(parts) == 3 and all(p and p.isascii() and p.isalnum() for p in parts)


VALID = {"teams": valid_webhook_url, "slack": valid_slack_url}


def open_webhook(team_id, item, channel):
    """The URL one channel posts to: sealed (KMS, bound to the team), or a
    Teams URL saved before sealing. None when unset or unreadable."""
    sealed = item.get(f"{channel}_webhook_sealed") or ""
    if sealed:
        try:
            out = kms_client.decrypt(
                CiphertextBlob=base64.b64decode(sealed),
                EncryptionContext={"team": team_id, "purpose": "notify-webhook"},
            )
            url = out["Plaintext"].decode("utf-8")
        except Exception as e:  # noqa: BLE001 - one channel never stops the other
            logger.warning(f"Could not open the saved {channel} webhook: {type(e).__name__}")
            return None
    elif channel == "teams":
        url = item.get("teams_webhook_url") or ""
    else:
        return None
    url = url.strip()
    return url if url and VALID[channel](url) else None


def open_channels(team_id, item):
    """{channel: url} for every channel the item can post to."""
    out = {}
    for ch in CHANNELS:
        url = open_webhook(team_id, item, ch)
        if url:
            out[ch] = url
    return out


def pick_channels(mode, own, team, team_enabled):
    """The channels new recommendations post to. Off unless the team turned it on."""
    if mode == "custom":
        return own
    if mode == "team" and team_enabled:
        return team
    return {}


def pick_webhook(mode, own_url, team_url, team_enabled):
    """One Teams URL (kept for callers of the single-channel shape)."""
    own = {"teams": own_url} if valid_webhook_url(own_url) else {}
    team = {"teams": team_url} if valid_webhook_url(team_url) else {}
    return pick_channels(mode, own, team, team_enabled).get("teams")


def card_title(rec):
    """A finding's title in chat; one that came back after it was resolved
    says so."""
    title = str(rec.get("title", "Finding"))
    return f"Seen again: {title}" if rec.get("seen_again") else title


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
                "text": f"{SEVERITY_ICON.get(sev, '⚪')} {card_title(r)[:150]}",
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
                    "url": f"{dashboard_url}/logs",
                }],
            },
        }],
    }


def slack_text(text):
    """Text as Slack mrkdwn: &, < and > escaped (mirrors common::notify::slack)."""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("**", "*")


def recommendations_slack(account_id, recs, dashboard_url):
    """Block Kit (Slack incoming webhook) listing new findings."""
    recs = sorted(recs, key=lambda r: SEVERITY_ORDER.get(r.get("severity"), 3))
    n = len(recs)
    title = f"🔎 {n} new log finding{'s' if n != 1 else ''}"
    blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": title[:150], "emoji": True}},
        {"type": "context", "elements": [{"type": "mrkdwn", "text": f"CloudWatch Logs · AWS account {account_id}"}]},
    ]
    for r in recs[:5]:
        sev = r.get("severity", "info")
        head = f"{SEVERITY_ICON.get(sev, '⚪')} *{slack_text(card_title(r)[:150])}*"
        summary = slack_text(str(r.get("summary", ""))[:400])
        blocks.append({"type": "divider"})
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": f"{head}\n{summary}".strip()[:3000]}})
        if r.get("source_log_group"):
            blocks.append({
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": slack_text(str(r["source_log_group"])[:200])}],
            })
    if n > 5:
        blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": f"+{n - 5} more"}]})
    blocks.append({
        "type": "actions",
        "elements": [{
            "type": "button",
            "action_id": "link-0",
            "text": {"type": "plain_text", "text": "Review findings", "emoji": True},
            "url": f"{dashboard_url}/logs",
        }],
    })
    return {"text": f"{title} in AWS account {account_id}", "blocks": blocks}


RENDER = {"teams": recommendations_card, "slack": recommendations_slack}


def post_json(url, payload):
    """POST one payload. Raises on a non-2xx answer; the URL is never logged."""
    req = Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(req, timeout=10) as resp:
        if resp.status >= 300:
            raise RuntimeError(f"webhook returned {resp.status}")


def deliver(channels, account_id, recs, post=post_json):
    """Post to every channel; one failing never stops the next. Returns
    {channel: True/False}."""
    sent = {}
    for ch, url in channels.items():
        try:
            post(url, RENDER[ch](account_id, recs, DASHBOARD_URL))
            sent[ch] = True
        except HTTPError as e:
            logger.warning(f"{ch} notification failed ({e.code})")
            sent[ch] = False
        except Exception as e:  # noqa: BLE001
            logger.warning(f"{ch} notification failed: {type(e).__name__}")
            sent[ch] = False
    return sent


def notify_new_recommendations(team_id, account_id, recs):
    """Post this run's new findings to the team's chosen channels. Best effort."""
    if not recs:
        return
    try:
        own = settings_table.get_item(Key={"pk": team_id, "sk": REC_NOTIFY_SK}).get("Item") or {}
        mode = own.get("notify_mode", "off")
        if mode == "off":
            return
        team = settings_table.get_item(Key={"pk": team_id, "sk": TEAM_CHANNEL_SK}).get("Item") or {}
        enabled = bool(team.get("enabled", False))
        channels = pick_channels(
            mode,
            open_channels(team_id, own) if mode == "custom" else {},
            open_channels(team_id, team) if mode == "team" and enabled else {},
            enabled,
        )
        if channels:
            deliver(channels, account_id, recs)
    except Exception as e:  # noqa: BLE001
        logger.warning(f"Recommendation notification failed: {type(e).__name__}")


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


def chunk_list(lst, chunk_size):
    """Split a list into chunks."""
    for i in range(0, len(lst), chunk_size):
        yield lst[i : i + chunk_size]
