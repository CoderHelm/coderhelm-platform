"""
Coderhelm Log Analyzer Lambda

Triggered by EventBridge every 6 hours. For each team with an AWS connection:
1. AssumeRole into the customer's account
2. Run pre-built CloudWatch Logs Insights queries
3. Send error summaries to Bedrock Claude for analysis
4. Deduplicate and store recommendations in DynamoDB

No raw logs are stored — only error summaries and recommendations.
"""

import hashlib
import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

SETTINGS_TABLE = os.environ.get("SETTINGS_TABLE_NAME", "coderhelm-prod-settings")
MODEL_ID = os.environ.get("MODEL_ID", "us.anthropic.claude-sonnet-4-6")
CODERHELM_ACCOUNT_ID = os.environ.get("CODERHELM_ACCOUNT_ID", "REDACTED_AWS_ACCOUNT_ID")
LOOKBACK_HOURS = int(os.environ.get("LOOKBACK_HOURS", "24"))

dynamodb = boto3.resource("dynamodb")
settings_table = dynamodb.Table(SETTINGS_TABLE)
bedrock = boto3.client("bedrock-runtime", region_name="us-east-1")
sts_client = boto3.client("sts")

# ── Pre-built Insights Queries ──────────────────────────────────

INSIGHTS_QUERIES = [
    {
        "name": "lambda_errors",
        "description": "Lambda function errors and timeouts",
        "log_group_pattern": "/aws/lambda/",
        "query": (
            "fields @timestamp, @message, @logStream "
            "| filter @message like /(?i)(error|exception|timeout|task timed out|out of memory|runtime exited)/ "
            "| filter @message not like /(?i)(info|debug|warn.*deprecat)/ "
            "| stats count() as error_count by @logStream "
            "| sort error_count desc "
            "| limit 20"
        ),
    },
    {
        "name": "api_gateway_5xx",
        "description": "API Gateway 5xx errors",
        "log_group_pattern": "api-gateway",
        "query": (
            "fields @timestamp, @message "
            "| filter @message like /\" 5\\d{2} \"/ "
            "| stats count() as error_count by @message "
            "| sort error_count desc "
            "| limit 20"
        ),
    },
    {
        "name": "ecs_crashes",
        "description": "ECS task crashes and OOM kills",
        "log_group_pattern": "/ecs/",
        "query": (
            "fields @timestamp, @message, @logStream "
            "| filter @message like /(?i)(oom|killed|signal|exit code [^0]|panic|fatal|segfault)/ "
            "| stats count() as crash_count by @logStream "
            "| sort crash_count desc "
            "| limit 20"
        ),
    },
    {
        "name": "general_errors",
        "description": "General application errors across all log groups",
        "log_group_pattern": None,  # matches any
        "query": (
            "fields @timestamp, @message "
            "| filter @message like /(?i)(ERROR|FATAL|CRITICAL|UnhandledPromiseRejection|Traceback)/ "
            "| filter @message not like /(?i)(healthcheck|ping|OPTIONS)/ "
            "| stats count() as error_count by @message "
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
    (re.compile(r"gh[pousr]_[A-Za-z0-9_]{36,}"), "[GITHUB_TOKEN]"),
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
            # Update connection status to error
            update_connection_status(
                conn["team_id"], conn["account_id"], "error", str(e)
            )

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

        resp = settings_table.scan(**scan_kwargs)

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


def analyze_connection(conn):
    """Analyze a single AWS connection — AssumeRole, query logs, generate recommendations."""
    team_id = conn["team_id"]
    account_id = conn["account_id"]
    role_arn = conn["role_arn"]
    external_id = conn["external_id"]
    region = conn["region"]

    logger.info(f"Analyzing {team_id} / account {account_id}")

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

    # Run queries and collect results
    all_results = []
    for query_def in INSIGHTS_QUERIES:
        matching_groups = filter_log_groups(log_groups, query_def.get("log_group_pattern"))
        if not matching_groups:
            continue

        # Run query against matching log groups (batch of 50 max)
        for batch in chunk_list(matching_groups, 50):
            try:
                results = run_insights_query(cw_logs, batch, query_def["query"])
                if results:
                    all_results.append(
                        {
                            "query_name": query_def["name"],
                            "description": query_def["description"],
                            "log_groups": batch,
                            "results": results,
                        }
                    )
            except Exception as e:
                logger.warning(f"Query {query_def['name']} failed: {e}")

    if not all_results:
        logger.info(f"No errors found for {team_id}/{account_id}")
        return 0

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
        if store_recommendation(team_id, account_id, advisory_rec):
            new_recs = 1
        else:
            new_recs = 0
    else:
        new_recs = 0

    # Send to Bedrock for analysis
    recommendations = analyze_with_bedrock(all_results, account_id)

    # Deduplicate and store
    for rec in recommendations:
        if store_recommendation(team_id, account_id, rec):
            new_recs += 1

    logger.info(f"{team_id}/{account_id}: {new_recs} new recommendations")
    return new_recs


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
    return [g for g in log_groups if pattern in g]


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
        return []

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
            return []

    # Timed out waiting — stop the query
    try:
        cw_logs.stop_query(queryId=query_id)
    except Exception:
        pass

    return []


def format_query_results(results):
    """Format Insights query results into readable lines."""
    lines = []
    for row in results[:20]:  # Cap at 20 rows
        fields = {f["field"]: f["value"] for f in row}
        lines.append(fields)
    return lines


def analyze_with_bedrock(query_results, account_id):
    """Send log error summaries to Bedrock Claude for analysis."""
    # Scrub any secrets/tokens from the data before sending to AI
    scrubbed_results = scrub_query_results(query_results)

    # Build context — NO raw log data, only aggregated summaries
    context = json.dumps(scrubbed_results, indent=2, default=str)

    # Truncate if too large (keep under 100K tokens)
    if len(context) > 50000:
        context = context[:50000] + "\n... (truncated)"

    prompt = f"""You are an AWS infrastructure expert analyzing CloudWatch Logs error summaries for AWS account {account_id}.

Below are aggregated error patterns from CloudWatch Logs Insights queries. Each section has a query name, the log groups searched, and the error patterns found.

<error_summaries>
{context}
</error_summaries>

Analyze these errors and create actionable recommendations. For each distinct issue:

1. Identify the root cause
2. Group related errors
3. Suggest a specific fix

Return a JSON array of recommendations. Each recommendation must have:
- "title": Short descriptive title (max 80 chars)
- "severity": "critical" | "warning" | "info"
- "summary": 2-3 sentence explanation of the issue
- "suggested_action": Specific steps to fix the issue
- "source_log_group": The primary log group where this was detected
- "error_pattern": A representative error string for deduplication

Rules:
- Only include actionable issues, not informational noise
- critical = service down, data loss risk, or security issue
- warning = degraded performance, recurring errors, resource constraints
- info = non-urgent improvements, cleanup opportunities
- Maximum 10 recommendations
- Be specific — include function names, error messages, resource names

Return ONLY the JSON array, no surrounding text."""

    try:
        response = bedrock.converse(
            modelId=MODEL_ID,
            messages=[{"role": "user", "content": [{"text": prompt}]}],
            inferenceConfig={"maxTokens": 4096, "temperature": 0.1},
        )

        output_text = response["output"]["message"]["content"][0]["text"]

        # Parse JSON from response (handle markdown code blocks)
        output_text = output_text.strip()
        if output_text.startswith("```"):
            output_text = output_text.split("\n", 1)[1]
            if output_text.endswith("```"):
                output_text = output_text[:-3]

        recommendations = json.loads(output_text)
        if not isinstance(recommendations, list):
            logger.error("Bedrock response was not a JSON array")
            return []

        return recommendations[:10]  # Cap at 10

    except Exception as e:
        logger.error(f"Bedrock analysis failed: {e}", exc_info=True)
        return []


def store_recommendation(team_id, account_id, rec):
    """Deduplicate and store a recommendation. Returns True if new."""
    # Create error hash for dedup: hash(account_id + log_group + error_pattern)
    raw = f"{account_id}:{rec.get('source_log_group', '')}:{rec.get('error_pattern', rec.get('title', ''))}"
    error_hash = hashlib.sha256(raw.encode()).hexdigest()[:16]

    # Check if we already have a non-dismissed rec with this hash
    rec_id = ulid_now()
    sk = f"REC#{rec_id}"
    now = datetime.now(timezone.utc).isoformat()

    # Query existing recs with same hash
    try:
        existing = settings_table.query(
            KeyConditionExpression="pk = :pk AND begins_with(sk, :prefix)",
            FilterExpression="error_hash = :hash AND #s <> :dismissed",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":pk": team_id,
                ":prefix": "REC#",
                ":hash": error_hash,
                ":dismissed": "dismissed",
            },
        )
        if existing.get("Items"):
            logger.info(f"Skipping duplicate recommendation (hash={error_hash})")
            return False
    except Exception as e:
        logger.warning(f"Dedup check failed: {e}")

    # Store new recommendation
    try:
        ttl_epoch = int((datetime.now(timezone.utc) + timedelta(days=7)).timestamp())
        settings_table.put_item(
            Item={
                "pk": team_id,
                "sk": sk,
                "status": "pending",
                "severity": rec.get("severity", "info"),
                "title": rec.get("title", "Untitled")[:200],
                "summary": rec.get("summary", "")[:2000],
                "suggested_action": rec.get("suggested_action", "")[:2000],
                "source_log_group": rec.get("source_log_group", "")[:500],
                "source_account_id": account_id,
                "error_pattern": rec.get("error_pattern", "")[:500],
                "error_hash": error_hash,
                "created_at": now,
                "updated_at": now,
                "ttl": ttl_epoch,
            }
        )
        return True
    except Exception as e:
        logger.error(f"Failed to store recommendation: {e}")
        return False


def update_connection_status(team_id, account_id, status, error_msg=None):
    """Update the status of an AWS connection."""
    try:
        update_expr = "SET #s = :s, updated_at = :t"
        expr_values = {
            ":s": status,
            ":t": datetime.now(timezone.utc).isoformat(),
        }

        if error_msg:
            update_expr += ", last_error = :e"
            expr_values[":e"] = error_msg[:500]

        settings_table.update_item(
            Key={"pk": team_id, "sk": f"AWS_CONN#{account_id}"},
            UpdateExpression=update_expr,
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues=expr_values,
        )
    except Exception as e:
        logger.error(f"Failed to update connection status: {e}")


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
