"""
Stuck Run Cleanup Lambda

Triggered by EventBridge every 10 minutes. Scans for runs stuck in
"running" status longer than the Lambda timeout (15 min) + buffer.
Marks them as failed so they don't block the dashboard forever.
"""

import json
import logging
import os
from datetime import datetime, timezone, timedelta

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

RUNS_TABLE = os.environ.get("RUNS_TABLE_NAME", "coderhelm-prod-runs")
MAX_RUN_MINUTES = int(os.environ.get("MAX_RUN_MINUTES", "20"))

dynamodb = boto3.resource("dynamodb")
runs_table = dynamodb.Table(RUNS_TABLE)


def handler(event, context):
    """Scan for stuck runs and mark them as failed."""
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=MAX_RUN_MINUTES)
    cutoff_iso = cutoff.isoformat()

    # Scan for all runs with status=running
    stuck_runs = []
    scan_kwargs = {
        "FilterExpression": "#s = :running",
        "ExpressionAttributeNames": {"#s": "status"},
        "ExpressionAttributeValues": {":running": "running"},
        "ProjectionExpression": "team_id, run_id, created_at, updated_at, title, current_pass",
    }

    while True:
        resp = runs_table.scan(**scan_kwargs)
        for item in resp.get("Items", []):
            updated = item.get("updated_at", item.get("created_at", ""))
            if updated and updated < cutoff_iso:
                stuck_runs.append(item)

        if "LastEvaluatedKey" not in resp:
            break
        scan_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

    if not stuck_runs:
        logger.info("No stuck runs found")
        return {"stuck_count": 0}

    logger.info(f"Found {len(stuck_runs)} stuck runs")

    failed_count = 0
    for run in stuck_runs:
        team_id = run["team_id"]
        run_id = run["run_id"]
        title = run.get("title", "unknown")
        current_pass = run.get("current_pass", "unknown")

        logger.warning(
            f"Unsticking run {run_id}: '{title}' stuck in {current_pass} "
            f"(last updated: {run.get('updated_at', 'N/A')})"
        )

        try:
            now_iso = datetime.now(timezone.utc).isoformat()
            runs_table.update_item(
                Key={"team_id": team_id, "run_id": run_id},
                UpdateExpression=(
                    "SET #s = :failed, "
                    "status_run_id = :sri, "
                    "error_message = :err, "
                    "updated_at = :now"
                ),
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={
                    ":failed": "failed",
                    ":sri": f"failed#{run_id}",
                    ":err": f"Run timed out during {current_pass} pass (auto-cleanup)",
                    ":now": now_iso,
                },
                # Only update if still running (avoid racing with normal completion)
                ConditionExpression="#s = :running",
            )
            failed_count += 1
        except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
            logger.info(f"Run {run_id} already resolved (race condition)")
        except Exception as e:
            logger.error(f"Failed to unstick {run_id}: {e}")

    logger.info(f"Unstuck {failed_count}/{len(stuck_runs)} runs")
    return {"stuck_count": len(stuck_runs), "unstuck_count": failed_count}
