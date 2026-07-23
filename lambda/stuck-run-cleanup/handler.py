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
# awaiting_ci can legitimately last a while (slow CI); only reap it well past
# the worker's own 24h wall cap, in case no resume ever fired to enforce it.
MAX_AWAITING_CI_HOURS = int(os.environ.get("MAX_AWAITING_CI_HOURS", "26"))

dynamodb = boto3.resource("dynamodb")
runs_table = dynamodb.Table(RUNS_TABLE)


def handler(event, context):
    """Scan for stuck runs and mark them as failed."""
    now = datetime.now(timezone.utc)
    running_cutoff = (now - timedelta(minutes=MAX_RUN_MINUTES)).isoformat()
    awaiting_cutoff = (now - timedelta(hours=MAX_AWAITING_CI_HOURS)).isoformat()

    # Sweep two stuck states:
    #  - "running" past the Lambda timeout (worker died mid-pass)
    #  - "awaiting_ci" past the wall cap (no resume ever fired — missed CI
    #    webhook + lost safety-net resume). The reaper only scanned "running"
    #    before, so awaiting_ci runs sat forever.
    stuck_runs = []
    scan_kwargs = {
        "FilterExpression": "#s = :running OR #s = :awaiting",
        "ExpressionAttributeNames": {"#s": "status"},
        "ExpressionAttributeValues": {
            ":running": "running",
            ":awaiting": "awaiting_ci",
        },
        "ProjectionExpression": "team_id, run_id, created_at, updated_at, title, current_pass, #s",
    }

    while True:
        resp = runs_table.scan(**scan_kwargs)
        for item in resp.get("Items", []):
            updated = item.get("updated_at", item.get("created_at", ""))
            status = item.get("status", "")
            cutoff = running_cutoff if status == "running" else awaiting_cutoff
            if updated and updated < cutoff:
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
        orig_status = run.get("status", "running")

        # A stuck "running" run is a hard failure; a stuck "awaiting_ci" run is
        # finalized as completed (its PR exists — hand it to a human).
        if orig_status == "awaiting_ci":
            new_status = "completed"
            err = "CI did not conclude within 26h — finalized for human review (auto-cleanup)"
        else:
            new_status = "failed"
            err = f"Run timed out during {current_pass} pass (auto-cleanup)"

        logger.warning(
            f"Unsticking run {run_id}: '{title}' stuck in {orig_status}/{current_pass} "
            f"(last updated: {run.get('updated_at', 'N/A')}) -> {new_status}"
        )

        try:
            now_iso = datetime.now(timezone.utc).isoformat()
            runs_table.update_item(
                Key={"team_id": team_id, "run_id": run_id},
                UpdateExpression=(
                    "SET #s = :new, "
                    "status_run_id = :sri, "
                    "error_message = :err, "
                    "updated_at = :now"
                ),
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={
                    ":new": new_status,
                    ":sri": f"{new_status}#{run_id}",
                    ":err": err,
                    ":now": now_iso,
                    ":orig": orig_status,
                },
                # Only update if still in the original stuck state (avoid racing
                # with normal completion).
                ConditionExpression="#s = :orig",
            )
            failed_count += 1
        except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
            logger.info(f"Run {run_id} already resolved (race condition)")
        except Exception as e:
            logger.error(f"Failed to unstick {run_id}: {e}")

    logger.info(f"Unstuck {failed_count}/{len(stuck_runs)} runs")
    return {"stuck_count": len(stuck_runs), "unstuck_count": failed_count}
