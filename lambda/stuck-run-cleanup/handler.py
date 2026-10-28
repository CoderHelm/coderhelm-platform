"""
Stuck Run Cleanup Lambda

Triggered by EventBridge every 10 minutes. Scans for runs stuck in
"running" status longer than the Lambda timeout (15 min) + buffer.
Marks them as failed so they don't block the dashboard forever.

A failed run always says why (`error_message`) and in which pass
(`failed_pass`), like every other failure (services/common/src/run_fail.rs).
The pass is the last one the run is known to have reached: the record's
`current_pass`, else the last entry of its `pass_history`, else its
checkpoint, else its newest pass trace. A run that reached none of them
stopped before its first pass, and the message says so instead of
"during unknown pass".

Fix rounds (services/common/src/run_round.rs): a CI fix, review fix or
feedback round on an open pull request has its own time budget, recorded on
the run as `round` with a `deadline_at`. The sweep leaves a run alone until
that deadline (plus a grace period) has passed, however long the run itself
has existed. And a stuck run whose pull request is open is never marked
failed: it is completed with `round_incomplete` saying the last round didn't
finish, so the person sees "Pull request open; the last fix round didn't
finish" and can resume it.
"""

import logging
import os
from datetime import datetime, timezone, timedelta

logger = logging.getLogger()
logger.setLevel(logging.INFO)

RUNS_TABLE = os.environ.get("RUNS_TABLE_NAME", "coderhelm-prod-runs")
TRACES_TABLE = os.environ.get("TRACES_TABLE_NAME", "")
CHECKPOINTS_TABLE = os.environ.get("CHECKPOINTS_TABLE_NAME", "")
MAX_RUN_MINUTES = int(os.environ.get("MAX_RUN_MINUTES", "20"))
# awaiting_ci can legitimately last a while (slow CI); only reap it well past
# the worker's own 24h wall cap, in case no resume ever fired to enforce it.
MAX_AWAITING_CI_HOURS = int(os.environ.get("MAX_AWAITING_CI_HOURS", "26"))

# The only statuses the sweep touches. A run a person ended (cancelled or
# closed) is never swept, and neither is any other finished run.
SWEPT_STATUSES = ("running", "awaiting_ci")

# Values of current_pass that name a state, not a pass the run was doing.
NOT_A_PASS = {"", "unknown", "cancelled", "closed", "done", "awaiting_ci"}
BEFORE_FIRST_PASS = "before its first pass"

# Mirrors services/common/src/run_round.rs (a test there reads these lines).
ROUND_FIELD = "round"
INCOMPLETE_FIELD = "round_incomplete"
ROUND_BUDGET_SECONDS = 750
ROUND_SWEEP_GRACE_SECONDS = 300
ROUND_LABELS = {
    "ci_fix": "CI fix round",
    "review_fix": "review fix round",
    "feedback": "feedback round",
    "security_fix": "security fix round",
    "conflict_fix": "conflict round",
}

_dynamodb = None


def _db():
    global _dynamodb
    if _dynamodb is None:
        import boto3

        _dynamodb = boto3.resource("dynamodb")
    return _dynamodb


def last_known_pass(run, checkpoint_pass="", trace_pass=""):
    """The last pass the run is known to have reached, or "" if none."""
    current = (run.get("current_pass") or "").strip()
    if current not in NOT_A_PASS:
        return current
    for entry in reversed(run.get("pass_history") or []):
        p = (entry.get("pass") or "").strip() if isinstance(entry, dict) else ""
        if p not in NOT_A_PASS:
            return p
    for p in (checkpoint_pass, trace_pass):
        p = (p or "").strip()
        # Checkpoints name a pass mid-way as "implement_started".
        p = p.removesuffix("_started")
        if p not in NOT_A_PASS:
            return p
    return ""


def failure_message(pass_name):
    if pass_name:
        return f"Run timed out during {pass_name} pass (auto-cleanup)"
    return (
        "Run stopped before its first pass started (auto-cleanup); "
        "the worker never reported progress"
    )


def _parse_time(value):
    try:
        t = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return t if t.tzinfo else t.replace(tzinfo=timezone.utc)


def round_in_budget(run, now):
    """True while the run's fix round is inside its own budget (plus grace).

    A round's clock starts when the round does, never when the run started:
    a run that is days old can be working a fresh round.
    """
    rnd = run.get(ROUND_FIELD) or {}
    if not isinstance(rnd, dict) or rnd.get("state", "running") != "running":
        return False
    deadline = _parse_time(rnd.get("deadline_at"))
    if deadline is None:
        started = _parse_time(rnd.get("started_at"))
        if started is None:
            return False
        deadline = started + timedelta(seconds=ROUND_BUDGET_SECONDS)
    return now < deadline + timedelta(seconds=ROUND_SWEEP_GRACE_SECONDS)


def has_pull_request(run):
    try:
        number = int(run.get("pr_number") or 0)
    except (TypeError, ValueError):
        number = 0
    return number > 0 or bool((run.get("pr_url") or "").strip())


def round_kind(run, pass_name):
    """The round a stuck run was working: the one it recorded, else the one
    its pass points to (the same mapping as run_round::Kind::of_pass)."""
    rnd = run.get(ROUND_FIELD) or {}
    kind = rnd.get("kind") if isinstance(rnd, dict) else ""
    if kind in ROUND_LABELS:
        return kind
    p = (pass_name or "").split(":")[0]
    if p in ("ci_fix", "test"):
        return "ci_fix"
    if p == "feedback":
        return "feedback"
    if p in ("security_fix", "conflict_fix"):
        return p
    return "review_fix"


def incomplete_reason(kind, pass_name):
    label = ROUND_LABELS.get(kind, "fix round")
    where = f" during the {pass_name} pass" if pass_name else ""
    return f"the {label} stopped{where} without reporting back (time limit)"


def incomplete_record(run, pass_name, now_iso):
    """The `round_incomplete` map for a stuck run whose pull request is open."""
    rnd = run.get(ROUND_FIELD) or {}
    if not isinstance(rnd, dict):
        rnd = {}
    kind = round_kind(run, pass_name)
    return {
        "kind": kind,
        "number": rnd.get("number", 0),
        "reason": incomplete_reason(kind, pass_name),
        "pushed": [],
        "files": [],
        "at": now_iso,
        "feedback": rnd.get("feedback", ""),
        "scoped": rnd.get("scoped", True),
        # The round's work so far stays on its WIP branch, never the PR's.
        "wip_branch": rnd.get("wip_branch", ""),
        "base_sha": rnd.get("base_sha", ""),
    }


def status_line(reason):
    return f"Pull request open; the last fix round didn't finish: {reason}."


def _checkpoint_pass(team_id, run_id):
    if not CHECKPOINTS_TABLE:
        return ""
    try:
        item = (
            _db()
            .Table(CHECKPOINTS_TABLE)
            .get_item(Key={"team_id": team_id, "sk": f"RUN#{run_id}"})
            .get("Item")
            or {}
        )
        return item.get("last_pass", "")
    except Exception as e:  # best effort: the message still names a pass or says none
        logger.warning(f"Could not read checkpoint for {run_id}: {e}")
        return ""


def _trace_pass(team_id, run_id):
    if not TRACES_TABLE:
        return ""
    try:
        from boto3.dynamodb.conditions import Key

        items = (
            _db()
            .Table(TRACES_TABLE)
            .query(
                KeyConditionExpression=Key("team_id").eq(team_id)
                & Key("sk").begins_with(f"RUN#{run_id}#PASS#")
            )
            .get("Items", [])
        )
        items.sort(key=lambda i: i.get("timestamp", ""))
        # "ci_fix:2" -> "ci_fix"
        return items[-1].get("pass", "").split(":")[0] if items else ""
    except Exception as e:
        logger.warning(f"Could not read traces for {run_id}: {e}")
        return ""


def handler(event, context):
    """Scan for stuck runs and mark them as failed."""
    runs_table = _db().Table(RUNS_TABLE)
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
            ":running": SWEPT_STATUSES[0],
            ":awaiting": SWEPT_STATUSES[1],
        },
        "ProjectionExpression": (
            "team_id, run_id, created_at, updated_at, title, current_pass, pass_history, #s, "
            "#round, pr_number, pr_url"
        ),
    }
    scan_kwargs["ExpressionAttributeNames"]["#round"] = ROUND_FIELD

    while True:
        resp = runs_table.scan(**scan_kwargs)
        for item in resp.get("Items", []):
            updated = item.get("updated_at", item.get("created_at", ""))
            status = item.get("status", "")
            cutoff = running_cutoff if status == "running" else awaiting_cutoff
            if status == "running" and round_in_budget(item, now):
                # Its fix round is still inside its own budget.
                continue
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
        orig_status = run.get("status", "running")
        pass_name = last_known_pass(run)
        if not pass_name:
            pass_name = last_known_pass(
                run, _checkpoint_pass(team_id, run_id), _trace_pass(team_id, run_id)
            )

        logger.warning(
            f"Unsticking run {run_id}: '{title}' stuck in {orig_status}/{pass_name or '-'} "
            f"(last updated: {run.get('updated_at', 'N/A')})"
        )

        names = {"#s": "status"}
        values = {":now": datetime.now(timezone.utc).isoformat(), ":orig": orig_status}
        # A stuck "running" run is a hard failure, unless its pull request is
        # open: then a fix round on it stopped, and the run says so instead
        # of failing. A stuck "awaiting_ci" run is finalized as completed (its
        # PR exists — hand it to a human).
        if orig_status == "running" and has_pull_request(run):
            now_iso = datetime.now(timezone.utc).isoformat()
            record = incomplete_record(run, pass_name, now_iso)
            names["#round"] = ROUND_FIELD
            names["#inc"] = INCOMPLETE_FIELD
            update = (
                "SET #s = :new, status_run_id = :sri, #inc = :inc, current_pass = :cp, "
                "updated_at = :now REMOVE #round"
            )
            values.update(
                {
                    ":new": "completed",
                    ":sri": f"completed#{run_id}",
                    ":inc": record,
                    ":cp": "done",
                }
            )
            logger.warning(f"Run {run_id}: {status_line(record['reason'])}")
        elif orig_status == "awaiting_ci":
            update = "SET #s = :new, status_run_id = :sri, error_message = :err, updated_at = :now"
            values.update(
                {
                    ":new": "completed",
                    ":sri": f"completed#{run_id}",
                    ":err": "CI did not conclude within 26h — finalized for human review (auto-cleanup)",
                }
            )
        else:
            update = (
                "SET #s = :new, status_run_id = :sri, error_message = :err, "
                "failed_pass = :pass, updated_at = :now"
            )
            values.update(
                {
                    ":new": "failed",
                    ":sri": f"failed#{run_id}",
                    ":err": failure_message(pass_name),
                    ":pass": pass_name or BEFORE_FIRST_PASS,
                }
            )

        try:
            runs_table.update_item(
                Key={"team_id": team_id, "run_id": run_id},
                UpdateExpression=update,
                ExpressionAttributeNames=names,
                ExpressionAttributeValues=values,
                # Only update if still in the original stuck state (avoid racing
                # with normal completion).
                ConditionExpression="#s = :orig",
            )
            failed_count += 1
        except _db().meta.client.exceptions.ConditionalCheckFailedException:
            logger.info(f"Run {run_id} already resolved (race condition)")
        except Exception as e:
            logger.error(f"Failed to unstick {run_id}: {e}")

    logger.info(f"Unstuck {failed_count}/{len(stuck_runs)} runs")
    return {"stuck_count": len(stuck_runs), "unstuck_count": failed_count}
