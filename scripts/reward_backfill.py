#!/usr/bin/env python3
"""Read-only: what reward events past pull requests would produce.

Reads one team's runs from the runs table (or a JSON file of run items with
--runs-file) and prints, per run, the events the reward system records when
a CoderHelm pull request closes (see services/common/src/rewards/mapping.rs):
merged as written, merged after edits (unknown here: needs GitHub), closed
without merging, time to merge, and CI's first try or fix rounds.

It never writes. It has no write call at all; the summary says what a real
backfill would add. Run it against a test account, never prod:

    AWS_PROFILE=dev python3 scripts/reward_backfill.py --table coderhelm-dev-runs --team TEAM#x
    python3 scripts/reward_backfill.py --runs-file runs.json --team TEAM#x
"""

import argparse
import collections
import datetime as dt
import json
import sys

# Mirrors default_weight / shape in services/common/src/rewards/score.rs.
WEIGHTS = {
    "pr_merged_unchanged": 1.5,
    "pr_merged_edited": 1.0,
    "pr_closed_unmerged": -1.0,
    "ci_first_try": 0.5,
    "ci_fix_rounds": -0.75,
    "time_to_merge": 0.5,
}


def _s(item, k):
    v = item.get(k) or {}
    return v.get("S", "") if isinstance(v, dict) else str(v)


def _n(item, k):
    v = item.get(k) or {}
    try:
        return float(v.get("N", 0)) if isinstance(v, dict) else float(v)
    except (TypeError, ValueError):
        return 0.0


def _hours(a, b):
    try:
        t0 = dt.datetime.fromisoformat(a.replace("Z", "+00:00"))
        t1 = dt.datetime.fromisoformat(b.replace("Z", "+00:00"))
    except ValueError:
        return None
    h = (t1 - t0).total_seconds() / 3600
    return h if h >= 0 else None


def shape(kind, value):
    if kind == "pr_merged_edited":
        return min(max(1 - value, 0), 1)
    if kind == "ci_fix_rounds":
        return min(max(value / 4, 0), 1)
    if kind == "time_to_merge":
        return min(max(1 - value / 72, 0), 1)
    return 1.0


def events_for(run):
    """The events one run record implies, as (kind, value)."""
    out = []
    outcome = _s(run, "pr_outcome")
    if outcome == "merged":
        if "human_commits" in run:
            if _n(run, "human_commits") == 0:
                out.append(("pr_merged_unchanged", 0.0))
            # Edited merges need the diff from GitHub: counted as unknown.
        h = _hours(_s(run, "pr_opened_at"), _s(run, "merged_at"))
        if h is not None:
            out.append(("time_to_merge", h))
    elif outcome == "closed":
        out.append(("pr_closed_unmerged", 0.0))
    else:
        return out
    last = _s(run, "ci_last")
    attempts = _n(run, "ci_fix_attempts")
    if last or "ci_fix_attempts" in run:
        if attempts == 0 and last == "passed":
            out.append(("ci_first_try", 0.0))
        elif attempts > 0:
            out.append(("ci_fix_rounds", attempts))
    return out


def load_runs(args):
    if args.runs_file:
        with open(args.runs_file) as f:
            return json.load(f)
    import boto3  # only for the live read

    client = boto3.client("dynamodb")
    items, start = [], None
    while True:
        kw = {
            "TableName": args.table,
            "KeyConditionExpression": "team_id = :t",
            "ExpressionAttributeValues": {":t": {"S": args.team}},
        }
        if start:
            kw["ExclusiveStartKey"] = start
        page = client.query(**kw)
        items.extend(page.get("Items", []))
        start = page.get("LastEvaluatedKey")
        if not start:
            return items


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--team", required=True)
    p.add_argument("--table")
    p.add_argument("--runs-file")
    args = p.parse_args(argv)
    if not args.table and not args.runs_file:
        p.error("give --table or --runs-file")
    if args.table and "prod" in args.table:
        p.error("refusing to read a prod table; use a test account")
    runs = load_runs(args)
    counts = collections.Counter()
    total, n = 0.0, 0
    unknown_edits = 0
    for run in runs:
        if _s(run, "pr_outcome") == "merged" and _n(run, "human_commits") > 0:
            unknown_edits += 1
        for kind, value in events_for(run):
            counts[kind] += 1
            total += WEIGHTS[kind] * shape(kind, value)
            n += 1
    print(f"runs read: {len(runs)} (nothing written)")
    for kind, c in sorted(counts.items()):
        print(f"  would record {c:5d} x {kind}")
    print(f"  merged after edits, needs GitHub to measure: {unknown_edits}")
    print(f"mean reward per event: {total / n:.3f}" if n else "no events")
    return 0


if __name__ == "__main__":
    sys.exit(main())
