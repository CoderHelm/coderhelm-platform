#!/usr/bin/env python3
"""Add what the team counts missed before they were counted once each.

The admin page reads each team's runs and PRs reviewed from its analytics
items (`<YYYY-MM>` and `ALL_TIME`), which `common::team_counts` adds to,
once per marker. Before that:

  * PRs reviewed (`prs_reviewed`) were not counted at all: the old `reviews`
    counter added only when a review's model usage reached the worker, and
    counted answers to questions and reviews that stopped before posting;
  * plugin runs were listed with the team's runs but never added to
    `total_runs`, `completed` or `failed`.

This script adds those from the records, through the same once-only markers
(`COUNTED#<team>` partition) the services write, so it can run any number of
times and never adds twice, and a review or plugin run the services already
counted after the deploy is skipped. Agent runs were always counted when
they started; they are not touched.

What counts as a review is what the weekly email and the analytics page
count: posted on GitHub, and not an answer to a question. Review stats
records (`RSTAT#`) say whether a review was posted; a review record
(`REVIEW#`) with no stats record counts as posted, as it does there.

Dry run by default: prints, per team, what would be added. Nothing is
written unless --apply is given.

Usage:
  python3 scripts/team_counts_backfill.py                    # dry run, prod
  python3 scripts/team_counts_backfill.py --stage dev
  python3 scripts/team_counts_backfill.py --apply            # after the deploy

Run --apply once, after the deploy that counts at write time.
"""

import argparse
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Attr

ALL_TIME = "ALL_TIME"
RUNS, COMPLETED, FAILED, PRS_REVIEWED = "total_runs", "completed", "failed", "prs_reviewed"


# ── The same rules and keys as services/common/src/team_counts.rs ──────────

def counts_as_review(verdict, posted):
    return bool(posted) and verdict != "QUESTION"


def month_of(at):
    try:
        d = datetime.fromisoformat(str(at).replace("Z", "+00:00"))
    except ValueError:
        d = datetime.now(timezone.utc)
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(timezone.utc).strftime("%Y-%m")


def marker_pk(team):
    return f"COUNTED#{team}"


def run_marker(run_id):
    return f"RUN#{run_id}"


def run_end_marker(run_id):
    return f"RUN_END#{run_id}"


def pr_marker(period, repo, pr):
    return f"PR#{period}#{repo.lower()}#{int(pr):06d}"


def stat_sk_from_review_sk(sk):
    """`REVIEW#{repo}#{pr}#{created_at}` to `RSTAT#{created_at}#{repo}#{pr}`."""
    rest = sk[len("REVIEW#"):]
    parts = rest.rsplit("#", 2)
    if len(parts) != 3:
        return None
    repo, pr, created_at = parts
    if not repo or not created_at or not pr.isdigit():
        return None
    return f"RSTAT#{created_at}#{repo}#{int(pr):06d}"


# ── What to add ────────────────────────────────────────────────────────────

def review_counts(items):
    """(team, marker, field, periods) for every PR with a review, per period.

    `items` are settings rows: REVIEW# and RSTAT# records of any team."""
    reviews = {}  # (team, stat sk) -> (repo, pr, verdict, posted, created_at)
    for i in items:
        sk = i.get("sk", "")
        if sk.startswith("RSTAT#"):
            reviews[(i["pk"], sk)] = (
                i.get("repo", ""), int(i.get("pr_number", 0) or 0), i.get("verdict", ""),
                i.get("posted", True), i.get("created_at", ""),
            )
    for i in items:
        sk = i.get("sk", "")
        if not sk.startswith("REVIEW#"):
            continue
        stat = stat_sk_from_review_sk(sk)
        if stat is None or (i["pk"], stat) in reviews:
            continue
        reviews[(i["pk"], stat)] = (
            i.get("repo", ""), int(i.get("pr_number", 0) or 0), i.get("verdict", ""),
            True, i.get("created_at", ""),
        )
    out = set()
    for (team, _), (repo, pr, verdict, posted, at) in reviews.items():
        if not counts_as_review(verdict, posted) or not repo or not pr:
            continue
        for period in (month_of(at), ALL_TIME):
            out.add((team, pr_marker(period, repo, pr), PRS_REVIEWED, (period,)))
    return sorted(out)


def plugin_run_counts(items):
    """(team, marker, field, periods) for every plugin run: started, and
    how it ended once it has (runs table rows)."""
    out = []
    for r in items:
        if r.get("ticket_source") != "plugin":
            continue
        team, run_id = r["team_id"], r["run_id"]
        out.append((team, run_marker(run_id), RUNS, (month_of(r.get("created_at", "")), ALL_TIME)))
        field = {"completed": COMPLETED, "failed": FAILED}.get(r.get("status", ""))
        if field:
            ended = r.get("updated_at") or r.get("created_at", "")
            out.append((team, run_end_marker(run_id), field, (month_of(ended), ALL_TIME)))
    return out


# ── Reading and writing ────────────────────────────────────────────────────

def scan_all(table, **kw):
    out, start = [], None
    while True:
        if start:
            kw["ExclusiveStartKey"] = start
        r = table.scan(**kw)
        out += r.get("Items", [])
        start = r.get("LastEvaluatedKey")
        if not start:
            return out


def counted(client, table, team, marker):
    r = client.get_item(
        TableName=table,
        Key={"team_id": {"S": marker_pk(team)}, "period": {"S": marker}},
        ProjectionExpression="team_id",
        ConsistentRead=True,
    )
    return "Item" in r


def add_once(client, table, team, marker, field, periods):
    """The marker and the ADDs in one transaction: True when added now,
    False when the marker was already there."""
    items = [{
        "Put": {
            "TableName": table,
            "Item": {
                "team_id": {"S": marker_pk(team)},
                "period": {"S": marker},
                "counted_at": {"S": datetime.now(timezone.utc).isoformat()},
                "backfilled": {"BOOL": True},
            },
            "ConditionExpression": "attribute_not_exists(team_id)",
        }
    }]
    for p in periods:
        items.append({
            "Update": {
                "TableName": table,
                "Key": {"team_id": {"S": team}, "period": {"S": p}},
                "UpdateExpression": "ADD #f :one",
                "ExpressionAttributeNames": {"#f": field},
                "ExpressionAttributeValues": {":one": {"N": "1"}},
            }
        })
    try:
        client.transact_write_items(TransactItems=items)
        return True
    except client.exceptions.TransactionCanceledException as e:
        reasons = e.response.get("CancellationReasons") or []
        if reasons and reasons[0].get("Code") == "ConditionalCheckFailed":
            return False
        raise


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--stage", default="prod")
    ap.add_argument("--region", default="us-east-1")
    ap.add_argument("--team", help="only this team id (e.g. TEAM#...)")
    ap.add_argument("--apply", action="store_true", help="add the missing counts (default: print only)")
    a = ap.parse_args()

    ddb = boto3.resource("dynamodb", region_name=a.region)
    client = boto3.client("dynamodb", region_name=a.region)
    p = f"coderhelm-{a.stage}"
    settings, runs, analytics = ddb.Table(f"{p}-settings"), ddb.Table(f"{p}-runs"), f"{p}-analytics"

    review_rows = scan_all(
        settings,
        FilterExpression=Attr("sk").begins_with("REVIEW#") | Attr("sk").begins_with("RSTAT#"),
        ProjectionExpression="pk, sk, repo, pr_number, verdict, posted, created_at",
    )
    run_rows = scan_all(
        runs,
        FilterExpression=Attr("ticket_source").eq("plugin"),
        ProjectionExpression="team_id, run_id, ticket_source, #s, created_at, updated_at",
        ExpressionAttributeNames={"#s": "status"},
    )
    todo = review_counts(review_rows) + plugin_run_counts(run_rows)
    if a.team:
        todo = [t for t in todo if t[0] == a.team]

    added = defaultdict(lambda: defaultdict(int))
    already = defaultdict(int)
    for team, marker, field, periods in todo:
        if a.apply:
            new = add_once(client, analytics, team, marker, field, periods)
        else:
            new = not counted(client, analytics, team, marker)
        if new:
            for period in periods:
                added[team][(period, field)] += 1
        else:
            already[team] += 1

    teams = sorted(set(added) | set(already))
    verb = "added" if a.apply else "would add"
    for team in teams:
        print(f"{team}: {already[team]} already counted")
        for (period, field), n in sorted(added[team].items()):
            print(f"  {verb} {n:>5} {field} to {period}")
    if not teams:
        print("nothing to count")
    if not a.apply:
        print("\n(dry run: nothing written; add --apply to add the counts)")


if __name__ == "__main__":
    main()
