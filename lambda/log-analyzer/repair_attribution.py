"""
One-time repair: re-check every open log finding against its claimed log group.

Findings stored before attribution came from Insights data could name a log
group the model guessed, with seen_count = 1 and times = the analyzer's run
time. This re-checks each open (pending or approved) finding with the
analyzer's own code: the same team AWS connection path
(handler.scan_aws_connections, handler.logs_client_for), the same literal
(attribution.pattern_literal of the stored error_pattern) and the same
per-group stats query (attribution.attribute, then check_group on one group).

Dry run by default: one JSON line per finding saying what would change, then
a summary. Nothing is written without --apply.

Per finding:
- its group has matching lines: counts and times are set from them
  (error_count and seen_count = lines in the window, first_seen_at never moves
  forward, last_seen_at = the last line, count_basis = events, samples).
- its group has none but others do: it is credited to the group with the most
  lines (source_log_group, error_hash, log_account_id, counts, times) and its
  title and summary are checked against that evidence (attribution.guard_texts).
  A finding whose fix run already started is only flagged
  (attribution_flag = "wrong_group"), never moved: its ticket and PR exist.
- no group has lines: flagged "unverified" (the error stopped, or the text
  never matched); kept as is.
- no usable pattern: flagged "no_pattern".

Run from lambda/log-analyzer with the CoderHelm account's credentials:

    AWS_PROFILE=<coderhelm-prod> AWS_REGION=us-east-1 \\
    AWS_INSIGHTS_TABLE_NAME=coderhelm-prod-aws-insights \\
    SETTINGS_TABLE_NAME=coderhelm-prod-settings \\
    CODERHELM_ACCOUNT_ID=<coderhelm account id> \\
    python3 repair_attribution.py [--hours 72] [--team TEAM#...] [--no-model] [--apply]
"""

import argparse
import json
import sys
from datetime import datetime, timezone

import attribution
import handler

OPEN = ("pending", "approved")


def open_findings(team_id, account_id):
    """Every open finding of one connection (paginated)."""
    items, last_key = [], None
    while True:
        kw = {
            "KeyConditionExpression": "pk = :pk AND begins_with(sk, :p)",
            "FilterExpression": "source_account_id = :a",
            "ExpressionAttributeValues": {":pk": team_id, ":p": "REC#", ":a": account_id},
        }
        if last_key:
            kw["ExclusiveStartKey"] = last_key
        r = handler.aws_insights_table.query(**kw)
        for i in r.get("Items", []):
            if i.get("status", "pending") in OPEN:
                i["rec_id"] = i["sk"][len("REC#"):]
                items.append(i)
        last_key = r.get("LastEvaluatedKey")
        if not last_key:
            return items


def plan(item, stats, check):
    """What to do with one finding. `stats` is its literal's per-group stats
    over the account's log groups; `check(group)` re-runs the one-group check
    (None when the query failed). Returns a dict: action (ok | recredit |
    flag), flag, group, stats, and the lines found per group."""
    claimed = item.get("source_log_group", "")
    groups = {g: s["count"] for g, s in sorted(stats.items(), key=lambda kv: (-kv[1]["count"], kv[0]))}
    out = {"claimed": claimed, "groups": groups}
    failed = False
    own = check(claimed) if claimed else None
    if claimed and own is None:
        failed = True
    elif own and own["count"] > 0:
        return {**out, "action": "ok", "group": claimed, "stats": own}
    best = None
    for g in groups:
        if g == claimed:
            continue
        s = check(g)
        if s is None:
            failed = True
        elif s["count"] > 0:
            best = s
            break
    if best is None:
        return {**out, "action": "flag", "flag": "check_failed" if failed else "unverified"}
    if item.get("fix_ticket_id"):
        return {**out, "action": "flag", "flag": "wrong_group", "group": best["log_group"], "stats": best}
    return {**out, "action": "recredit", "group": best["log_group"], "stats": best}


def write(item, p, cand, now):
    """Apply one plan to the table."""
    key = {"pk": item["pk"], "sk": item["sk"]}
    if p["action"] == "flag":
        handler.aws_insights_table.update_item(
            Key=key,
            UpdateExpression="SET attribution_flag = :f, attribution_checked_at = :t"
            + (", attribution_group = :g" if p.get("group") else ""),
            ConditionExpression="attribute_exists(sk)",
            ExpressionAttributeValues={
                ":f": p["flag"], ":t": now, **({":g": p["group"]} if p.get("group") else {}),
            },
        )
        return
    stats = p["stats"]
    seen, first, last = attribution.merge_seen(
        # Stored counts are runs, not lines: start them over from the lines.
        {"created_at": item.get("created_at"), "first_seen_at": item.get("first_seen_at")}
        if p["action"] == "ok" else None,
        stats, None,
    )
    values = {
        ":c": stats["count"], ":seen": seen,
        ":f": attribution.iso(first) or now, ":l": attribution.iso(last) or now,
        ":basis": "events", ":samples": stats.get("samples", []),
        ":acct": stats.get("account") or item.get("source_account_id", ""),
        ":t": now,
    }
    sets = (
        "error_count = :c, seen_count = :seen, first_seen_at = :f, last_seen_at = :l, "
        "count_basis = :basis, samples = :samples, log_account_id = :acct, "
        "attribution_checked_at = :t, updated_at = :t"
    )
    if p["action"] == "recredit":
        values.update({
            ":g": p["group"],
            ":h": handler.error_hash_for(
                item.get("source_account_id", ""),
                {"source_log_group": p["group"], "error_pattern": item.get("error_pattern", "")},
            ),
            ":title": cand["title"][:200], ":summary": cand["summary"][:2000],
            ":was": item.get("source_log_group", ""),
        })
        sets += (
            ", source_log_group = :g, error_hash = :h, title = :title, summary = :summary,"
            " attribution_moved_from = :was"
        )
    handler.aws_insights_table.update_item(
        Key=key,
        UpdateExpression=f"SET {sets} REMOVE attribution_flag",
        ConditionExpression="attribute_exists(sk)",
        ExpressionAttributeValues=values,
    )


def repair_connection(conn, hours, use_model, apply, emit):
    team_id, account_id = conn["team_id"], conn["account_id"]
    items = open_findings(team_id, account_id)
    if not items:
        return {}
    cw_logs = handler.logs_client_for(conn)
    log_groups = handler.log_groups_for(conn, cw_logs)
    start, end = handler.lookback_window(hours)
    api_key = None
    if use_model:
        api_key = "" if handler.usage.is_bedrock() else handler.get_team_api_key(team_id)
    now = datetime.now(timezone.utc).isoformat()
    tally = {}
    for item in items:
        literal = attribution.pattern_literal(item.get("error_pattern", ""))
        if item.get("source_log_group") == "multiple":
            # The secrets advisory spans groups by design; nothing to re-credit.
            p = {"action": "skip", "claimed": "multiple", "groups": {}}
        elif not literal:
            p = {"action": "flag", "flag": "no_pattern",
                 "claimed": item.get("source_log_group", ""), "groups": {}}
        else:
            stats = attribution.attribute(cw_logs, log_groups, literal, start, end, handler.scrub_secrets)
            if stats is None:
                p = {"action": "flag", "flag": "check_failed",
                     "claimed": item.get("source_log_group", ""), "groups": {}}
            else:
                p = plan(
                    item, stats,
                    lambda g: attribution.check_group(cw_logs, g, literal, start, end, handler.scrub_secrets),
                )
        cand = None
        if p["action"] == "recredit":
            cand = {
                "id": item["rec_id"], "title": item.get("title", ""), "summary": item.get("summary", ""),
                "log_group": p["group"], "literal": literal, "stats": p["stats"],
                "others": [{"log_group": g, "lines": n} for g, n in p["groups"].items() if g != p["group"]],
            }
            if use_model and api_key is not None:
                ask = lambda prompt: handler.ask_model(prompt, api_key, team_id)  # noqa: E731
            else:
                ask = lambda _p: None  # noqa: E731 - evidence-only words
            attribution.guard_texts([cand], log_groups, ask)
        kind = p["action"] if p["action"] != "flag" else f"flag:{p['flag']}"
        tally[kind] = tally.get(kind, 0) + 1
        emit({
            "team": team_id, "account": account_id, "rec_id": item["rec_id"],
            "status": item.get("status"), "title": item.get("title", ""),
            "claimed": p["claimed"], "action": kind, "to": p.get("group"),
            "lines_by_group": p["groups"],
            "lines": p["stats"]["count"] if p.get("stats") else None,
            "first_seen": attribution.iso(p["stats"]["first"]) if p.get("stats") else None,
            "last_seen": attribution.iso(p["stats"]["last"]) if p.get("stats") else None,
            "new_title": cand["title"] if cand else None,
            "applied": apply,
        })
        if apply and p["action"] != "skip":
            write(item, p, cand, now)
    return tally


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true", help="write the changes (default: dry run)")
    ap.add_argument("--hours", type=int, default=72, help="window searched for each finding's lines")
    ap.add_argument("--team", help="only this team id (TEAM#...)")
    ap.add_argument("--no-model", action="store_true", help="moved findings get evidence-only titles")
    args = ap.parse_args(argv)

    emit = lambda row: print(json.dumps(row, default=str), flush=True)  # noqa: E731
    total = {}
    for conn in handler.scan_aws_connections():
        if args.team and conn["team_id"] != args.team:
            continue
        try:
            tally = repair_connection(conn, args.hours, not args.no_model, args.apply, emit)
        except Exception as e:  # noqa: BLE001 - one connection never stops the rest
            print(json.dumps({"team": conn["team_id"], "account": conn["account_id"],
                              "error": f"{type(e).__name__}: {e}"}), file=sys.stderr)
            continue
        for k, v in tally.items():
            total[k] = total.get(k, 0) + v
    print(json.dumps({"summary": total, "applied": args.apply}), flush=True)


if __name__ == "__main__":
    main()
