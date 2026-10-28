"""
One-off merge: fold findings that are the same error into one.

Before findings were identified by the error signature of their lines
(fingerprint.py), the same error got a new finding whenever the model worded
its error pattern differently, or whenever it was resolved after one quiet
run and came back. This groups every stored finding of a team by account,
log group and signature, the same way the analyzer now matches a sighting
(lifecycle.find_match), and proposes one merge per group of duplicates.

Dry run by default: one JSON line per proposed merge (the finding kept, the
ones folded into it, and what the kept one would become), one line per
finding left alone with a reason, then a summary. Nothing is written without
--apply. Nothing is deleted, even with --apply.

Per group of duplicates:
- kept: the finding a person acted on first (a fix run, then a plan or
  acknowledgement, then ignored, then open, then resolved), then the oldest.
  Its status, fix, plan and title are left as they are.
- the kept finding gets: first_seen_at = the earliest, last_seen_at = the
  latest, seen_count = the sum of the counted lines (findings that counted
  lines), every signature hash, reopen_count = how many duplicates there
  were, merged_history = one entry per folded finding (its id, status, title,
  times and lines), and fingerprint/signature when it had none.
- each folded finding gets merged_into = the kept id and merged_at; the
  Logs page and the analyzer leave merged findings out from then on.
- a group with two findings that each have a fix run is only reported
  (`two_fixes`): both tickets and PRs exist, so a person picks.
- a finding whose log group is not one log group (findings stored before
  #301 could hold "a, b, c (+47 more)" or "multiple") is only reported
  (`ambiguous_group`): run repair_attribution.py on it first.

Run from lambda/log-analyzer with the CoderHelm account's credentials. Dry
run (prints the plan, writes nothing):

    AWS_PROFILE=<coderhelm-prod> AWS_REGION=us-east-1 \\
    AWS_INSIGHTS_TABLE_NAME=coderhelm-prod-aws-insights \\
    SETTINGS_TABLE_NAME=coderhelm-prod-settings \\
    CODERHELM_ACCOUNT_ID=<coderhelm account id> \\
    DASHBOARD_URL=<dashboard url> \\
    python3 merge_duplicates.py [--team TEAM#...] > merge-plan.jsonl

Read the plan, then the same command with --apply writes it.
"""

import argparse
import json
import re
import sys
from datetime import datetime, timezone

import attribution
import fingerprint
import lifecycle


# A CloudWatch log group name: letters, digits and . _ - / # only. A list
# ("a, b (+47 more)") or "multiple" is not one.
_GROUP = re.compile(r"^[\w.\-/#]{1,512}$")


def ambiguous(group):
    g = (group or "").strip()
    return g == "multiple" or not _GROUP.match(g)


def _lines(item):
    out = [s.get("message", "") for s in item.get("samples") or [] if isinstance(s, dict)]
    if item.get("error_pattern"):
        out.append(str(item["error_pattern"]))
    return [l for l in out if l]


def same_error(a, b):
    """Whether two stored findings of one log group are the same error, by
    the analyzer's own matching: fingerprint, a shared signature, the same
    legacy hash, or one's error pattern (12+ chars) in the other's lines."""
    for x, y in ((a, b), (b, a)):
        fp = x.get("fingerprint")
        if fp and fp in (y.get("fingerprint"), y.get("error_hash")):
            return True
    if a.get("error_hash") and a.get("error_hash") == b.get("error_hash"):
        return True
    if fingerprint.item_hashes(a) & fingerprint.item_hashes(b):
        return True
    for x, y in ((a, b), (b, a)):
        lit = attribution.pattern_literal(x.get("error_pattern"))
        if lit and len(lit) >= lifecycle.MIN_LITERAL_CHARS and any(lit in l for l in _lines(y)):
            return True
    return False


def clusters(items):
    """Groups of the same error, per account and log group (union-find), in
    a stable order. Merged findings and ambiguous groups are left out."""
    by_scope = {}
    for it in items:
        if it.get("merged_into") or ambiguous(it.get("source_log_group")):
            continue
        key = (it.get("source_account_id", ""), it.get("source_log_group", ""))
        by_scope.setdefault(key, []).append(it)
    out = []
    for key in sorted(by_scope):
        group = sorted(by_scope[key], key=lambda i: (i.get("created_at") or "", i["rec_id"]))
        parent = list(range(len(group)))

        def find(i):
            while parent[i] != i:
                parent[i] = parent[parent[i]]
                i = parent[i]
            return i

        for i in range(len(group)):
            for j in range(i + 1, len(group)):
                if same_error(group[i], group[j]):
                    parent[find(j)] = find(i)
        sets = {}
        for i, it in enumerate(group):
            sets.setdefault(find(i), []).append(it)
        out.extend(s for _, s in sorted(sets.items()) if len(s) > 1)
    return out


def _min_ts(values):
    ts = [t for t in (attribution.parse_ts(v) for v in values) if t]
    return min(ts) if ts else None


def _max_ts(values):
    ts = [t for t in (attribution.parse_ts(v) for v in values) if t]
    return max(ts) if ts else None


def plan_merge(members):
    """The merge for one group of duplicates: {action, keep, fold, set}."""
    members = sorted(members, key=lifecycle._order)
    keep, fold = members[0], members[1:]
    fixes = [m for m in members if m.get("fix_ticket_id")]
    if len(fixes) > 1:
        return {"action": "two_fixes", "keep": keep, "fold": [], "set": {},
                "fix_tickets": [m["fix_ticket_id"] for m in fixes]}
    counted = [m for m in members if m.get("count_basis") == "events"]
    seen = sum(attribution.as_int(m.get("seen_count")) for m in counted) if counted else None
    hashes = []
    for m in members:
        hashes = fingerprint.merge_hashes(hashes, sorted(fingerprint.item_hashes(m)))
    first = _min_ts([m.get("first_seen_at") or m.get("created_at") for m in members])
    last = _max_ts([m.get("last_seen_at") or m.get("updated_at") for m in members])
    history = [
        {
            "rec_id": m["rec_id"],
            "status": m.get("status", "pending"),
            "title": str(m.get("title", ""))[:200],
            "created_at": m.get("created_at", ""),
            "first_seen_at": m.get("first_seen_at", ""),
            "last_seen_at": m.get("last_seen_at", ""),
            "resolved_at": m.get("resolved_at", ""),
            "lines": attribution.as_int(m.get("seen_count")) if m.get("count_basis") == "events" else None,
        }
        for m in fold
    ]
    sets = {
        "signature_hashes": hashes,
        "reopen_count": attribution.as_int(keep.get("reopen_count")) + len(fold),
        "merged_history": (keep.get("merged_history") or []) + history,
    }
    if first:
        sets["first_seen_at"] = attribution.iso(first)
    if last:
        sets["last_seen_at"] = attribution.iso(last)
    if seen is not None and keep.get("count_basis") == "events":
        sets["seen_count"] = seen
    if not keep.get("fingerprint"):
        sigs = fingerprint.signatures(_lines(keep))
        if sigs:
            sets["signature"] = sigs[0][:300]
            sets["fingerprint"] = fingerprint.fingerprint(
                keep.get("source_account_id", ""), keep.get("source_log_group", ""), sigs[0]
            )
    return {"action": "merge", "keep": keep, "fold": fold, "set": sets}


def team_findings(table, team_id):
    items, last_key = [], None
    while True:
        kw = {
            "KeyConditionExpression": "pk = :pk AND begins_with(sk, :p)",
            "ExpressionAttributeValues": {":pk": team_id, ":p": "REC#"},
        }
        if last_key:
            kw["ExclusiveStartKey"] = last_key
        r = table.query(**kw)
        for i in r.get("Items", []):
            i["rec_id"] = i["sk"][len("REC#"):]
            items.append(i)
        last_key = r.get("LastEvaluatedKey")
        if not last_key:
            return items


def apply_merge(table, team_id, p, now):
    """Write one merge. The kept finding first, then each folded one, each
    only while it still exists."""
    keep = p["keep"]
    names, values, parts = {}, {":t": now}, ["updated_at = :t"]
    for i, (k, v) in enumerate(sorted(p["set"].items())):
        names[f"#a{i}"] = k
        values[f":v{i}"] = v
        parts.append(f"#a{i} = :v{i}")
    table.update_item(
        Key={"pk": team_id, "sk": keep["sk"]},
        UpdateExpression="SET " + ", ".join(parts),
        ConditionExpression="attribute_exists(sk)",
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=values,
    )
    for m in p["fold"]:
        table.update_item(
            Key={"pk": team_id, "sk": m["sk"]},
            UpdateExpression="SET merged_into = :k, merged_at = :t",
            ConditionExpression="attribute_exists(sk) AND attribute_not_exists(merged_into)",
            ExpressionAttributeValues={":k": keep["rec_id"], ":t": now},
        )


def run_team(table, team_id, apply, emit, now=None):
    now = now or datetime.now(timezone.utc).isoformat()
    items = team_findings(table, team_id)
    tally = {"findings": len(items)}
    for it in items:
        if not it.get("merged_into") and ambiguous(it.get("source_log_group")):
            tally["ambiguous_group"] = tally.get("ambiguous_group", 0) + 1
            emit({"team": team_id, "action": "ambiguous_group", "rec_id": it["rec_id"],
                  "log_group": it.get("source_log_group", ""), "title": it.get("title", ""),
                  "note": "run repair_attribution.py on it first"})
    for members in clusters(items):
        p = plan_merge(members)
        tally[p["action"]] = tally.get(p["action"], 0) + 1
        if p["action"] == "merge":
            tally["folded"] = tally.get("folded", 0) + len(p["fold"])
        emit({
            "team": team_id,
            "action": p["action"],
            "account": p["keep"].get("source_account_id", ""),
            "log_group": p["keep"].get("source_log_group", ""),
            "keep": {"rec_id": p["keep"]["rec_id"], "status": p["keep"].get("status", "pending"),
                     "title": p["keep"].get("title", "")},
            "fold": [{"rec_id": m["rec_id"], "status": m.get("status", "pending"),
                      "title": m.get("title", "")} for m in p["fold"]],
            "set": {k: v for k, v in p["set"].items() if k not in ("signature_hashes", "merged_history")},
            "fix_tickets": p.get("fix_tickets"),
            "applied": apply and p["action"] == "merge",
        })
        if apply and p["action"] == "merge":
            apply_merge(table, team_id, p, now)
    return tally


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true", help="write the merges (default: dry run)")
    ap.add_argument("--team", help="only this team id (TEAM#...)")
    args = ap.parse_args(argv)

    import handler  # AWS handles are made at import: only when run

    emit = lambda row: print(json.dumps(row, default=str), flush=True)  # noqa: E731
    teams = sorted({c["team_id"] for c in handler.scan_aws_connections()})
    if args.team:
        teams = [args.team]
    total = {}
    for team in teams:
        try:
            tally = run_team(handler.aws_insights_table, team, args.apply, emit)
        except Exception as e:  # noqa: BLE001 - one team never stops the rest
            print(json.dumps({"team": team, "error": f"{type(e).__name__}: {e}"}), file=sys.stderr)
            continue
        for k, v in tally.items():
            total[k] = total.get(k, 0) + v
    print(json.dumps({"summary": total, "applied": args.apply}), flush=True)


if __name__ == "__main__":
    main()
