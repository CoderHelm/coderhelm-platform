#!/usr/bin/env python3
"""
List the follow-up tickets CoderHelm filed for one source ticket, group the
likely duplicates and propose which to keep. READ-ONLY: it reads CoderHelm's
own records and never writes anywhere, and it never calls Jira. The product
owner decides what to close.

Where it reads (DynamoDB, Query and GetItem only):
- the settings table: `FOLLOWUP#<SOURCE>#<draft id>` records (key, url,
  filed_at, and on newer records summary, marks, covers, run_id), and
  `CONVERSATION#JIRA#<SOURCE>` (the follow-ups comment CoderHelm posted);
- the runs table: the given source runs (their "Filed N of N follow-up
  tickets: KEYS" notes say which run filed which keys), and, through the
  ticket index, a run on each follow-up ticket (its title is the summary
  when no record kept one).

Grouping uses the same topic test the worker uses before filing
(`common::followup_tickets::topic`): normalized title words, and the key
files and components a title names. Within a group the ticket filed first
is proposed to keep; each later one is proposed to close as its duplicate,
with the files or components it adds (to move onto the kept ticket first).

Usage:
  AWS_PROFILE=... python3 scripts/followup_dupes.py \
      --team 'TEAM#...' --source ABC-123 --runs RUN1 RUN2 [--stage prod] [--json]
"""

import argparse
import json
import re
import sys

import boto3

STOP_WORDS = set(
    "a an the and or to of on in for with when is are be it its as at by from that this these "
    "those into not no any all only via per so then than instead where which".split()
)


def mixed_case(t):
    return any(a.islower() and b.isupper() for a, b in zip(t, t[1:]))


def file_stem(t):
    t = t.split(":")[0]
    base = t.rsplit("/", 1)[-1]
    if "." not in base:
        return None
    stem, ext = base.rsplit(".", 1)
    if 1 <= len(ext) <= 5 and ext.isalpha() and stem and any(c.isalpha() for c in stem):
        return stem.lower()
    return None


def marks(text):
    out = set()
    for t in text.split("`")[1::2]:
        t = t.strip()
        if not t or " " in t:
            continue
        s = file_stem(t)
        if s:
            out.add(s)
        elif re.fullmatch(r"[A-Za-z0-9_-]{3,}", t) and (mixed_case(t) or "_" in t or "-" in t):
            out.add(t.lower())
    for raw in re.split(r"[\s,;()\[\]{}\"'`]+", text):
        t = raw.strip(".,;:!?")
        s = file_stem(t)
        core = re.sub(r"^[^A-Za-z0-9]+|[^A-Za-z0-9]+$", "", raw)
        if s:
            out.add(s)
        elif core and mixed_case(core) and re.fullmatch(r"[A-Za-z0-9_]+", core):
            out.add(core.lower())
    return out


def topic_words(text):
    skip = marks(text)
    out = set()
    for w in re.split(r"[^A-Za-z0-9]+", text):
        w = w.lower()
        if not w or w in skip or w in STOP_WORDS or w.isdigit():
            continue
        if len(w) > 3 and w.endswith("s") and not w.endswith("ss"):
            w = w[:-1]
        out.add(w)
    return out


def normal(s):
    return " ".join(s.split()).lower().rstrip(". ")


def topic(a, b):
    """'same', 'maybe' or 'new', as the worker decides before filing."""
    if normal(a["summary"]) == normal(b["summary"]):
        return "same", 1.0
    wa, wb = topic_words(a["summary"]), topic_words(b["summary"])
    union = wa | wb
    words = len(wa & wb) / len(union) if union else 0.0
    ma = marks(a["summary"]) | set(a.get("marks", []))
    mb = marks(b["summary"]) | set(b.get("marks", []))
    small = min(len(ma), len(mb))
    files = len(ma & mb) / small if small else 0.0
    if words >= 0.5 or (files >= 0.5 and words >= 0.15):
        return "same", words + files
    if words >= 0.25 or files > 0:
        return "maybe", words + files
    return "new", words + files


def s(item, k):
    v = item.get(k, {})
    return v.get("S") or v.get("N") or ""


def query_all(client, **kw):
    while True:
        page = client.query(**kw)
        yield from page.get("Items", [])
        if "LastEvaluatedKey" not in page:
            return
        kw["ExclusiveStartKey"] = page["LastEvaluatedKey"]


def comment_titles(text):
    out = {}
    for line in text.splitlines():
        m = re.match(r"^([A-Z][A-Z0-9_]*-\d+): (.+)$", line.strip())
        if not m or m.group(2).startswith("covered by "):
            continue
        title = re.sub(r" \[.*$", "", m.group(2))
        title = re.sub(r" \(https?://[^)]*\)$", "", title)
        out[m.group(1)] = title.strip()
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--team", required=True, help="the team id (TEAM#...)")
    ap.add_argument("--source", required=True, help="the source ticket key")
    ap.add_argument("--runs", nargs="*", default=[], help="the source runs that filed follow-ups")
    ap.add_argument("--stage", default="prod")
    ap.add_argument("--region", default="us-east-1")
    ap.add_argument("--json", action="store_true", help="print the result as JSON")
    a = ap.parse_args()

    ddb = boto3.client("dynamodb", region_name=a.region)
    settings = f"coderhelm-{a.stage}-settings"
    runs_table = f"coderhelm-{a.stage}-runs"
    source = a.source.strip().upper()

    tickets = {}
    for it in query_all(
        ddb,
        TableName=settings,
        KeyConditionExpression="pk = :p AND begins_with(sk, :s)",
        ExpressionAttributeValues={":p": {"S": a.team}, ":s": {"S": f"FOLLOWUP#{source}#"}},
    ):
        key = s(it, "key")
        if not key or s(it, "covers") == "1":
            continue
        t = tickets.setdefault(key, {"key": key, "url": s(it, "url"), "filed_at": s(it, "filed_at")})
        t["filed_at"] = min(t["filed_at"] or s(it, "filed_at"), s(it, "filed_at"))
        if s(it, "summary"):
            t["summary"] = s(it, "summary")
        if s(it, "marks"):
            t["marks"] = [m for m in s(it, "marks").split(",") if m]
        if s(it, "run_id"):
            t["run_id"] = s(it, "run_id")

    # Titles from the follow-ups comment CoderHelm posted on the source.
    conv = ddb.get_item(
        TableName=settings,
        Key={"pk": {"S": a.team}, "sk": {"S": f"CONVERSATION#JIRA#{source}"}},
    ).get("Item")
    if conv:
        for said in json.loads(s(conv, "doc") or "[]"):
            meta = said.get("meta") or {}
            bodies = [said.get("body", "")] if meta.get("slot") == "followups" else []
            bodies += [p.get("body", "") for p in meta.get("parts", []) if p.get("slot") == "followups"]
            for b in bodies:
                for k, title in comment_titles(b).items():
                    if k in tickets and not tickets[k].get("summary"):
                        tickets[k]["summary"] = title

    # Which source run filed which keys, from its notes.
    for run_id in a.runs:
        item = ddb.get_item(TableName=runs_table, Key={"team_id": {"S": a.team}, "run_id": {"S": run_id}}).get("Item")
        if not item:
            continue
        notes = item.get("progress_notes", {}).get("L", [])
        plan = 0
        for n in notes:
            msg = n.get("M", {}).get("message", {}).get("S", "")
            if msg == "Plan generated":
                plan += 1
            m = re.match(r"^Filed \d+ of \d+ follow-up tickets: (.+)$", msg)
            if m:
                for k in re.findall(r"[A-Z][A-Z0-9_]*-\d+", m.group(1)):
                    if k in tickets:
                        tickets[k].setdefault("run_id", run_id)
                        tickets[k].setdefault("plan", plan)

    # A title from a run on the follow-up ticket itself, when nothing else had one.
    for k, t in tickets.items():
        if t.get("summary"):
            continue
        for it in query_all(
            ddb,
            TableName=runs_table,
            IndexName="ticket-index",
            KeyConditionExpression="team_id = :t AND ticket_id = :k",
            ExpressionAttributeValues={":t": {"S": a.team}, ":k": {"S": k}},
            ProjectionExpression="title",
        ):
            if s(it, "title"):
                t["summary"] = s(it, "title")
                break

    ordered = sorted(tickets.values(), key=lambda t: (t.get("filed_at", ""), t["key"]))
    for t in ordered:
        t.setdefault("summary", "")
        t.setdefault("marks", [])

    # Greedy grouping, earliest first: a ticket joins the first kept ticket
    # whose topic is the same; a near one is flagged for a person to compare.
    groups = []
    for t in ordered:
        best = None
        for g in groups:
            verdict, score = topic(t, g["keep"])
            if verdict == "same" and (best is None or best[0] != "same" or score > best[1]):
                best = ("same", score, g)
            elif verdict == "maybe" and best is None:
                best = ("maybe", score, g)
        if best and best[0] == "same":
            adds = sorted((marks(t["summary"]) | set(t["marks"])) - (marks(best[2]["keep"]["summary"]) | set(best[2]["keep"]["marks"])))
            best[2]["duplicates"].append({**t, "adds": adds})
        else:
            g = {"keep": t, "duplicates": [], "near": []}
            if best and best[0] == "maybe":
                best[2]["near"].append(t["key"])
                g["near_of"] = best[2]["keep"]["key"]
            groups.append(g)

    result = {
        "source": source,
        "tickets": [{k: t.get(k) for k in ("key", "summary", "filed_at", "run_id", "plan", "url")} for t in ordered],
        "groups": [
            {
                "keep": g["keep"]["key"],
                "keep_summary": g["keep"]["summary"],
                "close_as_duplicate": [{"key": d["key"], "summary": d["summary"], "adds": d["adds"]} for d in g["duplicates"]],
                "near": g["near"],
            }
            for g in groups
        ],
        "proposed_keep": [g["keep"]["key"] for g in groups],
        "proposed_close": [d["key"] for g in groups for d in g["duplicates"]],
        "note": "Read-only proposal. Nothing was changed in Jira; the product owner decides.",
    }
    if a.json:
        json.dump(result, sys.stdout, indent=2)
        print()
        return

    print(f"Follow-ups filed for {source}: {len(ordered)}\n")
    for t in ordered:
        who = t.get("run_id", "?")
        plan = f" plan {t['plan']}" if t.get("plan") else ""
        print(f"  {t['key']:<10} {t.get('filed_at', '')[:19]}  {who}{plan}\n             {t['summary'] or '(no title on record)'}")
    print("\nGroups (keep the first filed; close the rest as its duplicates):\n")
    for g in result["groups"]:
        print(f"  KEEP  {g['keep']}: {g['keep_summary']}")
        for d in g["close_as_duplicate"]:
            extra = f"  (move first: {', '.join(d['adds'])})" if d["adds"] else ""
            print(f"  CLOSE {d['key']}: {d['summary']}{extra}")
        if g["near"]:
            print(f"        near, compare by hand: {', '.join(g['near'])}")
        print()
    print("Proposed keep:  " + ", ".join(result["proposed_keep"]))
    print("Proposed close: " + (", ".join(result["proposed_close"]) or "none"))
    print("\n" + result["note"])


if __name__ == "__main__":
    main()
