#!/usr/bin/env python3
"""Export a team's past reviews as replay cases for the reviewer route eval.

Usage:
  AWS_PROFILE=<profile> python3 scripts/export_real_review_cases.py \
      --team 'TEAM#...' --since 2026-11-01 --out /path/outside/the/repo [--limit 60]

For each review CoderHelm posted since --since, it reads the REVIEW# record
(the findings, where they were anchored, whether they held the merge) from
the settings table, and from GitHub (the `gh` CLI, the operator's own login)
the pull request's diff at the reviewed head, the changed files at that head,
the repository's instruction files and the review threads CoderHelm opened.
Each finding whose fate is clear becomes the answer:

  accepted   the thread went outdated (the code under it changed after the
             comment) or a person replied agreeing;
  dismissed  the thread was resolved, or answered by a person, with the code
             under it unchanged, and the pull request was merged or closed.

Findings with no thread or an open, unanswered one are left out. A review
with no finding of known fate is skipped.

The cases hold the team's code: they are written only to --out, which must
be outside this repository, and are never committed. Run the eval with
`EVAL_REAL_CASES=<out> cargo test -p worker route_review_real_live -- --ignored`
once per reviewer model (EVAL_REVIEW_MODEL) and compare the summaries.
"""

import argparse
import json
import os
import re
import subprocess
import sys

import boto3

INSTRUCTION_FILES = ["AGENTS.md", "CLAUDE.md", ".github/copilot-instructions.md", "CODERHELM.md"]
AGREE = re.compile(r"\b(good catch|fixed|done|agreed|addressed|will fix|updated)\b", re.I)
NEAR = 6
MAX_FILE_BYTES = 200_000


def gh(args, ok_missing=False):
    r = subprocess.run(["gh", "api", *args], capture_output=True, text=True)
    if r.returncode != 0:
        if ok_missing:
            return None
        raise SystemExit(f"gh api {' '.join(args)}: {r.stderr.strip()}")
    return json.loads(r.stdout) if r.stdout.strip() else None


def threads(repo, pr):
    owner, name = repo.split("/", 1)
    q = """query($o:String!,$n:String!,$p:Int!,$c:String){repository(owner:$o,name:$n){
      pullRequest(number:$p){state merged reviewThreads(first:100,after:$c){
        pageInfo{hasNextPage endCursor}
        nodes{isResolved isOutdated path line originalLine
          comments(first:20){nodes{author{login} body createdAt
            reactions(first:20){nodes{content}}}}}}}}}"""
    out, cursor, state = [], None, None
    while True:
        args = ["graphql", "-f", f"query={q}", "-F", f"o={owner}", "-F", f"n={name}", "-F", f"p={pr}"]
        if cursor:
            args += ["-F", f"c={cursor}"]
        d = gh(args)["data"]["repository"]["pullRequest"]
        state = "MERGED" if d["merged"] else d["state"]
        page = d["reviewThreads"]
        out += page["nodes"]
        if not page["pageInfo"]["hasNextPage"]:
            return out, state
        cursor = page["pageInfo"]["endCursor"]


def is_bot(login):
    return login.endswith("[bot]") or "coderhelm" in login.lower()


def fate(thread, pr_state):
    """accepted, dismissed or None for a CoderHelm thread."""
    comments = thread["comments"]["nodes"]
    people = [c for c in comments[1:] if not is_bot((c.get("author") or {}).get("login", ""))]
    reactions = [r["content"] for r in comments[0].get("reactions", {}).get("nodes", [])] if comments else []
    if thread["isOutdated"] or any(AGREE.search(c["body"]) for c in people) or "THUMBS_UP" in reactions:
        return "accepted"
    settled = pr_state in ("MERGED", "CLOSED")
    if settled and (thread["isResolved"] or people or "THUMBS_DOWN" in reactions):
        return "dismissed"
    return None


def match(finding, thread_list):
    for t in thread_list:
        line = t.get("originalLine") or t.get("line")
        if t["path"] == finding["file"] and line and abs(int(line) - int(finding["line"])) <= NEAR:
            return t
    return None


def case_for(repo, pr, record, findings, thread_list, pr_state):
    head = record["head_sha"]
    accepted, dismissed = [], []
    for f in findings:
        if not f.get("file") or not f.get("line"):
            continue
        t = match(f, thread_list)
        if not t:
            continue
        k = {"file": f["file"], "line": int(f["line"]),
             "blocking": f.get("tier") == "blocking" or f.get("severity") == "blocking" or bool(f.get("blocks"))}
        v = fate(t, pr_state)
        if v == "accepted":
            accepted.append(k)
        elif v == "dismissed":
            dismissed.append(k)
    if not accepted and not dismissed:
        return None
    pull = gh(["repos/" + repo + "/pulls/" + str(pr)])
    compare = gh([f"repos/{repo}/compare/{pull['base']['ref']}...{head}"], ok_missing=True)
    if not compare:
        return None
    diff, files, changed = "", {}, 0
    for f in compare.get("files", []):
        path = f["filename"]
        changed += f.get("additions", 0) + f.get("deletions", 0)
        diff += f"\n### {path} ({f['status']}, +{f.get('additions', 0)}/-{f.get('deletions', 0)})\n{f.get('patch', '(no textual diff)')}\n"
        if f["status"] != "removed":
            body = raw(repo, path, head)
            if body is not None:
                files[path] = body
    instructions = ""
    for p in INSTRUCTION_FILES:
        body = raw(repo, p, head)
        if body:
            instructions += f"\n\n# {p}\n{body}"
    return {
        "name": f"{repo.split('/')[1]}-{pr}-{head[:8]}",
        "title": record.get("pr_title", ""),
        "body": pull.get("body") or "",
        "diff": diff,
        "files": files,
        "instructions": instructions.strip(),
        "accepted": accepted,
        "dismissed": dismissed,
        "lines_changed": changed,
    }


def raw(repo, path, ref):
    r = subprocess.run(
        ["gh", "api", "-H", "Accept: application/vnd.github.raw", f"repos/{repo}/contents/{path}?ref={ref}"],
        capture_output=True, text=True)
    if r.returncode != 0 or len(r.stdout) > MAX_FILE_BYTES:
        return None
    return r.stdout


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--team", required=True)
    ap.add_argument("--since", required=True, help="YYYY-MM-DD")
    ap.add_argument("--out", required=True)
    ap.add_argument("--stage", default="prod")
    ap.add_argument("--limit", type=int, default=60)
    a = ap.parse_args()

    out = os.path.realpath(a.out)
    repo_root = os.path.realpath(os.path.join(os.path.dirname(__file__), ".."))
    if out == repo_root or out.startswith(repo_root + os.sep):
        raise SystemExit("--out must be outside the repository: the cases hold the team's code")
    os.makedirs(out, exist_ok=True)

    table = boto3.resource("dynamodb").Table(f"coderhelm-{a.stage}-settings")
    items, kwargs = [], {}
    while True:
        r = table.query(
            KeyConditionExpression="pk = :p AND begins_with(sk, :s)",
            ExpressionAttributeValues={":p": a.team, ":s": "REVIEW#"},
            **kwargs,
        )
        items += r["Items"]
        if "LastEvaluatedKey" not in r:
            break
        kwargs = {"ExclusiveStartKey": r["LastEvaluatedKey"]}

    reviews = []
    for it in items:
        when = it["sk"].rsplit("#", 1)[-1]
        if when < a.since or it.get("record_type", "review") != "review" or not it.get("findings"):
            continue
        findings = json.loads(it["findings"])
        if findings:
            reviews.append((when, it["repo"], int(it["pr_number"]), it, findings))
    reviews.sort(key=lambda r: r[0], reverse=True)

    written, seen_threads = 0, {}
    for _, repo, pr, record, findings in reviews:
        if written >= a.limit:
            break
        key = (repo, pr)
        if key not in seen_threads:
            t, state = threads(repo, pr)
            seen_threads[key] = ([x for x in t if x["comments"]["nodes"]
                                  and is_bot((x["comments"]["nodes"][0].get("author") or {}).get("login", ""))], state)
        bot_threads, state = seen_threads[key]
        case = case_for(repo, pr, record, findings, bot_threads, state)
        if not case:
            continue
        with open(os.path.join(out, case["name"] + ".json"), "w") as fh:
            json.dump(case, fh, indent=1)
        written += 1
        print(f"{case['name']}: {len(case['accepted'])} accepted, {len(case['dismissed'])} dismissed",
              file=sys.stderr)
    print(f"{written} cases in {out}", file=sys.stderr)


if __name__ == "__main__":
    main()
