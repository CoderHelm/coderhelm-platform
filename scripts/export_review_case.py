#!/usr/bin/env python3
"""Export a PR's CoderHelm review history into an anonymized eval case.

Usage:
  AWS_PROFILE=<profile> python3 scripts/export_review_case.py \
      --team 'TEAM#...' --repo owner/name --pr 123 [--stage prod] > case.json

Reads the PR's REVIEW# records (and REVIEWFAIL# note) from the settings
table and writes a case skeleton for services/worker/src/passes/review_cases:
the repo, people and commit shas are replaced with generic names (acme/app,
h1, h2, ...), bodies are cut to a few sentences, and every step the reviewer
took becomes a step to replay. Fill in what the records can't hold (the
findings each review emitted, each comment's ask_kind and the reply's
conclusion), set `expect`, check no customer text is left, and add the file.
See review_cases/README.md.
"""

import argparse
import json
import re
import sys

import boto3

LEGACY_FAILED_PREFIX = "Automated review could not complete"


def anonymize(text, repo, names):
    out = text.replace(repo, "acme/app")
    owner, name = repo.split("/", 1)
    out = out.replace(owner, "acme").replace(name, "app")
    for n in names:
        out = out.replace(n, "someone")
    out = re.sub(r"@[A-Za-z0-9-]+", "@someone", out).replace("@someone re-review", "@coderhelm re-review")
    return out


def short(text, limit=600):
    text = " ".join(text.split())
    return text if len(text) <= limit else text[:limit].rsplit(" ", 1)[0] + " ..."


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--team", required=True)
    ap.add_argument("--repo", required=True)
    ap.add_argument("--pr", type=int, required=True)
    ap.add_argument("--stage", default="prod")
    ap.add_argument("--names", default="", help="comma-separated names to scrub")
    a = ap.parse_args()

    table = boto3.resource("dynamodb").Table(f"coderhelm-{a.stage}-settings")
    prefix = f"REVIEW#{a.repo}#{a.pr:06d}#"
    items, kwargs = [], {}
    while True:
        r = table.query(
            KeyConditionExpression="pk = :p AND begins_with(sk, :s)",
            ExpressionAttributeValues={":p": a.team, ":s": prefix},
            **kwargs,
        )
        items += r["Items"]
        if "LastEvaluatedKey" not in r:
            break
        kwargs["ExclusiveStartKey"] = r["LastEvaluatedKey"]
    items.sort(key=lambda i: i["sk"])
    names = [n for n in a.names.split(",") if n]

    heads, steps, comment_id = {}, [], 100
    for it in items:
        sha = it.get("head_sha", "")
        if sha and sha not in heads:
            heads[sha] = f"h{len(heads) + 1}"
            steps.append({"push": heads[sha]})
        head = heads.get(sha, "h1")
        body = anonymize(it.get("body", ""), a.repo, names)
        verdict = it.get("verdict", "")
        if body.startswith(LEGACY_FAILED_PREFIX):
            steps.append({"legacy_error_review": {"head": head, "error": short(body, 200)}})
        elif verdict == "QUESTION":
            comment_id += 1
            steps.append({"comment": {
                "id": comment_id,
                "text": "TODO: the person's comment (anonymized)",
                "ask_kind": "TODO: re_review | approve_request | question | fix_request | other",
                "reply": short(body),
                "conclusion": {"verdict_change": "TODO: none | clear | block", "reason": "TODO",
                               "new_nonblocking_items": False, "revised_reply": ""},
            }})
        else:
            steps.append({"review": {"head": head, "model_output": {
                "verdict": verdict,
                "summary": short(body),
                "findings": [],
                "_todo": "add the findings this review emitted (anonymized)",
            }}})
    case = {
        "name": f"TODO: what this case is about ({len(steps)} steps)",
        "description": "TODO: what went wrong and what must hold now",
        "steps": steps,
        "expect": {"final_formal": {"head": f"h{max(len(heads), 1)}", "event": "TODO"}},
    }
    json.dump(case, sys.stdout, indent=2)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
