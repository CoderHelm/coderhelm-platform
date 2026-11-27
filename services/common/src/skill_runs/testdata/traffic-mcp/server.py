#!/usr/bin/env python3
"""A stdio MCP server for the skill-run tests: an example traffic tool set.

- traffic_counts: visits per page (no annotations: its risk is decided by a
  model check). Needs AWS keys in its environment, like a server that reads
  a team's account; without them it answers an access error.
- list_pages: read-only by its annotations.
- tag_page: writes, by its annotations (not destructive).
- set_banner: destructive by its annotations.

The week it answers for, and whether AWS refuses it, come from the files
FAKE_WEEK_FILE and FAKE_DENY_FILE name in its environment.
"""
import json
import os
import sys

TOOLS = [
    {
        "name": "traffic_counts",
        "description": "Visits per page over the last days.",
        "inputSchema": {"type": "object", "properties": {"days": {"type": "integer"}}},
    },
    {
        "name": "list_pages",
        "description": "The site's pages.",
        "inputSchema": {"type": "object"},
        "annotations": {"readOnlyHint": True},
    },
    {
        "name": "tag_page",
        "description": "Adds a tag to a page.",
        "inputSchema": {"type": "object", "properties": {"page": {"type": "string"}}},
        "annotations": {"readOnlyHint": False, "destructiveHint": False},
    },
    {
        "name": "set_banner",
        "description": "Changes the site's banner.",
        "inputSchema": {"type": "object", "properties": {"text": {"type": "string"}}},
        "annotations": {"readOnlyHint": False, "destructiveHint": True},
    },
]


def reply(rid, result=None, error=None):
    msg = {"jsonrpc": "2.0", "id": rid}
    if error:
        msg["error"] = {"code": -32000, "message": error}
    else:
        msg["result"] = result
    sys.stdout.write(json.dumps(msg) + "\n")
    sys.stdout.flush()


def call(name, args):
    if name == "traffic_counts":
        if not os.environ.get("AWS_ACCESS_KEY_ID"):
            return "User: anonymous is not authorized to perform: athena:StartQueryExecution", True
        deny = os.environ.get("FAKE_DENY_FILE", "")
        if deny and os.path.exists(deny):
            return "AccessDeniedException: User: arn:aws:sts::111122223333:assumed-role/CoderHelmLogReader/run is not authorized to perform: cloudfront:ListDistributions", True
        wf = os.environ.get("FAKE_WEEK_FILE", "")
        week = int(open(wf).read().strip() or "0") if wf and os.path.exists(wf) else 0
        if week == 0:
            return "/home 900\n/classes 120\n/pricing 300\n", False
        return "/home 950\n/classes 400\n/pricing 290\n/events 150\n", False
    if name == "list_pages":
        return "/home\n/classes\n/pricing\n", False
    return f"changed: {json.dumps(args)}", False


for line in sys.stdin:
    try:
        msg = json.loads(line)
    except ValueError:
        continue
    rid, method = msg.get("id"), msg.get("method")
    if rid is None:
        continue
    if method == "initialize":
        reply(rid, {"protocolVersion": "2025-06-18", "capabilities": {"tools": {}},
                    "serverInfo": {"name": "traffic", "version": "1.2.0"}})
    elif method == "tools/list":
        reply(rid, {"tools": TOOLS})
    elif method == "tools/call":
        p = msg.get("params") or {}
        text, err = call(p.get("name"), p.get("arguments") or {})
        reply(rid, {"content": [{"type": "text", "text": text}], "isError": err})
    else:
        reply(rid, error=f"unknown method {method}")
