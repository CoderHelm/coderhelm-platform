#!/usr/bin/env python3
"""A tiny weekly skill for the skill-run tests: plan, build, needs-login.

plan   prints the tool calls to make (a first run asks for 30 days too).
build  reads the raw files, compares with the newest earlier snapshot in
       OUT/snapshots, writes OUT/snapshots/<date>.json and
       OUT/reports/<date>.html, prints a JSON result.
needs-login writes OUT/reports/<date>-needs-login.md and nothing else.

Standard library only.
"""
import argparse
import datetime
import html
import json
import os
import sys
from pathlib import Path

HERE = Path(os.environ.get("CLAUDE_SKILL_DIR") or Path(__file__).resolve().parent)


def snapshots(out):
    d = Path(out) / "snapshots"
    return sorted(d.glob("*.json")) if d.is_dir() else []


def previous(out, date):
    earlier = [p for p in snapshots(out) if p.stem < date]
    return json.loads(earlier[-1].read_text()) if earlier else None


def plan(a):
    first = not snapshots(a.out)
    calls = [{"file": "visits_7.txt", "tool": "traffic_counts", "args": {"days": 7}}]
    if first:
        calls.append({"file": "visits_30.txt", "tool": "traffic_counts", "args": {"days": 30}})
    print(json.dumps({"first_run": first, "calls": calls}))


def read_counts(path):
    counts = {}
    for line in Path(path).read_text().splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[1].isdigit():
            counts[parts[0]] = int(parts[1])
    return counts


def build(a):
    date = a.date or datetime.date.today().isoformat()
    raw = Path(a.raw)
    week = read_counts(raw / "visits_7.txt")
    if not week:
        print(json.dumps({"error": "visits_7.txt is missing or empty"}))
        return 2
    baseline = json.loads((HERE / "baseline.json").read_text())
    prev = previous(a.out, date)
    findings = []
    for page, n in sorted(week.items()):
        before = (prev or {}).get("counts", {}).get(page)
        if before is None:
            if prev is not None:
                findings.append({"page": page, "kind": "new", "now": n})
        elif n >= 2 * before and n >= baseline["min_visits"]:
            findings.append({"page": page, "kind": "grew", "before": before, "now": n})
    out = Path(a.out)
    (out / "snapshots").mkdir(parents=True, exist_ok=True)
    (out / "reports").mkdir(parents=True, exist_ok=True)
    snap = out / "snapshots" / f"{date}.json"
    snap.write_text(json.dumps({"date": date, "counts": week, "findings": findings}, indent=1))
    rows = "".join(
        f"<tr><td>{html.escape(p)}</td><td>{(prev or {}).get('counts', {}).get(p, '')}</td><td>{n}</td></tr>"
        for p, n in sorted(week.items())
    )
    items = "".join(
        f"<li><b>{html.escape(f['page'])}</b> {f['kind']}"
        + (f" from {f['before']} to {f['now']}" if f["kind"] == "grew" else f" ({f['now']})")
        + "</li>"
        for f in findings
    ) or "<li>No page grew or appeared this week.</li>"
    compared = f"compared with {prev['date']}" if prev else "first run: no earlier week"
    report = out / "reports" / f"{date}.html"
    report.write_text(
        "<!doctype html><html><head><meta charset='utf-8'><title>Visit count "
        + date
        + "</title><style>body{font-family:system-ui,sans-serif;margin:24px;color:#1b1f24}"
        "table{border-collapse:collapse}td,th{border:1px solid #ccd;padding:4px 10px;text-align:left}"
        "th{background:#eef}h1{font-size:20px}</style></head><body>"
        f"<h1>Weekly visit count, {date}</h1><p>{len(week)} pages, {compared}.</p>"
        f"<h2>Findings</h2><ul>{items}</ul>"
        f"<h2>Pages</h2><table><tr><th>Page</th><th>Last week</th><th>This week</th></tr>{rows}</table>"
        "</body></html>"
    )
    print(json.dumps({
        "report": str(report.relative_to(out)),
        "snapshot": str(snap.relative_to(out)),
        "compared_with": prev["date"] if prev else None,
        "findings": findings,
    }))
    return 0


def needs_login(a):
    date = a.date or datetime.date.today().isoformat()
    d = Path(a.out) / "reports"
    d.mkdir(parents=True, exist_ok=True)
    p = d / f"{date}-needs-login.md"
    p.write_text("# Needs login\n\n" + "\n".join(f"- {f}" for f in a.failed) + "\n")
    print(p)


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("plan")
    p.add_argument("--out", required=True)
    b = sub.add_parser("build")
    b.add_argument("--raw", required=True)
    b.add_argument("--out", required=True)
    b.add_argument("--date")
    n = sub.add_parser("needs-login")
    n.add_argument("--out", required=True)
    n.add_argument("--failed", action="append", default=[])
    n.add_argument("--date")
    a = ap.parse_args()
    if a.cmd == "plan":
        return plan(a)
    if a.cmd == "build":
        return build(a)
    return needs_login(a)


if __name__ == "__main__":
    sys.exit(main() or 0)
