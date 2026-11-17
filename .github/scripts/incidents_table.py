#!/usr/bin/env python3
"""Prints the incident regression cases as one Markdown table.

Each case has its own page, docs/incidents/<case>.md, with a title
("# <letter>. `<case>`"), a **What happened:** paragraph and a
**The guarantee:** part. There is no committed table: two pull requests
that each add a case would collide on it. This builds it on demand.

Usage: python3 .github/scripts/incidents_table.py [DOCS_DIR]
"""

import os
import re
import sys


def one_line(text: str) -> str:
    return " ".join(text.replace("|", "\\|").split())


def row(path: str) -> str:
    text = open(path, encoding="utf-8").read()
    title = text.splitlines()[0].lstrip("# ").strip() if text else os.path.basename(path)
    what = re.search(r"\*\*What happened:\*\*(.*?)(?=\*\*The guarantee:\*\*|$)", text, re.S)
    guarantee = re.search(r"\*\*The guarantee:\*\*(.*)$", text, re.S)
    return "| {} | {} | {} |".format(
        one_line(title),
        one_line(what.group(1) if what else ""),
        one_line(guarantee.group(1) if guarantee else ""),
    )


def main() -> None:
    docs = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
        os.path.dirname(__file__), "..", "..", "docs", "incidents"
    )
    pages = sorted(p for p in os.listdir(docs) if p.endswith(".md"))
    print("| Case | What happened | The guarantee |")
    print("|---|---|---|")
    for p in pages:
        print(row(os.path.join(docs, p)))


if __name__ == "__main__":
    main()
