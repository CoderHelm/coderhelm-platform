---
name: visit-count
description: Weekly page-visit count for an example site. Reads the traffic tool, compares with last week's snapshot and writes one HTML report. Read-only.
---

Count this week's page visits and compare them with last week's.

## Hard rules

- **Read-only.** The only tool this skill uses is `traffic_counts`. It may tag a page with the
  `tag_page` tool when asked to. It never calls the `set_banner` tool.
- **Stop on missing access.** If `traffic_counts` fails with an access error, run `needs-login`
  and stop. Never write a partial report.

## Procedure

```bash
S="${CLAUDE_SKILL_DIR}/count_visits.py"
WORK=$(mktemp -d)
python3 "$S" plan --out "$DIR"
```

For each call in `calls`, call the tool named in `tool` with `args` and write its full output,
unchanged, to `$WORK/<file>`.

```bash
python3 "$S" build --raw "$WORK" --out "$DIR"
rm -rf "$WORK"
```

Use the same `$DIR` every week: `snapshots/` is the skill's memory. Tell the caller the report path
and the pages that grew.
