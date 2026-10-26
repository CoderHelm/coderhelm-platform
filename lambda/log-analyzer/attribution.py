"""
Which log group a finding belongs to, and how often its error happened.

Every number here comes from CloudWatch Logs Insights: each query run over
several log groups returns `@log` (account:log group) and `@logStream` and is
grouped by them, so a count, first seen and last seen always belong to one
log group. The model only writes words (title, summary, suggested action) from
that evidence; it never picks or renames the source.

Flow for one finding the model wrote:
1. `choose_literal`: the exact text Insights will match, copied from the
   evidence the model cited (a literal "is this text in that line" check is
   the only pre-filter; there are no matching rules).
2. `attribute`: one stats query over every log group the cited evidence was
   searched in, grouped by @log and @logStream.
3. `check_group`: before a finding is saved or updated, the same query on its
   one log group alone. Zero means the credit moves to the next group that
   matches, or the finding is dropped.
4. `guard_texts`: the title and summary must name only what the evidence
   shows; the model rewrites any that don't, from the evidence.

Pure helpers take plain data; the query helpers take a boto3 `logs` client.
"""

import json
import logging
import time
from datetime import datetime, timezone

logger = logging.getLogger()

# Most groups one finding is split into (most matching lines first).
MAX_GROUPS_PER_FINDING = 5
# Insights' own row cap; a full page means counts may be a lower bound.
STATS_ROW_LIMIT = 10000
MAX_SAMPLES = 3
SAMPLE_CHARS = 500
# Group names shorter than this are too generic for a literal "names it" check.
MIN_NAME_CHARS = 6
# Poll pause between result checks (tests set it to a no-op).
SLEEP = time.sleep


# ── Insights rows ────────────────────────────────────────────────


def split_log(value, fallback_groups=()):
    """(account, log group) from an `@log` value ("123456789012:/ecs/web").
    Without one, the only group the query ran on, else ("", "")."""
    value = (value or "").strip()
    if value:
        head, sep, tail = value.partition(":")
        if sep and head.isdigit() and tail:
            return head, tail
        return "", value
    if len(fallback_groups) == 1:
        return "", fallback_groups[0]
    return "", ""


def parse_ts(value):
    """An Insights timestamp ("2026-10-25 14:03:11.123", ISO, or epoch ms) as
    an aware UTC datetime, or None."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value).strip()
    if text.isdigit():
        n = int(text)
        return datetime.fromtimestamp(n / 1000 if n > 10**11 else n, tz=timezone.utc)
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    try:
        t = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return t if t.tzinfo else t.replace(tzinfo=timezone.utc)


def iso(t):
    return t.astimezone(timezone.utc).isoformat(timespec="milliseconds") if t else ""


def as_int(value):
    try:
        return max(int(float(value)), 0)
    except (TypeError, ValueError):
        return 0


def rows_of(results):
    """get_query_results rows as dicts."""
    return [{f.get("field"): f.get("value") for f in row} for row in results or []]


def run_query(cw_logs, log_groups, query, start, end, wait_secs=30, sleep=None):
    """Run one Insights query; rows as dicts, or None when it failed."""
    try:
        q = cw_logs.start_query(
            logGroupNames=list(log_groups),
            startTime=int(start),
            endTime=int(end),
            queryString=query,
        )
    except Exception as e:  # noqa: BLE001 - a failed query is reported, not raised
        logger.warning(f"StartQuery failed: {type(e).__name__}: {e}")
        return None
    query_id = q["queryId"]
    for _ in range(wait_secs):
        (sleep or SLEEP)(1)
        r = cw_logs.get_query_results(queryId=query_id)
        status = r.get("status")
        if status == "Complete":
            return rows_of(r.get("results", []))
        if status in ("Failed", "Cancelled", "Timeout"):
            logger.warning(f"Query {query_id} ended with status: {status}")
            return None
    try:
        cw_logs.stop_query(queryId=query_id)
    except Exception:  # noqa: BLE001
        pass
    logger.warning(f"Query {query_id} did not finish in time")
    return None


# ── Evidence the model reads ─────────────────────────────────────


def discovery_query(filters):
    """A discovery query: matching lines counted per log group, stream and
    message, with their first and last time."""
    return (
        "fields @timestamp, @message, @log, @logStream "
        + "".join(f"| {f} " for f in filters)
        + "| stats count() as n, min(@timestamp) as first_ts, max(@timestamp) as last_ts"
        " by @log, @logStream, @message "
        "| sort n desc "
        "| limit 100"
    )


def build_evidence(batches, scrub=lambda s: s):
    """One entry per (account, log group, message) from the discovery rows,
    each with the log groups its query searched (`scope`). Rows whose log
    group can't be told are left out: evidence nobody can attribute is not
    evidence. Ids are stable within a run ("e1" = most lines)."""
    merged = {}
    for b in batches:
        scope = tuple(b.get("log_groups", []))
        for row in b.get("results", []):
            account, group = split_log(row.get("@log"), scope)
            if not group:
                continue
            message = scrub(str(row.get("@message", "")).strip())[:SAMPLE_CHARS]
            if not message:
                continue
            key = (account, group, message)
            e = merged.setdefault(key, {
                "account": account, "log_group": group, "message": message,
                "count": 0, "first": None, "last": None, "streams": {},
                "queries": [], "scope": [],
            })
            n = as_int(row.get("n") or row.get("count()") or 1) or 1
            e["count"] += n
            stream = str(row.get("@logStream", "") or "")
            if stream:
                e["streams"][stream] = e["streams"].get(stream, 0) + n
            first, last = parse_ts(row.get("first_ts")), parse_ts(row.get("last_ts"))
            if first and (e["first"] is None or first < e["first"]):
                e["first"] = first
            if last and (e["last"] is None or last > e["last"]):
                e["last"] = last
            if b.get("query_name") and b["query_name"] not in e["queries"]:
                e["queries"].append(b["query_name"])
            for g in scope or (group,):
                if g not in e["scope"]:
                    e["scope"].append(g)
    out = sorted(merged.values(), key=lambda e: (-e["count"], e["log_group"], e["message"]))
    for i, e in enumerate(out, 1):
        e["id"] = f"e{i}"
    return out


def evidence_for_prompt(evidence, limit=120):
    """What the model sees: the numbers per log group, never a guessed one."""
    return [
        {
            "id": e["id"],
            "log_group": e["log_group"],
            "account": e["account"],
            "lines": e["count"],
            "first_seen": iso(e["first"]),
            "last_seen": iso(e["last"]),
            "log_streams": sorted(e["streams"], key=lambda s: -e["streams"][s])[:3],
            "query": ", ".join(e["queries"]),
            "message": e["message"],
        }
        for e in evidence[:limit]
    ]


# ── The literal Insights matches ─────────────────────────────────


def pattern_literal(pattern):
    """Text Insights can match with `like "..."`: the first non-empty line,
    without characters that end the string, at most 80 chars. Mirrors
    common::log_findings::pattern_literal so the dashboard's samples and the
    repair job search the same text. None when under 6 chars remain."""
    if not isinstance(pattern, str):
        return None
    first = next((l.strip() for l in pattern.splitlines() if l.strip()), "")
    clean = "".join(c for c in first if c not in '"\\\r\n`')[:80].strip()
    return clean if len(clean) >= 6 else None


def choose_literal(rec, evidence):
    """(literal, cited evidence) for one model finding, or (None, []).
    The literal must occur in a cited line; when the model cited nothing
    usable, the evidence lines that contain its literal are used. When its
    literal occurs nowhere, the cited line's own first line is the literal."""
    by_id = {e["id"]: e for e in evidence}
    ids = rec.get("evidence_ids")
    ids = ids if isinstance(ids, list) else []
    cited = [by_id[i] for i in ids if isinstance(i, str) and i in by_id]
    lit = pattern_literal(rec.get("error_pattern"))
    if lit and cited and any(lit in e["message"] for e in cited):
        return lit, [e for e in cited if lit in e["message"]]
    if lit:
        holders = [e for e in evidence if lit in e["message"]]
        if holders:
            return lit, holders
    for e in cited:
        own = pattern_literal(e["message"])
        if own:
            return own, [e]
    return None, []


def scope_of(cited):
    """Every log group the cited evidence's queries searched, in order."""
    seen = []
    for e in cited:
        for g in e.get("scope") or [e["log_group"]]:
            if g not in seen:
                seen.append(g)
    return seen


# ── Attribution from Insights ────────────────────────────────────


def stats_query(literal):
    """Matching lines of one literal per log group and stream."""
    return (
        "fields @timestamp, @message, @log, @logStream "
        f'| filter @message like "{literal}" '
        "| stats count() as n, min(@timestamp) as first_ts, max(@timestamp) as last_ts,"
        " latest(@message) as sample by @log, @logStream "
        "| sort n desc "
        f"| limit {STATS_ROW_LIMIT}"
    )


def group_stats(rows, scope=(), scrub=lambda s: s):
    """{log group: {account, count, first, last, streams, samples}} from
    stats rows grouped by @log and @logStream."""
    out = {}
    for row in rows:
        account, group = split_log(row.get("@log"), tuple(scope))
        if not group:
            continue
        n = as_int(row.get("n"))
        if n <= 0:
            continue
        g = out.setdefault(group, {
            "account": account, "log_group": group, "count": 0,
            "first": None, "last": None, "streams": {}, "samples": [],
        })
        g["count"] += n
        stream = str(row.get("@logStream", "") or "")
        g["streams"][stream] = g["streams"].get(stream, 0) + n
        first, last = parse_ts(row.get("first_ts")), parse_ts(row.get("last_ts"))
        if first and (g["first"] is None or first < g["first"]):
            g["first"] = first
        if last and (g["last"] is None or last > g["last"]):
            g["last"] = last
        if row.get("sample"):
            g["samples"].append({
                "log_group": group,
                "log_stream": stream,
                "timestamp": iso(last),
                "message": scrub(str(row["sample"]).strip())[:SAMPLE_CHARS],
            })
    for g in out.values():
        g["samples"] = sorted(g["samples"], key=lambda s: s["timestamp"], reverse=True)[:MAX_SAMPLES]
    if len(rows) >= STATS_ROW_LIMIT:
        logger.warning("Attribution hit the Insights row cap; counts are lower bounds")
    return out


def attribute(cw_logs, groups, literal, start, end, scrub=lambda s: s, run=run_query):
    """Per-group stats of `literal` over `groups` (50 per query). None when a
    query failed: an unanswered question is never read as zero."""
    total = {}
    for i in range(0, len(groups), 50):
        batch = groups[i : i + 50]
        rows = run(cw_logs, batch, stats_query(literal), start, end)
        if rows is None:
            return None
        for name, g in group_stats(rows, batch, scrub).items():
            total[name] = g
    return total


def check_group(cw_logs, group, literal, start, end, scrub=lambda s: s, run=run_query):
    """The check before saving: the literal's stats on this one log group.
    Zero lines = an empty dict's count of 0; None when the query failed."""
    stats = attribute(cw_logs, [group], literal, start, end, scrub, run)
    if stats is None:
        return None
    return stats.get(group) or {
        "account": "", "log_group": group, "count": 0,
        "first": None, "last": None, "streams": {}, "samples": [],
    }


def credited_groups(stats, cw_logs, literal, start, end, scrub=lambda s: s, run=run_query):
    """The groups a finding is credited to: every group with matching lines,
    most first, each confirmed by `check_group` on that group alone. A group
    the check finds empty is skipped and the credit goes to the next. Returns
    (credited stats list, check_failed)."""
    credited, failed = [], False
    ranked = sorted(stats.values(), key=lambda g: (-g["count"], g["log_group"]))
    for g in ranked:
        if len(credited) >= MAX_GROUPS_PER_FINDING:
            break
        checked = check_group(cw_logs, g["log_group"], literal, start, end, scrub, run)
        if checked is None:
            failed = True
            continue
        if checked["count"] <= 0:
            logger.info(f"Check found no '{literal[:40]}' lines in {g['log_group']}; not credited there")
            continue
        if not checked["account"]:
            checked["account"] = g["account"]
        credited.append(checked)
    return credited, failed


# ── Counts and times across runs ─────────────────────────────────


def merge_seen(prev, stats, new_since_last):
    """seen_count, first_seen_at and last_seen_at for a finding seen again.

    `prev` is the stored item (or None for a new finding); `stats` this run's
    checked stats; `new_since_last` the lines after the stored last_seen_at,
    or None when that time is outside this run's window (then every line in
    the window is new). seen_count counts matching events; first seen never
    moves forward and last seen never moves back. Items written before counts
    were events (no count_basis) start over from this run's lines."""
    first, last, count = stats["first"], stats["last"], stats["count"]
    if not prev:
        return count, first, last
    events = prev.get("count_basis") == "events"
    prev_first = parse_ts(prev.get("first_seen_at")) or parse_ts(prev.get("created_at"))
    if prev_first and (first is None or prev_first < first):
        first = prev_first
    if not events:
        return count, first, last
    prev_last = parse_ts(prev.get("last_seen_at"))
    if prev_last and (last is None or prev_last > last):
        last = prev_last
    seen = as_int(prev.get("seen_count"))
    seen += count if new_since_last is None else new_since_last
    return seen, first, last


# ── The title guard ──────────────────────────────────────────────


def leaf(group):
    return (group or "").rstrip("/").rsplit("/", 1)[-1].lower()


def names_other_group(text, own_group, all_groups):
    """Literal pre-filter: another log group's name (or path) appears in the
    text. Names shorter than MIN_NAME_CHARS or contained in this finding's own
    group name are skipped."""
    text = (text or "").lower()
    own = (own_group or "").lower()
    for g in all_groups:
        if not g or g.lower() == own:
            continue
        name = leaf(g)
        if g.lower() in text:
            return True
        if len(name) >= MIN_NAME_CHARS and name not in own and name in text:
            return True
    return False


def needs_guard(cand, all_groups):
    """Pre-filter only: True unless the text plainly names its own group and
    no other. Everything it lets through goes to the model's check."""
    text = f"{cand.get('title', '')} {cand.get('summary', '')}"
    own = cand["log_group"]
    names_own = own.lower() in text.lower() or (
        len(leaf(own)) >= MIN_NAME_CHARS and leaf(own) in text.lower()
    )
    return not names_own or names_other_group(text, own, all_groups)


def evidence_text(cand):
    """Title and summary built from the evidence alone (used when the model's
    rewrite is unavailable or still names another group)."""
    stats = cand["stats"]
    lit = cand["literal"]
    name = leaf(cand["log_group"]) or cand["log_group"]
    title = f"{lit[:60]} in {name}"[:80]
    when = ""
    if stats.get("first") and stats.get("last"):
        when = f" between {iso(stats['first'])} and {iso(stats['last'])}"
    summary = (
        f"{stats['count']} log lines matching \"{lit}\" in {cand['log_group']}{when}."
    )
    return title, summary


def guard_prompt(items):
    payload = [
        {
            "id": c["id"],
            "title": c.get("title", ""),
            "summary": c.get("summary", ""),
            "evidence": {
                "log_group": c["log_group"],
                "matching_lines": c["stats"]["count"],
                "first_seen": iso(c["stats"].get("first")),
                "last_seen": iso(c["stats"].get("last")),
                "error_text": c["literal"],
                "samples": [s["message"] for s in c["stats"].get("samples", [])],
                "other_log_groups_with_this_error": c.get("others", []),
            },
        }
        for c in items
    ]
    return f"""You check log findings against their evidence. Each item has a title and summary another model wrote, and the evidence: the one log group the error was counted in, how many lines matched, when, and sample lines.

<items>
{json.dumps(payload, indent=2)}
</items>

For each item decide: does the title or summary name a service, application, component or log group that the evidence does not support? The evidence supports the item's own log group and what its sample lines say. Another log group, or a service the samples never mention, is not supported.

If supported, return it unchanged. If not, rewrite the title (max 80 chars) and summary (2-3 sentences) from the evidence only, naming the item's own log group.

Return ONLY a JSON array: [{{"id": "...", "supported": true|false, "title": "...", "summary": "..."}}]"""


def guard_texts(cands, all_groups, ask_model):
    """Make every candidate's title and summary match its evidence.
    `ask_model(prompt)` returns the model's text, or None on failure.
    Candidates are changed in place; returns how many were rewritten."""
    flagged = [c for c in cands if needs_guard(c, all_groups)]
    if not flagged:
        return 0
    answers = {}
    reply = ask_model(guard_prompt(flagged))
    if reply:
        try:
            text = reply.strip()
            start, end = text.find("["), text.rfind("]")
            parsed = json.loads(text[start : end + 1]) if start != -1 and end > start else []
            for a in parsed if isinstance(parsed, list) else []:
                if isinstance(a, dict) and isinstance(a.get("id"), str):
                    answers[a["id"]] = a
        except ValueError:
            logger.warning("Title guard reply was not JSON; using evidence text")
    rewritten = 0
    for c in flagged:
        a = answers.get(c["id"])
        if a is None:
            # No verdict on text that doesn't plainly name its own group:
            # the evidence's own words, never an unchecked claim.
            c["title"], c["summary"] = evidence_text(c)
            rewritten += 1
            continue
        if a.get("supported") is True:
            continue
        title = str(a.get("title") or "").strip()[:200]
        summary = str(a.get("summary") or "").strip()[:2000]
        if not title or names_other_group(f"{title} {summary}", c["log_group"], all_groups):
            title, summary = evidence_text(c)
        c["title"], c["summary"] = title, summary or evidence_text(c)[1]
        rewritten += 1
    return rewritten
