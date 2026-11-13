"""
A log finding's life: which stored finding a new sighting is, when a finding
is resolved, and what happens when a resolved one comes back.

Stored statuses (common::log_findings::Finding): `pending` (open, New or
Acknowledged on the page), `approved` (a plan was made), `dismissed`
(Ignored), `resolved` (the analyzer stopped seeing it). A finding with a
`fix_ticket_id` is being fixed whatever its status.

Rules:
- A sighting is matched against every finding of the account and log group
  that is open, acknowledged, being fixed, ignored, or resolved within
  REOPEN_DAYS, never only the open ones.
- A resolved finding seen again is reopened (status back to pending,
  `reopened_at`, `reopen_count`, `last_resolved_at`), never duplicated.
- An ignored finding stays ignored; its counts and history still grow.
- An open finding (New, Acknowledged or being fixed) is resolved once it has
  been quiet for its quiet period, never because one run missed it.
  Acknowledging never holds a finding open.

The quiet period (quiet_days_for):
- A burst that stopped (lines on one run of consecutive days, or no daily
  history): DEFAULT_QUIET_DAYS.
- An error that keeps coming back: twice the longest silence it has come back
  from, counting gaps between days with lines and the time from a resolve to
  the reopen, so it isn't resolved while it is likely to return.
- Never less than MIN_QUIET_DAYS (24 hours), never more than MAX_QUIET_DAYS.

Whether it is still happening (activity_for), from the checks the analyzer
keeps on the finding (one per analysis of its log group: new matching lines,
the lines the group logged in the window checked, the window's length):
- happening: the latest analysis saw a new line, or only one has gone by
  without one (one run that saw nothing is not evidence);
- stopped: STOPPED_AFTER_CHECKS analyses in a row saw no matching line while
  the service kept logging (at least 1/VOLUME_FACTOR of its volume while the
  error was happening);
- unknown: no matching line, but the service itself went quiet or its volume
  could not be read, so nobody can tell.

Evidence resolves a burst sooner (quiet_period): a burst that stopped, with a
production deploy of the log group's repo or a change merged in it around the
time it stopped (`change`, recorded by the worker), resolves after
EVIDENCE_QUIET_DAYS without a line instead of DEFAULT_QUIET_DAYS. An error that keeps coming back keeps its
longer period whatever changed.

services/common/src/log_findings.rs has the same rules; both run
services/common/tests/fixtures/quiet_days_cases.json and
finding_activity_cases.json.

Pure functions only; the handler does the reads and writes.
"""

import hashlib
from datetime import datetime, timedelta, timezone

import attribution
import fingerprint

# A burst that stopped is resolved after this long without a matching line.
DEFAULT_QUIET_DAYS = 3
MIN_QUIET_DAYS = 1
MAX_QUIET_DAYS = 14
# How often the full analysis runs (its EventBridge schedule).
ANALYSIS_EVERY_HOURS = 6
# Analyses in a row without a matching line, at the service's usual volume,
# before a finding is stopped: one that saw nothing is never evidence (a run
# can miss it), so the smallest number that is.
STOPPED_AFTER_CHECKS = 2
# The service "kept logging" when a check's volume is at least this fraction
# of its volume while the error was happening: the factor the watch uses to
# call a change in volume a spike (handler.WATCH_FACTOR), the other way round.
VOLUME_FACTOR = 5
# Checks kept on a finding: the analyses in the default quiet period.
ACTIVITY_KEEP = DEFAULT_QUIET_DAYS * 24 // ANALYSIS_EVERY_HOURS
# A burst that stopped after a change in its repo resolves after this long
# without a line: the shortest quiet period there is, which is also the
# analyzer's lookback (every line it can read was read without a match).
EVIDENCE_QUIET_DAYS = MIN_QUIET_DAYS
# A change this long before the last matching line can still be what stopped
# it (merged, then deployed while the last lines were written): one analysis
# interval, the finest the analyzer tells time.
CHANGE_BEFORE_HOURS = ANALYSIS_EVERY_HOURS
# A resolved finding seen again within this long is reopened.
REOPEN_DAYS = 30
# Stored findings expire this long after their last sighting (DynamoDB TTL),
# longer than REOPEN_DAYS so a resolved finding is still there to reopen.
TTL_DAYS = 45
# Days of daily line counts kept on a finding.
HISTORY_DAYS = 30
# Streams and other log groups kept on a finding.
MAX_STREAMS = 10
MAX_OTHERS = 10
# A stored error pattern shorter than this is too generic to match on alone.
MIN_LITERAL_CHARS = 12

# Which of several matching findings a sighting belongs to: the one a person
# acted on first (its fix, plan or decision is what they see), then the oldest.
_RANK = {"approved": 1, "dismissed": 2, "pending": 3, "resolved": 4}


def _ts(value):
    return attribution.parse_ts(value)


def last_sighting(item):
    """When a finding's error was last seen: its last line's time, else
    when it was last refreshed or stored."""
    for k in ("last_seen_at", "updated_at", "created_at"):
        t = _ts(item.get(k))
        if t:
            return t
    return None


def is_recent(item, now=None):
    """Whether a stored finding takes part in matching: everything but merged
    duplicates and findings resolved more than REOPEN_DAYS ago."""
    if item.get("merged_into"):
        return False
    if item.get("status", "pending") != "resolved" or item.get("fix_ticket_id"):
        return True
    now = now or datetime.now(timezone.utc)
    resolved = _ts(item.get("resolved_at")) or last_sighting(item)
    return resolved is None or now - resolved <= timedelta(days=REOPEN_DAYS)


def rank(item):
    if item.get("fix_ticket_id"):
        return 0
    status = item.get("status", "pending")
    if status == "pending" and item.get("acknowledged_at"):
        return 1
    return _RANK.get(status, 3)


def _order(item):
    return (rank(item), item.get("created_at") or "", item.get("rec_id") or "")


def legacy_hash(account_id, group, literal):
    """The identity findings had before signatures: account, group and the
    model's error pattern (still matched, so older findings are found)."""
    raw = f"{account_id}:{group}:{literal}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def find_match(sighting, existing, now=None):
    """The stored finding a sighting is, or None.

    `sighting`: log_group, fingerprint, signature_hashes (set), legacy_hash,
    literal, lines (messages), existing_id (the model's guess).
    Only findings in the same log group can match: a match in another group
    would move its credit without evidence. Tiers, first that matches wins:
    1. the same fingerprint (or a stored error_hash equal to it)
    2. a shared signature
    3. the legacy hash (same account, group and error pattern)
    4. a stored error pattern (12+ chars) that occurs in the sighting's lines
    5. the model's existing_id
    Within a tier: a finding being fixed, then acknowledged, ignored, open,
    resolved; then the oldest."""
    group = sighting["log_group"]
    pool = [
        e for e in existing
        if e.get("source_log_group", group) == group and is_recent(e, now)
    ]
    if not pool:
        return None
    fp = sighting.get("fingerprint")
    hashes = set(sighting.get("signature_hashes") or ())
    lines = [str(l) for l in sighting.get("lines") or []]
    legacy = sighting.get("legacy_hash")

    def literal_in_lines(e):
        lit = attribution.pattern_literal(e.get("error_pattern"))
        return bool(lit and len(lit) >= MIN_LITERAL_CHARS and any(lit in l for l in lines))

    tiers = [
        lambda e: bool(fp) and fp in (e.get("fingerprint"), e.get("error_hash")),
        lambda e: bool(hashes & fingerprint.item_hashes(e)),
        lambda e: bool(legacy) and e.get("error_hash") == legacy,
        literal_in_lines,
        lambda e: isinstance(sighting.get("existing_id"), str)
        and e.get("rec_id") == sighting["existing_id"],
    ]
    for test in tiers:
        hits = [e for e in pool if test(e)]
        if hits:
            return min(hits, key=_order)
    return None


def return_gaps(item):
    """The silences (in days) a finding has come back from: gaps between
    days with lines, and the time from a resolve to the reopen. Empty for a
    burst that stopped."""
    days = sorted(
        str(h["day"])
        for h in item.get("daily_lines") or []
        if isinstance(h, dict) and h.get("day") and attribution.as_int(h.get("lines")) > 0
    )
    gaps = []
    for a, b in zip(days, days[1:]):
        try:
            gap = (datetime.fromisoformat(b) - datetime.fromisoformat(a)).days
        except ValueError:
            continue
        if gap > 1:
            gaps.append(gap)
    resolved, back = _ts(item.get("last_resolved_at")), _ts(item.get("reopened_at"))
    if resolved and back and back > resolved:
        gaps.append(max(1, round((back - resolved).total_seconds() / 86400)))
    return gaps


def quiet_days_for(item):
    """How long `item` must go without a matching line before it is
    resolved, from its history alone (see the module notes)."""
    gaps = return_gaps(item)
    quiet = 2 * max(gaps) if gaps else DEFAULT_QUIET_DAYS
    return max(MIN_QUIET_DAYS, min(MAX_QUIET_DAYS, quiet))


def checks_of(item):
    """The finding's checks, oldest first: (at, new matching lines, lines the
    group logged per minute or None when that wasn't read). Entries that are
    not checks are skipped."""
    out = []
    for c in item.get("activity") or []:
        if not isinstance(c, dict):
            continue
        at = _ts(c.get("at"))
        if at is None:
            continue
        minutes = attribution.as_int(c.get("minutes"))
        rate = None
        if c.get("scanned") is not None and minutes > 0:
            rate = attribution.as_int(c.get("scanned")) / minutes
        out.append((at, attribution.as_int(c.get("lines")), rate))
    return out


def activity_for(item):
    """Whether the finding is still happening (see the module notes):
    {state, quiet_checks, normal_checks, checked_at}. `quiet_checks` is the
    analyses in a row, up to the latest, with no new matching line;
    `normal_checks` those of them at the service's usual volume."""
    checks = checks_of(item)
    quiet = []
    for c in reversed(checks):
        if c[1] > 0:
            break
        quiet.append(c)
    # The service's usual volume: the middle rate of the checks with lines.
    busy = sorted(c[2] for c in checks if c[1] > 0 and c[2])
    usual = busy[(len(busy) - 1) // 2] if busy else None
    normal = sum(
        1 for c in quiet
        if c[2] is not None and (c[2] * VOLUME_FACTOR >= usual if usual else c[2] > 0)
    )
    if quiet and normal >= STOPPED_AFTER_CHECKS:
        state = "stopped"
    elif len(quiet) >= STOPPED_AFTER_CHECKS:
        state = "unknown"
    else:
        state = "happening"
    return {
        "state": state,
        "quiet_checks": len(quiet),
        "normal_checks": normal,
        "checked_at": checks[-1][0] if checks else None,
    }


def change_explains(item, activity=None):
    """Whether the change recorded on the finding (`change`: kind, repo, at,
    title) can be what stopped it: it landed no more than CHANGE_BEFORE_HOURS
    before the last matching line and no later than the latest check."""
    change = item.get("change")
    if not isinstance(change, dict):
        return False
    at, last = _ts(change.get("at")), _ts(item.get("last_seen_at"))
    checked = (activity or activity_for(item))["checked_at"]
    if not (at and last and checked):
        return False
    return last - timedelta(hours=CHANGE_BEFORE_HOURS) <= at <= checked


def quiet_period(item):
    """(days, basis): how long `item` must go without a matching line before
    it is resolved, and why: "returns" (it keeps coming back), "change" (a
    burst that stopped after a change in its repo) or "default"."""
    if return_gaps(item):
        return quiet_days_for(item), "returns"
    activity = activity_for(item)
    if activity["state"] == "stopped" and change_explains(item, activity):
        return EVIDENCE_QUIET_DAYS, "change"
    return quiet_days_for(item), "default"


def _days(n):
    return f"{n} day{'s' if n != 1 else ''}"


def resolved_reason(item, days, basis):
    """Why the analyzer resolved a finding, with the evidence when it
    resolved sooner than the default."""
    reason = f"Resolved: no new lines for {_days(days)}"
    if basis != "change":
        return reason
    change, activity = item.get("change") or {}, activity_for(item)
    what = "a deploy of" if change.get("kind") == "deploy" else "a change merged in"
    title = str(change.get("title") or "").strip()[:120]
    at = _ts(change.get("at"))
    return (
        f"{reason}. It stopped after {what} {change.get('repo') or 'its repo'}"
        + (f" ({title})" if title else "")
        + (f" on {at.strftime('%Y-%m-%d %H:%M')} UTC" if at else "")
        + f", and {activity['quiet_checks']} analyses in a row since found no matching line"
        " while the service kept logging"
    )


def add_check(activity, at, lines, scanned=None, minutes=None, keep=ACTIVITY_KEEP):
    """The finding's checks with one more (see checks_of); at most `keep`."""
    check = {"at": at, "lines": max(int(lines), 0)}
    if scanned is not None and minutes and int(minutes) > 0:
        check["scanned"] = max(int(scanned), 0)
        check["minutes"] = int(minutes)
    kept = [c for c in activity or [] if isinstance(c, dict)]
    return (kept + [check])[-keep:]


def stale(existing, seen_ids, now=None, quiet_days=None):
    """Open findings to resolve: pending (New, Acknowledged or being fixed),
    not seen this run, and quiet for their quiet period (quiet_period, or
    `quiet_days` when given). A finding with no time at all is left open."""
    now = now or datetime.now(timezone.utc)
    out = []
    for item in existing:
        if item.get("rec_id") in seen_ids or item.get("status", "pending") != "pending":
            continue
        if item.get("merged_into"):
            continue
        last = last_sighting(item)
        days = quiet_days if quiet_days is not None else quiet_period(item)[0]
        if last is not None and now - last >= timedelta(days=days):
            out.append(item)
    return out


def add_history(history, day, lines, keep=HISTORY_DAYS):
    """Daily line counts ([{day, lines}], oldest first) with `lines` added
    to `day` (YYYY-MM-DD); at most `keep` days."""
    by_day = {}
    for h in history or []:
        if isinstance(h, dict) and h.get("day"):
            by_day[str(h["day"])] = attribution.as_int(h.get("lines"))
    if lines > 0 or day not in by_day:
        by_day[day] = by_day.get(day, 0) + max(int(lines), 0)
    days = sorted(by_day)[-keep:]
    return [{"day": d, "lines": by_day[d]} for d in days]


def streams_of(stats, limit=MAX_STREAMS):
    """The log streams a finding's lines came from, most lines first."""
    streams = stats.get("streams") or {}
    ranked = sorted(streams.items(), key=lambda kv: (-kv[1], kv[0]))
    return [{"log_stream": s, "lines": int(n)} for s, n in ranked[:limit] if s]


def others_of(cand, limit=MAX_OTHERS):
    """Other log groups the same error was counted in this run."""
    return [
        {"log_group": o["log_group"], "lines": int(o.get("lines", 0))}
        for o in (cand.get("others") or [])[:limit]
        if o.get("log_group")
    ]


def ttl_from(now):
    return int((now + timedelta(days=TTL_DAYS)).timestamp())
