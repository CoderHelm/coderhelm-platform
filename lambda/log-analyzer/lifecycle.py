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
- An open finding is resolved only after QUIET_DAYS without a matching line,
  never because one run missed it.

Pure functions only; the handler does the reads and writes.
"""

import hashlib
from datetime import datetime, timedelta, timezone

import attribution
import fingerprint

# A finding is resolved after this long without a matching line.
QUIET_DAYS = 7
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


def stale(existing, seen_ids, now=None, quiet_days=QUIET_DAYS):
    """Open findings to resolve: pending, not seen this run, and no matching
    line for `quiet_days`. A finding with no time at all is left open."""
    now = now or datetime.now(timezone.utc)
    out = []
    for item in existing:
        if item.get("rec_id") in seen_ids or item.get("status", "pending") != "pending":
            continue
        if item.get("merged_into"):
            continue
        last = last_sighting(item)
        if last is not None and now - last >= timedelta(days=quiet_days):
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
