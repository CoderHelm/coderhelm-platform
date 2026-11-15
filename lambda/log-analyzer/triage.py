"""What a sighting is worth, and what it may cost.

Every finding the analyzer stores or refreshes gets one decision, the way an
engineer on call would read it, from the finding's own history:

- noise: it happens most days and this sighting is at its usual rate.
  Recorded on the finding; nobody is notified.
- watch: worth keeping an eye on (new and not critical, or back after a
  quiet spell, or above its usual rate but still small). New or back after
  a quiet spell, the team hears about it once.
- act: new and critical, or rising well above its usual rate. The team
  hears about it, and the worker's automatic fix (when on) picks it up.

And the cost guards for the model, so a burst never multiplies model calls:

- `collapse` folds evidence lines that are the same error (the same
  signature, see fingerprint.signature) into one entry before any model
  call, whatever ids or numbers each line carries.
- A line of an error that already has a finding is counted on that finding
  and never sent to the model again (handler.count_known).
- `take_model_call` holds a per-team hourly budget of analysis calls;
  past it the analyzer only counts (handler.BUDGET_STATE).
"""

from datetime import datetime, timedelta, timezone

import fingerprint

# The days before today a sighting is compared with.
BASELINE_DAYS = 7
# Seen on at least this many of those days: it is part of the service's
# usual day.
USUAL_DAYS = 2
# Rising: this many times its usual day, and at least this many lines.
RISING_FACTOR = 3
RISING_MIN_LINES = 20
# Analysis model calls one team may make per hour (every connection and
# every watch together). A burst needs one.
MODEL_CALLS_PER_HOUR = 6
BUDGET_PREFIX = "LOG_MODEL_CALLS#"

NOISE, WATCH, ACT = "noise", "watch", "act"


def _day(d):
    return d.strftime("%Y-%m-%d")


def baseline(daily_lines, today):
    """(lines a day on average, days seen) over the BASELINE_DAYS before
    `today` (a date), from the finding's daily_lines ([{name, lines}])."""
    by_day = {}
    for c in daily_lines or []:
        try:
            by_day[str(c.get("name"))] = int(c.get("lines") or 0)
        except (TypeError, ValueError, AttributeError):
            continue
    days = [_day(today - timedelta(days=i)) for i in range(1, BASELINE_DAYS + 1)]
    counts = [by_day.get(d, 0) for d in days]
    seen = sum(1 for n in counts if n > 0)
    return sum(counts) / BASELINE_DAYS, seen


def _per_day(avg):
    if avg <= 0:
        return "none"
    if avg < 1:
        return "less than one a day"
    return f"about {int(avg + 0.5)} a day"


def _lines(n):
    return f"{n} line{'s' if n != 1 else ''}"


def decide(item, lines, now=None, is_new=False, severity=""):
    """The decision for a sighting of `lines` lines on finding `item` (its
    daily_lines before this sighting): {"decision", "reason", "baseline",
    "days_seen", "lines"}. The reason is one plain sentence."""
    now = now or datetime.now(timezone.utc)
    avg, seen = baseline((item or {}).get("daily_lines"), now.date())
    lines = int(lines or 0)
    out = {"baseline": round(avg, 1), "days_seen": seen, "lines": lines}
    rising = lines >= RISING_MIN_LINES and lines > RISING_FACTOR * max(avg, 1)
    if is_new or not (item or {}).get("daily_lines"):
        if severity == "critical":
            return {**out, "decision": ACT, "reason": f"New and critical: {_lines(lines)}."}
        return {**out, "decision": WATCH, "reason": f"New: {_lines(lines)}, not seen before."}
    if rising:
        return {**out, "decision": ACT,
                "reason": f"Rising: {_lines(lines)} against {_per_day(avg)} over the last {BASELINE_DAYS} days."}
    if seen >= USUAL_DAYS:
        return {**out, "decision": NOISE,
                "reason": f"At its usual rate: {_lines(lines)}, {_per_day(avg)} on {seen} of the last {BASELINE_DAYS} days."}
    if seen == 0:
        return {**out, "decision": WATCH,
                "reason": f"Back after a quiet week: {_lines(lines)}, none in the last {BASELINE_DAYS} days."}
    return {**out, "decision": WATCH,
            "reason": f"Seen again: {_lines(lines)}, on {seen} of the last {BASELINE_DAYS} days."}


def notifies(d, is_new=False, reopened=False):
    """Whether a decision is worth a message: always to act; to watch only
    when the finding is new or back after a quiet week; never for noise."""
    if d["decision"] == ACT:
        return True
    if d["decision"] == WATCH:
        return is_new or (reopened and d["days_seen"] == 0)
    return False


def collapse(evidence):
    """Evidence entries of one account and log group that are the same error
    (the same signature) merged into one: counts summed, streams merged, the
    earliest first and latest last kept, the most frequent line as the
    message. Ids renumbered ("e1" = most lines). 10,000 lines that differ
    only by a request id are one entry."""
    merged = {}
    order = []
    for e in evidence:
        sig = fingerprint.signature(e.get("message", "")) or e.get("message", "")
        key = (e.get("account"), e.get("log_group"), sig)
        m = merged.get(key)
        if m is None:
            m = dict(e)
            m["streams"] = dict(e.get("streams") or {})
            m["queries"] = list(e.get("queries") or [])
            m["scope"] = list(e.get("scope") or [])
            m["variants"] = 1
            m["_top"] = e.get("count", 0)
            merged[key] = m
            order.append(key)
            continue
        m["count"] = m.get("count", 0) + e.get("count", 0)
        m["variants"] += 1
        for s, n in (e.get("streams") or {}).items():
            m["streams"][s] = m["streams"].get(s, 0) + n
        if e.get("first") and (m.get("first") is None or e["first"] < m["first"]):
            m["first"] = e["first"]
        if e.get("last") and (m.get("last") is None or e["last"] > m["last"]):
            m["last"] = e["last"]
        for q in e.get("queries") or []:
            if q not in m["queries"]:
                m["queries"].append(q)
        for g in e.get("scope") or []:
            if g not in m["scope"]:
                m["scope"].append(g)
        if e.get("count", 0) > m["_top"]:
            m["_top"] = e.get("count", 0)
            m["message"] = e.get("message", m.get("message"))
    out = [merged[k] for k in order]
    for m in out:
        m.pop("_top", None)
    out.sort(key=lambda e: (-e.get("count", 0), e.get("log_group", ""), e.get("message", "")))
    for i, e in enumerate(out, 1):
        e["id"] = f"e{i}"
    return out


def budget_key(now):
    return f"{BUDGET_PREFIX}{now.strftime('%Y-%m-%dT%H')}"


def take_model_call(table, team_id, now=None, limit=MODEL_CALLS_PER_HOUR):
    """Take one analysis call from the team's hourly budget: True when it
    was free. A refused call is counted on the same item, so the page can
    say the analyzer is counting only. A budget that can't be read allows
    the call (a lost count, never a lost analysis)."""
    now = now or datetime.now(timezone.utc)
    key = {"pk": team_id, "sk": budget_key(now)}
    ttl = int((now + timedelta(days=2)).timestamp())
    try:
        table.update_item(
            Key=key,
            UpdateExpression="ADD calls :one SET #ttl = :ttl",
            ConditionExpression="attribute_not_exists(calls) OR calls < :limit",
            ExpressionAttributeNames={"#ttl": "ttl"},
            ExpressionAttributeValues={":one": 1, ":limit": limit, ":ttl": ttl},
        )
        return True
    except Exception as e:  # noqa: BLE001
        code = getattr(e, "response", {}).get("Error", {}).get("Code", "")
        if code != "ConditionalCheckFailedException":
            return True
    try:
        table.update_item(
            Key=key,
            UpdateExpression="ADD refused :one SET #ttl = :ttl",
            ExpressionAttributeNames={"#ttl": "ttl"},
            ExpressionAttributeValues={":one": 1, ":ttl": ttl},
        )
    except Exception:  # noqa: BLE001
        pass
    return False
