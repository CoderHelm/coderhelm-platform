"""Model usage accounting for the log analyzer.

The Python twin of services/common/src/usage.rs: same price table
(model_prices.json, kept byte-identical by a Rust test), same analytics
attribute names, same periods. Each Anthropic response is ADDed once to the
team's month, all-time and day counters in one transaction.
"""

import json
import logging
import os
from datetime import datetime, timezone

logger = logging.getLogger()

FEATURE_LOG_ANALYSIS = "log_analysis"

with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "model_prices.json")) as _f:
    PRICES = json.load(_f)["models"]

_warned = set()


def normalize_model(model_id):
    m = (model_id or "").strip().lower()
    for sep in ("[", "@"):
        if sep in m:
            m = m[: m.index(sep)]
    for p in ("global.", "us.", "eu.", "apac.", "jp.", "au."):
        if m.startswith(p):
            m = m[len(p):]
    if m.startswith("anthropic."):
        m = m[len("anthropic."):]
    if "-v1" in m:
        m = m[: m.index("-v1")]
    if ":" in m:
        m = m[: m.index(":")]
    head, _, tail = m.rpartition("-")
    if head and len(tail) == 8 and tail.isdigit():
        m = head
    return m


def most_expensive_rates():
    return max(PRICES.values(), key=lambda r: r["input"] + r["output"])


def rates_for(model_id):
    m = normalize_model(model_id)
    if m in PRICES:
        return PRICES[m], True
    if m not in _warned:
        _warned.add(m)
        logger.warning(f"Model {model_id} missing from the price table; costing it at the most expensive rate")
    return most_expensive_rates(), False


def call_usage(usage, ttl="5m"):
    """Four token parts from a Messages API usage object."""
    usage = usage or {}
    n = lambda d, k: int((d or {}).get(k) or 0)
    write_total = n(usage, "cache_creation_input_tokens")
    cc = usage.get("cache_creation")
    if isinstance(cc, dict):
        w5, w1 = n(cc, "ephemeral_5m_input_tokens"), n(cc, "ephemeral_1h_input_tokens")
        rest = max(write_total - w5 - w1, 0)
        if ttl == "1h":
            w1 += rest
        else:
            w5 += rest
    elif ttl == "1h":
        w5, w1 = 0, write_total
    else:
        w5, w1 = write_total, 0
    return {
        "uncached_input": n(usage, "input_tokens"),
        "cache_read": n(usage, "cache_read_input_tokens"),
        "cache_write_5m": w5,
        "cache_write_1h": w1,
        "output": n(usage, "output_tokens"),
    }


def total_tokens(u):
    return u["uncached_input"] + u["cache_read"] + u["cache_write_5m"] + u["cache_write_1h"] + u["output"]


def call_cost(model_id, u):
    r, _ = rates_for(model_id)
    prompt = u["uncached_input"] + u["cache_read"] + u["cache_write_5m"] + u["cache_write_1h"]
    lp = r.get("long_prompt")
    if lp and prompt > lp["over_tokens"]:
        r = lp
    return (
        u["uncached_input"] * r["input"]
        + u["output"] * r["output"]
        + u["cache_read"] * r["cache_read"]
        + u["cache_write_5m"] * r["cache_write_5m"]
        + u["cache_write_1h"] * r["cache_write_1h"]
    ) / 1_000_000


def _key_part(s):
    k = "".join(c for c in (s or "").lower() if c.isascii() and (c.isalnum() or c in "-_."))
    return k or "unknown"


def add_expression(feature, model_id, u):
    cost = "%.9f" % call_cost(model_id, u)
    cw = u["cache_write_5m"] + u["cache_write_1h"]
    adds = [
        ("total_tokens_in", u["uncached_input"]),
        ("tokens_in_uncached", u["uncached_input"]),
        ("cache_read_tokens", u["cache_read"]),
        ("cache_write_tokens", cw),
        ("cache_write_5m_tokens", u["cache_write_5m"]),
        ("cache_write_1h_tokens", u["cache_write_1h"]),
        ("total_tokens_out", u["output"]),
        ("total_tokens", total_tokens(u)),
        ("total_cost_usd", cost),
        ("model_calls", 1),
    ]
    for prefix, key in (("model", _key_part(normalize_model(model_id))), ("feature", _key_part(feature))):
        adds += [
            (f"{prefix}#{key}#tokens_in", u["uncached_input"]),
            (f"{prefix}#{key}#cache_read", u["cache_read"]),
            (f"{prefix}#{key}#cache_write", cw),
            (f"{prefix}#{key}#tokens_out", u["output"]),
            (f"{prefix}#{key}#cost_usd", cost),
            (f"{prefix}#{key}#calls", 1),
        ]
    names, values, parts = {}, {}, []
    for i, (attr, v) in enumerate(adds):
        names[f"#a{i}"] = attr
        values[f":v{i}"] = {"N": str(v)}
        parts.append(f"#a{i} :v{i}")
    return "ADD " + ", ".join(parts), names, values


def periods(now=None):
    now = now or datetime.now(timezone.utc)
    return [now.strftime("%Y-%m"), "ALL_TIME", "DAY#" + now.strftime("%Y-%m-%d")]


def record_usage(client, table, team_id, feature, model_id, usage):
    """ADD one response's usage to the team's counters. Never raises."""
    try:
        u = call_usage(usage)
        if not table or not team_id or total_tokens(u) == 0:
            return
        expr, names, values = add_expression(feature, model_id, u)
        client.transact_write_items(
            TransactItems=[
                {
                    "Update": {
                        "TableName": table,
                        "Key": {"team_id": {"S": team_id}, "period": {"S": p}},
                        "UpdateExpression": expr,
                        "ExpressionAttributeNames": names,
                        "ExpressionAttributeValues": values,
                    }
                }
                for p in periods()
            ]
        )
    except Exception as e:  # never fail the analysis over accounting
        logger.warning(f"Couldn't record model usage for {team_id}: {e}")
