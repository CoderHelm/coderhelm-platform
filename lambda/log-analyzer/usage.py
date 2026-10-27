"""Model usage accounting for the log analyzer.

The Python twin of services/common/src/usage.rs: same price table
(model_prices.json, kept byte-identical by a Rust test), same analytics
attribute names, same periods. Each Anthropic response is ADDed once to the
team's month, all-time and day counters in one transaction.
"""

import json
import logging
import os
import time
import uuid
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
            (f"{prefix}#{key}#cache_write_5m", u["cache_write_5m"]),
            (f"{prefix}#{key}#cache_write_1h", u["cache_write_1h"]),
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


# How long a call's once-only marker is kept (as services/common/src/usage.rs).
CALL_MARKER_DAYS = 35
# Waits (seconds) before each retry of a failed counter write.
WRITE_RETRY_SECS = (0.1, 0.4, 1.6)


def _transaction(table, team_id, call_id, at, expr, names, values):
    """The marker (first, so its cancellation reason is index 0) and the
    month, all-time and day updates: one transaction, once per call id."""
    marker = {
        "Put": {
            "TableName": table,
            "Item": {
                "team_id": {"S": f"USAGE_CALL#{call_id}"},
                "period": {"S": "CALL"},
                "for_team": {"S": team_id},
                "expires_at": {"N": str(int(at.timestamp()) + CALL_MARKER_DAYS * 86400)},
            },
            "ConditionExpression": "attribute_not_exists(team_id)",
        }
    }
    return [marker] + [
        {
            "Update": {
                "TableName": table,
                "Key": {"team_id": {"S": team_id}, "period": {"S": p}},
                "UpdateExpression": expr,
                "ExpressionAttributeNames": names,
                "ExpressionAttributeValues": values,
            }
        }
        for p in periods(at)
    ]


def _already_counted(e):
    """A cancelled transaction whose marker already existed."""
    resp = getattr(e, "response", None) or {}
    reasons = resp.get("CancellationReasons") or []
    return bool(reasons) and (reasons[0] or {}).get("Code") == "ConditionalCheckFailed"


def record_call(client, table, team_id, feature, model_id, u, call_id=None, estimated=False,
                at=None, sleep=time.sleep, retry=WRITE_RETRY_SECS):
    """ADD one call (four token parts in `u`) to the team's counters, exactly
    once per `call_id`. A failed write is retried; if it still fails, the
    whole record is logged as an error (`usage_lost`) to be added by hand.
    Never raises."""
    if not table or not team_id or total_tokens(u) == 0:
        return
    call_id = call_id or uuid.uuid4().hex
    at = at or datetime.now(timezone.utc)
    expr, names, values = add_expression(feature, model_id, u)
    if estimated:
        expr += ", #x0 :x0"
        names["#x0"] = "estimated_calls"
        values[":x0"] = {"N": "1"}
    items = _transaction(table, team_id, call_id, at, expr, names, values)
    last = None
    for attempt in range(len(retry) + 1):
        try:
            client.transact_write_items(TransactItems=items)
            return
        except Exception as e:  # never fail the analysis over accounting
            if _already_counted(e):
                return
            last = e
        if attempt < len(retry):
            sleep(retry[attempt])
    logger.error(json.dumps({
        "event": "usage_lost", "team_id": team_id, "call_id": call_id, "feature": feature,
        "model": model_id, "estimated": estimated, "at_ms": int(at.timestamp() * 1000),
        "usage": u, "error": str(last),
    }))


def record_usage(client, table, team_id, feature, model_id, usage, call_id=None):
    """ADD one response's `usage` object to the team's counters. Never raises."""
    try:
        record_call(client, table, team_id, feature, model_id, call_usage(usage), call_id=call_id)
    except Exception as e:
        logger.warning(f"Couldn't record model usage for {team_id}: {e}")


# ── The team's monthly token limits, asked before every call ─────────────
#
# The Python twin of services/common/src/token_limits.rs: the three limits
# (new tokens, cache read, cache write; 0 or missing = none), or the old
# single max_tokens with counts_cache (team flag, else the
# TOKEN_LIMIT_COUNTS_CACHE env default). A failed read lets the call through.

SETTINGS_TABLE = os.environ.get("SETTINGS_TABLE_NAME", "")
_NEW_FIELDS = (
    ("max_new_tokens", "new tokens"),
    ("max_cache_read_tokens", "cache read tokens"),
    ("max_cache_write_tokens", "cache write tokens"),
)


class LimitReached(Exception):
    """The team reached a monthly token limit: the call was not sent."""


def _n(item, k):
    try:
        return max(0, int(float((item.get(k) or {}).get("N", 0))))
    except (TypeError, ValueError):
        return 0


def limit_reason(limits_item, month_item, env_counts_cache=None):
    """Why a call is refused, or None: the first limit the month has reached."""
    item = limits_item or {}
    month = month_item or {}
    t_in, t_out = _n(month, "total_tokens_in"), _n(month, "total_tokens_out")
    read, write = _n(month, "cache_read_tokens"), _n(month, "cache_write_tokens")
    used_of = {
        "max_new_tokens": t_in + t_out,
        "max_cache_read_tokens": read,
        "max_cache_write_tokens": write,
    }
    if any(f in item for f, _ in _NEW_FIELDS):
        checks = [(label, _n(item, f), used_of[f]) for f, label in _NEW_FIELDS]
    else:
        flag = (item.get("counts_cache") or {}).get("BOOL")
        if flag is None:
            env = os.environ.get("TOKEN_LIMIT_COUNTS_CACHE") if env_counts_cache is None else env_counts_cache
            flag = (env or "").strip().lower() == "true"
        used = t_in + t_out + (read + write if flag else 0)
        checks = [("token", _n(item, "max_tokens"), used)]
    for label, mx, used in checks:
        if mx > 0 and used >= mx:
            return (f"Monthly {label} limit reached ({used} of {mx}). New model calls are "
                    "stopped until the month turns or an admin raises the limit in Settings, Token Limit.")
    return None


def admit(client, table, team_id, settings_table=None, now=None):
    """Raise LimitReached when the team has reached a monthly limit. Asked
    before every attempt, so a retry loop stops at the limit too."""
    settings_table = SETTINGS_TABLE if settings_table is None else settings_table
    if not settings_table or not table or not team_id:
        return
    try:
        limits = client.get_item(
            TableName=settings_table,
            Key={"pk": {"S": team_id}, "sk": {"S": "SETTINGS#TOKEN_LIMIT"}},
        ).get("Item")
        if not limits:
            return
        month = (now or datetime.now(timezone.utc)).strftime("%Y-%m")
        totals = client.get_item(
            TableName=table, Key={"team_id": {"S": team_id}, "period": {"S": month}},
        ).get("Item")
    except Exception as e:  # an outage must not stop every team's analysis
        logger.warning(f"Token limits not read for {team_id}; the call goes ahead: {e}")
        return
    reason = limit_reason(limits, totals)
    if reason:
        logger.warning(f"Model call refused for {team_id}: {reason}")
        raise LimitReached(reason)


# ── The one Anthropic call, streamed and metered per attempt ─────────────

# Bytes of streamed text, thinking or tool-input JSON per output token, for
# a reply that broke off before its final count (same ratio and reasoning as
# services/common/src/model_stream.rs: ~3.5 chars per token on prose, fewer
# on code; 3 errs toward counting more, never less).
BYTES_PER_OUTPUT_TOKEN = 3
# Request bytes per input token, only when no message_start arrived.
BYTES_PER_INPUT_TOKEN = 4


class Accumulator:
    """One streamed reply, rebuilt event by event."""

    def __init__(self):
        self.started = False
        self.finished = False
        self.blocks = {}
        self.raw_json = {}
        self.usage = {}
        self.delta_output = None
        self.received_bytes = 0
        self.stop_reason = None
        self.error = None

    def feed(self, ev):
        t = ev.get("type")
        if t == "message_start":
            self.started = True
            self.usage = dict((ev.get("message") or {}).get("usage") or {})
        elif t == "content_block_start":
            self.blocks[ev.get("index", 0)] = dict(ev.get("content_block") or {})
        elif t == "content_block_delta":
            i, d = ev.get("index", 0), ev.get("delta") or {}
            b = self.blocks.setdefault(i, {})
            dt = d.get("type")
            for kind, field in (("text_delta", "text"), ("thinking_delta", "thinking")):
                if dt == kind:
                    piece = d.get(field, "")
                    b[field] = b.get(field, "") + piece
                    self.received_bytes += len(piece.encode("utf-8"))
            if dt == "signature_delta":
                b["signature"] = d.get("signature")
            if dt == "input_json_delta":
                piece = d.get("partial_json", "")
                self.raw_json[i] = self.raw_json.get(i, "") + piece
                self.received_bytes += len(piece.encode("utf-8"))
        elif t == "content_block_stop":
            i = ev.get("index", 0)
            if self.blocks.get(i, {}).get("type") == "tool_use" and i in self.raw_json:
                try:
                    self.blocks[i]["input"] = json.loads(self.raw_json[i])
                except ValueError:
                    self.blocks[i]["input"] = {}
        elif t == "message_delta":
            sr = (ev.get("delta") or {}).get("stop_reason")
            if sr:
                self.stop_reason = sr
                self.finished = True
            for k, v in (ev.get("usage") or {}).items():
                if v is not None:
                    self.usage[k] = v
            if isinstance((ev.get("usage") or {}).get("output_tokens"), int):
                self.delta_output = ev["usage"]["output_tokens"]
        elif t == "error":
            self.error = ev.get("error") or {}

    def call(self, request_bytes):
        """(four token parts, estimated): exact with the final message_delta;
        otherwise message_start's input and the output estimated from the
        bytes received (or the last cumulative count, whichever is larger)."""
        u = call_usage(self.usage)
        if self.finished:
            return u, False
        estimate = -(-self.received_bytes // BYTES_PER_OUTPUT_TOKEN)
        u["output"] = max(self.delta_output or 0, estimate)
        if not self.started:
            u = {"uncached_input": -(-request_bytes // BYTES_PER_INPUT_TOKEN), "cache_read": 0,
                 "cache_write_5m": 0, "cache_write_1h": 0, "output": u["output"]}
        return u, True

    def message(self):
        return {
            "content": [self.blocks[i] for i in sorted(self.blocks)],
            "stop_reason": self.stop_reason,
            "usage": self.usage,
        }


def is_transient_status(code):
    """True for statuses that may succeed when sent again: every 5xx
    (Cloudflare's 520-524 included), 408, 429 and 529."""
    return code in (408, 429) or 500 <= code <= 599


class StreamBroke(Exception):
    """The reply ended before its final usage (counted, estimated)."""


def call_messages(api_key, body, model_id, client, table, team_id, feature,
                  backoff=(2, 4, 8, 16, 32), sleep=time.sleep, opener=None, timeout=120):
    """One Anthropic Messages call, streamed, every attempt recorded on the
    team's counters as its own call (a broken reply with its output
    estimated). Retries transient statuses, network errors and broken
    replies after each wait in `backoff`; the last failure is raised.
    Returns the rebuilt message (content, stop_reason, usage)."""
    import random
    import urllib.error
    from urllib.request import Request, urlopen

    opener = opener or urlopen
    data = json.dumps(dict(body, stream=True)).encode("utf-8")
    for attempt in range(len(backoff) + 1):
        last = attempt == len(backoff)
        admit(client, table, team_id)
        req = Request(
            "https://api.anthropic.com/v1/messages",
            data=data,
            headers={"Content-Type": "application/json", "x-api-key": api_key,
                     "anthropic-version": "2023-06-01"},
            method="POST",
        )
        try:
            resp = opener(req, timeout=timeout)
        except urllib.error.HTTPError as e:
            if last or not is_transient_status(e.code):
                raise
            logger.warning(f"Anthropic transient status {e.code}, retrying (attempt {attempt + 1})")
            sleep(backoff[attempt] * random.uniform(0.75, 1.25))
            continue
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            if last:
                raise
            logger.warning(f"Anthropic request failed ({e}), retrying (attempt {attempt + 1})")
            sleep(backoff[attempt] * random.uniform(0.75, 1.25))
            continue
        acc = Accumulator()
        headers = getattr(resp, "headers", None)
        call_id = headers.get("request-id") if headers is not None else None
        broke = None
        try:
            with resp:
                for raw in resp:
                    line = raw.decode("utf-8", "replace").strip()
                    if line.startswith("data:"):
                        try:
                            acc.feed(json.loads(line[5:].strip()))
                        except ValueError:
                            continue
                    if acc.error is not None:
                        break
        except Exception as e:  # timeout or cut connection mid-reply
            broke = e
        u, estimated = acc.call(len(data))
        _record_attempt(client, table, team_id, feature, model_id, u, call_id, estimated)
        if not estimated and acc.error is None:
            return acc.message()
        err = StreamBroke(f"reply broke off: {broke or acc.error or 'no final usage'}")
        if last:
            raise err
        logger.warning(f"{err}, retrying (attempt {attempt + 1})")
        sleep(backoff[attempt] * random.uniform(0.75, 1.25))
    raise StreamBroke("unreachable")


# ── Amazon Bedrock or Anthropic API keys ─────────────────────────────────
#
# The deploy config sets the first-boot default (MODEL_PROVIDER,
# BEDROCK_INFERENCE_PREFIX, BEDROCK_MODEL_IDS); an install admin can change
# it in Settings, System, Models, stored on the settings table (pk SYSTEM,
# sk MODELS). The handler sets RUNTIME_LOADER to read that record; it is read
# again at most every RUNTIME_SECONDS, like the Rust services
# (services/common/src/model_provider.rs).

RUNTIME_SECONDS = 30
RUNTIME_LOADER = None
_runtime = {"at": None, "value": None}
_PREFIX_RE = __import__("re").compile(r"^([a-z]{2,6}\.)?$")


def deploy_settings(env=None):
    env = os.environ if env is None else env
    try:
        ids = json.loads(env.get("BEDROCK_MODEL_IDS") or "{}")
    except ValueError:
        ids = {}
    return {
        "provider": "bedrock" if (env.get("MODEL_PROVIDER") or "").strip().lower() == "bedrock" else "anthropic",
        "inference_prefix": env.get("BEDROCK_INFERENCE_PREFIX", "us."),
        "model_ids": ids if isinstance(ids, dict) else {},
    }


def resolve_settings(stored, default):
    """A stored choice when it reads and is valid, else the deploy config's."""
    if not isinstance(stored, dict):
        return default
    provider = str(stored.get("provider") or "").strip().lower()
    if provider not in ("anthropic", "bedrock"):
        return default
    prefix = str(stored.get("inference_prefix", default["inference_prefix"])).strip()
    ids = stored.get("model_ids") or {}
    if not _PREFIX_RE.match(prefix) or not isinstance(ids, dict):
        return default
    if any(not isinstance(v, str) or not ("anthropic." in v or v.startswith("arn:aws")) for v in ids.values()):
        return default
    return {"provider": provider, "inference_prefix": prefix, "model_ids": ids}


def provider_settings(env=None, clock=time.monotonic):
    """The setting in force: the stored choice (read at most every 30 s), else the deploy config."""
    default = deploy_settings(env)
    if RUNTIME_LOADER is None:
        return default
    now = clock()
    if _runtime["at"] is None or now - _runtime["at"] >= RUNTIME_SECONDS:
        try:
            _runtime["value"] = RUNTIME_LOADER()
        except Exception as e:  # keep the last one; try again later
            logger.warning(f"Model provider setting not read: {e}")
        _runtime["at"] = now
    return resolve_settings(_runtime["value"], default)


def is_bedrock(env=None):
    """True when the install calls Claude through Amazon Bedrock now."""
    return provider_settings(env)["provider"] == "bedrock"


def bedrock_model_id(model, env=None):
    """The Bedrock id for a model name (same rule as
    services/common/src/model_provider.rs): an explicit model id override, an
    id that already is a Bedrock id, else <prefix>anthropic.<model>."""
    s = provider_settings(env)
    mapping = s["model_ids"]
    if model in mapping:
        return mapping[model]
    if "anthropic." in model or model.startswith("arn:"):
        return model
    name = BEDROCK_NAMES.get(model, model)
    return f"{s['inference_prefix']}anthropic.{name}"


# Models whose Bedrock id isn't just their name (the release manifest's
# bedrock_names, services/common/release_models.json; a test holds them equal).
BEDROCK_NAMES = {
    "claude-haiku-4-5": "claude-haiku-4-5-20251001-v1:0",
    "claude-haiku-4-5-20251001": "claude-haiku-4-5-20251001-v1:0",
}


def bedrock_body(body):
    """A Messages body as Bedrock takes it: no model or stream field, its
    version marker added."""
    b = {k: v for k, v in body.items() if k not in ("model", "stream")}
    b["anthropic_version"] = "bedrock-2023-05-31"
    return b


def call_messages_bedrock(body, model_id, client, table, team_id, feature, bedrock=None,
                          backoff=(2, 4, 8, 16, 32), sleep=time.sleep):
    """The same call as call_messages, through Bedrock's streaming API with
    the Lambda's own role (no key). Every attempt is recorded the same way."""
    import random

    if bedrock is None:
        import boto3
        bedrock = boto3.client("bedrock-runtime")
    data = json.dumps(bedrock_body(body)).encode("utf-8")
    for attempt in range(len(backoff) + 1):
        last = attempt == len(backoff)
        admit(client, table, team_id)
        try:
            resp = bedrock.invoke_model_with_response_stream(
                modelId=bedrock_model_id(model_id), body=data,
                contentType="application/json", accept="application/json",
            )
        except Exception as e:
            status = getattr(e, "response", {}).get("ResponseMetadata", {}).get("HTTPStatusCode", 0) \
                if hasattr(e, "response") else 0
            if last or (status and not is_transient_status(status)):
                raise
            logger.warning(f"Bedrock call failed ({e}), retrying (attempt {attempt + 1})")
            sleep(backoff[attempt] * random.uniform(0.75, 1.25))
            continue
        acc = Accumulator()
        call_id = (resp.get("ResponseMetadata") or {}).get("RequestId")
        broke = None
        try:
            for event in resp["body"]:
                chunk = (event or {}).get("chunk")
                if not chunk:
                    continue
                try:
                    acc.feed(json.loads(chunk["bytes"]))
                except ValueError:
                    continue
                if acc.error is not None:
                    break
        except Exception as e:  # the stream broke mid-reply
            broke = e
        u, estimated = acc.call(len(data))
        _record_attempt(client, table, team_id, feature, model_id, u, call_id, estimated)
        if not estimated and acc.error is None:
            return acc.message()
        err = StreamBroke(f"reply broke off: {broke or acc.error or 'no final usage'}")
        if last:
            raise err
        logger.warning(f"{err}, retrying (attempt {attempt + 1})")
        sleep(backoff[attempt] * random.uniform(0.75, 1.25))
    raise StreamBroke("unreachable")


def _record_attempt(client, table, team_id, feature, model_id, u, call_id, estimated):
    if estimated:
        logger.warning(f"Anthropic reply broke off; recording {u} with the output estimated")
    record_call(client, table, team_id, feature, model_id, u, call_id=call_id, estimated=estimated)
