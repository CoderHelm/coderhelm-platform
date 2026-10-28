"""
What makes two log findings the same error: the log lines, never the words a
model wrote about them.

A finding's identity used to be a hash of the error_pattern the model copied
from the evidence. The model picks a different slice of the same line from
one run to the next ("Server Action not found" one day, 'Failed to find
Server Action "7f3a..."' the next), so the same error got a new hash and a new
finding most days.

Here a line is reduced to its error signature:
- the exception type when the line names one (Python `KeyError:`, JS
  `TypeError:`, Java `java.lang.IllegalStateException`, a Lambda JSON error's
  `errorType`), or the HTTP method and status of an access-log line;
- the message with everything that changes between occurrences masked: uuids,
  request ids, hex ids and hashes, timestamps, IP addresses, emails, URLs'
  query strings, quoted values, numbers;
- the top stack frames (file and function, line numbers dropped) when the
  line carries a trace.

A finding's fingerprint is a hash of its account, its log group and the
signature most of its lines share. Every signature a finding's lines have had
is kept on the finding (`signature_hashes`), so a line that renders slightly
differently (another stream, another endpoint) still finds it.

Pure functions only; nothing here reads AWS or calls a model.
"""

import hashlib
import json
import re
from collections import Counter

# Most signatures kept on one finding (the first ones and the latest ones).
MAX_SIGNATURES = 24
# Normalized message length that takes part in a signature.
MESSAGE_CHARS = 160
# Stack frames that take part in a signature.
TOP_FRAMES = 3

_LEVELS = r"(?:TRACE|DEBUG|INFO|NOTICE|WARN|WARNING|ERROR|ERR|FATAL|CRITICAL|CRIT|PANIC|SEVERE)"

# Order matters: wider shapes first, so a uuid is not read as hex then numbers.
_MASKS = [
    # ISO and log timestamps: 2026-10-25T14:03:11.123Z, 2026-10-25 14:03:11,123
    (re.compile(r"\b\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(?::\d{2}(?:[.,]\d+)?)?(?:Z|[+-]\d{2}:?\d{2})?"), "<ts>"),
    (re.compile(r"\b\d{1,2}/[A-Za-z]{3}/\d{4}:\d{2}:\d{2}:\d{2}(?: [+-]\d{4})?"), "<ts>"),
    (re.compile(r"\b(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun),? \d{1,2} [A-Za-z]{3} \d{4} \d{2}:\d{2}:\d{2}(?: GMT| [+-]\d{4})?"), "<ts>"),
    (re.compile(r"\b\d{2}:\d{2}:\d{2}(?:[.,]\d+)?\b"), "<ts>"),
    # uuids and AWS request ids
    (re.compile(r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b"), "<id>"),
    # ARNs carry account ids and resource ids
    (re.compile(r"\barn:aws[a-z-]*:[^\s\"',)]+"), "<arn>"),
    # URLs: keep scheme, host and path shape, drop the query
    (re.compile(r"(https?://[^\s\"'?#]+)\?[^\s\"']*"), r"\1"),
    (re.compile(r"\b[\w.+-]+@[\w-]+\.[\w.-]+\b"), "<email>"),
    (re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}(?::\d+)?\b"), "<ip>"),
    # hex ids, hashes, commit shas, Next.js action ids (8+ hex chars with a digit)
    (re.compile(r"\b(?=[0-9a-fA-F]*\d)[0-9a-fA-F]{8,}\b"), "<hex>"),
    (re.compile(r"\b0x[0-9a-fA-F]+\b"), "<hex>"),
    # opaque tokens: 16+ word chars mixing letters and digits (session ids, keys)
    (re.compile(r"\b(?=\w*\d)(?=\w*[A-Za-z])[\w-]{16,}\b"), "<tok>"),
    # numbers, with units glued on (123ms, 4.5s, 512MB)
    (re.compile(r"(?<![A-Za-z])[-+]?\d+(?:[.,]\d+)*(?:[a-zA-Z%]{1,3}\b)?"), "<n>"),
]

# Quoted values that carry digits or run long are data (ids, paths, payloads);
# short words in quotes ('id', "email") say which error it is and stay.
_QUOTED = re.compile(r"\"[^\"\n]{1,200}\"|'[^'\n]{1,200}'|`[^`\n]{1,200}`")

_EXC_TYPE = re.compile(
    r"\b((?:[a-z_][\w]*\.)*[A-Z][A-Za-z0-9_]*(?:Error|Exception|Fault|Failure|Timeout|Rejection|Exit|Panic|Interrupt))\b"
)
_ACCESS = re.compile(r"\"?\b(GET|POST|PUT|DELETE|PATCH|HEAD|OPTIONS)\s+(\S+)(?:\s+HTTP/[\d.]+)?\"?\s+([1-5]\d{2})\b")
_STATUS = re.compile(r"(?i)\b(?:status(?:code)?|http)[\s=:]+([1-5]\d{2})\b")

# Stack frames: Python, JS/Node, Java/Kotlin, Go, Rust.
_FRAME_PATTERNS = [
    re.compile(r"File \"([^\"]+)\", line \d+, in ([\w<>.]+)"),
    re.compile(r"\bat (?:async )?([\w$.<>\[\] ]+?) \(([^():]+)(?::\d+){0,2}\)"),
    re.compile(r"\bat ([\w$.<>]+)\(([\w.]+)(?::\d+)?\)"),
    re.compile(r"\b([\w./-]+\.(?:go|rs)):\d+(?::\d+)?\b"),
]

# Lambda's own prefix: "<ts>\t<request id>\tERROR\t" / "[ERROR] <ts> <id> ".
_LAMBDA_PREFIX = re.compile(
    r"^(?:\[?" + _LEVELS + r"\]?\s+)?(?:<ts>\s+)?(?:<id>\s+)?(?:\[?" + _LEVELS + r"\]?\s+)?",
    re.IGNORECASE,
)


def _sha(text, n=16):
    return hashlib.sha256(text.encode("utf-8", "replace")).hexdigest()[:n]


def _quoted(m):
    q, inner = m.group(0)[0], m.group(0)[1:-1]
    if re.search(r"\d", inner) or len(inner) > 32:
        return f"{q}<v>{q}"
    return m.group(0)


def mask(text):
    """`text` with every value that changes between occurrences masked."""
    out = str(text or "")
    out = _MASKS[0][0].sub("<ts>", out)
    out = _QUOTED.sub(_quoted, out)
    for pattern, repl in _MASKS:
        out = pattern.sub(repl, out)
    return out


def _structured(message):
    """(type, message, stack) from a JSON object in the line (Lambda's
    `{"errorType":..,"errorMessage":..,"stack":[..]}`, pino/winston
    `{"level":..,"msg":..,"err":{..}}`), or None."""
    start = message.find("{")
    if start == -1:
        return None
    try:
        obj = json.loads(message[start:])
    except ValueError:
        return None
    if not isinstance(obj, dict):
        return None
    err = obj.get("err") or obj.get("error") if isinstance(obj.get("err") or obj.get("error"), dict) else {}
    etype = obj.get("errorType") or err.get("type") or err.get("name") or ""
    emsg = (
        obj.get("errorMessage") or err.get("message") or obj.get("msg")
        or obj.get("message") or ""
    )
    stack = obj.get("stack") or obj.get("trace") or err.get("stack") or ""
    if isinstance(stack, list):
        stack = "\n".join(str(s) for s in stack)
    if not (etype or emsg):
        return None
    return str(etype), str(emsg), str(stack)


def frames(text, limit=TOP_FRAMES):
    """The top stack frames in `text` as "file:function" (no line numbers).
    Python tracebacks list the innermost frame last, so theirs are reversed."""
    python = _FRAME_PATTERNS[0].findall(text or "")
    if python:
        out = [f"{f.rsplit('/', 1)[-1]}:{fn}" for f, fn in reversed(python)]
        return out[:limit]
    out = []
    for m in _FRAME_PATTERNS[1].finditer(text or ""):
        out.append(f"{m.group(2).rsplit('/', 1)[-1]}:{m.group(1).strip()}")
    if not out:
        for m in _FRAME_PATTERNS[2].finditer(text or ""):
            out.append(f"{m.group(2)}:{m.group(1)}")
    if not out:
        for m in _FRAME_PATTERNS[3].finditer(text or ""):
            out.append(m.group(1).rsplit("/", 1)[-1])
    # Frames inside node_modules or the runtime say little about the bug.
    own = [f for f in out if "node_modules" not in f and not f.startswith(("internal", "node:"))]
    return (own or out)[:limit]


def _path_shape(path):
    """An HTTP path with ids masked and the query dropped: /api/users/<n>."""
    path = path.split("?", 1)[0].split("#", 1)[0]
    parts = [mask(p) if p else p for p in path.split("/")]
    return "/".join(parts)[:80]


def signature(message):
    """The error signature of one log line: "type | normalized message |
    frames". Two occurrences of the same error give the same signature
    whatever ids, times or numbers they carry. "" for an empty line."""
    raw = str(message or "").strip()
    if not raw:
        return ""
    etype, body, stack = "", raw, raw

    s = _structured(raw)
    if s:
        etype, body, stack = s[0], s[1] or raw, (s[2] or raw)

    access = _ACCESS.search(raw)
    if access and not s:
        method, path, status = access.groups()
        return f"http {status} | {method} {_path_shape(path)} |"

    first = next((l.strip() for l in body.splitlines() if l.strip()), "")
    lines = [l.strip() for l in body.splitlines() if l.strip()]
    if not etype:
        # A traceback's last line names the exception ("KeyError: 'id'").
        for line in reversed(lines):
            m = _EXC_TYPE.search(line)
            if m and (line.startswith(m.group(1)) or ": " in line):
                etype, first = m.group(1), line
                break
    if not etype:
        m = _EXC_TYPE.search(first)
        if m:
            etype = m.group(1)

    text = mask(first)
    text = _LAMBDA_PREFIX.sub("", text.strip())
    text = re.sub(r"\s+", " ", text).strip().lower()[:MESSAGE_CHARS]
    if not etype:
        status = _STATUS.search(raw)
        if status:
            etype = f"http {status.group(1)}"
    return f"{etype.rsplit('.', 1)[-1]} | {text} | {','.join(frames(stack))}"


def signature_hash(sig):
    return _sha(sig) if sig else ""


def signatures(lines):
    """Distinct signatures of `lines` ([(message, weight)] or [message]),
    most common first (ties in text order, so the order is stable)."""
    counts = Counter()
    for line in lines or []:
        msg, weight = (line if isinstance(line, tuple) else (line, 1))
        sig = signature(msg)
        if sig:
            counts[sig] += max(int(weight or 1), 1)
    return [s for s, _ in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))]


def fingerprint(account_id, log_group, sig):
    """A finding's identity: account, log group and its main signature."""
    return _sha(f"{account_id}\n{log_group}\n{sig}")


def merge_hashes(old, new, limit=MAX_SIGNATURES):
    """Signature hashes kept on a finding: every old one, then new ones, at
    most `limit` (the first kept for identity, the latest for matching)."""
    out = [h for h in (old or []) if isinstance(h, str) and h]
    for h in new or []:
        if h and h not in out:
            out.append(h)
    if len(out) > limit:
        out = out[: limit // 2] + out[-(limit - limit // 2):]
    return out


def item_hashes(item):
    """Signature hashes of a stored finding: the ones it carries, else ones
    worked out from its stored sample lines and error pattern (findings saved
    before signatures existed)."""
    stored = item.get("signature_hashes")
    if isinstance(stored, (list, tuple, set)) and stored:
        return {str(h) for h in stored if h}
    lines = [s.get("message", "") for s in item.get("samples") or [] if isinstance(s, dict)]
    out = {signature_hash(s) for s in signatures(lines)}
    pattern = item.get("error_pattern")
    if isinstance(pattern, str) and pattern.strip():
        out.add(signature_hash(signature(pattern)))
    out.discard("")
    return out
