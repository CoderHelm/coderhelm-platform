#!/usr/bin/env python3
"""A stdio MCP server, run inside a skill run's sandbox, reached one request at a time.

The worker can't hold a pipe open to a build, so requests and answers go
through numbered slots: slot N's request is read from `slots[N].req` and its
answer written to `slots[N].resp` (presigned S3 URLs in the sandbox, plain
file paths in tests). The worker writes a request, then waits for its answer.

Each request is JSON:
  {"op": "list"}                                   the server's tools
  {"op": "call", "name": ..., "arguments": {...}}  one tool call
  {"op": "stop"}                                   end the session
Each answer is {"ok": true, "result": ...} or {"ok": false, "error": ...}.

The server runs from `server/` with `command`, after `install` (npm ci). Its
AWS keys (short-lived, the team's read-only role) are read once from the
one-time secret and passed only into the server's environment; they are
taken out of every answer. Standard library only.
"""
import json
import os
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

POLL_SECS = 1.0
CALL_SECS = 600
PROTOCOL = "2025-06-18"


def fetch(where):
    """The body at `where`, or None when nothing is there yet."""
    if where.startswith("http"):
        try:
            with urllib.request.urlopen(urllib.request.Request(where), timeout=30) as r:
                return r.read().decode("utf-8")
        except urllib.error.HTTPError as e:
            if e.code in (403, 404):
                return None
            raise
    if not os.path.exists(where):
        return None
    with open(where, encoding="utf-8") as f:
        text = f.read()
    return text or None


def put(where, body):
    data = body.encode("utf-8")
    if where.startswith("http"):
        req = urllib.request.Request(where, data=data, method="PUT")
        req.add_header("Content-Type", "application/json")
        for attempt in range(3):
            try:
                urllib.request.urlopen(req, timeout=30).read()
                return
            except Exception:
                if attempt == 2:
                    raise
                time.sleep(2)
    tmp = where + ".part"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(body)
    os.replace(tmp, where)


class Server:
    """One stdio MCP server: JSON-RPC lines on its stdin and stdout."""

    def __init__(self, cmd, cwd, env):
        self.proc = subprocess.Popen(
            cmd, cwd=cwd, env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True, bufsize=1,
        )
        self.next_id = 1
        self.lines = []
        self.err = []
        self.cond = threading.Condition()
        threading.Thread(target=self._read, daemon=True).start()
        threading.Thread(target=self._read_err, daemon=True).start()

    def _read(self):
        for line in self.proc.stdout:
            with self.cond:
                self.lines.append(line)
                self.cond.notify_all()
        with self.cond:
            self.cond.notify_all()

    def _read_err(self):
        for line in self.proc.stderr:
            self.err.append(line.rstrip())
            del self.err[:-40]

    def stderr_tail(self):
        return "\n".join(self.err[-10:])

    def notify(self, method, params=None):
        msg = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            msg["params"] = params
        self.proc.stdin.write(json.dumps(msg) + "\n")
        self.proc.stdin.flush()

    def request(self, method, params, timeout=CALL_SECS):
        rid = self.next_id
        self.next_id += 1
        self.proc.stdin.write(json.dumps({"jsonrpc": "2.0", "id": rid, "method": method, "params": params}) + "\n")
        self.proc.stdin.flush()
        end = time.time() + timeout
        with self.cond:
            while True:
                for i, line in enumerate(self.lines):
                    try:
                        msg = json.loads(line)
                    except ValueError:
                        continue
                    if msg.get("id") == rid:
                        del self.lines[i]
                        if "error" in msg:
                            raise RuntimeError(msg["error"].get("message") or json.dumps(msg["error"]))
                        return msg.get("result")
                if self.proc.poll() is not None:
                    raise RuntimeError("the server exited: " + self.stderr_tail())
                left = end - time.time()
                if left <= 0:
                    raise RuntimeError(f"no answer in {timeout} seconds")
                self.cond.wait(min(left, 1.0))

    def start(self):
        self.request("initialize", {
            "protocolVersion": PROTOCOL,
            "capabilities": {},
            "clientInfo": {"name": "coderhelm-skill-run", "version": "1"},
        }, timeout=120)
        self.notify("notifications/initialized")

    def tools(self):
        out, cursor = [], None
        while True:
            res = self.request("tools/list", {"cursor": cursor} if cursor else {})
            out.extend(res.get("tools") or [])
            cursor = res.get("nextCursor")
            if not cursor:
                return out

    def call(self, name, arguments):
        res = self.request("tools/call", {"name": name, "arguments": arguments or {}})
        text = "\n".join(
            c.get("text", "") for c in (res.get("content") or []) if c.get("type") == "text"
        )
        return {"text": text, "is_error": bool(res.get("isError"))}

    def stop(self):
        try:
            self.proc.stdin.close()
            self.proc.wait(timeout=5)
        except Exception:
            self.proc.kill()


def redactor(secrets):
    secrets = [s for s in secrets if s and len(s) >= 8]

    def clean(text):
        for s in secrets:
            text = text.replace(s, "[redacted]")
        return text
    return clean


def main(manifest_path):
    with open(manifest_path, encoding="utf-8") as f:
        m = json.load(f)
    here = os.path.dirname(os.path.abspath(manifest_path))
    server_dir = os.path.join(here, m.get("cwd") or "server")
    env = {k: v for k, v in os.environ.items() if not k.startswith(("SANDBOX_", "AWS_", "MCP_"))}
    env.update(m.get("env") or {})
    secrets = []
    secret_at = os.environ.pop("SANDBOX_SECRET_URL", "") or m.get("secret") or ""
    if secret_at:
        creds = json.loads(fetch(secret_at) or "{}")
        for k in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_REGION", "AWS_DEFAULT_REGION"):
            if creds.get(k):
                env[k] = creds[k]
        secrets = [creds.get("AWS_SECRET_ACCESS_KEY", ""), creds.get("AWS_SESSION_TOKEN", "")]
    clean = redactor(secrets)
    slots = m["slots"]
    idle = float(m.get("idle_secs", 900))

    def answer(i, ok, value):
        body = {"ok": ok, "result": value} if ok else {"ok": False, "error": value}
        put(slots[i]["resp"], clean(json.dumps(body)))

    print("===CODERHELM_MCP_INSTALL===", flush=True)
    install = m.get("install") or []
    server = None
    start_error = None
    if install:
        r = subprocess.run(install, cwd=server_dir, env=env, capture_output=True, text=True)
        if r.returncode != 0:
            start_error = "installing the server failed: " + (r.stderr or r.stdout)[-1500:]
    if start_error is None:
        try:
            server = Server(m["command"], server_dir, env)
            server.start()
        except Exception as e:
            start_error = f"the server didn't start: {e}"
    print("===CODERHELM_MCP_READY===" if start_error is None else "===CODERHELM_MCP_FAILED===", flush=True)

    i = 0
    waited = 0.0
    while i < len(slots):
        raw = fetch(slots[i]["req"])
        if raw is None:
            time.sleep(POLL_SECS)
            waited += POLL_SECS
            if waited > idle:
                break
            continue
        waited = 0.0
        try:
            req = json.loads(raw)
        except ValueError:
            answer(i, False, "the request isn't JSON")
            i += 1
            continue
        op = req.get("op")
        if op == "stop":
            answer(i, True, "stopped")
            break
        if start_error is not None:
            answer(i, False, start_error)
        else:
            try:
                if op == "list":
                    answer(i, True, server.tools())
                elif op == "call":
                    answer(i, True, server.call(req.get("name", ""), req.get("arguments") or {}))
                else:
                    answer(i, False, f"unknown op {op!r}")
            except Exception as e:
                answer(i, False, clean(str(e)))
        i += 1
    if server is not None:
        server.stop()
    print("===CODERHELM_MCP_END===", flush=True)


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "manifest.json")
