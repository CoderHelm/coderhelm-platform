"""The sandbox's network allowlist: a small HTTP proxy that only connects to
allowed domains.

The worker passes this file to each sandbox build (base64 in an environment
variable) with the team's allowlist. The buildspec starts it on
127.0.0.1:3128 and points HTTP_PROXY, HTTPS_PROXY and the package managers'
proxy settings at it before the repository's checks run. A request to a host
that isn't on the list gets 403 and a line in the build log
("CODERHELM_EGRESS_BLOCKED host"), which the run reports.

It is a guard for the tools that honor proxy settings (npm, pip, cargo, go,
curl, git over https and most others), not a firewall.

Python 3 standard library only: the build image has nothing else.
"""

import os
import select
import socket
import socketserver
import sys
import threading

LISTEN = ("127.0.0.1", 3128)
BUFFER = 65536


def parse_allowlist(text):
    """Comma or newline separated domains, lowercased, `*.` prefix dropped."""
    out = []
    for part in (text or "").replace("\n", ",").split(","):
        d = part.strip().lower()
        if d.startswith("*."):
            d = d[2:]
        d = d.rstrip(".")
        if d:
            out.append(d)
    return out


def host_allowed(allow, host):
    """A domain covers itself and its subdomains."""
    host = (host or "").strip().lower().rstrip(".")
    if host.startswith("[") or not host:
        return False
    host = host.split(":")[0]
    return any(host == d or host.endswith("." + d) for d in allow)


def target_of(request_line, headers):
    """(method, host, port) a proxied request goes to."""
    parts = request_line.split()
    if len(parts) < 2:
        return None
    method, target = parts[0].upper(), parts[1]
    if method == "CONNECT":
        host, _, port = target.rpartition(":")
        if not host:
            host, port = target, "443"
        return method, host.strip("[]"), int(port or 443)
    if target.startswith("http://"):
        rest = target[len("http://"):]
        hostport = rest.split("/", 1)[0]
        host, _, port = hostport.partition(":")
        return method, host, int(port or 80)
    host = headers.get("host", "")
    h, _, port = host.partition(":")
    return method, h, int(port or 80)


def read_head(conn):
    data = b""
    while b"\r\n\r\n" not in data and len(data) < BUFFER:
        chunk = conn.recv(4096)
        if not chunk:
            break
        data += chunk
    head, _, rest = data.partition(b"\r\n\r\n")
    lines = head.decode("latin-1").split("\r\n")
    headers = {}
    for line in lines[1:]:
        k, _, v = line.partition(":")
        headers[k.strip().lower()] = v.strip()
    return lines[0] if lines else "", headers, head, rest


def pipe(a, b):
    socks = [a, b]
    try:
        while True:
            ready, _, _ = select.select(socks, [], [], 120)
            if not ready:
                return
            for s in ready:
                data = s.recv(BUFFER)
                if not data:
                    return
                (b if s is a else a).sendall(data)
    except OSError:
        return


class Handler(socketserver.BaseRequestHandler):
    allow = []

    def handle(self):
        conn = self.request
        line, headers, head, rest = read_head(conn)
        t = target_of(line, headers)
        if not t:
            conn.sendall(b"HTTP/1.1 400 Bad Request\r\n\r\n")
            return
        method, host, port = t
        if not host_allowed(self.allow, host):
            print("CODERHELM_EGRESS_BLOCKED " + host, file=sys.stderr, flush=True)
            conn.sendall(
                b"HTTP/1.1 403 Forbidden\r\nContent-Type: text/plain\r\n\r\n"
                b"Blocked by the team's sandbox network allowlist.\n"
            )
            return
        try:
            up = socket.create_connection((host, port), timeout=30)
        except OSError:
            conn.sendall(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
            return
        with up:
            if method == "CONNECT":
                conn.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            else:
                up.sendall(head + b"\r\n\r\n" + rest)
            pipe(conn, up)


class Server(socketserver.ThreadingMixIn, socketserver.TCPServer):
    daemon_threads = True
    allow_reuse_address = True


def main():
    Handler.allow = parse_allowlist(os.environ.get("EGRESS_ALLOW", ""))
    with Server(LISTEN, Handler) as srv:
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        print("CODERHELM_EGRESS_READY", flush=True)
        threading.Event().wait()


if __name__ == "__main__":
    main()
