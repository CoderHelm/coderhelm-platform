"""CoderHelm private network: carries a data source's read-only lookups to a
service that is reachable only inside one VPC (an OpenSearch domain with a
VPC endpoint, a search cluster on private addresses).

The install deploys one of these per private network into subnets an admin
chose (infra/lib/native/parent.ts, PrivateNetwork1..3), with its own
security group: outbound tcp 443 and 9200 only, nothing inbound. Only the
install's gateway and worker may invoke it.

It decides nothing about what is read. The gateway and the worker build
every request after their read-only checks (index allowlist, no scripts,
query limits, redaction of the answer); this function only carries it, and
refuses anything that isn't:

- HTTPS to a host name (never an address typed in), on port 443 or 9200;
- a name that resolves only to private addresses (10/8, 172.16/12,
  192.168/16, fc00::/7): public names go the normal way, and link-local,
  loopback and metadata addresses are never reached;
- GET or POST with a JSON body, no redirects followed, the certificate
  checked against the name, 25 seconds at most, 2 MB of answer at most.

Events:
  {"action": "send", "url", "method", "headers", "body"} -> {"status", "body"} or {"error": {...}}
  {"action": "check", "url"} -> {"hops": [{"hop", "ok", "detail"}]}: dns, tcp, tls, each named.
Standard library only, so the code is the template's own (no package).
"""

import http.client
import ipaddress
import json
import socket
import ssl
import time
from urllib.parse import urlsplit

TIMEOUT = 25
MAX_BYTES = 2 * 1024 * 1024
PORTS = (443, 9200)
METHODS = ("GET", "POST")
# Headers a lookup may send; anything else is dropped.
HEADERS = ("authorization", "accept", "content-type", "x-api-key")
USER_AGENT = "Coderhelm (read-only lookups, private network)"


class Refused(Exception):
    def __init__(self, hop, detail):
        super().__init__(detail)
        self.hop = hop
        self.detail = detail


def private_ip(ip):
    """True only for addresses inside a private network a VPC uses."""
    a = ipaddress.ip_address(ip)
    if a.version == 6 and a.ipv4_mapped:
        a = a.ipv4_mapped
    if a.is_loopback or a.is_link_local or a.is_multicast or a.is_unspecified or a.is_reserved:
        return False
    if a.version == 4:
        return any(a in ipaddress.ip_network(n) for n in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"))
    return a in ipaddress.ip_network("fc00::/7")


def target(url):
    """(host, port, path) of an address this function may call, or Refused."""
    if not isinstance(url, str) or len(url) > 4000:
        raise Refused("address", "no address was given")
    u = urlsplit(url.strip())
    if u.scheme != "https":
        raise Refused("address", "only https addresses are carried")
    if u.username or u.password:
        raise Refused("address", "the address must not hold a user or password")
    host = (u.hostname or "").lower()
    if not host or "." not in host:
        raise Refused("address", "the address has no host name")
    try:
        ipaddress.ip_address(host)
        raise Refused("address", "use the service's host name, not an address")
    except ValueError:
        pass
    port = u.port or 443
    if port not in PORTS:
        raise Refused("address", f"port {port} isn't carried (443 or 9200 only)")
    path = (u.path or "/") + (f"?{u.query}" if u.query else "")
    return host, port, path


def resolve(host, port):
    """The name's addresses: every one must be private, or Refused (hop dns)."""
    try:
        infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except socket.gaierror as e:
        raise Refused("dns", f"{host} doesn't resolve inside this network ({e.strerror or e})") from None
    addrs = sorted({i[4][0] for i in infos})
    if not addrs:
        raise Refused("dns", f"{host} has no address")
    public = [a for a in addrs if not private_ip(a)]
    if public:
        raise Refused("dns", f"{host} resolves to {', '.join(public[:3])}, which isn't a private address: it is reached over the internet, not through a private network")
    return addrs


def connect(ip, port):
    try:
        return socket.create_connection((ip, port), timeout=TIMEOUT)
    except socket.timeout:
        raise Refused("tcp", f"no answer on {ip}:{port} within {TIMEOUT} seconds: a security group or network ACL drops it") from None
    except OSError as e:
        raise Refused("tcp", f"{ip}:{port} refused the connection ({e.strerror or e})") from None


def tls(sock, host):
    ctx = ssl.create_default_context()
    try:
        return ctx.wrap_socket(sock, server_hostname=host)
    except ssl.SSLCertVerificationError as e:
        sock.close()
        raise Refused("tls", f"the certificate isn't valid for {host} ({e.verify_message})") from None
    except (ssl.SSLError, OSError) as e:
        sock.close()
        raise Refused("tls", f"the secure connection failed ({e})") from None


class Pinned(http.client.HTTPSConnection):
    """An HTTPS connection to the address already checked (no second lookup)."""

    def __init__(self, host, port, ip):
        super().__init__(host, port, timeout=TIMEOUT)
        self._ip = ip

    def connect(self):
        self.sock = tls(connect(self._ip, self.port), self.host)


def send(event):
    host, port, path = target(event.get("url"))
    method = str(event.get("method") or "GET").upper()
    if method not in METHODS:
        raise Refused("address", f"{method} isn't carried (GET or POST only)")
    ip = resolve(host, port)[0]
    headers = {k: str(v) for k, v in (event.get("headers") or {}).items() if str(k).lower() in HEADERS}
    headers["User-Agent"] = USER_AGENT
    body = None
    if event.get("body") is not None:
        body = json.dumps(event["body"]).encode()
        headers["Content-Type"] = "application/json"
    conn = Pinned(host, port, ip)
    try:
        conn.request(method, path, body=body, headers=headers)
        resp = conn.getresponse()
        data = resp.read(MAX_BYTES)
    except Refused:
        raise
    except socket.timeout:
        raise Refused("read", f"{host} didn't answer within {TIMEOUT} seconds") from None
    except (OSError, http.client.HTTPException) as e:
        raise Refused("read", f"the request to {host} failed ({e})") from None
    finally:
        conn.close()
    return {"status": resp.status, "body": data.decode("utf-8", "replace")}


def check(event):
    """Each hop to the service, in order, stopping at the first that fails."""
    hops = []

    def hop(name, fn):
        started = time.monotonic()
        try:
            detail, value = fn()
            hops.append({"hop": name, "ok": True, "detail": detail, "ms": int((time.monotonic() - started) * 1000)})
            return value
        except Refused as e:
            hops.append({"hop": e.hop if e.hop != "address" else name, "ok": False, "detail": e.detail})
            return None

    t = hop("address", lambda: ("an https address", target(event.get("url"))))
    if t is None:
        return {"hops": hops}
    host, port, _ = t
    def dns():
        found = resolve(host, port)
        return f"{host} is {', '.join(found[:3])}", found

    addrs = hop("dns", dns)
    if addrs is None:
        return {"hops": hops}
    sock = hop("tcp", lambda: (f"{addrs[0]}:{port} answered", connect(addrs[0], port)))
    if sock is None:
        return {"hops": hops}
    s = hop("tls", lambda: (f"certificate valid for {host}", tls(sock, host)))
    if s is not None:
        s.close()
    return {"hops": hops}


def handler(event, _context=None):
    event = event if isinstance(event, dict) else {}
    try:
        if event.get("action") == "check":
            return check(event)
        if event.get("action") == "send":
            return send(event)
        raise Refused("address", "unknown action")
    except Refused as e:
        return {"error": {"hop": e.hop, "detail": e.detail}}
