"""Unit tests for the sandbox's network allowlist proxy.

Run: python3 -m unittest discover -s lambda/tests
"""

import importlib.util
import pathlib
import socket
import threading
import unittest

PROXY = (
    pathlib.Path(__file__).resolve().parents[2]
    / "services" / "common" / "src" / "agent_policy" / "egress_proxy.py"
)
spec = importlib.util.spec_from_file_location("egress_proxy", PROXY)
egress = importlib.util.module_from_spec(spec)
spec.loader.exec_module(egress)


class AllowlistTests(unittest.TestCase):
    def test_parse(self):
        self.assertEqual(
            egress.parse_allowlist(" Registry.npmjs.org, *.pypi.org\ngithub.com.,, "),
            ["registry.npmjs.org", "pypi.org", "github.com"],
        )

    def test_domains_cover_subdomains_only(self):
        allow = ["npmjs.org", "github.com"]
        self.assertTrue(egress.host_allowed(allow, "registry.npmjs.org"))
        self.assertTrue(egress.host_allowed(allow, "github.com:443"))
        self.assertFalse(egress.host_allowed(allow, "evilgithub.com"))
        self.assertFalse(egress.host_allowed(allow, "github.com.evil.io"))
        self.assertFalse(egress.host_allowed(allow, ""))
        self.assertFalse(egress.host_allowed(allow, "[::1]"))

    def test_targets(self):
        self.assertEqual(
            egress.target_of("CONNECT registry.npmjs.org:443 HTTP/1.1", {}),
            ("CONNECT", "registry.npmjs.org", 443),
        )
        self.assertEqual(
            egress.target_of("GET http://example.com:8080/x HTTP/1.1", {}),
            ("GET", "example.com", 8080),
        )
        self.assertEqual(
            egress.target_of("GET /x HTTP/1.1", {"host": "pypi.org"}),
            ("GET", "pypi.org", 80),
        )
        self.assertIsNone(egress.target_of("", {}))


class ProxyTests(unittest.TestCase):
    """A blocked host gets 403 without any connection being made."""

    def test_blocked_host_is_refused(self):
        egress.Handler.allow = ["allowed.example"]
        srv = egress.Server(("127.0.0.1", 0), egress.Handler)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        try:
            port = srv.server_address[1]
            with socket.create_connection(("127.0.0.1", port), timeout=5) as c:
                c.sendall(b"CONNECT exfil.example:443 HTTP/1.1\r\nHost: exfil.example\r\n\r\n")
                reply = c.recv(1024)
            self.assertTrue(reply.startswith(b"HTTP/1.1 403"), reply)
        finally:
            srv.shutdown()
            srv.server_close()


if __name__ == "__main__":
    unittest.main()
