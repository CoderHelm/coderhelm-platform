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


class PlaywrightMirrorTests(unittest.TestCase):
    """The proxy serves Playwright's browser builds, and only those."""

    def test_only_a_plain_get_of_a_build_path_is_a_mirror_request(self):
        p = egress.MIRROR_PREFIX
        self.assertEqual(
            egress.mirror_path("GET " + p + "builds/cft/153.0.1/linux64/chrome-linux64.zip HTTP/1.1"),
            "builds/cft/153.0.1/linux64/chrome-linux64.zip",
        )
        self.assertIsNone(egress.mirror_path("POST " + p + "builds/x.zip HTTP/1.1"))
        self.assertIsNone(egress.mirror_path("GET " + p + "../etc/passwd HTTP/1.1"))
        self.assertIsNone(egress.mirror_path("GET " + p + "/abs HTTP/1.1"))
        self.assertIsNone(egress.mirror_path("GET " + p + "a%2f..%2fb HTTP/1.1"))
        self.assertIsNone(egress.mirror_path("GET http://example.com/x HTTP/1.1"))
        self.assertIsNone(egress.mirror_path("CONNECT cdn.playwright.dev:443 HTTP/1.1"))

    def test_builds_come_from_playwrights_hosts_in_its_order(self):
        self.assertEqual(
            egress.mirror_upstreams("builds/cft/1/linux64/chrome-linux64.zip"),
            ["https://cdn.playwright.dev/builds/cft/1/linux64/chrome-linux64.zip"],
        )
        ups = egress.mirror_upstreams("builds/chromium/1200/chromium-linux.zip")
        self.assertEqual(len(ups), 3)
        self.assertTrue(all(egress.mirror_allows(u) for u in ups))

    def test_a_redirect_reaches_only_the_browser_builds_folder_of_the_shared_host(self):
        self.assertTrue(egress.mirror_allows(
            "https://storage.googleapis.com/chrome-for-testing-public/153.0.1/linux64/chrome-linux64.zip"))
        self.assertFalse(egress.mirror_allows("https://storage.googleapis.com/other-bucket/x"))
        self.assertFalse(egress.mirror_allows("http://cdn.playwright.dev/builds/x.zip"))
        self.assertFalse(egress.mirror_allows("https://user@cdn.playwright.dev/builds/x.zip"))
        self.assertFalse(egress.mirror_allows("https://cdn.playwright.dev:8443/builds/x.zip"))
        self.assertFalse(egress.mirror_allows("https://evil.example/builds/x.zip"))


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
