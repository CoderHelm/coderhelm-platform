"""The private network function: what it carries, what it refuses, and the
hop it names when something on the way fails.

Run: python3 -m unittest discover -s lambda/tests
"""

import importlib.util
import pathlib
import socket
import unittest
from unittest import mock

HANDLER = pathlib.Path(__file__).resolve().parents[1] / "private-reach" / "handler.py"
spec = importlib.util.spec_from_file_location("private_reach", HANDLER)
reach = importlib.util.module_from_spec(spec)
spec.loader.exec_module(reach)


def addrinfo(*ips):
    return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (ip, 443)) for ip in ips]


class AddressTests(unittest.TestCase):
    def test_private_addresses_only(self):
        for ip in ("10.2.101.7", "172.20.0.5", "192.168.1.9", "fd00::5"):
            self.assertTrue(reach.private_ip(ip), ip)
        for ip in ("8.8.8.8", "127.0.0.1", "169.254.169.254", "0.0.0.0", "::1", "fe80::1", "100.64.0.1", "::ffff:169.254.169.254"):
            self.assertFalse(reach.private_ip(ip), ip)

    def test_only_https_names_on_known_ports(self):
        self.assertEqual(reach.target("https://vpc-orders-abc.us-east-1.es.amazonaws.com/orders/_search"),
                         ("vpc-orders-abc.us-east-1.es.amazonaws.com", 443, "/orders/_search"))
        self.assertEqual(reach.target("https://search.internal.example.com:9200/_cat/indices?format=json")[1:],
                         (9200, "/_cat/indices?format=json"))
        for bad in ("http://search.example.com/", "https://10.0.0.5/", "https://user:pw@search.example.com/",
                    "https://search.example.com:22/", "https://localhost/", None, "x" * 5000):
            with self.assertRaises(reach.Refused, msg=bad):
                reach.target(bad)

    def test_a_public_answer_is_refused(self):
        with mock.patch.object(reach.socket, "getaddrinfo", return_value=addrinfo("10.0.1.5", "52.1.2.3")):
            with self.assertRaises(reach.Refused) as e:
                reach.resolve("search.example.com", 443)
        self.assertEqual(e.exception.hop, "dns")
        self.assertIn("isn't a private address", e.exception.detail)


class HandlerTests(unittest.TestCase):
    def test_refusals_come_back_as_errors_never_exceptions(self):
        self.assertEqual(reach.handler({"action": "send", "url": "http://x.example.com"})["error"]["hop"], "address")
        self.assertEqual(reach.handler({"action": "drop"})["error"]["hop"], "address")
        self.assertIn("error", reach.handler(None))

    def test_only_get_and_post(self):
        with mock.patch.object(reach.socket, "getaddrinfo", return_value=addrinfo("10.0.1.5")):
            r = reach.handler({"action": "send", "url": "https://search.example.com/x", "method": "DELETE"})
        self.assertIn("GET or POST", r["error"]["detail"])

    def test_check_names_the_dns_hop(self):
        with mock.patch.object(reach.socket, "getaddrinfo", side_effect=socket.gaierror(-2, "Name or service not known")):
            r = reach.handler({"action": "check", "url": "https://vpc-gone-abc.us-east-1.es.amazonaws.com"})
        self.assertEqual([h["hop"] for h in r["hops"]], ["address", "dns"])
        self.assertFalse(r["hops"][-1]["ok"])
        self.assertIn("doesn't resolve", r["hops"][-1]["detail"])

    def test_check_names_the_tcp_hop(self):
        with mock.patch.object(reach.socket, "getaddrinfo", return_value=addrinfo("10.2.101.9")), \
             mock.patch.object(reach.socket, "create_connection", side_effect=socket.timeout()):
            r = reach.handler({"action": "check", "url": "https://vpc-orders-abc.us-east-1.es.amazonaws.com"})
        self.assertEqual([(h["hop"], h["ok"]) for h in r["hops"]], [("address", True), ("dns", True), ("tcp", False)])
        self.assertIn("security group", r["hops"][-1]["detail"])

    def test_check_names_the_tls_hop(self):
        bad = reach.ssl.SSLCertVerificationError("bad")
        bad.verify_message = "Hostname mismatch"
        ctx = mock.Mock()
        ctx.wrap_socket.side_effect = bad
        with mock.patch.object(reach.socket, "getaddrinfo", return_value=addrinfo("10.2.101.9")), \
             mock.patch.object(reach.socket, "create_connection", return_value=mock.Mock()), \
             mock.patch.object(reach.ssl, "create_default_context", return_value=ctx):
            r = reach.handler({"action": "check", "url": "https://vpc-orders-abc.us-east-1.es.amazonaws.com"})
        self.assertEqual(r["hops"][-1]["hop"], "tls")
        self.assertIn("Hostname mismatch", r["hops"][-1]["detail"])

    def test_check_passes_every_hop(self):
        ctx = mock.Mock()
        with mock.patch.object(reach.socket, "getaddrinfo", return_value=addrinfo("10.2.101.9")), \
             mock.patch.object(reach.socket, "create_connection", return_value=mock.Mock()), \
             mock.patch.object(reach.ssl, "create_default_context", return_value=ctx):
            r = reach.handler({"action": "check", "url": "https://vpc-orders-abc.us-east-1.es.amazonaws.com"})
        self.assertEqual([(h["hop"], h["ok"]) for h in r["hops"]], [("address", True), ("dns", True), ("tcp", True), ("tls", True)])

    def test_send_drops_unknown_headers_and_pins_the_checked_address(self):
        seen = {}

        class Conn:
            def __init__(self, host, port, ip):
                seen.update(host=host, port=port, ip=ip)

            def request(self, method, path, body=None, headers=None):
                seen.update(method=method, path=path, body=body, headers=headers)

            def getresponse(self):
                r = mock.Mock(status=200)
                r.read.return_value = b'{"count": 3}'
                return r

            def close(self):
                pass

        with mock.patch.object(reach.socket, "getaddrinfo", return_value=addrinfo("10.2.101.9")), \
             mock.patch.object(reach, "Pinned", Conn):
            r = reach.handler({"action": "send", "url": "https://vpc-orders-abc.us-east-1.es.amazonaws.com/orders/_count",
                               "method": "POST", "headers": {"Authorization": "Basic x", "Host": "evil", "X-Forwarded-For": "1"},
                               "body": {"query": {"match_all": {}}}})
        self.assertEqual(r, {"status": 200, "body": '{"count": 3}'})
        self.assertEqual(seen["ip"], "10.2.101.9")
        self.assertEqual(seen["path"], "/orders/_count")
        self.assertEqual(set(seen["headers"]), {"Authorization", "User-Agent", "Content-Type"})


if __name__ == "__main__":
    unittest.main()
