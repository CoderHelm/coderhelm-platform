"""The watch after an update probes Chat too. Chat runs in its own function
with its own role, so a grant it lost (every stream's member check, with no
read of the users table) left the worker and the gateway healthy while Chat
refused every question. The watch now asks the streaming function to make
Chat's own calls, and a failure rolls the release back the way a failed
worker probe does.

Run: python3 -m unittest discover -s lambda/tests
"""

import json
import unittest

import test_updater_health as health
import test_updater_native as base

u = base.u
NOW = base.NOW

REFUSED = {"ok": False, "refused": ["member check: the member check failed (503 Service Unavailable), so Chat answers 503"],
           "unknown": 0, "checks": 12}


def streamed(status, body):
    head = json.dumps({"statusCode": status, "headers": {"content-type": "application/json"}}).encode()
    data = head + b"\x00" * 8 + json.dumps(body).encode()
    return {"StatusCode": 200, "EventStream": [{"PayloadChunk": {"Payload": data[:20]}},
                                               {"PayloadChunk": {"Payload": data[20:]}}, {"InvokeComplete": {}}]}


class Lam:
    """The worker answers its probe; Chat's function answers `chat`."""

    def __init__(self, chat=None, worker=None, raise_=None):
        self.chat = chat if chat is not None else streamed(200, {"ok": True, "refused": [], "unknown": 0, "version": "v1.1.0"})
        self.worker = worker or {"ok": True, "version": "v1.1.0"}
        self.raise_ = raise_
        self.streamed_calls = []

    def invoke(self, FunctionName, InvocationType, Payload):
        import io
        return {"Payload": io.BytesIO(json.dumps(self.worker).encode())}

    def invoke_with_response_stream(self, FunctionName, InvocationType, Payload):
        self.streamed_calls.append((FunctionName, json.loads(Payload)))
        if self.raise_:
            raise self.raise_
        return self.chat


ENV = {"RESOURCE_PREFIX": "coderhelm-prod", "WORKER_FUNCTION_NAME": "coderhelm-prod-worker"}


class ProbeChat(unittest.TestCase):
    def test_a_healthy_chat(self):
        lam = Lam()
        self.assertEqual(u.probe_chat(ENV, lam), ("ok", "v1.1.0"))
        (fn, event), = lam.streamed_calls
        self.assertEqual(fn, "coderhelm-prod-streaming")
        # A function URL request to the probe, naming the probe's host.
        self.assertEqual((event["rawPath"], event["requestContext"]["http"]["method"]), (u.CHAT_PROBE_PATH, "POST"))
        self.assertEqual(event["requestContext"]["domainName"], u.CHAT_PROBE_HOST)

    def test_a_refused_grant_fails(self):
        result, detail = u.probe_chat(ENV, Lam(streamed(200, REFUSED)))
        self.assertEqual(result, "fail")
        self.assertIn("member check", detail)
        self.assertEqual(u.probe_chat(ENV, Lam(streamed(502, {"message": "Internal Server Error"})))[0], "fail")
        broken = {"EventStream": [{"InvokeComplete": {"ErrorCode": "Unhandled", "ErrorDetails": "Runtime.ExitError"}}]}
        self.assertEqual(u.probe_chat(ENV, Lam(broken))[0], "fail")

    def test_what_says_nothing_about_the_release_is_unknown(self):
        class TooManyRequestsException(Exception):
            pass
        self.assertEqual(u.probe_chat(ENV, Lam(raise_=TooManyRequestsException("Rate exceeded")))[0], "unknown")
        # A release from before the probe answers 404.
        self.assertEqual(u.probe_chat(ENV, Lam(streamed(404, {"error": "Not found"})))[0], "unknown")
        self.assertEqual(u.probe_chat(ENV, Lam(streamed(200, {"ok": False, "refused": [], "unknown": 2})))[0], "unknown")
        self.assertEqual(u.probe_chat({}, Lam())[0], "unknown")

    def test_the_release_probe_needs_both(self):
        saved = u.probe_client
        try:
            u.probe_client = lambda: Lam()
            self.assertEqual(u.probe_release(ENV)[0], "ok")
            u.probe_client = lambda: Lam(streamed(200, REFUSED))
            self.assertEqual(u.probe_release(ENV)[0], "fail")
            u.probe_client = lambda: Lam(worker={"ok": False})
            self.assertEqual(u.probe_release(ENV)[0], "fail")
        finally:
            u.probe_client = saved

    def test_chats_errors_are_read_too(self):
        self.assertIn("coderhelm-prod-streaming", u.watched_functions(ENV))


class ChatFailsAfterAnUpdate(unittest.TestCase):
    """The same install as the worker's watch tests, with the real probes."""

    setUp = health.Watch.setUp
    tearDown = health.Watch.tearDown

    def test_chat_refused_rolls_back_like_a_failed_worker(self):
        saved = u.probe_client
        u.probe_client = lambda: Lam(streamed(200, REFUSED))
        u.HEALTH_PROBE = None
        try:
            self.cfn.nested["nested-api"] = {"Changes": health.POLICY}
            w = u.watch_step(self.store, self.env, NOW)
        finally:
            u.probe_client = saved
        self.assertEqual(w["probe"]["fails"], u.PROBE_ATTEMPTS)
        self.assertEqual(w["status"], "rolling_back")
        self.assertIn("failed its health check", w["why"])
        self.assertIn("member check", w["why"])
        self.assertEqual(len(self.cfn.created), 1)
        self.assertIn("/v1.0.0/", self.cfn.created[0]["TemplateURL"])

    def test_a_healthy_chat_leaves_the_release(self):
        saved = u.probe_client
        u.probe_client = lambda: Lam()
        u.HEALTH_PROBE = None
        try:
            w = u.watch_step(self.store, self.env, NOW)
        finally:
            u.probe_client = saved
        self.assertEqual((w["status"], w["probe"]["result"]), ("watching", "ok"))
        self.assertEqual(self.cfn.created, [])


if __name__ == "__main__":
    unittest.main()
