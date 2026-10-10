"""Unit tests for the log analyzer's pure helpers.

Run: python3 -m unittest discover -s lambda/tests
"""

import importlib.util
import os
import pathlib
import sys
import types
import unittest

HANDLER = pathlib.Path(__file__).resolve().parents[1] / "log-analyzer" / "handler.py"


def load_handler(model_id):
    os.environ.update(
        CODERHELM_ACCOUNT_ID="000000000000",
        MODEL_ID=model_id,
        AWS_DEFAULT_REGION="us-east-1",
    )
    # boto3 is only touched at import for client/table handles.
    fake = types.ModuleType("boto3")
    fake.client = lambda *a, **k: object()
    fake.resource = lambda *a, **k: types.SimpleNamespace(Table=lambda name: object())
    sys.modules["boto3"] = fake
    exc = types.ModuleType("botocore.exceptions")
    exc.ClientError = type("ClientError", (Exception,), {})
    sys.modules.setdefault("botocore", types.ModuleType("botocore"))
    sys.modules["botocore.exceptions"] = exc
    spec = importlib.util.spec_from_file_location(f"handler_{model_id}", HANDLER)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


h = load_handler("claude-sonnet-5-5")


class ModelRequest(unittest.TestCase):
    def test_thinking_models_get_no_temperature(self):
        body = h.build_request_body("p")
        self.assertNotIn("temperature", body)
        self.assertEqual(body["output_config"], {"effort": "medium"})

    def test_older_models_keep_temperature(self):
        old = load_handler("claude-sonnet-4-6")
        body = old.build_request_body("p")
        self.assertEqual(body["temperature"], 0.1)
        self.assertNotIn("output_config", body)

    def test_text_skips_thinking_blocks(self):
        resp = {"content": [{"type": "thinking", "thinking": "..."}, {"type": "text", "text": "[]"}]}
        self.assertEqual(h.response_text(resp), "[]")

    def test_parse_handles_fences_and_rejects_garbage(self):
        self.assertEqual(h.parse_recommendations('```json\n[{"title": "a"}]\n```'), [{"title": "a"}])
        self.assertEqual(h.parse_recommendations("Here you go: [] done"), [])
        self.assertIsNone(h.parse_recommendations("no json"))
        self.assertIsNone(h.parse_recommendations('{"title": "a"}'))


class Dedup(unittest.TestCase):
    existing = [
        {"rec_id": "r1", "sk": "REC#r1", "error_hash": "aaa"},
        {"rec_id": "r2", "sk": "REC#r2", "error_hash": "bbb"},
    ]

    def test_model_named_match_wins(self):
        self.assertEqual(h.match_existing({"existing_id": "r2"}, self.existing, "zzz")["rec_id"], "r2")

    def test_unknown_id_falls_back_to_hash(self):
        self.assertEqual(h.match_existing({"existing_id": "nope"}, self.existing, "aaa")["rec_id"], "r1")
        self.assertIsNone(h.match_existing({"existing_id": None}, self.existing, "zzz"))


class LogGroups(unittest.TestCase):
    def test_pattern_is_case_insensitive(self):
        groups = ["API-Gateway-Execution-Logs_abc/prod", "/aws/lambda/x"]
        self.assertEqual(h.filter_log_groups(groups, "api-gateway"), [groups[0]])
        self.assertEqual(h.filter_log_groups(groups, None), groups)


class Teams(unittest.TestCase):
    def test_only_microsoft_webhook_hosts(self):
        self.assertTrue(h.valid_webhook_url("https://x.environment.api.powerplatform.com:443/powerautomate/a"))
        self.assertTrue(h.valid_webhook_url("https://prod-1.westus.logic.azure.com/workflows/a"))
        self.assertFalse(h.valid_webhook_url("https://evil.com/x"))
        self.assertFalse(h.valid_webhook_url("https://evil.com@prod-1.logic.azure.com/x"))
        self.assertFalse(h.valid_webhook_url("http://prod-1.logic.azure.com/x"))

    def test_channel_choice(self):
        team = "https://prod-1.westus.logic.azure.com/workflows/t"
        own = "https://prod-2.westus.logic.azure.com/workflows/o"
        self.assertEqual(h.pick_webhook("team", "", team, True), team)
        self.assertIsNone(h.pick_webhook("team", "", team, False))
        self.assertEqual(h.pick_webhook("custom", own, team, False), own)
        self.assertIsNone(h.pick_webhook("off", own, team, True))

    def test_card_orders_by_severity_and_caps(self):
        recs = [{"title": f"t{i}", "severity": "info"} for i in range(6)] + [{"title": "boom", "severity": "critical"}]
        card = h.recommendations_card("123456789012", recs, "https://app.example")
        c = card["attachments"][0]["content"]
        self.assertEqual(c["body"][0]["style"], "attention")
        self.assertIn("7 new log findings", c["body"][0]["items"][0]["text"])
        self.assertIn("boom", c["body"][2]["items"][0]["text"])
        self.assertEqual(c["body"][-1]["text"], "+2 more")
        self.assertEqual(c["actions"][0]["url"], "https://app.example/settings/aws")


if __name__ == "__main__":
    unittest.main()
