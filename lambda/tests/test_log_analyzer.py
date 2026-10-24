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
sys.path.insert(0, str(HANDLER.parent))


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
        self.assertEqual(c["actions"][0]["url"], "https://app.example/logs")


class UsageAccounting(unittest.TestCase):
    def setUp(self):
        import usage
        self.u = usage

    def test_all_four_parts_priced_per_model(self):
        u = self.u.call_usage({
            "input_tokens": 1_000_000, "output_tokens": 1_000_000,
            "cache_read_input_tokens": 2_000_000, "cache_creation_input_tokens": 2_000_000,
            "cache_creation": {"ephemeral_5m_input_tokens": 1_000_000, "ephemeral_1h_input_tokens": 1_000_000},
        })
        self.assertEqual(self.u.total_tokens(u), 6_000_000)
        # Same numbers as the Rust test: Sonnet 5.5 = 2 + 0.2 + 2.5 + 4 + 10.
        self.assertAlmostEqual(self.u.call_cost("claude-sonnet-5-5", u), 18.9, places=9)
        self.assertAlmostEqual(self.u.call_cost("claude-opus-5-5", u), 37.4, places=9)

    def test_unknown_model_is_never_free(self):
        u = self.u.call_usage({"input_tokens": 1000, "output_tokens": 1000})
        unknown = self.u.call_cost("claude-imaginary-9", u)
        self.assertGreater(unknown, 0)
        for m in self.u.PRICES:
            self.assertGreaterEqual(unknown, self.u.call_cost(m, u))

    def test_one_call_one_transaction_with_every_counter(self):
        calls = []

        class Client:
            def transact_write_items(self, **kw):
                calls.append(kw)

        self.u.record_usage(Client(), "t", "team1", "log_analysis", "claude-sonnet-5-5",
                            {"input_tokens": 5, "output_tokens": 7, "cache_read_input_tokens": 11})
        self.u.record_usage(Client(), "t", "team1", "log_analysis", "claude-sonnet-5-5", {})  # empty: skipped
        self.assertEqual(len(calls), 1)
        items = calls[0]["TransactItems"]
        self.assertEqual(len(items), 4)  # the once-only marker, month, all time, day
        upd = items[1]["Update"]
        attrs = {upd["ExpressionAttributeNames"][k]: upd["ExpressionAttributeValues"][k.replace("#a", ":v")]["N"]
                 for k in upd["ExpressionAttributeNames"]}
        self.assertEqual(attrs["total_tokens"], "23")
        self.assertEqual(attrs["total_tokens_in"], "5")
        self.assertEqual(attrs["cache_read_tokens"], "11")
        self.assertEqual(attrs["feature#log_analysis#tokens_out"], "7")
        self.assertEqual(attrs["model#claude-sonnet-5-5#calls"], "1")

    def test_accounting_errors_never_raise(self):
        class Broken:
            def transact_write_items(self, **kw):
                raise RuntimeError("down")

        self.u.record_usage(Broken(), "t", "team1", "log_analysis", "claude-sonnet-5-5", {"input_tokens": 1})


class ModelCallTest(unittest.TestCase):
    """The Anthropic call is streamed; every attempt is counted, broken ones
    with the output estimated; transient failures are retried by status."""

    def setUp(self):
        import usage
        self.u = usage

    @staticmethod
    def ev(v):
        import json
        return ("data: " + json.dumps(v) + "\n\n").encode()

    def start(self, inp=10, read=2000):
        return self.ev({"type": "message_start", "message": {"usage": {
            "input_tokens": inp, "cache_read_input_tokens": read, "output_tokens": 1}}})

    def text(self, t):
        return (self.ev({"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""}})
                + self.ev({"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": t}}))

    def end(self, out):
        return (self.ev({"type": "content_block_stop", "index": 0})
                + self.ev({"type": "message_delta", "delta": {"stop_reason": "end_turn"}, "usage": {"output_tokens": out}}))

    def opener(self, replies):
        """Each reply: an int status (an HTTPError) or bytes (a 200 stream,
        which may stop early), or an exception to raise mid-stream after bytes."""
        import io
        from urllib.error import HTTPError

        calls = []

        class Resp(io.BytesIO):
            def __init__(self, body, boom=None, rid=None):
                super().__init__(body)
                self.boom = boom
                self.headers = {"request-id": rid}

            def __iter__(self):
                for line in self.read().splitlines(keepends=True):
                    yield line
                if self.boom:
                    raise self.boom

        def opener(req, timeout=None):
            r = replies[min(len(calls), len(replies) - 1)]
            calls.append(r)
            if isinstance(r, int):
                raise HTTPError("u", r, "x", {}, io.BytesIO(b"error code: 520"))
            body, boom = r if isinstance(r, tuple) else (r, None)
            return Resp(body, boom, f"req_{len(calls)}")

        return opener, calls

    def client(self):
        writes = []

        class Client:
            def transact_write_items(self, **kw):
                writes.append(kw["TransactItems"])

        return Client(), writes

    def counted(self, items):
        upd = items[1]["Update"]
        return {upd["ExpressionAttributeNames"][k]: upd["ExpressionAttributeValues"][k.replace("#a", ":v").replace("#x", ":x")]["N"]
                for k in upd["ExpressionAttributeNames"]}

    def call(self, replies, backoff=()):
        opener, calls = self.opener(replies)
        c, writes = self.client()
        out = self.u.call_messages("k", {"model": "m", "messages": []}, "claude-sonnet-5-5", c, "t",
                                   "team1", "log_analysis", backoff=backoff, sleep=lambda s: None,
                                   opener=opener)
        return out, calls, writes

    def test_a_whole_reply_is_counted_exactly(self):
        out, _, writes = self.call([self.start() + self.text("[]") + self.end(42)])
        self.assertEqual(out["content"][0]["text"], "[]")
        self.assertEqual(len(writes), 1)
        self.assertEqual(writes[0][0]["Put"]["Item"]["team_id"]["S"], "USAGE_CALL#req_1")
        a = self.counted(writes[0])
        self.assertEqual(a["total_tokens_out"], "42")
        self.assertEqual(a["cache_read_tokens"], "2000")
        self.assertNotIn("estimated_calls", a)

    def test_a_drop_after_message_start_counts_input_and_estimates_output(self):
        with self.assertRaises(self.u.StreamBroke):
            self.call([(self.start() + self.text("x" * 30), TimeoutError("read timed out"))])

    def test_a_broken_reply_retried_counts_both_attempts(self):
        broken = (self.start() + self.text("x" * 30), ConnectionResetError("reset"))
        out, calls, writes = self.call([broken, self.start() + self.text("[]") + self.end(5)], backoff=(0,))
        self.assertEqual(len(calls), 2)
        self.assertEqual(len(writes), 2)
        first = self.counted(writes[0])
        self.assertEqual(first["total_tokens_in"], "10")
        self.assertEqual(first["total_tokens_out"], "10")  # 30 bytes / 3
        self.assertEqual(first["estimated_calls"], "1")
        self.assertEqual(self.counted(writes[1])["total_tokens_out"], "5")

    def test_a_520_is_retried(self):
        out, calls, writes = self.call([520, 529, self.start() + self.end(3)], backoff=(0, 0, 0))
        self.assertEqual(calls[:2], [520, 529])
        self.assertEqual(len(writes), 1)  # an error status is not billed

    def test_a_client_error_is_not_retried(self):
        from urllib.error import HTTPError

        with self.assertRaises(HTTPError):
            self.call([400, 200], backoff=(0, 0))

    def test_statuses(self):
        for c in (408, 429, 500, 502, 503, 504, 520, 524, 529):
            self.assertTrue(self.u.is_transient_status(c), c)
        for c in (200, 400, 401, 404, 413):
            self.assertFalse(self.u.is_transient_status(c), c)

    def test_a_replayed_call_is_counted_once(self):
        class Cancelled(Exception):
            response = {"CancellationReasons": [{"Code": "ConditionalCheckFailed"}, {"Code": "None"}]}

        tries = []

        class Client:
            def transact_write_items(self, **kw):
                tries.append(kw)
                raise Cancelled()

        self.u.record_call(Client(), "t", "team1", "log_analysis", "m",
                           self.u.call_usage({"input_tokens": 1}), call_id="req_1", sleep=lambda s: None)
        self.assertEqual(len(tries), 1, "already counted: not retried, not logged as lost")
class Slack(unittest.TestCase):
    SLACK = "https://hooks.slack.com/services/T0001/B0001/abcdefghijklmnop"
    TEAMS = "https://prod-1.westus.logic.azure.com/workflows/t"

    def test_only_slack_incoming_webhooks(self):
        self.assertTrue(h.valid_slack_url(self.SLACK))
        self.assertFalse(h.valid_slack_url("https://hooks.slack.com.evil.com/services/T0/B0/X"))
        self.assertFalse(h.valid_slack_url("https://hooks.slack.com/services/T0/B0"))
        self.assertFalse(h.valid_slack_url("https://hooks.slack.com/services/T0/B0/X?y=1"))
        self.assertFalse(h.valid_slack_url("https://hooks.slack.com/triggers/T0/1/abc"))
        self.assertFalse(h.valid_slack_url(self.TEAMS))

    def test_block_kit_lists_findings_with_a_button(self):
        recs = [{"title": "<boom>", "severity": "critical", "summary": "5xx & rising", "source_log_group": "/aws/lambda/x"}]
        msg = h.recommendations_slack("123456789012", recs, "https://app.example")
        self.assertEqual(msg["text"], "🔎 1 new log finding in AWS account 123456789012")
        self.assertEqual(msg["blocks"][0]["type"], "header")
        section = msg["blocks"][3]["text"]["text"]
        self.assertEqual(section, "🔴 *&lt;boom&gt;*\n5xx &amp; rising")
        self.assertEqual(msg["blocks"][-1]["elements"][0]["url"], "https://app.example/logs")

    def test_sealed_webhooks_open_for_their_team_only(self):
        class Kms:
            def decrypt(self, CiphertextBlob, EncryptionContext):
                if EncryptionContext != {"team": "TEAM#a", "purpose": "notify-webhook"}:
                    raise RuntimeError("wrong context")
                return {"Plaintext": CiphertextBlob}

        import base64
        old = h.kms_client
        h.kms_client = Kms()
        try:
            item = {
                "slack_webhook_sealed": base64.b64encode(self.SLACK.encode()).decode(),
                "teams_webhook_url": self.TEAMS,
            }
            self.assertEqual(h.open_channels("TEAM#a", item), {"teams": self.TEAMS, "slack": self.SLACK})
            self.assertEqual(h.open_channels("TEAM#b", item), {"teams": self.TEAMS})
            # A plain Slack value is never trusted.
            self.assertEqual(h.open_channels("TEAM#a", {"slack_webhook_url": self.SLACK}), {})
        finally:
            h.kms_client = old

    def test_channel_choice_covers_both(self):
        both = {"teams": self.TEAMS, "slack": self.SLACK}
        self.assertEqual(h.pick_channels("team", {}, both, True), both)
        self.assertEqual(h.pick_channels("team", {}, both, False), {})
        self.assertEqual(h.pick_channels("custom", {"slack": self.SLACK}, both, False), {"slack": self.SLACK})
        self.assertEqual(h.pick_channels("off", both, both, True), {})

    def test_a_failing_slack_post_does_not_stop_teams(self):
        posted = []

        def post(url, payload):
            posted.append(url)
            if "slack" in url:
                raise RuntimeError("500")

        recs = [{"title": "t", "severity": "info"}]
        sent = h.deliver({"slack": self.SLACK, "teams": self.TEAMS}, "1", recs, post=post)
        self.assertEqual(sent, {"slack": False, "teams": True})
        self.assertEqual(sorted(posted), sorted([self.SLACK, self.TEAMS]))


if __name__ == "__main__":
    unittest.main()


class FindingCounts(unittest.TestCase):
    GROUPED = [
        {"log_group": "/aws/lambda/shop", "queries": [
            {"results": [{"error_count": "12"}, {"error_count": "3"}]},
            {"results": [{"@message": "no count column"}]},
        ]},
        {"log_group": "/ecs/web", "queries": [{"results": [{"crash_count": "2"}, {"crash_count": "x"}]}]},
    ]

    def test_counts_sum_per_log_group(self):
        counts = h.error_counts_by_log_group(self.GROUPED)
        self.assertEqual(counts, {"/aws/lambda/shop": 16, "/ecs/web": 3})
        self.assertEqual(h.count_for({"source_log_group": "/ecs/web"}, counts), 3)
        self.assertIsNone(h.count_for({"source_log_group": "/missing"}, counts))
        self.assertIsNone(h.count_for({"source_log_group": "/ecs/web"}, None))

    def test_suspected_location_is_kept_only_when_named(self):
        self.assertEqual(h.suspected_location({"suspected_location": " src/checkout.py:42 "}), "src/checkout.py:42")
        for empty in (None, "", "null", "Unknown", 5):
            self.assertEqual(h.suspected_location({"suspected_location": empty}), "")
        self.assertEqual(len(h.suspected_location({"suspected_location": "x" * 999})), 300)


class StoreFindings(unittest.TestCase):
    class Table:
        def __init__(self, fail_condition=False):
            self.calls = []
            self.fail_condition = fail_condition

        def put_item(self, Item):
            self.calls.append(("put", Item))

        def update_item(self, **kw):
            self.calls.append(("update", kw))
            if self.fail_condition and "ConditionExpression" in kw:
                e = h.ClientError()
                e.response = {"Error": {"Code": "ConditionalCheckFailedException"}}
                raise e

    def setUp(self):
        self.saved = h.aws_insights_table

    def tearDown(self):
        h.aws_insights_table = self.saved

    def test_a_new_finding_stores_its_count_and_location(self):
        h.aws_insights_table = t = self.Table()
        rec = {"title": "Timeouts", "source_log_group": "/aws/lambda/shop", "error_pattern": "Task timed out",
               "suspected_location": "handler.py"}
        rec_id, new = h.store_recommendation("TEAM#a", "111122223333", rec, [], {"/aws/lambda/shop": 16})
        self.assertTrue(new and rec_id)
        item = t.calls[0][1]
        self.assertEqual((item["error_count"], item["suspected_location"]), (16, "handler.py"))

    def test_a_finding_being_fixed_is_refreshed_without_a_ttl(self):
        h.aws_insights_table = t = self.Table(fail_condition=True)
        existing = [{"rec_id": "r1", "sk": "REC#r1", "error_hash": h.error_hash_for("111122223333", {"source_log_group": "/g", "error_pattern": "p"})}]
        rec_id, new = h.store_recommendation("TEAM#a", "111122223333", {"source_log_group": "/g", "error_pattern": "p"}, existing, {"/g": 4})
        self.assertEqual((rec_id, new), ("r1", False))
        first, second = t.calls
        self.assertIn("attribute_not_exists(fix_ticket_id)", first[1]["ConditionExpression"])
        self.assertNotIn("ttl", second[1]["UpdateExpression"])
        self.assertEqual(second[1]["ExpressionAttributeValues"][":c"], 4)

    def test_chat_buttons_open_the_logs_page(self):
        card = h.recommendations_card("1", [{"title": "a", "severity": "critical"}], "https://app")
        self.assertEqual(card["attachments"][0]["content"]["actions"][0]["url"], "https://app/logs")
        slack = h.recommendations_slack("1", [{"title": "a"}], "https://app")
        self.assertEqual(slack["blocks"][-1]["elements"][0]["url"], "https://app/logs")


class BedrockProvider(unittest.TestCase):
    """MODEL_PROVIDER=bedrock: the same call through Bedrock, metered the same."""

    def setUp(self):
        import usage
        self.u = usage

    def test_switch_and_model_ids(self):
        self.assertTrue(self.u.is_bedrock({"MODEL_PROVIDER": "Bedrock"}))
        self.assertFalse(self.u.is_bedrock({}))
        env = {"BEDROCK_INFERENCE_PREFIX": "eu."}
        self.assertEqual(self.u.bedrock_model_id("claude-sonnet-5-5", env), "eu.anthropic.claude-sonnet-5-5")
        self.assertEqual(self.u.bedrock_model_id("claude-haiku-4-5", env),
                         "eu.anthropic.claude-haiku-4-5-20251001-v1:0")
        self.assertEqual(self.u.bedrock_model_id("us.anthropic.x", env), "us.anthropic.x")
        env["BEDROCK_MODEL_IDS"] = '{"claude-sonnet-5-5": "arn:aws:bedrock:eu-west-1:1:application-inference-profile/p"}'
        self.assertTrue(self.u.bedrock_model_id("claude-sonnet-5-5", env).startswith("arn:"))

    def test_body(self):
        b = self.u.bedrock_body({"model": "m", "stream": True, "max_tokens": 5})
        self.assertEqual(b, {"max_tokens": 5, "anthropic_version": "bedrock-2023-05-31"})

    def test_stream_is_read_and_recorded(self):
        import json as _j
        events = [
            {"type": "message_start", "message": {"usage": {"input_tokens": 10, "output_tokens": 1}}},
            {"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""}},
            {"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": "[]"}},
            {"type": "content_block_stop", "index": 0},
            {"type": "message_delta", "delta": {"stop_reason": "end_turn"}, "usage": {"output_tokens": 3}},
            {"type": "message_stop"},
        ]
        seen = {}

        class Bedrock:
            def invoke_model_with_response_stream(self, **kw):
                seen.update(kw)
                return {"ResponseMetadata": {"RequestId": "r1"},
                        "body": [{"chunk": {"bytes": _j.dumps(e).encode()}} for e in events]}

        recorded = []
        orig = self.u._record_attempt
        self.u._record_attempt = lambda *a: recorded.append(a)
        try:
            msg = self.u.call_messages_bedrock({"model": "claude-sonnet-5-5", "max_tokens": 5}, "claude-sonnet-5-5",
                                               None, "t", "team1", "log_analysis", bedrock=Bedrock(),
                                               sleep=lambda s: None)
        finally:
            self.u._record_attempt = orig
        self.assertEqual(msg["content"][0]["text"], "[]")
        self.assertEqual(msg["stop_reason"], "end_turn")
        self.assertTrue(seen["modelId"].endswith("anthropic.claude-sonnet-5-5"))
        self.assertNotIn(b'"model"', seen["body"])
        self.assertEqual(len(recorded), 1)
        u, call_id, estimated = recorded[0][5], recorded[0][6], recorded[0][7]
        self.assertEqual((u["uncached_input"], u["output"], call_id, estimated), (10, 3, "r1", False))
