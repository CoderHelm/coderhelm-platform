"""A burst never multiplies model calls, and a sighting is judged against
its own history before anyone is told: noise, watch or act."""

import json
import unittest
from datetime import datetime, timedelta, timezone

from test_log_analyzer import h
from test_log_attribution import ACCOUNT, NOW, FakeInsights, Patch, Table

import triage as T

API = "/ecs/prod-shop-api"
GROUPS = [API]
NOW_DT = datetime.fromtimestamp(NOW, tz=timezone.utc)


def upstream_line(request_id):
    return (
        f"[Nest] 1  - 11/14/2026, 2:26:14 PM   ERROR [AuthGuard] AxiosError: Request failed with "
        f"status code 503 GET http://identity.shop.internal/v1/session request_id={request_id}"
    )


class Calls:
    """The model, counted: one reply citing every evidence line."""

    def __init__(self):
        self.analyses = 0
        self.guards = 0

    def __call__(self, prompt, _key, _team=None):
        if prompt.startswith("You check log findings"):
            self.guards += 1
            items = json.loads(prompt.split("<items>\n", 1)[1].split("\n</items>", 1)[0])
            return json.dumps([{"id": i["id"], "supported": True} for i in items])
        self.analyses += 1
        evidence = json.loads(prompt.split("<evidence>\n", 1)[1].split("\n</evidence>", 1)[0])
        return json.dumps([{
            "title": "Auth guard gets 503 from the session service in prod-shop-api",
            "severity": "warning",
            "summary": "The auth guard's session lookup in prod-shop-api answers 503.",
            "suggested_action": "Retry the session lookup once with a short timeout.",
            "evidence_ids": [e["id"] for e in evidence],
            "error_pattern": "Request failed with status code 503",
            "existing_id": None,
        }])


class Refused(Exception):
    """DynamoDB's refusal of a condition, as boto3 raises it (whatever
    botocore the test run loaded)."""

    response = {"Error": {"Code": "ConditionalCheckFailedException"}}


class BudgetTable(Table):
    """A table whose hourly analysis budget is spent."""

    def update_item(self, **kw):
        if kw["Key"]["sk"].startswith(T.BUDGET_PREFIX) and "calls" in kw["UpdateExpression"]:
            raise Refused()
        super().update_item(**kw)


def run(events, existing, table=None):
    cw, table, calls, notified = FakeInsights(events), table or Table(), Calls(), []
    with Patch(
        get_team_api_key=lambda _t: "key",
        logs_client_for=lambda _c: cw,
        list_recent_findings=lambda *_a: existing,
        record_connection_success=lambda *_a: None,
        notify_new_recommendations=lambda _t, _a, recs: notified.extend(recs),
        ask_model=calls,
        aws_insights_table=table,
    ):
        h.analyze_connection({
            "team_id": "TEAM#t", "account_id": ACCOUNT, "role_arn": "r", "external_id": "x",
            "region": "us-east-1", "log_groups": GROUPS,
        })
    return table, calls, notified


def burst(n=10_000, minutes=5):
    """`n` lines in `minutes`, each with its own request id."""
    return [
        (API, f"api/api/t{i % 4}", NOW - minutes * 60 + (i * minutes * 60) // n, upstream_line(f"r{i:05d}"), 1)
        for i in range(n)
    ]


def stored(put):
    return {**put, "rec_id": put["sk"][len("REC#"):]}


class Burst(unittest.TestCase):
    def test_ten_thousand_lines_are_one_model_call_and_one_finding(self):
        table, calls, notified = run(burst(), [])
        self.assertEqual(calls.analyses + calls.guards, 1)
        self.assertEqual(len(table.puts), 1)
        item = table.puts[0]
        self.assertEqual(item["seen_count"], 10_000)
        self.assertEqual(len(notified), 1)

        # The same burst seen by the next run (a watch, minutes later): the
        # finding's counts move, the model is not asked again.
        table2, calls2, notified2 = run(burst(), [stored(item)])
        self.assertEqual(calls2.analyses + calls2.guards, 0)
        self.assertEqual(table2.puts, [])
        (u,) = [u for u in table2.updates if u["Key"]["sk"] == item["sk"]]
        self.assertIn("seen_count = :seen", u["UpdateExpression"])
        self.assertEqual(notified2, [])

    def test_past_the_hourly_budget_it_only_counts(self):
        known = stored(run(burst(50), [])[0].puts[0])
        other = [(API, "api/api/t1", NOW - 60, "TypeError: Cannot read properties of undefined (reading 'id')", 3)]
        table, calls, _ = run(burst(50) + other, [known], BudgetTable())
        # The new error waits for the next hour; the known one is counted.
        self.assertEqual(calls.analyses, 0)
        self.assertEqual(table.puts, [])
        self.assertTrue(any(u["Key"]["sk"] == known["sk"] for u in table.updates))
        refused = [u for u in table.updates if u["Key"]["sk"].startswith(T.BUDGET_PREFIX)]
        self.assertIn("ADD refused :one", refused[0]["UpdateExpression"])

    def test_collapse_folds_lines_that_differ_only_by_ids(self):
        ev = [
            {"account": ACCOUNT, "log_group": API, "message": upstream_line("a1"), "count": 3,
             "streams": {"s1": 3}, "first": None, "last": None},
            {"account": ACCOUNT, "log_group": API, "message": upstream_line("b2"), "count": 5,
             "streams": {"s2": 5}, "first": None, "last": None},
            {"account": ACCOUNT, "log_group": API, "message": "TypeError: x is not a function", "count": 1,
             "streams": {"s1": 1}, "first": None, "last": None},
        ]
        out = T.collapse(ev)
        self.assertEqual([(e["id"], e["count"]) for e in out], [("e1", 8), ("e2", 1)])
        self.assertEqual(out[0]["streams"], {"s1": 3, "s2": 5})


def history(per_day, days):
    return [{"name": (NOW_DT.date() - timedelta(days=d)).strftime("%Y-%m-%d"), "lines": per_day}
            for d in range(1, days + 1)]


class Decisions(unittest.TestCase):
    def test_usual_rate_is_noise_and_tells_nobody(self):
        d = T.decide({"daily_lines": history(3, 5)}, 2, NOW_DT)
        self.assertEqual(d["decision"], T.NOISE)
        self.assertEqual(d["reason"], "At its usual rate: 2 lines, about 2 a day on 5 of the last 7 days.")
        self.assertFalse(T.notifies(d, reopened=True))

    def test_rising_is_act(self):
        d = T.decide({"daily_lines": history(3, 7)}, 400, NOW_DT)
        self.assertEqual(d["decision"], T.ACT)
        self.assertTrue(d["reason"].startswith("Rising: 400 lines against about 3 a day"))
        self.assertTrue(T.notifies(d))

    def test_new_and_back_after_a_quiet_week_are_said_once(self):
        new = T.decide(None, 2, NOW_DT, is_new=True, severity="warning")
        self.assertEqual(new["decision"], T.WATCH)
        self.assertTrue(T.notifies(new, is_new=True))
        self.assertEqual(T.decide(None, 2, NOW_DT, is_new=True, severity="critical")["decision"], T.ACT)
        back = T.decide({"daily_lines": [{"name": "2026-01-01", "lines": 9}]}, 2, NOW_DT)
        self.assertEqual(back["days_seen"], 0)
        self.assertTrue(T.notifies(back, reopened=True))
        self.assertFalse(T.notifies(back))


CASES = __import__("pathlib").Path(__file__).resolve().parents[2] / "services/common/tests/fixtures/log_triage_cases.json"


class SharedCases(unittest.TestCase):
    """The worker reads the same cases (common::log_triage)."""

    def test_every_case_decides_the_same(self):
        for c in json.loads(CASES.read_text())["cases"]:
            item = None if c.get("new") else {"daily_lines": [
                {"name": (NOW_DT.date() - timedelta(days=i + 1)).strftime("%Y-%m-%d"), "lines": n}
                for i, n in enumerate(c["history"])
            ] or [{"name": "1999-01-01", "lines": 1}]}
            d = T.decide(item, c["lines"], NOW_DT, is_new=bool(c.get("new")), severity=c.get("severity", ""))
            self.assertEqual((d["decision"], d["reason"]), (c["decision"], c["reason"]), c["name"])
            if "notifies_new" in c:
                self.assertEqual(T.notifies(d, is_new=True), c["notifies_new"], c["name"])
            if "notifies_reopened" in c:
                self.assertEqual(T.notifies(d, reopened=True), c["notifies_reopened"], c["name"])


class SeenAgain(unittest.TestCase):
    def test_two_lines_at_the_usual_rate_reopen_quietly(self):
        """The card that said "Seen again: Request failed with 503" for two
        lines of an error the service logs most days."""
        first = stored(run(burst(3, 1), [])[0].puts[0])
        first.update(status="resolved", resolved_at=(NOW_DT - timedelta(hours=3)).isoformat(),
                     last_seen_at=(NOW_DT - timedelta(hours=20)).isoformat(), daily_lines=history(3, 5))
        table, calls, notified = run(burst(2, 1), [first])
        self.assertEqual(calls.analyses, 0)
        self.assertEqual(notified, [])
        (u,) = [u for u in table.updates if u["Key"]["sk"] == first["sk"]]
        tri = u["ExpressionAttributeValues"][":tri"]
        self.assertEqual(tri["decision"], T.NOISE)
        self.assertTrue(tri["reason"].startswith("At its usual rate: 2 lines"), tri["reason"])


class Card(unittest.TestCase):
    def test_the_card_leads_with_the_decision_and_numbers(self):
        rec = {
            "rec_id": "01abc", "title": "Auth guard gets 503 from the session service", "severity": "warning",
            "summary": "The auth guard's session lookup answers 503.", "source_log_group": API,
            "triage": {"decision": T.ACT, "reason": "Rising: 400 lines against about 3 a day over the last 7 days."},
            "stats": {"count": 400, "streams": {"a": 300, "b": 100}, "last": NOW_DT - timedelta(minutes=40),
                      "samples": [{"message": upstream_line("r1")}]},
        }
        top, facts = h.card_lines(rec, NOW_DT)
        self.assertEqual(top, "Act: Rising: 400 lines against about 3 a day over the last 7 days.")
        self.assertEqual(facts, f"400 lines in {API} from 2 tasks · upstream identity.shop.internal · none since "
                                f"{(NOW_DT - timedelta(minutes=40)).strftime('%H:%M')} UTC")
        card = json.dumps(h.recommendations_card(ACCOUNT, [rec], "https://app.example"))
        self.assertNotIn(ACCOUNT, card)
        self.assertIn("https://app.example/logs/?finding=01abc", card)
        self.assertNotIn("new log finding", card)


if __name__ == "__main__":
    unittest.main()
