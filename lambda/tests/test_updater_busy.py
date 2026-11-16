"""AWS turning calls away (throttling) before an update changed anything:
the updater retries by itself, the update keeps waiting ("AWS is busy,
retrying (attempt n)"), and only after BUSY_LIMIT does it fail, with words
that never hand the retry to a person, and start again by itself later.

Run: python3 -m unittest discover -s lambda/tests
"""

import unittest
from datetime import timedelta

from test_updater_native import ENV, NOW, FakeCfn, FakeDynamo, FakeIam, FakeLambda, u

ROLE_ENV = dict(ENV, DEPLOY_ROLE_ARN="arn:aws:iam::1:role/coderhelm-prod-deploy")
ADMIN = "admin@example.com"


class ClientError(Exception):
    """What botocore raises once its own retries are used up."""

    def __init__(self, code="Throttling", op="DescribeChangeSet", http=400, message="Rate exceeded"):
        self.response = {"Error": {"Code": code, "Message": message}, "ResponseMetadata": {"HTTPStatusCode": http}}
        super().__init__(f"An error occurred ({code}) when calling the {op} operation (reached max retries: 5): {message}")


class TooManyRequestsException(Exception):
    pass


class BusyCfn(FakeCfn):
    """CloudFormation that throttles reading the change set's pages (the
    checks before it runs) and creating change sets while told to."""

    def __init__(self):
        super().__init__()
        self.busy_pages = False
        self.busy_create = False
        self.turned_away = 0

    def describe_change_set(self, ChangeSetName, StackName=None, IncludePropertyValues=False, NextToken=None):
        if self.busy_pages and IncludePropertyValues:
            self.turned_away += 1
            raise ClientError()
        return super().describe_change_set(ChangeSetName, StackName, IncludePropertyValues, NextToken)

    def create_change_set(self, **kw):
        if self.busy_create:
            self.turned_away += 1
            raise ClientError(op="CreateChangeSet")
        return super().create_change_set(**kw)


class BusyIam(FakeIam):
    """IAM throttling the deploy role's reads (the preflight) while told to."""

    busy = True

    def get_role(self, RoleName):
        if self.busy:
            raise ClientError(op="GetRole")
        return super().get_role(RoleName)


def no_press(text):
    return "press update again" not in str(text).lower()


class Busy(unittest.TestCase):
    def setUp(self):
        self.store = u.Store(FakeDynamo(), "t")
        self.cfn = BusyCfn()
        u._clients["cloudformation"] = self.cfn
        u._clients["lambda"] = FakeLambda()
        self.sent = []
        self._notify, self._fetch, self._sleep = u.notify, u.fetch_template, u._sleep
        u.notify = lambda *a, **k: self.sent.append(a)
        u.fetch_template = lambda url, opener=None: __import__("test_updater_native").TEMPLATE
        self.pauses = []
        u._sleep = self.pauses.append
        self.store.put("MANIFEST#v1.1.0", {"verified": True, "manifest": None, "template_sha256": __import__("test_updater_native").SHA})
        self.store.put("MODEL_CHECK", {"version": "v1.1.0", "ok": True, "results": [], "checked_at": NOW.isoformat()})

    def tearDown(self):
        u.notify, u.fetch_template, u._sleep = self._notify, self._fetch, self._sleep
        u._clients.pop("cloudformation", None)
        u._clients.pop("lambda", None)

    def prepared(self, actor=ADMIN):
        """An update whose change set exists and is ready to run."""
        e = u.begin(self.store, ROLE_ENV, NOW, "v1.1.0", actor, "update")
        self.cfn.cs = {"Status": "CREATE_COMPLETE", "ExecutionStatus": "AVAILABLE"}
        return e["id"]

    # ── telling throttling apart ─────────────────────────────────────────

    def test_throttling_is_told_apart_from_refusals(self):
        for e in (ClientError(), ClientError("ThrottlingException"), ClientError("RequestLimitExceeded"),
                  ClientError("SlowDown", http=503), ClientError("Other", http=429),
                  TooManyRequestsException("Rate exceeded"), Exception("Rate exceeded")):
            self.assertTrue(u.throttled(e), e)
        wrapped = ValueError("couldn't prepare")
        wrapped.__cause__ = ClientError()
        self.assertTrue(u.throttled(wrapped))
        for e in (ClientError("AccessDenied", message="not authorized"), ClientError("LimitExceededException", message="Stack limit reached"),
                  Exception("ValidationError: throttled"),
                  ValueError("the stack is UPDATE_IN_PROGRESS")):
            self.assertFalse(u.throttled(e), e)

    def test_one_call_retries_with_jittered_backoff_then_gives_up(self):
        tries = []

        def flaky():
            tries.append(1)
            if len(tries) < 3:
                raise ClientError()
            return "ok"
        self.assertEqual(u.busy_retry(flaky), "ok")
        self.assertEqual(len(self.pauses), 2)
        self.assertTrue(all(0 <= p <= u.BUSY_PAUSE_CAP for p in self.pauses))
        tries.clear()
        with self.assertRaises(ClientError):
            u.busy_retry(lambda: (_ for _ in ()).throw(ClientError()))
        # Anything else is never retried.
        tries.clear()

        def refused():
            tries.append(1)
            raise ClientError("AccessDenied", message="not authorized")
        with self.assertRaises(ClientError):
            u.busy_retry(refused)
        self.assertEqual(len(tries), 1)

    # ── the change set exists: its checks are throttled ──────────────────

    def test_a_throttled_check_waits_and_the_next_poll_starts_the_update(self):
        hid = self.prepared(actor="auto")
        self.cfn.busy_pages = True
        r = u.native_poll(self.store, ROLE_ENV, NOW)
        self.assertEqual(r["native"], "busy")
        entry = self.store.get(hid)
        self.assertEqual(entry["status"], "preparing")
        self.assertEqual(entry["reason"], "AWS is busy, retrying (attempt 1)")
        self.assertEqual(entry["busy"]["attempts"], 1)
        self.assertEqual(self.cfn.deleted, [])
        self.assertGreaterEqual(self.cfn.turned_away, u.BUSY_TRIES)
        # Once more while still busy: attempt 2, still waiting, never failed.
        r = u.native_poll(self.store, ROLE_ENV, NOW + timedelta(minutes=5))
        self.assertEqual(self.store.get(hid)["reason"], "AWS is busy, retrying (attempt 2)")
        # AWS answers again: the checks pass and the update runs by itself.
        self.cfn.busy_pages = False
        r = u.native_poll(self.store, ROLE_ENV, NOW + timedelta(minutes=6))
        self.assertEqual(r["native"], "executing")
        entry = self.store.get(hid)
        self.assertEqual(entry["status"], "running")
        self.assertNotIn("busy", entry)
        self.assertEqual(entry["reason"], "")
        self.assertEqual(entry["preflight"], [])  # never a "couldn't check" refusal

    def test_a_throttled_preflight_is_never_a_refusal(self):
        # What happened live: the deploy role's check read was throttled and
        # came back as "couldn't check", which failed the update at once.
        iam, u._clients["iam"] = u._clients["iam"], BusyIam()
        try:
            hid = self.prepared(actor="auto")
            r = u.native_poll(self.store, ROLE_ENV, NOW)
            self.assertEqual(r["native"], "busy")
            self.assertEqual(self.store.get(hid)["status"], "preparing")
            self.assertNotIn("preflight", self.store.get(hid))
            u._clients["iam"].busy = False
            self.assertEqual(u.native_poll(self.store, ROLE_ENV, NOW + timedelta(minutes=5))["native"], "executing")
            self.assertEqual(self.store.get(hid)["preflight"], [])
        finally:
            u._clients["iam"] = iam

    def test_still_throttled_after_the_bound_fails_in_plain_words_and_starts_again_by_itself(self):
        hid = self.prepared()
        self.cfn.busy_pages = True
        u.native_poll(self.store, ROLE_ENV, NOW)
        later = NOW + u.BUSY_LIMIT + timedelta(minutes=1)
        r = u.native_poll(self.store, ROLE_ENV, later)
        self.assertEqual(r["status"], "failed")
        row = self.store.get(hid)
        self.assertEqual(row["status"], "failed")
        at = later + u.BUSY_RETRY_AFTER
        self.assertIn("AWS was busy", row["summary"])
        self.assertIn("Nothing changed: your install stayed on v1.0.0.", row["summary"])
        self.assertEqual(row["fix"], f"Nothing to do: CoderHelm starts it again by itself at {at.strftime('%H:%M')} UTC.")
        self.assertIn("Rate exceeded", row["technical"])
        for text in (row["reason"], row["summary"], row["fix"]):
            self.assertTrue(no_press(text), text)
        self.assertEqual(self.cfn.deleted, ["arn:cs/1"])
        # Never counted against the release, and the email asks nothing of anyone.
        self.assertNotIn("v1.1.0", self.store.get("UPDATE_STATE").get("failed_versions") or [])
        self.assertTrue(all(no_press(a) for sent in self.sent for a in sent))
        req = self.store.get("UPDATE_REQUEST")
        self.assertEqual((req["status"], req["version"], req["actor"], req["failed_id"]), ("waiting", "v1.1.0", ADMIN, hid))
        self.assertEqual(req["retry_at"], at.isoformat())
        self.assertTrue(no_press(req["waiting_for"]))
        # Not before its time...
        self.cfn.busy_pages = False
        u.follow_request(self.store, ROLE_ENV, later + timedelta(minutes=10))
        self.assertEqual(len(self.cfn.created), 1)
        # ...then the scheduled poll starts it again, as the same admin's press.
        u.handle({"action": "native_poll"}, ROLE_ENV, self.store, at + timedelta(minutes=1), fetch=lambda repo: [])
        self.assertEqual(len(self.cfn.created), 2)
        req = self.store.get("UPDATE_REQUEST")
        self.assertEqual(req["status"], "started")
        self.assertEqual(self.store.get(req["id"])["actor"], ADMIN)

    def test_cancel_stops_the_automatic_retry(self):
        self.prepared()
        self.cfn.busy_pages = True
        u.native_poll(self.store, ROLE_ENV, NOW)
        later = NOW + u.BUSY_LIMIT + timedelta(minutes=1)
        u.native_poll(self.store, ROLE_ENV, later)
        r = u.handle({"action": "cancel_request", "actor": ADMIN}, ROLE_ENV, self.store, later)
        self.assertTrue(r["cancelled"])
        self.cfn.busy_pages = False
        u.follow_request(self.store, ROLE_ENV, later + u.BUSY_RETRY_AFTER * 2)
        self.assertEqual(len(self.cfn.created), 1)
        self.assertEqual(self.store.get("UPDATE_REQUEST")["status"], "ended")

    # ── the change set can't be created: the start itself is throttled ───

    def test_a_throttled_start_waits_and_starts_once_aws_answers(self):
        self.cfn.busy_create = True
        r = u.handle({"action": "update_now", "version": "v1.1.0", "actor": ADMIN}, ROLE_ENV, self.store, NOW,
                     fetch=lambda repo: [], probe=lambda env, p: {"results": []})
        self.assertEqual(r["state"], "checking")
        self.assertEqual(r["request"]["status"], "waiting")
        self.assertEqual(r["request"]["waiting_for"], "AWS is busy, retrying (attempt 1)")
        self.assertEqual(r["request"]["busy"]["attempts"], 1)
        self.assertEqual(self.store.history(), [])  # nothing started, nothing failed
        u.follow_request(self.store, ROLE_ENV, NOW + timedelta(minutes=5))
        self.assertEqual(self.store.get("UPDATE_REQUEST")["waiting_for"], "AWS is busy, retrying (attempt 2)")
        self.cfn.busy_create = False
        req = u.follow_request(self.store, ROLE_ENV, NOW + timedelta(minutes=6))
        self.assertEqual(req["status"], "started")
        self.assertEqual(self.store.get(req["id"])["status"], "preparing")

    def test_a_start_throttled_past_the_bound_fails_and_starts_again_by_itself(self):
        self.cfn.busy_create = True
        u.handle({"action": "update_now", "version": "v1.1.0", "actor": ADMIN}, ROLE_ENV, self.store, NOW,
                 fetch=lambda repo: [], probe=lambda env, p: {"results": []})
        later = NOW + u.BUSY_LIMIT + timedelta(minutes=1)
        req = u.follow_request(self.store, ROLE_ENV, later)
        self.assertEqual(req["status"], "waiting")
        row = self.store.get(req["failed_id"])
        self.assertEqual(row["status"], "failed")
        self.assertTrue(row["summary"].startswith("The update couldn't start: AWS was busy"))
        self.assertIn("starts it again by itself", row["fix"])
        self.assertTrue(no_press(row["reason"]))
        self.cfn.busy_create = False
        req = u.follow_request(self.store, ROLE_ENV, later + u.BUSY_RETRY_AFTER)
        self.assertEqual(req["status"], "started")

    # ── the release check and status reads ──────────────────────────────

    def test_a_throttled_model_check_stays_pending_never_refused(self):
        self.store.put("MANIFEST#v1.1.0", {"verified": True, "manifest": {"calls": ["sonnet"]}, "template_sha256": "x"})

        def busy(env, payload):
            raise TooManyRequestsException("Rate exceeded")
        mc = u.model_check(self.store, ENV, NOW, "v1.1.0", u.RELEASE_REPO, busy)
        self.assertTrue(mc["pending"])
        self.assertEqual(u.failures(mc), [])
        self.assertIn("AWS is busy, retrying (attempt 1)", mc["note"])
        mc = u.model_check(self.store, ENV, NOW + timedelta(minutes=1), "v1.1.0", u.RELEASE_REPO, busy)
        self.assertIn("attempt 2", mc["note"])
        mc = u.model_check(self.store, ENV, NOW + timedelta(minutes=2), "v1.1.0", u.RELEASE_REPO,
                           lambda env, p: {"results": [{"model": "sonnet", "status": "ok"}]})
        self.assertTrue(mc["ok"])
        self.assertNotIn("busy", mc)

    def test_the_status_route_answers_while_aws_is_busy(self):
        hid = self.prepared()
        self.cfn.busy_pages = True
        r = u.handle({"action": "status"}, ROLE_ENV, self.store, NOW, fetch=lambda repo: [])
        row = next(h for h in r["history"] if h["id"] == hid)
        self.assertEqual((row["status"], row["reason"]), ("preparing", "AWS is busy, retrying (attempt 1)"))

    def test_no_throttling_message_hands_the_retry_to_a_person(self):
        raw = "ClientError: An error occurred (Throttling) when calling the DescribeChangeSet operation: Rate exceeded"
        for phase in (u.PREPARE, u.ROLLED_BACK):
            for retry_at in (None, NOW):
                d = u.explain_failure(raw, "v1.1.0", "v1.0.0", phase, retry_at=retry_at)
                self.assertTrue(no_press(d["reason"]), (phase, retry_at, d["reason"]))
        self.assertTrue(no_press(u.busy_note({"busy": {"attempts": 3}})))


if __name__ == "__main__":
    unittest.main()
