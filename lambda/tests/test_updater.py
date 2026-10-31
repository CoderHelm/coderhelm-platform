"""The self-host updater: when it updates, when it only notifies, and what it records.

Run: python3 -m unittest discover -s lambda/tests
"""

import importlib.util
import json
import pathlib
import sys
import unittest
from datetime import datetime, timedelta, timezone

PATH = pathlib.Path(__file__).resolve().parents[1] / "updater" / "handler.py"
spec = importlib.util.spec_from_file_location("updater_handler", PATH)
u = importlib.util.module_from_spec(spec)
sys.modules["updater_handler"] = u
spec.loader.exec_module(u)


def rel(tag, pre=False, body="notes"):
    return {"tag_name": tag, "prerelease": pre, "draft": False, "body": body, "published_at": "2026-10-01T00:00:00Z", "html_url": f"https://x/{tag}"}


SUNDAY_3AM = datetime(2026, 10, 25, 3, 15, tzinfo=timezone.utc)  # a Sunday
MONDAY = datetime(2026, 10, 26, 3, 15, tzinfo=timezone.utc)
ON = dict(u.DEFAULT_SETTINGS, auto=True)


class FakeDynamo:
    def __init__(self):
        self.items = {}

    def get_item(self, TableName, Key):
        k = (Key["pk"]["S"], Key["sk"]["S"])
        return {"Item": self.items[k]} if k in self.items else {}

    def put_item(self, TableName, Item):
        self.items[(Item["pk"]["S"], Item["sk"]["S"])] = Item

    def query(self, TableName, KeyConditionExpression, ExpressionAttributeValues, ScanIndexForward, Limit):
        p, s = ExpressionAttributeValues[":p"]["S"], ExpressionAttributeValues[":s"]["S"]
        ks = sorted((k for k in self.items if k[0] == p and k[1].startswith(s)), key=lambda k: k[1], reverse=not ScanIndexForward)
        return {"Items": [self.items[k] for k in ks[:Limit]]}


class Versions(unittest.TestCase):
    def test_compare_and_majors(self):
        self.assertEqual(u.compare("v1.10.0", "v1.9.9"), 1)
        self.assertEqual(u.compare("v1.2.0-rc.1", "v1.2.0"), -1)
        self.assertEqual(u.compare("1.2.0", "v1.2.0"), 0)
        self.assertTrue(u.is_major("v1.4.0", "v2.0.0"))
        self.assertFalse(u.is_major("v1.4.0", "v1.5.0"))
        self.assertIsNone(u.parse_version("latest"))

    def test_channel_picks(self):
        rs = [rel("v1.1.0"), rel("v1.2.0-rc.1", pre=True), rel("v0.9.0"), {"tag_name": "nightly"}]
        self.assertEqual(u.pick_latest(rs, "stable", "v1.0.0")["version"], "v1.1.0")
        self.assertEqual(u.pick_latest(rs, "preview", "v1.0.0")["version"], "v1.2.0-rc.1")
        self.assertIsNone(u.pick_latest(rs, "stable", "v1.1.0"))
        self.assertTrue(u.pick_latest([rel("v2.0.0")], "stable", "v1.1.0")["breaking"])


class Window(unittest.TestCase):
    def test_day_hour_and_timezone(self):
        s = dict(u.DEFAULT_SETTINGS, day="sun", hour=3)
        self.assertTrue(u.in_window(SUNDAY_3AM, s))
        self.assertFalse(u.in_window(SUNDAY_3AM.replace(hour=4), s))
        self.assertFalse(u.in_window(MONDAY, s))
        self.assertTrue(u.in_window(MONDAY, dict(s, day="daily")))
        # 03:15 UTC is 05:15 in Berlin in late October (CEST ends that day at 03:00 UTC: 04:15 CET).
        berlin = dict(s, timezone="Europe/Berlin", hour=4)
        self.assertTrue(u.in_window(SUNDAY_3AM, berlin))

    def test_settings_validation(self):
        self.assertEqual(u.validate_settings({"day": "Saturday", "hour": 22})["day"], "sat")
        for bad in ({"hour": 24}, {"hour": True}, {"channel": "beta"}, {"majors": "never"}, {"timezone": "Mars/Base"}, {"auto": "yes"}, {"colour": 1}):
            with self.assertRaises(ValueError, msg=bad):
                u.validate_settings(bad)


class Decide(unittest.TestCase):
    def test_off_only_notifies_once(self):
        d = u.decide(SUNDAY_3AM, u.DEFAULT_SETTINGS, "v1.0.0", [rel("v1.1.0")], {})
        self.assertEqual((d["action"], d["reason"]), ("notify", "auto-update is off"))
        d = u.decide(SUNDAY_3AM, u.DEFAULT_SETTINGS, "v1.0.0", [rel("v1.1.0")], {"notified_version": "v1.1.0"})
        self.assertEqual(d["action"], "none")

    def test_updates_in_the_window_only(self):
        self.assertEqual(u.decide(SUNDAY_3AM, ON, "v1.0.0", [rel("v1.1.0")], {})["action"], "update")
        d = u.decide(MONDAY, ON, "v1.0.0", [rel("v1.1.0")], {"notified_version": "v1.1.0"})
        self.assertEqual((d["action"], d["reason"]), ("none", "waiting for the maintenance window"))

    def test_majors_wait_for_an_admin_unless_allowed(self):
        d = u.decide(SUNDAY_3AM, ON, "v1.4.0", [rel("v2.0.0")], {})
        self.assertEqual((d["action"], d["reason"]), ("notify", "a major release waits for an admin"))
        self.assertEqual(u.decide(SUNDAY_3AM, dict(ON, majors="auto"), "v1.4.0", [rel("v2.0.0")], {})["action"], "update")

    def test_preview_channel(self):
        self.assertEqual(u.decide(SUNDAY_3AM, ON, "v1.0.0", [rel("v1.1.0-rc.1", pre=True)], {})["action"], "none")
        self.assertEqual(u.decide(SUNDAY_3AM, dict(ON, channel="preview"), "v1.0.0", [rel("v1.1.0-rc.1", pre=True)], {})["action"], "update")

    def test_running_and_failed_versions(self):
        st = {"running_since": (SUNDAY_3AM - timedelta(minutes=20)).isoformat()}
        self.assertEqual(u.decide(SUNDAY_3AM, ON, "v1.0.0", [rel("v1.1.0")], st)["reason"], "an update is running")
        stale = {"running_since": (SUNDAY_3AM - timedelta(hours=5)).isoformat()}
        self.assertEqual(u.decide(SUNDAY_3AM, ON, "v1.0.0", [rel("v1.1.0")], stale)["action"], "update")
        d = u.decide(SUNDAY_3AM, ON, "v1.0.0", [rel("v1.1.0")], {"failed_versions": ["v1.1.0"], "notified_version": "v1.1.0"})
        self.assertEqual(d["action"], "none")


class Handler(unittest.TestCase):
    def setUp(self):
        self.db = FakeDynamo()
        self.store = u.Store(self.db, "t")
        self.builds, self.notices = [], []
        u.start_build = lambda env, version, actor, mode, hid, extra=None: self.builds.append((version, actor, mode, hid)) or f"b-{len(self.builds)}"
        u.notify = lambda subject, text, env=None, **kw: self.notices.append(subject) or 1
        # Releases checked by the build and listing no models: nothing to check.
        real = u.verified_manifest
        u.verified_manifest = lambda store, version: (True, None)
        self.addCleanup(setattr, u, "verified_manifest", real)
        self.env = {"CODERHELM_VERSION": "v1.0.0", "APP_URL": "https://helm.example.org", "UPDATE_PROJECT_NAME": "p"}

    def fetch(self, _repo):
        return [rel("v1.1.0", body="Fixes."), rel("v2.0.0")]

    def test_status_reports_current_latest_and_history(self):
        s = u.handle({"action": "status"}, dict(self.env, CODERHELM_COMMIT="abc123"), self.store, SUNDAY_3AM, self.fetch)
        self.assertEqual(s["current"]["version"], "v1.0.0")
        self.assertEqual(s["current"]["commit"], "abc123")
        self.assertEqual(s["latest"]["version"], "v2.0.0")
        self.assertTrue(s["latest"]["breaking"])
        self.assertTrue(s["update_available"])
        self.assertEqual(s["history"], [])
        self.assertIsNone(s["install"], "no prefix or region: no install to name")

    def test_status_names_the_install_for_the_guided_uninstall(self):
        env = dict(self.env, RESOURCE_PREFIX="coderhelm-prod-eu", AWS_REGION="eu-west-1")
        s = u.handle({"action": "status"}, env, self.store, SUNDAY_3AM, self.fetch)
        self.assertEqual(s["install"], {"stage": "prod-eu", "region": "eu-west-1"})
        self.assertIsNone(u.install_identity(dict(env, RESOURCE_PREFIX="acme-helm")), "a custom prefix is not a coderhelm-<name> install")

    def test_releases_are_cached_for_an_hour(self):
        calls = []
        fetch = lambda r: calls.append(r) or [rel("v1.1.0")]
        u.handle({"action": "status"}, self.env, self.store, SUNDAY_3AM, fetch)
        u.handle({"action": "status"}, self.env, self.store, SUNDAY_3AM + timedelta(minutes=30), fetch)
        self.assertEqual(len(calls), 1)
        u.handle({"action": "status"}, self.env, self.store, SUNDAY_3AM + timedelta(minutes=61), fetch)
        self.assertEqual(len(calls), 2)

    def test_tick_with_auto_update_on_starts_the_build_and_notifies_once(self):
        u.handle({"action": "settings", "settings": {"auto": True, "majors": "ask"}}, self.env, self.store, SUNDAY_3AM, self.fetch)
        d = u.handle({"action": "tick"}, self.env, self.store, SUNDAY_3AM, lambda r: [rel("v1.1.0")])
        self.assertEqual(d["action"], "update")
        self.assertEqual(self.builds[0][:3], ("v1.1.0", "auto", "update"))
        self.assertEqual(self.notices, ["CoderHelm v1.1.0 is available"])
        again = u.handle({"action": "tick"}, self.env, self.store, SUNDAY_3AM + timedelta(minutes=1), lambda r: [rel("v1.1.0")])
        self.assertEqual(again["reason"], "an update is running")
        self.assertEqual(len(self.builds), 1)

    def test_update_now_and_finished_failure_marks_the_version(self):
        e = u.handle({"action": "update_now", "actor": "owner@example.org"}, self.env, self.store, SUNDAY_3AM, self.fetch)
        self.assertEqual((e["version"], e["actor"], e["status"]), ("v2.0.0", "owner@example.org", "running"))
        with self.assertRaises(ValueError):
            u.handle({"action": "update_now"}, self.env, self.store, SUNDAY_3AM, self.fetch)
        r = u.handle({"action": "finished", "status": "rolled_back", "reason": "health check failed after deploy", "history_id": e["id"]},
                     self.env, self.store, SUNDAY_3AM, self.fetch)
        self.assertEqual(r["status"], "rolled_back")
        h = self.store.history()[0]
        self.assertEqual((h["status"], h["reason"]), ("rolled_back", "health check failed after deploy"))
        self.assertIn("v2.0.0", self.store.get("UPDATE_STATE")["failed_versions"])
        self.assertTrue(self.notices[-1].startswith("CoderHelm rolled back"))
        # The dashboard's preflight reads the failed versions from status.
        s = u.handle({"action": "status"}, self.env, self.store, SUNDAY_3AM, self.fetch)
        self.assertEqual(s["failed_versions"], ["v2.0.0"])

    def test_rollback_targets_the_version_before_the_last_success(self):
        with self.assertRaises(ValueError):
            u.handle({"action": "rollback"}, self.env, self.store, SUNDAY_3AM, self.fetch)
        e = u.handle({"action": "update_now", "version": "v1.1.0", "actor": "a"}, self.env, self.store, SUNDAY_3AM, self.fetch)
        u.handle({"action": "finished", "status": "succeeded", "history_id": e["id"]}, self.env, self.store, SUNDAY_3AM, self.fetch)
        later = SUNDAY_3AM + timedelta(hours=1)
        r = u.handle({"action": "rollback", "actor": "a"}, dict(self.env, CODERHELM_VERSION="v1.1.0"), self.store, later, self.fetch)
        self.assertEqual((r["version"], r["mode"]), ("v1.0.0", "rollback"))

    def test_bad_settings_are_refused(self):
        with self.assertRaises(ValueError):
            u.handle({"action": "settings", "settings": {"hour": 99}}, self.env, self.store, SUNDAY_3AM, self.fetch)
        self.assertIsNone(self.store.get("AUTO_UPDATE"))


class NoDowngrade(unittest.TestCase):
    """Only forward, unless an admin rolls back to a version this install ran; a stale list can't hide a release."""

    def setUp(self):
        self.db = FakeDynamo()
        self.store = u.Store(self.db, "t")
        self.builds = []
        u.start_build = lambda env, version, actor, mode, hid, extra=None: self.builds.append((version, mode)) or "b"
        u.notify = lambda subject, text, env=None, **kw: 1
        self.env = {"CODERHELM_VERSION": "v1.2.0", "APP_URL": "https://helm.example.org", "UPDATE_PROJECT_NAME": "p"}
        self.fetch = lambda _r: [rel("v1.3.0")]
        # Signed releases that list no models: the model check has nothing to hold.
        real = u.verified_manifest
        u.verified_manifest = lambda store, version: (True, None)
        self.addCleanup(setattr, u, "verified_manifest", real)

    def test_update_now_never_goes_back_or_sideways(self):
        for v in ("v1.1.0", "v1.2.0", "v1.2.0-rc.1"):
            with self.assertRaisesRegex(ValueError, "not newer"):
                u.handle({"action": "update_now", "version": v}, self.env, self.store, SUNDAY_3AM, self.fetch)
        self.assertEqual(self.builds, [])
        u.handle({"action": "update_now", "version": "v1.3.0"}, self.env, self.store, SUNDAY_3AM, self.fetch)
        self.assertEqual(self.builds, [("v1.3.0", "update")])

    def test_rollback_only_to_a_version_this_install_ran(self):
        # Never ran v1.0.0 (an older signed release): refused.
        with self.assertRaisesRegex(ValueError, "not a version this install ran"):
            u.handle({"action": "rollback", "version": "v1.0.0"}, self.env, self.store, SUNDAY_3AM, self.fetch)
        # The running version is not a rollback target either.
        with self.assertRaisesRegex(ValueError, "not a version this install ran"):
            u.handle({"action": "rollback", "version": "v1.2.0"}, self.env, self.store, SUNDAY_3AM, self.fetch)
        # Run v1.2.0, update to v1.3.0: now v1.2.0 is a version it ran.
        e = u.handle({"action": "update_now", "version": "v1.3.0"}, self.env, self.store, SUNDAY_3AM, self.fetch)
        u.handle({"action": "finished", "status": "succeeded", "history_id": e["id"]}, self.env, self.store, SUNDAY_3AM, self.fetch)
        self.assertEqual(self.store.get("UPDATE_STATE")["ran_versions"], ["v1.2.0", "v1.3.0"])
        on13 = dict(self.env, CODERHELM_VERSION="v1.3.0")
        r = u.handle({"action": "rollback", "version": "v1.2.0", "actor": "admin"}, on13, self.store, SUNDAY_3AM + timedelta(hours=1), self.fetch)
        self.assertEqual((r["version"], r["mode"]), ("v1.2.0", "rollback"))
        with self.assertRaisesRegex(ValueError, "not a version this install ran"):
            u.handle({"action": "rollback", "version": "v1.1.0"}, on13, self.store, SUNDAY_3AM + timedelta(hours=1), self.fetch)

    def test_a_stale_release_list_is_not_believed(self):
        u.handle({"action": "status"}, self.env, self.store, SUNDAY_3AM, lambda r: [rel("v1.3.0"), rel("v1.2.0")])
        # An hour later the "latest" list no longer has v1.3.0 (a stale mirror or cache).
        later = SUNDAY_3AM + timedelta(hours=2)
        s = u.handle({"action": "status"}, self.env, self.store, later, lambda r: [rel("v1.2.0"), rel("v1.1.0")])
        self.assertIsNone(s["latest"], "nothing older than the running version is offered")
        self.assertTrue(s["releases"]["stale"])
        self.assertIn("v1.3.0", s["releases"]["reason"])
        # A fresh list with v1.3.0 (or newer) is believed again.
        s = u.handle({"action": "status"}, self.env, self.store, later + timedelta(hours=2), lambda r: [rel("v1.4.0"), rel("v1.3.0")])
        self.assertEqual(s["latest"]["version"], "v1.4.0")
        self.assertFalse(s["releases"]["stale"])

    def test_an_old_list_is_reported_stale(self):
        u.handle({"action": "status"}, self.env, self.store, SUNDAY_3AM, self.fetch)
        def down(_r):
            raise OSError("GitHub is down")
        s = u.handle({"action": "status"}, self.env, self.store, SUNDAY_3AM + timedelta(days=4), down)
        self.assertTrue(s["releases"]["stale"])

    def test_releases_only_from_coderhelms_repository(self):
        calls = []
        s = u.handle({"action": "status"}, dict(self.env, RELEASE_REPO="evil/coderhelm-platform"), self.store, SUNDAY_3AM, lambda r: calls.append(r) or [rel("v9.0.0")])
        self.assertEqual(calls, [])
        self.assertIsNone(s["latest"])
        with self.assertRaises(ValueError):
            u.fetch_releases("evil/coderhelm-platform")


MANIFEST = {
    "schema": 1, "version": "v1.1.0",
    "calls": ["claude-opus-5-5", "claude-sonnet-5-5"],
    "bedrock": {
        "claude-opus-5-5": {"us.": "us.anthropic.claude-opus-5-5", "eu.": "eu.anthropic.claude-opus-5-5"},
        "claude-sonnet-5-5": {"us.": "us.anthropic.claude-sonnet-5-5", "eu.": "eu.anthropic.claude-sonnet-5-5"},
    },
}

REFUSED = {
    "status": "fail", "code": "use_case_required",
    "detail": "Bedrock refused us.anthropic.claude-sonnet-5-5: Anthropic's use case details haven't been submitted for this AWS account.",
    "fix": "Submit the use case details on the Bedrock console's Model access page (a person fills in the form once per account), then check again.",
    "url": "https://console.aws.amazon.com/bedrock/home?region=us-east-1#/modelaccess",
}


class FakeWorker:
    """Stands in for the worker's model check: no real model calls."""

    def __init__(self, refuse=("claude-sonnet-5-5",)):
        self.refuse = set(refuse)
        self.payloads = []

    def __call__(self, env, payload):
        self.payloads.append(payload)
        out = []
        for t in payload["targets"]:
            m = t["model"]
            if m in self.refuse:
                out.append(dict(REFUSED, model=m, id=t["bedrock_ids"].get("us.", m)))
            else:
                out.append({"model": m, "id": t["bedrock_ids"].get("us.", m), "status": "pass", "code": "ok", "detail": "answered"})
        return {"provider": "bedrock", "region": "us-east-1", "inference_prefix": "us.", "results": out}


class ModelCheck(unittest.TestCase):
    """The check before an update: a refused model holds it, a pass lets it go."""

    def setUp(self):
        self.db = FakeDynamo()
        self.store = u.Store(self.db, "t")
        self.builds, self.notices, self.texts = [], [], []
        real_build, real_notify = u.start_build, u.notify
        u.start_build = lambda env, version, actor, mode, hid, extra=None: self.builds.append((version, actor, mode)) or "b-1"
        u.notify = lambda subject, text, env=None, **kw: self.notices.append(subject) or self.texts.append(text) or 1
        self.addCleanup(setattr, u, "start_build", real_build)
        self.addCleanup(setattr, u, "notify", real_notify)
        real_check = u.start_check
        u.start_check = lambda env, version: self.builds.append((version, "check", "release-check")) or "c-1"
        self.addCleanup(setattr, u, "start_check", real_check)
        self.env = {"CODERHELM_VERSION": "v1.0.0", "APP_URL": "https://helm.example.org", "UPDATE_PROJECT_NAME": "p",
                    "WORKER_FUNCTION_NAME": "coderhelm-prod-worker", "CONFIGURED_MODELS": "claude-opus-5-5,claude-sonnet-5-5"}
        self.worker = FakeWorker()
        # The update build checked v1.1.0's signature and models.json.
        self.store.put("MANIFEST#v1.1.0", {"verified": True, "manifest": MANIFEST})
        u.handle({"action": "settings", "settings": {"auto": True, "day": "daily"}}, self.env, self.store, SUNDAY_3AM, self.releases)

    def releases(self, _repo):
        return [rel("v1.1.0")]

    def tick(self, at):
        return u.handle({"action": "tick"}, self.env, self.store, at, self.releases, self.worker)

    def test_a_refused_model_holds_the_update_and_tells_the_admins_once(self):
        d = self.tick(SUNDAY_3AM)
        self.assertEqual(d["action"], "notify")
        self.assertIn("model this release calls was refused", d["reason"])
        self.assertEqual(self.builds, [])
        self.assertIn("Update to v1.1.0 needs your action", self.notices)
        text = self.texts[self.notices.index("Update to v1.1.0 needs your action")]
        self.assertIn("claude-sonnet-5-5", text)
        self.assertIn("#/modelaccess", text)
        self.assertIn("done by a person in the AWS console", text)
        # The worker was asked with the release's Bedrock ids per prefix.
        t = self.worker.payloads[0]["targets"]
        self.assertEqual([x["model"] for x in t], ["claude-opus-5-5", "claude-sonnet-5-5"])
        self.assertEqual(t[0]["bedrock_ids"]["eu."], "eu.anthropic.claude-opus-5-5")
        # Status carries the check for Settings, System and Setup.
        s = u.handle({"action": "status"}, self.env, self.store, SUNDAY_3AM, self.releases)
        self.assertFalse(s["model_check"]["ok"])
        self.assertTrue(s["model_check"]["results"][1]["required"])
        # An hour later: not checked again, not notified again.
        self.tick(SUNDAY_3AM + timedelta(hours=1))
        self.assertEqual(len(self.worker.payloads), 1)
        self.assertEqual(self.notices.count("Update to v1.1.0 needs your action"), 1)

    def test_the_daily_retry_applies_the_update_once_it_passes(self):
        self.tick(SUNDAY_3AM)
        self.assertEqual(self.builds, [])
        # A person submitted the form; the next day's check passes.
        self.worker.refuse = set()
        nextday = SUNDAY_3AM + timedelta(days=1)
        d = self.tick(nextday)
        self.assertEqual(d["action"], "update")
        self.assertEqual(self.builds, [("v1.1.0", "auto", "update")])
        # Checked on first sight, the daily retry, and again right before applying.
        self.assertEqual(len(self.worker.payloads), 3)
        self.assertEqual(self.notices.count("Update to v1.1.0 needs your action"), 1)

    def test_a_pass_applies_in_the_window(self):
        self.worker.refuse = set()
        d = self.tick(SUNDAY_3AM)
        self.assertEqual(d["action"], "update")
        self.assertEqual(self.builds[0][0], "v1.1.0")
        self.assertNotIn("Update to v1.1.0 needs your action", self.notices)

    def test_a_refusal_found_right_before_applying_stops_it(self):
        # Checked an hour before the window: passes, nothing applied yet.
        self.worker.refuse = set()
        self.assertEqual(self.tick(SUNDAY_3AM - timedelta(hours=1))["action"], "notify")
        # Model access was turned off since: the check right before applying stops it.
        self.worker.refuse = {"claude-opus-5-5"}
        d = self.tick(SUNDAY_3AM)
        self.assertEqual(d["action"], "none")
        self.assertEqual(self.builds, [])
        self.assertIn("Update to v1.1.0 needs your action", self.notices)

    def test_update_now_is_refused_with_the_check_and_no_override(self):
        with self.assertRaises(u.ModelsBlocked) as e:
            u.handle({"action": "update_now", "actor": "a@example.org", "skip_check": True}, self.env, self.store,
                     SUNDAY_3AM, self.releases, self.worker)
        self.assertTrue(str(e.exception).startswith("Update to v1.1.0 needs your action"))
        self.assertEqual(e.exception.record["results"][1]["code"], "use_case_required")
        self.assertNotIn("skippable", e.exception.record)
        self.assertEqual(self.builds, [])
        # Fixed: Update now goes ahead.
        self.worker.refuse = set()
        e2 = u.handle({"action": "update_now", "actor": "a@example.org"}, self.env, self.store, SUNDAY_3AM,
                      self.releases, self.worker)
        self.assertEqual(e2["version"], "v1.1.0")

    def test_skip_is_offered_only_for_a_model_the_release_doesnt_call(self):
        # The worker reports a model the release doesn't call (not required).
        class Extra(FakeWorker):
            def __call__(self, env, payload):
                out = super().__call__(env, payload)
                out["results"].append(dict(REFUSED, model="claude-haiku-4-5", id="us.anthropic.claude-haiku-4-5-20251001-v1:0"))
                return out
        self.worker = Extra(refuse=())
        with self.assertRaises(u.ModelsBlocked) as e:
            u.handle({"action": "update_now", "actor": "a"}, self.env, self.store, SUNDAY_3AM, self.releases, self.worker)
        self.assertTrue(e.exception.record["skippable"])
        self.assertIn("you can skip this check", str(e.exception))
        done = u.handle({"action": "update_now", "actor": "a", "skip_check": True}, self.env, self.store, SUNDAY_3AM,
                        self.releases, self.worker)
        self.assertEqual(done["note"], "model check skipped for claude-haiku-4-5")

    def test_check_again_runs_now(self):
        self.tick(SUNDAY_3AM)
        self.worker.refuse = set()
        r = u.handle({"action": "model_check"}, self.env, self.store, SUNDAY_3AM + timedelta(minutes=5),
                     self.releases, self.worker)
        self.assertTrue(r["model_check"]["ok"])
        self.assertEqual(len(self.worker.payloads), 2)

    def test_a_signed_release_without_a_manifest_has_nothing_to_check(self):
        self.store.put("MANIFEST#v1.1.0", {"verified": True, "manifest": None})
        r = u.handle({"action": "model_check"}, self.env, self.store, SUNDAY_3AM, self.releases, self.worker)
        self.assertTrue(r["model_check"]["ok"])
        self.assertEqual(self.worker.payloads, [])

    def test_the_manifest_is_read_only_after_the_signature_check(self):
        # Nothing verified yet: the build is started in its read-only mode, and the update waits.
        self.db.items.pop(("SYSTEM", "MANIFEST#v1.1.0"))
        self.worker.refuse = set()
        d = self.tick(SUNDAY_3AM)
        self.assertEqual(self.builds, [("v1.1.0", "check", "release-check")])
        self.assertEqual(d["action"], "notify")
        self.assertEqual(self.worker.payloads, [])
        self.assertTrue(self.store.get("MODEL_CHECK")["pending"])
        with self.assertRaises(u.ModelsBlocked) as e:
            u.handle({"action": "update_now", "actor": "a"}, self.env, self.store, SUNDAY_3AM, self.releases, self.worker)
        self.assertIn("signature", str(e.exception))
        # Not started again within 30 minutes.
        self.assertEqual(self.builds, [("v1.1.0", "check", "release-check")])
        # A failed signature check trusts nothing.
        u.handle({"action": "models_verified", "version": "v1.1.0", "status": "failed", "reason": "bad signature"},
                 self.env, self.store, SUNDAY_3AM, self.releases, self.worker)
        self.assertIsNone(self.store.get("MANIFEST#v1.1.0"))
        # The build reports the verified file: the check runs at once.
        r = u.handle({"action": "models_verified", "version": "v1.1.0", "status": "verified", "manifest": MANIFEST},
                     self.env, self.store, SUNDAY_3AM, self.releases, self.worker)
        self.assertTrue(r["model_check"]["ok"])
        self.assertEqual(len(self.worker.payloads), 1)
        # A manifest for another release is refused.
        with self.assertRaises(ValueError):
            u.handle({"action": "models_verified", "version": "v1.2.0", "status": "verified", "manifest": MANIFEST},
                     self.env, self.store, SUNDAY_3AM, self.releases, self.worker)

    def test_the_source_never_downloads_models_json_itself(self):
        src = PATH.read_text()
        self.assertNotIn("releases/download", src)

    def test_a_worker_that_doesnt_answer_holds_the_update(self):
        def broken(env, payload):
            raise RuntimeError("timeout")
        d = u.handle({"action": "tick"}, self.env, self.store, SUNDAY_3AM, self.releases, broken)
        self.assertEqual(d["action"], "notify")
        self.assertEqual(self.builds, [])
        self.assertEqual(self.store.get("MODEL_CHECK")["results"][0]["code"], "unreachable")

    def test_the_handler_returns_the_check_with_a_refusal(self):
        real_handle = u.handle
        u.handle = lambda event, env, store, now: real_handle(event, env, store, now, self.releases, self.worker)
        self.addCleanup(setattr, u, "handle", real_handle)
        real_client = u.client
        u.client = lambda name, region=None: self.db
        self.addCleanup(setattr, u, "client", real_client)
        import os
        old = dict(os.environ)
        os.environ.update(self.env, SETTINGS_TABLE_NAME="t")
        self.addCleanup(lambda: (os.environ.clear(), os.environ.update(old)))
        out = u.handler({"action": "update_now", "actor": "a"}, None)
        self.assertFalse(out["ok"])
        self.assertEqual(out["model_check"]["version"], "v1.1.0")


class NeverAcceptsTerms(unittest.TestCase):
    """The updater only checks: no subscription, agreement or form is ever sent."""

    def test_the_source_never_calls_an_accepting_api(self):
        src = PATH.read_text()
        for word in ("aws-marketplace", "marketplace", "create_foundation_model_agreement",
                     "put_use_case_for_model_access", "accept_agreement", "subscribe("):
            code = "\n".join(l for l in src.splitlines() if not l.strip().startswith("#") and '"""' not in l)
            self.assertNotIn(word, code.lower() if word.islower() else code, word)

    def test_the_worker_call_is_the_only_aws_call(self):
        calls = []

        class Recorder:
            def __getattr__(self, name):
                def f(**kw):
                    calls.append(name)
                    return {"Payload": type("P", (), {"read": lambda self: json.dumps({"results": []}).encode()})()}
                return f
        real = u.client
        u.client = lambda name, region=None: calls.append(f"client:{name}") or Recorder()
        self.addCleanup(setattr, u, "client", real)
        u.invoke_worker({"WORKER_FUNCTION_NAME": "w"}, {"targets": []})
        self.assertEqual(calls, ["client:lambda", "invoke"])


class FakeLambda:
    def __init__(self, conflicts=0):
        self.env = {"f-gateway": {"A": "1"}, "f-worker": {}}
        self.conflicts = conflicts
        self.updates = []

    def get_function_configuration(self, FunctionName):
        return {"Environment": {"Variables": dict(self.env[FunctionName])}}

    def update_function_configuration(self, FunctionName, Environment):
        if self.conflicts:
            self.conflicts -= 1
            raise type("ResourceConflictException", (Exception,), {})("an update is in progress")
        self.env[FunctionName] = Environment["Variables"]
        self.updates.append(FunctionName)


class RefreshSecrets(unittest.TestCase):
    def test_stamps_each_function_and_keeps_its_settings(self):
        lam = FakeLambda()
        self.assertEqual(u.refresh_functions(lam, ["f-gateway", "f-worker"], "v1", sleep=lambda s: None), ["f-gateway", "f-worker"])
        self.assertEqual(lam.env["f-gateway"], {"A": "1", "SECRETS_REVISION": "v1"})
        self.assertEqual(lam.env["f-worker"], {"SECRETS_REVISION": "v1"})

    def test_waits_out_an_update_in_progress(self):
        lam = FakeLambda(conflicts=2)
        self.assertEqual(u.refresh_functions(lam, ["f-gateway"], "v2", sleep=lambda s: None), ["f-gateway"])

    def test_the_action_uses_the_configured_functions_and_a_clean_revision(self):
        lam = FakeLambda()
        u._clients["lambda"] = lam
        try:
            env = {"REFRESH_FUNCTIONS": "f-gateway,f-worker", "SETTINGS_TABLE_NAME": "t"}
            store = u.Store(FakeDynamo(), "t")
            out = u.handle({"action": "refresh_secrets", "revision": "abc-123;rm -rf"}, env, store, SUNDAY_3AM)
        finally:
            u._clients.pop("lambda", None)
        self.assertEqual(out, {"refreshed": ["f-gateway", "f-worker"]})
        self.assertEqual(lam.env["f-worker"]["SECRETS_REVISION"], "abc-123rm-rf")


# ── Your own domain ───────────────────────────────────────────────────────

ARN = "arn:aws:acm:us-east-1:111122223333:certificate/0f0e0d0c-1111-2222-3333-444455556666"
VALIDATION = {"Name": "_a1b2.helm.example.org.", "Type": "CNAME", "Value": "_c3d4.xyz.acm-validations.aws."}


class FakeAcm:
    def __init__(self, status="PENDING_VALIDATION", ready_after=0):
        self.status = status
        self.reason = None
        self.ready_after = ready_after
        self.requests = []

    def request_certificate(self, **kw):
        self.requests.append(kw)
        return {"CertificateArn": ARN}

    def describe_certificate(self, CertificateArn):
        opts = [{"DomainName": "helm.example.org"}]
        if self.ready_after <= 0:
            opts[0]["ResourceRecord"] = VALIDATION
        self.ready_after -= 1
        c = {"Status": self.status, "DomainValidationOptions": opts}
        if self.reason:
            c["FailureReason"] = self.reason
        return {"Certificate": c}


class FakeRoute53:
    def __init__(self, zones):
        self.zones = zones
        self.changes = []

    def list_hosted_zones(self, **kw):
        return {"HostedZones": self.zones, "IsTruncated": False}

    def change_resource_record_sets(self, HostedZoneId, ChangeBatch):
        self.changes.append((HostedZoneId, ChangeBatch))

    def get_hosted_zone(self, Id):
        return {"DelegationSet": {"NameServers": ["ns-1.awsdns-01.org.", "ns-2.awsdns-02.com."]}}


class FakeSsm:
    def get_parameter(self, Name):
        return {"Parameter": {"Value": "d111test.cloudfront.net"}}


class Domain(unittest.TestCase):
    def setUp(self):
        self.db = FakeDynamo()
        self.store = u.Store(self.db, "t")
        self.builds, self.notices = [], []
        self._start, self._notify = u.start_build, u.notify
        u.start_build = lambda env, version, actor, mode, hid, extra=None: self.builds.append((version, actor, mode, extra)) or "b-1"
        u.notify = lambda subject, text, env=None, **kw: self.notices.append(subject) or 1
        self.acm = FakeAcm()
        self.r53 = FakeRoute53([])
        u._clients.update({"acm@us-east-1": self.acm, "route53": self.r53, "ssm": FakeSsm()})
        self.env = {"CODERHELM_VERSION": "v1.2.0", "APP_URL": "https://d111test.cloudfront.net", "UPDATE_PROJECT_NAME": "p",
                    "DISTRIBUTION_DOMAIN_PARAMETER": "/coderhelm-prod/distribution-domain", "RESOURCE_PREFIX": "coderhelm-prod"}

    def tearDown(self):
        u.start_build, u.notify = self._start, self._notify
        for k in ("acm@us-east-1", "route53", "ssm"):
            u._clients.pop(k, None)

    def h(self, event, now=SUNDAY_3AM, env=None):
        return u.handle(event, env or self.env, self.store, now)

    def test_hostnames_are_cleaned_and_bad_ones_refused_in_plain_words(self):
        self.assertEqual(u.clean_hostname(" https://Helm.Example.org/ "), "helm.example.org")
        for bad in ("", "localhost", "helm", "-x.example.org", "d111.cloudfront.net", "a b.example.org"):
            with self.assertRaises(ValueError, msg=bad):
                u.clean_hostname(bad)

    def test_the_longest_public_zone_wins_and_private_zones_never_do(self):
        zones = [
            {"Id": "/hostedzone/ZROOT", "Name": "example.org.", "Config": {"PrivateZone": False}},
            {"Id": "/hostedzone/ZSUB", "Name": "dev.example.org.", "Config": {"PrivateZone": False}},
            {"Id": "/hostedzone/ZPRIV", "Name": "helm.dev.example.org.", "Config": {"PrivateZone": True}},
        ]
        self.assertEqual(u.find_zone(zones, "helm.dev.example.org"), {"name": "dev.example.org", "id": "ZSUB"})
        self.assertEqual(u.find_zone(zones, "example.org")["id"], "ZROOT")
        self.assertIsNone(u.find_zone(zones, "helm.example.net"))
        self.assertIsNone(u.find_zone(zones, "badexample.org"))

    def test_no_domain_reports_the_aws_address(self):
        v = self.h({"action": "domain_status"})
        self.assertEqual((v["state"], v["hostname"], v["target"]), ("none", None, "d111test.cloudfront.net"))
        v = self.h({"action": "domain_status"}, env=dict(self.env, APP_URL="https://helm.example.org"))
        self.assertEqual((v["state"], v["mode"], v["hostname"]), ("live", "config", "helm.example.org"))

    def test_external_dns_shows_both_records_then_attaches_once_issued(self):
        v = self.h({"action": "domain_set", "hostname": "helm.example.org", "actor": "owner@example.org"})
        self.assertEqual((v["state"], v["mode"]), ("validating", "external"))
        self.assertEqual(self.acm.requests[0]["DomainName"], "helm.example.org")
        self.assertEqual(self.acm.requests[0]["ValidationMethod"], "DNS")
        self.assertEqual(v["records"], [
            {"purpose": "certificate", "name": "_a1b2.helm.example.org", "type": "CNAME", "value": "_c3d4.xyz.acm-validations.aws"},
            {"purpose": "address", "name": "helm.example.org", "type": "CNAME", "value": "d111test.cloudfront.net"},
        ])
        self.assertNotIn("111122223333", json.dumps(v))
        self.assertEqual(self.builds, [])
        self.assertEqual(self.h({"action": "domain_status"})["state"], "validating")
        self.acm.status = "ISSUED"
        v = self.h({"action": "domain_status"})
        self.assertEqual(v["state"], "attaching")
        version, actor, mode, extra = self.builds[0]
        self.assertEqual((version, actor, mode), ("v1.2.0", "owner@example.org", "domain"))
        self.assertEqual(extra["DOMAIN_ACTION"], "attach")
        self.assertEqual(extra["DOMAIN_CERT"], ARN)
        self.assertEqual(extra["DOMAIN_ZONE"], "")
        self.h({"action": "domain_status"})
        self.assertEqual(len(self.builds), 1)
        hid = self.store.get("DOMAIN")["history_id"]
        r = self.h({"action": "finished", "status": "succeeded", "history_id": hid})
        self.assertEqual(r["domain"], "live")
        self.assertEqual(self.notices[-1], "CoderHelm now answers on helm.example.org")
        self.assertNotIn("failed_versions", self.store.get("UPDATE_STATE"))

    def test_a_route53_zone_gets_the_validation_record_automatically(self):
        self.r53.zones = [{"Id": "/hostedzone/ZTEST1", "Name": "example.org.", "Config": {"PrivateZone": False}}]
        self.acm.ready_after = 2
        v = u.domain_set(self.store, self.env, SUNDAY_3AM, "helm.example.org", "a", sleep=lambda s: None)
        self.assertEqual((v["mode"], v["zone"], v["records"]), ("route53", "example.org", []))
        self.assertEqual(v["name_servers"], ["ns-1.awsdns-01.org", "ns-2.awsdns-02.com"])
        zone_id, batch = self.r53.changes[0]
        self.assertEqual(zone_id, "ZTEST1")
        rr = batch["Changes"][0]["ResourceRecordSet"]
        self.assertEqual((batch["Changes"][0]["Action"], rr["Name"], rr["ResourceRecords"][0]["Value"]),
                         ("UPSERT", "_a1b2.helm.example.org.", "_c3d4.xyz.acm-validations.aws."))
        self.acm.status = "ISSUED"
        self.h({"action": "domain_status"})
        self.assertEqual(self.builds[0][3]["DOMAIN_ZONE"], "example.org")
        self.assertEqual(self.builds[0][3]["DOMAIN_ZONE_ID"], "ZTEST1")

    def test_a_failed_certificate_stops_with_its_reason_and_try_again_asks_for_a_new_one(self):
        self.h({"action": "domain_set", "hostname": "helm.example.org"})
        self.acm.status, self.acm.reason = "FAILED", "CAA_ERROR"
        v = self.h({"action": "domain_status"})
        self.assertEqual((v["state"], v["reason"], v["certificate"]["failure_reason"]), ("failed", "CAA_ERROR", "CAA_ERROR"))
        self.assertEqual(self.builds, [])
        self.acm.status, self.acm.reason = "PENDING_VALIDATION", None
        self.h({"action": "domain_set", "hostname": "helm.example.org"}, now=SUNDAY_3AM + timedelta(minutes=5))
        first, second = (r["IdempotencyToken"] for r in self.acm.requests)
        self.assertNotEqual(first, second)
        self.assertEqual(self.store.get("DOMAIN")["state"], "validating")

    def test_a_failed_attach_is_kept_with_the_reason_and_can_be_retried(self):
        self.h({"action": "domain_set", "hostname": "helm.example.org"})
        self.acm.status = "ISSUED"
        self.h({"action": "domain_status"})
        hid = self.store.get("DOMAIN")["history_id"]
        self.h({"action": "finished", "status": "failed", "reason": "deploy failed: CNAMEAlreadyExists", "history_id": hid})
        d = self.store.get("DOMAIN")
        self.assertEqual((d["state"], d["reason"]), ("failed", "deploy failed: CNAMEAlreadyExists"))
        self.assertEqual(self.notices[-1], "CoderHelm could not change its address")
        later = SUNDAY_3AM + timedelta(minutes=30)
        self.assertEqual(self.h({"action": "domain_set", "hostname": "helm.example.org"}, now=later)["state"], "attaching")

    def test_busy_and_same_address_are_refused(self):
        self.h({"action": "domain_set", "hostname": "helm.example.org"})
        self.acm.status = "ISSUED"
        self.h({"action": "domain_status"})
        with self.assertRaises(ValueError):
            self.h({"action": "domain_set", "hostname": "other.example.org"})
        with self.assertRaises(ValueError):
            self.h({"action": "domain_remove"})
        with self.assertRaises(ValueError):
            self.h({"action": "domain_set", "hostname": "helm.example.org"}, env=dict(self.env, APP_URL="https://helm.example.org"))

    def test_removing_a_live_domain_redeploys_and_one_never_attached_is_just_forgotten(self):
        with self.assertRaises(ValueError):
            self.h({"action": "domain_remove"})
        self.h({"action": "domain_set", "hostname": "helm.example.org"})
        v = self.h({"action": "domain_remove"})
        self.assertEqual(v["state"], "none")
        self.assertEqual(self.builds, [])
        live = dict(self.env, APP_URL="https://helm.example.org")
        v = self.h({"action": "domain_remove", "actor": "a"}, env=live)
        self.assertEqual(v["state"], "removing")
        self.assertEqual(self.builds[0][3]["DOMAIN_ACTION"], "remove")
        hid = self.store.get("DOMAIN")["history_id"]
        self.h({"action": "finished", "status": "succeeded", "history_id": hid}, env=live)
        self.assertEqual(self.store.get("DOMAIN"), {})
        self.assertEqual(self.notices[-1], "CoderHelm is back on its AWS address")


class EmailRouteTest(unittest.TestCase):
    """Update notices go out the install's chosen way, never both, never to CoderHelm."""

    def setUp(self):
        self.sent = []
        self.posts = []
        test = self

        class Ses:
            def send_email(self, **kw):
                test.sent.append(kw)

        class Dyn:
            def __init__(self, item=None):
                self.item = item

            def get_item(self, **kw):
                return {"Item": self.item} if self.item else {}

        class Secrets:
            def get_secret_value(self, **kw):
                return {"SecretString": json.dumps({"hybrid_install_id": "ins_abcdefghijklmnopqrstuvwx", "hybrid_install_secret": "s3cret"})}

        self.Dyn = Dyn
        u._clients.clear()
        u._clients["sesv2"] = Ses()
        u._clients["secretsmanager"] = Secrets()
        self.env = {"ADMIN_EMAILS": "admin@example.org,ops@coderhelm.com", "SES_FROM_ADDRESS": "ch@example.org",
                    "SETTINGS_TABLE_NAME": "t", "SECRETS_NAME": "x", "APP_URL": "https://d1.cloudfront.net",
                    "HYBRID_URL": "https://hosted.example.test"}

        class Resp:
            def close(self):
                pass

        def opener(req, timeout=None):
            test.posts.append((req.full_url, json.loads(req.data), dict(req.header_items())))
            return Resp()

        self.opener = opener

    def tearDown(self):
        u._clients.clear()

    def test_own_ses_by_default_and_never_a_coderhelm_address(self):
        u._clients["dynamodb"] = self.Dyn()
        u.notify("s", "t", self.env, what="available", version="v1.2.0", opener=self.opener)
        self.assertEqual(len(self.sent), 1)
        self.assertEqual(self.sent[0]["Destination"]["ToAddresses"], ["admin@example.org"])
        self.assertEqual(self.posts, [], "nothing goes to CoderHelm unless an admin chose it")

    def test_through_coderhelm_only_when_chosen_templates_only_and_signed(self):
        u._clients["dynamodb"] = self.Dyn({"data": {"S": json.dumps({"transport": "coderhelm"})}})
        u.notify("s", "t", self.env, what="succeeded", version="v1.2.0", opener=self.opener)
        self.assertEqual(self.sent, [], "never both routes")
        urls = [p[0] for p in self.posts]
        self.assertEqual(urls, ["https://hosted.example.test/hybrid/email/recipients", "https://hosted.example.test/hybrid/email"])
        body = self.posts[1][1]
        self.assertEqual(body["template"], "update_notice")
        self.assertEqual(set(body), {"request_id", "to", "template", "params", "contact"})
        self.assertNotIn("subject", body["params"])
        self.assertEqual(body["params"]["link"], "https://d1.cloudfront.net/settings/system/")
        self.assertEqual(body["to"], "admin@example.org")
        headers = {k.lower(): v for k, v in self.posts[1][2].items()}
        self.assertTrue(headers["x-coderhelm-signature"].startswith("v1="))

    def test_the_signature_matches_the_services(self):
        # services/common/src/hybrid/sign.rs, the_signature_matches_the_installer_and_a_fixed_vector
        self.assertEqual(
            u.sign_request("s3cret", 1_800_000_000, "delete", "/hybrid/installs", b""),
            "v1=ae93880dea04941037494d54f0116ec78c7f934c8a4fceed86d64fa41d24a769",
        )

    def test_hybrid_turned_off_sends_nothing_through_coderhelm(self):
        u._clients["dynamodb"] = self.Dyn({"data": {"S": json.dumps({"transport": "coderhelm"})}})
        u.notify("s", "t", dict(self.env, HYBRID_URL="off"), opener=self.opener)
        self.assertEqual((self.sent, self.posts), ([], []), "never SES instead, never the relay")

    def test_unreadable_settings_send_nothing(self):
        class Broken:
            def get_item(self, **kw):
                raise RuntimeError("down")

        u._clients["dynamodb"] = Broken()
        u.notify("s", "t", self.env, opener=self.opener)
        self.assertEqual((self.sent, self.posts), ([], []))


# ── The deploy window (hardened installs) ─────────────────────────────────

SPEC = {"version": "0.2", "phases": {"build": {"commands": ["echo é", "coderhelm-selfhost update"]}}}
SPEC_TEXT = json.dumps(SPEC, indent=2)
SPEC_SHA = u.buildspec_sha256(SPEC_TEXT)
ROLE_ID = "AROAEXAMPLEROLEID01"


class FakeBuilds:
    def __init__(self, build):
        self.build = build

    def batch_get_builds(self, ids):
        return {"builds": [dict(self.build, id=ids[0])] if self.build else []}


class FakeIam:
    def __init__(self):
        self.policies = {}
        self.calls = []

    def get_role(self, RoleName):
        return {"Role": {"RoleName": RoleName, "RoleId": ROLE_ID}}

    def put_role_policy(self, RoleName, PolicyName, PolicyDocument):
        self.calls.append(("put", RoleName, PolicyName))
        self.policies[(RoleName, PolicyName)] = PolicyDocument

    def delete_role_policy(self, RoleName, PolicyName):
        self.calls.append(("delete", RoleName, PolicyName))
        if (RoleName, PolicyName) not in self.policies:
            raise type("NoSuchEntityException", (Exception,), {})("no such policy")
        del self.policies[(RoleName, PolicyName)]


class FakeParams:
    def __init__(self, parts):
        self.parts = parts

    def get_parameter(self, Name):
        i = int(Name.rsplit("/", 1)[1])
        return {"Parameter": {"Value": json.dumps(self.parts[i - 1])}}


WINDOW_PARTS = [
    [{"Sid": "WindowStacks", "Effect": "Allow", "Action": "cloudformation:UpdateStack", "Resource": "*",
      "Condition": {"StringEquals": {"aws:userid": u.WINDOW_USER_ID}, "DateLessThan": {"aws:CurrentTime": u.WINDOW_EXPIRES}}}],
    [{"Sid": "WindowPass", "Effect": "Allow", "Action": "iam:PassRole", "Resource": "arn:aws:iam::111122223333:role/coderhelm-prod-deploy",
      "Condition": {"StringEquals": {"aws:userid": u.WINDOW_USER_ID}, "DateLessThan": {"aws:CurrentTime": u.WINDOW_EXPIRES}}}],
]


def good_build(**over):
    b = {
        "projectName": "coderhelm-prod-update",
        "buildStatus": "IN_PROGRESS",
        "source": {"type": "NO_SOURCE", "buildspec": SPEC_TEXT},
        "environment": {"type": "LINUX_CONTAINER", "image": "aws/codebuild/standard:7.0", "privilegedMode": False,
                        "environmentVariables": [{"name": "VERSION", "value": "v0.8.0", "type": "PLAINTEXT"},
                                                 {"name": "HISTORY_ID", "value": "h", "type": "PLAINTEXT"}]},
        "serviceRole": "arn:aws:iam::111122223333:role/coderhelm-prod-update-build",
    }
    b.update(over)
    return b


class DeployWindow(unittest.TestCase):
    def setUp(self):
        self.db = FakeDynamo()
        self.store = u.Store(self.db, "t")
        self.env = {
            "UPDATE_PROJECT_NAME": "coderhelm-prod-update",
            "DEPLOY_WINDOW": json.dumps({"role": "coderhelm-prod-update-build", "buildspec": SPEC_SHA, "image": "aws/codebuild/standard:7.0",
                                         "variables": ["VERSION", "ACTOR", "MODE", "HISTORY_ID"], "param": "/coderhelm-prod/access/window-policy",
                                         "parts": 2, "minutes": 100}),
        }
        self.hid = "UPDATE#2026-10-25T03:00:00Z#abc"
        self.store.put(self.hid, {"version": "v0.8.0", "status": "running", "build_id": "coderhelm-prod-update:1111"})
        self.iam = FakeIam()
        self.ssm = FakeParams(WINDOW_PARTS)

    def ask(self, build, **event):
        e = {"build_id": "coderhelm-prod-update:1111", "user_id": f"{ROLE_ID}:AWSCodeBuild-1111", "history_id": self.hid, **event}
        return u.open_window(self.store, self.env, SUNDAY_3AM, e, cb=FakeBuilds(build), iam=self.iam, ssm=self.ssm)

    def test_canonical_json_matches_the_infra_side(self):
        # infra/test/access.test.ts hashes the same object with canonicalJson.
        self.assertEqual(u.canonical_json({"b": [1, {"d": "é", "c": None}], "a": "x\n"}), '{"a":"x\\n","b":[1,{"c":null,"d":"é"}]}')

    def test_opens_for_the_build_it_started_bound_to_that_session_and_an_end_time(self):
        r = self.ask(good_build())
        self.assertEqual(r["until"], "2026-10-25T04:55:00Z")
        doc = json.loads(self.iam.policies[("coderhelm-prod-update-build", "deploy-window")])
        self.assertEqual([s["Sid"] for s in doc["Statement"]], ["WindowStacks", "WindowPass"])
        for st in doc["Statement"]:
            self.assertEqual(st["Condition"]["StringEquals"]["aws:userid"], f"{ROLE_ID}:AWSCodeBuild-1111")
            self.assertEqual(st["Condition"]["DateLessThan"]["aws:CurrentTime"], "2026-10-25T04:55:00Z")
        self.assertNotIn("__CODERHELM", json.dumps(doc))
        self.assertEqual(self.store.get(self.hid)["window_until"], "2026-10-25T04:55:00Z")

    def test_refuses_a_build_it_did_not_start(self):
        with self.assertRaisesRegex(ValueError, "not an update this updater started"):
            self.ask(good_build(), build_id="coderhelm-prod-update:2222")
        with self.assertRaisesRegex(ValueError, "not an update this updater started"):
            self.ask(good_build(), history_id="")
        self.assertEqual(self.iam.policies, {})

    def test_refuses_an_edited_or_overridden_build(self):
        cases = {
            "buildspec": good_build(source={"type": "NO_SOURCE", "buildspec": SPEC_TEXT.replace("update", "update; curl evil")}),
            "build image": good_build(environment=dict(good_build()["environment"], image="evil/image:1")),
            "sets NODE_OPTIONS": good_build(environment=dict(good_build()["environment"], environmentVariables=[{"name": "NODE_OPTIONS", "value": "--require /x"}])),
            "source was changed": good_build(source={"type": "S3", "buildspec": SPEC_TEXT}),
            "another role": good_build(serviceRole="arn:aws:iam::111122223333:role/admin"),
            "not running": good_build(buildStatus="SUCCEEDED"),
        }
        for why, build in cases.items():
            with self.subTest(why), self.assertRaisesRegex(ValueError, why.split()[-1] if why != "build image" else "build image"):
                self.ask(build)
        self.assertEqual(self.iam.policies, {}, "nothing was opened")

    def test_refuses_a_caller_that_is_not_the_build_role(self):
        for uid in ["AROAOTHERROLE00001:AWSCodeBuild-1111", "AIDAUSER000000001", f"{ROLE_ID}"]:
            with self.subTest(uid), self.assertRaisesRegex(ValueError, "not the update build's role"):
                self.ask(good_build(), user_id=uid)
        self.assertEqual(self.iam.policies, {})

    def test_an_install_that_is_not_hardened_has_no_window(self):
        env = {"UPDATE_PROJECT_NAME": "coderhelm-prod-update"}
        with self.assertRaisesRegex(ValueError, "isn't hardened"):
            u.open_window(self.store, env, SUNDAY_3AM, {"build_id": "x"}, cb=FakeBuilds(good_build()), iam=self.iam, ssm=self.ssm)
        self.assertFalse(u.close_window(env, iam=self.iam))
        self.assertEqual(self.iam.calls, [], "no IAM call at all without a window")

    def test_finished_and_an_idle_tick_close_it(self):
        self.ask(good_build())
        u._clients["iam"] = self.iam
        try:
            self.assertTrue(u.close_window(self.env))
            self.assertEqual(self.iam.policies, {})
            self.assertFalse(u.close_window(self.env), "closing twice is fine")
        finally:
            u._clients.pop("iam", None)
