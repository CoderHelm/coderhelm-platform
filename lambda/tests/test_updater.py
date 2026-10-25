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
        u.start_build = lambda env, version, actor, mode, hid: self.builds.append((version, actor, mode, hid)) or f"b-{len(self.builds)}"
        u.notify = lambda subject, text, env=None: self.notices.append(subject) or 1
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
