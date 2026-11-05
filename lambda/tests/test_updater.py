"""The self-host updater: when it updates, when it only notifies, and what it records.

Run: python3 -m unittest discover -s lambda/tests
"""

import hashlib
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
        self.assertEqual(u.pick_latest(rs, "beta", "v1.0.0")["version"], "v1.2.0-rc.1")
        self.assertEqual(u.pick_latest(rs, "preview", "v1.0.0")["version"], "v1.2.0-rc.1", "preview is the old name of beta")
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
        self.assertEqual(u.validate_settings({"channel": "beta"})["channel"], "beta")
        self.assertEqual(u.validate_settings({"channel": "preview"})["channel"], "beta", "stored settings from before keep working")
        self.assertEqual(u.validate_settings({}, {**u.DEFAULT_SETTINGS, "channel": "preview"})["channel"], "preview", "a base is kept as given")
        self.assertEqual(u.defaults_from_env({"UPDATE_DEFAULTS": '{"channel": "preview"}'})["channel"], "beta")
        self.assertEqual(u.defaults_from_env({"UPDATE_DEFAULTS": '{"channel": "nightly"}'})["channel"], "stable")
        for bad in ({"hour": 24}, {"hour": True}, {"channel": "nightly"}, {"channel": ["beta"]}, {"majors": "never"}, {"timezone": "Mars/Base"}, {"auto": "yes"}, {"colour": 1}):
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

    def test_beta_channel(self):
        nightly = [rel("v1.1.0-beta.20261105", pre=True)]
        self.assertEqual(u.decide(SUNDAY_3AM, ON, "v1.0.0", nightly, {})["action"], "none")
        self.assertEqual(u.decide(SUNDAY_3AM, dict(ON, channel="beta"), "v1.0.0", nightly, {})["action"], "update")
        self.assertEqual(u.decide(SUNDAY_3AM, dict(ON, channel="preview"), "v1.0.0", nightly, {})["action"], "update")

    def test_beta_to_stable_never_goes_back(self):
        # On a nightly, then switched to stable: the older stable release is never offered...
        rs = [rel("v1.0.0"), rel("v1.1.0-beta.20261105", pre=True), rel("v1.1.0-beta.20261106", pre=True)]
        self.assertIsNone(u.pick_latest(rs, "stable", "v1.1.0-beta.20261106"))
        self.assertEqual(u.decide(SUNDAY_3AM, ON, "v1.1.0-beta.20261106", rs, {})["action"], "none")
        # ...and the install moves on at the next stable release: the same version, promoted, sorts above its nightly.
        got = u.pick_latest(rs + [rel("v1.1.0")], "stable", "v1.1.0-beta.20261106")
        self.assertEqual((got["version"], got["prerelease"]), ("v1.1.0", False))
        self.assertEqual(u.compare("v1.1.0-beta.20261106", "v1.1.0-beta.20261105"), 1)
        self.assertEqual(u.compare("v1.1.1-beta.20261101", "v1.1.0"), 1)

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

    def test_the_hourly_check_keeps_its_copy_for_an_hour(self):
        calls = []
        fetch = lambda r: calls.append(r) or [rel("v1.1.0")]
        u.handle({"action": "tick"}, self.env, self.store, SUNDAY_3AM, fetch)
        u.handle({"action": "tick"}, self.env, self.store, SUNDAY_3AM + timedelta(minutes=30), fetch)
        self.assertEqual(len(calls), 1)
        u.handle({"action": "tick"}, self.env, self.store, SUNDAY_3AM + timedelta(minutes=61), fetch)
        self.assertEqual(len(calls), 2)

    def test_opening_settings_reads_a_copy_older_than_two_minutes_again(self):
        # 16:51 the hourly check read v1.1.0; 17:22 v1.2.0 is out.
        out = [rel("v1.1.0")]
        calls = []
        fetch = lambda r: calls.append(r) or list(out)
        u.handle({"action": "tick"}, self.env, self.store, SUNDAY_3AM, fetch)
        out.append(rel("v1.2.0"))
        later = SUNDAY_3AM + timedelta(minutes=31)
        s = u.handle({"action": "status"}, self.env, self.store, later, fetch)
        self.assertEqual(len(calls), 2)
        self.assertEqual(s["latest"]["version"], "v1.2.0")
        self.assertEqual(s["releases"]["fetched_at"], later.timestamp())
        # Reloads within two minutes use the copy just read.
        u.handle({"action": "status"}, self.env, self.store, later + timedelta(seconds=90), fetch)
        self.assertEqual(len(calls), 2)
        u.handle({"action": "status"}, self.env, self.store, later + timedelta(minutes=3), fetch)
        self.assertEqual(len(calls), 3)

    def test_check_for_updates_reads_now_but_at_most_once_per_gap(self):
        calls = []
        fetch = lambda r: calls.append(r) or [rel("v1.1.0")]
        u.handle({"action": "status"}, self.env, self.store, SUNDAY_3AM, fetch)
        for sec in (1, 2, 4):
            s = u.handle({"action": "status", "refresh": True}, self.env, self.store, SUNDAY_3AM + timedelta(seconds=sec), fetch)
        self.assertEqual(len(calls), 1, "presses within a few seconds share one read")
        self.assertEqual(s["releases"]["fetched_at"], SUNDAY_3AM.timestamp())
        # Check for updates right after opening the page reads again (a page reload would not).
        u.handle({"action": "status"}, self.env, self.store, SUNDAY_3AM + timedelta(seconds=10), fetch)
        self.assertEqual(len(calls), 1)
        s = u.handle({"action": "status", "refresh": True}, self.env, self.store, SUNDAY_3AM + timedelta(seconds=10), fetch)
        self.assertEqual(len(calls), 2)
        self.assertEqual(s["releases"]["fetched_at"], (SUNDAY_3AM + timedelta(seconds=10)).timestamp())

    def test_a_failed_read_counts_toward_the_gap_and_is_reported(self):
        calls = []
        def down(r):
            calls.append(r)
            raise OSError("GitHub is down")
        u.handle({"action": "status"}, self.env, self.store, SUNDAY_3AM, lambda r: [rel("v1.1.0")])
        later = SUNDAY_3AM + timedelta(minutes=5)
        s = u.handle({"action": "status"}, self.env, self.store, later, down)
        u.handle({"action": "status", "refresh": True}, self.env, self.store, later + timedelta(seconds=3), down)
        u.handle({"action": "status"}, self.env, self.store, later + timedelta(seconds=20), down)
        self.assertEqual(len(calls), 1, "a failed try is not retried on every reload")
        self.assertEqual(s["latest"]["version"], "v1.1.0", "the old list is kept")
        self.assertEqual(s["releases"]["error"], "GitHub is down")
        self.assertEqual(s["releases"]["fetched_at"], SUNDAY_3AM.timestamp())

    def test_the_etag_is_sent_and_a_304_keeps_the_list(self):
        seen = []
        def fetch(r, etag=None):
            seen.append(etag)
            if etag == '"abc"':
                raise u.NotModified()
            got = u.Fetched([rel("v1.1.0")])
            got.etag = '"abc"'
            return got
        u.handle({"action": "status"}, self.env, self.store, SUNDAY_3AM, fetch)
        later = SUNDAY_3AM + timedelta(minutes=5)
        s = u.handle({"action": "status"}, self.env, self.store, later, fetch)
        self.assertEqual(seen, [None, '"abc"'])
        self.assertEqual(s["latest"]["version"], "v1.1.0")
        self.assertEqual(s["releases"]["fetched_at"], later.timestamp(), "a 304 is a check")

    def test_fetch_releases_sends_if_none_match_and_reads_a_304(self):
        import urllib.error
        sent = []
        class Resp:
            headers = {"etag": '"new"'}
            def __enter__(self): return self
            def __exit__(self, *a): return False
            def read(self): return b'[{"tag_name": "v1.1.0"}]'
            def geturl(self): return u.RELEASES_URL
        def opener(req, timeout=None):
            sent.append(req.get_header("If-none-match"))
            if req.get_header("If-none-match") == '"new"':
                raise urllib.error.HTTPError(req.full_url, 304, "Not Modified", {}, None)
            return Resp()
        got = u.fetch_releases(u.RELEASE_REPO, opener=opener)
        self.assertEqual(got.etag, '"new"')
        self.assertEqual(got[0]["tag_name"], "v1.1.0")
        with self.assertRaises(u.NotModified):
            u.fetch_releases(u.RELEASE_REPO, etag='"new"', opener=opener)
        self.assertEqual(sent, [None, '"new"'])

    def test_latest_json_naming_a_newer_release_reads_the_list_again(self):
        out = [rel("v1.1.0")]
        calls = []
        fetch = lambda r: calls.append(r) or list(out)
        hint = {"version": "v1.1.0"}
        read = lambda channel, etag=None: dict(hint)
        u.handle({"action": "status"}, self.env, self.store, SUNDAY_3AM, fetch, hint=read)
        hint["version"] = "v1.2.0"
        out.append(rel("v1.2.0"))
        # Within the two minutes, but latest.json names a release the copy lacks.
        s = u.handle({"action": "status", "refresh": True}, self.env, self.store, SUNDAY_3AM + timedelta(seconds=40), fetch, hint=read)
        self.assertEqual(len(calls), 2)
        self.assertEqual(s["latest"]["version"], "v1.2.0")

    def test_never_offers_an_older_release_while_a_newer_one_is_out(self):
        # latest.json names v1.2.0; GitHub's list (a lagging copy) still ends at v1.1.0.
        read = lambda channel, etag=None: {"version": "v1.2.0"}
        fetch = lambda r: [rel("v1.1.0")]
        s = u.handle({"action": "status"}, self.env, self.store, SUNDAY_3AM, fetch, hint=read)
        self.assertIsNone(s["latest"], "v1.1.0 is never offered while v1.2.0 is out")
        self.assertFalse(s["update_available"])
        self.assertEqual(s["releases"]["behind"]["version"], "v1.2.0")
        with self.assertRaisesRegex(ValueError, "v1.2.0 is out"):
            u.handle({"action": "update_now", "actor": "a"}, self.env, self.store, SUNDAY_3AM + timedelta(minutes=1), fetch, hint=read)
        self.assertEqual(self.builds, [])
        u.handle({"action": "settings", "settings": {"auto": True, "majors": "auto", "day": "daily"}}, self.env, self.store, SUNDAY_3AM, fetch)
        d = u.handle({"action": "tick"}, self.env, self.store, SUNDAY_3AM + timedelta(minutes=2), fetch, hint=read)
        self.assertEqual(d["action"], "none")
        self.assertEqual(self.builds, [], "nor applied by the hourly check")
        # Once listed, it is the one offered.
        listed = lambda r: [rel("v1.1.0"), rel("v1.2.0")]
        s = u.handle({"action": "status", "refresh": True}, self.env, self.store, SUNDAY_3AM + timedelta(minutes=3), listed, hint=read)
        self.assertEqual(s["latest"]["version"], "v1.2.0")
        self.assertIsNone(s["releases"]["behind"])

    def test_a_release_missing_for_a_day_is_taken_as_withdrawn(self):
        read = lambda channel, etag=None: {"version": "v1.2.0"}
        fetch = lambda r: [rel("v1.1.0")]
        s = u.handle({"action": "status"}, self.env, self.store, SUNDAY_3AM, fetch, hint=read)
        self.assertIsNone(s["latest"])
        s = u.handle({"action": "status"}, self.env, self.store, SUNDAY_3AM + timedelta(hours=25), fetch, hint=read)
        self.assertEqual(s["latest"]["version"], "v1.1.0")

    def test_each_channel_reads_its_own_hint(self):
        asked = []
        hints = {"stable": "v1.1.0", "beta": "v1.2.0-beta.20261105"}
        read = lambda channel, etag=None: asked.append(channel) or {"version": hints[channel]}
        fetch = lambda r: [rel("v1.1.0"), rel("v1.2.0-beta.20261105", pre=True)]
        s = u.handle({"action": "status"}, self.env, self.store, SUNDAY_3AM, fetch, hint=read)
        self.assertEqual((asked, s["latest"]["version"]), (["stable"], "v1.1.0"))
        u.handle({"action": "settings", "settings": {"channel": "beta"}}, self.env, self.store, SUNDAY_3AM, fetch)
        s = u.handle({"action": "status", "refresh": True}, self.env, self.store, SUNDAY_3AM + timedelta(minutes=1), fetch, hint=read)
        self.assertEqual((asked[-1], s["latest"]["version"], s["settings"]["channel"]), ("beta", "v1.2.0-beta.20261105", "beta"))
        # Cached apart: the stable hint is never moved by the beta one.
        self.assertEqual(self.store.get("RELEASE_HINT")["version"], "v1.1.0")
        self.assertEqual(self.store.get("RELEASE_HINT#beta")["version"], "v1.2.0-beta.20261105")

    def test_a_stored_preview_setting_reads_as_beta(self):
        self.store.put("AUTO_UPDATE", dict(u.DEFAULT_SETTINGS, channel="preview"))
        fetch = lambda r: [rel("v1.1.0"), rel("v1.2.0-beta.20261105", pre=True)]
        s = u.handle({"action": "status"}, self.env, self.store, SUNDAY_3AM, fetch)
        self.assertEqual((s["settings"]["channel"], s["latest"]["version"]), ("beta", "v1.2.0-beta.20261105"))

    def test_a_nightly_published_since_the_hourly_read_shows_within_minutes(self):
        # The hourly check read the list at 03:15; a nightly is published at 03:20.
        out = [rel("v1.1.0")]
        calls = []
        fetch = lambda r: calls.append(r) or list(out)
        hint = {"version": "v1.1.0"}
        read = lambda channel, etag=None: dict(hint)
        u.handle({"action": "settings", "settings": {"channel": "beta"}}, self.env, self.store, SUNDAY_3AM, fetch)
        u.handle({"action": "tick"}, self.env, self.store, SUNDAY_3AM, fetch, hint=read)
        out.append(rel("v1.2.0-beta.20261025", pre=True))
        hint["version"] = "v1.2.0-beta.20261025"
        # The next hourly check would be at 04:15; the hint (read every few minutes) brings it at 03:25.
        d = u.handle({"action": "tick"}, self.env, self.store, SUNDAY_3AM + timedelta(minutes=10), fetch, hint=read)
        self.assertEqual(len(calls), 2)
        self.assertEqual(d["version"], "v1.2.0-beta.20261025")

    def test_a_beta_hint_never_holds_back_a_stable_install(self):
        # A stable install somehow holding a prerelease hint: it is not a stable release, so nothing is behind.
        self.store.put("RELEASE_HINT", {"version": "v1.2.0-beta.20261105", "fetched_at": SUNDAY_3AM.timestamp()})
        s = u.handle({"action": "status"}, self.env, self.store, SUNDAY_3AM, lambda r: [rel("v1.1.0")])
        self.assertEqual(s["latest"]["version"], "v1.1.0")
        self.assertIsNone(s["releases"]["behind"])

    def test_the_hint_urls(self):
        self.assertEqual(u.HINT_URLS, {"stable": "https://coderhelm-public.s3.amazonaws.com/launch/latest.json",
                                       "beta": "https://coderhelm-public.s3.amazonaws.com/launch/beta.json"})
        asked = []

        class Resp:
            headers = {"etag": '"e"'}
            def __init__(self, url): self.url = url
            def read(self): return b'{"version": "v1.2.0-beta.20261105"}'
            def geturl(self): return self.url
            def __enter__(self): return self
            def __exit__(self, *a): return False

        def opener(req, timeout=None):
            asked.append(req.full_url)
            return Resp(req.full_url)
        self.assertEqual(u.fetch_latest("beta", opener=opener)["version"], "v1.2.0-beta.20261105")
        u.fetch_latest("preview", opener=opener)
        u.fetch_latest(opener=opener)
        self.assertEqual(asked, [u.BETA_URL, u.BETA_URL, u.LATEST_URL])

    def test_a_newer_preview_seen_before_never_holds_back_stable(self):
        fetch = lambda r: [rel("v1.1.0"), rel("v1.2.0-rc.1", pre=True)]
        s = u.handle({"action": "status"}, self.env, self.store, SUNDAY_3AM, fetch)
        self.assertEqual(s["latest"]["version"], "v1.1.0")

    def test_update_now_reads_the_newest_release_first(self):
        out = [rel("v1.1.0")]
        calls = []
        fetch = lambda r: calls.append(r) or list(out)
        u.handle({"action": "status"}, self.env, self.store, SUNDAY_3AM, fetch)
        out.append(rel("v1.2.0"))
        # The page showed v1.1.0 a minute ago; pressing Update installs v1.2.0.
        e = u.handle({"action": "update_now", "actor": "a"}, self.env, self.store, SUNDAY_3AM + timedelta(minutes=1), fetch)
        self.assertEqual(len(calls), 2)
        self.assertEqual(e["version"], "v1.2.0")
        self.assertEqual(self.builds[0][0], "v1.2.0")

    def test_the_lambda_reads_latest_json(self):
        real = u.handle
        got = {}
        u.handle = lambda event, env, store, now, **k: got.update(k) or {}
        self.addCleanup(setattr, u, "handle", real)
        real_client = u.client
        u.client = lambda name, region=None: self.db
        self.addCleanup(setattr, u, "client", real_client)
        import os
        old = dict(os.environ)
        os.environ.update(SETTINGS_TABLE_NAME="t")
        self.addCleanup(lambda: (os.environ.clear(), os.environ.update(old)))
        u.handler({"action": "status"}, None)
        self.assertIs(got["hint"], u.fetch_latest)

    def test_tick_with_auto_update_on_starts_the_build_without_an_email(self):
        u.handle({"action": "settings", "settings": {"auto": True, "majors": "ask"}}, self.env, self.store, SUNDAY_3AM, self.fetch)
        d = u.handle({"action": "tick"}, self.env, self.store, SUNDAY_3AM, lambda r: [rel("v1.1.0")])
        self.assertEqual(d["action"], "update")
        self.assertEqual(self.builds[0][:3], ("v1.1.0", "auto", "update"))
        self.assertEqual(self.notices, [], "a new release never emails: the page shows it")
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
        self.assertEqual(self.notices[-1], "update to v2.0.0 rolled back")
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


def released(tag, at, pre=False, body="## What's changed\n- Faster reviews on large pull requests.\n- A guided uninstall."):
    return {"tag_name": tag, "prerelease": pre, "draft": False, "body": body,
            "published_at": at.strftime("%Y-%m-%dT%H:%M:%SZ"), "html_url": f"https://x/{tag}"}


class NoticePolicy(unittest.TestCase):
    """Admins are emailed only when a person must act or something went
    wrong, each at most once per version and reason; plus the digest."""

    def setUp(self):
        self.store = u.Store(FakeDynamo(), "t")
        self.builds, self.sent = [], []
        real_build, real_notify, real_manifest = u.start_build, u.notify, u.verified_manifest
        u.start_build = lambda env, version, actor, mode, hid, extra=None: self.builds.append((version, actor, mode, hid)) or "b-1"
        u.notify = lambda subject, text, env=None, **kw: self.sent.append(dict(kw, subject=subject, text=text)) or 1
        u.verified_manifest = lambda store, version: (True, None)
        self.addCleanup(setattr, u, "start_build", real_build)
        self.addCleanup(setattr, u, "notify", real_notify)
        self.addCleanup(setattr, u, "verified_manifest", real_manifest)
        self.env = {"CODERHELM_VERSION": "v1.0.0", "APP_URL": "https://helm.example.org", "UPDATE_PROJECT_NAME": "p"}
        self.out = []

    def fetch(self, _repo):
        return list(self.out)

    def settings(self, **kw):
        u.handle({"action": "settings", "settings": kw}, self.env, self.store, SUNDAY_3AM, self.fetch)

    def tick(self, at):
        return u.handle({"action": "tick"}, self.env, self.store, at, self.fetch)

    def ticks(self, start, days, every=timedelta(hours=6)):
        at = start
        while at <= start + timedelta(days=days):
            self.tick(at)
            at += every

    def whats(self):
        return [n.get("what") for n in self.sent]

    def finish(self, status, version="v1.1.0"):
        e = u.handle({"action": "update_now", "actor": "a@example.org", "version": version}, self.env, self.store, SUNDAY_3AM, self.fetch)
        u.handle({"action": "finished", "status": status, "reason": "the api stack failed", "history_id": e["id"]},
                 self.env, self.store, SUNDAY_3AM, self.fetch)

    def test_new_releases_and_majors_never_email(self):
        self.settings(digest="off")
        self.out = [released("v1.1.0", SUNDAY_3AM - timedelta(days=1))]
        self.ticks(SUNDAY_3AM, 2)
        self.out.append(released("v2.0.0", SUNDAY_3AM + timedelta(days=2)))
        self.ticks(SUNDAY_3AM + timedelta(days=2), 2)
        self.assertEqual(self.sent, [])

    def test_success_never_emails_failure_and_rollback_once_each(self):
        self.out = [released("v1.1.0", SUNDAY_3AM - timedelta(days=1))]
        self.finish("succeeded")
        self.assertEqual(self.sent, [], "a finished update is in the history")
        self.finish("failed")
        self.finish("failed")
        self.assertEqual(self.whats(), ["failed"])
        self.assertEqual(self.sent[0]["subject"], "update to v1.1.0 failed")
        self.finish("rolled_back")
        self.finish("rolled_back")
        self.assertEqual(self.whats(), ["failed", "rolled_back"])

    def test_beta_prereleases_never_email_unless_they_fail(self):
        self.settings(channel="preview", digest="off")
        self.out = [released("v1.1.0-rc.1", SUNDAY_3AM - timedelta(hours=1), pre=True)]
        self.ticks(SUNDAY_3AM, 3)
        self.assertEqual(self.sent, [])
        self.finish("failed", "v1.1.0-rc.1")
        self.assertEqual(self.whats(), ["failed"])

    def test_digest_off_sends_nothing(self):
        self.settings(digest="off")
        self.ticks(SUNDAY_3AM, 1)
        self.out = [released("v1.1.0", SUNDAY_3AM + timedelta(days=2))]
        self.ticks(SUNDAY_3AM + timedelta(days=1), 30)
        self.assertEqual(self.sent, [])

    def test_no_digest_when_nothing_is_new(self):
        self.out = [released("v1.1.0", SUNDAY_3AM - timedelta(days=2))]
        self.ticks(SUNDAY_3AM, 20)
        self.assertEqual(self.sent, [], "the first tick starts the clock; nothing came out since")

    def test_one_digest_per_week_across_many_ticks(self):
        self.assertEqual(u.handle({"action": "status"}, self.env, self.store, SUNDAY_3AM, self.fetch)["settings"]["digest"], "weekly")
        self.tick(SUNDAY_3AM)
        self.out = [released("v1.1.0", SUNDAY_3AM + timedelta(days=1), body="## Changes\n- **Faster** reviews, see [notes](https://x).")]
        self.ticks(SUNDAY_3AM + timedelta(hours=1), 6.9, timedelta(hours=1))
        self.assertEqual(self.sent, [], "never before a week has passed")
        self.ticks(SUNDAY_3AM + timedelta(days=7), 2, timedelta(hours=1))
        self.assertEqual(self.whats(), ["digest"])
        d = self.sent[0]
        self.assertEqual((d["subject"], d["version"]), ("what's new, up to v1.1.0", "v1.1.0"))
        self.assertIn("v1.1.0 (2026-10-26): Faster reviews, see notes.", d["facts"]["notes"])
        self.assertEqual(d["facts"]["on_latest"], "no")
        # A release three days later waits for the next week.
        self.out.append(released("v1.2.0", SUNDAY_3AM + timedelta(days=10)))
        self.ticks(SUNDAY_3AM + timedelta(days=9), 4.9, timedelta(hours=1))
        self.assertEqual(self.whats(), ["digest"])
        self.ticks(SUNDAY_3AM + timedelta(days=14), 6)
        self.assertEqual(self.whats(), ["digest", "digest"])
        self.assertIn("v1.2.0", self.sent[1]["facts"]["notes"])
        self.assertNotIn("v1.1.0", self.sent[1]["facts"]["notes"], "only what came out since the last one")

    def test_biweekly_waits_two_weeks(self):
        self.settings(digest="biweekly")
        self.tick(SUNDAY_3AM)
        self.out = [released("v1.1.0", SUNDAY_3AM + timedelta(days=1))]
        self.ticks(SUNDAY_3AM + timedelta(hours=6), 13.5)
        self.assertEqual(self.sent, [])
        self.ticks(SUNDAY_3AM + timedelta(days=14), 5)
        self.assertEqual(self.whats(), ["digest"])

    def test_digest_folds_in_prereleases_only_for_preview_installs(self):
        self.env["CODERHELM_VERSION"] = "v1.1.0"
        self.tick(SUNDAY_3AM)
        self.out = [released("v1.1.0", SUNDAY_3AM - timedelta(days=3)),
                    released("v1.2.0-rc.1", SUNDAY_3AM + timedelta(days=2), pre=True)]
        self.ticks(SUNDAY_3AM + timedelta(days=7), 1)
        self.assertEqual(self.sent, [], "a stable install never hears of a prerelease")
        self.settings(channel="preview")
        self.ticks(SUNDAY_3AM + timedelta(days=8), 1)
        self.assertEqual(self.whats(), ["digest"])
        self.assertIn("v1.2.0-rc.1", self.sent[0]["facts"]["notes"])

    def test_digest_says_when_the_install_is_on_the_newest_release(self):
        self.env["CODERHELM_VERSION"] = "v1.1.0"
        self.tick(SUNDAY_3AM)
        self.out = [released("v1.1.0", SUNDAY_3AM + timedelta(days=1))]
        self.ticks(SUNDAY_3AM + timedelta(days=7), 1)
        self.assertEqual(self.sent[0]["facts"]["on_latest"], "yes")

    def test_digest_setting_is_checked(self):
        with self.assertRaisesRegex(ValueError, "digest: weekly, biweekly or off"):
            self.settings(digest="daily")

    def test_digest_renders_for_the_install_ses(self):
        n = u.notice("digest", "v1.2.0", self.env, {"current": "v1.1.0", "notes": "v1.2.0 (2026-11-01): Faster.", "on_latest": "no"})
        self.assertEqual(n["base_subject"], "what's new, up to v1.2.0")
        self.assertIn("isn't on the newest release yet", n["text"])
        self.assertNotIn("notes_link", n["params"], "one link: Settings, System")
        self.assertEqual(n["params"]["on_latest"], "no")
        r = u.notice("review", "v1.2.0", self.env, {"reason": "it changes permissions", "waiting": "2026-10-24 03:10 UTC"})
        self.assertEqual(r["base_subject"], "update to v1.2.0 needs your OK")
        self.assertIn("Waiting since: 2026-10-24 03:10 UTC", r["text"])


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


class FakeCodeBuild:
    """CodeBuild's own record of each release check build."""

    def __init__(self):
        self.builds = {}

    def finish(self, build_id, status="SUCCEEDED", version="v1.1.0", template=None, models="none",
               project="coderhelm-prod-release-check"):
        self.builds[build_id] = {
            "id": build_id, "buildStatus": status, "projectName": project,
            "environment": {"environmentVariables": [{"name": "VERSION", "value": version}]},
            "exportedEnvironmentVariables": [{"name": "TEMPLATE_SHA256", "value": template or ""}, {"name": "MODELS_SHA256", "value": models}],
        }

    def batch_get_builds(self, ids):
        return {"builds": [self.builds[i] for i in ids if i in self.builds]}


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
        self.env = {"CODERHELM_VERSION": "v1.0.0", "APP_URL": "https://helm.example.org", "UPDATE_PROJECT_NAME": "p", "RELEASE_CHECK_PROJECT_NAME": "coderhelm-prod-release-check",
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
        self.assertIn("update to v1.1.0 needs your OK", self.notices)
        text = self.texts[self.notices.index("update to v1.1.0 needs your OK")]
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
        self.assertEqual(self.notices.count("update to v1.1.0 needs your OK"), 1)

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
        self.assertEqual(self.notices.count("update to v1.1.0 needs your OK"), 1)

    def test_a_pass_applies_in_the_window(self):
        self.worker.refuse = set()
        d = self.tick(SUNDAY_3AM)
        self.assertEqual(d["action"], "update")
        self.assertEqual(self.builds[0][0], "v1.1.0")
        self.assertNotIn("update to v1.1.0 needs your OK", self.notices)

    def test_a_refusal_found_right_before_applying_stops_it(self):
        # Checked an hour before the window: passes, nothing applied yet.
        self.worker.refuse = set()
        self.assertEqual(self.tick(SUNDAY_3AM - timedelta(hours=1))["action"], "notify")
        # Model access was turned off since: the check right before applying stops it.
        self.worker.refuse = {"claude-opus-5-5"}
        d = self.tick(SUNDAY_3AM)
        self.assertEqual(d["action"], "none")
        self.assertEqual(self.builds, [])
        self.assertIn("update to v1.1.0 needs your OK", self.notices)

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
        cb = FakeCodeBuild()
        u._clients["codebuild"] = cb
        self.addCleanup(u._clients.pop, "codebuild", None)
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
        # A report that isn't from the build the updater started is refused.
        with self.assertRaisesRegex(ValueError, "doesn't come from the release check build"):
            u.handle({"action": "models_verified", "version": "v1.1.0", "build": "other", "status": "failed", "reason": "x"},
                     self.env, self.store, SUNDAY_3AM, self.releases, self.worker)
        # A failed signature check trusts nothing.
        u.handle({"action": "models_verified", "version": "v1.1.0", "build": "c-1", "status": "failed", "reason": "bad signature"},
                 self.env, self.store, SUNDAY_3AM, self.releases, self.worker)
        self.assertIsNone(self.store.get("MANIFEST#v1.1.0"))
        # The next check starts at once; the build reports the verified file.
        self.tick(SUNDAY_3AM)
        text = json.dumps(MANIFEST)
        r = u.handle({"action": "models_verified", "version": "v1.1.0", "build": "c-1", "status": "verified", "manifest_text": text},
                     self.env, self.store, SUNDAY_3AM, self.releases, self.worker)
        # Only a claim while CodeBuild hasn't finished that build.
        self.assertEqual(r["verified"], "pending")
        self.assertIsNone(self.store.get("MANIFEST#v1.1.0"))
        self.assertEqual(self.worker.payloads, [])
        # CodeBuild says it succeeded with the same models.json: the check runs.
        cb.finish("c-1", models=hashlib.sha256(text.encode()).hexdigest())
        u.handle({"action": "status"}, self.env, self.store, SUNDAY_3AM, self.releases, self.worker)
        self.assertEqual(self.store.get("MANIFEST#v1.1.0")["manifest"], MANIFEST)
        self.assertEqual(len(self.worker.payloads), 1)
        # A manifest for another release is refused.
        with self.assertRaises(ValueError):
            u.handle({"action": "models_verified", "version": "v1.2.0", "build": "c-1", "status": "verified", "manifest_text": text},
                     self.env, self.store, SUNDAY_3AM, self.releases, self.worker)

    def test_a_claim_codebuild_doesnt_back_is_never_believed(self):
        self.db.items.pop(("SYSTEM", "MANIFEST#v1.1.0"))
        self.worker.refuse = set()
        cb = FakeCodeBuild()
        u._clients["codebuild"] = cb
        self.addCleanup(u._clients.pop, "codebuild", None)
        text = json.dumps(MANIFEST)
        good = hashlib.sha256(text.encode()).hexdigest()
        cases = [
            ("build ended FAILED", dict(status="FAILED", models=good)),
            ("isn't this install's release check", dict(project="coderhelm-prod-update", models=good)),
            ("checked another version", dict(version="v1.2.0", models=good)),
            ("models.json isn't the one", dict(models="0" * 64)),
        ]
        for why, build in cases:
            with self.subTest(why):
                self.store.put("UPDATE_STATE", {})
                cb.builds.clear()
                self.tick(SUNDAY_3AM)
                u.handle({"action": "models_verified", "version": "v1.1.0", "build": "c-1", "status": "verified", "manifest_text": text},
                         self.env, self.store, SUNDAY_3AM, self.releases, self.worker)
                cb.finish("c-1", **build)
                with self.assertLogs(u.logger, "WARNING") as logs:
                    u.handle({"action": "status"}, self.env, self.store, SUNDAY_3AM, self.releases, self.worker)
                self.assertIn(why, "\n".join(logs.output))
                self.assertIsNone(self.store.get("MANIFEST#v1.1.0"))
                self.assertNotIn("verifying", self.store.get("UPDATE_STATE"))
        self.assertEqual(self.worker.payloads, [])
        # With no check running, a report is refused outright.
        with self.assertRaisesRegex(ValueError, "no release check of v1.1.0 is running"):
            u.handle({"action": "models_verified", "version": "v1.1.0", "build": "c-1", "status": "verified", "manifest_text": text},
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
        u.handle = lambda event, env, store, now, **k: real_handle(event, env, store, now, self.releases, self.worker)
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


    def test_one_reminder_after_a_week_of_waiting_and_no_more(self):
        self.tick(SUNDAY_3AM)
        self.assertEqual(self.notices, ["update to v1.1.0 needs your OK"])
        for h in range(6, 24 * 12, 6):
            self.tick(SUNDAY_3AM + timedelta(hours=h))
        self.assertEqual(self.notices, ["update to v1.1.0 needs your OK", "update to v1.1.0 still needs your OK"])


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


class NoRoute53:
    """CoderHelm never changes DNS: any Route 53 call fails the test."""
    def __getattr__(self, name):
        raise AssertionError(f"route53.{name} called: CoderHelm never touches DNS")


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
        u._clients.update({"acm@us-east-1": self.acm, "route53": NoRoute53(), "ssm": FakeSsm()})
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
        self.assertEqual(self.notices[-1], "now answers on helm.example.org")
        self.assertNotIn("failed_versions", self.store.get("UPDATE_STATE"))

    def test_dns_is_never_touched_and_the_two_records_are_always_shown(self):
        # Route 53 is a tripwire here: setting the domain, checking it and the
        # move after the certificate is issued never call it.
        v = u.domain_set(self.store, self.env, SUNDAY_3AM, "helm.example.org", "a", sleep=lambda s: None)
        self.assertEqual(v["mode"], "external")
        self.assertNotIn("route53_offer", v)
        self.assertEqual([r["purpose"] for r in v["records"]], ["certificate", "address"])
        self.acm.status = "ISSUED"
        self.h({"action": "domain_status"})
        self.assertEqual((self.builds[0][3]["DOMAIN_ZONE"], self.builds[0][3]["DOMAIN_ZONE_ID"]), ("", ""))
        with self.assertRaises(ValueError):
            self.h({"action": "domain_use_route53", "zone": "example.org"})

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
        self.assertEqual(self.notices[-1], "the address change failed")
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
        self.assertEqual(self.notices[-1], "back on the AWS address")


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
        self.assertTrue(headers["x-coderhelm-signature"].startswith("v2="))
        self.assertRegex(headers["x-coderhelm-request-id"], r"^req_[a-z0-9]{32}$")

    def test_the_signature_matches_the_services(self):
        # services/common/src/hybrid/sign.rs, the_signature_matches_the_installer_and_a_fixed_vector
        self.assertEqual(
            u.sign_request(
                "s3cret", 1_800_000_000, "delete", "/hybrid/installs?x=1", b"",
                "req_abcdefghijklmnopqrstuvwxyz012345",
            ),
            "v2=4769ebe0b0e6f0a907f4db72095eb65f2729f7763d2793b8ea4a18965508d0f8",
        )

    def test_each_call_gets_its_own_request_id(self):
        self.assertNotEqual(u.new_request_id(), u.new_request_id())

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

    BLOCKED = {"current": "v0.7.4", "checked_at": "2026-10-31 14:05 UTC",
               "reason": "- claude-opus-5-5: Bedrock refused the call.",
               "action": "- claude-opus-5-5: request model access in the Bedrock console."}

    def test_own_ses_names_the_install_never_its_cloudfront_address(self):
        u._clients["dynamodb"] = self.Dyn()
        u.notify("s", "t", self.env, what="blocked", version="v0.8.0", opener=self.opener, facts=self.BLOCKED)
        msg = self.sent[0]
        self.assertEqual(msg["FromEmailAddress"], '"CoderHelm" <ch@example.org>')
        self.assertNotIn("cloudfront", msg["FromEmailAddress"])
        self.assertEqual(msg["Content"]["Simple"]["Subject"]["Data"], "Your CoderHelm install: update to v0.8.0 needs your OK")
        self.sent.clear()
        env = dict(self.env, ORGANIZATION_NAME="Acme")
        u.notify("s", "t", env, what="blocked", version="v0.8.0", opener=self.opener, facts=self.BLOCKED)
        msg = self.sent[0]
        self.assertEqual(msg["FromEmailAddress"], '"Acme CoderHelm" <ch@example.org>')
        self.assertEqual(msg["Content"]["Simple"]["Subject"]["Data"], "Acme CoderHelm: update to v0.8.0 needs your OK")
        # The install's own address when no organization is named.
        self.sent.clear()
        u.notify("s", "t", dict(self.env, APP_URL="https://acme.coderhelm.com"), what="available", version="v0.8.0", opener=self.opener)
        self.assertEqual(self.sent[0]["FromEmailAddress"], '"acme CoderHelm" <ch@example.org>')
        # An admin's own sender name wins, and a non-ASCII one is encoded.
        self.sent.clear()
        u.notify("s", "t", dict(self.env, EMAIL_FROM_NAME="Société Robots"), what="available", version="v0.8.0", opener=self.opener)
        self.assertTrue(self.sent[0]["FromEmailAddress"].startswith("=?UTF-8?B?"))
        self.assertTrue(self.sent[0]["FromEmailAddress"].endswith(" <ch@example.org>"))

    def test_the_sender_name_set_in_settings_wins_over_the_config(self):
        class ByKey:
            def __init__(self, items):
                self.items = items

            def get_item(self, **kw):
                item = self.items.get(kw["Key"]["sk"]["S"])
                return {"Item": item} if item else {}

        def stored(transport, sender):
            return ByKey({"SETTINGS#EMAIL_OPTIONAL": {"data": {"S": json.dumps({"transport": transport})}},
                          "SETTINGS#EMAIL_SENDER": {"data": {"S": json.dumps(sender)}}})

        env = dict(self.env, ORGANIZATION_NAME="Config Org", EMAIL_FROM_NAME="Config Name")
        # Own SES: the stored sender name, as typed; subjects name the stored organization.
        u._clients["dynamodb"] = stored("ses", {"sender_name": "Acme Robots", "organization": "Acme"})
        u.notify("s", "t", env, what="blocked", version="v0.8.0", opener=self.opener, facts=self.BLOCKED)
        msg = self.sent[0]
        self.assertEqual(msg["FromEmailAddress"], '"Acme Robots" <ch@example.org>')
        self.assertEqual(msg["Content"]["Simple"]["Subject"]["Data"], "Acme CoderHelm: update to v0.8.0 needs your OK")
        # Only an organization: "<org> CoderHelm", over the config's name.
        self.sent.clear()
        u._clients["dynamodb"] = stored("ses", {"organization": "Acme"})
        u.notify("s", "t", env, what="available", version="v0.8.0", opener=self.opener)
        self.assertEqual(self.sent[0]["FromEmailAddress"], '"Acme CoderHelm" <ch@example.org>')
        # Nothing stored: the config, as before.
        self.sent.clear()
        u._clients["dynamodb"] = stored("ses", {})
        u.notify("s", "t", env, what="available", version="v0.8.0", opener=self.opener)
        self.assertEqual(self.sent[0]["FromEmailAddress"], '"Config Name" <ch@example.org>')
        # A stored name with a line break is not used.
        self.sent.clear()
        u._clients["dynamodb"] = stored("ses", {"sender_name": "Acme\r\nBcc: x@example.org"})
        u.notify("s", "t", env, what="available", version="v0.8.0", opener=self.opener)
        self.assertEqual(self.sent[0]["FromEmailAddress"], '"Config Name" <ch@example.org>')
        # Through CoderHelm: the stored organization goes to the relay.
        u._clients["dynamodb"] = stored("coderhelm", {"sender_name": "Acme Robots", "organization": "Acme"})
        u.notify("s", "t", env, what="available", version="v0.8.0", opener=self.opener)
        self.assertEqual(self.posts[-1][1]["org"], "Acme")
        u._clients["dynamodb"] = stored("coderhelm", {"sender_name": "Acme Robots"})
        u.notify("s", "t", env, what="available", version="v0.8.0", opener=self.opener)
        self.assertEqual(self.posts[-1][1]["org"], "Acme Robots")

    def test_the_available_notice_links_the_release_notes_and_names_no_command(self):
        n = u.notice("available", "v0.8.0", dict(self.env, APP_URL="https://helm.example.org"),
                     {"current": "v0.7.4", "notes": "Faster reviews on large pull requests."})
        self.assertIn("press Update now. The release notes say what changed.", n["text"])
        self.assertIn("https://helm.example.org/settings/system/#version", n["text"])
        for gone in ["What's new", "Faster reviews", "update --to", "npx"]:
            self.assertNotIn(gone, n["text"])
            self.assertNotIn(gone, n["html"])

    def test_the_blocked_notice_says_which_install_versions_why_and_what_to_do(self):
        n = u.notice("blocked", "v0.8.0", dict(self.env, APP_URL="https://helm.example.org"), self.BLOCKED)
        for want in ["Install: helm.example.org", "Running now: v0.7.4", "Waiting to install: v0.8.0",
                     "Checked: 2026-10-31 14:05 UTC", "Why it's blocked:", "Bedrock refused the call", "What to do:",
                     "Update readiness", "Check again", "doctor",
                     "https://helm.example.org/settings/system/#readiness", "https://helm.example.org/settings/system/#version"]:
            self.assertIn(want, n["text"])
        self.assertNotIn("update --to", n["text"])
        self.assertIn("<table", n["html"])
        self.assertEqual(n["subject"], "helm.example.org CoderHelm: update to v0.8.0 needs your OK")
        # An older shape: no facts, nothing blank.
        n = u.notice("blocked", "v0.8.0", self.env)
        self.assertNotIn("Running now", n["text"])
        self.assertNotIn("{", n["text"])

    def test_through_coderhelm_the_notice_carries_the_facts_and_the_organization(self):
        u._clients["dynamodb"] = self.Dyn({"data": {"S": json.dumps({"transport": "coderhelm"})}})
        env = dict(self.env, ORGANIZATION_NAME="Acme")
        u.notify("s", "t", env, what="blocked", version="v0.8.0", opener=self.opener, facts=self.BLOCKED)
        body = self.posts[1][1]
        self.assertEqual(body["org"], "Acme")
        p = body["params"]
        self.assertEqual((p["what"], p["version"], p["current"]), ("blocked", "v0.8.0", "v0.7.4"))
        self.assertEqual(p["link"], "https://d1.cloudfront.net/settings/system/#readiness")
        self.assertEqual(p["notes_link"], "https://d1.cloudfront.net/settings/system/#version")
        self.assertIn("Bedrock refused", p["reason"])
        self.assertEqual(p["checked_at"], "2026-10-31 14:05 UTC")

    def test_relay_parameters_stay_within_the_templates_limits(self):
        p = u.update_params("available", "v0.8.0", self.env, {
            "current": "not a version!", "checked_at": "x" * 100, "waiting": "line\nbreak " * 40,
            "notes": "n" * 5000, "reason": "Ask CoderHelm support for help", "major": True})
        self.assertNotIn("current", p)
        self.assertLessEqual(len(p["checked_at"]), 40)
        self.assertLessEqual(len(p["waiting"]), 160)
        self.assertNotIn("\n", p["waiting"])
        self.assertLessEqual(len(p["notes"]), 1200)
        self.assertNotIn("reason", p, "wording the relay refuses is left out, not sent")
        self.assertEqual(p["major"], "yes")
        self.assertEqual(set(p) - {"what", "version", "link", "current", "checked_at", "reason", "action",
                                   "waiting", "notes", "major", "notes_link"}, set())


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


class SafeButtonLinks(unittest.TestCase):
    def test_a_button_link_with_spaces_or_controls_is_left_out(self):
        self.assertIsNone(u.safe_link("https://acme.example/a\n\nReset at evil.vip"))
        self.assertIsNone(u.safe_link("https://acme.example/a b"))
        self.assertIsNone(u.safe_link("http://acme.example/"))
        self.assertIsNone(u.safe_link("https://user@acme.example/"))
        self.assertIsNone(u.safe_link("javascript:alert(1)"))
        self.assertEqual(u.safe_link("https://acme.example/settings/system/#version"),
                         "https://acme.example/settings/system/#version")
