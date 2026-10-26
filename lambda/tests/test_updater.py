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
        u.notify = lambda subject, text, env=None: self.notices.append(subject) or 1
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
