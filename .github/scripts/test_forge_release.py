"""Tests for forge_release.py. Run: python3 -m unittest discover -s .github/scripts"""

import copy
import io
import json
import os
import sys
import tempfile
import unittest
from contextlib import redirect_stdout

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import yaml  # noqa: E402

import forge_release as fr  # noqa: E402

FEATURES = os.path.join(os.path.dirname(__file__), "..", "..", "services", "common", "src", "jira_app_features.json")
REPO_MANIFEST = os.path.join(os.path.dirname(__file__), "..", "..", "coderhelm-jira", "manifest.yml")

BASE = {
    "modules": {
        "trigger": [{"key": "issue-trigger", "function": "f", "events": ["avi:jira:created:issue"]}],
        "webtrigger": [{"key": "list-trigger", "function": "f"}],
        "function": [{"key": "f", "handler": "index.handler"}],
    },
    "permissions": {
        "scopes": ["read:jira-work", "storage:app"],
        "external": {"fetch": {"backend": ["https://api.example.com"]}},
    },
    "app": {"id": "ari:cloud:ecosystem::app/x"},
}


def changed(fn):
    m = copy.deepcopy(BASE)
    fn(m)
    return m


class DiffTests(unittest.TestCase):
    def test_unchanged_needs_no_admin(self):
        r = fr.diff(BASE, copy.deepcopy(BASE))
        self.assertFalse(r["needs_admin"])
        self.assertEqual(r["reasons"], [])
        self.assertIn("no one needs to do anything", fr.summary_markdown(r))

    def test_code_and_event_changes_need_no_admin(self):
        def edit(m):
            m["modules"]["trigger"][0]["events"].append("avi:jira:updated:issue")
            m["modules"]["function"].append({"key": "g", "handler": "index.g"})
        r = fr.diff(BASE, changed(edit))
        self.assertFalse(r["needs_admin"])
        self.assertEqual(r["modules_added"], ["function:g"])

    def test_widened_scopes_need_admin(self):
        r = fr.diff(BASE, changed(lambda m: m["permissions"]["scopes"].append("write:jira-work")))
        self.assertTrue(r["needs_admin"])
        self.assertEqual(r["scopes_added"], ["write:jira-work"])
        md = fr.summary_markdown(r)
        self.assertIn("An Atlassian site admin must accept", md)
        self.assertIn("Atlassian Administration (admin.atlassian.com): Apps, Sites, choose the site, Connected apps, Coderhelm, View app details, Update", md)
        self.assertNotIn("Manage apps", md)
        self.assertIn("write:jira-work", md)

    def test_removed_scope_still_needs_admin(self):
        # Forge makes a major version for a removed scope as well.
        r = fr.diff(BASE, changed(lambda m: m["permissions"]["scopes"].remove("storage:app")))
        self.assertTrue(r["needs_admin"])
        self.assertEqual(r["scopes_removed"], ["storage:app"])

    def test_scope_order_does_not_matter(self):
        r = fr.diff(BASE, changed(lambda m: m["permissions"]["scopes"].reverse()))
        self.assertFalse(r["needs_admin"])

    def test_new_egress_needs_admin(self):
        r = fr.diff(BASE, changed(lambda m: m["permissions"]["external"]["fetch"]["backend"].append("https://other.example.com")))
        self.assertTrue(r["needs_admin"])
        self.assertEqual(r["egress_added"], ["external.fetch.backend: https://other.example.com"])

    def test_new_egress_kind_needs_admin(self):
        r = fr.diff(BASE, changed(lambda m: m["permissions"]["external"].update({"images": ["https://img.example.com"]})))
        self.assertTrue(r["needs_admin"])

    def test_egress_category_change_needs_admin(self):
        def edit(m):
            m["permissions"]["external"]["fetch"]["backend"] = [{"address": "https://api.example.com", "category": "analytics"}]
        r = fr.diff(BASE, changed(edit))
        self.assertTrue(r["needs_admin"])

    def test_removed_egress_needs_no_admin(self):
        r = fr.diff(BASE, changed(lambda m: m["permissions"].pop("external")))
        self.assertFalse(r["needs_admin"])
        self.assertIn("Access removed", fr.summary_markdown(r))

    def test_new_dynamic_webtrigger_needs_admin(self):
        r = fr.diff(BASE, changed(lambda m: m["modules"]["webtrigger"].append({"key": "new-trigger", "function": "f"})))
        self.assertTrue(r["needs_admin"])
        self.assertEqual(r["dynamic_webtriggers_added"], ["new-trigger"])

    def test_new_static_webtrigger_needs_no_admin(self):
        def edit(m):
            m["modules"]["webtrigger"].append({"key": "s", "function": "f", "response": {"type": "static", "outputs": []}})
        r = fr.diff(BASE, changed(edit))
        self.assertFalse(r["needs_admin"])

    def test_licensing_and_remotes_need_admin(self):
        self.assertTrue(fr.diff(BASE, changed(lambda m: m["app"].update({"licensing": {"enabled": True}})))["needs_admin"])
        self.assertTrue(fr.diff(BASE, changed(lambda m: m.update({"remotes": [{"key": "r", "baseUrl": "https://r.example.com"}]})))["needs_admin"])

    def test_no_earlier_manifest_is_flagged(self):
        r = fr.diff({}, BASE)
        self.assertTrue(r["needs_admin"])
        self.assertIn("no earlier release to compare with", r["reasons"])

    def test_real_manifest_against_itself(self):
        with open(REPO_MANIFEST, encoding="utf-8") as f:
            m = yaml.safe_load(f)
        self.assertFalse(fr.diff(m, copy.deepcopy(m))["needs_admin"])


class CliTests(unittest.TestCase):
    def run_cli(self, *args):
        buf = io.StringIO()
        with redirect_stdout(buf):
            self.assertEqual(fr.main(list(args)), 0)
        return buf.getvalue()

    def write(self, d, name, data, as_yaml=True):
        path = os.path.join(d, name)
        with open(path, "w", encoding="utf-8") as f:
            (yaml.safe_dump if as_yaml else json.dump)(data, f)
        return path

    def test_diff_dry_run_writes_summary_and_json(self):
        with tempfile.TemporaryDirectory() as d:
            old = self.write(d, "old.yml", BASE)
            new = self.write(d, "new.yml", changed(lambda m: m["permissions"]["scopes"].append("read:me")))
            summary, out = os.path.join(d, "summary.md"), os.path.join(d, "r.json")
            text = self.run_cli("diff", "--old", old, "--new", new, "--json", out, "--summary", summary)
            self.assertIn("needs_admin=true", text)
            with open(out) as f:
                self.assertEqual(json.load(f)["scopes_added"], ["read:me"])
            with open(summary) as f:
                text = f.read()
                self.assertIn("Connected apps", text)
                self.assertNotIn("Manage apps", text)

    def test_summary_from_saved_result(self):
        with tempfile.TemporaryDirectory() as d:
            out = os.path.join(d, "r.json")
            r = fr.diff(BASE, copy.deepcopy(BASE))
            r["needs_admin"], r["reasons"] = True, ["Forge lint reports a major version"]
            with open(out, "w") as f:
                json.dump(r, f)
            text = self.run_cli("summary", "--json", out)
            self.assertIn("An Atlassian site admin must accept", text)
            self.assertNotIn("no one needs to do anything", text)

    def test_diff_missing_old_file(self):
        with tempfile.TemporaryDirectory() as d:
            new = self.write(d, "new.yml", BASE)
            self.assertIn("needs_admin=true", self.run_cli("diff", "--old", os.path.join(d, "nope.yml"), "--new", new))

    def test_pending_lists_only_behind_production_installs(self):
        installs = [
            {"environment": "production", "site": "a.example.net", "product": "Jira", "majorVersion": "5 (Out-of-date)", "status": "Outdated app"},
            {"environment": "production", "site": "b.example.net", "product": "Confluence", "majorVersion": "6", "status": "Up-to-date"},
            {"environment": "staging", "site": "c.example.net", "product": "Jira", "majorVersion": "1 (Out-of-date)", "status": "Outdated app"},
        ]
        with tempfile.TemporaryDirectory() as d:
            path = self.write(d, "i.json", installs, as_yaml=False)
            self.assertEqual(self.run_cli("pending", "--installs", path), "a.example.net\tjira\t5\n")
            self.assertEqual(self.run_cli("pending", "--installs", path, "--environment", "staging"), "c.example.net\tjira\t1\n")

    def test_the_installation_link_rides_on_the_pointer_and_the_steps(self):
        link = "https://developer.atlassian.com/console/install/00000000-0000-0000-0000-000000000002?signature=abc&product=jira"
        p = fr.latest_pointer([{"version": "7.0.0"}], {"app": {"id": "ari:x"}}, install_url=link)
        self.assertEqual(p["installUrl"], link)
        self.assertNotIn("installUrl", fr.latest_pointer([{"version": "7.0.0"}], {}, install_url="https://example.com/x"))
        self.assertIn(link, fr.admin_steps(link))
        self.assertIn("Connected apps", fr.admin_steps(link))
        self.assertNotIn("Manage apps", fr.admin_steps() + fr.admin_steps(link))

    def test_latest_pointer_is_the_highest_production_version(self):
        versions = [{"version": "5.20.0"}, {"version": "6.11.0"}, {"version": "6.9.0"}, {"version": "soon"}]
        manifest = {"app": {"id": "ari:cloud:ecosystem::app/00000000-0000-0000-0000-000000000001"}}
        self.assertEqual(fr.latest_version(versions), "6.11.0")
        self.assertEqual(fr.latest_version([]), None)
        p = fr.latest_pointer(versions, manifest, "abc123", "2026-11-14T00:00:00Z")
        self.assertEqual(p, {"version": "6.11.0", "appId": manifest["app"]["id"], "commit": "abc123", "publishedAt": "2026-11-14T00:00:00Z"})
        self.assertIsNone(fr.latest_pointer([], manifest))
        with tempfile.TemporaryDirectory() as d:
            v = self.write(d, "v.json", versions, as_yaml=False)
            m = self.write(d, "m.yml", manifest)
            self.assertEqual(json.loads(self.run_cli("latest", "--versions", v, "--manifest", m))["version"], "6.11.0")

    def test_behind_lists_each_site_by_number_with_what_it_lacks(self):
        features = fr.load_features(FEATURES)
        names = [n for _, n in features]
        for f in ["full comment reading", "mentions", "in-place edits", "threaded replies", "issue type", "extended issue fields"]:
            self.assertIn(f, names)
        self.assertEqual(fr.missing_on_major(features, 5), names)
        self.assertEqual(fr.missing_on_major(features, 6), [])
        self.assertEqual(fr.missing_on_major(features, None), names)
        md, warning = fr.behind_report(["5\tjira\n", "\n", "\tjira\n"], "6.11.0", features)
        self.assertIn("- Site 1 (jira): major version 5 (latest 6.11.0). Missing: ", md)
        self.assertIn("- Site 2 (jira): an unknown version (latest 6.11.0).", md)
        self.assertIn("threaded replies", md)
        self.assertNotIn("example", md)
        self.assertTrue(warning.startswith("2 installation(s) still run an older Coderhelm app (latest 6.11.0). They lack: "))
        self.assertIn("Connected apps, Coderhelm, View app details, Update.", warning)
        self.assertNotIn("Manage apps", warning + md)
        self.assertEqual(fr.behind_report([], "6.11.0", features), ("", ""))
        self.assertEqual(fr.parse_version("6.10"), (6, 10, 0))
        self.assertIsNone(fr.parse_version("6.x"))


if __name__ == "__main__":
    unittest.main()
