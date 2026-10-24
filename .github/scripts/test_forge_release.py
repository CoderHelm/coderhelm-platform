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
        self.assertIn("A site admin must accept", md)
        self.assertIn("Apps → Manage apps", md)
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
                self.assertIn("Manage apps", f.read())

    def test_summary_from_saved_result(self):
        with tempfile.TemporaryDirectory() as d:
            out = os.path.join(d, "r.json")
            r = fr.diff(BASE, copy.deepcopy(BASE))
            r["needs_admin"], r["reasons"] = True, ["Forge lint reports a major version"]
            with open(out, "w") as f:
                json.dump(r, f)
            text = self.run_cli("summary", "--json", out)
            self.assertIn("A site admin must accept", text)
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
            self.assertEqual(self.run_cli("pending", "--installs", path), "a.example.net\tjira\n")
            self.assertEqual(self.run_cli("pending", "--installs", path, "--environment", "staging"), "c.example.net\tjira\n")


if __name__ == "__main__":
    unittest.main()
