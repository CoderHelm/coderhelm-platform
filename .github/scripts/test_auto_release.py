"""Tests for auto_release.py. Run: python3 -m unittest discover -s .github/scripts"""

import os
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import auto_release as ar  # noqa: E402

SHA = "a" * 40


def pr(n, title="Fix a thing", labels=()):
    return {"number": n, "title": title, "labels": list(labels)}


def plan(tags=("v0.6.0",), at=(), ancestors=None, changed=("services/gateway/src/main.rs",), prs=(), floor=None, on_main=None):
    anc = set(tags if ancestors is None else ancestors)
    return ar.plan(SHA, list(tags), list(at), lambda t: t in anc, lambda prev: list(changed), lambda prev: list(prs), floor, on_main=on_main)


class Version(unittest.TestCase):
    def test_patch_by_default_minor_for_features(self):
        self.assertEqual(ar.next_version((0, 7, 0), [pr(1)]), (0, 7, 1))
        self.assertEqual(ar.next_version((0, 7, 3), [pr(1), pr(2, labels=["enhancement"])]), (0, 8, 0))
        self.assertEqual(ar.next_version((0, 7, 3), [pr(2, labels=["Feature"])]), (0, 8, 0))
        self.assertEqual(ar.next_version((0, 7, 3), [pr(2, "feat(installer): pick a region")]), (0, 8, 0))
        self.assertEqual(ar.next_version((0, 7, 3), [pr(2, "feat: x")]), (0, 8, 0))
        self.assertEqual(ar.next_version((0, 7, 3), [pr(2, "Features of the run page")]), (0, 7, 4), "not a conventional prefix")

    def test_installer_version_is_a_floor(self):
        # The first self-host release: v0.6.0 is the last pre-self-host tag, installer/package.json says 0.7.0.
        self.assertEqual(ar.next_version((0, 6, 0), [pr(1)], floor=(0, 7, 0)), (0, 7, 0))
        self.assertEqual(ar.next_version((0, 7, 0), [pr(1)], floor=(0, 7, 0)), (0, 7, 1))
        self.assertEqual(ar.next_version((0, 9, 2), [pr(1)], floor=(1, 0, 0)), (1, 0, 0), "a maintainer asks for a major")

    def test_only_stable_tags_count(self):
        self.assertEqual(ar.parse("v1.2.3"), (1, 2, 3))
        for t in ("v1.2.3-rc.1", "1.2.3", "v1.2", "release-1", "", None):
            self.assertIsNone(ar.parse(t), t)


class Shipped(unittest.TestCase):
    def test_paths_the_release_is_built_from(self):
        for p in ("services/worker/src/lib.rs", "lambda/mcp-proxy/src/index.ts", "lambda/updater/handler.py", "infra/cfn/coderhelm-selfhost-launch.yaml",
                  "installer/lib/release.mjs", "selfhost/versions.json", "selfhost/install.sh", "scripts/check-dashboard-ref.mjs", ".github/workflows/release.yml"):
            self.assertEqual(ar.ships([p]), [p], p)
        for p in ("docs/self-host/updates.md", "README.md", ".github/workflows/ci.yml", "coderhelm-jira/src/index.js", "plugins/x.md",
                  "scripts/bump-dashboard.sh", "servicesX/readme", "infra"):
            self.assertEqual(ar.ships([p]), [], p)


class Plan(unittest.TestCase):
    def test_first_self_host_release(self):
        r = plan(tags=("v0.5.0", "v0.6.0"), prs=[pr(3), pr(4)], floor=(0, 7, 0))
        self.assertFalse(r["skip"])
        self.assertEqual((r["tag"], r["previous"]), ("v0.7.0", "v0.6.0"))

    def test_skips(self):
        self.assertIn("already released as v0.7.0", plan(tags=("v0.6.0", "v0.7.0"), at=("v0.7.0",))["reason"])
        r = plan(changed=("docs/self-host/faq.md", ".github/workflows/ci.yml"))
        self.assertTrue(r["skip"])
        self.assertIn("nothing that ships", r["reason"])
        # A later commit of main was already released (v0.7.1 is on main, not in this commit's history).
        r = plan(tags=("v0.7.0", "v0.7.1"), ancestors=("v0.7.0",), on_main=lambda t: True)
        self.assertTrue(r["skip"])
        self.assertIn("v0.7.1 is newer", r["reason"])

    def test_pre_releases_are_ignored_for_the_base(self):
        r = plan(tags=("v0.7.0", "v0.8.0-rc.1"), ancestors=("v0.7.0", "v0.8.0-rc.1"))
        self.assertEqual(r["tag"], "v0.7.1")

    def test_tags_off_main_are_not_releases(self):
        # Someone pushed v0.7.1 on a branch: it is not the base, does not block, and its name is skipped, never reused.
        tags = ("v0.7.0", "v0.7.1")
        r = ar.plan(SHA, list(tags), [], lambda t: t == "v0.7.0", lambda p: ["services/a.rs"], lambda p: [], None, on_main=lambda t: t == "v0.7.0")
        self.assertEqual((r["skip"], r["tag"], r["previous"]), (False, "v0.7.2", "v0.7.0"))
        # A v9.9.9 off main changes nothing either.
        r = ar.plan(SHA, ["v0.7.0", "v9.9.9"], [], lambda t: t == "v0.7.0", lambda p: ["services/a.rs"], lambda p: [], None, on_main=lambda t: t == "v0.7.0")
        self.assertEqual(r["tag"], "v0.7.1")

    def test_no_release_yet(self):
        r = plan(tags=(), prs=[pr(1)], floor=(0, 7, 0))
        self.assertEqual((r["tag"], r["previous"]), ("v0.7.0", None))


class Notes(unittest.TestCase):
    def test_lists_pull_requests_and_compare_link(self):
        n = ar.notes("CoderHelm/coderhelm-platform", "v0.8.0", "v0.7.2", [pr(5, "Fix\nthe   gateway"), pr(7, "x", ["enhancement"])])
        self.assertIn("### Features\n\n- x (#7)", n)
        self.assertIn("### Fixes and changes\n\n- Fix the gateway (#5)", n)
        self.assertIn("https://github.com/CoderHelm/coderhelm-platform/compare/v0.7.2...v0.8.0", n)
        self.assertNotIn("\n#", n.replace("\n## ", "").replace("\n### ", ""), "a title can't start a heading")
        self.assertIn("Maintenance changes", ar.notes("o/r", "v1.0.1", "v1.0.0", []))
        many = ar.notes("o/r", "v1.0.1", "v1.0.0", [pr(n) for n in range(1, 151)])
        self.assertIn("- Fix a thing (#150)", many)
        self.assertNotIn("(#50)", many)
        self.assertIn("And 50 earlier pull request(s).", many)


class WithGit(unittest.TestCase):
    """The plan against a real repository (no GitHub: the PR lookup finds none)."""

    def test_real_history(self):
        d = tempfile.mkdtemp()
        env = dict(os.environ, GIT_CONFIG_GLOBAL="/dev/null", GIT_CONFIG_NOSYSTEM="1", GIT_AUTHOR_NAME="M", GIT_AUTHOR_EMAIL="m@e",
                   GIT_COMMITTER_NAME="M", GIT_COMMITTER_EMAIL="m@e")

        def git(*a):
            return subprocess.run(["git", *a], cwd=d, env=env, check=True, capture_output=True, text=True).stdout.strip()

        def write(p, s):
            os.makedirs(os.path.dirname(os.path.join(d, p)), exist_ok=True)
            with open(os.path.join(d, p), "w") as f:
                f.write(s)

        git("init", "-q", "-b", "main")
        write("installer/package.json", '{"version": "0.7.0"}')
        git("add", "-A")
        git("commit", "-q", "-m", "start")
        git("tag", "-a", "v0.6.0", "-m", "v0.6.0")
        write("docs/a.md", "x")
        git("add", "-A")
        git("commit", "-q", "-m", "docs only")
        head = git("rev-parse", "HEAD")
        old, pulls = os.getcwd(), ar._merged_prs
        os.chdir(d)
        try:
            ar._merged_prs = lambda repo, since: []
            self.assertTrue(ar.git_plan("o/r", head)["skip"])
            write("services/x.rs", "fn main() {}")
            git("add", "-A")
            git("commit", "-q", "-m", "code")
            r = ar.git_plan("o/r", git("rev-parse", "HEAD"))
            self.assertEqual((r["skip"], r["tag"], r["previous"]), (False, "v0.7.0", "v0.6.0"))
        finally:
            os.chdir(old)
            ar._merged_prs = pulls


if __name__ == "__main__":
    unittest.main()
