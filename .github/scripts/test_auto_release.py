"""Tests for auto_release.py. Run: python3 -m unittest discover -s .github/scripts"""

import os
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import auto_release as ar  # noqa: E402

SHA = "a" * 40


def pr(n, title="Fix a thing", labels=()):
    return {"number": n, "title": title, "labels": list(labels)}


def plan(tags=("v0.6.0",), at=(), ancestors=None, changed=("services/gateway/src/main.rs",), prs=(), floor=None, on_main=None,
         channel="stable", date="20261105", since=None):
    anc = set(tags if ancestors is None else ancestors)
    asked = since if since is not None else []
    return ar.plan(SHA, list(tags), list(at), lambda t: t in anc, lambda prev: asked.append(prev) or list(changed),
                   lambda prev: list(prs), floor, on_main=on_main, channel=channel, date=date)


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
        for p in ("services/worker/src/lib.rs", "lambda/mcp-proxy/src/index.ts", "lambda/updater/handler.py", "infra/lib/native/parent.ts", "lambda/native-helper/handler.py",
                  "installer/lib/release.mjs", "selfhost/versions.json", "selfhost/install.sh", "scripts/check-dashboard-ref.mjs", "scripts/rust-build.sh", "scripts/rust-inputs.mjs",
                  ".github/scripts/rust-build-reuse.sh", ".github/workflows/release.yml"):
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

    def test_deployed_commit_behind_main_is_released(self):
        # main moved on during the deploy (its new commits are untagged): the deployed commit is still released.
        r = plan(tags=("v0.7.0",), ancestors=("v0.7.0",), on_main=lambda t: True)
        self.assertEqual((r["skip"], r["tag"], r["previous"]), (False, "v0.7.1", "v0.7.0"))

    def test_an_older_deploy_never_releases_after_a_newer_one(self):
        # A later commit's release exists: the older commit is never released, so latest never goes back.
        for newer in ("v0.7.1", "v0.8.0", "v1.0.0"):
            r = plan(tags=("v0.7.0", newer), ancestors=("v0.7.0",), on_main=lambda t: True)
            self.assertTrue(r["skip"], newer)
            self.assertNotIn("tag", r)

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


class Beta(unittest.TestCase):
    def test_tags(self):
        self.assertEqual(ar.parse_beta("v0.8.38-beta.20261105"), ((0, 8, 38), "20261105"))
        for t in ("v0.8.38", "v0.8.38-beta.2026110", "v0.8.38-rc.1", "v0.8.38-beta.20261105.1", None):
            self.assertIsNone(ar.parse_beta(t), t)
        self.assertEqual(ar.target("v0.8.38-beta.20261105"), (0, 8, 38))
        ordered = sorted(["v0.8.39", "v0.8.38", "v0.8.39-beta.20261106", "v0.8.39-beta.20261105", "v0.8.40-beta.20261101"], key=ar.order)
        self.assertEqual(ordered, ["v0.8.38", "v0.8.39-beta.20261105", "v0.8.39-beta.20261106", "v0.8.39", "v0.8.40-beta.20261101"])

    def test_nightly_beta(self):
        r = plan(tags=("v0.8.37",), channel="beta")
        self.assertEqual((r["skip"], r["tag"], r["stable_tag"], r["previous"]), (False, "v0.8.38-beta.20261105", "v0.8.38", "v0.8.37"))
        # Every beta gets its own number: tomorrow's comes after yesterday's, promoted or not.
        r = plan(tags=("v0.8.37", "v0.8.38-beta.20261105"), channel="beta", date="20261106")
        self.assertEqual((r["tag"], r["previous"]), ("v0.8.39-beta.20261106", "v0.8.38-beta.20261105"))
        r = plan(tags=("v0.8.37", "v0.8.38-beta.20261105"), channel="beta", date="20261106", prs=[pr(1, "feat: x")])
        self.assertEqual(r["tag"], "v0.9.0-beta.20261106")

    def test_beta_skips(self):
        r = plan(tags=("v0.8.37", "v0.8.38-beta.20261105"), at=("v0.8.38-beta.20261105",), channel="beta")
        self.assertIn("already released as v0.8.38-beta.20261105", r["reason"])
        self.assertTrue(plan(tags=("v0.8.37",), at=("v0.8.37",), channel="beta")["skip"])
        # Main didn't change anything that ships since the last beta.
        since = []
        r = plan(tags=("v0.8.37", "v0.8.38-beta.20261105"), channel="beta", changed=("docs/x.md",), since=since)
        self.assertTrue(r["skip"])
        self.assertEqual(since, ["v0.8.38-beta.20261105"])
        # A later commit already has a beta.
        r = plan(tags=("v0.8.37", "v0.8.38-beta.20261105"), ancestors=("v0.8.37",), on_main=lambda t: True, channel="beta")
        self.assertIn("v0.8.38-beta.20261105 is newer", r["reason"])
        with self.assertRaises(ValueError):
            plan(channel="beta", date=None)

    def test_an_urgent_stable_release_sorts_above_every_beta(self):
        # Betas v0.8.38 and v0.8.39 are out, neither promoted: the urgent release is v0.8.40.
        since = []
        r = plan(tags=("v0.8.37", "v0.8.38-beta.20261105", "v0.8.39-beta.20261106"), since=since)
        self.assertEqual((r["tag"], r["previous"]), ("v0.8.40", "v0.8.37"))
        self.assertEqual(since, ["v0.8.37"], "a stable release counts changes since the last stable one")
        # Right after tonight's beta of the same commit: still released, the beta doesn't count as stable.
        r = plan(tags=("v0.8.37", "v0.8.38-beta.20261105"), at=("v0.8.38-beta.20261105",))
        self.assertEqual(r["tag"], "v0.8.39")
        # And a promoted beta's number is never reused.
        r = plan(tags=("v0.8.37", "v0.8.38-beta.20261105", "v0.8.38"))
        self.assertEqual(r["tag"], "v0.8.39")

    def test_a_stable_number_taken_off_main_is_skipped(self):
        r = ar.plan(SHA, ["v0.8.37", "v0.8.38"], [], lambda t: t == "v0.8.37", lambda p: ["services/a.rs"], lambda p: [], None,
                    on_main=lambda t: t == "v0.8.37", channel="beta", date="20261105")
        self.assertEqual((r["tag"], r["stable_tag"]), ("v0.8.39-beta.20261105", "v0.8.39"))


NOW = datetime(2026, 11, 6, 6, 20, tzinfo=timezone.utc)


def beta(tag, hours=30, attested=True, draft=False, commit=None):
    return {"tag": tag, "commit": commit or ("b" * 40), "published_at": NOW - timedelta(hours=hours) if hours is not None else None,
            "draft": draft, "attested": attested}


def promote(betas, tags=None, stables=("v0.8.37",), blockers=(), version=None):
    names = list(tags if tags is not None else list(stables) + [b["tag"] for b in betas])
    return ar.plan_promote(betas, names, list(stables), NOW, None if blockers is None else list(blockers), version)


class Promote(unittest.TestCase):
    def test_the_beta_out_for_a_day_becomes_stable(self):
        r = promote([beta("v0.8.38-beta.20261105", hours=25)])
        self.assertEqual((r["skip"], r["tag"], r["beta"]), (False, "v0.8.38", "v0.8.38-beta.20261105"))

    def test_too_young_waits(self):
        r = promote([beta("v0.8.38-beta.20261105", hours=23)])
        self.assertTrue(r["skip"])
        self.assertIn("out for 23h of the 24h", r["reason"])

    def test_the_newest_ready_beta_wins_and_a_young_newer_one_stays_beta(self):
        r = promote([beta("v0.8.38-beta.20261104", hours=50), beta("v0.8.39-beta.20261105", hours=25), beta("v0.8.40-beta.20261106", hours=1)])
        self.assertEqual((r["tag"], r["beta"]), ("v0.8.39", "v0.8.39-beta.20261105"))
        self.assertIn("v0.8.40-beta.20261106: out for 1h", r["reason"])

    def test_a_reported_problem_holds_it(self):
        b = [beta("v0.8.38-beta.20261104", hours=50), beta("v0.8.39-beta.20261105", hours=25)]
        for text in ("Update to v0.8.39-beta.20261105 rolled back", "v0.8.39 fails to apply"):
            r = promote(b, blockers=[{"number": 12, "text": text}])
            self.assertEqual(r["tag"], "v0.8.38", "the older clean beta instead")
        r = promote(b, blockers=[{"number": 12, "text": "v0.8.390 is unrelated, and v0.8.3"}])
        self.assertEqual(r["tag"], "v0.8.39")
        r = promote(b, blockers=None)
        self.assertTrue(r["skip"])
        self.assertIn("couldn't be read", r["reason"])

    def test_never_behind_a_stable_release(self):
        # An urgent v0.8.40 went out: the older betas can't become stable any more.
        r = promote([beta("v0.8.38-beta.20261104", hours=50), beta("v0.8.39-beta.20261105", hours=25)], stables=("v0.8.37", "v0.8.40"))
        self.assertTrue(r["skip"])
        self.assertIn("v0.8.40 is out", r["reason"])
        r = promote([beta("v0.8.38-beta.20261105")], stables=("v0.8.37", "v0.8.38"))
        self.assertIn("v0.8.38 exists already", r["reason"])
        # A stable name taken off main also stops it.
        r = promote([beta("v0.8.38-beta.20261105")], tags=["v0.8.37", "v0.8.38", "v0.8.38-beta.20261105"])
        self.assertIn("v0.8.38 exists already", r["reason"])

    def test_needs_the_signed_statement_and_a_published_release(self):
        self.assertIn("no signed statement", promote([beta("v0.8.38-beta.20261105", attested=False)])["reason"])
        self.assertIn("isn't published", promote([beta("v0.8.38-beta.20261105", draft=True)])["reason"])
        self.assertIn("isn't published", promote([beta("v0.8.38-beta.20261105", hours=None)])["reason"])
        self.assertIn("none on main", promote([])["reason"])

    def test_by_hand(self):
        b = [beta("v0.8.38-beta.20261104", hours=50), beta("v0.8.39-beta.20261105", hours=1)]
        for v in ("v0.8.39-beta.20261105", "v0.8.39"):
            r = promote(b, version=v)
            self.assertEqual((r["skip"], r["tag"]), (False, "v0.8.39"), "an urgent fix doesn't wait the day")
        r = promote(b, version="v0.8.38")
        self.assertEqual(r["tag"], "v0.8.38")
        r = promote(b, version="v0.8.41")
        self.assertTrue(r["skip"] and r["error"])
        r = promote(b, version="v0.8.39", blockers=[{"number": 3, "text": "v0.8.39"}])
        self.assertTrue(r["error"])
        self.assertIn("issue #3", r["reason"])


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


class Wiring(unittest.TestCase):
    """deploy.yml releases its own commit, in its own run, and auto-release.yml refuses any other."""

    @staticmethod
    def load(name):
        import yaml
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "workflows", name)) as f:
            text = f.read()
        return text, yaml.safe_load(text)

    def test_deploy_releases_its_own_commit_only_when_asked(self):
        _, deploy = self.load("deploy.yml")
        job = deploy["jobs"]["release"]
        self.assertEqual(job["uses"], "./.github/workflows/auto-release.yml")
        self.assertEqual(job["with"]["commit"], "${{ github.sha }}")
        self.assertIn("deploy", job["needs"])
        self.assertEqual(job["permissions"]["id-token"], "write")
        # The scheduled deploys release nothing; a deploy by hand with "stable release" does.
        self.assertIn("github.event_name == 'workflow_dispatch' && inputs.stable_release", job["if"])
        on = deploy.get("on", deploy.get(True))
        self.assertEqual(on["workflow_dispatch"]["inputs"]["stable_release"]["default"], False)
        # A called workflow's jobs get no more than the calling job grants.
        _, auto = self.load("auto-release.yml")
        for name, j in auto["jobs"].items():
            for perm, level in (j.get("permissions") or {}).items():
                self.assertIn(perm, job["permissions"], "%s.%s" % (name, perm))
                if level == "write":
                    self.assertEqual(job["permissions"][perm], "write", "%s.%s" % (name, perm))

    def test_auto_release_runs_only_on_the_released_commit(self):
        text, doc = self.load("auto-release.yml")
        on = doc.get("on", doc.get(True))
        self.assertEqual(set(on), {"workflow_call", "workflow_dispatch", "schedule"})
        guard = next(s for s in doc["jobs"]["plan"]["steps"] if s.get("id") == "guard")["run"]
        tag = doc["jobs"]["tag"]["steps"][0]["run"]
        for run in (guard, tag):
            self.assertIn('"$GITHUB_SHA" != "$COMMIT"', run)
            self.assertIn("refs/heads/main", run)
            self.assertIn("deploy.yml@refs/heads/main", run)
        # The run's commit differing from the asked commit is an error, never a quiet release of another commit.
        self.assertIn("exit 1", guard.split('"$GITHUB_SHA" != "$COMMIT"')[1].split("fi")[0])
        self.assertIn("exit 1", tag.split('"$GITHUB_SHA" != "$COMMIT"')[1].split("fi")[0])
        self.assertIn("--certificate-github-workflow-sha \"$COMMIT\"", text)
        # A promotion signs nothing: it checks the beta's statement for the stable tag; only new releases sign.
        sign = next(s for s in doc["jobs"]["tag"]["steps"] if s.get("name") == "Sign the statements")
        self.assertEqual(sign["if"], "matrix.kind != 'promote'")
        self.assertIn("CODERHELM STABLE ATTESTATION", tag)
        self.assertIn("compare/$COMMIT...main", tag)
        # The promotion runs hours after the nightly beta, so last night's beta is a full day old by then.
        crons = [s["cron"] for s in on["schedule"]]
        self.assertEqual(crons, ["17 6 * * *", "17 10 * * *"])
        self.assertIn('[ "$SCHEDULE" = "17 10 * * *" ]; then mode=promote', guard)

    def test_the_stable_block_never_reads_as_the_release_attestation(self):
        # Installs find the release attestation by exact lines; a beta's second block must not match them.
        self.assertNotEqual(ar.STABLE_BEGIN, "-----BEGIN CODERHELM RELEASE ATTESTATION-----")
        text, _ = self.load("auto-release.yml")
        self.assertIn(ar.STABLE_BEGIN, text)
        self.assertIn(ar.STABLE_END, text)


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
            # Tonight's beta of that commit, with its stable statement in the tag message.
            code = git("rev-parse", "HEAD")
            r = ar.git_plan("o/r", code, "beta", "20261105")
            self.assertEqual((r["tag"], r["stable_tag"]), ("v0.7.0-beta.20261105", "v0.7.0"))
            git("tag", "-a", "v0.7.0-beta.20261105", "-m", "v0.7.0-beta.20261105\n\n%s\nAAAA\n%s" % (ar.STABLE_BEGIN, ar.STABLE_END))
            git("tag", "-a", "v0.6.1-beta.20261101", "-m", "from before promotion", "HEAD~1")
            self.assertTrue(ar.git_plan("o/r", code, "beta", "20261106")["skip"], "main didn't change since tonight's beta")
            real_release, real_blockers = ar._release, ar._blockers
            published = {"v0.7.0-beta.20261105": "2026-11-05T06:40:00Z", "v0.6.1-beta.20261101": "2026-11-01T06:40:00Z"}
            ar._release = lambda repo, tag: {"draft": False, "published_at": published[tag]}
            ar._blockers = lambda repo: []
            try:
                now = ar.datetime(2026, 11, 6, 7, 0, tzinfo=ar.timezone.utc)
                r = ar.git_promote("o/r", None, now)
                self.assertEqual((r["skip"], r["tag"], r["beta"], r["commit"], r["previous"]), (False, "v0.7.0", "v0.7.0-beta.20261105", code, "v0.6.0"))
                r = ar.git_promote("o/r", None, now - ar.timedelta(hours=2))
                self.assertTrue(r["skip"])
                self.assertIn("v0.6.1-beta.20261101: its tag carries no signed statement", r["reason"])
            finally:
                ar._release, ar._blockers = real_release, real_blockers
        finally:
            os.chdir(old)
            ar._merged_prs = pulls


if __name__ == "__main__":
    unittest.main()
