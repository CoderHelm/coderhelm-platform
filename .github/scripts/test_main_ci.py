"""Tests for main_ci.py, against a fake GitHub API, and its wiring into CI,
the hosted deploy and the self-host releases."""

import contextlib
import io
import os
import pathlib
import sys
import unittest
from unittest import mock

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import main_ci as m  # noqa: E402

SHA = "c0ffee" + "0" * 34


def ci_run(conclusion, status="completed", event="push", branch="main", sha=SHA, number=1, path=m.WORKFLOW):
    return {"path": path, "head_sha": sha, "event": event, "head_branch": branch,
            "status": status, "conclusion": conclusion, "run_number": number, "id": number}


class Fake:
    """GitHub, answering from a list of run listings (one per poll)."""

    def __init__(self, polls):
        self.polls, self.paths = list(polls), []

    def __call__(self, path):
        self.paths.append(path)
        runs = self.polls.pop(0) if len(self.polls) > 1 else self.polls[0]
        if isinstance(runs, Exception):
            raise runs
        return {"workflow_runs": runs}


class Clock:
    def __init__(self):
        self.t = 0.0

    def now(self):
        return self.t

    def sleep(self, s):
        self.t += s


def verdict(polls, wait=0):
    c = Clock()
    with contextlib.redirect_stderr(io.StringIO()):
        return m.run("o/r", SHA, get=Fake(polls), sleep=c.sleep, now=c.now, wait=wait)


class Verdict(unittest.TestCase):
    def test_a_passing_push_run_on_main_is_green(self):
        self.assertEqual(verdict([[ci_run("success")]]), "green")

    def test_any_other_ending_is_red(self):
        for c in ("failure", "cancelled", "timed_out", "action_required", None):
            self.assertEqual(verdict([[ci_run(c)]]), "red", c)

    def test_the_newest_run_decides(self):
        # A run on the same commit started again later (a second push run).
        self.assertEqual(verdict([[ci_run("success", number=1), ci_run("failure", number=2)]]), "red")
        self.assertEqual(verdict([[ci_run("failure", number=1), ci_run("success", number=2)]]), "green")

    def test_only_cis_push_run_on_main_for_this_commit_counts(self):
        # A pull request's run tests its head against an older base; another
        # workflow, branch or commit says nothing about this commit of main.
        for other in (ci_run("success", event="pull_request"), ci_run("success", event="merge_group"),
                      ci_run("success", branch="feature"), ci_run("success", sha="f" * 40),
                      ci_run("success", path=".github/workflows/deploy.yml")):
            self.assertEqual(verdict([[other]]), "pending", other)

    def test_a_running_ci_is_waited_for(self):
        running = [ci_run(None, status="in_progress")]
        self.assertEqual(verdict([running, running, [ci_run("success")]], wait=600), "green")
        self.assertEqual(verdict([running, [ci_run("failure")]], wait=600), "red")

    def test_no_verdict_in_time_or_an_unreadable_github_is_never_green(self):
        self.assertEqual(verdict([[ci_run(None, status="queued")]], wait=120), "pending")
        self.assertEqual(verdict([[]], wait=0), "pending")
        self.assertEqual(verdict([RuntimeError("500")], wait=60), "pending")
        self.assertEqual(verdict([RuntimeError("500"), [ci_run("success")]], wait=60), "green")

    def test_the_listing_asks_for_push_runs_on_main_for_the_commit(self):
        fake = Fake([[ci_run("success")]])
        m.run("o/r", SHA, get=fake)
        self.assertIn("/repos/o/r/actions/workflows/ci.yml/runs?", fake.paths[0])
        for q in ("branch=main", "event=push", "head_sha=" + SHA):
            self.assertIn(q, fake.paths[0])

    def test_main_exits_zero_only_on_green(self):
        for result, code in (("green", 0), ("red", 1), ("pending", 1)):
            with mock.patch.object(m, "run", return_value=result), \
                    mock.patch.dict(os.environ, {"GITHUB_REPOSITORY": "o/r"}), \
                    contextlib.redirect_stdout(io.StringIO()) as out:
                self.assertEqual(m.main(["main_ci.py", SHA, "--wait", "5"]), code)
            self.assertTrue(out.getvalue().startswith(result))
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(m.main(["main_ci.py", "abc"]), 2)


class Wiring(unittest.TestCase):
    """CI runs on the merged result, and nothing red on main ships."""

    @staticmethod
    def load(name):
        import yaml
        with open(HERE.parent / "workflows" / name) as f:
            text = f.read()
        doc = yaml.safe_load(text)
        return text, doc, doc.get("on", doc.get(True))

    def test_ci_runs_on_main_and_in_the_merge_queue(self):
        _, doc, on = self.load("ci.yml")
        self.assertEqual(on["pull_request"]["branches"], ["main"])
        self.assertEqual(on["push"]["branches"], ["main"])
        self.assertIn("merge_group", on)
        # A run on main is never cancelled by the next push: a cancelled run
        # is red, and every commit of main gets its own verdict.
        cc = doc["concurrency"]
        self.assertIn("github.sha", cc["group"])
        self.assertIn("github.event_name", cc["group"])
        self.assertEqual(cc["cancel-in-progress"], "${{ github.event_name == 'pull_request' }}")
        # The Rust jobs never skip on main: a squash commit has no second parent.
        for job in ("rust-lint", "rust-test"):
            step = next(s for s in doc["jobs"][job]["steps"] if s.get("id") == "changes")
            self.assertIn("HEAD^2", step["run"])
            self.assertIn("run=true", step["run"].split("else")[1])

    def test_the_deploy_gate_ships_only_a_commit_ci_passed_on(self):
        _, doc, _ = self.load("deploy.yml")
        gate = doc["jobs"]["gate"]
        step = next(s for s in gate["steps"] if s.get("name") == "CI passed on this commit of main")
        self.assertIn('main_ci.py "$COMMIT" --wait', step["run"])
        self.assertEqual(step["env"]["COMMIT"], "${{ github.sha }}")
        self.assertEqual(step["if"], "steps.g.outputs.changed == 'true'")
        # It fails the gate (no continue-on-error), and every later job needs the gate.
        self.assertNotIn("continue-on-error", step)
        self.assertGreater(gate["timeout-minutes"], 60)
        self.assertIn("gate", doc["jobs"]["build"]["needs"])
        self.assertIn("gate", doc["jobs"]["deploy"]["needs"])
        self.assertIn("deploy", doc["jobs"]["release"]["needs"])
        for job in ("build", "deploy", "release"):
            self.assertNotIn("always()", doc["jobs"][job]["if"])
        # A run by hand still needs CI: only the batch check is skipped.
        self.assertNotIn("workflow_dispatch", step["if"])

    def test_a_release_ships_only_a_commit_ci_passed_on(self):
        _, doc, _ = self.load("auto-release.yml")
        steps = doc["jobs"]["plan"]["steps"]
        ids = [s.get("id") for s in steps]
        self.assertLess(ids.index("ci"), ids.index("plan"))
        ci = steps[ids.index("ci")]
        self.assertEqual(ci["if"], "steps.guard.outputs.commit != ''")
        self.assertIn('main_ci.py "$COMMIT"', ci["run"])
        # Tonight's beta is skipped; any other new release fails.
        self.assertIn("exit 1", ci["run"].split("fi")[-1])
        plan = steps[ids.index("plan")]
        self.assertEqual(plan["env"]["CI_RED"], "${{ steps.ci.outputs.red }}")
        self.assertIn('[ "$CI_RED" = true ]; then echo "count=0"', plan["run"])


if __name__ == "__main__":
    unittest.main()
