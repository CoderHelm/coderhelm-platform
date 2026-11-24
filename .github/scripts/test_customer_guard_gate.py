"""Tests for customer_guard_gate.py, against a fake GitHub API."""

import contextlib
import io
import os
import pathlib
import sys
import unittest
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import customer_guard_gate as g  # noqa: E402


def check(conclusion, status="completed", app="coderhelm", started="2026-01-01T00:00:00Z", id=1):
    return {"name": g.CHECK, "app": {"slug": app}, "status": status,
            "conclusion": conclusion, "started_at": started, "id": id}


class Fake:
    """GitHub, answering from a list of check-run listings (one per poll)."""

    def __init__(self, polls, live=True):
        self.polls, self.live, self.calls = list(polls), live, 0

    def __call__(self, path):
        if "/actions/workflows/deploy.yml/runs" in path:
            return {"workflow_runs": [{"head_sha": "d3p10y"}]}
        if "/contents/" in path:
            if self.live:
                return {}
            raise g.NotFound()
        self.calls += 1
        runs = self.polls.pop(0) if len(self.polls) > 1 else self.polls[0]
        return {"check_runs": runs}


class Clock:
    def __init__(self):
        self.t = 0.0

    def now(self):
        return self.t

    def sleep(self, s):
        self.t += s


def gate(polls, live=True, wait=300):
    c = Clock()
    return g.run("o/r", "abc", get=Fake(polls, live), sleep=c.sleep, now=c.now, wait=wait)


class Gate(unittest.TestCase):
    def test_passing_and_skipped_pass(self):
        self.assertEqual(gate([[check("success")]]), "pass")
        self.assertEqual(gate([[check("neutral")]]), "pass")

    def test_failing_fails(self):
        self.assertEqual(gate([[check("failure")]]), "fail")

    def test_missing_fails_after_the_wait(self):
        self.assertEqual(gate([[]]), "fail")

    def test_it_waits_for_a_running_guard(self):
        polls = [[], [check(None, status="in_progress")], [check("success")]]
        self.assertEqual(gate(polls), "pass")

    def test_the_newest_run_counts(self):
        runs = [check("failure", id=1), check("success", started="2026-01-01T00:05:00Z", id=2)]
        self.assertEqual(gate([runs]), "pass")
        runs = [check("success", id=1), check("failure", started="2026-01-01T00:05:00Z", id=2)]
        self.assertEqual(gate([runs]), "fail")

    def test_only_the_guards_own_app_counts(self):
        self.assertEqual(gate([[check("success", app="github-actions")]]), "fail")
        both = [check("success", app="github-actions", started="2026-01-02T00:00:00Z"), check("failure")]
        self.assertEqual(gate([both]), "fail")

    def test_before_the_guard_is_deployed_it_passes(self):
        self.assertEqual(gate([[]], live=False), "pass")

    def test_a_failed_read_never_passes(self):
        def broken(path):
            raise OSError("down")
        c = Clock()
        self.assertTrue(g.guard_live(broken))
        self.assertEqual(g.run("o/r", "abc", get=broken, sleep=c.sleep, now=c.now, wait=60), "fail")

    def test_it_prints_only_pass_or_fail(self):
        out = io.StringIO()
        env = {"GITHUB_REPOSITORY": "o/r", "HEAD_SHA": "abc"}
        with mock.patch.dict(os.environ, env), mock.patch.object(g, "run", return_value="fail"), \
                contextlib.redirect_stdout(out):
            code = g.main()
        self.assertEqual((code, out.getvalue()), (1, "fail\n"))
        out = io.StringIO()
        with mock.patch.dict(os.environ, env), mock.patch.object(g, "run", side_effect=RuntimeError("x")), \
                contextlib.redirect_stdout(out):
            code = g.main()
        self.assertEqual((code, out.getvalue()), (1, "fail\n"))


if __name__ == "__main__":
    unittest.main()
