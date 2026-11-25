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

    def __init__(self, polls):
        self.polls = list(polls)

    def __call__(self, path):
        runs = self.polls.pop(0) if len(self.polls) > 1 else self.polls[0]
        if isinstance(runs, Exception):
            raise runs
        return {"check_runs": runs}


class Clock:
    def __init__(self):
        self.t = 0.0

    def now(self):
        return self.t

    def sleep(self, s):
        self.t += s


EV = {"pull_request": {"head": {"sha": "abc"}}}


def gate(polls, scan=lambda e: [], wait=300):
    c = Clock()
    return g.run("o/r", "abc", EV, get=Fake(polls), sleep=c.sleep, now=c.now, wait=wait, scan=scan)


def never(e):
    raise AssertionError("the fallback runs only when the guard is missing")


class Gate(unittest.TestCase):
    def test_a_present_guard_decides_and_the_fallback_never_runs(self):
        self.assertEqual(gate([[check("success")]], scan=never), (True, g.PRESENT_PASS[1], []))
        self.assertEqual(gate([[check("neutral")]], scan=never)[:2], g.PRESENT_SKIP)
        self.assertEqual(gate([[check("failure")]], scan=never)[:2], g.PRESENT_FAIL)

    def test_a_failing_guard_is_not_overridden_by_a_clean_fallback(self):
        passed, why, _ = gate([[check("failure")]], scan=lambda e: [])
        self.assertFalse(passed)
        self.assertIn("reported a customer detail", why)

    def test_it_waits_for_a_running_guard(self):
        polls = [[], [check(None, status="in_progress")], [check("success")]]
        self.assertTrue(gate(polls, scan=never)[0])

    def test_the_newest_run_counts(self):
        runs = [check("failure", id=1), check("success", started="2026-01-01T00:05:00Z", id=2)]
        self.assertTrue(gate([runs], scan=never)[0])
        runs = [check("success", id=1), check("failure", started="2026-01-01T00:05:00Z", id=2)]
        self.assertFalse(gate([runs], scan=never)[0])

    def test_only_the_guards_own_app_counts(self):
        # A same-named check from another app is ignored: missing, so the fallback runs.
        passed, why, _ = gate([[check("success", app="github-actions")]], scan=lambda e: ["x"])
        self.assertFalse(passed)
        self.assertIn("no Customer details guard check found after 5 min", why)
        both = [check("success", app="github-actions", started="2026-01-02T00:00:00Z"), check("failure")]
        self.assertFalse(gate([both], scan=never)[0])

    def test_missing_falls_back_to_cis_own_rules_and_says_so(self):
        passed, why, lines = gate([[]], scan=lambda e: [])
        self.assertTrue(passed)
        self.assertIn("no Customer details guard check found after 5 min", why)
        self.assertIn("none found", why)
        passed, why, lines = gate([[]], scan=lambda e: ["::error title=Public text guard::PR body line 2: email address"])
        self.assertFalse(passed)
        self.assertIn("1 line(s) carry an id, key or email", why)
        self.assertEqual(len(lines), 1)

    def test_a_guard_stuck_running_or_unreadable_counts_as_missing(self):
        self.assertIn("after 5 min", gate([[check(None, status="queued")]])[1])
        self.assertIn("after 5 min", gate([OSError("down")])[1])

    def test_a_fallback_that_cannot_read_fails(self):
        def broken(e):
            raise OSError("no git")
        passed, why, _ = gate([[]], scan=broken)
        self.assertFalse(passed)
        self.assertIn("could not read", why)

    def test_the_fallback_uses_the_public_text_guard(self):
        with mock.patch("text_guard.sources_from_pr", return_value=[("PR body", None, [(1, "mail a@zorbmail.io")])]):
            passed, why, lines = g.fallback(EV)
        self.assertFalse(passed)
        self.assertTrue(any("email address" in line for line in lines))
        self.assertFalse(any("zorbmail" in line for line in lines + [why]))

    def test_it_prints_why_and_never_text(self):
        out = io.StringIO()
        env = {"GITHUB_REPOSITORY": "o/r", "HEAD_SHA": "abc"}
        with mock.patch.dict(os.environ, env), mock.patch.object(g, "event", return_value=EV), \
                mock.patch.object(g, "run", return_value=g.PRESENT_FAIL + ([],)), contextlib.redirect_stdout(out):
            code = g.main()
        self.assertEqual((code, out.getvalue()), (1, g.PRESENT_FAIL[1] + "\n"))
        out = io.StringIO()
        with mock.patch.dict(os.environ, env), mock.patch.object(g, "event", side_effect=OSError("x")), \
                contextlib.redirect_stdout(out):
            code = g.main()
        self.assertEqual(code, 1)
        self.assertTrue(out.getvalue().startswith("fail: "))


if __name__ == "__main__":
    unittest.main()
