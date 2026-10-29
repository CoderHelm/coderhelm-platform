"""Unit tests for the stuck-run sweep's pure helpers.

Run: python3 -m unittest discover -s lambda/tests
"""

import importlib.util
import pathlib
import unittest

HANDLER = pathlib.Path(__file__).resolve().parents[1] / "stuck-run-cleanup" / "handler.py"
_spec = importlib.util.spec_from_file_location("stuck_run_cleanup_handler", HANDLER)
sweep = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(sweep)


class LastKnownPass(unittest.TestCase):
    def test_current_pass_wins(self):
        self.assertEqual(sweep.last_known_pass({"current_pass": "implement"}), "implement")

    def test_cleared_pass_falls_back_to_history(self):
        run = {
            "current_pass": "",
            "pass_history": [{"pass": "plan"}, {"pass": "implement"}],
        }
        self.assertEqual(sweep.last_known_pass(run), "implement")

    def test_a_state_is_not_a_pass(self):
        run = {"current_pass": "awaiting_ci", "pass_history": [{"pass": "pr"}]}
        self.assertEqual(sweep.last_known_pass(run), "pr")

    def test_checkpoint_then_trace(self):
        self.assertEqual(sweep.last_known_pass({}, "implement_started", ""), "implement")
        self.assertEqual(sweep.last_known_pass({}, "", "security"), "security")

    def test_nothing_known(self):
        self.assertEqual(sweep.last_known_pass({}), "")


class Message(unittest.TestCase):
    def test_never_unknown_pass(self):
        self.assertNotIn("unknown", sweep.failure_message(""))
        self.assertIn("before its first pass", sweep.failure_message(""))
        self.assertEqual(
            sweep.failure_message("plan"),
            "Run timed out during plan pass (auto-cleanup)",
        )
        self.assertEqual(sweep.BEFORE_FIRST_PASS, "before its first pass")


class Swept(unittest.TestCase):
    def test_only_stuck_working_runs_are_swept(self):
        self.assertEqual(sweep.SWEPT_STATUSES, ("running", "awaiting_ci"))
        for status in ("closed", "cancelled", "completed", "failed", "needs_input"):
            self.assertNotIn(status, sweep.SWEPT_STATUSES)

    def test_closed_is_a_state_not_a_pass(self):
        self.assertIn("closed", sweep.NOT_A_PASS)
        run = {"current_pass": "closed", "pass_history": [{"pass": "implement"}]}
        self.assertEqual(sweep.last_known_pass(run), "implement")


class Rounds(unittest.TestCase):
    NOW = sweep.datetime(2026, 10, 26, 15, 0, tzinfo=sweep.timezone.utc)

    def test_a_round_inside_its_own_budget_is_not_swept(self):
        # The run is days old; its round started a few minutes ago.
        run = {
            "created_at": "2026-10-20T09:00:00+00:00",
            "round": {"state": "running", "deadline_at": "2026-10-26T15:05:00+00:00"},
        }
        self.assertTrue(sweep.round_in_budget(run, self.NOW))
        # Past the deadline and the grace period, it is.
        late = self.NOW + sweep.timedelta(seconds=300 + sweep.ROUND_SWEEP_GRACE_SECONDS + 1)
        self.assertFalse(sweep.round_in_budget(run, late))

    def test_a_round_without_a_deadline_uses_its_start_and_the_budget(self):
        run = {"round": {"state": "running", "started_at": "2026-10-26T14:55:00+00:00"}}
        self.assertTrue(sweep.round_in_budget(run, self.NOW))

    def test_a_parked_round_or_no_round_has_no_budget_to_wait_for(self):
        self.assertFalse(sweep.round_in_budget({}, self.NOW))
        parked = {"round": {"state": "pending", "deadline_at": "2026-10-26T16:00:00+00:00"}}
        self.assertFalse(sweep.round_in_budget(parked, self.NOW))

    def test_a_stuck_run_with_an_open_pr_is_not_failed(self):
        run = {
            "pr_number": 41,
            "current_pass": "implement",
            "round": {
                "kind": "review_fix",
                "number": 3,
                "feedback": "Add the tabs",
                "wip_branch": "coderhelm-wip/feature/x",
                "base_sha": "abc",
            },
        }
        self.assertTrue(sweep.has_pull_request(run))
        rec = sweep.incomplete_record(run, "implement", "2026-10-26T15:00:00+00:00")
        self.assertEqual(rec["kind"], "review_fix")
        self.assertEqual(rec["number"], 3)
        self.assertEqual(rec["feedback"], "Add the tabs")
        self.assertEqual(rec["wip_branch"], "coderhelm-wip/feature/x")
        self.assertEqual(rec["base_sha"], "abc")
        line = sweep.status_line(rec["reason"])
        self.assertTrue(line.startswith("Pull request open; the last fix round didn't finish:"))
        self.assertNotIn("failed", line.lower())

    def test_the_round_kind_falls_back_to_the_pass(self):
        self.assertEqual(sweep.round_kind({}, "ci_fix:2"), "ci_fix")
        self.assertEqual(sweep.round_kind({}, "feedback"), "feedback")
        self.assertEqual(sweep.round_kind({}, "implement"), "review_fix")
        self.assertEqual(sweep.round_kind({}, "security_fix:1"), "security_fix")

    def test_no_pr_means_no_pr(self):
        self.assertFalse(sweep.has_pull_request({}))
        self.assertFalse(sweep.has_pull_request({"pr_number": 0, "pr_url": ""}))
        self.assertTrue(sweep.has_pull_request({"pr_url": "https://example.com/pull/1"}))


if __name__ == "__main__":
    unittest.main()


class StepQueued(unittest.TestCase):
    NOW = sweep.datetime(2026, 10, 28, 12, 0, tzinfo=sweep.timezone.utc)

    def at(self, minutes):
        return (self.NOW - sweep.timedelta(minutes=minutes)).isoformat()

    def test_a_queued_step_not_started_is_not_stuck(self):
        run = {"updated_at": self.at(40), "step_queued_at": self.at(39)}
        self.assertTrue(sweep.step_queued(run, self.NOW))

    def test_a_started_step_that_died_is_swept(self):
        run = {"updated_at": self.at(30), "step_queued_at": self.at(45)}
        self.assertFalse(sweep.step_queued(run, self.NOW))

    def test_a_step_queued_too_long_is_swept(self):
        run = {"updated_at": self.at(60 * 8), "step_queued_at": self.at(60 * 7)}
        self.assertFalse(sweep.step_queued(run, self.NOW))

    def test_no_step_queued(self):
        self.assertFalse(sweep.step_queued({"updated_at": self.at(30)}, self.NOW))
