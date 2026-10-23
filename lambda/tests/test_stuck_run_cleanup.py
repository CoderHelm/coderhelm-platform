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


if __name__ == "__main__":
    unittest.main()
