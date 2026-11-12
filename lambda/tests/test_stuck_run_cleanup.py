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


class OverCeiling(unittest.TestCase):
    def test_under_every_ceiling_is_left_alone(self):
        self.assertIsNone(sweep.over_ceiling({}))
        self.assertIsNone(
            sweep.over_ceiling(
                {
                    "ceil_passes": sweep.MAX_RUN_PASSES,
                    "ceil_continuations": sweep.MAX_RUN_CONTINUATIONS,
                    "ceil_worked_secs": sweep.MAX_RUN_WORKED_SECS,
                    "ceil_spend_usd": sweep.MAX_RUN_SPEND_USD,
                }
            )
        )

    def test_each_ceiling_has_its_own_code(self):
        for field, value, code in [
            ("ceil_passes", sweep.MAX_RUN_PASSES + 1, "ceiling_passes"),
            ("ceil_continuations", sweep.MAX_RUN_CONTINUATIONS + 1, "ceiling_continuations"),
            ("ceil_worked_secs", sweep.MAX_RUN_WORKED_SECS + 1, "ceiling_worked_time"),
            ("ceil_spend_usd", sweep.MAX_RUN_SPEND_USD + 1, "ceiling_spend"),
        ]:
            self.assertEqual(sweep.over_ceiling({field: value})[0], code)

    def test_the_reason_notes_the_pull_request_or_the_branch(self):
        with_pr = sweep.over_ceiling_reason("the limit", {"pr_url": "https://x/1", "pr_number": 1})
        self.assertIn("https://x/1", with_pr)
        no_pr = sweep.over_ceiling_reason("the limit", {}, "coderhelm/x")
        self.assertIn("`coderhelm/x`", no_pr)
        self.assertIn("Retry", no_pr)

    def test_the_handoff_waits_on_a_person_and_keeps_steps_from_restarting(self):
        update, values = sweep.over_ceiling_update("R1", "ceiling_spend", "why", "now")
        self.assertEqual(values[":new"], "needs_input")
        self.assertEqual(values[":sri"], "needs_input#R1")
        self.assertIs(values[":yes"], True)
        self.assertIn("guard_open = :yes", update)
        self.assertIn("loop_stop_reason = :code", update)

    def test_a_run_over_it_is_ended_whatever_its_last_update_says(self):
        import datetime as dt

        now = dt.datetime.now(dt.timezone.utc).isoformat()
        run = {
            "team_id": "T",
            "run_id": "R",
            "status": "running",
            "updated_at": now,
            "ceil_spend_usd": sweep.MAX_RUN_SPEND_USD + 1,
        }
        updates = []

        class Table:
            def scan(self, **kw):
                return {"Items": [run]}

            def update_item(self, **kw):
                updates.append(kw)

        class Db:
            class meta:
                class client:
                    class exceptions:
                        ConditionalCheckFailedException = KeyError

            def Table(self, _name):
                return Table()

        sweep._dynamodb = Db()
        try:
            out = sweep.handler({}, None)
        finally:
            sweep._dynamodb = None
        self.assertEqual(out["over_ceiling"], 1)
        self.assertEqual(len(updates), 1)
        self.assertEqual(updates[0]["ExpressionAttributeValues"][":new"], "needs_input")


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


class WaitingOnAPerson(unittest.TestCase):
    """A needs_input run never waits for nothing (services/common/src/run_wait.rs)."""

    NOW = sweep.datetime(2026, 11, 5, 15, 0, tzinfo=sweep.timezone.utc)

    def _sweep(self, runs):
        updates, sent = [], []

        class Table:
            def scan(self, **kw):
                return {"Items": runs}

            def update_item(self, **kw):
                updates.append(kw)

        class Db:
            class meta:
                class client:
                    class exceptions:
                        ConditionalCheckFailedException = KeyError

            def Table(self, _name):
                return Table()

        class Sqs:
            def send_message(self, **kw):
                sent.append(sweep.json.loads(kw["MessageBody"]))

        sweep._dynamodb, sweep._sqs = Db(), Sqs()
        old_url, sweep.TICKET_QUEUE_URL = sweep.TICKET_QUEUE_URL, "https://sqs/q"
        try:
            out = sweep.handler({}, None)
        finally:
            sweep._dynamodb, sweep._sqs, sweep.TICKET_QUEUE_URL = None, None, old_url
        return out, updates, sent

    def test_a_stalled_run_with_its_pull_request_is_found_and_handed_to_review(self):
        # It did its work, opened a draft pull request with a check's finding
        # as its open item, and waited with no question for three days.
        run = {
            "team_id": "T",
            "run_id": "R",
            "status": "needs_input",
            "updated_at": "2026-11-02T10:00:00+00:00",
            "pr_number": 7,
            "pr_url": "https://github.com/o/r/pull/7",
            "error_message": "The new-feature rules blocked the pull request: new items aren't registered",
            "questions": '{"questions":[],"asked_at":""}',
        }
        self.assertFalse(sweep.asks_something(run))
        out, updates, sent = self._sweep([run])
        self.assertEqual(out["waiting_named"], 1)
        self.assertEqual(out["stuck_count"], 0)
        self.assertEqual(len(updates), 1)
        wait = sweep.json.loads(updates[0]["ExpressionAttributeValues"][":w"])
        self.assertEqual(wait["action"], "Review pull request #7")
        self.assertIn("new items aren't registered", wait["detail"])
        self.assertEqual(wait["url"], "https://github.com/o/r/pull/7")
        self.assertEqual(updates[0]["ConditionExpression"], "#s = :ni")
        self.assertEqual(sent, [{"type": "run_waiting", "team_id": "T", "run_id": "R", "why": "named"}])
        # Never failed or completed: still waiting, now for something named.
        self.assertNotIn(":new", updates[0]["ExpressionAttributeValues"])

    def test_a_passed_question_deadline_goes_ahead_with_the_default_once(self):
        run = {
            "team_id": "T",
            "run_id": "R",
            "status": "needs_input",
            "updated_at": self.NOW.isoformat(),
            "questions": '{"questions":[{"id":"q1","question":"Monthly or yearly?"}]}',
            "ask_default": "Monthly",
            "ask_deadline": (self.NOW - sweep.timedelta(hours=1)).isoformat(),
        }
        self.assertTrue(sweep.deadline_passed(run, self.NOW))
        out, updates, sent = self._sweep([run])
        self.assertEqual(out["waiting_defaulted"], 1)
        self.assertIn("attribute_not_exists(ask_default_taken_at)", updates[0]["ConditionExpression"])
        self.assertEqual(sent, [{"type": "run_waiting", "team_id": "T", "run_id": "R", "why": "deadline"}])
        # Before the deadline, or already taken, or with no default: nothing.
        early = dict(run, ask_deadline=(self.NOW + sweep.timedelta(hours=1)).isoformat())
        self.assertFalse(sweep.deadline_passed(early, self.NOW))
        self.assertFalse(sweep.deadline_passed(dict(run, ask_default=""), self.NOW))
        out, _, sent = self._sweep([dict(run, ask_default_taken_at=self.NOW.isoformat())])
        self.assertEqual(out["waiting_defaulted"], 0)
        self.assertNotIn("deadline", [m["why"] for m in sent])

    def test_without_a_pull_request_it_waits_for_a_retry(self):
        wait = sweep.named_wait({"error_message": "stopped"}, "now")
        self.assertIn("Retry", wait["action"])
        self.assertEqual(wait["detail"], "stopped")

    # Runs that stopped before waits were recorded wrote why only in their
    # progress notes (an open pull request already existed) or in their
    # outside-change report (unsure where the fix belongs).
    NOTES = [
        {"message": "Plan generated", "timestamp": "2026-10-26T14:25:12+00:00"},
        {
            "message": "An open pull request for ABC-1 already exists in o/r: #9. I didn't open another.",
            "timestamp": "2026-10-26T14:27:59+00:00",
        },
        {
            "message": "This run was waiting with nothing asked; it now waits for: Read why",
            "timestamp": "2026-11-06T16:41:19+00:00",
        },
        {"message": "Reminder: still waiting after 24 hours for: Read why", "timestamp": "2026-11-07T16:51:18+00:00"},
    ]
    REPORT = (
        "**Not sure yet where the fix belongs**\n\n"
        "**What I found:** The question's style sets no font.\n\n"
        "**What I checked**\n- [code] read styles.ts"
    )

    def test_the_reason_is_read_from_the_last_stop_note(self):
        wait = sweep.named_wait({"progress_notes": self.NOTES}, "now")
        self.assertEqual(
            wait["detail"], "An open pull request for ABC-1 already exists in o/r: #9. I didn't open another."
        )
        # The outside-change report comes before the notes; error_message first of all.
        run = {"progress_notes": self.NOTES, "outside_change_report": self.REPORT}
        self.assertEqual(
            sweep.named_wait(run, "now")["detail"],
            "Not sure yet where the fix belongs: The question's style sets no font.",
        )
        self.assertEqual(sweep.named_wait(dict(run, error_message="stopped"), "now")["detail"], "stopped")
        # A routine step note says where it stopped, never that it is a reason.
        steps = [{"message": "Triaged as simple complexity"}, {"message": "Plan generated"}] + self.NOTES[2:]
        self.assertEqual(
            sweep.named_wait({"progress_notes": steps}, "now")["detail"],
            "It stopped right after its last step (Plan generated) without saying why.",
        )
        # Only the sweep's own notes: nothing said.
        self.assertEqual(sweep.named_wait({"progress_notes": self.NOTES[2:]}, "now")["detail"], sweep.NOTHING_SAID)
        self.assertEqual(sweep.named_wait({}, "now")["detail"], sweep.NOTHING_SAID)
        long = sweep.named_wait({"progress_notes": [{"message": "x " * 400}]}, "now")["detail"]
        self.assertLessEqual(len(long), sweep.REASON_CHARS)

    def test_a_wait_named_without_a_reason_gets_it_once_without_a_notice(self):
        wait = sweep.json.dumps(
            {"action": "Read why CoderHelm stopped this run", "detail": sweep.NOTHING_SAID, "since": "2026-11-11T15:00:00+00:00"}
        )
        run = {"team_id": "T", "run_id": "R", "status": "needs_input", "waiting_on": wait, "progress_notes": self.NOTES}
        out, updates, sent = self._sweep([run])
        self.assertEqual(out["waiting_explained"], 1)
        written = sweep.json.loads(updates[0]["ExpressionAttributeValues"][":w"])
        self.assertIn("already exists", written["detail"])
        self.assertEqual(written["since"], "2026-11-11T15:00:00+00:00")
        self.assertEqual(sent, [])
        # Once filled in (or with nothing to say), it is left alone.
        explained = dict(run, waiting_on=sweep.json.dumps(written))
        self.assertFalse(sweep.said_nothing(explained))
        self.assertFalse(sweep.said_nothing(dict(run, progress_notes=[])))

    def test_one_reminder_after_the_wait(self):
        wait = sweep.json.dumps({"action": "Review pull request #7", "since": "2026-11-04T16:00:00+00:00"})
        run = {"team_id": "T", "run_id": "R", "status": "needs_input", "waiting_on": wait}
        self.assertTrue(sweep.asks_something(run))
        self.assertFalse(sweep.reminder_due(run, self.NOW))
        later = self.NOW + sweep.timedelta(hours=2)
        self.assertTrue(sweep.reminder_due(run, later))
        reminded = dict(run, wait_reminded_at="2026-11-05T16:30:00+00:00")
        self.assertFalse(sweep.reminder_due(reminded, later + sweep.timedelta(days=3)))

        old = dict(run, waiting_on=sweep.json.dumps({"action": "Answer", "since": "2026-11-01T00:00:00+00:00"}))
        out, updates, sent = self._sweep([old])
        self.assertEqual(out["waiting_reminded"], 1)
        self.assertIn("attribute_not_exists(#r)", updates[0]["ConditionExpression"])
        self.assertEqual(sent[0]["why"], "reminder")

    def test_a_run_with_questions_and_a_fresh_wait_is_left_alone(self):
        run = {
            "team_id": "T",
            "run_id": "R",
            "status": "needs_input",
            "updated_at": self.NOW.isoformat(),
            "questions": '{"questions":[{"id":"q1","question":"Which one?"}]}',
        }
        self.assertTrue(sweep.asks_something(run))
        self.assertFalse(sweep.reminder_due(run, self.NOW))

    def test_the_message_is_one_the_worker_reads(self):
        contract = sweep.json.loads(
            (HANDLER.parents[2] / "services/common/contract/worker_messages.json").read_text()
        )
        sample = contract["scheduled"]["run_waiting"]
        msg = sweep.waiting_message({"team_id": "T", "run_id": "R"}, "reminder")
        self.assertEqual(set(msg), set(sample))
        self.assertEqual(msg["type"], sample["type"])

    def test_over_its_ceiling_it_waits_for_something_named(self):
        _, values = sweep.over_ceiling_update("R1", "ceiling_spend", "why", "now")
        self.assertIn("Retry", sweep.json.loads(values[":wo"])["action"])
