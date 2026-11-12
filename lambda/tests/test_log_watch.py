"""The log analyzer's watch: a new error spike is analyzed within minutes.

Run: python3 -m unittest discover -s lambda/tests
"""

import calendar
import time
import unittest

from test_log_analyzer import h


def ts(s):
    """Epoch seconds of a UTC 'YYYY-MM-DD HH:MM:SS'."""
    return calendar.timegm(time.strptime(s, "%Y-%m-%d %H:%M:%S"))


# The real incident's shape, per minute (UTC): the API's usual trickle of
# error lines, then a domain swap at 05:19 that failed it for seven minutes
# (timeouts and 503s, then DNS failures on the deleted domain's hostname,
# then 404s for an index the new domain didn't have yet).
API = "/ecs/prod-ledger-api"
WEB = "/ecs/prod-storefront"
WORKER = "/ecs/prod-ledger-api-worker"


def lines_at(minute):
    """Error lines written in the minute starting at `minute` (epoch)."""
    t = time.strftime("%H:%M", time.gmtime(minute))
    usual = {API: 4, WEB: 3, WORKER: 1}
    if "05:19" <= t <= "05:26":
        return {API: 370, WEB: 60, WORKER: 1}
    return usual


class FakeLogs:
    """Counts what a Logs Insights count over [start, end] would return."""

    def count(self, cw_logs, groups, start, end):
        out = {}
        m = start - start % 60
        while m < end:
            for g, n in lines_at(m).items():
                if g in groups:
                    out[g] = out.get(g, 0) + n
            m += 60
        return out


def conn():
    return {"team_id": "TEAM#t", "account_id": "111122223333", "log_groups": [API, WEB, WORKER],
            "role_arn": "arn:aws:iam::111122223333:role/r", "external_id": "x", "region": "us-east-1"}


class Pure(unittest.TestCase):
    def test_a_spike_is_many_lines_and_many_times_the_usual(self):
        base = {API: 20, WEB: 15}
        self.assertEqual(h.spiking({API: 2000, WEB: 40}, base), [API])
        # The usual noise of a busy group is not a spike.
        self.assertEqual(h.spiking({API: 90}, base), [])
        # A quiet group's first burst is.
        self.assertEqual(h.spiking({WORKER: 60}, base), [WORKER])

    def test_the_usual_count_follows_slowly(self):
        b = h.next_baseline({API: 2000}, {API: 20})
        self.assertLess(b[API], 500)
        b = h.next_baseline({}, {API: 20})
        self.assertEqual(b[API], 16.0)

    def test_analyses_are_rationed(self):
        now = ts("2026-11-12 05:25:00")
        self.assertTrue(h.watch_due({}, now))
        self.assertFalse(h.watch_due({"last_analysis": now - 600}, now))
        full = {"day": "2026-11-12", "analyses": h.WATCH_MAX_ANALYSES_PER_DAY}
        self.assertFalse(h.watch_due(full, now))
        # A new day starts the count again.
        self.assertTrue(h.watch_due({"day": "2026-11-11", "analyses": 99}, now))


class Replay(unittest.TestCase):
    """The incident replayed through the watch and through the old
    schedule: when would a fix have been looked at?"""

    def run_watch(self, start, end):
        fake = FakeLogs()
        state = {}
        events = []
        old_groups = h.log_groups_for
        h.log_groups_for = lambda c, cw: list(c["log_groups"])
        self.addCleanup(setattr, h, "log_groups_for", old_groups)

        def analyze(c, lookback_hours=None, only_groups=None, resolve=True):
            events.append(("analyze", tick, tuple(only_groups), lookback_hours, resolve))
            return 2

        def act():
            events.append(("worker", tick))

        tick = start
        while tick <= end:
            h.watch_connection(
                conn(), now=tick, logs=object(), count=fake.count, analyze=analyze,
                state_io=(lambda t, a: state, lambda t, a, s: state.update(s)), act=act,
            )
            tick += h.WATCH_MINUTES * 60
        return events

    def test_the_incident_is_analyzed_within_minutes_and_the_worker_acts_at_once(self):
        # Watch ticks every 5 minutes, from well before the incident (so the
        # usual counts are learned) to an hour after it.
        events = self.run_watch(ts("2026-11-12 03:00:00"), ts("2026-11-12 06:30:00"))
        analyses = [e for e in events if e[0] == "analyze"]
        self.assertEqual(len(analyses), 1, events)
        _, at, groups, lookback, resolve = analyses[0]
        began = ts("2026-11-12 05:19:00")
        delay = at - began
        # Seen at the first watch whose window covers the spike.
        self.assertLessEqual(delay, 7 * 60, f"{delay / 60:.0f} minutes")
        self.assertEqual(groups, (API, WEB))
        self.assertEqual((lookback, resolve), (h.WATCH_LOOKBACK_HOURS, False))
        # The worker is asked to act in the same run.
        self.assertIn(("worker", at), events)
        # The usual trickle never triggered an analysis.
        self.assertTrue(all(e[1] >= began for e in analyses))

    def test_the_old_schedule_took_five_hours(self):
        # Before: a 6-hourly analysis (it ran at 03:26 and 09:26 that day)
        # and an hourly automatic-fix sweep (at :19).
        began = ts("2026-11-12 05:19:00")
        analysis = next(t for t in range(ts("2026-11-12 03:26:48"), began + 86400, 6 * 3600) if t > began)
        sweep = next(t for t in range(ts("2026-11-12 00:19:20"), began + 86400, 3600) if t > analysis)
        self.assertEqual(time.strftime("%H:%M", time.gmtime(sweep)), "10:19")
        self.assertGreaterEqual(sweep - began, 5 * 3600)
        # After: the watch's run plus the worker's immediate sweep.
        events = self.run_watch(ts("2026-11-12 03:00:00"), ts("2026-11-12 06:30:00"))
        seen = next(e[1] for e in events if e[0] == "worker")
        self.assertLess(seen - began, (sweep - began) / 40)


class Handler(unittest.TestCase):
    def test_the_watch_event_runs_the_watch_not_the_full_analysis(self):
        calls = []
        old_watch, old_scan = h.watch, h.scan_aws_connections
        try:
            h.watch = lambda now=None: calls.append("watch") or {"statusCode": 200}
            h.scan_aws_connections = lambda: calls.append("full") or []
            h.handler({"mode": "watch"}, None)
            h.handler({}, None)
        finally:
            h.watch, h.scan_aws_connections = old_watch, old_scan
        self.assertEqual(calls, ["watch", "full"])

    def test_the_worker_is_asked_on_its_queue(self):
        sent = []
        old = h.TICKET_QUEUE_URL
        try:
            h.TICKET_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/1/q"
            self.assertTrue(h.ask_worker_to_act(send=sent.append))
            h.TICKET_QUEUE_URL = ""
            self.assertFalse(h.ask_worker_to_act(send=sent.append))
        finally:
            h.TICKET_QUEUE_URL = old
        self.assertEqual(sent, ['{"type": "log_autofix"}'])


if __name__ == "__main__":
    unittest.main()
