"""Finding attribution: log group, counts and times come from Insights data,
the model only writes words.

Run: python3 -m unittest discover -s lambda/tests
"""

import json
import time
import unittest
from datetime import datetime, timezone

from test_log_analyzer import h

A = h.attribution
A.SLEEP = lambda _s: None

ACCOUNT = "111122223333"
WORKER = "/ecs/prod-orbit-api-worker"
WEB = "/ecs/prod-orbit-web"
JOBS = "/aws/lambda/orbit-jobs"
GROUPS = [WORKER, WEB, JOBS]
NOW = int(time.time())
ENOSPC = "Error: ENOSPC: no space left on device, write"
CRASH = "Essential container in task exited: exit code 137 (OutOfMemoryError: Container killed)"


def fmt(ts):
    """Insights' own timestamp format."""
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def web_events():
    """44,534 ENOSPC lines in the web group only, over two streams and many hashes."""
    out, left, i = [], 44534, 0
    while left:
        n = min(left, 5000)
        stream = f"web/web/{'a1' if i % 2 else 'b2'}"
        msg = f"Failed to update prerender cache for {i:08x}deadbeef {ENOSPC}"
        out.append((WEB, stream, NOW - 20 * 3600 + i * 600, msg, n))
        left -= n
        i += 1
    return out


def worker_events():
    return [
        (WORKER, "worker/api/c3", NOW - 3 * 3600, CRASH, 7),
        (WORKER, "worker/api/d4", NOW - 2 * 3600, CRASH, 5),
        (WORKER, "worker/api/d4", NOW - 2 * 3600 + 5, "Worker started, listening on queue", 40),
    ]


class FakeInsights:
    """A CloudWatch Logs client over in-memory events
    (group, stream, epoch secs, message, how many identical lines).
    Discovery queries keep lines that look like errors; stats queries match
    their `like "..."` literal. Rows come back the way Insights sends them."""

    def __init__(self, events, fail_on=()):
        self.events = events
        self.fail_on = fail_on
        self.queries = []
        self.results = {}

    def start_query(self, logGroupNames, startTime, endTime, queryString):
        self.queries.append((tuple(logGroupNames), queryString, startTime, endTime))
        qid = str(len(self.queries))
        if any(f in queryString for f in self.fail_on):
            self.results[qid] = {"status": "Failed"}
            return {"queryId": qid}
        groups = set(logGroupNames)
        if 'like "' in queryString:
            literal = queryString.split('like "', 1)[1].split('"', 1)[0]
            keep = lambda m: literal in m  # noqa: E731
            by = lambda e: (e[0], e[1])  # noqa: E731
        else:
            keep = lambda m: "Error" in m or "exit code" in m  # noqa: E731
            by = lambda e: (e[0], e[1], e[3])  # noqa: E731
        agg = {}
        for e in self.events:
            group, stream, ts, msg, n = e
            if group not in groups or not keep(msg) or not (startTime <= ts <= endTime):
                continue
            a = agg.setdefault(by(e), {"n": 0, "first": ts, "last": ts, "sample": msg, "e": e})
            a["n"] += n
            a["first"] = min(a["first"], ts)
            if ts >= a["last"]:
                a["last"], a["sample"] = ts, msg
        rows = []
        for key, a in sorted(agg.items(), key=lambda kv: -kv[1]["n"]):
            row = [
                {"field": "@log", "value": f"{ACCOUNT}:{key[0]}"},
                {"field": "@logStream", "value": key[1]},
                {"field": "n", "value": str(a["n"])},
                {"field": "first_ts", "value": fmt(a["first"])},
                {"field": "last_ts", "value": fmt(a["last"])},
            ]
            if len(key) == 3:
                row.append({"field": "@message", "value": key[2]})
            else:
                row.append({"field": "sample", "value": a["sample"]})
            rows.append(row)
        self.results[qid] = {"status": "Complete", "results": rows}
        return {"queryId": qid}

    def get_query_results(self, queryId):
        return self.results[queryId]

    def stop_query(self, queryId):
        pass


class Table:
    def __init__(self):
        self.puts, self.updates = [], []

    def put_item(self, Item):
        self.puts.append(Item)

    def update_item(self, **kw):
        self.updates.append(kw)


class Patch:
    """Swap handler globals for one test."""

    def __init__(self, **kw):
        self.kw, self.saved = kw, {}

    def __enter__(self):
        for k, v in self.kw.items():
            self.saved[k] = getattr(h, k)
            setattr(h, k, v)
        return self

    def __exit__(self, *exc):
        for k, v in self.saved.items():
            setattr(h, k, v)


def analysis_reply(evidence_ids_for):
    """What the analysis model returns: the incident's wrong-service title,
    citing the worker's evidence for an error that never happened there."""
    return json.dumps([
        {
            "title": "Prerender cache writes failing with ENOSPC (disk full) in orbit-api",
            "severity": "critical",
            "summary": "The orbit-api worker cannot write its prerender cache: the disk is full.",
            "suggested_action": "Free disk space on the orbit-api worker tasks.",
            "evidence_ids": evidence_ids_for(CRASH),
            "error_pattern": ENOSPC,
            "suspected_location": None,
            "existing_id": None,
        },
        {
            "title": "orbit-api worker crash-looping on out-of-memory kills",
            "severity": "critical",
            "summary": "prod-orbit-api-worker tasks exit with code 137 after running out of memory.",
            "suggested_action": "Raise the task memory or find the leak.",
            "evidence_ids": evidence_ids_for(CRASH),
            "error_pattern": "exit code 137",
            "existing_id": None,
        },
    ])


def run_incident(guard_reply):
    """analyze_connection end to end on fake Insights and a fake model."""
    cw = FakeInsights(web_events() + worker_events())
    table = Table()
    prompts = []

    def ask(prompt, _key, _team=None):
        prompts.append(prompt)
        if prompt.startswith("You check log findings"):
            return guard_reply(prompt)
        evidence = json.loads(prompt.split("<evidence>\n", 1)[1].split("\n</evidence>", 1)[0])
        return analysis_reply(lambda text: [e["id"] for e in evidence if text in e["message"]])

    with Patch(
        get_team_api_key=lambda _t: "key",
        logs_client_for=lambda _c: cw,
        list_pending_recommendations=lambda *_a: [],
        record_connection_success=lambda *_a: None,
        resolve_stale_recommendations=lambda *_a: None,
        notify_new_recommendations=lambda *_a: None,
        ask_model=ask,
        aws_insights_table=table,
    ):
        new = h.analyze_connection({
            "team_id": "TEAM#t", "account_id": ACCOUNT, "role_arn": "r", "external_id": "x",
            "region": "us-east-1", "log_groups": GROUPS,
        })
    return new, table, cw, prompts


def rewrite_named(prompt):
    items = json.loads(prompt.split("<items>\n", 1)[1].split("\n</items>", 1)[0])
    out = []
    for it in items:
        ev = it["evidence"]
        wrong = "orbit-api" in it["title"] and ev["log_group"] != WORKER
        out.append({
            "id": it["id"],
            "supported": not wrong,
            "title": f"Prerender cache writes failing with ENOSPC in {ev['log_group']}" if wrong else it["title"],
            "summary": f"{ev['matching_lines']} lines in {ev['log_group']} report ENOSPC." if wrong else it["summary"],
        })
    return json.dumps(out)


class TheIncident(unittest.TestCase):
    """A pattern only in the web group, a crash only in the worker group."""

    def setUp(self):
        self.new, self.table, self.cw, self.prompts = run_incident(rewrite_named)
        self.by_group = {}
        for item in self.table.puts:
            self.by_group.setdefault(item["source_log_group"], []).append(item)

    def test_every_query_returns_and_groups_by_log_and_stream(self):
        self.assertTrue(self.cw.queries)
        for _groups, q, _s, _e in self.cw.queries:
            self.assertIn("@log, @logStream", q.split("| stats", 1)[0])
            self.assertIn("by @log, @logStream", q)

    def test_the_enospc_finding_is_credited_to_the_web_group_only(self):
        enospc = [i for i in self.table.puts if "ENOSPC" in i["error_pattern"]]
        self.assertEqual([i["source_log_group"] for i in enospc], [WEB])
        self.assertNotIn(ENOSPC, json.dumps(self.by_group.get(WORKER, [])))

    def test_counts_and_times_are_the_events(self):
        (item,) = [i for i in self.table.puts if "ENOSPC" in i["error_pattern"]]
        events = web_events()
        self.assertEqual(item["seen_count"], 44534)
        self.assertEqual(item["error_count"], 44534)
        self.assertEqual(item["count_basis"], "events")
        first = A.parse_ts(fmt(min(e[2] for e in events)))
        last = A.parse_ts(fmt(max(e[2] for e in events)))
        self.assertEqual(A.parse_ts(item["first_seen_at"]), first)
        self.assertEqual(A.parse_ts(item["last_seen_at"]), last)
        self.assertNotEqual(item["first_seen_at"], item["created_at"])

    def test_samples_carry_their_own_group_and_stream(self):
        (item,) = [i for i in self.table.puts if "ENOSPC" in i["error_pattern"]]
        self.assertTrue(item["samples"])
        for s in item["samples"]:
            self.assertEqual(s["log_group"], WEB)
            self.assertTrue(s["log_stream"].startswith("web/"))
            self.assertIn(ENOSPC, s["message"])

    def test_the_title_no_longer_names_the_api_service(self):
        (item,) = [i for i in self.table.puts if "ENOSPC" in i["error_pattern"]]
        self.assertNotIn("orbit-api", item["title"] + item["summary"])
        self.assertIn(WEB, item["title"])

    def test_the_crash_finding_stays_on_the_worker(self):
        (crash,) = self.by_group[WORKER]
        self.assertEqual(crash["error_pattern"], "exit code 137")
        self.assertEqual(crash["seen_count"], 12)
        self.assertEqual(crash["samples"][0]["log_stream"], "worker/api/d4")
        self.assertEqual(self.new, 2)

    def test_the_model_never_names_the_source(self):
        analysis = [p for p in self.prompts if not p.startswith("You check log findings")][0]
        self.assertIn('"evidence_ids"', analysis)
        self.assertNotIn('"source_log_group": The primary', analysis)


class OneErrorManyGroups(unittest.TestCase):
    def test_one_finding_per_group_with_its_own_count(self):
        events = [
            (WEB, "s1", NOW - 100, f"x {ENOSPC}", 30),
            (JOBS, "s9", NOW - 50, f"y {ENOSPC}", 4),
        ]
        cw = FakeInsights(events)
        evidence = A.build_evidence([{
            "query_name": "general_errors", "log_groups": GROUPS,
            "results": h.run_insights_query(cw, GROUPS, h.INSIGHTS_QUERIES[3]["query"], NOW - 3600, NOW),
        }])
        rec = {"title": "Disk full", "error_pattern": ENOSPC, "evidence_ids": [evidence[0]["id"]]}
        cands, failed = h.attribute_recommendations(cw, [rec], evidence, NOW - 3600, NOW)
        self.assertFalse(failed)
        self.assertEqual([(c["log_group"], c["stats"]["count"]) for c in cands], [(WEB, 30), (JOBS, 4)])
        self.assertEqual(cands[0]["others"], [{"log_group": JOBS, "lines": 4}])


class CheckBeforeSaving(unittest.TestCase):
    def stats(self, **counts):
        return {
            g: {"account": ACCOUNT, "log_group": g, "count": n, "first": None, "last": None,
                "streams": {}, "samples": []}
            for g, n in counts.items()
        }

    def test_an_empty_claimed_group_passes_the_credit_to_the_one_that_matches(self):
        # The wide query claims the worker; the worker alone has no such line.
        cw = FakeInsights([(WEB, "s", NOW - 10, ENOSPC, 3)])
        credited, failed = A.credited_groups(
            self.stats(**{WORKER: 50, WEB: 3}), cw, ENOSPC, NOW - 3600, NOW
        )
        self.assertFalse(failed)
        self.assertEqual([(g["log_group"], g["count"]) for g in credited], [(WEB, 3)])

    def test_no_group_matches_means_no_finding(self):
        cw = FakeInsights([])
        credited, failed = A.credited_groups(self.stats(**{WORKER: 5}), cw, ENOSPC, NOW - 3600, NOW)
        self.assertEqual((credited, failed), ([], False))
        rec = {"title": "t", "error_pattern": ENOSPC, "evidence_ids": []}
        ev = [{"id": "e1", "log_group": WORKER, "message": f"a {ENOSPC}", "scope": [WORKER]}]
        cands, _ = h.attribute_recommendations(cw, [rec], ev, NOW - 3600, NOW)
        self.assertEqual(cands, [])

    def test_a_failed_check_is_never_read_as_zero(self):
        cw = FakeInsights([(WEB, "s", NOW - 10, ENOSPC, 3)], fail_on=("like",))
        credited, failed = A.credited_groups(self.stats(**{WEB: 3}), cw, ENOSPC, NOW - 3600, NOW)
        self.assertEqual((credited, failed), ([], True))

    def test_each_check_runs_on_one_group_alone(self):
        cw = FakeInsights([(WEB, "s", NOW - 10, ENOSPC, 3)])
        A.credited_groups(self.stats(**{WORKER: 9, WEB: 3}), cw, ENOSPC, NOW - 3600, NOW)
        self.assertEqual([q[0] for q in cw.queries], [(WORKER,), (WEB,)])

    def test_the_literal_must_occur_in_the_evidence(self):
        ev = [
            {"id": "e1", "log_group": WEB, "message": f"cache abc {ENOSPC}", "scope": [WEB]},
            {"id": "e2", "log_group": WORKER, "message": CRASH, "scope": [WORKER]},
        ]
        # Cited the worker, but the text is only in the web line: the web line backs it.
        lit, cited = A.choose_literal({"error_pattern": ENOSPC, "evidence_ids": ["e2"]}, ev)
        self.assertEqual((lit, [e["id"] for e in cited]), (ENOSPC, ["e1"]))
        # Invented text: the cited line's own words.
        lit, cited = A.choose_literal({"error_pattern": "made up words", "evidence_ids": ["e2"]}, ev)
        self.assertEqual((lit, [e["id"] for e in cited]), (CRASH[:80], ["e2"]))
        self.assertEqual(A.choose_literal({"error_pattern": "made up words"}, ev), (None, []))


class CountsAndTimes(unittest.TestCase):
    T = staticmethod(A.parse_ts)

    def stats(self, count, first, last):
        return {"count": count, "first": self.T(first), "last": self.T(last), "samples": []}

    def test_a_new_finding_counts_its_lines(self):
        seen, first, last = A.merge_seen(None, self.stats(9, "2026-10-25 01:00:00", "2026-10-25 02:00:00"), None)
        self.assertEqual((seen, first, last), (9, self.T("2026-10-25 01:00:00"), self.T("2026-10-25 02:00:00")))

    def test_seen_again_adds_only_new_lines_and_first_seen_never_moves_forward(self):
        prev = {"count_basis": "events", "seen_count": 100,
                "first_seen_at": "2026-10-24T00:00:00+00:00", "last_seen_at": "2026-10-25T01:30:00+00:00"}
        seen, first, last = A.merge_seen(prev, self.stats(40, "2026-10-25 01:00:00", "2026-10-25 03:00:00"), 15)
        self.assertEqual(seen, 115)
        self.assertEqual(first, self.T("2026-10-24T00:00:00+00:00"))
        self.assertEqual(last, self.T("2026-10-25 03:00:00"))

    def test_last_seen_never_moves_back(self):
        prev = {"count_basis": "events", "seen_count": 5,
                "first_seen_at": "2026-10-25T00:00:00+00:00", "last_seen_at": "2026-10-25T09:00:00+00:00"}
        _, _, last = A.merge_seen(prev, self.stats(1, "2026-10-25 01:00:00", "2026-10-25 02:00:00"), 0)
        self.assertEqual(last, self.T("2026-10-25T09:00:00+00:00"))

    def test_a_gap_since_last_seen_counts_the_whole_window(self):
        prev = {"count_basis": "events", "seen_count": 5,
                "first_seen_at": "2026-10-20T00:00:00+00:00", "last_seen_at": "2026-10-20T01:00:00+00:00"}
        seen, _, _ = A.merge_seen(prev, self.stats(7, "2026-10-25 01:00:00", "2026-10-25 02:00:00"), None)
        self.assertEqual(seen, 12)

    def test_an_old_item_counted_in_runs_starts_over_from_events(self):
        # Before this change seen_count was runs and created_at the run time.
        prev = {"seen_count": 4, "created_at": "2026-10-23T06:00:00+00:00", "last_seen_at": "2026-10-25T06:00:00+00:00"}
        seen, first, last = A.merge_seen(prev, self.stats(44534, "2026-10-25 01:00:00", "2026-10-25 05:00:00"), 3)
        self.assertEqual(seen, 44534)
        self.assertEqual(first, self.T("2026-10-23T06:00:00+00:00"))
        self.assertEqual(last, self.T("2026-10-25 05:00:00"))

    def test_a_refresh_counts_lines_after_the_stored_last_seen_on_that_group(self):
        last_seen = NOW - 3600
        cw = FakeInsights([
            (WEB, "s", last_seen, ENOSPC, 10),        # already counted
            (WEB, "s", last_seen + 60, ENOSPC, 4),    # new
            (WORKER, "s", last_seen + 60, ENOSPC, 99),  # another group: never counted here
        ])
        prev = {"rec_id": "r1", "sk": "REC#r1", "source_log_group": WEB, "count_basis": "events",
                "seen_count": 10, "first_seen_at": A.iso(A.parse_ts(last_seen)),
                "last_seen_at": A.iso(A.parse_ts(last_seen)), "error_hash": "h"}
        cand = {"log_group": WEB, "account": ACCOUNT, "literal": ENOSPC, "existing_id": "r1",
                "stats": {"count": 14, "first": A.parse_ts(last_seen), "last": A.parse_ts(last_seen + 60), "samples": []}}
        with Patch(aws_insights_table=Table()) as p:
            rec_id, new = h.store_finding("TEAM#t", ACCOUNT, cand, [prev], cw, (NOW - 6 * 3600, NOW))
            vals = p.kw["aws_insights_table"].updates[0]["ExpressionAttributeValues"]
        self.assertEqual((rec_id, new), ("r1", False))
        self.assertEqual(vals[":seen"], 14)
        self.assertEqual(vals[":c"], 14)
        self.assertEqual(cw.queries[0][0], (WEB,))

    def test_an_open_finding_in_another_group_is_never_reused(self):
        prev = {"rec_id": "r1", "sk": "REC#r1", "source_log_group": WORKER, "error_hash": "h"}
        cand = {"log_group": WEB, "account": ACCOUNT, "literal": ENOSPC, "existing_id": "r1",
                "stats": {"count": 3, "first": None, "last": None, "samples": []}}
        with Patch(aws_insights_table=Table()) as p:
            rec_id, new = h.store_finding("TEAM#t", ACCOUNT, cand, [prev], None, (NOW - 3600, NOW))
            t = p.kw["aws_insights_table"]
        self.assertTrue(new)
        self.assertEqual((len(t.puts), len(t.updates)), (1, 0))
        self.assertEqual(t.puts[0]["source_log_group"], WEB)


class TitleGuard(unittest.TestCase):
    def cand(self, title, summary, group=WEB):
        return {"id": "c1", "title": title, "summary": summary, "log_group": group, "literal": ENOSPC,
                "stats": {"count": 44534, "first": A.parse_ts("2026-10-25 01:00:00"),
                          "last": A.parse_ts("2026-10-25 05:00:00"), "samples": [{"message": ENOSPC}]}}

    def test_prefilter_lets_through_only_text_that_names_its_own_group_alone(self):
        self.assertFalse(A.needs_guard(self.cand("ENOSPC in prod-orbit-web", "Disk full."), GROUPS))
        self.assertTrue(A.needs_guard(self.cand("ENOSPC in orbit-api", "Disk full."), GROUPS))
        self.assertTrue(A.needs_guard(
            self.cand("ENOSPC in prod-orbit-web", "Also hits prod-orbit-api-worker."), GROUPS))

    def test_the_model_rewrites_unsupported_text_from_the_evidence(self):
        c = self.cand("Prerender cache failing in orbit-api", "The orbit-api worker disk is full.")
        n = A.guard_texts([c], GROUPS, rewrite_named)
        self.assertEqual(n, 1)
        self.assertIn(WEB, c["title"])
        self.assertNotIn("orbit-api", c["summary"])

    def test_supported_text_is_kept(self):
        c = self.cand("Prerender cache writes failing", "The web app's disk is full.")
        reply = json.dumps([{"id": "c1", "supported": True, "title": "x", "summary": "y"}])
        self.assertEqual(A.guard_texts([c], GROUPS, lambda _p: reply), 0)
        self.assertEqual(c["title"], "Prerender cache writes failing")

    def test_no_verdict_means_the_evidence_words(self):
        c = self.cand("Prerender cache failing in orbit-api", "The orbit-api worker disk is full.")
        A.guard_texts([c], GROUPS, lambda _p: None)
        self.assertEqual(c["title"], f"{ENOSPC} in prod-orbit-web")
        self.assertIn("44534 log lines", c["summary"])
        self.assertIn(WEB, c["summary"])

    def test_a_rewrite_that_still_names_another_group_is_replaced(self):
        c = self.cand("Prerender cache failing in orbit-api", "x")
        reply = json.dumps([{"id": "c1", "supported": False,
                             "title": "Disk full on prod-orbit-api-worker", "summary": "y"}])
        A.guard_texts([c], GROUPS, lambda _p: reply)
        self.assertEqual(c["title"], f"{ENOSPC} in prod-orbit-web")

    def test_no_model_call_when_every_text_is_plain(self):
        calls = []
        c = self.cand("ENOSPC in prod-orbit-web", "Disk full in /ecs/prod-orbit-web.")
        A.guard_texts([c], GROUPS, lambda p: calls.append(p))
        self.assertEqual(calls, [])


class Rows(unittest.TestCase):
    def test_at_log_splits_into_account_and_group(self):
        self.assertEqual(A.split_log(f"{ACCOUNT}:{WEB}"), (ACCOUNT, WEB))
        self.assertEqual(A.split_log(WEB), ("", WEB))
        self.assertEqual(A.split_log("", (WEB,)), ("", WEB))
        self.assertEqual(A.split_log("", (WEB, JOBS)), ("", ""))

    def test_timestamps_parse_in_every_shape(self):
        want = A.parse_ts("2026-10-25T01:02:03.004+00:00")
        self.assertEqual(A.parse_ts("2026-10-25 01:02:03.004"), want)
        self.assertEqual(A.parse_ts(str(int(want.timestamp() * 1000))), want)
        self.assertIsNone(A.parse_ts("soon"))

    def test_rows_without_a_log_group_are_not_evidence(self):
        ev = A.build_evidence([{"log_groups": [WEB, JOBS], "results": [{"@message": "Error", "n": "3"}]}])
        self.assertEqual(ev, [])



class RepairExisting(unittest.TestCase):
    """repair_attribution: the one-time re-check of stored findings."""

    @classmethod
    def setUpClass(cls):
        import repair_attribution
        repair_attribution.handler = h  # the handler the tests already loaded
        cls.R = repair_attribution

    class Store(Table):
        def __init__(self, items):
            super().__init__()
            self.items = items

        def query(self, **_kw):
            return {"Items": [dict(i) for i in self.items]}

    def incident_item(self, **kw):
        return {"pk": "TEAM#t", "sk": "REC#r1", "status": "pending", "source_account_id": ACCOUNT,
                "source_log_group": WORKER, "error_pattern": ENOSPC, "seen_count": 1,
                "title": "Prerender cache writes failing with ENOSPC (disk full) in orbit-api",
                "summary": "The orbit-api worker disk is full.",
                "created_at": A.iso(A.parse_ts(NOW - 600)), "last_seen_at": A.iso(A.parse_ts(NOW - 600)), **kw}

    def repair(self, items, apply):
        cw = FakeInsights(web_events() + worker_events())
        table = self.Store(items)
        rows = []
        conn = {"team_id": "TEAM#t", "account_id": ACCOUNT, "role_arn": "r", "external_id": "x",
                "region": "us-east-1", "log_groups": GROUPS}
        with Patch(aws_insights_table=table, logs_client_for=lambda _c: cw):
            tally = self.R.repair_connection(conn, 24, False, apply, rows.append)
        return tally, rows, table

    def test_the_dry_run_finds_the_wrong_credit_and_writes_nothing(self):
        tally, rows, table = self.repair([self.incident_item()], apply=False)
        self.assertEqual(tally, {"recredit": 1})
        (row,) = rows
        self.assertEqual((row["claimed"], row["to"], row["lines"]), (WORKER, WEB, 44534))
        self.assertEqual(row["lines_by_group"], {WEB: 44534})
        self.assertNotIn("orbit-api", row["new_title"])
        self.assertEqual((table.puts, table.updates), ([], []))

    def test_apply_moves_it_with_the_lines_counts_and_times(self):
        _, _, table = self.repair([self.incident_item()], apply=True)
        (u,) = table.updates
        v = u["ExpressionAttributeValues"]
        self.assertEqual((v[":g"], v[":was"], v[":seen"], v[":c"]), (WEB, WORKER, 44534, 44534))
        self.assertEqual(A.parse_ts(v[":f"]), A.parse_ts(fmt(min(e[2] for e in web_events()))))
        self.assertNotEqual(v[":h"], h.error_hash_for(ACCOUNT, {"source_log_group": WORKER, "error_pattern": ENOSPC}))
        self.assertIn("source_log_group = :g", u["UpdateExpression"])

    def test_a_finding_being_fixed_is_flagged_not_moved(self):
        tally, _, table = self.repair([self.incident_item(fix_ticket_id="LOG-1")], apply=True)
        self.assertEqual(tally, {"flag:wrong_group": 1})
        (u,) = table.updates
        self.assertEqual(u["ExpressionAttributeValues"][":f"], "wrong_group")
        self.assertNotIn("source_log_group", u["UpdateExpression"])

    def test_a_right_credit_gets_its_lines_and_a_dead_pattern_is_flagged(self):
        crash = self.incident_item(sk="REC#r2", error_pattern="exit code 137", title="Crash loop")
        gone = self.incident_item(sk="REC#r3", error_pattern="never logged anywhere")
        closed = self.incident_item(sk="REC#r4", status="dismissed")
        tally, rows, table = self.repair([crash, gone, closed], apply=True)
        self.assertEqual(tally, {"ok": 1, "flag:unverified": 1})
        ok = [u for u in table.updates if u["Key"]["sk"] == "REC#r2"][0]["ExpressionAttributeValues"]
        self.assertEqual((ok[":seen"], ok[":basis"]), (12, "events"))
        self.assertNotIn(":g", ok)


if __name__ == "__main__":
    unittest.main()
