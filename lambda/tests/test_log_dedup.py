"""Finding identity and lifecycle: one finding per error, whatever the model
calls it, through resolve, reappear and ignore; credited to the groups and
streams its lines are in.

Run: python3 -m unittest discover -s lambda/tests
"""

import json
import unittest
from datetime import datetime, timedelta, timezone

from test_log_analyzer import h
from test_log_attribution import ACCOUNT, NOW, FakeInsights, Patch, Table

import merge_duplicates as M

A, F, L = h.attribution, h.fingerprint, h.lifecycle

WEB = "/ecs/prod-shop-web"
API = "/ecs/prod-shop-api"
JOBS = "/aws/lambda/shop-jobs"
GROUPS = [WEB, API, JOBS]


def action_line(action_id):
    return (
        f'Error: Failed to find Server Action "{action_id}". This request might be from an '
        "older or newer deployment."
    )


def parse_line(position, request_id):
    return (
        f"2026-10-25T14:03:11.123Z\t{request_id}\tERROR\tInvoke Error \t"
        + json.dumps({
            "errorType": "SyntaxError",
            "errorMessage": f"Unexpected token < in JSON at position {position}",
            "stack": [
                f"SyntaxError: Unexpected token < in JSON at position {position}",
                "    at JSON.parse (<anonymous>)",
                f"    at parseBody (/var/task/src/handler.js:{40 + position % 7}:17)",
            ],
        })
    )


class Signatures(unittest.TestCase):
    def test_ids_numbers_and_times_never_change_the_signature(self):
        self.assertEqual(F.signature(action_line("7f3a9b2c1d00")), F.signature(action_line("00ab12cd34ef99")))
        self.assertEqual(
            F.signature(parse_line(0, "1b2c3d4e-1111-2222-3333-444455556666")),
            F.signature(parse_line(1234, "9f9f9f9f-aaaa-bbbb-cccc-ddddeeeeffff")),
        )
        self.assertEqual(
            F.signature('10.0.1.5 - - [25/Oct/2026:14:03:11 +0000] "GET /api/users/12345/orders?x=1 HTTP/1.1" 404 512'),
            F.signature('10.0.3.9 - - [26/Oct/2026:02:00:00 +0000] "GET /api/users/987/orders HTTP/1.1" 404 77'),
        )

    def test_the_signature_names_type_message_and_frames(self):
        sig = F.signature(parse_line(12, "1b2c3d4e-1111-2222-3333-444455556666"))
        etype, message, frames = [p.strip() for p in sig.split("|")]
        self.assertEqual(etype, "SyntaxError")
        self.assertEqual(message, "unexpected token < in json at position <n>")
        self.assertIn("handler.js:parseBody", frames)
        tb = (
            'Traceback (most recent call last):\n  File "/var/task/app.py", line 41, in handler\n'
            '  File "/var/task/parse.py", line 12, in parse\nKeyError: \'id\''
        )
        self.assertEqual(F.signature(tb), "KeyError | keyerror: 'id' | parse.py:parse,app.py:handler")

    def test_different_errors_differ(self):
        self.assertNotEqual(F.signature(action_line("7f3a9b2c1d00")), F.signature(parse_line(0, "x")))
        self.assertNotEqual(
            F.signature('"GET /api/users/1 HTTP/1.1" 404 5'), F.signature('"GET /api/users/1 HTTP/1.1" 403 5')
        )
        self.assertNotEqual(F.signature("KeyError: 'id'"), F.signature("KeyError: 'email'"))

    def test_the_fingerprint_is_per_account_and_group(self):
        sig = F.signature(action_line("abc12345"))
        self.assertNotEqual(F.fingerprint(ACCOUNT, WEB, sig), F.fingerprint(ACCOUNT, API, sig))
        self.assertEqual(F.fingerprint(ACCOUNT, WEB, sig), F.fingerprint(ACCOUNT, WEB, sig))


# ── End to end through analyze_connection ─────────────────────────


def run(events, wording, existing, now=None):
    """One analyzer run over `events` whose model reply cites every evidence
    line containing wording["cite"] with wording["pattern"] as its pattern."""
    cw = FakeInsights(events)
    table = Table()
    notified = []

    def ask(prompt, _key, _team=None):
        if prompt.startswith("You check log findings"):
            items = json.loads(prompt.split("<items>\n", 1)[1].split("\n</items>", 1)[0])
            return json.dumps([{"id": i["id"], "supported": True, "title": i["title"],
                                "summary": i["summary"]} for i in items])
        evidence = json.loads(prompt.split("<evidence>\n", 1)[1].split("\n</evidence>", 1)[0])
        return json.dumps([{
            "title": wording["title"],
            "severity": "warning",
            "summary": wording["title"],
            "suggested_action": "Fix it.",
            "evidence_ids": [e["id"] for e in evidence if wording["cite"] in e["message"]],
            "error_pattern": wording["pattern"],
            "existing_id": None,
        }])

    with Patch(
        get_team_api_key=lambda _t: "key",
        logs_client_for=lambda _c: cw,
        list_recent_findings=lambda *_a: existing,
        record_connection_success=lambda *_a: None,
        notify_new_recommendations=lambda _t, _a, recs: notified.extend(recs),
        ask_model=ask,
        aws_insights_table=table,
    ):
        new = h.analyze_connection({
            "team_id": "TEAM#t", "account_id": ACCOUNT, "role_arn": "r", "external_id": "x",
            "region": "us-east-1", "log_groups": GROUPS,
        })
    return new, table, notified


def stored(put):
    """A put item as list_recent_findings returns it."""
    return {**put, "rec_id": put["sk"][len("REC#"):]}


def iso_ago(**kw):
    return (datetime.now(timezone.utc) - timedelta(**kw)).isoformat()


class Rewording(unittest.TestCase):
    """The same Server Action error, worded three ways on three days."""

    def events(self, day):
        return [(WEB, f"web/web/{day}", NOW - 3600 + i, action_line(f"{day}{i:06x}ab"), 3) for i in range(4)]

    def test_rewording_never_makes_a_new_finding(self):
        new, t1, _ = run(self.events(1), {"title": "Server Action not found", "cite": "Server Action",
                                          "pattern": "Failed to find Server Action"}, [])
        self.assertEqual((new, len(t1.puts)), (1, 1))
        item = stored(t1.puts[0])
        self.assertTrue(item["fingerprint"])
        self.assertEqual(item["error_hash"], item["fingerprint"])

        new, t2, _ = run(self.events(2), {"title": "Stale deployment: missing server actions",
                                          "cite": "Server Action",
                                          "pattern": "This request might be from an older or newer deployment"}, [item])
        self.assertEqual((new, len(t2.puts)), (0, 0))
        self.assertEqual(t2.updates[0]["Key"]["sk"], item["sk"])

        # Different words, different literal, a model that names no id: still it.
        new, t3, _ = run(self.events(3), {"title": "Next.js action lookup failures", "cite": "Server Action",
                                          "pattern": "Error: Failed to find"}, [item])
        self.assertEqual((new, len(t3.puts)), (0, 0))

    def test_a_finding_saved_before_signatures_is_still_found(self):
        legacy = {
            "rec_id": "old1", "sk": "REC#old1", "status": "pending", "source_log_group": WEB,
            "source_account_id": ACCOUNT, "error_pattern": "Failed to find Server Action",
            "error_hash": "0123456789abcdef", "created_at": iso_ago(days=5),
            "last_seen_at": iso_ago(days=1),
        }
        new, t, _ = run(self.events(4), {"title": "x", "cite": "Server Action",
                                         "pattern": "older or newer deployment"}, [legacy])
        self.assertEqual((new, len(t.puts)), (0, 0))
        vals = t.updates[0]["ExpressionAttributeValues"]
        self.assertTrue(vals[":sigs"])
        self.assertIn("if_not_exists(fingerprint", t.updates[0]["UpdateExpression"])


class Lifecycle(unittest.TestCase):
    def events(self):
        return [(API, "api/api/c3", NOW - 600, parse_line(i, f"1b2c3d4e-1111-2222-3333-44445555666{i}"), 2)
                for i in range(3)]

    WORDS = {"title": "Request body parse failures", "cite": "SyntaxError", "pattern": "Unexpected token < in JSON"}

    def first(self):
        _, t, _ = run(self.events(), self.WORDS, [])
        return stored(t.puts[0])

    def test_a_resolved_finding_that_reappears_is_reopened_not_duplicated(self):
        item = self.first()
        resolved_at = iso_ago(days=2)
        item.update(status="resolved", resolved_at=resolved_at, last_seen_at=iso_ago(days=9))
        reported, t, notified = run(self.events(), dict(self.WORDS, pattern="in JSON at position"), [item])
        # Reported again, never stored again.
        self.assertEqual((reported, len(t.puts)), (1, 0))
        (u,) = t.updates
        self.assertEqual(u["Key"]["sk"], item["sk"])
        self.assertIn("#s = :open", u["UpdateExpression"])
        self.assertIn("REMOVE resolved_at", u["UpdateExpression"])
        self.assertIn("reopen_count = if_not_exists(reopen_count, :zero) + :one", u["UpdateExpression"])
        self.assertEqual(u["ExpressionAttributeValues"][":was"], resolved_at)
        # The analyzer's own copy is open again for the rest of the run.
        self.assertEqual((item["status"], item.get("resolved_at")), ("pending", None))
        self.assertEqual(u["ExpressionAttributeValues"][":open"], "pending")
        # The team hears it came back.
        self.assertEqual([h.card_title(r) for r in notified], ["Seen again: Request body parse failures"])

    def test_an_ignored_finding_stays_ignored_and_is_counted(self):
        item = self.first()
        item.update(status="dismissed", dismissed_by="a@b.c")
        new, t, notified = run(self.events(), self.WORDS, [item])
        self.assertEqual((new, len(t.puts), notified), (0, 0, []))
        (u,) = t.updates
        self.assertNotIn("#s", u["UpdateExpression"].replace("#sig", ""))
        self.assertEqual(u["ExpressionAttributeValues"][":c"], 6)

    def test_a_finding_with_a_plan_keeps_no_ttl(self):
        item = self.first()
        item.update(status="approved", plan_id="p1")
        item.pop("ttl", None)
        _, t, notified = run(self.events(), self.WORDS, [item])
        (u,) = t.updates
        self.assertNotIn("ttl", u["UpdateExpression"])
        self.assertNotIn(":ttl", u["ExpressionAttributeValues"])
        self.assertEqual(notified, [])

    def test_a_long_resolved_finding_is_not_reopened(self):
        item = self.first()
        item.update(status="resolved", resolved_at=iso_ago(days=L.REOPEN_DAYS + 1))
        self.assertFalse(L.is_recent(item))

    def test_an_ignored_finding_wins_over_a_duplicate_open_one(self):
        a = {"rec_id": "a", "status": "pending", "source_log_group": API, "fingerprint": "f", "created_at": "1"}
        b = {"rec_id": "b", "status": "dismissed", "source_log_group": API, "fingerprint": "f", "created_at": "2"}
        c = {"rec_id": "c", "status": "pending", "source_log_group": API, "fingerprint": "f",
             "created_at": "3", "fix_ticket_id": "LOG-1"}
        s = {"log_group": API, "fingerprint": "f"}
        self.assertEqual(L.find_match(s, [a, b])["rec_id"], "b")
        self.assertEqual(L.find_match(s, [a, b, c])["rec_id"], "c")

    def test_one_missed_run_never_resolves(self):
        seen_yesterday = {"rec_id": "r1", "sk": "REC#r1", "status": "pending", "last_seen_at": iso_ago(days=1)}
        quiet = {"rec_id": "r2", "sk": "REC#r2", "status": "pending", "last_seen_at": iso_ago(days=8)}
        ignored = {"rec_id": "r3", "sk": "REC#r3", "status": "dismissed", "last_seen_at": iso_ago(days=20)}
        self.assertEqual([i["rec_id"] for i in L.stale([seen_yesterday, quiet, ignored], set())], ["r2"])
        t = Table()
        with Patch(aws_insights_table=t, emit_event=lambda *_a, **_k: None):
            h.resolve_stale_recommendations("TEAM#t", [seen_yesterday, quiet, ignored], set())
        self.assertEqual([u["Key"]["sk"] for u in t.updates], ["REC#r2"])

    def test_history_adds_new_lines_per_day(self):
        hist = L.add_history([], "2026-10-25", 5)
        hist = L.add_history(hist, "2026-10-25", 2)
        hist = L.add_history(hist, "2026-10-26", 0)
        self.assertEqual(hist, [{"day": "2026-10-25", "lines": 7}, {"day": "2026-10-26", "lines": 0}])
        long = []
        for d in range(40):
            long = L.add_history(long, f"2026-09-{d:02d}", 1)
        self.assertEqual(len(long), L.HISTORY_DAYS)


class MultiGroupAttribution(unittest.TestCase):
    """The same parse error in the API (two streams) and the jobs function."""

    def test_each_group_gets_its_own_finding_with_its_own_streams(self):
        events = [
            (API, "api/api/c3", NOW - 900, parse_line(1, "1b2c3d4e-1111-2222-3333-444455556661"), 7),
            (API, "api/api/d4", NOW - 800, parse_line(2, "1b2c3d4e-1111-2222-3333-444455556662"), 2),
            (JOBS, "2026/10/25/[$LATEST]ab", NOW - 700, parse_line(3, "1b2c3d4e-1111-2222-3333-444455556663"), 4),
        ]
        words = {"title": "Request body parse failures", "cite": "SyntaxError", "pattern": "Unexpected token < in JSON"}
        new, t, _ = run(events, words, [])
        self.assertEqual(new, 2)
        by = {p["source_log_group"]: p for p in t.puts}
        self.assertEqual(set(by), {API, JOBS})
        self.assertNotIn(WEB, json.dumps(t.puts))
        self.assertEqual(by[API]["log_streams"], [{"log_stream": "api/api/c3", "lines": 7},
                                                  {"log_stream": "api/api/d4", "lines": 2}])
        self.assertEqual(by[API]["also_in"], [{"log_group": JOBS, "lines": 4}])
        self.assertEqual(by[JOBS]["also_in"], [{"log_group": API, "lines": 9}])
        self.assertNotEqual(by[API]["fingerprint"], by[JOBS]["fingerprint"])
        self.assertEqual(by[API]["signature"], by[JOBS]["signature"])

    def test_a_sighting_in_one_group_never_matches_a_finding_in_another(self):
        other = {"rec_id": "r1", "status": "pending", "source_log_group": API, "fingerprint": "f",
                 "signature_hashes": ["s"]}
        self.assertIsNone(L.find_match({"log_group": JOBS, "fingerprint": "f", "signature_hashes": ["s"],
                                        "existing_id": "r1"}, [other]))

    def test_secrets_are_credited_per_group_never_to_multiple(self):
        batches = [{"query_name": "general_errors", "log_groups": GROUPS, "results": [
            {"@log": f"{ACCOUNT}:{API}", "@logStream": "a", "@message": "ERROR token ghp_" + "a" * 40, "n": "3"},
            {"@log": f"{ACCOUNT}:{JOBS}", "@logStream": "b", "@message": "ERROR password=hunter2", "n": "1"},
            {"@log": f"{ACCOUNT}:{WEB}", "@logStream": "c", "@message": "ERROR nothing secret", "n": "9"},
        ]}]
        advisories = h.secrets_advisories(batches, [], ACCOUNT)
        self.assertEqual([(a["log_group"], a["stats"]["count"]) for a in advisories], [(JOBS, 1), (API, 3)])
        self.assertEqual(advisories[1]["stats"]["streams"], {"a": 3})


# ── The one-off merge ─────────────────────────────────────────────


def dup(rec_id, created, status="resolved", pattern="Failed to find Server Action", group=WEB, **kw):
    return {
        "pk": "TEAM#t", "sk": f"REC#{rec_id}", "rec_id": rec_id, "status": status,
        "source_account_id": ACCOUNT, "source_log_group": group, "title": f"title {rec_id}",
        "error_pattern": pattern, "error_hash": f"h{rec_id}", "created_at": created,
        "first_seen_at": created, "last_seen_at": created, "count_basis": "events", "seen_count": 10,
        "samples": [{"message": action_line(f"{rec_id}aa11bb22")}], **kw,
    }


class QueryTable(Table):
    def __init__(self, items):
        super().__init__()
        self.items = items

    def query(self, **_kw):
        return {"Items": [dict(i) for i in self.items]}


class MergeDuplicates(unittest.TestCase):
    def items(self):
        return [
            dup("a1", "2026-10-01T06:00:00+00:00", pattern="Failed to find Server Action"),
            dup("a2", "2026-10-03T06:00:00+00:00", pattern="older or newer deployment"),
            dup("a3", "2026-10-05T06:00:00+00:00", status="dismissed", pattern='Server Action "'),
            dup("a4", "2026-10-07T06:00:00+00:00", status="pending", pattern="Error: Failed to find"),
            # Another error in the same group, and the same error in another group.
            dup("b1", "2026-10-02T06:00:00+00:00", pattern="Unexpected token < in JSON",
                samples=[{"message": parse_line(1, "x")}]),
            dup("c1", "2026-10-02T06:00:00+00:00", group=API),
            # Stored before #301 with a list for its group.
            dup("z1", "2026-09-20T06:00:00+00:00", group="/ecs/a, /ecs/b, /ecs/c (+47 more)"),
        ]

    def test_dry_run_proposes_one_merge_and_writes_nothing(self):
        t = QueryTable(self.items())
        rows = []
        tally = M.run_team(t, "TEAM#t", False, rows.append, now="2026-10-27T00:00:00+00:00")
        self.assertEqual((t.updates, t.puts), ([], []))
        (merge,) = [r for r in rows if r["action"] == "merge"]
        # Ignored wins: the error stays ignored.
        self.assertEqual(merge["keep"]["rec_id"], "a3")
        self.assertEqual(sorted(f["rec_id"] for f in merge["fold"]), ["a1", "a2", "a4"])
        self.assertEqual(merge["set"]["seen_count"], 40)
        self.assertEqual(merge["set"]["reopen_count"], 3)
        self.assertEqual(A.parse_ts(merge["set"]["first_seen_at"]), A.parse_ts("2026-10-01T06:00:00+00:00"))
        self.assertEqual(A.parse_ts(merge["set"]["last_seen_at"]), A.parse_ts("2026-10-07T06:00:00+00:00"))
        self.assertFalse(merge["applied"])
        (amb,) = [r for r in rows if r["action"] == "ambiguous_group"]
        self.assertEqual(amb["rec_id"], "z1")
        self.assertEqual(tally["merge"], 1)
        self.assertEqual(tally["folded"], 3)

    def test_apply_marks_duplicates_merged_and_deletes_nothing(self):
        t = QueryTable(self.items())
        M.run_team(t, "TEAM#t", True, lambda _r: None, now="2026-10-27T00:00:00+00:00")
        keep, *folded = t.updates
        self.assertEqual(keep["Key"]["sk"], "REC#a3")
        self.assertEqual(sorted(u["Key"]["sk"] for u in folded), ["REC#a1", "REC#a2", "REC#a4"])
        for u in folded:
            self.assertEqual(u["ExpressionAttributeValues"][":k"], "a3")
            self.assertIn("attribute_not_exists(merged_into)", u["ConditionExpression"])
        names = keep["ExpressionAttributeNames"].values()
        self.assertIn("merged_history", names)
        self.assertNotIn("status", names)

    def test_two_fix_runs_are_only_reported(self):
        items = [dup("f1", "2026-10-01T00:00:00+00:00", fix_ticket_id="LOG-A"),
                 dup("f2", "2026-10-02T00:00:00+00:00", fix_ticket_id="LOG-B")]
        rows = []
        t = QueryTable(items)
        M.run_team(t, "TEAM#t", True, rows.append)
        self.assertEqual([r["action"] for r in rows], ["two_fixes"])
        self.assertEqual(t.updates, [])

    def test_a_merged_finding_is_never_matched_again(self):
        gone = dup("m1", "2026-10-01T00:00:00+00:00", status="pending", merged_into="a3")
        self.assertFalse(L.is_recent(gone))
        self.assertEqual(M.clusters([gone, dup("m2", "2026-10-02T00:00:00+00:00")]), [])


if __name__ == "__main__":
    unittest.main()
