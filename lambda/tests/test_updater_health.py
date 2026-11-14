"""After an update the updater watches the new release: it probes the worker
and reads its errors against the hour before, and when either fails it rolls
the install back by itself, with no review, and tells the admins. A person's
Roll back away from a failing release is one call.

Run: python3 -m unittest discover -s lambda/tests
"""

import io
import json
import unittest
from datetime import timedelta

import test_updater_native as base

u = base.u
NOW = base.NOW
ROLE = base.ROLE
POLICY = [{"ResourceChange": {"Action": "Modify", "ResourceType": "AWS::IAM::Policy", "LogicalResourceId": "WorkerRoleDefaultPolicy"}}]
TABLE = [{"ResourceChange": {"Action": "Remove", "ResourceType": "AWS::DynamoDB::Table", "LogicalResourceId": "NewTable"}}]
SEGFAULT = ("fail", 'the worker failed: {"errorType":"Runtime.ExitError","errorMessage":"RequestId: 0f0f Error: Runtime exited '
                    'with error: signal: segmentation fault"}')


class Cfn(base.FakeCfn):
    """Each change set its own Id and state: created ready to run, then executing once executed."""

    def __init__(self):
        super().__init__()
        self.states = {}

    def create_change_set(self, **kw):
        self.created.append(kw)
        cs = f"arn:cs/{len(self.created)}"
        self.states[cs] = {"Status": "CREATE_COMPLETE", "ExecutionStatus": "AVAILABLE"}
        return {"Id": cs}

    def describe_change_set(self, ChangeSetName, StackName=None, IncludePropertyValues=False, NextToken=None):
        if ChangeSetName in self.states:
            self.cs = self.states[ChangeSetName]
        return super().describe_change_set(ChangeSetName, StackName, IncludePropertyValues, NextToken)

    def execute_change_set(self, ChangeSetName, StackName):
        super().execute_change_set(ChangeSetName, StackName)
        self.states[ChangeSetName] = {"Status": "CREATE_COMPLETE", "ExecutionStatus": "EXECUTE_IN_PROGRESS"}


class Probe:
    """The worker's answers to the health probe, in order (the last repeats)."""

    def __init__(self, *answers):
        self.answers = answers or (("ok", "v1.1.0"),)
        self.calls = 0

    def __call__(self, env):
        self.calls += 1
        return self.answers[min(self.calls, len(self.answers)) - 1]


def rates(after, before=(0, 600), fn="coderhelm-prod-worker"):
    return lambda env, since, now: {fn: {"before": list(before), "after": list(after)}}


class Watch(unittest.TestCase):
    """A native install that just finished updating v1.0.0 to v1.1.0, as the new release's updater sees it."""

    def setUp(self):
        self.store = u.Store(base.FakeDynamo(), "t")
        self.cfn = Cfn()
        u._clients["cloudformation"] = self.cfn
        u._clients["lambda"] = base.FakeLambda()
        self._saved = (u.fetch_template, u.notify, u._sleep, u._clock, u.METRICS, u.HEALTH_PROBE, u._deadline)
        u.fetch_template = lambda url, opener=None: base.TEMPLATE
        self.notices = []
        u.notify = lambda subject, text, env=None, **kw: self.notices.append(dict(kw, subject=subject, text=text)) or 1
        self.clock = [0.0]
        self.slept = []
        u._clock = lambda: self.clock[0]

        def sleep(s):
            self.slept.append(s)
            self.clock[0] += s
        u._sleep = sleep
        u.METRICS = rates((0, 40))
        u.HEALTH_PROBE = lambda env: ("unknown", "not asked in tests")
        u._deadline = None
        self.env = dict(base.ENV, DEPLOY_ROLE_ARN=ROLE, CODERHELM_VERSION="v1.1.0", WORKER_FUNCTION_NAME="coderhelm-prod-worker",
                        RESOURCE_PREFIX="coderhelm-prod")
        for v in ("v1.0.0", "v1.1.0"):
            self.store.put(f"MANIFEST#{v}", {"verified": True, "manifest": None, "template_sha256": base.SHA})
        self.hid = "UPDATE#2026-10-31T11:50:00Z#aaaa0001"
        self.store.put(self.hid, {"version": "v1.1.0", "from_version": "v1.0.0", "actor": "owner@example.org", "mode": "update",
                                  "status": "succeeded", "started_at": "2026-10-31T11:50:00+00:00", "change_set_id": "arn:cs/0",
                                  "executed_at": "2026-10-31T11:51:00+00:00", "finished_at": NOW.isoformat(), "reason": ""})
        self.store.put("UPDATE_STATE", {"ran_versions": ["v1.0.0", "v1.1.0"]})

    def tearDown(self):
        (u.fetch_template, u.notify, u._sleep, u._clock, u.METRICS, u.HEALTH_PROBE, u._deadline) = self._saved
        u._clients.pop("cloudformation", None)
        u._clients.pop("lambda", None)

    def step(self, at=NOW, probe=None):
        return u.watch_step(self.store, self.env, at, probe or Probe())

    def rollback_entry(self):
        w = self.store.get(u.WATCH)
        return self.store.get(w["rollback_id"]) if w.get("rollback_id") else None

    # ── The probe ─────────────────────────────────────────────────────────

    def test_a_healthy_release_is_watched_for_the_window_and_left_alone(self):
        p = Probe()
        w = self.step(probe=p)
        self.assertEqual((w["status"], w["probe"]["result"], p.calls), ("watching", "ok", 1))
        # Once probed, later steps only read the errors.
        w = self.step(NOW + timedelta(minutes=5), p)
        self.assertEqual((w["status"], p.calls), ("watching", 1))
        w = self.step(NOW + u.WATCH_WINDOW, p)
        self.assertEqual(w["status"], "healthy")
        self.assertEqual(self.cfn.created, [])
        self.assertEqual(self.notices, [])
        self.assertEqual(self.store.get(self.hid)["status"], "succeeded")

    def test_one_failed_probe_then_an_answer_is_healthy(self):
        w = self.step(probe=Probe(SEGFAULT, ("ok", "v1.1.0")))
        self.assertEqual((w["probe"]["result"], w["probe"]["fails"]), ("ok", 1))
        self.assertEqual(self.slept, [u.PROBE_PAUSE])
        self.assertEqual(self.cfn.created, [])

    def test_a_worker_that_fails_every_probe_is_rolled_back_with_no_review(self):
        self.cfn.nested["nested-api"] = {"Changes": POLICY}  # the rollback changes a role's policy back
        w = self.step(probe=Probe(SEGFAULT))
        self.assertEqual(w["probe"]["fails"], u.PROBE_ATTEMPTS)
        self.assertEqual(self.slept, [u.PROBE_PAUSE] * (u.PROBE_ATTEMPTS - 1))  # about 90 seconds of probing
        self.assertEqual(w["status"], "rolling_back")
        self.assertIn("failed its health check 4 times in a row", w["why"])
        self.assertIn("segmentation fault", w["why"])
        # A change set back to v1.0.0, through the deploy role, executed at once: no review, no confirm.
        self.assertEqual(len(self.cfn.created), 1)
        self.assertIn("/v1.0.0/", self.cfn.created[0]["TemplateURL"])
        self.assertEqual(self.cfn.created[0]["RoleARN"], ROLE)
        self.assertEqual(self.cfn.executed, ["arn:cs/1"])
        e = self.rollback_entry()
        self.assertEqual((e["mode"], e["actor"], e["status"]), ("rollback", "auto-rollback", "running"))
        self.assertTrue(e["recovery"]["auto"])
        self.assertEqual(e["confirmed"]["on"], "recovery")
        self.assertIn("Api.WorkerRoleDefaultPolicy", e["confirmed"]["permission_changes"][0])
        # The update's own row, and auto-update never takes v1.1.0 again by itself.
        row = self.store.get(self.hid)
        self.assertEqual(row["status"], "rolled_back")
        self.assertIn("rolled it back to v1.0.0 by itself", row["summary"])
        self.assertIn("v1.1.0", self.store.get("UPDATE_STATE")["failed_versions"])
        # One notice: the version, what was seen, and that it went back.
        self.assertEqual(len(self.notices), 1)
        n = self.notices[0]
        self.assertEqual((n["what"], n["version"], n["facts"]["current"]), ("rolled_back", "v1.1.0", "v1.0.0"))
        self.assertIn("segmentation fault", n["facts"]["reason"])
        self.assertIn("rolled back to v1.0.0", n["subject"])
        # Later steps follow it and never send it twice.
        self.step(NOW + timedelta(minutes=5))
        self.assertEqual(len(self.notices), 1)
        self.assertEqual(len(self.cfn.created), 1)

    def test_a_call_that_could_not_be_made_never_counts_as_a_failure(self):
        throttled = ("unknown", "couldn't ask the worker (TooManyRequestsException: Rate exceeded)")
        w = self.step(probe=Probe(throttled))
        self.assertEqual((w["probe"]["result"], w["status"]), ("unknown", "watching"))
        self.assertEqual(self.cfn.created, [])
        w = self.step(NOW + u.WATCH_WINDOW)
        self.assertEqual(w["status"], "healthy")

    def test_a_short_invocation_stops_probing_and_the_next_poll_goes_on(self):
        u._deadline = 50  # 50 seconds left in this invocation
        w = self.step(probe=Probe(SEGFAULT))
        self.assertEqual((w["probe"]["fails"], w["probe"]["result"], w["status"]), (1, None, "watching"))
        u._deadline = None
        w = self.step(NOW + timedelta(minutes=5), Probe(SEGFAULT))
        self.assertEqual((w["probe"]["fails"], w["status"]), (u.PROBE_ATTEMPTS, "rolling_back"))

    # ── The errors ────────────────────────────────────────────────────────

    def test_an_error_spike_after_the_update_rolls_it_back(self):
        u.METRICS = rates(after=(30, 40), before=(3, 600))
        w = self.step()
        self.assertEqual(w["status"], "rolling_back")
        self.assertIn("coderhelm-prod-worker failed 30 of 40 invocations since the update (75%), against 0% in the hour before", w["why"])
        self.assertEqual(self.cfn.executed, ["arn:cs/1"])
        self.assertIn("75%", self.notices[0]["facts"]["reason"])

    def test_normal_noise_never_rolls_back(self):
        for after, before in (
            ((3, 3), (0, 600)),        # every call failed, but too few calls to judge
            ((4, 40), (2, 600)),       # 10%: below the line
            ((20, 40), (180, 600)),    # 50%, but it failed 30% the hour before: not the release
            ((0, 0), (0, 0)),          # no traffic at all
        ):
            self.assertEqual(u.errors_tripped({"coderhelm-prod-worker": {"before": list(before), "after": list(after)}}), "", (after, before))
        u.METRICS = rates(after=(4, 40))
        self.step()
        w = self.step(NOW + u.WATCH_WINDOW)
        self.assertEqual(w["status"], "healthy")
        self.assertEqual(self.cfn.created, [])

    def test_the_gateway_counts_too(self):
        u.METRICS = lambda env, since, now: {"coderhelm-prod-worker": {"before": [0, 100], "after": [0, 10]},
                                             "coderhelm-prod-gateway": {"before": [1, 900], "after": [80, 100]}}
        self.assertIn("coderhelm-prod-gateway failed 80 of 100", self.step()["why"])

    def test_metrics_that_cannot_be_read_leave_the_probe_to_decide(self):
        def refused(env, since, now):
            raise RuntimeError("AccessDenied: cloudwatch:GetMetricData")
        u.METRICS = refused
        w = self.step()
        self.assertIn("AccessDenied", w["errors_note"])
        self.assertEqual(w["status"], "watching")
        self.assertEqual(self.cfn.created, [])

    # ── When the rollback doesn't happen ──────────────────────────────────

    def test_a_rollback_that_cannot_start_is_loud_and_never_tried_again(self):
        self.cfn.stack_status = "UPDATE_IN_PROGRESS"  # AWS is busy with the stack: begin refuses
        w = self.step(probe=Probe(SEGFAULT))
        self.assertEqual(w["status"], "rollback_failed")
        self.assertIn("couldn't start", w["problem"])
        self.assertEqual(self.cfn.created, [])
        self.assertEqual(len(self.notices), 1)
        n = self.notices[0]
        self.assertTrue(n["subject"].startswith("URGENT: v1.1.0 is unhealthy and the automatic rollback failed"))
        self.assertEqual(n["what"], "failed")
        self.assertIn("still runs v1.1.0", n["facts"]["action"])
        # It stops: no second try, no second email.
        self.cfn.stack_status = "UPDATE_COMPLETE"
        self.step(NOW + timedelta(minutes=5), Probe(SEGFAULT))
        self.assertEqual(self.cfn.created, [])
        self.assertEqual(len(self.notices), 1)

    def test_a_rollback_that_fails_in_aws_is_loud_and_stops(self):
        self.step(probe=Probe(SEGFAULT))
        self.assertEqual(self.cfn.executed, ["arn:cs/1"])
        # CloudFormation couldn't put v1.0.0 back and returned the stack to v1.1.0.
        self.cfn.states["arn:cs/1"] = {"Status": "CREATE_COMPLETE", "ExecutionStatus": "EXECUTE_FAILED"}
        self.cfn.stack_status = "UPDATE_ROLLBACK_COMPLETE"
        u.handle({"action": "native_poll"}, self.env, self.store, NOW + timedelta(minutes=5), health=Probe(SEGFAULT))
        w = self.store.get(u.WATCH)
        self.assertEqual(w["status"], "rollback_failed")
        self.assertTrue(any(n["subject"].startswith("URGENT") for n in self.notices))
        created = len(self.cfn.created)
        u.handle({"action": "native_poll"}, self.env, self.store, NOW + timedelta(minutes=10), health=Probe(SEGFAULT))
        self.assertEqual(len(self.cfn.created), created)

    def test_data_the_rollback_would_remove_still_waits_for_a_person_and_says_so_loudly(self):
        self.cfn.nested["nested-api"] = {"Changes": TABLE}
        w = self.step(probe=Probe(SEGFAULT))
        self.assertEqual(w["status"], "rolling_back")
        self.assertEqual(self.cfn.executed, [])
        self.assertEqual(self.rollback_entry()["status"], "review")
        self.assertTrue(any(n["subject"] == "URGENT: v1.1.0 is unhealthy; its rollback needs your OK" for n in self.notices))

    def test_another_release_running_ends_the_watch(self):
        self.step()
        w = u.watch_step(self.store, dict(self.env, CODERHELM_VERSION="v1.2.0"), NOW + timedelta(minutes=5), Probe(SEGFAULT))
        self.assertEqual(w["status"], "ended")
        self.assertEqual(self.cfn.created, [])

    def test_an_update_that_finished_long_ago_is_not_watched(self):
        self.assertIsNone(self.step(NOW + u.WATCH_START_LIMIT + timedelta(minutes=1), Probe(SEGFAULT)))
        self.assertEqual(self.cfn.created, [])

    def test_a_rollback_is_never_watched_into_another_rollback(self):
        e = self.store.get(self.hid)
        self.store.put(self.hid, dict(e, mode="rollback"))
        self.assertIsNone(self.step(probe=Probe(SEGFAULT)))

    # ── A person's Roll back ──────────────────────────────────────────────

    def roll_back(self, probe, **kw):
        return u.handle(dict({"action": "rollback", "actor": "owner@example.org"}, **kw), self.env, self.store, NOW, health=probe)

    def test_a_persons_rollback_from_a_failing_release_is_one_call(self):
        self.cfn.nested["nested-api"] = {"Changes": POLICY}
        r = self.roll_back(Probe(SEGFAULT))
        self.assertEqual(r["status"], "running")
        self.assertEqual(r["version"], "v1.0.0")
        self.assertEqual(self.cfn.executed, ["arn:cs/1"])  # no review, no apply_update, no confirm
        self.assertEqual(r["recovery"]["by"], "owner@example.org")
        self.assertIn("segmentation fault", r["recovery"]["why"])
        self.assertEqual(r["confirmed"]["on"], "recovery")

    def test_a_persons_rollback_from_a_healthy_release_still_shows_its_permission_changes(self):
        self.cfn.nested["nested-api"] = {"Changes": POLICY}
        r = self.roll_back(Probe())
        self.assertNotIn("recovery", r)
        self.assertEqual(u.native_poll(self.store, self.env, NOW)["native"], "review")
        self.assertEqual(self.cfn.executed, [])

    def test_a_persons_rollback_that_would_remove_data_applies_with_their_ok_in_the_same_call(self):
        self.cfn.nested["nested-api"] = {"Changes": TABLE}
        r = self.roll_back(Probe(SEGFAULT))
        self.assertEqual(r["status"], "review")
        self.assertIn("NewTable", r["pending"]["data_changes"][0])
        self.assertEqual(self.cfn.executed, [])
        r = self.roll_back(Probe(SEGFAULT), accept_data_changes=True)
        self.assertEqual(r["status"], "running")
        self.assertEqual(len(self.cfn.executed), 1)
        self.assertIn("NewTable", r["confirmed"]["data_changes"][0])

    def test_the_watch_finding_it_failing_is_enough_without_a_new_probe(self):
        self.store.put(u.WATCH, {"id": self.hid, "version": "v1.1.0", "from_version": "v1.0.0", "status": "rollback_failed",
                                 "why": "errors rose after the update", "since": NOW.isoformat(), "probe": {}})
        p = Probe()
        r = self.roll_back(p)
        self.assertEqual(p.calls, 0)
        self.assertEqual((r["status"], r["version"]), ("running", "v1.0.0"))

    # ── The incident ──────────────────────────────────────────────────────

    def test_incident_a_release_whose_worker_segfaults_on_every_event(self):
        """A release installed whose worker crashed at the start of every event
        (a stack overflow, Runtime.ExitError signal: segmentation fault, about
        360 ms in). The gateway still answered GET /api/me, so the update
        finished as succeeded; every job failed for about 50 minutes until a
        person ran rollback, then apply_update with its confirm token, after the
        prepared rollback sat in review for 10 minutes. The model check before
        the next update was the first thing to notice. Now the poll that
        finishes the update probes the worker, rolls back with no review and
        tells the admins, all in the same invocation."""
        self.store = u.Store(base.FakeDynamo(), "t")
        for v in ("v2.4.0", "v2.4.1"):
            self.store.put(f"MANIFEST#{v}", {"verified": True, "manifest": None, "template_sha256": base.SHA})
        old = dict(self.env, CODERHELM_VERSION="v2.4.0")
        u.begin(self.store, old, NOW - timedelta(minutes=12), "v2.4.1", "owner@example.org", "update", consent=True)
        u.native_poll(self.store, old, NOW - timedelta(minutes=11))
        self.assertEqual(self.cfn.executed, ["arn:cs/1"])
        # The stack finished; the poll now runs the new release's updater.
        self.cfn.states["arn:cs/1"] = {"Status": "CREATE_COMPLETE", "ExecutionStatus": "EXECUTE_COMPLETE"}
        self.cfn.nested["nested-api"] = {"Changes": POLICY}  # going back narrows a role's policy: once held for review
        new = dict(self.env, CODERHELM_VERSION="v2.4.1")
        worker = Probe(SEGFAULT)
        out = u.handle({"action": "native_poll"}, new, self.store, NOW, health=worker)
        self.assertEqual(out["status"], "succeeded")  # the gateway answers: the stack update itself succeeded
        w = out["health_watch"]
        self.assertEqual(worker.calls, u.PROBE_ATTEMPTS)
        self.assertEqual(sum(self.slept), u.PROBE_PAUSE * (u.PROBE_ATTEMPTS - 1))
        self.assertEqual((w["version"], w["from_version"], w["status"]), ("v2.4.1", "v2.4.0", "rolling_back"))
        # Back to v2.4.0 in the same invocation: no review, no apply_update, no confirm token.
        self.assertEqual(self.cfn.executed, ["arn:cs/1", "arn:cs/2"])
        self.assertIn("/v2.4.0/", self.cfn.created[1]["TemplateURL"])
        rb = self.store.get(w["rollback_id"])
        self.assertEqual((rb["status"], rb["actor"]), ("running", "auto-rollback"))
        n = [x for x in self.notices if x["what"] == "rolled_back"]
        self.assertEqual(len(n), 1)
        self.assertEqual((n[0]["version"], n[0]["facts"]["current"]), ("v2.4.1", "v2.4.0"))
        self.assertIn("Runtime.ExitError", n[0]["facts"]["reason"])
        self.assertIn("v2.4.1", self.store.get("UPDATE_STATE")["failed_versions"])
        # The hourly tick never takes v2.4.1 again by itself.
        state = {k: v for k, v in self.store.get("UPDATE_STATE").items() if k not in ("running_id", "running_since")}
        settings = u.validate_settings({"auto": True, "day": "daily", "hour": NOW.hour}, u.defaults_from_env({}))
        release = [{"version": "v2.4.1", "breaking": False, "prerelease": False, "published_at": "2026-10-30T00:00:00Z"}]
        self.assertIn("failed before", u.decide(NOW, settings, "v2.4.0", release, state)["reason"])


class BuildInstall(unittest.TestCase):
    """An install with an update build: the watch runs from the 5-minute domain check, and the rollback is a build."""

    def setUp(self):
        self.store = u.Store(base.FakeDynamo(), "t")
        self.builds = []
        test = self

        class CodeBuild:
            def start_build(self, projectName, environmentVariablesOverride):
                test.builds.append({v["name"]: v["value"] for v in environmentVariablesOverride})
                return {"build": {"id": f"coderhelm-prod-update:{len(test.builds)}"}}
        u._clients["codebuild"] = CodeBuild()
        self._saved = (u.notify, u._sleep, u.METRICS, u.domain_status)
        self.notices = []
        u.notify = lambda subject, text, env=None, **kw: self.notices.append(dict(kw, subject=subject)) or 1
        u._sleep = lambda s: None
        u.METRICS = rates((0, 10))
        u.domain_status = lambda store, env, now: {"domain": None}
        self.env = {"CODERHELM_VERSION": "v1.1.0", "UPDATE_PROJECT_NAME": "coderhelm-prod-update", "WORKER_FUNCTION_NAME": "coderhelm-prod-worker",
                    "APP_URL": "https://x.example.com"}
        self.store.put("UPDATE#2026-10-31T11:40:00Z#bbbb0001", {"version": "v1.1.0", "from_version": "v1.0.0", "mode": "update", "actor": "auto",
                                                                 "status": "succeeded", "build_id": "b:1", "finished_at": NOW.isoformat()})

    def tearDown(self):
        (u.notify, u._sleep, u.METRICS, u.domain_status) = self._saved
        u._clients.pop("codebuild", None)

    def test_the_scheduled_domain_check_watches_and_rolls_back_through_the_build(self):
        # Opening the page (the gateway's domain_status) never probes.
        p = Probe(SEGFAULT)
        self.assertNotIn("health_watch", u.handle({"action": "domain_status"}, self.env, self.store, NOW, health=p))
        self.assertEqual(p.calls, 0)
        out = u.handle({"action": "domain_status", "scheduled": True}, self.env, self.store, NOW, health=p)
        self.assertEqual(out["health_watch"]["status"], "rolling_back")
        self.assertEqual(self.builds, [{"VERSION": "v1.0.0", "ACTOR": "auto-rollback", "MODE": "rollback",
                                        "HISTORY_ID": out["health_watch"]["rollback_id"]}])
        self.assertEqual([n["what"] for n in self.notices], ["rolled_back"])


class ProbeAndMetrics(unittest.TestCase):
    """What the worker and CloudWatch answer, as the watch reads them."""

    class Lam:
        def __init__(self, answer=None, error=None, raise_=None):
            self.answer, self.error, self.raise_ = answer, error, raise_
            self.calls = []

        def invoke(self, FunctionName, InvocationType, Payload):
            self.calls.append((FunctionName, json.loads(Payload)))
            if self.raise_:
                raise self.raise_
            r = {"Payload": io.BytesIO(json.dumps(self.answer).encode())}
            if self.error:
                r["FunctionError"] = self.error
            return r

    ENV = {"WORKER_FUNCTION_NAME": "coderhelm-prod-worker"}

    def test_the_probe_reads_the_workers_answer(self):
        lam = self.Lam({"ok": True, "version": "v1.1.0"})
        self.assertEqual(u.probe_worker(self.ENV, lam), ("ok", "v1.1.0"))
        self.assertEqual(lam.calls, [("coderhelm-prod-worker", {"health": {}})])
        crash = self.Lam({"errorType": "Runtime.ExitError", "errorMessage": "Runtime exited with error: signal: segmentation fault"}, "Unhandled")
        result, detail = u.probe_worker(self.ENV, crash)
        self.assertEqual(result, "fail")
        self.assertIn("segmentation fault", detail)
        self.assertEqual(u.probe_worker(self.ENV, self.Lam({"ok": False}))[0], "fail")

    def test_what_says_nothing_about_the_release_is_unknown(self):
        class TooManyRequestsException(Exception):
            pass

        class ReadTimeoutError(Exception):
            pass
        self.assertEqual(u.probe_worker(self.ENV, self.Lam(raise_=TooManyRequestsException("Rate exceeded")))[0], "unknown")
        older = self.Lam({"errorType": "&serde_json::error::Error", "errorMessage": "missing field `Records`"}, "Unhandled")
        self.assertEqual(u.probe_worker(self.ENV, older)[0], "unknown")
        self.assertEqual(u.probe_worker({}, self.Lam())[0], "unknown")
        # A worker that hangs is not healthy.
        self.assertEqual(u.probe_worker(self.ENV, self.Lam(raise_=ReadTimeoutError("Read timeout on endpoint URL")))[0], "fail")

    def test_errors_are_split_at_the_update_from_one_read(self):
        since = NOW - timedelta(minutes=4, seconds=30)  # its minute (11:55) counts as after
        calls = []

        class Cw:
            def get_metric_data(self, **kw):
                calls.append(kw)
                ts = [NOW - timedelta(minutes=30), NOW - timedelta(minutes=5), NOW - timedelta(minutes=4), NOW - timedelta(minutes=1)]
                return {"MetricDataResults": [
                    {"Id": "errors0", "Timestamps": ts, "Values": [1, 0, 9, 10]},
                    {"Id": "invocations0", "Timestamps": ts, "Values": [100, 5, 10, 10]},
                    {"Id": "errors1", "Timestamps": [], "Values": []},
                    {"Id": "invocations1", "Timestamps": ts[:1], "Values": [7]},
                ]}
        out = u.error_rates({"WORKER_FUNCTION_NAME": "coderhelm-prod-worker", "RESOURCE_PREFIX": "coderhelm-prod"}, since, NOW, Cw())
        self.assertEqual(out["coderhelm-prod-worker"], {"before": [1, 100], "after": [19, 25]})
        self.assertEqual(out["coderhelm-prod-gateway"], {"before": [0, 7], "after": [0, 0]})
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["StartTime"], since.replace(second=0) - timedelta(hours=1))
        names = {q["MetricStat"]["Metric"]["MetricName"] for q in calls[0]["MetricDataQueries"]}
        self.assertEqual(names, {"Errors", "Invocations"})
        self.assertIn("failed 19 of 25 invocations since the update (76%), against 1%", u.errors_tripped(out))


if __name__ == "__main__":
    unittest.main()
