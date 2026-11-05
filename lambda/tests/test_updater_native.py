"""A native install's updates: a verified release becomes a change set on the
parent stack and runs through the install's deploy role (an admin's Update,
or the maintenance window with auto-update on); what a person must decide
waits for an admin's confirmation, and its end is reported in
CloudFormation's own words.

Run: python3 -m unittest discover -s lambda/tests
"""

import hashlib
import importlib.util
import io
import json
import pathlib
import sys
import unittest
from datetime import datetime, timezone

PATH = pathlib.Path(__file__).resolve().parents[1] / "updater" / "handler.py"
spec = importlib.util.spec_from_file_location("updater_native", PATH)
u = importlib.util.module_from_spec(spec)
sys.modules["updater_native"] = u
spec.loader.exec_module(u)

NOW = datetime(2026, 10, 31, 12, 0, tzinfo=timezone.utc)
TEMPLATE = json.dumps({"Parameters": {"Stage": {}, "AdminEmail": {}, "KeepDataOnDelete": {}, "Hostname": {}, "DomainCertificateArn": {}, "DomainZoneId": {}}}).encode()
SHA = hashlib.sha256(TEMPLATE).hexdigest()
ENV = {
    "NATIVE_STACK": "coderhelm-prod",
    "NATIVE_TEMPLATE_BASE": "https://coderhelm-public.s3.amazonaws.com/launch",
    "NATIVE_TEMPLATE_FILE": "coderhelm.json",
    "AWS_REGION": "us-east-1",
    "CODERHELM_VERSION": "v1.0.0",
    "APP_URL": "https://x.example.com",
    "NATIVE_HELPER_FUNCTION": "coderhelm-prod-install-helper",
}
FIXTURES = pathlib.Path(__file__).resolve().parents[2] / "infra" / "test" / "fixtures"


class FakeLambda:
    """The install helper's "stage": records each call, answers ok (or not)."""

    def __init__(self, answer=None, log=None):
        self.answer = answer or {"ok": True, "staged": 10, "copied": 10}
        self.calls = []
        self.log = log

    def invoke(self, FunctionName, InvocationType, Payload):
        self.calls.append((FunctionName, json.loads(Payload)))
        if self.log is not None:
            self.log.append("stage")
        return {"Payload": io.BytesIO(json.dumps(self.answer).encode())}


class FakeDynamo:
    def __init__(self):
        self.items = {}

    def get_item(self, TableName, Key):
        k = (Key["pk"]["S"], Key["sk"]["S"])
        return {"Item": self.items[k]} if k in self.items else {}

    def put_item(self, TableName, Item):
        self.items[(Item["pk"]["S"], Item["sk"]["S"])] = Item

    def query(self, TableName, KeyConditionExpression, ExpressionAttributeValues, ScanIndexForward, Limit):
        p, s = ExpressionAttributeValues[":p"]["S"], ExpressionAttributeValues[":s"]["S"]
        ks = sorted((k for k in self.items if k[0] == p and k[1].startswith(s)), key=lambda k: k[1], reverse=not ScanIndexForward)
        return {"Items": [self.items[k] for k in ks[:Limit]]}


class FakeCfn:
    def __init__(self):
        self.created = []
        self.executed = []
        self.cs = {"Status": "CREATE_PENDING", "ExecutionStatus": "UNAVAILABLE"}
        self.stack_status = "UPDATE_COMPLETE"
        self.events = []
        self.fetched = json.loads(TEMPLATE)
        self.nested = {}
        self.deleted = []

    def describe_stacks(self, StackName):
        return {"Stacks": [{"StackId": "arn:aws:cloudformation:us-east-1:1:stack/coderhelm-prod/abc", "StackStatus": self.stack_status,
                            "Parameters": [{"ParameterKey": k} for k in ("Stage", "AdminEmail", "Removed", "Hostname")]}]}

    def create_change_set(self, **kw):
        self.created.append(kw)
        if getattr(self, "log", None) is not None:
            self.log.append("change set")
        return {"Id": "arn:cs/1"}

    def get_template(self, StackName, ChangeSetName, TemplateStage):
        return {"TemplateBody": self.fetched}

    def delete_change_set(self, ChangeSetName, StackName):
        self.deleted.append(ChangeSetName)

    def describe_change_set(self, ChangeSetName, StackName=None, IncludePropertyValues=False, NextToken=None):
        self.described = getattr(self, "described", []) + [(ChangeSetName, IncludePropertyValues, NextToken)]
        # The real DescribeChangeSet output has no RoleARN (botocore's model): a fake that
        # returned it hid an apply that could never pass.
        assert "RoleARN" not in self.cs, "DescribeChangeSet never returns RoleARN"
        if ChangeSetName in self.nested:
            page = self.nested[ChangeSetName]
            # A nested change set given as a list of pages: NextToken is the page's index.
            if isinstance(page, list):
                i = int(NextToken or 0)
                return dict(page[i], **({"NextToken": str(i + 1)} if i + 1 < len(page) else {}))
            return page
        return dict({"ChangeSetName": "cs", "ChangeSetId": ChangeSetName, "StackId": "arn:stack/coderhelm-prod/abc", "Changes": [
            {"ResourceChange": {"Action": "Modify", "LogicalResourceId": "Api", "ResourceType": "AWS::CloudFormation::Stack", "Replacement": "False", "ChangeSetId": "nested-api"}},
            {"ResourceChange": {"Action": "Add", "LogicalResourceId": "NewThing", "ResourceType": "AWS::SQS::Queue"}},
        ]}, **self.cs)

    def execute_change_set(self, ChangeSetName, StackName):
        self.executed.append(ChangeSetName)

    def describe_stack_events(self, StackName):
        return {"StackEvents": self.events}


class FakeCodeBuild:
    """CodeBuild's own record of each release check build."""

    def __init__(self):
        self.builds = {}

    def finish(self, build_id, status="SUCCEEDED", version="v1.1.0", template=None, models="none", project="coderhelm-prod-release-check"):
        self.builds[build_id] = {
            "id": build_id, "buildStatus": status, "projectName": project,
            "environment": {"environmentVariables": [{"name": "VERSION", "value": version}]},
            "exportedEnvironmentVariables": [{"name": "TEMPLATE_SHA256", "value": template or ""}, {"name": "MODELS_SHA256", "value": models}],
        }

    def batch_get_builds(self, ids):
        return {"builds": [self.builds[i] for i in ids if i in self.builds]}


def opener_for(body):
    class R(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False
    return lambda req, timeout=None: R(body)


class Native(unittest.TestCase):
    def setUp(self):
        self.db = FakeDynamo()
        self.store = u.Store(self.db, "t")
        self.cfn = FakeCfn()
        u._clients["cloudformation"] = self.cfn
        self.lam = FakeLambda()
        u._clients["lambda"] = self.lam
        self.sent = []
        self._notify = u.notify
        u.notify = lambda *a, **k: self.sent.append(a[0])
        self._fetch = u.fetch_template
        u.fetch_template = lambda url, opener=None: TEMPLATE

    def tearDown(self):
        u.notify = self._notify
        u.fetch_template = self._fetch
        u._clients.pop("cloudformation", None)
        u._clients.pop("lambda", None)

    def verified(self, version="v1.1.0", sha=SHA):
        self.store.put(f"MANIFEST#{version}", {"verified": True, "manifest": None, "template_sha256": sha})

    def test_refuses_an_unverified_template(self):
        with self.assertRaisesRegex(ValueError, "isn't verified"):
            u.begin(self.store, ENV, NOW, "v1.1.0", "admin", "update")
        self.assertEqual(self.cfn.created, [])

    def test_refuses_a_template_that_does_not_match_the_signed_checksum(self):
        self.verified(sha="0" * 64)
        with self.assertRaisesRegex(ValueError, "doesn't match its signed checksum"):
            u.begin(self.store, ENV, NOW, "v1.1.0", "admin", "update")
        self.assertEqual(self.cfn.created, [])

    def test_proposes_a_change_set_with_previous_values_and_no_role(self):
        self.verified()
        u.begin(self.store, ENV, NOW, "v1.1.0", "admin", "update")
        kw = self.cfn.created[0]
        self.assertEqual(kw["TemplateURL"], "https://coderhelm-public.s3.amazonaws.com/launch/v1.1.0/coderhelm.json")
        self.assertNotIn("RoleARN", kw)
        self.assertIn("CAPABILITY_AUTO_EXPAND", kw["Capabilities"])
        keys = {p["ParameterKey"]: p for p in kw["Parameters"]}
        # Only what the new template declares; previous values where the stack has them.
        self.assertEqual(sorted(keys), ["AdminEmail", "Hostname", "Stage"])
        self.assertTrue(all(p.get("UsePreviousValue") for p in keys.values()))

    def test_review_flow_waits_for_the_admin_and_never_executes(self):
        self.verified()
        u.begin(self.store, ENV, NOW, "v1.1.0", "admin", "update")
        self.cfn.cs = {"Status": "CREATE_COMPLETE", "ExecutionStatus": "AVAILABLE"}
        r = u.native_poll(self.store, ENV, NOW)
        self.assertEqual(r["native"], "review")
        self.assertEqual(self.cfn.executed, [])
        state = self.store.get("UPDATE_STATE")
        self.assertNotIn("running_since", state)  # waiting on a person is not "running"
        status = u.handle({"action": "status"}, ENV, self.store, NOW, fetch=lambda repo: [])
        pending = status["native"]["pending"]
        self.assertEqual(pending["counts"], {"add": 1, "modify": 1, "remove": 0})
        self.assertEqual(pending["parts"], ["Api"])
        self.assertIn("changesets/changes", pending["link"])
        self.assertFalse(status["native"]["auto_apply"])
        # The admin executes it in the console; the poll follows it to the end.
        self.cfn.cs = {"Status": "CREATE_COMPLETE", "ExecutionStatus": "EXECUTE_COMPLETE"}
        r = u.native_poll(self.store, ENV, NOW)
        self.assertEqual(r["status"], "succeeded")

    def test_auto_update_executes_through_the_deploy_role(self):
        env = dict(ENV, DEPLOY_ROLE_ARN="arn:aws:iam::1:role/coderhelm-prod-deploy")
        self.verified()
        u.begin(self.store, env, NOW, "v1.1.0", "auto", "update")
        self.assertEqual(self.cfn.created[0]["RoleARN"], env["DEPLOY_ROLE_ARN"])
        self.cfn.cs = {"Status": "CREATE_COMPLETE", "ExecutionStatus": "AVAILABLE"}
        self.assertEqual(u.native_poll(self.store, env, NOW)["native"], "executing")
        self.assertEqual(self.cfn.executed, ["arn:cs/1"])

    def test_rollback_reports_cloudformations_first_failure(self):
        env = dict(ENV, DEPLOY_ROLE_ARN="arn:role")
        self.verified()
        u.begin(self.store, env, NOW, "v1.1.0", "auto", "update")
        self.cfn.cs = {"Status": "CREATE_COMPLETE", "ExecutionStatus": "AVAILABLE"}
        u.native_poll(self.store, env, NOW)
        self.cfn.cs = {"Status": "CREATE_COMPLETE", "ExecutionStatus": "EXECUTE_FAILED"}
        self.cfn.stack_status = "UPDATE_ROLLBACK_COMPLETE"
        self.cfn.events = [
            {"LogicalResourceId": "Worker", "ResourceStatus": "UPDATE_FAILED", "ResourceStatusReason": "Resource update cancelled", "Timestamp": "2026-10-31T12:05:00"},
            {"LogicalResourceId": "Api", "ResourceStatus": "UPDATE_FAILED", "ResourceStatusReason": "Embedded stack was not successfully updated", "Timestamp": "2026-10-31T12:03:00"},
        ]
        r = u.native_poll(self.store, env, NOW)
        self.assertEqual(r["status"], "rolled_back")
        hist = self.store.history()
        # Plain words first (the part's name), CloudFormation's own text under technical.
        self.assertIn("The update to v1.1.0 stopped: the Api part of the install could not be updated. AWS put everything back", hist[0]["reason"])
        self.assertIn("Api: Embedded stack", hist[0]["technical"])

    def test_no_changes_is_success_not_failure(self):
        self.verified()
        u.begin(self.store, ENV, NOW, "v1.1.0", "admin", "update")
        self.cfn.cs = {"Status": "FAILED", "StatusReason": "The submitted information didn't contain changes.", "ExecutionStatus": "UNAVAILABLE"}
        self.assertEqual(u.native_poll(self.store, ENV, NOW)["status"], "succeeded")

    def test_a_template_swapped_after_the_check_is_never_run(self):
        env = dict(ENV, DEPLOY_ROLE_ARN="arn:role")
        self.verified()
        u.begin(self.store, env, NOW, "v1.1.0", "auto", "update")
        self.cfn.fetched = {"Parameters": {}, "Resources": {"Evil": {}}}
        self.cfn.cs = {"Status": "CREATE_COMPLETE", "ExecutionStatus": "AVAILABLE"}
        r = u.native_poll(self.store, env, NOW)
        self.assertEqual(r["status"], "failed")
        self.assertEqual(self.cfn.executed, [])
        self.assertEqual(self.cfn.deleted, ["arn:cs/1"])
        self.assertIn("didn't match the signed release", self.store.history()[0]["reason"])
        self.assertIn("not the verified one", self.store.history()[0]["technical"])

    def test_auto_update_holds_when_a_part_would_replace_data(self):
        env = dict(ENV, DEPLOY_ROLE_ARN="arn:role")
        self.verified()
        self.cfn.nested["nested-api"] = {"Changes": [{"ResourceChange": {"Action": "Modify", "ResourceType": "AWS::DynamoDB::GlobalTable", "Replacement": "True", "LogicalResourceId": "Table"}}]}
        u.begin(self.store, env, NOW, "v1.1.0", "auto", "update")
        self.assertTrue(self.cfn.created[0]["IncludeNestedStacks"])
        self.cfn.cs = {"Status": "CREATE_COMPLETE", "ExecutionStatus": "AVAILABLE"}
        r = u.native_poll(self.store, env, NOW)
        self.assertEqual(r["native"], "review")
        self.assertEqual(self.cfn.executed, [])
        pending = u.handle({"action": "status"}, env, self.store, NOW, fetch=lambda repo: [])["native"]["pending"]
        self.assertIn("Api.Table (AWS::DynamoDB::GlobalTable", pending["data_changes"][0])

    def test_a_domain_move_needs_a_verified_template_too(self):
        started = []
        self._sv = u.start_verify
        u.start_verify = lambda store, env, now, version: started.append(version)
        try:
            with self.assertRaisesRegex(ValueError, "isn't verified"):
                u.begin(self.store, ENV, NOW, "v1.0.0", "admin", "domain", {"DOMAIN_ACTION": "attach", "DOMAIN_HOSTNAME": "x.example.com"})
        finally:
            u.start_verify = self._sv
        self.assertEqual(started, ["v1.0.0"])
        self.assertEqual(self.cfn.created, [])

    def test_never_an_older_release_as_an_update(self):
        self.store.put("MANIFEST#v0.9.0", {"verified": True, "template_sha256": SHA})
        with self.assertRaisesRegex(ValueError, "older"):
            u.native_propose(self.store, ENV, "v0.9.0", "update")

    def test_domain_attach_sets_the_parameters(self):
        self.verified("v1.0.0")
        u.begin(self.store, ENV, NOW, "v1.0.0", "admin", "domain",
                {"DOMAIN_ACTION": "attach", "DOMAIN_HOSTNAME": "ch.example.com", "DOMAIN_CERT": "arn:cert", "DOMAIN_ZONE_ID": "Z1"})
        keys = {p["ParameterKey"]: p for p in self.cfn.created[0]["Parameters"]}
        self.assertEqual(keys["Hostname"]["ParameterValue"], "ch.example.com")
        self.assertEqual(keys["DomainCertificateArn"]["ParameterValue"], "arn:cert")
        self.assertEqual(keys["DomainZoneId"]["ParameterValue"], "Z1")

    def test_models_verified_keeps_the_template_sha(self):
        env = dict(ENV, RELEASE_CHECK_PROJECT_NAME="coderhelm-prod-release-check")
        self._mc, self._nb = u.model_check, u.notify_blocked
        u.model_check = lambda *a, **k: {}
        u.notify_blocked = lambda *a, **k: None
        cb = FakeCodeBuild()
        u._clients["codebuild"] = cb
        try:
            self.store.put("UPDATE_STATE", {"verifying": {"version": "v1.1.0", "at": NOW.isoformat(), "build": "c-1"}})
            r = u.handle({"action": "models_verified", "version": "v1.1.0", "build": "c-1", "status": "verified", "manifest_text": None, "template_sha256": SHA}, env, self.store, NOW)
            self.assertEqual(r["verified"], "pending")
            self.assertIsNone(self.store.get("MANIFEST#v1.1.0"))
            cb.finish("c-1", template=SHA)
            u.handle({"action": "status"}, env, self.store, NOW, fetch=lambda repo: [])
            self.assertEqual(self.store.get("MANIFEST#v1.1.0")["template_sha256"], SHA)
            self.assertEqual(self.store.get("MANIFEST#v1.1.0")["build"], "c-1")
            with self.assertRaises(ValueError):
                u.handle({"action": "models_verified", "version": "v1.1.0", "build": "c-1", "status": "verified", "template_sha256": "nope"}, env, self.store, NOW)
        finally:
            u.model_check, u.notify_blocked = self._mc, self._nb
            u._clients.pop("codebuild", None)

    def test_a_template_checksum_the_build_didnt_export_is_never_believed(self):
        # Whoever else may invoke the updater can send a report, but not CodeBuild's record of the build.
        env = dict(ENV, RELEASE_CHECK_PROJECT_NAME="coderhelm-prod-release-check")
        cb = FakeCodeBuild()
        u._clients["codebuild"] = cb
        try:
            self.store.put("UPDATE_STATE", {"verifying": {"version": "v1.1.0", "at": NOW.isoformat(), "build": "c-1"}})
            u.handle({"action": "models_verified", "version": "v1.1.0", "build": "c-1", "status": "verified", "manifest_text": None, "template_sha256": "a" * 64}, env, self.store, NOW)
            cb.finish("c-1", template=SHA)
            with self.assertLogs(u.logger, "WARNING") as logs:
                u.handle({"action": "status"}, env, self.store, NOW, fetch=lambda repo: [])
            self.assertIn("template checksum isn't the one the build exported", "\n".join(logs.output))
            self.assertIsNone(self.store.get("MANIFEST#v1.1.0"))
        finally:
            u._clients.pop("codebuild", None)

    def test_a_domain_change_keeps_the_running_release(self):
        self.verified("v1.1.0")
        with self.assertRaisesRegex(ValueError, "keeps the running release"):
            u.native_propose(self.store, ENV, "v1.1.0", "domain", {"DOMAIN_ACTION": "attach", "DOMAIN_HOSTNAME": "x.example.com"})
        self.assertEqual(self.cfn.created, [])

    def test_a_rollback_only_goes_to_a_release_this_install_ran(self):
        self.store.put("MANIFEST#v0.9.0", {"verified": True, "template_sha256": SHA})
        with self.assertRaisesRegex(ValueError, "not a version this install ran"):
            u.native_propose(self.store, ENV, "v0.9.0", "rollback")
        self.assertEqual(self.cfn.created, [])
        self.store.put("UPDATE_STATE", {"ran_versions": ["v0.9.0"]})
        u.native_propose(self.store, ENV, "v0.9.0", "rollback")
        self.assertEqual(len(self.cfn.created), 1)

    def test_an_unknown_kind_of_change_is_refused(self):
        self.verified()
        with self.assertRaisesRegex(ValueError, "unknown change"):
            u.native_propose(self.store, ENV, "v1.1.0", "sideways")

    def test_only_release_urls_are_fetched(self):
        with self.assertRaises(ValueError):
            self._fetch("https://evil.example.com/launch/v1.0.0/coderhelm.json")
        self.assertEqual(self._fetch("https://coderhelm-public.s3.amazonaws.com/launch/v1.0.0/coderhelm.json", opener_for(b"{}")), b"{}")


if __name__ == "__main__":
    unittest.main()


TRANSFORM_DENIED = ("User: arn:aws:sts::111122223333:assumed-role/coderhelm-prod-deploy-stack/AWSCloudFormation is not authorized to perform: "
                    "cloudformation:CreateChangeSet on resource: arn:aws:cloudformation:us-east-1:aws:transform/LanguageExtensions because no "
                    "identity-based policy allows the cloudformation:CreateChangeSet action")


class PlainFailures(unittest.TestCase):
    """Why an update didn't succeed, in plain words: what happened and where
    the install is, the next step, and CloudFormation's own text apart (under
    Technical details). A failed try never blocks the next release."""

    setUp = Native.setUp
    tearDown = Native.tearDown
    verified = Native.verified
    ENV = dict(ENV, DEPLOY_ROLE_ARN="arn:aws:iam::111122223333:role/coderhelm-prod-deploy-stack")

    def fail_prepare(self, reason, version="v1.1.0"):
        self.verified(version)
        u.begin(self.store, self.ENV, NOW, version, "admin@example.com", "update")
        self.cfn.cs = {"Status": "FAILED", "StatusReason": reason, "ExecutionStatus": "UNAVAILABLE"}
        return u.native_poll(self.store, self.ENV, NOW)

    def test_the_transform_refusal_reads_plainly_with_aws_text_apart(self):
        self.assertEqual(self.fail_prepare("Nested change set arn:aws:cloudformation:us-east-1:111122223333:changeSet/x for Storage FAILED: " + TRANSFORM_DENIED)["status"], "failed")
        h = self.store.history()[0]
        self.assertEqual(h["summary"], "The update couldn't start: CoderHelm's update role isn't allowed to use an AWS template feature this release needs. "
                                       "Nothing changed: your install stayed on v1.0.0.")
        self.assertEqual(h["fix"], "Update again once a newer release is out (CoderHelm checks for one every hour).")
        self.assertEqual(h["reason"], h["summary"] + " " + h["fix"])
        self.assertIn("transform/LanguageExtensions", h["technical"])
        for k in ("summary", "fix", "reason"):
            self.assertNotIn("arn:", h[k], k)
            self.assertNotIn("cloudformation:", h[k], k)
        # The status the dashboard reads carries all four.
        row = u.handle({"action": "status"}, self.ENV, self.store, NOW, fetch=lambda repo: [])["history"][0]
        self.assertEqual((row["summary"], row["technical"]), (h["summary"], h["technical"]))

    def test_a_failed_try_never_blocks_the_next_release_or_a_retry(self):
        self.fail_prepare(TRANSFORM_DENIED)
        state = self.store.get("UPDATE_STATE")
        self.assertEqual(state["failed_versions"], ["v1.1.0"])
        self.assertFalse(u.running(state, NOW), "nothing is left running")
        self.assertNotIn("review_id", state)
        rel = [{"tag_name": "v1.1.0"}, {"tag_name": "v1.1.1"}]
        settings = dict(u.validate_settings({}, u.defaults_from_env({})), auto=True)
        # Auto-update skips only the release that failed; the fixed one goes ahead.
        d = u.decide(NOW, settings, "v1.0.0", rel, state)
        self.assertEqual(d["version"], "v1.1.1")
        self.assertNotIn("failed before", d["reason"])
        self.assertIn("failed before", u.decide(NOW, settings, "v1.0.0", rel[:1], state)["reason"])
        # The Update button prepares the fixed release at once...
        self.verified("v1.1.1")
        e = u.begin(self.store, self.ENV, NOW, "v1.1.1", "admin@example.com", "update")
        self.assertEqual(e["status"], "preparing")
        self.assertEqual(self.cfn.created[-1]["RoleARN"], self.ENV["DEPLOY_ROLE_ARN"])
        self.cfn.cs = {"Status": "CREATE_COMPLETE", "ExecutionStatus": "AVAILABLE"}
        u.native_poll(self.store, self.ENV, NOW)
        self.cfn.cs = {"Status": "CREATE_COMPLETE", "ExecutionStatus": "EXECUTE_COMPLETE"}
        self.assertEqual(u.native_poll(self.store, self.ENV, NOW)["status"], "succeeded")
        # ...and the failed one may be tried again by an admin.
        self.assertEqual(u.begin(self.store, self.ENV, NOW, "v1.1.0", "admin@example.com", "update")["status"], "preparing")

    def test_generic_and_other_known_causes_still_read_as_plain_words(self):
        cases = {
            "Something AWS never said before": ("AWS could not prepare the change this release needs", "Press Update again."),
            "Rate exceeded": ("AWS was busy", "in a few minutes"),
            "Resource limit exceeded for log groups": ("reached one of its limits", "Service Quotas"),
            "coderhelm-prod-x already exists in stack arn:aws:cloudformation:us-east-1:1:stack/other/1": ("same name is already in your AWS account", "leftover"),
            "User: arn:aws:sts::1:assumed-role/r/AWSCloudFormation is not authorized to perform: iam:CreateRole on resource: arn:aws:iam::1:role/x": ("update role isn't allowed to do something", "newer release"),
        }
        for raw, (what, fix) in cases.items():
            d = u.explain_failure(raw, "v1.1.0", "v1.0.0", u.PREPARE)
            self.assertTrue(d["summary"].startswith("The update couldn't start: "), raw)
            self.assertIn(what, d["summary"], raw)
            self.assertIn("Nothing changed: your install stayed on v1.0.0.", d["summary"], raw)
            self.assertIn(fix, d["fix"], raw)
            self.assertEqual(d["technical"], raw)
            self.assertNotIn("arn:", d["summary"] + d["fix"], raw)
        rb = u.explain_failure("Worker: Resource handler returned message: boom", "v1.1.0", "v1.0.0", u.ROLLED_BACK)
        self.assertEqual(rb["summary"], "The update to v1.1.0 stopped: the Worker part of the install could not be updated. AWS put everything back: your install stayed on v1.0.0.")
        mid = u.explain_failure("the stack ended UPDATE_ROLLBACK_FAILED", "v1.1.0", "v1.0.0", u.STOPPED)
        self.assertIn("may be between v1.0.0 and v1.1.0", mid["summary"])
        self.assertIn("AWS console", mid["fix"])

    def test_an_older_rows_aws_text_reads_plainly_in_history(self):
        # Written by an updater before this change: CloudFormation's text as the reason.
        self.store.put("UPDATE#2026-10-30T10:00:00Z#aaaa", {"version": "v1.1.0", "from_version": "v1.0.0", "status": "failed", "change_set_id": "arn:cs/0",
                                                           "started_at": "2026-10-30T10:00:00+00:00", "finished_at": "2026-10-30T10:01:00+00:00", "reason": TRANSFORM_DENIED[:500]})
        row = [h for h in u.handle({"action": "status"}, self.ENV, self.store, NOW, fetch=lambda repo: [])["history"] if h.get("version") == "v1.1.0"][0]
        self.assertTrue(row["reason"].startswith("The update couldn't start: CoderHelm's update role isn't allowed to use an AWS template feature"))
        self.assertIn("transform/LanguageExtensions", row["technical"])

    def test_an_aws_refusal_before_the_change_set_reads_plainly(self):
        self.verified()

        class Denied(Exception):
            pass

        def refuse(**kw):
            raise Denied("An error occurred (AccessDenied) when calling the CreateChangeSet operation: " + TRANSFORM_DENIED)
        self.cfn.create_change_set = refuse
        with self.assertRaises(ValueError):
            u.begin(self.store, self.ENV, NOW, "v1.1.0", "admin@example.com", "update")
        h = self.store.history()[0]
        self.assertEqual(h["status"], "not_started")
        self.assertIn("update role isn't allowed to use an AWS template feature", h["summary"])
        self.assertIn("AccessDenied", h["technical"])


class HonestHistory(unittest.TestCase):
    """History lists updates that started. A wait leaves no row, a refusal
    before anything changed is not_started (never failed), no row stays
    running after a crash, and a call with no actor names the AWS CLI."""

    def setUp(self):
        self.store = u.Store(FakeDynamo(), "t")
        self.cfn = FakeCfn()
        u._clients["cloudformation"] = self.cfn
        u._clients["lambda"] = FakeLambda()
        self._fetch, self._notify = u.fetch_template, u.notify
        u.notify = lambda *a, **k: None
        u.fetch_template = lambda url, opener=None: TEMPLATE

    def tearDown(self):
        u.fetch_template, u.notify = self._fetch, self._notify
        u._clients.pop("cloudformation", None)
        u._clients.pop("lambda", None)

    def verified(self, sha=SHA):
        self.store.put("MANIFEST#v1.1.0", {"verified": True, "manifest": None, "template_sha256": sha})

    def test_a_release_still_publishing_is_a_wait_with_no_row(self):
        def not_there_yet(url, opener=None):
            raise u.urllib.error.HTTPError(url, 403, "Forbidden", {}, None)
        u.fetch_template = not_there_yet
        self.verified()
        with self.assertRaisesRegex(u.ReleaseNotReady, r"v1\.1\.0 is still publishing; try again in a minute"):
            u.begin(self.store, ENV, NOW, "v1.1.0", "admin@example.org", "update")
        self.assertEqual(self.store.history(), [])
        self.assertNotIn("running_id", self.store.get("UPDATE_STATE", {}))
        self.assertEqual(self.cfn.created, [])

    def test_the_handler_answers_a_wait_and_never_crashes(self):
        def not_there_yet(url, opener=None):
            raise u.urllib.error.HTTPError(url, 404, "Not Found", {}, None)
        u.fetch_template = not_there_yet
        self.verified()
        try:
            u.begin(self.store, ENV, NOW, "v1.1.0", "admin@example.org", "update")
        except u.ReleaseNotReady as e:
            self.assertIsInstance(e, ValueError)

    def test_not_verified_yet_is_a_wait_too(self):
        with self.assertRaisesRegex(u.ReleaseNotReady, "isn't verified yet"):
            u.begin(self.store, ENV, NOW, "v1.1.0", "install guide", "update")
        self.assertEqual(self.store.history(), [])

    def test_a_refusal_before_anything_changed_is_not_started(self):
        self.verified(sha="0" * 64)
        with self.assertRaisesRegex(ValueError, "doesn't match its signed checksum"):
            u.begin(self.store, ENV, NOW, "v1.1.0", "admin@example.org", "update")
        [h] = self.store.history()
        self.assertEqual(h["status"], "not_started")
        self.assertIn("doesn't match its signed checksum", h["reason"])
        self.assertNotIn("running_id", self.store.get("UPDATE_STATE", {}))

    def test_an_unexpected_error_never_leaves_a_row_running(self):
        def broken(url, opener=None):
            raise ConnectionResetError("reset by peer")
        u.fetch_template = broken
        self.verified()
        with self.assertRaisesRegex(ValueError, "couldn't prepare the update.*nothing was changed"):
            u.begin(self.store, ENV, NOW, "v1.1.0", "admin@example.org", "update")
        [h] = self.store.history()
        self.assertEqual(h["status"], "not_started")
        self.assertEqual(self.cfn.created, [])

    def test_one_row_per_update_after_a_wait_then_the_retry(self):
        calls = []
        def first_403(url, opener=None):
            calls.append(url)
            if len(calls) == 1:
                raise u.urllib.error.HTTPError(url, 403, "Forbidden", {}, None)
            return TEMPLATE
        u.fetch_template = first_403
        self.verified()
        with self.assertRaises(u.ReleaseNotReady):
            u.begin(self.store, ENV, NOW, "v1.1.0", "admin@example.org", "update")
        e = u.begin(self.store, ENV, NOW, "v1.1.0", "admin@example.org", "update")
        rows = self.store.history()
        self.assertEqual([(r["id"], r["status"]) for r in rows], [(e["id"], "preparing")])

    def test_a_row_left_running_without_a_change_set_reads_as_not_started(self):
        self.store.put("UPDATE#2026-11-03T02:04:00Z#aaaa", {"version": "v1.1.0", "from_version": "v1.0.0", "actor": "unknown",
                                                           "mode": "update", "status": "running", "started_at": NOW.isoformat()})
        [h] = u.history_view(self.store.history(), {}, NOW)
        self.assertEqual(h["status"], "not_started")
        self.assertIn("stopped before it changed anything", h["reason"])

    def test_the_update_in_progress_still_reads_as_running(self):
        self.store.put("UPDATE#x", {"version": "v1.1.0", "status": "preparing", "change_set_id": "cs", "started_at": NOW.isoformat()})
        [h] = u.history_view(self.store.history(), {"running_id": "UPDATE#x"}, NOW)
        self.assertEqual(h["status"], "preparing")

    def test_a_started_update_past_the_running_limit_ended_without_a_result(self):
        old = (NOW - u.RUNNING_STALE - u.timedelta(minutes=1)).isoformat()
        self.store.put("UPDATE#y", {"version": "v1.1.0", "status": "running", "change_set_id": "cs", "started_at": old})
        [h] = u.history_view(self.store.history(), {}, NOW)
        self.assertEqual(h["status"], "failed")
        self.assertIn("ended without a result", h["reason"])

    def test_a_call_with_no_actor_names_the_aws_cli(self):
        self.verified()
        e = u.handle({"action": "update_now", "version": "v1.1.0"}, dict(ENV, CODERHELM_VERSION="v1.0.0"), self.store, NOW,
                     lambda repo: [], probe=lambda env, payload: {"ok": True, "results": []})
        self.assertEqual(e["actor"], "AWS CLI")


class LiveTestFindings(unittest.TestCase):
    """The live test's findings: the parts' templates are staged before the
    change set (bug 1); a template CloudFormation stores resolved can't be
    checked (bug 2's other half); an install on Anthropic API keys never
    needs Bedrock to update."""

    def setUp(self):
        self.store = u.Store(FakeDynamo(), "t")
        self.cfn = FakeCfn()
        self.order = []
        self.cfn.log = self.order
        self.lam = FakeLambda(log=self.order)
        u._clients["cloudformation"] = self.cfn
        u._clients["lambda"] = self.lam
        self._fetch = u.fetch_template
        self._notify = u.notify
        u.notify = lambda *a, **k: None

    def tearDown(self):
        u.fetch_template = self._fetch
        u.notify = self._notify
        u._clients.pop("cloudformation", None)
        u._clients.pop("lambda", None)

    def propose(self, body):
        u.fetch_template = lambda url, opener=None: body
        self.store.put("MANIFEST#v1.1.0", {"verified": True, "manifest": None, "template_sha256": hashlib.sha256(body).hexdigest()})
        return u.native_propose(self.store, ENV, "v1.1.0", "update")

    def test_the_parts_templates_are_staged_before_the_change_set_with_nested_stacks(self):
        self.propose(TEMPLATE)
        self.assertEqual(self.order, ["stage", "change set"])
        fn, payload = self.lam.calls[0]
        self.assertEqual(fn, "coderhelm-prod-install-helper")
        self.assertEqual(payload, {"action": "stage", "version": "v1.1.0", "template_sha256": SHA})
        self.assertTrue(self.cfn.created[0]["IncludeNestedStacks"])

    def test_no_change_set_when_the_files_could_not_be_staged(self):
        self.lam.answer = {"ok": False, "error": "ValueError: has sha256 x, not y as the signed template lists: refused"}
        with self.assertRaisesRegex(ValueError, "couldn't be staged.*refused"):
            self.propose(TEMPLATE)
        self.assertEqual(self.cfn.created, [])

    def test_a_template_with_a_transform_is_refused(self):
        body = json.dumps({"Transform": "AWS::LanguageExtensions", "Parameters": {"Stage": {}}}).encode()
        with self.assertRaisesRegex(ValueError, "uses a transform"):
            self.propose(body)
        self.assertEqual(self.cfn.created, [])
        self.assertEqual(self.lam.calls, [])

    def test_v085s_stored_template_never_matched_what_was_verified(self):
        # A real one, from the live test: CloudFormation keeps a template with
        # AWS::LanguageExtensions already resolved (every parameter and pseudo
        # parameter put in, the Transform gone), so the check rejected every
        # genuine update. That is why the parent has no transform now.
        source = json.loads((FIXTURES / "release-v0.8.5-parent.json").read_text())
        stored = json.loads((FIXTURES / "cfn-stored-v0.8.5-parent.json").read_text())
        self.assertEqual(source["Transform"], "AWS::LanguageExtensions")
        self.assertNotIn("Transform", stored)
        self.cfn.fetched = stored
        self.assertFalse(u.fetched_template_matches(self.cfn, ENV, "arn:cs/1", u.canonical_sha(source)))

    def test_a_template_without_a_transform_matches_as_cloudformation_returns_it(self):
        # Without a transform, CloudFormation keeps the template as it was
        # sent; GetTemplate returns it parsed or as text, keys in any order.
        source = json.loads((FIXTURES / "release-v0.8.5-parent.json").read_text())
        source.pop("Transform")
        want = u.canonical_sha(source)
        for body in (source, json.dumps(source, indent=2), json.loads(json.dumps(source, sort_keys=True))):
            self.cfn.fetched = body
            self.assertTrue(u.fetched_template_matches(self.cfn, ENV, "arn:cs/1", want))
        swapped = dict(source, Description="not the release")
        self.cfn.fetched = swapped
        self.assertFalse(u.fetched_template_matches(self.cfn, ENV, "arn:cs/1", want))


class AnthropicKeysInstall(unittest.TestCase):
    """ModelProvider=anthropic: the worker checks the models with a team's key,
    never Bedrock; with no key yet nothing holds the update."""

    def setUp(self):
        self.store = u.Store(FakeDynamo(), "t")
        self.store.put("MANIFEST#v1.1.0", {"verified": True, "manifest": {"calls": ["claude-opus-5-5"], "bedrock": {}}, "template_sha256": SHA})

    def no_key(self, env, payload):
        self.asked = payload
        return {"provider": "anthropic", "region": "us-east-1", "inference_prefix": "",
                "results": [{"model": t["model"], "id": t["model"], "status": "skip", "code": "no_key",
                             "detail": f"No Anthropic API key to check {t['model']} with."} for t in payload["targets"]]}

    def test_no_team_key_yet_skips_with_a_note_and_does_not_block(self):
        rec = u.model_check(self.store, dict(ENV, CONFIGURED_MODELS="claude-opus-5-5"), NOW, "v1.1.0", "r", probe=self.no_key)
        self.assertTrue(rec["ok"])
        self.assertFalse(u.blocking(rec, "v1.1.0"))
        self.assertEqual(rec["provider"], "anthropic")
        self.assertIn("Anthropic API keys, added per team", rec["note"])
        self.assertIn("nothing holds the update", rec["note"])

    def test_with_a_team_key_a_refused_model_still_holds_the_update(self):
        def refused(env, payload):
            return {"provider": "anthropic", "results": [{"model": "claude-opus-5-5", "status": "fail", "code": "bad_key", "detail": "401"}]}
        rec = u.model_check(self.store, ENV, NOW, "v1.1.0", "r", probe=refused)
        self.assertTrue(u.blocking(rec, "v1.1.0"))
        self.assertIn("doesn't use Bedrock", rec["note"])

    def test_bedrock_installs_get_no_such_note(self):
        def ok(env, payload):
            return {"provider": "bedrock", "results": [{"model": "claude-opus-5-5", "status": "pass", "code": "ok"}]}
        rec = u.model_check(self.store, ENV, NOW, "v1.1.0", "r", probe=ok)
        self.assertNotIn("note", rec)


class AdminOnlyChanges(unittest.TestCase):
    """The auto-update role can't change itself or a boundary (the boundary
    refuses it): such a change set waits for an admin instead of failing."""

    def setUp(self):
        self.store = u.Store(FakeDynamo(), "t")
        self.cfn = FakeCfn()
        u._clients["cloudformation"] = self.cfn
        u._clients["lambda"] = FakeLambda()
        self._fetch, self._notify = u.fetch_template, u.notify
        u.fetch_template = lambda url, opener=None: TEMPLATE
        u.notify = lambda *a, **k: None

    def tearDown(self):
        u.fetch_template, u.notify = self._fetch, self._notify
        u._clients.pop("cloudformation", None)
        u._clients.pop("lambda", None)

    def test_a_change_to_the_deploy_role_or_a_boundary_is_held_for_an_admin(self):
        env = dict(ENV, DEPLOY_ROLE_ARN="arn:aws:iam::1:role/coderhelm-prod-deploy-stack")
        self.store.put("MANIFEST#v1.1.0", {"verified": True, "manifest": None, "template_sha256": SHA})
        u.begin(self.store, env, NOW, "v1.1.0", "auto", "update")
        self.cfn.cs = {"Status": "CREATE_COMPLETE", "ExecutionStatus": "AVAILABLE", "Changes": [
            {"ResourceChange": {"Action": "Modify", "LogicalResourceId": "DeployRole", "ResourceType": "AWS::IAM::Role", "Replacement": "False"}},
            {"ResourceChange": {"Action": "Add", "LogicalResourceId": "DeployBoundary", "ResourceType": "AWS::IAM::ManagedPolicy"}},
        ]}
        r = u.native_poll(self.store, env, NOW)
        self.assertEqual(r["native"], "review")
        self.assertEqual(self.cfn.executed, [])

    def test_admin_changes_names_only_those(self):
        cs = {"Changes": [{"ResourceChange": {"Action": "Modify", "LogicalResourceId": "Boundary"}},
                          {"ResourceChange": {"Action": "Modify", "LogicalResourceId": "Api"}}]}
        self.assertEqual(u.admin_changes(cs), ["Boundary (Modify)"])


ROLE = "arn:aws:iam::1:role/coderhelm-prod-deploy-stack"


class AdminApplies(unittest.TestCase):
    """Every install has the deploy role: an admin's Update applies at once,
    a scheduled one only with auto-update on, and what a person must decide
    (data, permissions) applies only once an admin confirms exactly that in
    Settings, System. A change to the role or a boundary never applies here."""

    def setUp(self):
        self.store = u.Store(FakeDynamo(), "t")
        self.cfn = FakeCfn()
        u._clients["cloudformation"] = self.cfn
        u._clients["lambda"] = FakeLambda()
        self._fetch, self._notify = u.fetch_template, u.notify
        u.fetch_template = lambda url, opener=None: TEMPLATE
        self.sent = []
        u.notify = lambda *a, **k: self.sent.append(a[1])
        self.env = dict(ENV, DEPLOY_ROLE_ARN=ROLE)
        self.store.put("MANIFEST#v1.1.0", {"verified": True, "manifest": None, "template_sha256": SHA})

    def tearDown(self):
        u.fetch_template, u.notify = self._fetch, self._notify
        u._clients.pop("cloudformation", None)
        u._clients.pop("lambda", None)

    def prepare(self, actor="owner@example.org", scheduled=False, api=None, parent=None):
        if api is not None:
            self.cfn.nested["nested-api"] = {"Changes": api}
        u.begin(self.store, self.env, NOW, "v1.1.0", actor, "update", scheduled=scheduled)
        self.cfn.cs = {"Status": "CREATE_COMPLETE", "ExecutionStatus": "AVAILABLE"}
        if parent is not None:
            self.cfn.cs["Changes"] = parent
        return u.native_poll(self.store, self.env, NOW)

    def pending(self):
        return u.handle({"action": "status"}, self.env, self.store, NOW, fetch=lambda repo: [])["native"]["pending"]

    def apply(self, **kw):
        return u.handle(dict({"action": "apply_update", "actor": "owner@example.org"}, **kw), self.env, self.store, NOW)

    def test_an_admins_update_applies_through_the_role_with_auto_update_off(self):
        self.assertFalse(u.auto_on(self.store, self.env))
        self.assertEqual(self.prepare()["native"], "executing")
        self.assertEqual(self.cfn.created[0]["RoleARN"], ROLE)
        self.assertEqual(self.cfn.executed, ["arn:cs/1"])
        status = u.handle({"action": "status"}, self.env, self.store, NOW, fetch=lambda repo: [])
        self.assertTrue(status["native"]["can_apply"])
        self.assertEqual(status["native"]["pending"]["status"], "running")

    def test_a_scheduled_update_applies_only_while_auto_update_is_on(self):
        self.store.put("AUTO_UPDATE", {"auto": True})
        self.assertEqual(self.prepare(actor="auto", scheduled=True)["native"], "executing")

    def test_a_scheduled_update_waits_when_auto_update_was_turned_off(self):
        self.store.put("AUTO_UPDATE", {"auto": False})
        self.assertEqual(self.prepare(actor="auto", scheduled=True)["native"], "review")
        self.assertEqual(self.cfn.executed, [])
        p = self.pending()
        self.assertIn("auto-update was turned off", p["hold"])
        self.assertTrue(p["can_apply"])
        self.assertEqual(p["confirm"], "")
        # Nothing to confirm: Apply runs it.
        self.assertEqual(self.apply()["status"], "running")
        self.assertEqual(self.cfn.executed, ["arn:cs/1"])

    def test_the_tick_marks_its_update_as_scheduled(self):
        calls = []
        orig = u.begin
        u.begin = lambda *a, **k: calls.append(k)
        try:
            self.store.put("AUTO_UPDATE", {"auto": True, "day": "daily", "hour": NOW.hour})
            self.store.put("MODEL_CHECK", {"version": "v1.1.0", "results": [], "checked_at": NOW.isoformat()})
            orig_mc = u.model_check
            u.model_check = lambda *a, **k: {"version": "v1.1.0", "results": []}
            try:
                u.handle({"action": "tick"}, self.env, self.store, NOW,
                         fetch=lambda repo: [{"tag_name": "v1.1.0", "body": "", "html_url": "", "published_at": "2026-10-01T00:00:00Z"}])
            finally:
                u.model_check = orig_mc
        finally:
            u.begin = orig
        self.assertEqual(calls, [{"scheduled": True}])

    def test_scheduled_data_changes_are_held_as_today(self):
        self.store.put("AUTO_UPDATE", {"auto": True})
        r = self.prepare(actor="auto", scheduled=True, api=[{"ResourceChange": {"Action": "Remove", "ResourceType": "AWS::S3::Bucket", "LogicalResourceId": "Data"}}])
        self.assertEqual(r["native"], "review")
        self.assertEqual(self.cfn.executed, [])
        self.assertIn("auto-update held: it would replace or remove", self.pending()["hold"])

    def test_data_changes_apply_only_with_the_admins_confirmation_of_exactly_them(self):
        r = self.prepare(api=[{"ResourceChange": {"Action": "Modify", "ResourceType": "AWS::DynamoDB::GlobalTable", "Replacement": "True", "LogicalResourceId": "Table"}}])
        self.assertEqual(r["native"], "review")
        self.assertEqual(self.cfn.executed, [])
        p = self.pending()
        self.assertIn("waiting for you to confirm", p["hold"])
        self.assertIn("Api.Table", p["data_changes"][0])
        self.assertTrue(p["can_apply"])
        self.assertRegex(p["confirm"], r"^[0-9a-f]{64}$")
        self.assertIn("Settings, then System", self.sent[-1])
        self.assertNotIn("AWS console", self.sent[-1])
        for wrong in (None, "", "0" * 64):
            with self.assertRaisesRegex(ValueError, "confirm the changes"):
                self.apply(**({} if wrong is None else {"confirm": wrong}))
        self.assertEqual(self.cfn.executed, [])
        e = self.apply(confirm=p["confirm"], history_id=p["id"])
        self.assertEqual(e["status"], "running")
        self.assertEqual(self.cfn.executed, ["arn:cs/1"])
        self.assertEqual(e["confirmed"]["by"], "owner@example.org")
        self.assertIn("Api.Table", e["confirmed"]["data_changes"][0])
        state = self.store.get("UPDATE_STATE")
        self.assertEqual(state["running_id"], p["id"])
        self.assertNotIn("review_id", state)
        # The poll follows it from there; it is never executed twice.
        self.cfn.cs = {"Status": "CREATE_COMPLETE", "ExecutionStatus": "EXECUTE_COMPLETE"}
        self.assertEqual(u.native_poll(self.store, self.env, NOW)["status"], "succeeded")
        self.assertEqual(self.cfn.executed, ["arn:cs/1"])

    def test_permission_changes_need_the_confirmation_too(self):
        r = self.prepare(api=[{"ResourceChange": {"Action": "Add", "ResourceType": "AWS::IAM::Role", "LogicalResourceId": "NewRole"}}])
        self.assertEqual(r["native"], "review")
        p = self.pending()
        self.assertIn("Api.NewRole", p["permission_changes"][0])
        with self.assertRaisesRegex(ValueError, "confirm the changes"):
            self.apply()
        self.assertEqual(self.apply(confirm=p["confirm"])["status"], "running")

    def test_a_confirmation_for_another_prepared_update_is_refused(self):
        self.prepare(api=[{"ResourceChange": {"Action": "Add", "ResourceType": "AWS::IAM::Role", "LogicalResourceId": "NewRole"}}])
        p = self.pending()
        with self.assertRaisesRegex(ValueError, "no prepared update"):
            self.apply(confirm=p["confirm"], history_id="UPDATE#other")
        other = u.confirm_token("UPDATE#other", self.store.get(p["id"]))
        with self.assertRaisesRegex(ValueError, "confirm the changes"):
            self.apply(confirm=other)
        self.assertEqual(self.cfn.executed, [])

    def test_a_change_to_the_role_or_a_boundary_is_never_applied_here(self):
        r = self.prepare(parent=[{"ResourceChange": {"Action": "Modify", "LogicalResourceId": "DeployBoundary", "ResourceType": "AWS::IAM::ManagedPolicy"}}])
        self.assertEqual(r["native"], "review")
        p = self.pending()
        self.assertFalse(p["can_apply"])
        self.assertIn("DeployBoundary", p["admin_changes"][0])
        self.assertIn("AWS console", self.sent[-1])
        with self.assertRaisesRegex(ValueError, "can't apply for you"):
            self.apply(confirm=p["confirm"])
        self.assertEqual(self.cfn.executed, [])

    def test_a_change_set_prepared_with_the_role_applies(self):
        # DescribeChangeSet returns no RoleARN: the role recorded at prepare is what counts.
        self.prepare(api=[{"ResourceChange": {"Action": "Add", "ResourceType": "AWS::IAM::Role", "LogicalResourceId": "NewRole"}}])
        p = self.pending()
        self.assertEqual(self.store.get(p["id"])["role_arn"], ROLE)
        self.assertTrue(p["role"])
        self.assertEqual(self.apply(confirm=p["confirm"])["status"], "running")
        self.assertEqual(self.cfn.executed, ["arn:cs/1"])

    def test_an_entry_from_an_older_updater_with_apply_set_applies(self):
        self.prepare(api=[{"ResourceChange": {"Action": "Add", "ResourceType": "AWS::IAM::Role", "LogicalResourceId": "NewRole"}}])
        p = self.pending()
        entry = self.store.get(p["id"])
        entry.pop("role_arn")
        self.store.put(p["id"], entry)
        self.assertEqual(self.apply(confirm=p["confirm"])["status"], "running")
        self.assertEqual(self.cfn.executed, ["arn:cs/1"])

    def test_a_change_set_made_without_the_role_is_never_applied_here(self):
        self.prepare(api=[{"ResourceChange": {"Action": "Add", "ResourceType": "AWS::IAM::Role", "LogicalResourceId": "NewRole"}}])
        p = self.pending()
        entry = self.store.get(p["id"])
        for recorded in ("", "arn:aws:iam::111122223333:role/another-role"):
            self.store.put(p["id"], dict(entry, role_arn=recorded))
            with self.assertRaisesRegex(ValueError, "without the deploy role"):
                self.apply(confirm=p["confirm"])
        self.store.put(p["id"], entry)
        self.cfn.cs = {"Status": "CREATE_COMPLETE", "ExecutionStatus": "AVAILABLE", "ChangeSetId": "arn:cs/other"}
        with self.assertRaisesRegex(ValueError, "not the one this update prepared"):
            self.apply(confirm=p["confirm"])
        self.cfn.cs = {"Status": "CREATE_COMPLETE", "ExecutionStatus": "OBSOLETE"}
        with self.assertRaisesRegex(ValueError, "can no longer run"):
            self.apply(confirm=p["confirm"])
        self.assertEqual(self.cfn.executed, [])

    def test_an_older_stack_without_the_role_waits_for_one_execute_in_aws(self):
        env = dict(ENV)
        u.begin(self.store, env, NOW, "v1.1.0", "owner@example.org", "update")
        self.assertNotIn("RoleARN", self.cfn.created[0])
        self.cfn.cs = {"Status": "CREATE_COMPLETE", "ExecutionStatus": "AVAILABLE"}
        self.assertEqual(u.native_poll(self.store, env, NOW)["native"], "review")
        p = u.handle({"action": "status"}, env, self.store, NOW, fetch=lambda repo: [])["native"]["pending"]
        self.assertFalse(p["can_apply"])
        self.assertIn("no deploy role yet", p["hold"])
        with self.assertRaisesRegex(ValueError, "no deploy role yet"):
            u.handle({"action": "apply_update", "actor": "a"}, env, self.store, NOW)
        self.assertEqual(self.cfn.executed, [])

    def test_nothing_waiting_is_refused(self):
        with self.assertRaisesRegex(ValueError, "no prepared update"):
            self.apply()

    def test_a_new_update_replaces_the_one_waiting(self):
        self.prepare(api=[{"ResourceChange": {"Action": "Add", "ResourceType": "AWS::IAM::Role", "LogicalResourceId": "NewRole"}}])
        old = self.pending()["id"]
        self.cfn.nested = {}
        later = NOW.replace(minute=30)
        u.begin(self.store, self.env, later, "v1.1.0", "owner@example.org", "update")
        self.assertEqual(self.cfn.deleted, ["arn:cs/1"])
        self.assertEqual(self.store.get(old)["status"], "superseded")
        self.assertIn("nothing was changed", self.store.get(old)["reason"])
        self.assertNotIn("review_id", self.store.get("UPDATE_STATE"))


class PermissionChanges(unittest.TestCase):
    """Auto-update never applies a change to who may do what, in the parent
    or any part: an admin reads it first. Read with property values, every
    page of every nested change set."""

    def setUp(self):
        self.store = u.Store(FakeDynamo(), "t")
        self.cfn = FakeCfn()
        u._clients["cloudformation"] = self.cfn
        u._clients["lambda"] = FakeLambda()
        self._fetch, self._notify = u.fetch_template, u.notify
        u.fetch_template = lambda url, opener=None: TEMPLATE
        u.notify = lambda *a, **k: None
        self.env = dict(ENV, DEPLOY_ROLE_ARN="arn:aws:iam::1:role/coderhelm-prod-deploy-stack")
        self.store.put("MANIFEST#v1.1.0", {"verified": True, "manifest": None, "template_sha256": SHA})

    def tearDown(self):
        u.fetch_template, u.notify = self._fetch, self._notify
        u._clients.pop("cloudformation", None)
        u._clients.pop("lambda", None)

    def poll_with(self, *api_pages):
        self.cfn.nested["nested-api"] = list(api_pages)
        u.begin(self.store, self.env, NOW, "v1.1.0", "auto", "update")
        self.cfn.cs = {"Status": "CREATE_COMPLETE", "ExecutionStatus": "AVAILABLE"}
        return u.native_poll(self.store, self.env, NOW)

    @staticmethod
    def change(t, action="Modify", before=None, after=None, name="Thing"):
        rc = {"Action": action, "ResourceType": t, "LogicalResourceId": name, "Replacement": "False"}
        if before is not None:
            rc["BeforeContext"] = json.dumps({"Properties": before})
        if after is not None:
            rc["AfterContext"] = json.dumps({"Properties": after})
        return {"ResourceChange": rc}

    def test_a_new_or_changed_role_in_a_part_is_held(self):
        r = self.poll_with({"Changes": [self.change("AWS::IAM::Role", "Add", name="NewRole")]})
        self.assertEqual(r["native"], "review")
        self.assertEqual(self.cfn.executed, [])

    def test_a_resource_policy_property_is_held(self):
        r = self.poll_with({"Changes": [self.change("AWS::KMS::Key", before={"KeyPolicy": {"a": 1}}, after={"KeyPolicy": {"a": 2}})]})
        self.assertEqual(r["native"], "review")
        self.assertEqual(self.cfn.executed, [])

    def test_a_function_permission_is_held(self):
        r = self.poll_with({"Changes": [self.change("AWS::Lambda::Permission", "Add")]})
        self.assertEqual(r["native"], "review")

    def test_a_change_on_the_second_page_is_found(self):
        r = self.poll_with({"Changes": [self.change("AWS::SQS::Queue")]}, {"Changes": [self.change("AWS::IAM::Policy", name="Late")]})
        self.assertEqual(r["native"], "review")
        self.assertIn(("nested-api", True, "1"), self.cfn.described)

    def test_every_page_is_read_with_property_values(self):
        self.poll_with({"Changes": []})
        # The parent's change set and the part's, each read with its before and after values.
        read = {cid for cid, include, _ in self.cfn.described if include}
        self.assertEqual(read, {"arn:cs/1", "nested-api"})

    def test_an_ordinary_change_still_applies_by_itself(self):
        r = self.poll_with({"Changes": [self.change("AWS::Lambda::Function", before={"MemorySize": 512}, after={"MemorySize": 1024})]})
        self.assertEqual(r["native"], "executing")
        self.assertEqual(self.cfn.executed, ["arn:cs/1"])

    def test_principals(self):
        same = {"Principal": {"Service": "lambda.amazonaws.com"}}
        self.assertEqual(u.permission_change({"Action": "Modify", "ResourceType": "AWS::X::Y",
                                              "BeforeContext": json.dumps({"Properties": {}}),
                                              "AfterContext": json.dumps({"Properties": {"Statement": [same]}})}), "")
        why = u.permission_change({"Action": "Modify", "ResourceType": "AWS::X::Y",
                                   "BeforeContext": json.dumps({"Properties": {}}),
                                   "AfterContext": json.dumps({"Properties": {"Statement": [{"Principal": {"AWS": "arn:aws:iam::999:root"}}]}})})
        self.assertIn("AWS:arn:aws:iam::999:root", why)
        self.assertIn("*:*", u.permission_change({"Action": "Add", "ResourceType": "AWS::X::Y", "AfterContext": {"Properties": {"S": [{"Principal": "*"}]}}}))
        # A removal never widens anything.
        self.assertEqual(u.permission_change({"Action": "Remove", "ResourceType": "AWS::IAM::Role"}), "")


class FakeSsmParam:
    def __init__(self):
        self.value = None
        self.puts = []

    def get_parameter(self, Name):
        if self.value is None:
            raise type("ParameterNotFound", (Exception,), {})("not found")
        return {"Parameter": {"Value": self.value}}

    def put_parameter(self, Name, Value, Type, Overwrite, Description):
        self.puts.append(Name)
        self.value = Value

    def delete_parameter(self, Name):
        if self.value is None:
            raise type("ParameterNotFound", (Exception,), {})("not found")
        self.value = None


class UninstallMode(unittest.TestCase):
    """Settings, System, Uninstall: keep the data (default) or delete
    everything when the stack is deleted. On needs the install's name typed
    back; every change records who and when."""

    def setUp(self):
        self.env = dict(ENV, RESOURCE_PREFIX="coderhelm-prod")
        self.store = u.Store(FakeDynamo(), "t")
        self.ssm = FakeSsmParam()
        self.now = datetime(2026, 11, 1, tzinfo=timezone.utc)

    def test_on_needs_the_typed_name_then_writes_the_switch(self):
        r = u.set_uninstall_mode(self.store, self.env, self.now, {"delete_data": True, "confirm": "staging", "actor": "a@x.org"}, ssm=self.ssm)
        self.assertEqual(r["ok"], False)
        self.assertIn("(prod)", r["error"])
        self.assertIsNone(self.ssm.value)
        r = u.set_uninstall_mode(self.store, self.env, self.now, {"delete_data": True, "confirm": "prod", "actor": "a@x.org"}, ssm=self.ssm)
        self.assertTrue(r["ok"], r)
        self.assertEqual(self.ssm.puts, ["/coderhelm-prod/uninstall/delete-data"])
        self.assertEqual(self.ssm.value, "delete")
        self.assertEqual(r["uninstall"], {"delete_data": True, "changed_by": "a@x.org", "changed_at": self.now.isoformat()})

    def test_off_removes_it_and_both_changes_are_recorded(self):
        u.set_uninstall_mode(self.store, self.env, self.now, {"delete_data": True, "confirm": "prod", "actor": "a@x.org"}, ssm=self.ssm)
        r = u.set_uninstall_mode(self.store, self.env, self.now, {"delete_data": False, "actor": "b@x.org"}, ssm=self.ssm)
        self.assertTrue(r["ok"])
        self.assertIsNone(self.ssm.value)
        self.assertEqual(r["uninstall"]["delete_data"], False)
        log = self.store.get("UNINSTALL_LOG")
        self.assertEqual([(x["delete_data"], x["actor"]) for x in log], [(False, "b@x.org"), (True, "a@x.org")])
        # Off again when already off is fine.
        self.assertTrue(u.set_uninstall_mode(self.store, self.env, self.now, {"delete_data": False, "actor": "b@x.org"}, ssm=self.ssm)["ok"])

    def test_only_a_bool_and_only_on_a_one_stack_install(self):
        self.assertFalse(u.set_uninstall_mode(self.store, self.env, self.now, {"delete_data": "yes", "confirm": "prod"}, ssm=self.ssm)["ok"])
        env = dict(self.env)
        env.pop("NATIVE_STACK")
        self.assertFalse(u.set_uninstall_mode(self.store, env, self.now, {"delete_data": True, "confirm": "prod"}, ssm=self.ssm)["ok"])
        self.assertIsNone(self.ssm.value)

    def test_status_says_keep_by_default(self):
        self.assertEqual(u.uninstall_status(self.store, self.env, ssm=self.ssm), {"delete_data": False, "changed_by": None, "changed_at": None})


class ManyChangeSets(FakeCfn):
    """CloudFormation with one record per change set: each create gets its
    own id, and each id its own Status and ExecutionStatus."""

    def __init__(self):
        super().__init__()
        self.made = 0
        self.per = {}
        self.outputs = []

    def create_change_set(self, **kw):
        self.created.append(kw)
        self.made += 1
        cs_id = f"arn:cs/{self.made}"
        self.per[cs_id] = {"Status": "CREATE_PENDING", "ExecutionStatus": "UNAVAILABLE"}
        return {"Id": cs_id}

    def describe_change_set(self, ChangeSetName, StackName=None, IncludePropertyValues=False, NextToken=None):
        if ChangeSetName in self.per:
            if self.per[ChangeSetName] is None:
                raise Exception(f"ChangeSet [{ChangeSetName}] does not exist")
            self.cs = self.per[ChangeSetName]
        return super().describe_change_set(ChangeSetName, StackName, IncludePropertyValues, NextToken)

    def delete_change_set(self, ChangeSetName, StackName):
        super().delete_change_set(ChangeSetName, StackName)
        self.per[ChangeSetName] = None

    def execute_change_set(self, ChangeSetName, StackName):
        super().execute_change_set(ChangeSetName, StackName)
        self.per[ChangeSetName] = {"Status": "CREATE_COMPLETE", "ExecutionStatus": "EXECUTE_IN_PROGRESS"}

    def describe_stacks(self, StackName):
        out = super().describe_stacks(StackName)
        out["Stacks"][0]["Outputs"] = self.outputs
        return out


IAM_CHANGE = [{"ResourceChange": {"Action": "Add", "ResourceType": "AWS::IAM::Role", "LogicalResourceId": "NewRole"}}]


class OnePendingUpdate(unittest.TestCase):
    """Only one update is ever pending: preparing a newer one supersedes an
    older prepared-but-unapplied one (its change set deleted, history keeps
    it as superseded), and the version route returns that one. What
    CloudFormation says decides what the route reports."""

    def setUp(self):
        self.store = u.Store(FakeDynamo(), "t")
        self.cfn = ManyChangeSets()
        self.cfn.nested["nested-api"] = {"Changes": IAM_CHANGE}
        u._clients["cloudformation"] = self.cfn
        u._clients["lambda"] = FakeLambda()
        self._fetch, self._notify = u.fetch_template, u.notify
        u.fetch_template = lambda url, opener=None: TEMPLATE
        self.sent = []
        u.notify = lambda *a, **k: self.sent.append(a[0])
        self.env = dict(ENV, DEPLOY_ROLE_ARN=ROLE, CODERHELM_VERSION="v0.8.32")
        for v in ("v0.8.37", "v0.8.39"):
            self.store.put(f"MANIFEST#{v}", {"verified": True, "manifest": None, "template_sha256": SHA})

    def tearDown(self):
        u.fetch_template, u.notify = self._fetch, self._notify
        u._clients.pop("cloudformation", None)
        u._clients.pop("lambda", None)

    def status(self, at=NOW):
        return u.handle({"action": "status"}, self.env, self.store, at, fetch=lambda repo: [
            {"tag_name": "v0.8.37"}, {"tag_name": "v0.8.39"}])

    def prepared(self, version, at):
        """An admin's Update to `version`, prepared: it waits for the OK (it changes IAM)."""
        e = u.begin(self.store, self.env, at, version, "owner@example.org", "update")
        self.cfn.per[e["change_set_id"]] = {"Status": "CREATE_COMPLETE", "ExecutionStatus": "AVAILABLE"}
        self.assertEqual(u.native_poll(self.store, self.env, at)["native"], "review")
        return e

    def test_a_newer_prepared_update_supersedes_the_older_one(self):
        old = self.prepared("v0.8.37", NOW)
        new = self.prepared("v0.8.39", NOW.replace(minute=20))
        # The older change set never ran: deleted, and kept in history as superseded.
        self.assertEqual(self.cfn.deleted, [old["change_set_id"]])
        row = self.store.get(old["id"])
        self.assertEqual(row["status"], "superseded")
        self.assertEqual(row["superseded_by"], new["id"])
        self.assertIn("replaced by the update to v0.8.39", row["reason"])
        s = self.status(NOW.replace(minute=21))
        p = s["native"]["pending"]
        self.assertEqual((p["version"], p["status"], p["id"]), ("v0.8.39", "review", new["id"]))
        self.assertTrue(p["permission_changes"])
        self.assertEqual([h["status"] for h in s["history"]], ["review", "superseded"])

    def test_an_older_prepared_update_left_open_is_superseded_by_the_newest(self):
        # What an older updater left: the state points at v0.8.37 waiting for
        # an OK, while a newer v0.8.39 change set was prepared and sits open.
        old = self.prepared("v0.8.37", NOW)
        self.cfn.per["arn:cs/2"] = {"Status": "CREATE_COMPLETE", "ExecutionStatus": "AVAILABLE"}
        newer = "UPDATE#2026-10-31T12:20:00Z#bbbbbbbb"
        self.store.put(newer, {"version": "v0.8.39", "from_version": "v0.8.32", "actor": "owner@example.org", "mode": "update",
                               "status": "review", "started_at": NOW.replace(minute=20).isoformat(), "change_set_id": "arn:cs/2",
                               "apply": True, "role_arn": ROLE, "template_canonical": u.canonical_sha(json.loads(TEMPLATE))})
        s = self.status(NOW.replace(minute=25))
        self.assertEqual(s["native"]["pending"]["version"], "v0.8.39")
        self.assertEqual(s["native"]["pending"]["id"], newer)
        self.assertEqual(self.store.get(old["id"])["status"], "superseded")
        self.assertEqual(self.store.get("UPDATE_STATE")["review_id"], newer)
        self.assertIn(old["change_set_id"], self.cfn.deleted)
        statuses = {h["id"]: h["status"] for h in s["history"]}
        self.assertEqual(statuses, {newer: "review", old["id"]: "superseded"})

    def test_history_never_shows_two_updates_waiting(self):
        old = self.prepared("v0.8.37", NOW)
        stray = dict(self.store.get(old["id"]))
        self.prepared("v0.8.39", NOW.replace(minute=20))
        # A lost race wrote the older row back as it was.
        self.store.put(old["id"], stray)
        rows = u.history_view(self.store.history(), self.store.get("UPDATE_STATE"), NOW)
        self.assertEqual([r["status"] for r in rows], ["review", "superseded"])

    def test_one_still_being_prepared_is_not_replaced_mid_way(self):
        u.begin(self.store, self.env, NOW, "v0.8.37", "owner@example.org", "update")
        with self.assertRaisesRegex(ValueError, "still being prepared"):
            u.begin(self.store, self.env, NOW.replace(minute=1), "v0.8.39", "owner@example.org", "update")
        self.assertEqual(self.cfn.deleted, [])

    def test_a_change_set_executed_in_aws_is_never_replaced(self):
        old = self.prepared("v0.8.37", NOW)
        self.cfn.per[old["change_set_id"]] = {"Status": "CREATE_COMPLETE", "ExecutionStatus": "EXECUTE_IN_PROGRESS"}
        with self.assertRaisesRegex(ValueError, "already being applied"):
            u.begin(self.store, self.env, NOW.replace(minute=20), "v0.8.39", "owner@example.org", "update")
        self.assertEqual(self.cfn.deleted, [])

    def test_preparing_then_review_without_waiting_for_the_poll(self):
        e = u.begin(self.store, self.env, NOW, "v0.8.39", "owner@example.org", "update")
        self.assertEqual(self.status()["native"]["pending"]["status"], "preparing")
        # The change set is ready and holds an IAM change: the route says so at once.
        self.cfn.per[e["change_set_id"]] = {"Status": "CREATE_COMPLETE", "ExecutionStatus": "AVAILABLE"}
        p = self.status(NOW.replace(minute=2))["native"]["pending"]
        self.assertEqual(p["status"], "review")
        self.assertTrue(p["can_apply"])
        self.assertTrue(p["confirm"])
        self.assertEqual(p["actor"], "owner@example.org")
        self.assertEqual(self.cfn.executed, [])

    def test_executed_in_the_console_reads_as_applying_then_updated(self):
        e = self.prepared("v0.8.39", NOW)
        # Someone executed the change set in the AWS console.
        self.cfn.per[e["change_set_id"]] = {"Status": "CREATE_COMPLETE", "ExecutionStatus": "EXECUTE_IN_PROGRESS"}
        self.cfn.stack_status = "UPDATE_IN_PROGRESS"
        s = self.status(NOW.replace(minute=10))
        p = s["native"]["pending"]
        self.assertEqual(p["status"], "running")
        self.assertEqual(p["applied_by"], u.CONSOLE_ACTOR)
        self.assertNotIn("confirm", p)
        self.assertTrue(s["running"])
        self.assertNotIn("review_id", self.store.get("UPDATE_STATE"))
        # Done: the stack reports the new release.
        self.cfn.per[e["change_set_id"]] = {"Status": "CREATE_COMPLETE", "ExecutionStatus": "EXECUTE_COMPLETE"}
        self.cfn.stack_status = "UPDATE_COMPLETE"
        self.cfn.outputs = [{"OutputKey": "Version", "OutputValue": "v0.8.39"}]
        s = self.status(NOW.replace(minute=25))
        self.assertIsNone(s["native"]["pending"])
        self.assertFalse(s["running"])
        self.assertEqual(s["history"][0]["status"], "succeeded")

    def test_executed_in_the_console_and_rolled_back_says_why(self):
        e = self.prepared("v0.8.39", NOW)
        self.cfn.per[e["change_set_id"]] = {"Status": "CREATE_COMPLETE", "ExecutionStatus": "EXECUTE_FAILED"}
        self.cfn.stack_status = "UPDATE_ROLLBACK_COMPLETE"
        self.cfn.events = [{"LogicalResourceId": "Worker", "ResourceStatus": "UPDATE_FAILED", "ResourceStatusReason": "Resource limit exceeded",
                            "Timestamp": NOW.replace(minute=11).isoformat()}]
        s = self.status(NOW.replace(minute=15))
        self.assertIsNone(s["native"]["pending"])
        h = s["history"][0]
        self.assertEqual(h["status"], "rolled_back")
        self.assertIn("stayed on v0.8.32", h["summary"])
        self.assertIn("Resource limit exceeded", h["technical"])

    def test_a_stack_that_reports_another_release_is_not_a_success(self):
        e = self.prepared("v0.8.39", NOW)
        self.cfn.per[e["change_set_id"]] = {"Status": "CREATE_COMPLETE", "ExecutionStatus": "EXECUTE_COMPLETE"}
        self.cfn.outputs = [{"OutputKey": "Version", "OutputValue": "v0.8.37"}]
        self.assertEqual(self.status()["history"][0]["status"], "failed")

    def test_just_applied_never_goes_back_to_waiting_for_an_ok(self):
        e = self.prepared("v0.8.39", NOW)
        token = self.status()["native"]["pending"]["confirm"]
        self.assertEqual(u.handle({"action": "apply_update", "actor": "owner@example.org", "confirm": token},
                                  self.env, self.store, NOW)["status"], "running")
        # CloudFormation still answers AVAILABLE for a moment.
        self.cfn.per[e["change_set_id"]] = {"Status": "CREATE_COMPLETE", "ExecutionStatus": "AVAILABLE"}
        self.assertEqual(self.status()["native"]["pending"]["status"], "running")

    def test_a_poll_never_writes_over_a_newer_state(self):
        e = u.begin(self.store, self.env, NOW, "v0.8.39", "owner@example.org", "update")
        self.cfn.per[e["change_set_id"]] = {"Status": "CREATE_IN_PROGRESS", "ExecutionStatus": "UNAVAILABLE"}
        real = self.cfn.describe_change_set

        def meanwhile(*a, **k):
            # A newer update replaced this one while the poll read CloudFormation.
            row = self.store.get(e["id"])
            self.store.put(e["id"], dict(row, status="superseded"))
            return real(*a, **k)
        self.cfn.describe_change_set = meanwhile
        u.native_poll(self.store, self.env, NOW)
        self.assertEqual(self.store.get(e["id"])["status"], "superseded")


class ModelCheckTruth(unittest.TestCase):
    """The version route's model check is pending only while the release
    check really runs: it clears once the release is verified, and a check
    that stopped reporting shows as stalled, never as Checking forever."""

    def setUp(self):
        self.store = u.Store(FakeDynamo(), "t")
        self.cb = FakeCodeBuild()
        u._clients["codebuild"] = self.cb
        self.env = dict(ENV, CODERHELM_VERSION="v0.8.32", RELEASE_CHECK_PROJECT_NAME="coderhelm-prod-release-check")
        self.started = []
        self._check, self._notify = u.start_check, u.notify
        u.start_check = lambda env, version: self.started.append(version) or f"c-{len(self.started)}"
        u.notify = lambda *a, **k: None

    def tearDown(self):
        u.start_check, u.notify = self._check, self._notify
        u._clients.pop("codebuild", None)

    def status(self, at):
        return u.handle({"action": "status"}, self.env, self.store, at, fetch=lambda repo: [{"tag_name": "v0.8.39"}])

    def pending(self, at=NOW):
        return u.model_check(self.store, self.env, at, "v0.8.39", u.RELEASE_REPO)

    def test_pending_while_the_release_check_runs_with_its_progress(self):
        self.pending()
        self.cb.builds["c-1"] = {"id": "c-1", "buildStatus": "IN_PROGRESS"}
        mc = self.status(NOW.replace(minute=4))["model_check"]
        self.assertTrue(mc["pending"])
        self.assertEqual(mc["progress"], {"step": "signature", "started_at": NOW.isoformat(), "build": "IN_PROGRESS"})

    def test_a_verified_release_is_never_still_checking(self):
        self.pending()
        # The release was verified (and an update prepared from it), the record left behind.
        self.store.put("MANIFEST#v0.8.39", {"verified": True, "manifest": None, "template_sha256": SHA})
        mc = self.status(NOW.replace(minute=6))["model_check"]
        self.assertFalse(mc.get("pending"))
        self.assertTrue(mc["ok"])
        self.assertFalse(self.store.get("MODEL_CHECK").get("pending"))

    def test_a_build_that_ended_without_reporting_is_stalled_and_restarts(self):
        self.pending()
        self.cb.builds["c-1"] = {"id": "c-1", "buildStatus": "FAILED"}
        mc = self.status(NOW.replace(minute=5))["model_check"]
        self.assertFalse(mc["pending"])
        self.assertTrue(mc["stalled"])
        self.assertIn("ended FAILED", mc["note"])
        # It still holds the update.
        self.assertTrue(u.blocking(self.store.get("MODEL_CHECK"), "v0.8.39"))
        # Check again starts it over at once, not 30 minutes later.
        u.handle({"action": "model_check", "version": "v0.8.39"}, self.env, self.store, NOW.replace(minute=6))
        self.assertEqual(self.started, ["v0.8.39", "v0.8.39"])

    def test_a_check_past_its_time_bound_is_stalled(self):
        self.pending()
        mc = self.status(NOW + u.CHECK_LIMIT + u.timedelta(minutes=1))["model_check"]
        self.assertTrue(mc["stalled"])
        self.assertIn("within 30 minutes", mc["note"])

    def test_a_failed_release_check_says_so_and_holds(self):
        self.pending()
        u.handle({"action": "models_verified", "version": "v0.8.39", "build": "c-1", "status": "failed", "reason": "bad signature"},
                 self.env, self.store, NOW.replace(minute=3))
        rec = self.store.get("MODEL_CHECK")
        self.assertFalse(rec["pending"])
        self.assertTrue(rec["stalled"])
        self.assertIn("bad signature", rec["note"])
        self.assertTrue(u.blocking(rec, "v0.8.39"))
