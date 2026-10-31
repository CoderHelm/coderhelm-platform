"""A native install's updates: a verified release becomes a change set on the
parent stack; it waits for an admin, or runs through the deploy role with
auto-update on, and its end is reported in CloudFormation's own words.

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

    def describe_change_set(self, ChangeSetName, StackName=None):
        if ChangeSetName in self.nested:
            return self.nested[ChangeSetName]
        return dict({"ChangeSetName": "cs", "ChangeSetId": ChangeSetName, "StackId": "arn:stack/coderhelm-prod/abc", "Changes": [
            {"ResourceChange": {"Action": "Modify", "LogicalResourceId": "Api", "ResourceType": "AWS::CloudFormation::Stack", "Replacement": "False", "ChangeSetId": "nested-api"}},
            {"ResourceChange": {"Action": "Add", "LogicalResourceId": "NewThing", "ResourceType": "AWS::SQS::Queue"}},
        ]}, **self.cs)

    def execute_change_set(self, ChangeSetName, StackName):
        self.executed.append(ChangeSetName)

    def describe_stack_events(self, StackName):
        return {"StackEvents": self.events}


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
        self.assertIn("Api: Embedded stack", hist[0]["reason"])

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
        self.assertIn("not the verified one", self.store.history()[0]["reason"])

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
        u._clients["lambda"] = None
        self._mc = u.model_check
        u.model_check = lambda *a, **k: {}
        u.notify_blocked = lambda *a, **k: None
        try:
            u.handle({"action": "models_verified", "version": "v1.1.0", "status": "verified", "manifest": None, "template_sha256": SHA}, ENV, self.store, NOW)
        finally:
            u.model_check = self._mc
        self.assertEqual(self.store.get("MANIFEST#v1.1.0")["template_sha256"], SHA)
        with self.assertRaises(ValueError):
            u.handle({"action": "models_verified", "version": "v1.1.0", "status": "verified", "template_sha256": "nope"}, ENV, self.store, NOW)

    def test_only_release_urls_are_fetched(self):
        with self.assertRaises(ValueError):
            self._fetch("https://evil.example.com/launch/v1.0.0/coderhelm.json")
        self.assertEqual(self._fetch("https://coderhelm-public.s3.amazonaws.com/launch/v1.0.0/coderhelm.json", opener_for(b"{}")), b"{}")


if __name__ == "__main__":
    unittest.main()


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
