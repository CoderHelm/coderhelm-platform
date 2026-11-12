"""What a native install reads in its own account (infrastructure
vulnerabilities, Amazon Inspector findings, AWS Health events) is three
parameters of its stack. An admin switches one from the AWS card: the
updater makes a change set on the running release that changes only those
parameters, and it goes the way a version update goes: the deploy role's
preflight, the review of what it changes about permissions, the admin's
confirmation, the history row, and CloudFormation's own rollback.

Run: python3 -m unittest discover -s lambda/tests
"""

import unittest

from test_updater_native import ENV, NOW, FakeCfn, FakeDynamo, FakeIam, FakeLambda, u

ROLE = "arn:aws:iam::111122223333:role/coderhelm-prod-deploy-stack"
DEPLOYED = {
    "Parameters": {"Stage": {}, "AdminEmail": {}, "InfrastructureVulnerabilities": {}, "InspectorFindings": {}, "HealthEvents": {}},
    "Resources": {"Api": {"Type": "AWS::CloudFormation::Stack"}},
}
# The gateway's own read policy, as a nested change set lists the switch.
POLICY_CHANGE = {"ResourceChange": {
    "Action": "Modify", "ResourceType": "AWS::IAM::ManagedPolicy", "LogicalResourceId": "GatewayOwnAccountRead",
    "PhysicalResourceId": "arn:aws:iam::111122223333:policy/coderhelm-prod-GatewayOwnAccountRead-aa934871",
    "Replacement": "False",
}}


class InstallCfn(FakeCfn):
    """The install's stack, with the three switches as its release declares them."""

    def __init__(self, options=None):
        super().__init__()
        self.options = {"InfrastructureVulnerabilities": "on", "InspectorFindings": "off", "HealthEvents": "off"} if options is None else options
        self.deployed = DEPLOYED
        self.fetched = DEPLOYED
        self.nested["nested-api"] = {"Changes": [POLICY_CHANGE]}

    def describe_stacks(self, StackName):
        params = [{"ParameterKey": "Stage", "ParameterValue": "prod"}, {"ParameterKey": "AdminEmail", "ParameterValue": "owner@example.org"}]
        params += [{"ParameterKey": k, "ParameterValue": v} for k, v in self.options.items()]
        return {"Stacks": [{"StackId": "arn:aws:cloudformation:us-east-1:111122223333:stack/coderhelm-prod/abc",
                            "StackStatus": self.stack_status, "Parameters": params,
                            "Outputs": [{"OutputKey": "Version", "OutputValue": "v1.0.0"}]}]}


class Options(unittest.TestCase):
    def setUp(self):
        self.store = u.Store(FakeDynamo(), "t")
        self.cfn = InstallCfn()
        u._clients["cloudformation"] = self.cfn
        self.lam = FakeLambda()
        u._clients["lambda"] = self.lam
        self._iam = u._clients.get("iam")
        u._clients["iam"] = FakeIam()
        self._notify = u.notify
        self.sent = []
        u.notify = lambda *a, **k: self.sent.append(a[0])
        self.env = dict(ENV, DEPLOY_ROLE_ARN=ROLE)

    def tearDown(self):
        u.notify = self._notify
        u._clients["iam"] = self._iam
        u._clients.pop("cloudformation", None)
        u._clients.pop("lambda", None)

    def switch(self, **options):
        return u.handle({"action": "set_options", "options": options, "actor": "owner@example.org"}, self.env, self.store, NOW)

    def poll(self, execution="AVAILABLE"):
        self.cfn.cs = {"Status": "CREATE_COMPLETE", "ExecutionStatus": execution}
        return u.native_poll(self.store, self.env, NOW)

    def status(self):
        return u.handle({"action": "status"}, self.env, self.store, NOW, fetch=lambda repo: [])

    def test_a_parameter_only_change_set_on_the_running_release(self):
        started = self.switch(InspectorFindings="on")
        self.assertEqual(started["mode"], "options")
        self.assertEqual(started["version"], "v1.0.0")
        self.assertEqual(started["options"], {"InspectorFindings": {"from": "off", "to": "on"}})
        kw = self.cfn.created[0]
        # The running release's template, exactly: no template URL, nothing staged.
        self.assertTrue(kw["UsePreviousTemplate"])
        self.assertNotIn("TemplateURL", kw)
        self.assertEqual(self.lam.calls, [])
        self.assertEqual(kw["RoleARN"], ROLE)
        self.assertTrue(kw["IncludeNestedStacks"])
        self.assertIn("turn on Amazon Inspector findings", kw["Description"])
        params = {p["ParameterKey"]: p for p in kw["Parameters"]}
        self.assertEqual(params["InspectorFindings"], {"ParameterKey": "InspectorFindings", "ParameterValue": "on"})
        for k in ("Stage", "AdminEmail", "InfrastructureVulnerabilities", "HealthEvents"):
            self.assertEqual(params[k], {"ParameterKey": k, "UsePreviousValue": True}, k)

    def test_the_review_lists_the_permission_change_and_waits_for_the_confirmation(self):
        self.switch(InspectorFindings="on")
        self.assertEqual(self.poll()["native"], "review")
        # Never applied on the press alone: who may do what changes.
        self.assertEqual(self.cfn.executed, [])
        pending = self.status()["native"]["pending"]
        self.assertEqual(pending["mode"], "options")
        self.assertEqual(pending["options"], {"InspectorFindings": {"from": "off", "to": "on"}})
        self.assertEqual(pending["preflight"], [])
        self.assertIn("Api.GatewayOwnAccountRead", pending["permission_changes"][0])
        self.assertIn("waiting for you to confirm: it changes permissions", pending["hold"])
        self.assertTrue(pending["can_apply"])
        self.assertRegex(pending["confirm"], r"^[0-9a-f]{64}$")
        # The card asks the admin; no email for a switch they just pressed.
        self.assertEqual(self.sent, [])
        with self.assertRaisesRegex(ValueError, "confirm the changes"):
            u.handle({"action": "apply_update", "actor": "owner@example.org"}, self.env, self.store, NOW)
        e = u.handle({"action": "apply_update", "actor": "owner@example.org", "history_id": pending["id"], "confirm": pending["confirm"]},
                     self.env, self.store, NOW)
        self.assertEqual(e["status"], "running")
        self.assertEqual(self.cfn.executed, ["arn:cs/1"])
        # Who confirmed exactly which change, kept with the row (the audit).
        self.assertEqual(e["confirmed"]["by"], "owner@example.org")
        self.assertIn("Api.GatewayOwnAccountRead", e["confirmed"]["permission_changes"][0])
        self.assertEqual(self.poll("EXECUTE_COMPLETE")["status"], "succeeded")
        row = self.status()["history"][0]
        self.assertEqual((row["mode"], row["status"], row["actor"], row["applied_by"]), ("options", "succeeded", "owner@example.org", "owner@example.org"))
        self.assertEqual(row["options"], {"InspectorFindings": {"from": "off", "to": "on"}})
        # A switch says nothing about the release: never marked as a version that failed or ran.
        state = self.store.get("UPDATE_STATE")
        self.assertNotIn("running_id", state)
        self.assertEqual(state.get("failed_versions") or [], [])

    def test_a_failed_switch_rolls_back_and_says_so_without_marking_the_release(self):
        self.switch(HealthEvents="on")
        self.poll()
        p = self.status()["native"]["pending"]
        u.handle({"action": "apply_update", "actor": "owner@example.org", "confirm": p["confirm"]}, self.env, self.store, NOW)
        self.cfn.stack_status = "UPDATE_ROLLBACK_COMPLETE"
        self.cfn.events = [{"LogicalResourceId": "Api", "ResourceStatus": "UPDATE_FAILED", "ResourceStatusReason": "Embedded stack failed",
                            "Timestamp": NOW.isoformat()}]
        r = self.poll("EXECUTE_COMPLETE")
        self.assertEqual(r["status"], "rolled_back")
        row = self.status()["history"][0]
        self.assertEqual(row["status"], "rolled_back")
        self.assertIn("AWS put everything back", row["summary"])
        self.assertIn("Embedded stack failed", row["technical"])
        self.assertEqual(self.store.get("UPDATE_STATE").get("failed_versions") or [], [])
        # The next press starts again from what the stack has.
        self.cfn.stack_status = "UPDATE_COMPLETE"
        self.assertEqual(self.switch(HealthEvents="on")["status"], "preparing")

    def test_the_deploy_roles_preflight_stops_a_switch_it_would_be_refused(self):
        u._clients["iam"] = FakeIam(deploy={"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": "cloudformation:*", "Resource": "*"}]})
        self.switch(InspectorFindings="on")
        r = self.poll()
        self.assertEqual(r["status"], "failed")
        self.assertEqual(self.cfn.executed, [])
        self.assertEqual(self.cfn.deleted, ["arn:cs/1"])
        row = self.status()["history"][0]
        self.assertEqual(row["mode"], "options")
        self.assertIn(u.PREFLIGHT_MARK, row["technical"])
        self.assertIn("Nothing changed", row["summary"])

    def test_the_status_reports_each_switch_as_the_stack_has_it(self):
        self.assertEqual(self.status()["native"]["options"],
                         {"InfrastructureVulnerabilities": "on", "InspectorFindings": "off", "HealthEvents": "off"})
        # A release before the switches: none of them yet.
        self.cfn.options = {}
        self.assertEqual(self.status()["native"]["options"],
                         {"InfrastructureVulnerabilities": None, "InspectorFindings": None, "HealthEvents": None})

    def test_refusals_say_why_and_change_nothing(self):
        for bad, why in (({}, "at least one"), ({"Hostname": "on"}, "not a switch"), ({"InspectorFindings": "yes"}, "on or off")):
            with self.assertRaisesRegex(ValueError, why):
                u.handle({"action": "set_options", "options": bad}, self.env, self.store, NOW)
        with self.assertRaisesRegex(ValueError, "already set that way"):
            self.switch(InfrastructureVulnerabilities="on")
        self.cfn.options = {}
        with self.assertRaisesRegex(ValueError, "arrives with the next version update"):
            self.switch(InspectorFindings="on")
        self.assertEqual(self.cfn.created, [])
        # Refused before anything changed: kept in history as not started, never as failed.
        rows = self.status()["history"]
        self.assertTrue(rows and all(r["status"] == "not_started" for r in rows))
        with self.assertRaisesRegex(ValueError, "one-stack install"):
            u.handle({"action": "set_options", "options": {"InspectorFindings": "on"}}, {"CODERHELM_VERSION": "v1.0.0"}, self.store, NOW)

    def test_a_version_update_waiting_for_the_admin_is_never_replaced_by_a_switch(self):
        self.store.put("MANIFEST#v1.1.0", {"verified": True, "manifest": None, "template_sha256": "0" * 64})
        hid = "UPDATE#2026-10-31T11:00:00Z#abcd1234"
        self.store.put(hid, {"version": "v1.1.0", "mode": "update", "status": "review", "change_set_id": "arn:cs/update"})
        self.store.put("UPDATE_STATE", {"review_id": hid})
        with self.assertRaisesRegex(ValueError, "waiting for your OK"):
            self.switch(InspectorFindings="on")
        self.assertEqual(self.cfn.created, [])
        self.assertEqual(self.store.get(hid)["status"], "review")

    def test_a_switch_waiting_for_review_is_replaced_by_a_version_update(self):
        self.switch(InspectorFindings="on")
        self.poll()
        old = self.status()["native"]["pending"]["id"]
        self.cfn.cs = {"Status": "CREATE_PENDING", "ExecutionStatus": "UNAVAILABLE"}
        u.supersede_open(self.store, self.env, self.store.get("UPDATE_STATE"), NOW, "UPDATE#later", "v1.1.0")
        self.assertEqual(self.store.get(old)["status"], "superseded")


if __name__ == "__main__":
    unittest.main()
