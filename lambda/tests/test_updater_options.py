"""What a native install reads in its own account (infrastructure
vulnerabilities, Amazon Inspector findings, AWS Health events) is three
parameters of its stack. An admin switches one from the AWS card: the
updater makes a change set on the running release that changes only those
parameters, and it goes the way a version update goes: the deploy role's
preflight, the review of what it changes about permissions, the admin's
confirmation, the history row, and CloudFormation's own rollback.

Run: python3 -m unittest discover -s lambda/tests
"""

import hashlib
import json
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
        # The running release's published template: what the install's stack runs.
        self._fetch = u.fetch_template
        self.fetched_urls = []
        u.fetch_template = lambda url, opener=None: self.fetched_urls.append(url) or self.release_body()

    def release_body(self):
        return json.dumps(getattr(self, "release", None) or self.cfn.deployed).encode()

    def tearDown(self):
        u.fetch_template = self._fetch
        u.notify = self._notify
        u._clients["iam"] = self._iam
        u._clients.pop("cloudformation", None)
        u._clients.pop("lambda", None)

    def switch(self, **options):
        # The running release as its release check verified it.
        self.store.put("MANIFEST#v1.0.0", {"verified": True, "manifest": None, "template_sha256": hashlib.sha256(self.release_body()).hexdigest()})
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
        # The running release's template, exactly, by the release's own URL (the updater role
        # may create a change set only from one: UsePreviousTemplate was refused by IAM), its parts staged.
        self.assertNotIn("UsePreviousTemplate", kw)
        self.assertEqual(kw["TemplateURL"], "https://coderhelm-public.s3.amazonaws.com/launch/v1.0.0/coderhelm.json")
        self.assertRegex(kw["TemplateURL"], r"^https://coderhelm-public\.s3\.amazonaws\.com/launch/v\d+\.\d+\.\d+/coderhelm\.json$")
        self.assertEqual(self.lam.calls, [("coderhelm-prod-install-helper", {"action": "stage", "version": "v1.0.0",
                                                                             "template_sha256": hashlib.sha256(self.release_body()).hexdigest()})])
        self.assertEqual(kw["RoleARN"], ROLE)
        self.assertTrue(kw["IncludeNestedStacks"])
        self.assertIn("turn on Amazon Inspector findings", kw["Description"])
        params = {p["ParameterKey"]: p for p in kw["Parameters"]}
        self.assertEqual(params["InspectorFindings"], {"ParameterKey": "InspectorFindings", "ParameterValue": "on"})
        for k in ("Stage", "AdminEmail", "InfrastructureVulnerabilities", "HealthEvents"):
            self.assertEqual(params[k], {"ParameterKey": k, "UsePreviousValue": True}, k)

    def test_an_install_that_keeps_its_own_deploy_role_switches_on_the_held_copy(self):
        # The release's DeployRole differs from the install's (an update kept the install's own):
        # the template running is the held copy, so the switch names that one.
        own = {"Type": "AWS::IAM::Role", "Properties": {"RoleName": "coderhelm-prod-deploy-stack", "Policies": ["own"]}}
        self.cfn.deployed = {"Parameters": DEPLOYED["Parameters"], "Resources": dict(DEPLOYED["Resources"], DeployRole=own)}
        self.release = {"Parameters": DEPLOYED["Parameters"],
                        "Resources": dict(DEPLOYED["Resources"], DeployRole=dict(own, Properties={"RoleName": "coderhelm-prod-deploy-stack", "Policies": ["newer"]}))}
        self.lam.answer = {"ok": True, "canonical": u.canonical_sha(self.cfn.deployed)}
        self.switch(InspectorFindings="on")
        kw = self.cfn.created[0]
        self.assertNotIn("UsePreviousTemplate", kw)
        self.assertEqual(kw["TemplateURL"], "https://coderhelm-prod-release-111122223333.s3.us-east-1.amazonaws.com/releases/v1.0.0/held/coderhelm.json")
        self.assertEqual([c[1]["action"] for c in self.lam.calls], ["stage", "stage_held"])
        self.assertEqual(self.lam.calls[1][1]["keep"], {"DeployRole": own})
        self.assertEqual(self.store.get(self.store.get("UPDATE_STATE")["running_id"])["template_canonical"], u.canonical_sha(self.cfn.deployed))

    def test_a_stack_not_running_its_release_template_is_never_switched(self):
        # Anything but the deploy role and boundaries differs: the switch would change the template too.
        self.release = {"Parameters": DEPLOYED["Parameters"], "Resources": dict(DEPLOYED["Resources"], Extra={"Type": "AWS::SQS::Queue"})}
        with self.assertRaisesRegex(ValueError, "isn't v1.0.0 as released"):
            self.switch(InspectorFindings="on")
        self.assertEqual(self.cfn.created, [])

    def test_an_unverified_running_release_waits_for_its_check(self):
        self.store.put("MANIFEST#v1.0.0", {})
        started = []
        saved = u.start_verify
        u.start_verify = lambda store, env, now, version: started.append(version)
        try:
            with self.assertRaises(u.ReleaseNotReady):
                u.handle({"action": "set_options", "options": {"InspectorFindings": "on"}, "actor": "owner@example.org"}, self.env, self.store, NOW)
        finally:
            u.start_verify = saved
        self.assertEqual(started, ["v1.0.0"])
        self.assertEqual(self.cfn.created, [])

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
        # A release before private networks: those read as not there yet.
        self.assertEqual(self.status()["native"]["options"],
                         {"InfrastructureVulnerabilities": "on", "InspectorFindings": "off", "HealthEvents": "off",
                          "PrivateNetwork1": None, "PrivateNetwork2": None, "PrivateNetwork3": None})
        # A release before the switches: none of them yet.
        self.cfn.options = {}
        self.assertEqual(self.status()["native"]["options"],
                         {"InfrastructureVulnerabilities": None, "InspectorFindings": None, "HealthEvents": None,
                          "PrivateNetwork1": None, "PrivateNetwork2": None, "PrivateNetwork3": None})

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


NET = "vpc-0a1b2c3d4e5f60718:subnet-0a1b2c3d4e5f60719,subnet-0a1b2c3d4e5f6071a"
NET_DEPLOYED = {
    "Parameters": dict(DEPLOYED["Parameters"], PrivateNetwork1={}, PrivateNetwork2={}, PrivateNetwork3={}),
    "Resources": DEPLOYED["Resources"],
}
# What the change set adds for a private network: exactly its four resources.
NETWORK_CHANGES = [{"ResourceChange": {"Action": "Add", "ResourceType": t, "LogicalResourceId": lid, "Replacement": "False"}}
                   for lid, t in (("Reach2Group", "AWS::EC2::SecurityGroup"), ("Reach2Logs", "AWS::Logs::LogGroup"),
                                  ("Reach2Role", "AWS::IAM::Role"), ("Reach2", "AWS::Lambda::Function"))]


class PrivateNetworks(unittest.TestCase):
    """A private network is a parameter of the install's stack, changed the same way."""

    tearDown = Options.tearDown
    switch = Options.switch
    release_body = Options.release_body
    poll = Options.poll
    status = Options.status

    def setUp(self):
        Options.setUp(self)
        self.cfn = InstallCfn({"InfrastructureVulnerabilities": "on", "InspectorFindings": "off", "HealthEvents": "off",
                               "PrivateNetwork1": NET.replace("60718", "60799"), "PrivateNetwork2": "", "PrivateNetwork3": ""})
        self.cfn.deployed = self.cfn.fetched = NET_DEPLOYED
        self.cfn.changes = NETWORK_CHANGES
        u._clients["cloudformation"] = self.cfn

    def test_adding_one_changes_only_its_parameter_and_says_what_it_adds(self):
        started = self.switch(PrivateNetwork2=" vpc-0a1b2c3d4e5f60718:subnet-0a1b2c3d4e5f60719, subnet-0a1b2c3d4e5f6071a ")
        self.assertEqual(started["options"], {"PrivateNetwork2": {"from": "", "to": NET}})
        kw = self.cfn.created[0]
        self.assertNotIn("UsePreviousTemplate", kw)
        self.assertEqual(kw["TemplateURL"], "https://coderhelm-public.s3.amazonaws.com/launch/v1.0.0/coderhelm.json")
        self.assertIn("add private network 2 (vpc-0a1b2c3d4e5f60718, subnets subnet-0a1b2c3d4e5f60719, subnet-0a1b2c3d4e5f6071a)", kw["Description"])
        params = {p["ParameterKey"]: p for p in kw["Parameters"]}
        self.assertEqual(params["PrivateNetwork2"], {"ParameterKey": "PrivateNetwork2", "ParameterValue": NET})
        for k in ("PrivateNetwork1", "PrivateNetwork3", "InspectorFindings"):
            self.assertEqual(params[k], {"ParameterKey": k, "UsePreviousValue": True}, k)
        # Reviewed and confirmed like any switch: never applied on the press alone.
        self.assertEqual(self.poll()["native"], "review")
        self.assertEqual(self.cfn.executed, [])
        self.assertEqual(self.status()["native"]["pending"]["options"], {"PrivateNetwork2": {"from": "", "to": NET}})

    def test_removing_one_says_so(self):
        started = self.switch(PrivateNetwork1="")
        self.assertEqual(started["options"]["PrivateNetwork1"]["to"], "")
        self.assertIn("remove private network 1", self.cfn.created[0]["Description"])

    def test_refuses_anything_but_a_vpc_and_its_subnets(self):
        for bad in ("on", "vpc-0a1b2c3d", "vpc-0a1b2c3d4e5f60718:sg-0a1b2c3d4e5f60719", "vpc-0a1b2c3d4e5f60718:subnet-0a1b2c3d4e5f60719;x",
                    "vpc-0a1b2c3d4e5f60718:" + ",".join(["subnet-0a1b2c3d4e5f60719"] * 7), 3):
            with self.assertRaisesRegex(ValueError, "vpc-<id>:subnet-<id>"):
                self.switch(PrivateNetwork3=bad)
        self.assertEqual(self.cfn.created, [])

    def test_a_release_without_networks_says_it_arrives_with_the_next_update(self):
        self.cfn.options = {"InfrastructureVulnerabilities": "on", "InspectorFindings": "off", "HealthEvents": "off"}
        with self.assertRaisesRegex(ValueError, "private network 2 switch yet"):
            self.switch(PrivateNetwork2=NET)

    def test_the_access_update_is_needed_only_once_a_network_is_set(self):
        view = {"state": "pending", "rules": [], "adds": u.access_groups(["ec2:*SecurityGroup*", "health:Describe*"], u.OPTION_READS)}
        self.assertEqual([g["use"] for g in view["adds"]], ["HealthEvents", "PrivateNetworks"])
        none = u.access_decide(view, {"HealthEvents": "off", "PrivateNetwork1": "", "PrivateNetwork2": ""})
        self.assertEqual(none["state"], "optional")
        one = u.access_decide(view, {"HealthEvents": "off", "PrivateNetwork2": NET})
        self.assertEqual(one["state"], "pending")
        self.assertEqual([g["use"] for g in one["needed"]], ["PrivateNetworks"])
        self.assertEqual(u.access_decide(view, None)["state"], "pending", "unreadable: never hidden")


if __name__ == "__main__":
    unittest.main()
