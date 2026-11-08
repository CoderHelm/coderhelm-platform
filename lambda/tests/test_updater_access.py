"""A native install's deploy role and boundaries during updates.

Every update runs as the parent stack's deploy role, and CloudFormation keeps
that service role for every operation on the stack, an AWS admin's Execute
in the console included. The boundary forbids that role to change itself or
a boundary. These tests pin what the updater does about it:

- the incident: a change set that changes Boundary and DeployBoundary,
  prepared with the deploy role, is refused by the preflight before it can
  run (and it is never offered for an admin to execute);
- a release whose only admin-only parts are those installs the rest, with
  the install's own copies kept (the helper writes the same "held" template
  the updater expects);
- a release without such a change is prepared exactly as before;
- the newer permissions are a separate item: an AWS admin's access stack
  writes them, and the item then reads as applied;
- a stack left in UPDATE_ROLLBACK_FAILED is rolled back the rest of the way,
  skipping only resources checked to be as the rollback leaves them.

Run: python3 -m unittest discover -s lambda/tests
"""

import copy
import hashlib
import importlib.util
import io
import json
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import test_updater_native as base  # noqa: E402

u = base.u
ROOT = pathlib.Path(__file__).resolve().parents[2]


def load(name, rel):
    spec = importlib.util.spec_from_file_location(name, ROOT / rel)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


helper = load("native_helper_for_access", "lambda/native-helper/handler.py")
writer = load("native_access_writer", "lambda/native-access/handler.py")

PINNED = json.loads((ROOT / "infra/lib/native/access-pinned.json").read_text())["resources"]
ACCOUNT, ROLE = "1", "arn:aws:iam::1:role/coderhelm-prod-deploy-stack"
BOUNDARY_ARN = "arn:aws:iam::1:policy/coderhelm-prod-boundary-stack"
DEPLOY_BOUNDARY_ARN = "arn:aws:iam::1:policy/coderhelm-prod-deploy-boundary-stack"
PARAMS = {"Stage": "prod", "AdminEmail": "owner@example.org", "EmailFrom": "", "StatusId": "", "KeepDataOnDelete": "true"}
CONDITIONS = {"HasEmailFrom": {"Fn::Not": [{"Fn::Equals": [{"Ref": "EmailFrom"}, ""]}]}}
VALUES = dict(PARAMS, **{"AWS::AccountId": ACCOUNT, "AWS::Partition": "aws", "AWS::Region": "us-east-1",
                         "AWS::StackName": "coderhelm-prod", "AWS::URLSuffix": "amazonaws.com"})


def resolved(v):
    return u.cfn_resolve(v, VALUES, CONDITIONS)


OLD_BOUNDARY = resolved(PINNED["Boundary"]["Properties"]["PolicyDocument"])
OLD_DEPLOY = resolved(PINNED["DeployRole"]["Properties"]["Policies"][0]["PolicyDocument"])


def newer(doc):
    """The pinned boundary as v0.8.54 changed it: Airflow, ECS and ECR reads, and the log analyzer's tag."""
    d = copy.deepcopy(doc)
    for s in d["Statement"]:
        if s["Sid"] == "CoderHelmServices":
            s["Action"] = s["Action"][:1] + ["airflow:Get*", "airflow:List*", "airflow:InvokeRestApi", "ecs:Describe*", "ecs:List*",
                                             "ecr:BatchGetImage", "ecr:List*"] + s["Action"][1:]
        if s["Sid"] == "ReadRolesHereOnlyGatewayAndWorker":
            s["Sid"] = "ReadRolesHereOnlyCoderHelmRuntime"
            s["Condition"]["StringNotEquals"]["aws:PrincipalTag/coderhelm:role"] = ["gateway", "worker", "log-analyzer"]
    return d


def release_template(boundary_doc):
    """A parent template with the three access resources (others as the base tests have them)."""
    t = json.loads(base.TEMPLATE)
    t["Conditions"] = CONDITIONS
    t["Resources"] = copy.deepcopy(PINNED)
    for k in ("Boundary", "DeployBoundary"):
        t["Resources"][k]["Properties"]["PolicyDocument"] = boundary_doc
    t["Resources"]["Api"] = {"Type": "AWS::CloudFormation::Stack", "Properties": {"TemplateURL": "x"}}
    return t


class Stack(base.FakeCfn):
    """The parent stack with its parameters' values, events and a deployed template."""

    def __init__(self):
        super().__init__()
        self.deployed = {}
        self.access_stack = None

    def describe_stacks(self, StackName):
        if StackName.endswith("-access"):
            if not self.access_stack:
                raise Exception(f"Stack with id {StackName} does not exist")
            return {"Stacks": [self.access_stack]}
        return {"Stacks": [{"StackId": "arn:aws:cloudformation:us-east-1:1:stack/coderhelm-prod/abc", "StackName": "coderhelm-prod",
                            "StackStatus": self.stack_status, "RoleARN": ROLE,
                            "Parameters": [{"ParameterKey": k, "ParameterValue": v} for k, v in PARAMS.items()],
                            "Outputs": [{"OutputKey": "Version", "OutputValue": "v1.0.0"}]}]}


class HeldLambda(base.FakeLambda):
    """The install helper: "stage" as before, "stage_held" through the helper's own code."""

    def __init__(self, release_body):
        super().__init__()
        self.release_body = release_body
        self.put = []

    def invoke(self, FunctionName, InvocationType, Payload):
        ev = json.loads(Payload)
        self.calls.append((FunctionName, ev))
        if ev["action"] != "stage_held":
            return {"Payload": io.BytesIO(json.dumps(self.answer).encode())}
        body = self.release_body

        class S3:
            def __init__(s):
                s.objects = {}

            def put_object(s, **kw):
                s.objects[kw["Key"]] = kw["Body"]
                self.put.append(kw)

        def opener(req, timeout=0):
            return io.BytesIO(body)
        r = helper.stage_held(ev, "coderhelm-prod-release-1", s3=S3(), opener=opener)
        return {"Payload": io.BytesIO(json.dumps(r).encode())}


class Base(unittest.TestCase):
    def setUp(self):
        self.store = u.Store(base.FakeDynamo(), "t")
        self.cfn = Stack()
        self.iam = base.FakeIam(deploy=OLD_DEPLOY, boundary=OLD_BOUNDARY, policies={BOUNDARY_ARN: OLD_BOUNDARY, DEPLOY_BOUNDARY_ARN: OLD_BOUNDARY})
        self.sent = []
        self._saved = (u.fetch_template, u.notify, dict(u._clients))
        u.notify = lambda *a, **k: self.sent.append(a)
        u._clients.update({"cloudformation": self.cfn, "iam": self.iam})
        self.env = dict(base.ENV, DEPLOY_ROLE_ARN=ROLE)

    def tearDown(self):
        u.fetch_template, u.notify = self._saved[0], self._saved[1]
        u._clients.clear()
        u._clients.update(self._saved[2])

    def release(self, t):
        body = json.dumps(t).encode()
        self.store.put("MANIFEST#v1.1.0", {"verified": True, "manifest": None, "template_sha256": hashlib.sha256(body).hexdigest()})
        u.fetch_template = lambda url, opener=None: body
        self.lam = HeldLambda(body)
        u._clients["lambda"] = self.lam
        return body


class TheIncident(Base):
    """v0.8.54 on a live install: the change set changed Boundary and
    DeployBoundary; an AWS admin executed it in the console; it ran as the
    deploy role (the stack's RoleARN), was denied, and so was its rollback."""

    def test_a_boundary_change_set_made_with_the_deploy_role_is_refused_before_anyone_can_run_it(self):
        self.release(json.loads(base.TEMPLATE))
        u.begin(self.store, self.env, base.NOW, "v1.1.0", "owner@example.org", "update")
        self.cfn.cs = {"Status": "CREATE_COMPLETE", "ExecutionStatus": "AVAILABLE", "Changes": [
            {"ResourceChange": {"Action": "Modify", "LogicalResourceId": "Boundary", "ResourceType": "AWS::IAM::ManagedPolicy",
                                "PhysicalResourceId": BOUNDARY_ARN, "Replacement": "False"}},
            {"ResourceChange": {"Action": "Modify", "LogicalResourceId": "DeployBoundary", "ResourceType": "AWS::IAM::ManagedPolicy",
                                "PhysicalResourceId": DEPLOY_BOUNDARY_ARN, "Replacement": "False"}},
        ]}
        r = u.native_poll(self.store, self.env, base.NOW)
        self.assertEqual(r["status"], "failed")
        self.assertEqual(self.cfn.executed, [], "never executed")
        self.assertEqual(self.cfn.deleted, ["arn:cs/1"], "and never left for someone to execute in the console")
        h = self.store.history()[0]
        # Exactly what AWS refused live, from the deploy role's own policies as IAM has them.
        for call in (f"parent.Boundary: iam:CreatePolicyVersion on {BOUNDARY_ARN} (denied by TheBoundaryStays)",
                     f"parent.DeployBoundary: iam:CreatePolicyVersion on {DEPLOY_BOUNDARY_ARN} (denied by TheBoundaryStays)",
                     f"parent.Boundary: iam:DeletePolicyVersion on {BOUNDARY_ARN} (denied by TheBoundaryStays)"):
            self.assertIn(call, h["technical"])
        self.assertIn("Permissions", h["fix"])
        status = u.handle({"action": "status"}, self.env, self.store, base.NOW, fetch=lambda repo: [])
        self.assertIsNone(status["native"]["pending"])

    def test_an_update_an_older_updater_left_waiting_for_an_aws_admin_ends_instead(self):
        self.release(json.loads(base.TEMPLATE))
        u.begin(self.store, self.env, base.NOW, "v1.1.0", "owner@example.org", "update")
        hid = self.store.get("UPDATE_STATE")["running_id"]
        # As an older updater left it: checked, waiting for review, admin_changes and no preflight.
        e = self.store.get(hid)
        e.update({"status": "review", "checked": True, "admin_changes": ["DeployBoundary (Modify)"], "data_changes": [], "permission_changes": []})
        e.pop("role_arn", None)
        self.store.put(hid, e)
        st = self.store.get("UPDATE_STATE")
        st.pop("running_id", None)
        st["review_id"] = hid
        self.store.put("UPDATE_STATE", st)
        self.cfn.cs = {"Status": "CREATE_COMPLETE", "ExecutionStatus": "AVAILABLE"}
        r = u.native_poll(self.store, self.env, base.NOW)
        self.assertEqual(r["status"], "failed")
        self.assertEqual(self.cfn.deleted, ["arn:cs/1"])
        self.assertEqual(self.cfn.executed, [])

    def test_the_preflight_lets_through_what_the_deploy_role_may_do(self):
        self.release(json.loads(base.TEMPLATE))
        u.begin(self.store, self.env, base.NOW, "v1.1.0", "owner@example.org", "update", consent=True)
        self.cfn.cs = {"Status": "CREATE_COMPLETE", "ExecutionStatus": "AVAILABLE", "Changes": [
            {"ResourceChange": {"Action": "Modify", "LogicalResourceId": "Api", "ResourceType": "AWS::CloudFormation::Stack", "ChangeSetId": "nested-api"}}]}
        self.cfn.nested["nested-api"] = {"Changes": [{"ResourceChange": {
            "Action": "Modify", "LogicalResourceId": "GatewayRole", "ResourceType": "AWS::IAM::Role", "PhysicalResourceId": "coderhelm-prod-gateway",
            "AfterContext": json.dumps({"Properties": {"PermissionsBoundary": BOUNDARY_ARN}}),
            "Details": [{"Target": {"Name": "Policies"}}, {"Target": {"Name": "Tags"}}]}}]}
        r = u.native_poll(self.store, self.env, base.NOW)
        self.assertEqual(r["native"], "executing")
        self.assertEqual(self.store.get(self.store.get("UPDATE_STATE")["running_id"])["preflight"], [])

    def test_a_role_without_the_boundary_is_refused_by_the_preflight(self):
        self.release(json.loads(base.TEMPLATE))
        u.begin(self.store, self.env, base.NOW, "v1.1.0", "owner@example.org", "update")
        self.cfn.cs = {"Status": "CREATE_COMPLETE", "ExecutionStatus": "AVAILABLE", "Changes": [
            {"ResourceChange": {"Action": "Add", "LogicalResourceId": "Loose", "ResourceType": "AWS::IAM::Role",
                                "AfterContext": json.dumps({"Properties": {"RoleName": "coderhelm-prod-loose", "PermissionsBoundary": "arn:aws:iam::1:policy/other"}})}}]}
        r = u.native_poll(self.store, self.env, base.NOW)
        self.assertEqual(r["status"], "failed")
        self.assertIn("iam:CreateRole on arn:aws:iam::1:role/coderhelm-prod-loose (denied by RolesOnlyWithTheBoundary)", self.store.history()[0]["technical"])


class HeldAccess(Base):
    def test_a_release_with_a_boundary_change_installs_the_rest_with_the_installs_own_kept(self):
        new = release_template(newer(PINNED["Boundary"]["Properties"]["PolicyDocument"]))
        self.release(new)
        self.cfn.deployed = release_template(PINNED["Boundary"]["Properties"]["PolicyDocument"])
        e = u.begin(self.store, self.env, base.NOW, "v1.1.0", "owner@example.org", "update")
        self.assertEqual(e["held"], ["Boundary", "DeployBoundary"])
        kw = self.cfn.created[0]
        self.assertEqual(kw["TemplateURL"], "https://coderhelm-prod-release-1.s3.us-east-1.amazonaws.com/releases/v1.1.0/held/coderhelm.json")
        self.assertEqual(kw["RoleARN"], ROLE)
        # What the helper wrote is the release with only those two replaced by the install's own.
        written = json.loads(self.lam.put[0]["Body"])
        self.assertEqual(written["Resources"]["Boundary"], self.cfn.deployed["Resources"]["Boundary"])
        self.assertEqual({k: v for k, v in written["Resources"].items() if k not in u.HELD},
                         {k: v for k, v in new["Resources"].items() if k not in u.HELD})
        # CloudFormation fetched that template: it runs, the rest of the release installs.
        self.cfn.fetched = written
        self.cfn.cs = {"Status": "CREATE_COMPLETE", "ExecutionStatus": "AVAILABLE"}
        self.assertEqual(u.native_poll(self.store, self.env, base.NOW)["native"], "executing")
        self.assertEqual(self.cfn.executed, ["arn:cs/1"])
        status = u.handle({"action": "status"}, self.env, self.store, base.NOW, fetch=lambda repo: [])
        self.assertEqual(status["native"]["pending"]["held"], ["Boundary", "DeployBoundary"])

    def test_a_release_without_a_boundary_change_is_prepared_as_before(self):
        same = release_template(PINNED["Boundary"]["Properties"]["PolicyDocument"])
        self.release(same)
        self.cfn.deployed = copy.deepcopy(same)
        e = u.begin(self.store, self.env, base.NOW, "v1.1.0", "owner@example.org", "update")
        self.assertEqual(e["held"], [])
        self.assertEqual(self.cfn.created[0]["TemplateURL"], "https://coderhelm-public.s3.amazonaws.com/launch/v1.1.0/coderhelm.json")
        self.assertEqual([c[1]["action"] for c in self.lam.calls], ["stage"])
        self.cfn.fetched = same
        self.cfn.cs = {"Status": "CREATE_COMPLETE", "ExecutionStatus": "AVAILABLE"}
        self.assertEqual(u.native_poll(self.store, self.env, base.NOW)["native"], "executing")

    def test_a_held_template_that_isnt_the_one_expected_is_never_proposed(self):
        new = release_template(newer(PINNED["Boundary"]["Properties"]["PolicyDocument"]))
        self.release(new)
        self.cfn.deployed = release_template(PINNED["Boundary"]["Properties"]["PolicyDocument"])
        self.lam.invoke = lambda **kw: {"Payload": io.BytesIO(json.dumps({"ok": True, "canonical": "0" * 64}).encode())}
        with self.assertRaisesRegex(ValueError, "isn't the one the updater made"):
            u.begin(self.store, self.env, base.NOW, "v1.1.0", "owner@example.org", "update")
        self.assertEqual(self.cfn.created, [])

    def test_the_helper_keeps_only_the_three_and_only_of_the_same_type(self):
        t = release_template(PINNED["Boundary"]["Properties"]["PolicyDocument"])
        with self.assertRaisesRegex(ValueError, "not a resource an update keeps"):
            helper.held_template(t, {"Api": {"Type": "AWS::CloudFormation::Stack"}})
        with self.assertRaisesRegex(ValueError, "can't be kept"):
            helper.held_template(t, {"Boundary": {"Type": "AWS::IAM::Role"}})
        with self.assertRaisesRegex(ValueError, "can't be kept"):
            helper.held_template(t, {"Boundary": dict(t["Resources"]["Boundary"], Condition="X")})


class AccessItem(Base):
    """The newer permissions: a separate, optional item an AWS admin applies."""

    def access_json(self):
        return {
            "Parameters": {p: {"Type": "String"} for p in ("Stage", "AdminEmail", "EmailFrom", "StatusId")},
            "Conditions": CONDITIONS,
            "Resources": {"Access": {"Type": "Custom::Access", "Properties": {
                "Boundary": {"Arn": {"Fn::Sub": "arn:${AWS::Partition}:iam::${AWS::AccountId}:policy/coderhelm-${Stage}-boundary-stack"},
                             "Document": newer(PINNED["Boundary"]["Properties"]["PolicyDocument"])},
                "DeployBoundary": {"Arn": {"Fn::Sub": "arn:${AWS::Partition}:iam::${AWS::AccountId}:policy/coderhelm-${Stage}-deploy-boundary-stack"},
                                   "Document": newer(PINNED["Boundary"]["Properties"]["PolicyDocument"])},
                "DeployRole": {"RoleName": {"Fn::Sub": "coderhelm-${Stage}-deploy-stack"}, "PolicyName": "deploy",
                               "Document": PINNED["DeployRole"]["Properties"]["Policies"][0]["PolicyDocument"]},
            }}},
        }

    def setUp(self):
        super().setUp()
        self.body = json.dumps(self.access_json()).encode()
        self.cfn.deployed = {"Resources": {"ReleaseAssets": {"Properties": {"Files": [
            {"Key": "access.json", "Dest": "releases/v1.0.0/access.json", "Sha256": hashlib.sha256(self.body).hexdigest()}]}}}}
        self.urls = []
        u.fetch_template = lambda url, opener=None: self.urls.append(url) or self.body

    def test_pending_names_the_change_in_plain_words_with_one_link(self):
        v = u.access_compute(self.env)
        self.assertEqual(v["state"], "pending")
        self.assertEqual(v["resources"], ["Boundary", "DeployBoundary"])
        self.assertIn("adds read access to Airflow, ECR and ECS (airflow:Get*, airflow:List*, airflow:InvokeRestApi, ecs:Describe*, ecs:List*, ecr:BatchGetImage, ecr:List*)", v["summary"])
        self.assertIn("lets the log analyzer take on CoderHelm's read roles in this account, like the gateway and worker", v["summary"])
        self.assertEqual(self.urls, ["https://coderhelm-public.s3.amazonaws.com/launch/v1.0.0/access.json"])
        self.assertIn("#/stacks/quickcreate?stackName=coderhelm-prod-access&templateURL=https%3A%2F%2Fcoderhelm-public.s3.amazonaws.com%2Flaunch%2Fv1.0.0%2Faccess.json", v["link"])
        self.assertIn("param_Stage=prod", v["link"])
        self.assertIsNone(v["existing"])

    def test_a_changed_access_json_is_never_linked(self):
        self.body = self.body + b" "
        self.assertEqual(u.access_compute(self.env)["state"], "unknown")

    def test_the_admins_access_stack_applies_it_cleanly_and_then_it_reads_applied(self):
        # What CloudFormation hands the writer: the documents resolved.
        props = resolved(self.access_json()["Resources"]["Access"]["Properties"])

        class Iam(base.FakeIam):
            def __init__(s, outer):
                super().__init__(deploy=OLD_DEPLOY, boundary=OLD_BOUNDARY, policies=dict(outer.iam.policies))
                s.versions = {a: [{"VersionId": f"v{i}", "IsDefaultVersion": i == 5, "CreateDate": f"2026-11-0{i}"} for i in range(1, 6)] for a in s.policies}
                s.deleted, s.created, s.put = [], [], []

            def list_policy_versions(s, PolicyArn):
                return {"Versions": s.versions[PolicyArn]}

            def delete_policy_version(s, PolicyArn, VersionId):
                s.deleted.append((PolicyArn, VersionId))

            def create_policy_version(s, PolicyArn, PolicyDocument, SetAsDefault):
                assert SetAsDefault
                s.created.append(PolicyArn)
                s.policies[PolicyArn] = json.loads(PolicyDocument)

            def put_role_policy(s, RoleName, PolicyName, PolicyDocument):
                s.put.append(RoleName)

        iam = Iam(self)
        self.assertEqual(writer.apply(props, iam), ["DeployBoundary", "Boundary"])
        # IAM's five versions were used up: the oldest that isn't the default went first.
        self.assertEqual(sorted(iam.deleted), [(BOUNDARY_ARN, "v1"), (DEPLOY_BOUNDARY_ARN, "v1")])
        self.assertEqual(iam.put, [], "the deploy policy was already this release's")
        # Run again (a stack update with the same release): nothing to write.
        self.assertEqual(writer.apply(props, iam), [])
        u._clients["iam"] = iam
        self.assertEqual(u.access_compute(self.env)["state"], "applied")

    def test_the_status_carries_it_and_reads_it_at_most_every_five_minutes(self):
        u.handle({"action": "status"}, self.env, self.store, base.NOW, fetch=lambda repo: [])
        u.handle({"action": "status"}, self.env, self.store, base.NOW, fetch=lambda repo: [])
        st = u.handle({"action": "status"}, self.env, self.store, base.NOW, fetch=lambda repo: [])
        self.assertEqual(st["native"]["access"]["state"], "pending")
        self.assertEqual(len(self.urls), 1)


class StuckRollback(Base):
    def setUp(self):
        super().setUp()
        self.cfn.stack_status = "UPDATE_ROLLBACK_FAILED"
        self.cfn.events = [
            {"LogicalResourceId": "DeployBoundary", "ResourceType": "AWS::IAM::ManagedPolicy", "ResourceStatus": "UPDATE_FAILED",
             "ResourceStatusReason": "not authorized to perform: iam:CreatePolicyVersion ... explicit deny in a permissions boundary"},
            {"LogicalResourceId": "Boundary", "ResourceType": "AWS::IAM::ManagedPolicy", "ResourceStatus": "UPDATE_FAILED",
             "ResourceStatusReason": "not authorized to perform: iam:DeletePolicyVersion ... explicit deny in a permissions boundary"},
            {"LogicalResourceId": "coderhelm-prod", "ResourceStatus": "UPDATE_ROLLBACK_IN_PROGRESS"},
            {"LogicalResourceId": "Boundary", "ResourceType": "AWS::IAM::ManagedPolicy", "ResourceStatus": "UPDATE_FAILED"},
        ]
        old = release_template(PINNED["Boundary"]["Properties"]["PolicyDocument"])
        self.old_body = json.dumps(old).encode()
        self.store.put("MANIFEST#v1.0.0", {"template_sha256": hashlib.sha256(self.old_body).hexdigest()})
        u.fetch_template = lambda url, opener=None: self.old_body

    def test_it_continues_the_rollback_skipping_only_what_it_checked_unchanged(self):
        r = u.native_poll(self.store, self.env, base.NOW)
        self.assertEqual(r["native"], "recovering")
        self.assertEqual(self.cfn.continued, [{"StackName": "coderhelm-prod", "ResourcesToSkip": ["DeployBoundary", "Boundary"], "RoleARN": ROLE}])
        rec = u.handle({"action": "status"}, self.env, self.store, base.NOW, fetch=lambda repo: [])["native"]["recovery"]
        self.assertEqual(rec["status"], "continuing")
        self.assertIn("each checked to be exactly as v1.0.0 has it", rec["reason"])
        self.cfn.stack_status = "UPDATE_ROLLBACK_COMPLETE"
        u.native_poll(self.store, self.env, base.NOW)
        self.assertEqual(self.store.get("UPDATE_STATE")["recovery"]["status"], "recovered")

    def test_a_policy_that_differs_is_never_skipped(self):
        self.iam.policies[BOUNDARY_ARN] = newer(OLD_BOUNDARY)
        r = u.native_poll(self.store, self.env, base.NOW)
        self.assertEqual(r["native"], "recovering")
        self.assertEqual(r["recovery"]["status"], "stuck")
        self.assertFalse(hasattr(self.cfn, "continued"))
        self.assertIn("Boundary: its policy differs from the release it rolls back to", r["recovery"]["reason"])
        self.assertEqual(len(self.sent), 1)
        u.native_poll(self.store, self.env, base.NOW)
        self.assertEqual(len(self.sent), 1, "told once")

    def test_an_update_while_stuck_is_refused_in_words(self):
        self.release(json.loads(base.TEMPLATE))
        u.fetch_template = lambda url, opener=None: json.dumps(json.loads(base.TEMPLATE)).encode()
        with self.assertRaisesRegex(ValueError, "still stuck"):
            u.begin(self.store, self.env, base.NOW, "v1.1.0", "owner@example.org", "update")

    def test_an_update_whose_rollback_stops_ends_rolled_back_once_recovered(self):
        self.release(json.loads(base.TEMPLATE))
        self.cfn.stack_status = "UPDATE_COMPLETE"
        u.begin(self.store, self.env, base.NOW, "v1.1.0", "owner@example.org", "update")
        self.cfn.cs = {"Status": "CREATE_COMPLETE", "ExecutionStatus": "EXECUTE_FAILED"}
        self.cfn.stack_status = "UPDATE_ROLLBACK_FAILED"
        u.fetch_template = lambda url, opener=None: self.old_body
        self.store.put("UPDATE#0", {})  # unrelated history keeps working
        r = u.native_poll(self.store, self.env, base.NOW)
        self.assertEqual(r["native"], "recovering")
        self.cfn.stack_status = "UPDATE_ROLLBACK_COMPLETE"
        r = u.native_poll(self.store, self.env, base.NOW)
        self.assertEqual(r["status"], "rolled_back")
        h = [x for x in self.store.history() if x.get("version") == "v1.1.0"][0]
        self.assertIn("AWS put everything back", h["summary"])


if __name__ == "__main__":
    unittest.main()
