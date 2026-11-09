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
        self.access_template = {}

    def get_template(self, StackName, ChangeSetName=None, TemplateStage="Original"):
        if "-access" in StackName:
            return {"TemplateBody": json.dumps(self.access_template)}
        return super().get_template(StackName, ChangeSetName, TemplateStage)

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

    def __init__(self, release_body, cfn):
        super().__init__()
        self.release_body = release_body
        self.cfn = cfn
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
        try:
            r = helper.stage_held(ev, "coderhelm-prod-release-1", s3=S3(), opener=opener, cfn=self.cfn, stack="coderhelm-prod")
        except ValueError as e:
            r = {"ok": False, "error": str(e)}
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
        self.lam = HeldLambda(body, self.cfn)
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

    def test_the_helper_never_takes_what_to_keep_from_its_caller(self):
        """A compromised updater role asks the helper to keep a deploy role of its own
        (another name, trusted by anyone, Allow *): the helper reads the stack's own and refuses."""
        new = release_template(newer(PINNED["Boundary"]["Properties"]["PolicyDocument"]))
        body = self.release(new)
        self.cfn.deployed = release_template(PINNED["Boundary"]["Properties"]["PolicyDocument"])
        evil = copy.deepcopy(PINNED["DeployRole"])
        evil["Properties"]["RoleName"] = "coderhelm-prod-mine"
        evil["Properties"]["AssumeRolePolicyDocument"]["Statement"][0]["Principal"] = {"AWS": "*"}
        evil["Properties"]["Policies"] = [{"PolicyName": "all", "PolicyDocument": {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": "*", "Resource": "*"}]}}]
        for keep in ({"DeployRole": evil}, dict(u.keep_for(new, self.cfn.deployed), DeployRole=evil), {}):
            out = self.lam.invoke(FunctionName="h", InvocationType="RequestResponse", Payload=json.dumps(
                {"action": "stage_held", "version": "v1.1.0", "template_sha256": hashlib.sha256(body).hexdigest(), "keep": keep}).encode())
            r = json.loads(out["Payload"].read())
            self.assertFalse(r["ok"])
            self.assertIn("aren't the root stack's own", r["error"])
        self.assertEqual(self.lam.put, [], "nothing written")
        # Without a "keep" it uses the stack's own: never the caller's.
        out = self.lam.invoke(FunctionName="h", InvocationType="RequestResponse", Payload=json.dumps(
            {"action": "stage_held", "version": "v1.1.0", "template_sha256": hashlib.sha256(body).hexdigest()}).encode())
        written = json.loads(self.lam.put[0]["Body"])
        self.assertEqual(written["Resources"]["DeployRole"], PINNED["DeployRole"])
        self.assertEqual(written["Resources"]["Boundary"], self.cfn.deployed["Resources"]["Boundary"])

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

    def test_an_access_stack_a_release_made_says_so_and_its_version(self):
        self.cfn.access_stack = {"StackId": "arn:aws:cloudformation:us-east-1:1:stack/coderhelm-prod-access/x", "StackName": "coderhelm-prod-access", "StackStatus": "CREATE_COMPLETE"}
        self.cfn.access_template = {"Metadata": {"CoderHelm": {"version": "v0.9.0", "kind": "access"}}, "Resources": {}}
        e = u.access_compute(self.env)["existing"]
        self.assertEqual((e["status"], e["kind"], e["version"]), ("CREATE_COMPLETE", "access", "v0.9.0"))
        self.assertIn("#/stacks/stackinfo?stackId=arn:aws:cloudformation:us-east-1:1:stack/coderhelm-prod-access/x", e["link"])

    def test_a_stack_of_that_name_a_release_didnt_make_is_never_called_deletable(self):
        # Like the boundary stack of an install hardened from the CLI: deleting that one would matter.
        self.cfn.access_stack = {"StackId": "arn:aws:cloudformation:us-east-1:1:stack/coderhelm-prod-access/y", "StackName": "coderhelm-prod-access", "StackStatus": "UPDATE_COMPLETE"}
        self.cfn.access_template = {"Description": "CoderHelm prod: the permissions boundary, deploy role and assets bucket", "Resources": {}}
        self.assertEqual(u.access_compute(self.env)["existing"]["kind"], "other")
        root = self.cfn.get_template

        def denied(StackName, **kw):
            if "-access" in StackName:
                raise Exception("AccessDenied")
            return root(StackName, **kw)
        self.cfn.get_template = denied
        self.assertEqual(u.access_compute(self.env)["existing"]["kind"], "other", "unreadable is never called deletable")

    def test_mid_update_the_version_is_the_pins_not_the_older_output(self):
        # The template already pins v1.1.0's access.json; the Version output still says v1.0.0.
        self.cfn.deployed["Resources"]["ReleaseAssets"]["Properties"]["Files"][0]["Dest"] = "releases/v1.1.0/access.json"
        v = u.access_compute(self.env)
        self.assertEqual((v["state"], v["version"]), ("pending", "v1.1.0"))
        self.assertEqual(self.urls, ["https://coderhelm-public.s3.amazonaws.com/launch/v1.1.0/access.json"])

    def test_an_unsettled_read_is_read_again_within_a_minute(self):
        from datetime import timedelta
        self.body = self.body + b" "
        u.handle({"action": "status"}, self.env, self.store, base.NOW, fetch=lambda repo: [])
        self.body = self.body[:-1]
        st = u.handle({"action": "status"}, self.env, self.store, base.NOW + timedelta(seconds=40), fetch=lambda repo: [])
        self.assertEqual(st["native"]["access"]["state"], "pending")

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

    def test_a_pending_change_is_read_again_within_a_minute(self):
        from datetime import timedelta
        u.handle({"action": "status"}, self.env, self.store, base.NOW, fetch=lambda repo: [])
        u.handle({"action": "status"}, self.env, self.store, base.NOW + timedelta(seconds=20), fetch=lambda repo: [])
        self.assertEqual(len(self.urls), 1)
        u.handle({"action": "status"}, self.env, self.store, base.NOW + timedelta(seconds=40), fetch=lambda repo: [])
        self.assertEqual(len(self.urls), 2, "an admin's finished access stack shows within a minute")


class StuckBase(Base):
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



class StuckRollback(StuckBase):
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


class FailsClosed(Base):
    """The preflight never lets an update start unchecked."""

    def prepare(self, changes):
        self.release(json.loads(base.TEMPLATE))
        u.begin(self.store, self.env, base.NOW, "v1.1.0", "owner@example.org", "update", consent=True)
        self.cfn.cs = {"Status": "CREATE_COMPLETE", "ExecutionStatus": "AVAILABLE", "Changes": changes}
        return u.native_poll(self.store, self.env, base.NOW)

    def technical(self):
        return self.store.history()[0]["technical"]

    def test_unreadable_policies_hold_it(self):
        class Broken(base.FakeIam):
            def get_role(self, RoleName):
                raise Exception("AccessDenied: iam:GetRole")
        u._clients["iam"] = Broken()
        r = self.prepare([{"ResourceChange": {"Action": "Add", "LogicalResourceId": "Q", "ResourceType": "AWS::SQS::Queue"}}])
        self.assertEqual(r["status"], "failed")
        self.assertIn("couldn't check: the deploy role's policies couldn't be read", self.technical())
        self.assertEqual(self.cfn.executed, [])

    def test_an_unknown_condition_operator_holds_it(self):
        weird = copy.deepcopy(OLD_DEPLOY)
        weird["Statement"].append({"Sid": "Odd", "Effect": "Deny", "Action": "iam:*", "Resource": "*", "Condition": {"NumericLessThan": {"aws:MultiFactorAuthAge": "1"}}})
        self.iam.deploy = weird
        r = self.prepare([{"ResourceChange": {"Action": "Modify", "LogicalResourceId": "GatewayRole", "ResourceType": "AWS::IAM::Role",
                                              "PhysicalResourceId": "coderhelm-prod-gateway", "Details": [{"Target": {"Name": "Tags"}}]}}])
        self.assertEqual(r["status"], "failed")
        self.assertIn("couldn't check: the preflight can't evaluate the condition operator NumericLessThan", self.technical())

    def test_a_deny_on_a_key_it_doesnt_supply_counts_as_applying(self):
        d = copy.deepcopy(OLD_DEPLOY)
        d["Statement"].append({"Sid": "NoTagsHere", "Effect": "Deny", "Action": "iam:TagRole", "Resource": "*", "Condition": {"StringEquals": {"aws:RequestTag/x": "y"}}})
        self.iam.deploy = d
        r = self.prepare([{"ResourceChange": {"Action": "Modify", "LogicalResourceId": "GatewayRole", "ResourceType": "AWS::IAM::Role",
                                              "PhysicalResourceId": "coderhelm-prod-gateway", "Details": [{"Target": {"Name": "Tags"}}]}}])
        self.assertEqual(r["status"], "failed")
        self.assertIn("iam:TagRole on arn:aws:iam::1:role/coderhelm-prod-gateway (denied by NoTagsHere)", self.technical())

    def test_an_older_entry_that_couldnt_be_checked_is_never_run(self):
        self.assertIn("couldn't check", u.never_runs({"role_arn": ROLE, "preflight": None}))

    def test_replacements_attachments_inline_policies_passrole_and_unnamed_adds_are_checked(self):
        arn = "arn:aws:iam::1"
        calls = lambda rc: [(a, r) for a, r, _ in u.iam_calls(rc, "1", "aws", f"{arn}:policy/coderhelm-prod-boundary-stack", "coderhelm-prod")]
        self.assertEqual(calls({"Action": "Modify", "Replacement": "True", "LogicalResourceId": "R", "ResourceType": "AWS::IAM::Role", "PhysicalResourceId": "coderhelm-prod-old",
                                "AfterContext": json.dumps({"Properties": {"RoleName": "coderhelm-prod-new"}})})[:2],
                         [("iam:CreateRole", f"{arn}:role/coderhelm-prod-new"), ("iam:DeleteRole", f"{arn}:role/coderhelm-prod-old")])
        self.assertIn(("iam:DeletePolicy", f"{arn}:policy/coderhelm-prod-old"), calls({"Action": "Modify", "Replacement": "True", "LogicalResourceId": "P", "ResourceType": "AWS::IAM::ManagedPolicy",
                       "PhysicalResourceId": f"{arn}:policy/coderhelm-prod-old", "AfterContext": json.dumps({"Properties": {"ManagedPolicyName": "coderhelm-prod-new"}})}))
        self.assertIn(("iam:AttachRolePolicy", f"{arn}:role/coderhelm-prod-gateway"), calls({"Action": "Add", "LogicalResourceId": "P", "ResourceType": "AWS::IAM::ManagedPolicy",
                       "AfterContext": json.dumps({"Properties": {"Roles": ["coderhelm-prod-gateway"]}})}))
        self.assertEqual(calls({"Action": "Add", "LogicalResourceId": "P", "ResourceType": "AWS::IAM::RolePolicy", "AfterContext": json.dumps({"Properties": {"RoleName": "coderhelm-prod-x"}})}),
                         [("iam:PutRolePolicy", f"{arn}:role/coderhelm-prod-x")])
        self.assertIn(("iam:PassRole", f"{arn}:role/coderhelm-prod-fn"), calls({"Action": "Add", "LogicalResourceId": "F", "ResourceType": "AWS::Lambda::Function",
                       "AfterContext": json.dumps({"Properties": {"Role": f"{arn}:role/coderhelm-prod-fn"}})}))
        self.assertEqual(calls({"Action": "Add", "LogicalResourceId": "Anon", "ResourceType": "AWS::IAM::Role", "AfterContext": "{}"}), [("iam:CreateRole", f"{arn}:role/coderhelm-prod-Anon-GENERATED")])

    def test_statement_order_is_not_a_change(self):
        a = {"Version": "2012-10-17", "Statement": [{"Sid": "A", "Effect": "Allow", "Action": "s3:Get*", "Resource": "*"}, {"Sid": "B", "Effect": "Deny", "Action": ["iam:*"], "Resource": "*"}]}
        b = {"Version": "2012-10-17", "Statement": list(reversed(a["Statement"]))}
        self.assertTrue(u.same_policy(a, b))
        self.assertTrue(writer.same(a, b))


class RecoveryChecks(StuckBase):
    def test_a_role_is_compared_whole(self):
        old = release_template(PINNED["Boundary"]["Properties"]["PolicyDocument"])
        values = dict(VALUES)

        class Iam(base.FakeIam):
            def __init__(s, trust):
                super().__init__(deploy=OLD_DEPLOY, boundary=OLD_BOUNDARY, policies={DEPLOY_BOUNDARY_ARN: OLD_BOUNDARY})
                s.trust = trust

            def get_role(s, RoleName):
                return {"Role": {"RoleName": RoleName, "AssumeRolePolicyDocument": s.trust,
                                 "PermissionsBoundary": {"PermissionsBoundaryArn": DEPLOY_BOUNDARY_ARN},
                                 "Tags": [{"Key": "coderhelm:install", "Value": "coderhelm-prod"}]}}
        trust = resolved(PINNED["DeployRole"]["Properties"]["AssumeRolePolicyDocument"])
        self.assertEqual(u.unchanged_by_rollback("DeployRole", "AWS::IAM::Role", old, values, Iam(trust)), "")
        anyone = copy.deepcopy(trust)
        anyone["Statement"][0]["Principal"] = {"AWS": "*"}
        self.assertEqual(u.unchanged_by_rollback("DeployRole", "AWS::IAM::Role", old, values, Iam(anyone)),
                         "its trust policy differs from the release it rolls back to")
        self.assertIn("isn't one CoderHelm checks", u.unchanged_by_rollback("Api", "AWS::CloudFormation::Stack", old, values, Iam(trust)))

    def test_an_unverified_target_is_never_compared(self):
        self.store.put("MANIFEST#v1.0.0", {})
        r = u.recover_stuck(self.store, self.env, base.NOW)
        self.assertEqual(r["status"], "stuck")
        self.assertIn("isn't verified on this install", r["reason"])
        self.assertFalse(hasattr(self.cfn, "continued"))

    def test_a_refused_continue_is_recorded_counted_and_told_at_the_last_try(self):
        def refuse(**kw):
            raise Exception("ValidationError: throttled")
        self.cfn.continue_update_rollback = refuse
        for n in (1, 2):
            r = u.recover_stuck(self.store, self.env, base.NOW)
            self.assertEqual((r["status"], r["tries"]), ("retrying", n))
            self.assertIn("throttled", r["error"])
        self.assertEqual(self.sent, [])
        r = u.recover_stuck(self.store, self.env, base.NOW)
        self.assertEqual((r["status"], r["tries"]), ("stuck", 3))
        self.assertEqual(len(self.sent), 1)

    def test_every_page_of_events_is_read(self):
        events = self.cfn.events
        pages = [events[:1], events[1:]]

        def page(StackName, NextToken=None):
            i = int(NextToken or 0)
            return dict({"StackEvents": pages[i]}, **({"NextToken": "1"} if i == 0 else {}))
        self.cfn.describe_stack_events = page
        self.assertEqual([f[0] for f in u.rollback_failures(self.cfn, "coderhelm-prod")], ["DeployBoundary", "Boundary"])

    def test_a_held_update_rolls_back_to_the_installs_own_boundaries(self):
        # The install ran v1.0.0 with the newer boundary kept (held): that is what the rollback restores.
        mine = release_template(newer(PINNED["Boundary"]["Properties"]["PolicyDocument"]))["Resources"]
        self.iam.policies[BOUNDARY_ARN] = newer(OLD_BOUNDARY)
        self.iam.policies[DEPLOY_BOUNDARY_ARN] = newer(OLD_BOUNDARY)
        self.store.put("UPDATE#2026-10-31T11:00:00Z#a", {"version": "v1.1.0", "from_version": "v1.0.0", "executed_at": "2026-10-31T11:00:00Z",
                                                       "deployed_access": {k: mine[k] for k in u.HELD}})
        r = u.recover_stuck(self.store, self.env, base.NOW)
        self.assertEqual(r["status"], "continuing")


if __name__ == "__main__":
    unittest.main()
