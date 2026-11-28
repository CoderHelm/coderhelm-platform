"""A permission change is one the policies really make. CloudFormation
reports a Modify for every policy that names another part's output (its
value is "known after apply"), on every release, even when the policy is
byte for byte the same: counted as a permission change, that held every
scheduled update for a person. Each Modify is now compared with the running
release, both sides resolved the way the template resolves them.

Run: python3 -m unittest discover -s lambda/tests
"""

import copy
import hashlib
import json
import unittest

import test_updater_native as base

u = base.u
NOW = base.NOW
ROLE = base.ROLE

API_STACK = "arn:aws:cloudformation:us-east-1:1:stack/coderhelm-prod-Api-AAAA/1"
DB_STACK = "arn:aws:cloudformation:us-east-1:1:stack/coderhelm-prod-Database-BBBB/2"
TABLE_ARN = "arn:aws:dynamodb:us-east-1:1:table/coderhelm-prod"
IMP = "Imp0123456789ab"

POLICY = {
    "Type": "AWS::IAM::Policy",
    "Properties": {
        "PolicyName": "WorkerRoleDefaultPolicy",
        "Roles": [{"Ref": "WorkerRole"}],
        "PolicyDocument": {
            "Version": "2012-10-17",
            "Statement": [
                {"Effect": "Allow", "Action": ["dynamodb:GetItem", "dynamodb:Query"],
                 "Resource": [{"Ref": IMP}, {"Fn::Join": ["", [{"Ref": IMP}, "/index/*"]]}]},
                {"Effect": "Allow", "Action": "logs:PutLogEvents",
                 "Resource": {"Fn::Sub": "arn:${AWS::Partition}:logs:${AWS::Region}:${AWS::AccountId}:log-group:/aws/lambda/${Stage}-worker:*"}},
            ],
        },
    },
}
ROLE_RES = {"Type": "AWS::IAM::Role", "Properties": {"AssumeRolePolicyDocument": {"Statement": [
    {"Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}]}}}


def api_template(policy=POLICY):
    return {"Parameters": {IMP: {"Type": "String"}, "Stage": {"Type": "String"}},
            "Resources": {"WorkerRole": ROLE_RES, "WorkerRoleDefaultPolicy": policy}}


def db_template(output=None):
    return {"Resources": {"Table": {"Type": "AWS::DynamoDB::Table", "Properties": {}}},
            "Outputs": {"TableArn": {"Value": output or {"Fn::GetAtt": ["Table", "Arn"]}}}}


def parent_template(version, feed=None):
    params = json.loads(base.TEMPLATE)["Parameters"]
    return {"Parameters": params, "Resources": {
        "Database": {"Type": "AWS::CloudFormation::Stack",
                     "Properties": {"TemplateURL": f"https://x/{version}/stacks/Database.json"}},
        "Api": {"Type": "AWS::CloudFormation::Stack", "Properties": {
            "TemplateURL": f"https://x/{version}/stacks/Api.json",
            "Parameters": {IMP: feed or {"Fn::GetAtt": ["Database", "Outputs.TableArn"]}, "Stage": {"Ref": "Stage"}}}}}}


def same_order_swapped():
    p = copy.deepcopy(POLICY)
    st = p["Properties"]["PolicyDocument"]["Statement"]
    st.reverse()
    st[1]["Action"] = list(reversed(st[1]["Action"]))
    return p


def with_action(action):
    p = copy.deepcopy(POLICY)
    p["Properties"]["PolicyDocument"]["Statement"][0]["Action"].append(action)
    return p


class Templates(base.FakeCfn):
    """The parent and two parts, each with the running template and the
    change set's, and the nested change sets CloudFormation reports: the
    policy as a dynamic Modify, its import known only after apply."""

    def __init__(self, new_policy=POLICY, db_changes=None, new_feed=None, new_output=None):
        super().__init__()
        self.deployed = parent_template("v1.0.0")
        self.fetched = parent_template("v1.1.0", new_feed)
        self.templates = {API_STACK: (api_template(), api_template(new_policy)),
                          DB_STACK: (db_template(), db_template(new_output))}
        self.db_changes = db_changes or []

    def describe_stacks(self, StackName):
        if StackName == API_STACK:
            return {"Stacks": [{"StackId": API_STACK, "StackName": "coderhelm-prod-Api-AAAA", "Parameters": [
                {"ParameterKey": IMP, "ParameterValue": TABLE_ARN}, {"ParameterKey": "Stage", "ParameterValue": "prod"}]}]}
        if StackName == DB_STACK:
            return {"Stacks": [{"StackId": DB_STACK, "StackName": "coderhelm-prod-Database-BBBB", "Parameters": []}]}
        out = super().describe_stacks(StackName)
        out["Stacks"][0]["StackName"] = "coderhelm-prod"
        out["Stacks"][0]["Parameters"] = [{"ParameterKey": "Stage", "ParameterValue": "prod"}]
        return out

    def get_template(self, StackName, ChangeSetName=None, TemplateStage="Original"):
        if StackName in self.templates:
            old, new = self.templates[StackName]
            return {"TemplateBody": new if ChangeSetName else old}
        return super().get_template(StackName, ChangeSetName, TemplateStage)

    def describe_change_set(self, ChangeSetName, StackName=None, IncludePropertyValues=False, NextToken=None):
        if ChangeSetName == "nested-api":
            return {"ChangeSetId": "nested-api", "StackId": API_STACK, "Parameters": [
                {"ParameterKey": IMP, "ParameterValue": "{{IntrinsicFunction:coderhelm-prod/Database.Outputs.TableArn/Fn::GetAtt}}"},
                {"ParameterKey": "Stage", "ParameterValue": "prod"}],
                "Changes": [{"ResourceChange": {
                    "Action": "Modify", "LogicalResourceId": "WorkerRoleDefaultPolicy", "ResourceType": "AWS::IAM::Policy",
                    "Replacement": "False", "Details": [{"Evaluation": "Dynamic", "ChangeSource": "ParameterReference"}],
                    "BeforeContext": json.dumps({"Properties": {"PolicyDocument": "{}"}}),
                    "AfterContext": json.dumps({"Properties": {"PolicyDocument": "{{changeSet:KNOWN_AFTER_APPLY}}"}})}}]}
        if ChangeSetName == "nested-db":
            return {"ChangeSetId": "nested-db", "StackId": DB_STACK, "Parameters": [], "Changes": self.db_changes}
        assert "RoleARN" not in self.cs
        return dict({"ChangeSetName": "cs", "ChangeSetId": ChangeSetName, "StackId": "arn:stack/coderhelm-prod/abc",
                     "Parameters": [{"ParameterKey": "Stage", "UsePreviousValue": True}], "Changes": [
            {"ResourceChange": {"Action": "Modify", "LogicalResourceId": "Database", "ResourceType": "AWS::CloudFormation::Stack",
                                "Replacement": "False", "ChangeSetId": "nested-db", "PhysicalResourceId": DB_STACK}},
            {"ResourceChange": {"Action": "Modify", "LogicalResourceId": "Api", "ResourceType": "AWS::CloudFormation::Stack",
                                "Replacement": "False", "ChangeSetId": "nested-api", "PhysicalResourceId": API_STACK}},
        ]}, **self.cs)


def changes(cfn):
    return u.permission_changes(cfn, "arn:cs/1", "coderhelm-prod", {"AWS_REGION": "us-east-1"})


class OnlyRealPermissionChanges(unittest.TestCase):
    def test_the_same_policy_reported_as_a_modify_is_no_permission_change(self):
        self.assertEqual(changes(Templates()), [])
        # Order of statements and actions is not a difference either.
        self.assertEqual(changes(Templates(new_policy=same_order_swapped())), [])

    def test_an_added_action_is_one(self):
        out = changes(Templates(new_policy=with_action("dynamodb:PutItem")))
        self.assertEqual(len(out), 1, out)
        self.assertIn("Api.WorkerRoleDefaultPolicy", out[0])
        self.assertIn("allows dynamodb:PutItem", out[0])

    def test_a_new_resource_or_condition_is_one(self):
        p = copy.deepcopy(POLICY)
        p["Properties"]["PolicyDocument"]["Statement"][1]["Resource"] = "*"
        self.assertEqual(len(changes(Templates(new_policy=p))), 1)
        p = copy.deepcopy(POLICY)
        p["Properties"]["PolicyDocument"]["Statement"][0]["Condition"] = {"StringEquals": {"aws:RequestedRegion": "us-east-1"}}
        out = changes(Templates(new_policy=p))
        self.assertEqual(len(out), 1)
        self.assertIn("resources or conditions change", out[0])

    def test_the_import_counts_when_its_source_changes(self):
        # The parent feeds the import from another output.
        feed = {"Fn::GetAtt": ["Database", "Outputs.OtherArn"]}
        cfn = Templates(new_feed=feed)
        cfn.templates[DB_STACK][1]["Outputs"]["OtherArn"] = {"Value": {"Fn::GetAtt": ["Table", "StreamArn"]}}
        out = changes(cfn)
        self.assertEqual(len(out), 1)
        self.assertIn("couldn't be compared", out[0])
        # The output itself names something else.
        self.assertEqual(len(changes(Templates(new_output={"Fn::GetAtt": ["Table", "StreamArn"]}))), 1)
        # The table it names is replaced: its ARN is new.
        replaced = [{"ResourceChange": {"Action": "Modify", "LogicalResourceId": "Table", "ResourceType": "AWS::DynamoDB::Table",
                                        "Replacement": "True"}}]
        self.assertEqual(len(changes(Templates(db_changes=replaced))), 1)

    def test_a_template_that_cant_be_read_counts(self):
        cfn = Templates()
        del cfn.templates[API_STACK]
        cfn.get_template = lambda **kw: (_ for _ in ()).throw(RuntimeError("AccessDenied"))
        out = changes(cfn)
        self.assertEqual(len(out), 1)
        self.assertIn("couldn't be compared", out[0])

    def test_an_added_policy_always_counts(self):
        rc = {"Action": "Add", "ResourceType": "AWS::IAM::Policy", "LogicalResourceId": "New"}
        cfn = Templates()
        orig = cfn.describe_change_set

        def pages(ChangeSetName, **kw):
            page = orig(ChangeSetName, **kw)
            if ChangeSetName == "nested-api":
                page["Changes"].append({"ResourceChange": rc})
            return page
        cfn.describe_change_set = pages
        out = changes(cfn)
        self.assertEqual([x.split(" ")[0] for x in out], ["Api.New"])


class ScheduledUpdates(unittest.TestCase):
    """The hourly window applies a release whose policies are the same by
    itself, and still holds one that widens them for a person."""

    def setUp(self):
        self.store = u.Store(base.FakeDynamo(), "t")
        u._clients["lambda"] = base.FakeLambda()
        self._fetch, self._notify = u.fetch_template, u.notify
        u.notify = lambda *a, **k: None
        self.env = dict(base.ENV, DEPLOY_ROLE_ARN=ROLE)
        self.store.put("AUTO_UPDATE", {"auto": True})

    def tearDown(self):
        u.fetch_template, u.notify = self._fetch, self._notify
        u._clients.pop("cloudformation", None)
        u._clients.pop("lambda", None)

    def run_scheduled(self, cfn):
        body = json.dumps(cfn.fetched).encode()
        u.fetch_template = lambda url, opener=None: body
        self.store.put("MANIFEST#v1.1.0", {"verified": True, "manifest": None, "template_sha256": hashlib.sha256(body).hexdigest()})
        u._clients["cloudformation"] = cfn
        u.begin(self.store, self.env, NOW, "v1.1.0", "auto", "update", scheduled=True)
        cfn.cs = {"Status": "CREATE_COMPLETE", "ExecutionStatus": "AVAILABLE"}
        return u.native_poll(self.store, self.env, NOW)

    def entry(self):
        state = self.store.get("UPDATE_STATE")
        return self.store.get(state.get("running_id") or state.get("review_id"))

    def test_the_same_policies_apply_by_themselves(self):
        cfn = Templates()
        self.assertEqual(self.run_scheduled(cfn)["native"], "executing")
        self.assertEqual(cfn.executed, ["arn:cs/1"])
        e = self.entry()
        self.assertEqual(e["permission_changes"], [])
        self.assertEqual(u.native_hold(self.store, self.env, e), "")

    def test_a_widening_still_waits_for_a_person(self):
        cfn = Templates(new_policy=with_action("s3:PutObject"))
        self.assertEqual(self.run_scheduled(cfn)["native"], "review")
        self.assertEqual(cfn.executed, [])
        hold = u.native_hold(self.store, self.env, self.entry())
        self.assertIn("auto-update held: it changes permissions", hold)
        self.assertIn("s3:PutObject", hold)


class Resolve(unittest.TestCase):
    def side(self, **kw):
        return dict({"t": api_template(), "values": {IMP: TABLE_ARN, "Stage": "prod", "AWS::Partition": "aws",
                                                     "AWS::Region": "us-east-1", "AWS::AccountId": "1"}}, **kw)

    def test_resources_are_names_and_parameters_values(self):
        doc = u.template_resolve(POLICY["Properties"], self.side())
        self.assertEqual(doc["Roles"], ["${WorkerRole}"])
        self.assertEqual(doc["PolicyDocument"]["Statement"][0]["Resource"], [TABLE_ARN, TABLE_ARN + "/index/*"])
        self.assertEqual(doc["PolicyDocument"]["Statement"][1]["Resource"], "arn:aws:logs:us-east-1:1:log-group:/aws/lambda/prod-worker:*")

    def test_what_cant_be_known_raises(self):
        with self.assertRaises(u.Unknown):
            u.template_resolve({"Ref": IMP}, self.side(values={IMP: "{{changeSet:KNOWN_AFTER_APPLY}}"}))
        with self.assertRaises(u.Unknown):
            u.template_resolve({"Fn::GetAtt": ["WorkerRole", "Arn"]}, self.side(replaced={"WorkerRole"}))
        with self.assertRaises(u.Unknown):
            u.template_resolve({"Fn::ImportValue": "x"}, self.side())


if __name__ == "__main__":
    unittest.main()
