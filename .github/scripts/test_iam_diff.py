"""The release notes' permission changes: only a real difference in a role's
or a policy's actions, resources or conditions is listed, compared the way
the install's updater compares them.

Run: python3 -m unittest discover -s .github/scripts
"""

import copy
import json
import pathlib
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import iam_diff  # noqa: E402

IMP = "Imp0123456789ab"
POLICY = {"Type": "AWS::IAM::Policy", "Properties": {
    "PolicyName": "WorkerRoleDefaultPolicy", "Roles": [{"Ref": "WorkerRole"}],
    "PolicyDocument": {"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": ["dynamodb:GetItem", "dynamodb:Query"], "Resource": [{"Ref": IMP}]},
        {"Effect": "Allow", "Action": "s3:GetObject",
         "Resource": {"Fn::If": ["HasBucket", {"Fn::Sub": "arn:${AWS::Partition}:s3:::${Bucket}/*"}, {"Ref": "AWS::NoValue"}]}},
    ]}}}


def release(root, policy, conditions=None):
    worker = {"Parameters": {IMP: {"Type": "String"}, "Bucket": {"Type": "String"}},
              "Conditions": conditions or {"HasBucket": {"Fn::Not": [{"Fn::Equals": [{"Ref": "Bucket"}, ""]}]}},
              "Resources": {"WorkerRole": {"Type": "AWS::IAM::Role", "Properties": {}}, "WorkerRoleDefaultPolicy": policy,
                            "Queue": {"Type": "AWS::SQS::Queue", "Properties": {"VisibilityTimeout": 30}}}}
    (root / "stacks").mkdir(parents=True)
    (root / "stacks" / "Worker.json").write_text(json.dumps(worker))
    (root / "coderhelm.json").write_text(json.dumps({"Resources": {}}))


class Diff(unittest.TestCase):
    def setUp(self):
        self.tmp = pathlib.Path(tempfile.mkdtemp())

    def run_diff(self, old, new, **kw):
        release(self.tmp / "old", old)
        release(self.tmp / "new", new, **kw)
        return iam_diff.diff(self.tmp / "old", self.tmp / "new")

    def test_the_same_policy_in_another_order_is_no_change(self):
        p = copy.deepcopy(POLICY)
        p["Properties"]["PolicyDocument"]["Statement"].reverse()
        p["Properties"]["PolicyDocument"]["Statement"][1]["Action"].reverse()
        self.assertEqual(self.run_diff(POLICY, p), [])
        self.assertIn("None: every role", iam_diff.markdown([], "v1.0.0"))

    def test_an_added_action_is_listed(self):
        p = copy.deepcopy(POLICY)
        p["Properties"]["PolicyDocument"]["Statement"][0]["Action"].append("dynamodb:PutItem")
        (change,) = self.run_diff(POLICY, p)
        self.assertEqual(change[:3], ("stacks/Worker.json", "WorkerRoleDefaultPolicy", "AWS::IAM::Policy"))
        self.assertIn("allows dynamodb:PutItem", change[3])
        md = iam_diff.markdown([change], "v1.0.0")
        self.assertIn("## Permission changes since v1.0.0", md)
        self.assertIn("dynamodb:PutItem", md)

    def test_a_change_in_a_branch_no_value_takes_is_listed(self):
        p = copy.deepcopy(POLICY)
        p["Properties"]["PolicyDocument"]["Statement"][1]["Resource"]["Fn::If"][1] = "*"
        self.assertEqual(len(self.run_diff(POLICY, p)), 1)

    def test_a_changed_condition_is_listed(self):
        out = self.run_diff(POLICY, POLICY, conditions={"HasBucket": {"Fn::Equals": ["a", "a"]}})
        self.assertEqual([(c[1], c[2]) for c in out], [("HasBucket", "Condition")])

    def test_added_and_removed_resources(self):
        release(self.tmp / "old", POLICY)
        release(self.tmp / "new", POLICY)
        t = json.loads((self.tmp / "new" / "stacks" / "Worker.json").read_text())
        t["Resources"]["Extra"] = {"Type": "AWS::Lambda::Permission", "Properties": {"Principal": "s3.amazonaws.com"}}
        (self.tmp / "new" / "stacks" / "Worker.json").write_text(json.dumps(t))
        self.assertEqual([c[1:] for c in iam_diff.diff(self.tmp / "old", self.tmp / "new")],
                         [("Extra", "AWS::Lambda::Permission", "added")])


class Previous(unittest.TestCase):
    RELEASES = [{"tagName": "v1.3.0-beta.20261102", "isPrerelease": True}, {"tagName": "v1.2.0", "isPrerelease": False},
                {"tagName": "v1.3.0-beta.20261101", "isPrerelease": True}, {"tagName": "v1.4.0", "isDraft": True},
                {"tagName": "v1.1.0", "isPrerelease": False}]

    def test_a_beta_moves_from_the_newest_release_below_it(self):
        self.assertEqual(iam_diff.previous("v1.3.0-beta.20261103", self.RELEASES), "v1.3.0-beta.20261102")

    def test_a_stable_release_moves_from_the_last_stable(self):
        self.assertEqual(iam_diff.previous("v1.3.0", self.RELEASES), "v1.2.0")
        self.assertIsNone(iam_diff.previous("v1.0.0", self.RELEASES))


if __name__ == "__main__":
    unittest.main()
