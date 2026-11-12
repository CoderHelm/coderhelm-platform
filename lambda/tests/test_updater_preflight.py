"""The deploy role's preflight on a change set that adds a private network.

v0.8.95's first network went: proposed, reviewed (it listed only "Reach1Role,
Reach2Role Add: an IAM change"), executed, then CloudFormation's security
group handler was refused ec2:DescribeSecurityGroups as the deploy role and
the stack rolled back. The install's access stack was still v0.8.90's, so the
deploy role had none of the private networks' statements; the preflight had
checked only the IAM calls. The failure then read "a part of the install
could not be updated ... Press Update again": the stack events' datetimes
were compared as text with the ISO time the update ran, so every failure of
the same day was skipped.

Here the deploy role's policy and boundary are the ones a release gives
(infra/test/fixtures/deploy-role.json, pinned by infra/test/deploy-role.test.ts):
"pinned" is what an install has until an AWS admin applies a newer access
stack, "current" what this release's access.json writes.

Run: python3 -m unittest discover -s lambda/tests
"""

import json
import pathlib
import unittest
from datetime import timedelta

from test_updater_native import NOW, FakeIam, u
from test_updater_options import NET_DEPLOYED, InstallCfn, Options

FIXTURE = pathlib.Path(__file__).resolve().parents[2] / "infra" / "test" / "fixtures" / "deploy-role.json"
F = json.loads(FIXTURE.read_text())
ACCOUNT, REGION = F["account"], F["region"]
VPC, SUBNETS = "vpc-0a1b2c3d4e5f60718", ["subnet-0a1b2c3d4e5f60719", "subnet-0a1b2c3d4e5f6071a"]
NET = f"{VPC}:{','.join(SUBNETS)}"
NAME = "coderhelm-prod-reach-1"


def network_changes():
    """The change set a private network adds (the parent's Reach1 resources),
    with property values as DescribeChangeSet gives them."""
    props = {
        "Reach1Group": {"GroupDescription": f"CoderHelm {NAME}", "VpcId": VPC, "Tags": F["network"]["Reach1Group"]["tags"],
                        "SecurityGroupEgress": [{"IpProtocol": "tcp", "FromPort": 443, "ToPort": 443, "CidrIp": "0.0.0.0/0"}]},
        "Reach1Logs": {"LogGroupName": f"/aws/lambda/{NAME}", "RetentionInDays": 30, "Tags": [{"Key": F["installTag"], "Value": "coderhelm-prod"}]},
        "Reach1Role": {"RoleName": NAME, "PermissionsBoundary": f"arn:aws:iam::{ACCOUNT}:policy/coderhelm-prod-boundary-stack",
                       "Policies": [{"PolicyName": "reach", "PolicyDocument": {}}],
                       "AssumeRolePolicyDocument": {"Statement": [{"Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}]}},
        "Reach1": {"FunctionName": NAME, "Role": "{{changeSet:KNOWN_AFTER_APPLY}}",
                   "VpcConfig": {"SubnetIds": SUBNETS, "SecurityGroupIds": ["{{changeSet:KNOWN_AFTER_APPLY}}"]}},
    }
    return [{"ResourceChange": {"Action": "Add", "ResourceType": F["network"][lid]["type"], "LogicalResourceId": lid,
                                "AfterContext": json.dumps({"Properties": p})}} for lid, p in props.items()]


class NetworkCfn(InstallCfn):
    """The install's stack (a release with private networks), its access stack, and the network's change set."""

    def __init__(self, access_version="v0.8.90"):
        super().__init__({"InfrastructureVulnerabilities": "on", "InspectorFindings": "off", "HealthEvents": "off",
                          "PrivateNetwork1": "", "PrivateNetwork2": "", "PrivateNetwork3": ""})
        self.deployed = self.fetched = NET_DEPLOYED
        self.access_version = access_version
        self.network = network_changes()

    def describe_stacks(self, StackName):
        if StackName.endswith("-access"):
            if self.access_version is None:
                raise Exception(f"Stack with id {StackName} does not exist")
            return {"Stacks": [{"StackId": f"arn:aws:cloudformation:{REGION}:{ACCOUNT}:stack/{StackName}/acc", "StackStatus": "CREATE_COMPLETE",
                                "Outputs": [{"OutputKey": "Version", "OutputValue": self.access_version}]}]}
        return super().describe_stacks(StackName)

    def describe_change_set(self, ChangeSetName, StackName=None, IncludePropertyValues=False, NextToken=None):
        out = super().describe_change_set(ChangeSetName, StackName, IncludePropertyValues, NextToken)
        if ChangeSetName in self.nested:
            return out
        return dict(out, Changes=self.network)


def deploy_role(which):
    """The deploy role as IAM has it on an install with the `which` policy and boundary."""
    d = F[which]
    assert len(d["identity"]) == 1
    return FakeIam(deploy=d["identity"][0], boundary=d["boundary"])


class TheTwoEvaluatorsAgree(unittest.TestCase):
    def test_the_updaters_evaluator_gives_iam_evals_verdicts(self):
        # infra/test/iam-eval.ts gave these; the preflight's own evaluator must give the same.
        self.assertTrue(F["checks"])
        for c in F["checks"]:
            q = c["request"]
            for which in ("pinned", "current"):
                got, _ = u.iam_decision(F[which]["identity"], F[which]["boundary"], q["action"], q["resource"], q.get("context") or {})
                self.assertEqual(got, c[which], f"{which}: {q}")


class NetworkPreflight(unittest.TestCase):
    tearDown = Options.tearDown
    switch = Options.switch
    release_body = Options.release_body
    poll = Options.poll
    status = Options.status

    def setUp(self):
        Options.setUp(self)
        self.cfn = NetworkCfn()
        u._clients["cloudformation"] = self.cfn

    def test_an_old_deploy_role_is_refused_before_anything_starts_with_the_access_update_first(self):
        u._clients["iam"] = deploy_role("pinned")
        self.assertEqual(self.switch(PrivateNetwork1=NET)["status"], "preparing")
        r = self.poll()
        self.assertEqual(r["status"], "failed")
        # Never executed: the change set is deleted before anyone can run it.
        self.assertEqual(self.cfn.executed, [])
        self.assertEqual(self.cfn.deleted, ["arn:cs/1"])
        row = self.status()["history"][0]
        self.assertEqual(row["mode"], "options")
        self.assertIn("couldn't start", row["summary"])
        self.assertIn("Nothing changed", row["summary"])
        # The calls CloudFormation's handlers would have been refused, by resource.
        self.assertIn("parent.Reach1Group: ec2:CreateSecurityGroup, ec2:DescribeSecurityGroups", row["technical"])
        self.assertIn("parent.Reach1: ec2:DescribeSecurityGroups, ec2:DescribeSubnets, ec2:DescribeVpcs", row["technical"])
        # Its role and log group were always allowed: only what the access stack adds is named.
        self.assertNotIn("Reach1Role", row["technical"])
        self.assertNotIn("Reach1Logs", row["technical"])
        # What to do, plainly: the access stack update first, which version it is at and which it needs, and where.
        self.assertIn("Apply the access stack update first (it's at v0.8.90, this needs v1.0.0)", row["fix"])
        self.assertIn("https://coderhelm-public.s3.amazonaws.com/launch/v1.0.0/access.json", row["fix"])
        self.assertIn("console.aws.amazon.com/cloudformation/home?region=us-east-1#/stacks/stackinfo?stackId=", row["fix"])
        self.assertNotIn("Press Update again", row["fix"])
        # Settings, System shows the access card as needed for this change, whatever the switches read now.
        access = self.status()["native"]["access"]
        self.assertEqual(access["state"], "pending")
        self.assertEqual(access["needed_for"]["needs"], "v1.0.0")
        self.assertEqual(access["needed_for"]["version"], "v0.8.90")
        self.assertIn("ec2:DescribeSecurityGroups", access["summary"][0])
        self.assertIn("add private network 1", access["summary"][0])

    def test_the_card_stops_asking_once_the_access_stack_has_the_release(self):
        u._clients["iam"] = deploy_role("pinned")
        self.switch(PrivateNetwork1=NET)
        self.poll()
        view = {"state": "applied", "current": {"version": "v1.0.0"}, "adds": [], "rules": []}
        self.assertEqual(u.access_with_needed(view, self.store.get("ACCESS_NEEDED"), {})["state"], "applied")
        behind = dict(view, current={"version": "v0.8.90"})
        self.assertEqual(u.access_with_needed(behind, self.store.get("ACCESS_NEEDED"), {})["state"], "pending")

    def test_the_current_deploy_role_may_add_a_network(self):
        u._clients["iam"] = deploy_role("current")
        self.cfn.access_version = "v1.0.0"
        self.switch(PrivateNetwork1=NET)
        self.assertEqual(self.poll()["native"], "review")
        pending = self.status()["native"]["pending"]
        self.assertEqual(pending["preflight"], [])
        self.assertEqual(pending["access_note"], "")
        self.assertTrue(pending["can_apply"])

    def test_the_review_says_when_the_access_stack_is_behind(self):
        # The deploy role already allows it (checked): the review says the access stack is behind, plainly.
        u._clients["iam"] = deploy_role("current")
        self.switch(PrivateNetwork1=NET)
        self.assertEqual(self.poll()["native"], "review")
        note = self.status()["native"]["pending"]["access_note"]
        self.assertIn("The access stack is at v0.8.90, behind the running release (v1.0.0)", note)
        self.assertIn("doesn't need it", note)

    def test_an_install_with_no_access_stack_is_told_to_create_it(self):
        u._clients["iam"] = deploy_role("pinned")
        self.cfn.access_version = None
        self.switch(PrivateNetwork1=NET)
        self.poll()
        row = self.status()["history"][0]
        self.assertIn("Create the access stack first", row["fix"])


class RollbackNamesTheCause(unittest.TestCase):
    """A change that ran and rolled back says which call the deploy role was refused."""

    tearDown = Options.tearDown
    switch = Options.switch
    release_body = Options.release_body
    poll = Options.poll
    status = Options.status
    setUp = NetworkPreflight.setUp

    def events(self, at):
        # As boto3 gives them: datetimes, newest first, the knock-on cancellations after the real failure.
        reason = ('Resource handler returned message: "You are not authorized to perform this operation. User: '
                  f'arn:aws:sts::{ACCOUNT}:assumed-role/coderhelm-prod-deploy-stack/AWSCloudFormation is not authorized to perform: '
                  'ec2:DescribeSecurityGroups because no identity-based policy allows the ec2:DescribeSecurityGroups action '
                  '(Service: Ec2, Status Code: 403)" (HandlerErrorCode: AccessDenied)')
        s = lambda n: at + timedelta(seconds=n)  # noqa: E731
        return [
            {"LogicalResourceId": "coderhelm-prod", "ResourceStatus": "UPDATE_ROLLBACK_COMPLETE", "Timestamp": s(160)},
            {"LogicalResourceId": "coderhelm-prod", "ResourceStatus": "UPDATE_ROLLBACK_IN_PROGRESS", "Timestamp": s(5),
             "ResourceStatusReason": "The following resource(s) failed to create: [Reach1Logs, Reach1Group, Reach1Role]."},
            {"LogicalResourceId": "Reach1Role", "ResourceStatus": "CREATE_FAILED", "Timestamp": s(4), "ResourceStatusReason": "Resource creation cancelled"},
            {"LogicalResourceId": "Reach1Logs", "ResourceStatus": "CREATE_FAILED", "Timestamp": s(4), "ResourceStatusReason": "Resource creation cancelled"},
            {"LogicalResourceId": "Reach1Group", "ResourceStatus": "CREATE_FAILED", "Timestamp": s(3), "ResourceStatusReason": reason},
            {"LogicalResourceId": "coderhelm-prod", "ResourceStatus": "UPDATE_IN_PROGRESS", "Timestamp": s(1), "ResourceStatusReason": "User Initiated"},
            # An earlier update's failure, before this one ran: never this one's cause.
            {"LogicalResourceId": "Api", "ResourceStatus": "UPDATE_FAILED", "Timestamp": at - timedelta(hours=2), "ResourceStatusReason": "older"},
        ]

    def test_the_rollback_message_names_the_denied_action_and_the_access_update(self):
        # Executed in the AWS console with a deploy role the preflight allowed (as v0.8.95's did).
        self.switch(PrivateNetwork1=NET)
        self.poll()
        self.cfn.stack_status = "UPDATE_ROLLBACK_COMPLETE"
        self.cfn.events = self.events(NOW)
        r = self.poll("EXECUTE_COMPLETE")
        self.assertEqual(r["status"], "rolled_back")
        row = self.status()["history"][0]
        self.assertIn("the deploy role isn't allowed ec2:DescribeSecurityGroups yet", row["summary"])
        self.assertIn("AWS put everything back", row["summary"])
        self.assertIn("Apply the access stack update first (it's at v0.8.90, this needs v1.0.0)", row["fix"])
        self.assertNotIn("Press Update again", row["fix"])
        self.assertTrue(row["technical"].startswith("Reach1Group: Resource handler returned message"), row["technical"])
        # The card shows the access update as needed.
        access = self.status()["native"]["access"]
        self.assertEqual(access["state"], "pending")
        self.assertIn("ec2:DescribeSecurityGroups", access["summary"][0])

    def test_first_failure_reads_same_day_datetimes_and_skips_cancellations(self):
        self.cfn.events = self.events(NOW)
        got = u.first_failure(self.cfn, "coderhelm-prod", NOW.isoformat())
        self.assertTrue(got.startswith("Reach1Group: Resource handler returned message"), got)
        self.assertEqual(u.first_failure(self.cfn, "coderhelm-prod", (NOW + timedelta(minutes=5)).isoformat()), "")


class HandlerCalls(unittest.TestCase):
    def test_each_resource_a_network_adds_has_its_handlers_calls(self):
        calls = {}
        for ch in network_changes():
            rc = ch["ResourceChange"]
            got = u.resource_calls(rc, ACCOUNT, "aws", REGION, "coderhelm-prod") + u.iam_calls(
                rc, ACCOUNT, "aws", f"arn:aws:iam::{ACCOUNT}:policy/coderhelm-prod-boundary-stack", "coderhelm-prod")
            calls[rc["LogicalResourceId"]] = {a for a, _, _ in got}
        self.assertLessEqual({"ec2:CreateSecurityGroup", "ec2:DescribeSecurityGroups", "ec2:AuthorizeSecurityGroupEgress", "ec2:CreateTags"}, calls["Reach1Group"])
        self.assertEqual({"ec2:DescribeSecurityGroups", "ec2:DescribeSubnets", "ec2:DescribeVpcs"}, calls["Reach1"])
        self.assertIn("logs:CreateLogGroup", calls["Reach1Logs"])
        self.assertIn("iam:CreateRole", calls["Reach1Role"])

    def test_a_function_outside_a_vpc_needs_no_ec2_calls(self):
        rc = {"Action": "Add", "ResourceType": "AWS::Lambda::Function", "LogicalResourceId": "Fn", "AfterContext": json.dumps({"Properties": {"FunctionName": "x"}})}
        self.assertEqual(u.resource_calls(rc, ACCOUNT, "aws", REGION), [])


if __name__ == "__main__":
    unittest.main()
