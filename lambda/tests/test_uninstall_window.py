"""The uninstall stack's inline functions (infra/cfn/uninstall/*.py, inlined
into infra/cfn/coderhelm-selfhost-uninstall.yaml), against fake AWS clients:

- the window function gives the window only to this stack's own running
  build, with its own mode, filled in with this install's Launch Stacks,
  ids and progress buckets in this region, and a deny for every longer
  install name; it never takes the uninstaller's own progress bucket;
- the starter starts the build once, and on the rollback closes the window
  and removes its log and progress bucket only after a finished uninstall.

Run: python3 -m unittest discover -s lambda/tests
"""

import hashlib
import importlib.util
import io
import json
import os
import pathlib
import sys
import types
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[2]
CLIENTS = {}
boto3 = types.ModuleType("boto3")
boto3.client = lambda name, *a, **k: CLIENTS[name]


def load(name):
    """Loads the file with this test's fake boto3 (whatever else is installed)."""
    saved = sys.modules.get("boto3")
    sys.modules["boto3"] = boto3
    try:
        spec = importlib.util.spec_from_file_location("uninstall_" + name, ROOT / "infra" / "cfn" / "uninstall" / f"{name}.py")
        m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)
    finally:
        if saved is not None:
            sys.modules["boto3"] = saved
        else:
            del sys.modules["boto3"]
    return m


W = load("window")
S = load("starter")
A = "111122223333"


class Paginator:
    def __init__(self, pages):
        self.pages = pages

    def paginate(self, **kw):
        return self.pages


class Cfn:
    def __init__(self, stacks):
        self.stacks = stacks

    def get_paginator(self, op):
        return Paginator([{"StackSummaries": [{"StackName": n, "StackId": s["id"]} for n, s in self.stacks.items()]}])

    def describe_stacks(self, StackName):
        s = next(v for v in self.stacks.values() if v["id"] == StackName)
        return {"Stacks": [{"StackId": s["id"], "Parameters": [{"ParameterKey": k, "ParameterValue": v} for k, v in s["params"].items()]}]}


class S3:
    def __init__(self, buckets, files=None):
        self.buckets = buckets
        self.files = files or {}
        self.deleted = []

    def list_buckets(self):
        return {"Buckets": [{"Name": n} for n in self.buckets]}

    def get_bucket_location(self, Bucket):
        r = self.buckets[Bucket]
        return {"LocationConstraint": None if r == "us-east-1" else r}

    def get_object(self, Bucket, Key):
        if (Bucket, Key) not in self.files:
            raise Exception("NoSuchKey")
        return {"Body": io.BytesIO(json.dumps(self.files[(Bucket, Key)]).encode())}

    def list_object_versions(self, **kw):
        return {"Versions": [{"Key": "status.json", "VersionId": "null"}], "IsTruncated": False}

    def delete_objects(self, **kw):
        self.deleted.append(("objects", kw["Bucket"]))

    def delete_bucket(self, Bucket):
        self.deleted.append(("bucket", Bucket))


class Tags:
    def get_tag_values(self, **kw):
        return {"TagValues": ["coderhelm-prod", "coderhelm-prod-eu", "coderhelm-production", "my-app"]}


class Secrets:
    def get_paginator(self, op):
        return Paginator([{"SecretList": [{"Name": "coderhelm/prod/secrets"}, {"Name": "coderhelm/prod-staging/secrets"}, {"Name": "other/x"}]}])


def stack(id8, stage="prod", status="abcdefghijklmnop", launch=True):
    p = {"Stage": stage, "StatusId": status}
    if launch:
        p.update({"SetupToken": "", "AdminEmail": "a@example.org"})
    return {"id": f"arn:aws:cloudformation:us-east-1:{A}:stack/x/{id8}-1111-2222-3333-444455556666", "params": p}


def world():
    cfn = Cfn({
        "coderhelm-prod": stack("1a2b3c4d"),
        "coderhelm-prod-2": stack("5f5f5f5f", status=""),
        "coderhelm-prod-eu": stack("77777777", stage="prod-eu", status="zzzzzzzzzzzzzzzz"),
        "coderhelm-prod-api": stack("88888888", launch=False),
        "coderhelm-prod-uninstall": stack("5e6f7a8b", launch=False),
        "my-launch-renamed": stack("99999999"),
    })
    s3 = S3({
        f"coderhelm-prod-assets-9f8e7d6c-{A}": "us-east-1",
        f"coderhelm-prod-assets-22223333-{A}": "eu-west-1",
        f"coderhelm-prod-eu-assets-77777777-{A}": "us-east-1",
    }, {("coderhelm-setup-installprogress1", "status.json"): {"stage": "prod", "region": "us-east-1"},
        ("coderhelm-setup-otherinstallxxxx", "status.json"): {"stage": "staging", "region": "us-east-1"}})
    return cfn, s3


class Discover(unittest.TestCase):
    def test_finds_this_installs_launch_stacks_ids_and_progress_in_this_region(self):
        cfn, s3 = world()
        f = W.discover("prod", A, "us-east-1", cfn, s3, Tags(), Secrets(), own_status="uninstallprogres", install_status="installprogress1")
        self.assertEqual(f["launch"], ["coderhelm-prod", "coderhelm-prod-2"])
        self.assertEqual(f["ids"], ["1a2b3c4d", "5f5f5f5f", "9f8e7d6c"], "not eu-west-1's kept pieces, not another install's")
        self.assertEqual(f["setup"], ["abcdefghijklmnop", "installprogress1"])
        self.assertEqual(f["siblings"], ["coderhelm-prod-eu", "coderhelm-prod-staging"])

    def test_never_its_own_progress_bucket_nor_another_installs(self):
        cfn, s3 = world()
        f = W.discover("prod", A, "us-east-1", cfn, s3, Tags(), Secrets(), own_status="abcdefghijklmnop", install_status="otherinstallxxxx")
        self.assertEqual(f["setup"], [])


class Expand(unittest.TestCase):
    def test_tokens_become_one_string_per_value_and_empty_statements_go(self):
        st = [
            {"Sid": "A", "Effect": "Allow", "Action": "x:Y", "Resource": ["r/__ID__", "fixed"]},
            {"Sid": "B", "Effect": "Allow", "Action": "x:Y", "Resource": "s/__LAUNCH__/*", "Condition": {"StringEquals": {"k": "v-__ID__"}}},
            {"Sid": "Sib", "Effect": "Deny", "Action": "x:Y", "Resource": ["t/__SIB__-*"]},
        ]
        out = W.expand(st, [("__ID__", ["1", "2"]), ("__LAUNCH__", ["L"]), ("__SIB__", [])])
        self.assertEqual(out[0]["Resource"], ["r/1", "r/2", "fixed"])
        self.assertEqual(out[1]["Resource"], ["s/L/*"])
        self.assertEqual(out[1]["Condition"]["StringEquals"]["k"], ["v-1", "v-2"])
        self.assertEqual(len(out), 2, "no longer install: no deny statement")
        none = W.expand(st, [("__ID__", []), ("__LAUNCH__", []), ("__SIB__", ["coderhelm-prod-eu"])])
        self.assertEqual([s["Sid"] for s in none], ["A", "Sib"])
        self.assertEqual(none[0]["Resource"], ["fixed"])


SPEC = "version: 0.2\nphases: {}\n"


class Window(unittest.TestCase):
    def setUp(self):
        self.iam_puts = []
        cfn, s3 = world()
        statements = [
            {"Sid": "InstallPieces", "Effect": "Allow", "Action": "lambda:DeleteFunction", "Resource": "arn:aws:lambda:us-east-1:111122223333:function:coderhelm-prod-*-__ID__"},
            {"Sid": "DataKeys", "Effect": "Allow", "Action": "kms:ScheduleKeyDeletion", "Resource": "*"},
            {"Sid": "OnlyThisBuild", "Effect": "Deny", "Action": "*", "NotResource": ["x"], "Condition": {"StringNotEquals": {"aws:userid": "__USER_ID__"}}},
        ]
        test = self

        class Iam:
            def get_role(self, RoleName):
                return {"Role": {"RoleId": "AROAEXAMPLEBUILDROLE1"}}

            def put_role_policy(self, **kw):
                test.iam_puts.append(kw)

        class Ssm:
            def get_parameter(self, Name):
                return {"Parameter": {"Value": json.dumps(statements)}}

        class Builds:
            def __init__(self, b):
                self.b = b

            def batch_get_builds(self, ids):
                return {"builds": [self.b]}

        self.build = {"projectName": "coderhelm-prod-uninstall-5e6f7a8b", "buildStatus": "IN_PROGRESS", "source": {"type": "NO_SOURCE", "buildspec": SPEC},
                      "environment": {"image": "aws/codebuild/standard:7.0", "environmentVariables": [{"name": "STAGE", "value": "prod"}, {"name": "MODE", "value": "everything"}]},
                      "serviceRole": f"arn:aws:iam::{A}:role/coderhelm-prod-uninstall-build-5e6f7a8b"}
        CLIENTS.update({"iam": Iam(), "ssm": Ssm(), "codebuild": Builds(self.build), "cloudformation": cfn, "s3": s3,
                        "resourcegroupstaggingapi": Tags(), "secretsmanager": Secrets()})
        os.environ.update({"PROJECT": "coderhelm-prod-uninstall-5e6f7a8b", "ROLE": "coderhelm-prod-uninstall-build-5e6f7a8b", "IMAGE": "aws/codebuild/standard:7.0",
                           "PARAM": "/coderhelm-prod/uninstall-5e6f7a8b/window-policy", "PARTS": "1", "BUILDSPEC_SHA256": hashlib.sha256(W.norm(SPEC).encode()).hexdigest(),
                           "VARIABLES": "VERSION,STAGE,MODE,STATUS_BUCKET,INSTALL_STATUS_ID,UNINSTALL_ID,BUILD_ROLE,WINDOW_FUNCTION,WAIT_URL",
                           "STAGE": "prod", "MODE": "keep", "ACCOUNT": A, "REGION": "us-east-1", "OWN_STATUS": "uninstallprogres", "INSTALL_STATUS": ""})

    def ask(self):
        return W.handler({"build_id": "b:1", "user_id": "AROAEXAMPLEBUILDROLE1:AWSCodeBuild-1"}, None)

    def test_this_stacks_build_gets_its_window_in_the_stacks_own_mode(self):
        r = self.ask()
        self.assertTrue(r["ok"], r)
        doc = json.loads(self.iam_puts[0]["PolicyDocument"])
        self.assertEqual(self.iam_puts[0]["PolicyName"], "uninstall-window")
        sids = [s["Sid"] for s in doc["Statement"]]
        self.assertNotIn("DataKeys", sids, "MODE=keep in the stack: the build's own MODE=everything changes nothing")
        self.assertEqual(doc["Statement"][0]["Resource"], [f"arn:aws:lambda:us-east-1:{A}:function:coderhelm-prod-*-{i}" for i in ["1a2b3c4d", "5f5f5f5f", "9f8e7d6c"]])
        self.assertEqual(doc["Statement"][-1]["Condition"]["StringNotEquals"]["aws:userid"], ["AROAEXAMPLEBUILDROLE1:AWSCodeBuild-1"])
        self.assertNotIn("__", self.iam_puts[0]["PolicyDocument"])

    def test_refused_for_anything_but_this_templates_running_build(self):
        for change, why in [({"buildStatus": "SUCCEEDED"}, "not running"), ({"projectName": "other"}, "uninstall build"),
                            ({"source": {"type": "NO_SOURCE", "buildspec": SPEC + "  - curl evil\n"}}, "buildspec"),
                            ({"environment": {"image": "evil:latest"}}, "image"),
                            ({"environment": {"image": "aws/codebuild/standard:7.0", "environmentVariables": [{"name": "AWS_CONFIG_FILE", "value": "x"}]}}, "AWS_CONFIG_FILE"),
                            ({"serviceRole": f"arn:aws:iam::{A}:role/Admin"}, "another role")]:
            saved = dict(self.build)
            self.build.update(change)
            r = self.ask()
            self.build.clear()
            self.build.update(saved)
            self.assertFalse(r["ok"])
            self.assertIn(why, r["error"])
        self.assertEqual(self.iam_puts, [])
        r = W.handler({"build_id": "b:1", "user_id": "AROAOTHERROLE:x"}, None)
        self.assertFalse(r["ok"])
        self.assertEqual(self.iam_puts, [])

    def test_a_window_too_big_for_iam_is_refused_with_the_terminal_way(self):
        old = W.MAX_WINDOW
        W.MAX_WINDOW = 50
        try:
            r = self.ask()
        finally:
            W.MAX_WINDOW = old
        self.assertFalse(r["ok"])
        self.assertIn("uninstall --everything", r["error"])
        self.assertEqual(self.iam_puts, [])


class Starter(unittest.TestCase):
    def run_cleanup(self, build_status, phase):
        s3 = S3({}, {("coderhelm-setup-uninstallprogres", "status.json"): {"phase": phase}})
        deleted_logs = []

        class Logs:
            def delete_log_group(self, logGroupName):
                deleted_logs.append(logGroupName)

        class Builds:
            def list_builds_for_project(self, projectName):
                return {"ids": ["b:2", "b:1"]}

            def batch_get_builds(self, ids):
                return {"builds": [{"buildStatus": build_status}, {"buildStatus": "FAILED"}]}

        p = {"Bucket": "coderhelm-setup-uninstallprogres", "Project": "coderhelm-prod-uninstall-5e6f7a8b", "LogGroup": "/aws/codebuild/coderhelm-prod-uninstall-5e6f7a8b"}
        ok = S.clean_up(p, s3=s3, logs=Logs(), codebuild=Builds(), sleep=lambda s: None)
        return ok, s3.deleted, deleted_logs

    def test_after_a_finished_uninstall_its_log_and_progress_bucket_go_last(self):
        ok, deleted, logs = self.run_cleanup("SUCCEEDED", "done")
        self.assertTrue(ok)
        self.assertIn(("bucket", "coderhelm-setup-uninstallprogres"), deleted)
        self.assertEqual(logs, ["/aws/codebuild/coderhelm-prod-uninstall-5e6f7a8b"])

    def test_after_a_failure_they_stay_so_the_reason_is_readable(self):
        for status, phase in [("FAILED", "failed"), ("SUCCEEDED", "failed"), ("FAILED", "done")]:
            ok, deleted, logs = self.run_cleanup(status, phase)
            self.assertFalse(ok)
            self.assertEqual((deleted, logs), ([], []))


if __name__ == "__main__":
    unittest.main()
