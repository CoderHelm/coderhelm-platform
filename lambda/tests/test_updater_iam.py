"""Every call the updater makes to the install's stack, checked against the
updater role's own policy and boundary as a release gives them
(infra/test/fixtures/updater-role.json, pinned by infra/test/updater-role.test.ts
from the synthesized release).

v0.8.94's switches (InspectorFindings, PrivateNetwork1..3) made their change
set with UsePreviousTemplate: no template URL, so ProposeReleaseUpdate's
cloudformation:TemplateUrl condition (StringLike, not IfExists) refused it
with AccessDenied on every real install, while every fake here accepted it.
This drives the real updater code (a version update, a held update, a switch,
a private network, held or not) through propose, apply and the deletes, and
evaluates each request IAM sees: the change set on the parent, the parts'
nested change sets CloudFormation creates as the caller, the staged
templates it reads, the role it is passed, the helper it invokes.

The EC2 and Lambda calls a private network's deploy makes run as the deploy
role, not this one: infra/test/native.test.ts checks those against the
deploy role's policy and boundary.

Run: python3 -m unittest discover -s lambda/tests
"""

import fnmatch
import hashlib
import json
import pathlib
import re
import unittest

from test_updater_native import ENV, NOW, FakeDynamo, FakeIam, FakeLambda, u
from test_updater_options import DEPLOYED, InstallCfn

ROLE_FILE = pathlib.Path(__file__).resolve().parents[2] / "infra" / "test" / "fixtures" / "updater-role.json"
F = json.loads(ROLE_FILE.read_text())
ACCOUNT, REGION, STACK = F["account"], F["region"], F["stack"]
DEPLOY_ROLE = F["deployRoleArn"]
ROOT_ARN = f"arn:aws:cloudformation:{REGION}:{ACCOUNT}:stack/{STACK}/abc"
PART_ARN = f"arn:aws:cloudformation:{REGION}:{ACCOUNT}:stack/{STACK}-Api-1A2B3C/def"
RELEASE_BUCKET = f"{STACK}-release-{ACCOUNT}"
BUCKET_URL = re.compile(rf"^https://{re.escape(RELEASE_BUCKET)}\.s3\.{re.escape(REGION)}\.amazonaws\.com/(.+)$")


# ── A small IAM evaluator: the same rules as infra/test/iam-eval.ts ─────────

def _list(v):
    return [] if v is None else v if isinstance(v, list) else [v]


def _glob(pattern, value, ci=False):
    rx = "^" + ".*".join(".".join(re.escape(q) for q in p.split("?")) for p in pattern.split("*")) + "$"
    return re.match(rx, value, re.I if ci else 0) is not None


def condition_holds(cond, ctx):
    for full, kv in (cond or {}).items():
        if_exists = full.endswith("IfExists") and full != "IfExists"
        op = full[: -len("IfExists")] if if_exists else full
        for key, want in kv.items():
            have = ctx.get(key)
            if have is None:
                have = next((v for k, v in ctx.items() if k.lower() == key.lower()), None)
            if if_exists and have is None:
                continue
            wants, haves = _list(want), _list(have)

            def any_(f):
                return any(f(h, w) for h in haves for w in wants)
            if op == "StringEquals" or op == "ArnEquals" or op == "ForAnyValue:StringEquals":
                ok = any_(lambda h, w: h == w)
            elif op == "StringNotEquals" or op == "ArnNotEquals":
                ok = have is None or not any_(lambda h, w: h == w)
            elif op in ("StringLike", "ArnLike"):
                ok = any_(lambda h, w: _glob(w, h))
            elif op == "ArnNotLike":
                ok = have is None or not any_(lambda h, w: _glob(w, h))
            elif op == "Null":
                ok = (have is None) == (wants[0] == "true")
            elif op == "ForAnyValue:StringEqualsIgnoreCase":
                ok = any_(lambda h, w: h.lower() == w.lower())
            elif op == "ForAllValues:StringEquals":
                ok = all(h in wants for h in haves)
            elif op == "ForAllValues:StringLike":
                ok = all(any(_glob(w, h) for w in wants) for h in haves)
            else:
                raise AssertionError(f"the evaluator doesn't know {full}")
            if not ok:
                return False
    return True


def matches(s, r):
    if "Action" in s:
        hit = any(_glob(a, r["action"], True) for a in _list(s["Action"]))
    else:
        hit = not any(_glob(a, r["action"], True) for a in _list(s.get("NotAction")))
    if not hit:
        return False
    # A resource left as an intrinsic (another part's export) names nothing here.
    if "Resource" in s:
        res = any(isinstance(x, str) and _glob(x, r["resource"]) for x in _list(s["Resource"]))
    else:
        res = not any(isinstance(x, str) and _glob(x, r["resource"]) for x in _list(s.get("NotResource")))
    return res and condition_holds(s.get("Condition"), r.get("context") or {})


def decision(r):
    identity = [s for d in F["identity"] for s in d["Statement"]]
    boundary = F["boundary"]["Statement"]
    if any(s["Effect"] == "Deny" and matches(s, r) for s in identity + boundary):
        return "explicitDeny"
    by_identity = any(s["Effect"] == "Allow" and matches(s, r) for s in identity)
    by_boundary = any(s["Effect"] == "Allow" and matches(s, r) for s in boundary)
    return "allowed" if by_identity and by_boundary else "implicitDeny"


# ── What IAM sees for each call ─────────────────────────────────────────────

def s3_object(url):
    """The install bucket's object a template URL names (None: a public URL)."""
    m = BUCKET_URL.match(url or "")
    return f"arn:aws:s3:::{RELEASE_BUCKET}/{m.group(1)}" if m else None


def url_version(url):
    m = re.search(r"/(v\d+\.\d+\.\d+)/", url or "")
    return m.group(1) if m else None


class RecordingCfn(InstallCfn):
    """The install's stack: records every call as the requests IAM checks."""

    def __init__(self, options=None):
        super().__init__(options)
        self.requests = []
        self.change_sets = []
        self.serve = None

    def _req(self, action, resource=ROOT_ARN, **context):
        self.requests.append({"action": action, "resource": resource, "context": {k: v for k, v in context.items() if v is not None}})

    def describe_stacks(self, StackName):
        self._req("cloudformation:DescribeStacks")
        return super().describe_stacks(StackName)

    def get_template(self, StackName, ChangeSetName=None, TemplateStage="Original"):
        self._req("cloudformation:GetTemplate")
        return super().get_template(StackName, ChangeSetName, TemplateStage)

    def describe_change_set(self, ChangeSetName, StackName=None, IncludePropertyValues=False, NextToken=None):
        self._req("cloudformation:DescribeChangeSet", PART_ARN if ChangeSetName in self.nested else ROOT_ARN)
        return super().describe_change_set(ChangeSetName, StackName, IncludePropertyValues, NextToken)

    def describe_stack_events(self, StackName):
        self._req("cloudformation:DescribeStackEvents")
        return super().describe_stack_events(StackName)

    def create_change_set(self, **kw):
        self.change_sets.append(kw)
        url, role = kw.get("TemplateURL"), kw.get("RoleARN")
        self._req("cloudformation:CreateChangeSet", **{"cloudformation:TemplateUrl": url, "cloudformation:RoleArn": role})
        if role:
            self._req("iam:PassRole", role, **{"iam:PassedToService": "cloudformation.amazonaws.com"})
        if s3_object(url):
            # CloudFormation reads a template in the install's bucket with the caller's access.
            self._req("s3:GetObject", s3_object(url))
        if kw.get("IncludeNestedStacks"):
            # Each part's nested change set, created as the caller: from the staged
            # part template of the release the parent template names.
            v = url_version(url)
            part_url = f"https://{RELEASE_BUCKET}.s3.{REGION}.amazonaws.com/releases/{v}/stacks/Api.json" if v else None
            self._req("cloudformation:CreateChangeSet", PART_ARN, **{"cloudformation:TemplateUrl": part_url, "cloudformation:RoleArn": role})
            if part_url:
                self._req("s3:GetObject", s3_object(part_url))
        # What CloudFormation fetched from that URL (the updater compares it with what it verified).
        if self.serve:
            self.fetched = self.serve(url)
        return super().create_change_set(**kw)

    def execute_change_set(self, ChangeSetName, StackName):
        self._req("cloudformation:ExecuteChangeSet")
        return super().execute_change_set(ChangeSetName, StackName)

    def delete_change_set(self, ChangeSetName, StackName):
        self._req("cloudformation:DeleteChangeSet")
        return super().delete_change_set(ChangeSetName, StackName)

    def continue_update_rollback(self, **kw):
        self._req("cloudformation:ContinueUpdateRollback")
        return super().continue_update_rollback(**kw)


class RecordingLambda(FakeLambda):
    def __init__(self, cfn, **kw):
        super().__init__(**kw)
        self.cfn = cfn

    def invoke(self, FunctionName, InvocationType, Payload):
        self.cfn._req("lambda:InvokeFunction", f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{FunctionName}")
        return super().invoke(FunctionName, InvocationType, Payload)


class NoSsm:
    """The uninstall switch, never set (never AWS itself)."""

    def get_parameter(self, Name, **kw):
        raise type("ParameterNotFound", (Exception,), {})(Name)


NETWORK = "vpc-0a1b2c3d4e5f60718:subnet-0a1b2c3d4e5f60719,subnet-0a1b2c3d4e5f6071a"
PARAMS = dict(DEPLOYED["Parameters"], PrivateNetwork1={}, PrivateNetwork2={}, PrivateNetwork3={})
OWN_ROLE = {"Type": "AWS::IAM::Role", "Properties": {"RoleName": "coderhelm-prod-deploy-stack", "Policies": ["the install's own"]}}
NEW_ROLE = {"Type": "AWS::IAM::Role", "Properties": {"RoleName": "coderhelm-prod-deploy-stack", "Policies": ["the release's newer"]}}


def template(role, extra=None):
    return {"Parameters": PARAMS, "Resources": dict(DEPLOYED["Resources"], DeployRole=role, **(extra or {}))}


class EveryCallTheUpdaterMakes(unittest.TestCase):
    def setUp(self):
        self.store = u.Store(FakeDynamo(), "t")
        self.cfn = RecordingCfn({"InfrastructureVulnerabilities": "on", "InspectorFindings": "off", "HealthEvents": "off",
                                 "PrivateNetwork1": "", "PrivateNetwork2": "", "PrivateNetwork3": ""})
        self.lam = RecordingLambda(self.cfn)
        self.saved = {k: u._clients.get(k) for k in ("cloudformation", "lambda", "iam")}
        self.saved["ssm"] = u._clients.get("ssm")
        u._clients.update({"cloudformation": self.cfn, "lambda": self.lam, "iam": FakeIam(), "ssm": NoSsm()})
        self.cfn.serve = self.served
        self._notify, self._fetch = u.notify, u.fetch_template
        u.notify = lambda *a, **k: None
        self.releases = {}
        u.fetch_template = lambda url, opener=None: json.dumps(self.releases[url_version(url)]).encode()
        self.env = dict(ENV, DEPLOY_ROLE_ARN=DEPLOY_ROLE, NATIVE_STACK=STACK, AWS_REGION=REGION, RESOURCE_PREFIX=STACK)

    def tearDown(self):
        u.notify, u.fetch_template = self._notify, self._fetch
        for k, v in self.saved.items():
            if v is None:
                u._clients.pop(k, None)
            else:
                u._clients[k] = v

    def served(self, url):
        """The template at a change set's URL: the release's, or its held copy as the helper wrote it."""
        if not url:
            return self.cfn.deployed  # UsePreviousTemplate
        rel = self.releases[url_version(url)]
        return u.held_template(rel, u.keep_for(rel, self.cfn.deployed)) if "/held/" in url else rel

    def release(self, version, body):
        self.releases[version] = body
        self.store.put(f"MANIFEST#{version}", {"verified": True, "manifest": None,
                                               "template_sha256": hashlib.sha256(json.dumps(body).encode()).hexdigest()})

    def install(self, held):
        """The install runs v1.0.0: the release's template, or its held copy (its own deploy role kept)."""
        running = template(OWN_ROLE)
        self.release("v1.0.0", template(NEW_ROLE) if held else running)
        self.cfn.deployed = self.cfn.fetched = running
        self.lam.answer = {"ok": True, "canonical": u.canonical_sha(running)}

    def run_through(self, start):
        """Propose one that is deleted (what CloudFormation fetched isn't what was verified), then
        propose, review and apply (as an admin confirms) the next: every call recorded."""
        serve = self.cfn.serve
        self.cfn.serve = lambda url: dict(serve(url), Tampered=True)
        start()
        self.cfn.cs = {"Status": "CREATE_COMPLETE", "ExecutionStatus": "AVAILABLE"}
        u.native_poll(self.store, self.env, NOW)
        self.assertEqual(len(self.cfn.deleted), 1)
        self.cfn.serve = serve
        start()
        u.native_poll(self.store, self.env, NOW)
        pending = u.handle({"action": "status"}, self.env, self.store, NOW, fetch=lambda repo: [])["native"]["pending"]
        if not self.cfn.executed:
            # A switch waits for the admin's confirmation (it changes permissions).
            self.assertTrue(pending and pending.get("can_apply"), pending)
            u.handle({"action": "apply_update", "actor": "owner@example.org", "history_id": pending["id"], "confirm": pending["confirm"]},
                     self.env, self.store, NOW)
        self.assertEqual(len(self.cfn.executed), 1)

    def refused(self):
        return [f"{decision(r)}: {r['action']} on {r['resource']} {r['context']}" for r in self.cfn.requests if decision(r) != "allowed"]

    def assert_every_call_allowed(self, url_pattern):
        self.assertTrue(self.cfn.change_sets, "the updater made a change set")
        # Every request IAM sees, allowed by the updater role's policy and boundary.
        self.assertEqual(self.refused(), [])
        for kw in self.cfn.change_sets:
            self.assertNotIn("UsePreviousTemplate", kw)
            self.assertRegex(kw.get("TemplateURL") or "", url_pattern)
            # The parts' templates are staged for that release before the change set names them.
            staged = [c[1]["version"] for c in self.lam.calls if c[1]["action"] == "stage"]
            self.assertIn(url_version(kw["TemplateURL"]), staged)
        actions = {r["action"] for r in self.cfn.requests}
        for a in ("cloudformation:CreateChangeSet", "cloudformation:ExecuteChangeSet", "cloudformation:DeleteChangeSet", "iam:PassRole"):
            self.assertIn(a, actions)

    # The evaluators agree on what the infra test decided.
    def test_this_evaluator_gives_iam_evals_verdicts(self):
        self.assertGreater(len(F["checks"]), 5)
        for c in F["checks"]:
            self.assertEqual(decision(c["request"]), c["verdict"], c["request"])

    def test_a_version_update(self):
        self.install(held=False)
        self.release("v1.1.0", template(OWN_ROLE, {"NewQueue": {"Type": "AWS::SQS::Queue"}}))
        self.run_through(lambda: u.begin(self.store, self.env, NOW, "v1.1.0", "owner@example.org", "update", consent=True))
        self.assert_every_call_allowed(r"^https://coderhelm-public\.s3\.amazonaws\.com/launch/v1\.1\.0/coderhelm\.json$")

    def test_a_held_version_update(self):
        self.install(held=False)
        self.release("v1.1.0", template(NEW_ROLE, {"NewQueue": {"Type": "AWS::SQS::Queue"}}))
        self.lam.answer = {"ok": True, "canonical": u.canonical_sha(template(OWN_ROLE, {"NewQueue": {"Type": "AWS::SQS::Queue"}}))}
        self.run_through(lambda: u.begin(self.store, self.env, NOW, "v1.1.0", "owner@example.org", "update", consent=True))
        self.assert_every_call_allowed(rf"^https://{RELEASE_BUCKET}\.s3\.{REGION}\.amazonaws\.com/releases/v1\.1\.0/held/coderhelm\.json$")

    def _switch(self, **options):
        return lambda: u.handle({"action": "set_options", "options": options, "actor": "owner@example.org"}, self.env, self.store, NOW)

    def test_a_switch(self):
        self.install(held=False)
        self.run_through(self._switch(InspectorFindings="on"))
        self.assert_every_call_allowed(r"^https://coderhelm-public\.s3\.amazonaws\.com/launch/v1\.0\.0/coderhelm\.json$")

    def test_a_private_network(self):
        self.install(held=False)
        self.run_through(self._switch(PrivateNetwork1=NETWORK))
        self.assert_every_call_allowed(r"^https://coderhelm-public\.s3\.amazonaws\.com/launch/v1\.0\.0/coderhelm\.json$")

    def test_a_private_network_on_an_install_that_keeps_its_own_deploy_role(self):
        self.install(held=True)
        self.run_through(self._switch(PrivateNetwork1=NETWORK, PrivateNetwork2=NETWORK.replace("60718", "60799")))
        self.assert_every_call_allowed(rf"^https://{RELEASE_BUCKET}\.s3\.{REGION}\.amazonaws\.com/releases/v1\.0\.0/held/coderhelm\.json$")

    def test_the_watch_after_an_update_may_probe_the_worker_and_read_its_errors(self):
        # lambda/updater, watch_step: the probe and the metrics, and nothing past them.
        worker = f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{STACK}-worker"
        for action, resource, want in (("lambda:InvokeFunction", worker, "allowed"),
                                       ("cloudwatch:GetMetricData", "*", "allowed"),
                                       ("cloudwatch:PutMetricAlarm", "*", "implicitDeny"),
                                       ("lambda:InvokeFunction", f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{STACK}-gateway", "implicitDeny")):
            self.assertEqual(decision({"action": action, "resource": resource, "context": {}}), want, (action, resource))

    def test_a_change_set_without_a_template_url_is_refused(self):
        # What v0.8.94 sent for a switch: the guard above would have caught it.
        self.cfn.create_change_set(StackName=STACK, ChangeSetName="x", UsePreviousTemplate=True, RoleARN=DEPLOY_ROLE, IncludeNestedStacks=True)
        self.assertEqual(self.refused(), [f"implicitDeny: cloudformation:CreateChangeSet on {ROOT_ARN} {{'cloudformation:RoleArn': '{DEPLOY_ROLE}'}}"])


if __name__ == "__main__":
    unittest.main()
