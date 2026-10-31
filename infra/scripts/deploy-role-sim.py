#!/usr/bin/env python3
"""Every call CloudFormation makes for a hardened self-host install, checked
with AWS's IAM policy simulator against the deploy role and its boundary.

1. Synthesizes every self-host stack as a hardened Launch Stack install, with
   no domain and with a domain plus Bedrock (infra/lib/app.ts, stub code).
2. For each resource, takes the IAM actions its CloudFormation handlers need
   (create, read, update, delete, and tagging) from the resource type's
   registry schema (cloudformation:DescribeType), and the ARN each call hits
   from the resource's names in the template; AWS's service reference
   (servicereference.us-east-1.amazonaws.com) says which actions take no
   resource ARN and which condition keys they carry.
3. Simulates every (action, ARN, context) with iam:SimulateCustomPolicy, the
   deploy policy as the identity policy and the boundary as its boundary.
4. Simulates what the boundary leaves the install's functions in its own
   account (`runtime`): sts actions on CoderHelm's two read roles, on
   lookalike names, the CDK bootstrap, the deploy role and the install's own
   roles, by the gateway's and worker's roles (their coderhelm:role tag), by
   another role and by an untagged one; and who may set that tag. The
   identity policy allows all of it, so only the boundary decides.
5. Writes infra/test/fixtures/deploy-role-sim.json, which
   infra/test/deploy-role-sim.test.ts checks the current policy against
   with a local evaluator (and the evaluator against these verdicts).

Read-only in AWS (DescribeType, SimulateCustomPolicy); needs credentials
for any account (AWS_PROFILE). Run from infra/ after `npx tsc`:

    python3 scripts/deploy-role-sim.py

With --uninstall, it checks the guided uninstall instead
(infra/lib/uninstall-access.ts): each call in infra/lib/uninstall-cases.ts,
by the uninstall build's session inside its window (idle policy and window
as identity policies, the uninstall boundary as boundary), in both modes,
by another session and after the window's end; and every condition key the
window's allows use is one AWS's service reference lists for that action
(else AWS would never match it). It writes test/fixtures/uninstall-sim.json
for infra/test/uninstall-sim.test.ts:

    python3 scripts/deploy-role-sim.py --uninstall
"""
import argparse, concurrent.futures as cf, collections, glob, hashlib, json, os, re, subprocess, sys, time, urllib.parse, urllib.request

ACCT = "111122223333"
REG = "us-east-1"
P = "coderhelm-prod"
LAUNCH_ID = "1a2b3c4d"
TAG = "coderhelm:install"
BOUNDARY = ""
SCHEMA = {}
SVC = {}
SERVICES = ["apigateway", "acm", "acm-pca", "cloudfront", "cloudwatch", "codebuild", "cognito-idp", "dynamodb", "events", "iam", "kms", "lambda", "logs",
            "route53", "s3", "ses", "sns", "sqs", "ssm", "scheduler", "secretsmanager", "cloudformation", "application-autoscaling", "kinesis", "ec2", "elasticfilesystem", "firehose", "s3tables", "s3files"]

SERVICE_OF = {"ApiGatewayV2": "apigateway", "CertificateManager": "acm", "CloudFront": "cloudfront", "CloudWatch": "cloudwatch", "CodeBuild": "codebuild",
              "Cognito": "cognito-idp", "DynamoDB": "dynamodb", "Events": "events", "IAM": "iam", "KMS": "kms", "Lambda": "lambda", "Logs": "logs",
              "Route53": "route53", "S3": "s3", "SES": "ses", "SNS": "sns", "SQS": "sqs", "SSM": "ssm", "Scheduler": "scheduler",
              "SecretsManager": "secretsmanager", "CloudFormation": "cloudformation"}

SYNTH_JS = r"""
const fs = require("fs"), os = require("os"), path = require("path");
const cdk = require("aws-cdk-lib");
const { buildApp } = require("./dist/lib/app");
const { resolveConfig } = require("./dist/lib/config");
const { accessNames, deployPolicy, boundaryPolicy } = require("./dist/lib/access");
const [ACCT, REG, ID] = process.argv.slice(1);
const STUBS = fs.mkdtempSync(path.join(os.tmpdir(), "chsim-"));
for (const d of ["gateway", "worker", "mcp"]) { fs.mkdirSync(path.join(STUBS, d), { recursive: true }); fs.writeFileSync(path.join(STUBS, d, d === "mcp" ? "handler.js" : "bootstrap"), "stub"); }
process.env.GATEWAY_ZIP = path.join(STUBS, "gateway"); process.env.WORKER_ZIP = path.join(STUBS, "worker"); process.env.MCP_PROXY_DIR = path.join(STUBS, "mcp");
delete process.env.MODEL_ID; delete process.env.LIGHT_MODEL_ID;
const zone = { [`hosted-zone:account=${ACCT}:domainName=example.org:region=${REG}`]: { Id: "/hostedzone/ZTEST000001", Name: "example.org." } };
const base = { access: { mode: "hardened", owner: "launch", id: ID }, account: ACCT, region: REG, stage: "prod", email: { from: "coderhelm@example.org" }, admins: ["owner@example.org"] };
const cfgs = {
  "no domain": [base, {}],
  "domain and Bedrock": [{ ...base, domain: { hostname: "helm.example.org", hostedZone: "example.org" }, models: { provider: "bedrock" }, email: { from: "helm@example.org" }, integrations: { googleSignIn: true } }, zone],
};
const out = { templates: {} };
for (const [k, [c, ctx]] of Object.entries(cfgs)) {
  const app = new cdk.App({ outdir: fs.mkdtempSync(path.join(os.tmpdir(), "chsimout-")), context: ctx });
  buildApp(app, resolveConfig(c));
  out.templates[k] = {};
  for (const s of app.synth().stacks) out.templates[k][s.stackName] = { template: s.template, tags: s.tags };
}
const n = accessNames({ account: ACCT, region: REG, prefix: "coderhelm-prod", suffix: "-" + ACCT, secretName: "coderhelm/prod/secrets", id: ID });
out.names = n; out.deploy = deployPolicy(n); out.boundary = boundaryPolicy(n);
process.stdout.write(JSON.stringify(out));
"""

def at(svc):
    return f"arn:aws:{svc}:{REG}:{ACCT}"


# Placeholders for ids AWS generates.
def fake_id(kind, lid):
    h = int(hashlib.sha256(f"{kind}/{lid}".encode()).hexdigest(), 16) % (10**12)
    return {
        "kms": f"1234abcd-12ab-34cd-56ef-{h:012d}",
        "api": "a1b2c3d4e5",
        "pool": "us-east-1_AbCdEf123",
        "dist": "E2QWRUHEXAMPLE",
        "oac": "E1OACEXAMPLE01",
        "rhp": "5cc3b908-e619-4b99-88e5-2cf7f45965bd",
        "esm": f"14e0db71-5d35-4eb5-b481-{h:012d}",
        "cert": "f1e2d3c4-b5a6-4789-a012-345678901234",
        "route": "abc1234",
        "integ": "def5678",
        "client": "4example5client6id7",
        "ced": f"events-{h:012d}",
    }[kind]


class Stack:
    def __init__(self, name, tmpl, allstacks):
        self.name = name
        self.res = tmpl.get("Resources", {})
        self.outputs = tmpl.get("Outputs", {})
        self.all = allstacks

    def export(self, name):
        for st in self.all.values():
            for o in st.outputs.values():
                ex = o.get("Export", {}).get("Name")
                if ex == name:
                    return st.resolve(o["Value"])
        return None

    def ident(self, lid):
        """(Ref value, Arn) of a resource."""
        r = self.res[lid]
        ty = r["Type"]
        p = r.get("Properties", {})
        nm = lambda k: self.resolve(p.get(k))
        if ty == "AWS::Lambda::Function":
            n = nm("FunctionName"); return n, f"{at('lambda')}:function:{n}"
        if ty == "AWS::IAM::Role":
            n = nm("RoleName"); return n, f"arn:aws:iam::{ACCT}:role/{n}"
        if ty == "AWS::IAM::ManagedPolicy":
            n = nm("ManagedPolicyName"); a = f"arn:aws:iam::{ACCT}:policy/{n}"; return a, a
        if ty == "AWS::KMS::Key":
            k = fake_id("kms", self.name + lid); return k, f"{at('kms')}:key/{k}"
        if ty == "AWS::KMS::Alias":
            n = nm("AliasName"); return n, f"{at('kms')}:{n}"
        if ty in ("AWS::DynamoDB::GlobalTable", "AWS::DynamoDB::Table"):
            n = nm("TableName"); return n, f"{at('dynamodb')}:table/{n}"
        if ty == "AWS::S3::Bucket":
            n = nm("BucketName"); return n, f"arn:aws:s3:::{n}"
        if ty == "AWS::SQS::Queue":
            n = nm("QueueName"); return f"https://sqs.{REG}.amazonaws.com/{ACCT}/{n}", f"{at('sqs')}:{n}"
        if ty == "AWS::SNS::Topic":
            n = nm("TopicName"); a = f"{at('sns')}:{n}"; return a, a
        if ty == "AWS::Logs::LogGroup":
            n = nm("LogGroupName"); return n, f"{at('logs')}:log-group:{n}"
        if ty == "AWS::Events::Rule":
            n = nm("Name"); return n, f"{at('events')}:rule/{n}"
        if ty == "AWS::Scheduler::ScheduleGroup":
            n = nm("Name"); return n, f"{at('scheduler')}:schedule-group/{n}"
        if ty == "AWS::CloudWatch::Alarm":
            n = nm("AlarmName"); return n, f"{at('cloudwatch')}:alarm:{n}"
        if ty == "AWS::CloudWatch::Dashboard":
            n = nm("DashboardName"); return n, f"arn:aws:cloudwatch::{ACCT}:dashboard/{n}"
        if ty == "AWS::CodeBuild::Project":
            n = nm("Name"); return n, f"{at('codebuild')}:project/{n}"
        if ty == "AWS::Cognito::UserPool":
            i = fake_id("pool", lid); return i, f"{at('cognito-idp')}:userpool/{i}"
        if ty == "AWS::CloudFront::Distribution":
            i = fake_id("dist", lid); return i, f"arn:aws:cloudfront::{ACCT}:distribution/{i}"
        if ty == "AWS::CloudFront::Function":
            n = nm("Name"); return n, f"arn:aws:cloudfront::{ACCT}:function/{n}"
        if ty == "AWS::CloudFront::OriginAccessControl":
            i = fake_id("oac", lid); return i, f"arn:aws:cloudfront::{ACCT}:origin-access-control/{i}"
        if ty == "AWS::CloudFront::ResponseHeadersPolicy":
            i = fake_id("rhp", lid); return i, f"arn:aws:cloudfront::{ACCT}:response-headers-policy/{i}"
        if ty == "AWS::ApiGatewayV2::Api":
            i = fake_id("api", lid); return i, f"arn:aws:apigateway:{REG}::/apis/{i}"
        if ty == "AWS::ApiGatewayV2::Stage":
            api = self.resolve(p["ApiId"]); s = p["StageName"]; return s, f"arn:aws:apigateway:{REG}::/apis/{api}/stages/{s}"
        if ty == "AWS::ApiGatewayV2::Route":
            api = self.resolve(p["ApiId"]); return fake_id("route", lid), f"arn:aws:apigateway:{REG}::/apis/{api}/routes/{fake_id('route', lid)}"
        if ty == "AWS::ApiGatewayV2::Integration":
            api = self.resolve(p["ApiId"]); return fake_id("integ", lid), f"arn:aws:apigateway:{REG}::/apis/{api}/integrations/{fake_id('integ', lid)}"
        if ty == "AWS::CertificateManager::Certificate":
            a = f"{at('acm')}:certificate/{fake_id('cert', lid)}"; return a, a
        if ty == "AWS::SES::ConfigurationSet":
            n = nm("Name"); return n, f"{at('ses')}:configuration-set/{n}"
        if ty == "AWS::SES::ConfigurationSetEventDestination":
            n = nm("ConfigurationSetName"); return fake_id("ced", lid), f"{at('ses')}:configuration-set/{n}"
        if ty == "AWS::SES::EmailIdentity":
            n = nm("EmailIdentity"); return n, f"{at('ses')}:identity/{n}"
        if ty == "AWS::SES::Template":
            n = self.resolve(p["Template"].get("TemplateName")); return n, f"{at('ses')}:template/{n}"
        if ty == "AWS::SSM::Parameter":
            n = nm("Name"); return n, f"{at('ssm')}:parameter/{n.lstrip('/')}"
        if ty == "AWS::Lambda::EventSourceMapping":
            i = fake_id("esm", self.name + lid); return i, f"{at('lambda')}:event-source-mapping:{i}"
        if ty == "AWS::Lambda::LayerVersion":
            n = nm("LayerName"); a = f"{at('lambda')}:layer:{n}:1"; return a, a
        if ty == "AWS::Cognito::UserPoolClient":
            return fake_id("client", lid), None
        if ty == "AWS::SecretsManager::Secret":
            n = nm("Name"); a = f"{at('secretsmanager')}:secret:{n}-AbCdEf"; return a, a
        if ty == "AWS::CloudFormation::Stack":
            # A nested stack: <parent>-<logical id>-<random>.
            a = f"{at('cloudformation')}:stack/{P}-{lid}-1A2B3C4D5E6F/0f0f0f0f-1111-2222-3333-444455556666"; return a, a
        return lid, None

    def resolve(self, v):
        if isinstance(v, str) or v is None or isinstance(v, (int, bool)):
            return v
        if isinstance(v, list):
            return [self.resolve(x) for x in v]
        if isinstance(v, dict):
            if "Ref" in v:
                r = v["Ref"]
                if r == "AWS::Region": return REG
                if r == "AWS::AccountId": return ACCT
                if r == "AWS::Partition": return "aws"
                if r == "AWS::URLSuffix": return "amazonaws.com"
                if r == "AWS::StackName": return self.name
                if r in self.res: return self.ident(r)[0]
                return f"<ref {r}>"
            if "Fn::GetAtt" in v:
                lid, attr = v["Fn::GetAtt"] if isinstance(v["Fn::GetAtt"], list) else v["Fn::GetAtt"].split(".", 1)
                if lid not in self.res: return f"<getatt {lid}>"
                ty = self.res[lid]["Type"]
                if attr == "Arn": return self.ident(lid)[1]
                if attr == "KeyId": return self.ident(lid)[0]
                if attr == "ApiId": return self.ident(lid)[0]
                if attr == "UserPoolId": return self.ident(lid)[0]
                if attr == "Id" and ty == "AWS::CloudFront::Distribution": return self.ident(lid)[0]
                return f"<{lid}.{attr}>"
            if "Fn::Join" in v:
                sep, parts = v["Fn::Join"]
                return sep.join(str(self.resolve(x)) for x in parts)
            if "Fn::ImportValue" in v:
                return self.export(self.resolve(v["Fn::ImportValue"]))
            if "Fn::Sub" in v:
                s = v["Fn::Sub"]
                vars_ = {}
                if isinstance(s, list):
                    s, m = s; vars_ = {k: self.resolve(x) for k, x in m.items()}
                def rep(mm):
                    k = mm.group(1)
                    if k in vars_: return str(vars_[k])
                    if "." in k: return str(self.resolve({"Fn::GetAtt": k.split(".", 1)}))
                    return str(self.resolve({"Ref": k}))
                return re.sub(r"\$\{([^}!]+)\}", rep, s)
            if "Fn::Select" in v:
                i, lst = v["Fn::Select"]; lst = self.resolve(lst)
                return lst[int(i)] if isinstance(lst, list) else f"<select>"
            if "Fn::Split" in v:
                sep, s = v["Fn::Split"]; return str(self.resolve(s)).split(sep)
            if "Fn::If" in v:
                return self.resolve(v["Fn::If"][1])
            return {k: self.resolve(x) for k, x in v.items()}
        return v


# Which IAM resource "types" of an action this CloudFormation resource touches,
# beyond the resource itself: a function's role (PassRole), its code (S3), keys.
PASSED_TO = {
    "AWS::Lambda::Function": "lambda.amazonaws.com",
    "AWS::CodeBuild::Project": "codebuild.amazonaws.com",
    "AWS::Events::Rule": "events.amazonaws.com",
    "AWS::Cognito::UserPool": "cognito-idp.amazonaws.com",
    "AWS::Cognito::UserPoolClient": "cognito-idp.amazonaws.com",
    "AWS::ApiGatewayV2::Integration": "apigateway.amazonaws.com",
    "AWS::SNS::Topic": "sns.amazonaws.com",
    "AWS::S3::Bucket": "s3.amazonaws.com",
}


def svc_action(action):
    s, a = action.split(":", 1)
    return s, SVC.get(s, {}).get("actions", {}).get(a.lower())


def condition_keys(action):
    s, a = svc_action(action)
    return set(a.get("ActionConditionKeys", [])) if a else set()


def rtypes(action):
    s, a = svc_action(action)
    if not a: return None  # unknown to the service reference
    return [r["Name"] for r in a.get("Resources", [])]


def targets(st, lid, r, action):
    """The ARNs one handler call of `action` hits for resource `lid`.
    Returns list of (arn, extra_context)."""
    ty = r["Type"]
    p = r.get("Properties", {})
    ref, arn = st.ident(lid)
    svc, _ = action.split(":", 1)
    res_types = rtypes(action)
    out = []
    star = res_types == [] if res_types is not None else False

    # Cross-service calls the handler makes.
    if action == "iam:PassRole":
        roles = []
        for k in ("Role", "ServiceRole", "CredentialsArn", "RoleArn"):
            if k in p: roles.append(st.resolve(p[k]))
        for tg in p.get("Targets", []) or []:
            if tg.get("RoleArn"): roles.append(st.resolve(tg["RoleArn"]))
        if ty == "AWS::Cognito::UserPool" and p.get("SmsConfiguration"): roles.append(st.resolve(p["SmsConfiguration"]["SnsCallerArn"]))
        if not roles: return [("UNUSED:no role passed", {})]
        return [(x, {"iam:PassedToService": PASSED_TO.get(ty, "")}) for x in roles]
    if action == "iam:CreateServiceLinkedRole":
        slr = {"AWS::DynamoDB::GlobalTable": "replication.dynamodb.amazonaws.com/AWSServiceRoleForDynamoDBReplication",
               "AWS::Cognito::UserPool": "email.cognito-idp.amazonaws.com/AWSServiceRoleForAmazonCognitoIdpEmailService",
               "AWS::Cognito::UserPoolClient": "email.cognito-idp.amazonaws.com/AWSServiceRoleForAmazonCognitoIdpEmailService"}[ty]
        return [(f"arn:aws:iam::{ACCT}:role/aws-service-role/{slr}", {"iam:AWSServiceName": slr.split("/")[0]})]
    if svc == "iam" and ty in ("AWS::IAM::Policy",):
        roles = [st.resolve(x) for x in p.get("Roles", [])]
        if action.startswith("iam:") and ("User" in action or "Group" in action):
            return [("UNUSED:policy on users/groups", {})]
        return [(f"arn:aws:iam::{ACCT}:role/{x}", {}) for x in roles]
    if svc == "iam" and ty == "AWS::IAM::ManagedPolicy":
        if "User" in action or "Group" in action:
            return [("UNUSED:policy on users/groups", {})]
        if action in ("iam:AttachRolePolicy", "iam:DetachRolePolicy"):
            return [(f"arn:aws:iam::{ACCT}:role/{st.resolve(x)}", {}) for x in p.get("Roles", [])]
        return [(arn, {})]
    if svc == "iam" and ty == "AWS::SNS::Topic":
        return [("UNUSED:no delivery status logging role", {})]
    if svc == "sns" and action in ("sns:Subscribe", "sns:Unsubscribe", "sns:ListSubscriptionsByTopic") and not p.get("Subscription"):
        return [("UNUSED:no inline subscriptions", {})] if action != "sns:ListSubscriptionsByTopic" else [(arn, {})]
    if svc == "iam" and ty == "AWS::Cognito::UserPoolClient":
        return [("UNUSED:client analytics role", {})]
    if svc == "s3" and ty in ("AWS::Lambda::Function", "AWS::Lambda::LayerVersion"):
        c = p.get("Code") or p.get("Content") or {}
        if "S3Bucket" not in c: return [("UNUSED:inline code", {})]
        return [(f"arn:aws:s3:::{st.resolve(c['S3Bucket'])}/{st.resolve(c['S3Key'])}", {})]
    if svc == "s3" and ty == "AWS::ApiGatewayV2::Api":
        return [("UNUSED:no BodyS3Location", {})] if "BodyS3Location" not in p else [("*", {})]
    if svc == "kms":
        keys = []
        def find(v):
            if isinstance(v, dict):
                if "Fn::GetAtt" in v or "Ref" in v:
                    x = st.resolve(v)
                    if isinstance(x, str) and ":kms:" in x and ":key/" in x: keys.append(x)
                    elif isinstance(x, str) and re.match(r"^[0-9a-f-]{36}$", x): keys.append(f"{at('kms')}:key/{x}")
                for y in v.values(): find(y)
            elif isinstance(v, list):
                for y in v: find(y)
        if ty == "AWS::KMS::Key":
            keys = [arn]
        elif ty == "AWS::KMS::Alias":
            keys = [st.resolve(p["TargetKeyId"])]
            keys = [k if k.startswith("arn:") else f"{at('kms')}:key/{k}" for k in keys]
            return [(arn, {}), *[(k, {"tagged": True}) for k in keys]]
        else:
            find(p)
        if action in ("kms:ListAliases",):
            return [("*", {})]
        if not keys:
            return [("UNUSED:no customer key on this resource", {})]
        return [(k, {"tagged": True}) for k in sorted(set(keys))]
    if svc == "lambda" and ty == "AWS::Lambda::Function" and action in ("lambda:GetLayerVersion",):
        ls = st.resolve(p.get("Layers") or [])
        return [(l, {}) for l in ls] or [("UNUSED:no layers", {})]
    if svc == "lambda" and ty in ("AWS::Lambda::Function",) and action in ("lambda:GetCodeSigningConfig", "lambda:PutFunctionCodeSigningConfig", "lambda:DeleteFunctionCodeSigningConfig"):
        if action == "lambda:GetCodeSigningConfig":
            return [("UNUSED:no code signing config", {})] if "CodeSigningConfigArn" not in p else [("*", {})]
    if svc == "lambda" and action in ("lambda:PassCapacityProvider",):
        return [("UNUSED:no capacity provider", {})]
    if ty in ("AWS::Lambda::Permission", "AWS::Lambda::Url"):
        fn = st.resolve(p.get("FunctionName") or p.get("TargetFunctionArn"))
        if fn and not fn.startswith("arn:"): fn = f"{at('lambda')}:function:{fn}"
        return [(fn, {})]
    if ty == "AWS::Lambda::EventSourceMapping" and svc == "lambda":
        fn = st.resolve(p["FunctionName"])
        fnarn = fn if fn.startswith("arn:") else f"{at('lambda')}:function:{fn}"
        ctx = {"lambda:FunctionArn": fnarn}
        if action == "lambda:CreateEventSourceMapping":
            return [("*", ctx)]
        return [(arn, ctx)]
    if ty == "AWS::Lambda::LayerVersion" and svc == "lambda":
        return [(arn, {})]
    if ty == "AWS::KMS::Alias":
        return [(arn, {})]
    if svc == "route53":
        if action == "route53:GetChange": return [(f"arn:aws:route53:::change/C0123456789EXAMPLE", {})]
        if action in ("route53:ListHostedZones",): return [("*", {})]
        z = st.resolve(p.get("HostedZoneId")) or "ZTEST000001"
        if ty == "AWS::CertificateManager::Certificate": z = "ZTEST000001"
        return [(f"arn:aws:route53:::hostedzone/{z}", {})]
    if svc == "cloudfront" and ty == "AWS::Cognito::UserPoolDomain":
        if "CustomDomainConfig" not in p: return [("UNUSED:prefix domain, no custom domain", {})]
        return [("*", {})]
    if svc == "cognito-idp" and ty in ("AWS::Cognito::UserPoolClient", "AWS::Cognito::UserPoolDomain", "AWS::Cognito::UserPoolIdentityProvider"):
        pool = st.resolve(p["UserPoolId"])
        return [(f"{at('cognito-idp')}:userpool/{pool}", {"tagged": True})]
    if svc == "sqs" and ty == "AWS::SQS::QueuePolicy":
        return [(f"{at('sqs')}:{q.rsplit('/', 1)[-1]}", {}) for q in st.resolve(p["Queues"])]
    if svc == "s3" and ty == "AWS::S3::BucketPolicy":
        return [(f"arn:aws:s3:::{st.resolve(p['Bucket'])}", {})]
    if svc == "logs" and ty == "AWS::Logs::MetricFilter":
        return [(f"{at('logs')}:log-group:{st.resolve(p['LogGroupName'])}", {})]
    if svc == "scheduler" and action == "scheduler:DeleteSchedule":
        return [(f"{at('scheduler')}:schedule/{ref}/any-schedule", {})]
    if ty == "AWS::ApiGatewayV2::Stage" and action in ("apigateway:TagResource", "apigateway:UntagResource"):
        return [(arn, {}), (f"arn:aws:apigateway:{REG}::/tags/{urllib.parse.quote(arn, safe='')}", {})]
    if ty == "AWS::ApiGatewayV2::Api" and svc == "apigateway":
        api = arn
        outs = [(api, {})]
        if action == "apigateway:POST": outs = [(f"arn:aws:apigateway:{REG}::/apis", {})] + outs
        if action in ("apigateway:PUT", "apigateway:DELETE", "apigateway:GET", "apigateway:PATCH"):
            outs.append((f"arn:aws:apigateway:{REG}::/tags/{urllib.parse.quote(api, safe='')}", {}))
        return outs
    if ty in ("AWS::ApiGatewayV2::Route", "AWS::ApiGatewayV2::Integration", "AWS::ApiGatewayV2::Stage") and svc == "apigateway":
        coll = {"AWS::ApiGatewayV2::Route": "routes", "AWS::ApiGatewayV2::Integration": "integrations", "AWS::ApiGatewayV2::Stage": "stages"}[ty]
        api = st.resolve(p["ApiId"])
        if action == "apigateway:POST": return [(f"arn:aws:apigateway:{REG}::/apis/{api}/{coll}", {}), (arn, {})]
        return [(arn, {})]
    if action == "cloudfront:GetConnectionGroup":
        return [(f"arn:aws:cloudfront::{ACCT}:connection-group/cg_2wjDHXyJfEXAMPLE", {})]
    if action == "cloudfront:GetVpcOrigin":
        return [(f"arn:aws:cloudfront::{ACCT}:vpcorigin/vo_2wjDHXyJfEXAMPLE", {})]
    if svc == "acm-pca":
        return [("UNUSED:no private CA", {})]
    if svc in ("application-autoscaling", "kinesis", "ec2", "elasticfilesystem", "firehose", "s3tables", "s3files"):
        return [(f"UNUSED:{svc}", {})]
    if svc == "cloudwatch" and action == "cloudwatch:PutMetricData":
        return [("*", {})]
    if svc == "s3" and action == "s3:REST.PUT.OBJECT":
        return [("UNUSED:not an IAM action", {})]
    if svc == "logs" and action in ("logs:CreateLogDelivery", "logs:PutResourcePolicy", "logs:DescribeResourcePolicies", "logs:DeleteResourcePolicy", "logs:PutBearerTokenAuthentication"):
        return [("*", {})]
    if svc == "ses" and ty == "AWS::SES::Template" and action in ("ses:CreateTemplate", "ses:GetTemplate", "ses:UpdateTemplate", "ses:DeleteTemplate"):
        pass
    # The resource itself (or '*' when the action takes no resource).
    if star:
        return [("*", {})]
    if arn is None:
        return [(f"UNMAPPED:{ty}", {})]
    if svc == "dynamodb" and action.startswith("dynamodb:") and ty == "AWS::DynamoDB::GlobalTable":
        return [(arn, {})]
    if svc == "logs" and ty == "AWS::Logs::LogGroup" and action in ("logs:DescribeLogGroups",):
        return [(f"{at('logs')}:log-group:*", {})]
    return [(arn, {})]


def is_create_of_self(action, ty):
    a = action.split(":", 1)[1].lower()
    return a.startswith(("create", "request", "publish", "put")) and not a.startswith(("putrole", "putfunction", "putkey"))



def build_rows(T):
    rows = {}
    TYPES_SEEN = set()
    for cfg, stacks in T.items():
        allst = {}
        for sn, s in stacks.items():
            allst[sn] = Stack(sn, s["template"], allst)
        for sn, st in allst.items():
            for lid, r in st.res.items():
                ty = r["Type"]
                if ty.startswith("Custom::"):
                    # CloudFormation invokes the provider function with the deploy role.
                    tok = st.resolve(r["Properties"]["ServiceToken"])
                    key = ("lambda:InvokeFunction", tok)
                    rows.setdefault(key, {"action": "lambda:InvokeFunction", "arn": tok, "context": {}, "why": set()})["why"].add(f"{ty} {sn}/{lid} create/update/delete")
                    continue
                if ty == "AWS::CDK::Metadata":
                    continue
                TYPES_SEEN.add(ty)
                sc = SCHEMA[ty]
                calls = []
                for h in ("create", "read", "update", "delete"):
                    for a in sc.get("handlers", {}).get(h, {}).get("permissions", []):
                        calls.append((h, a))
                tg = sc.get("tagging", {})
                if tg.get("taggable") or ty in ("AWS::DynamoDB::GlobalTable",):
                    for a in tg.get("permissions", []) or []:
                        calls.append(("tag", a))
                exp = []
                for h, a in calls:
                    s, name = a.split(":", 1)
                    if "*" in name:
                        rx = re.compile("^" + name.lower().replace("*", ".*") + "$")
                        exp += [(h, f"{s}:{x['Name']}") for k, x in SVC[s]["actions"].items() if rx.match(k)]
                    else:
                        exp.append((h, a))
                    # The API call of the same name may need more actions than
                    # the registry lists (AWS's service reference says which):
                    # CreateEventSourceMapping also needs lambda:TagResource.
                    # Only for the resource's own service, and only the actions of
                    # that service plus PassRole and KMS (an S3 call also lists
                    # S3 Object Lambda's actions, a CloudWatch one OAM's).
                    own = SERVICE_OF[ty.split("::")[1]]
                    op = SVC.get(s, {}).get("operations", {}).get(name.lower()) if s == own else None
                    if op:
                        exp += [(h, x) for x in op if x.lower() != a.lower() and (x.startswith(own + ":") or x == "iam:PassRole" or x.startswith("kms:"))]
                        # The registry's own action stays even when the service reference
                        # doesn't list it: AWS checks apigateway:TagResource all the same.
                exp = list(dict.fromkeys(exp))
                for h, a in exp:
                    s, name = a.split(":", 1)
                    # Normalize case to the service reference's spelling.
                    ref = SVC.get(s, {}).get("actions", {}).get(name.lower())
                    if ref: a = f"{s}:{ref['Name']}"
                    elif s == "apigateway" and name.upper() in ("GET", "POST", "PUT", "PATCH", "DELETE"): a = f"{s}:{name.upper()}"
                    tl = targets(st, lid, r, a)
                    rt = rtypes(a)
                    if rt == [] and a != "iam:PassRole":
                        # AWS gives this action no resource-level permission: it is checked against "*".
                        tl = [("*" if not x.startswith("UNUSED") else x, e) for x, e in tl]
                    elif rt == ["global-table"]:
                        tl = [(f"arn:aws:dynamodb::{ACCT}:global-table/{st.ident(lid)[0]}", e) for x, e in tl]
                    for arn, extra in tl:
                        ctx = {}
                        ck = condition_keys(a)
                        tagged = extra.pop("tagged", None)
                        ctx.update(extra)
                        if "aws:RequestTag/${TagKey}" in ck or a.endswith(("TagResource", "TagQueue")) or a in ("acm:AddTagsToCertificate", "ssm:AddTagsToResource", "iam:TagRole", "s3:PutBucketTagging", "cloudfront:CreateDistributionWithTags"):
                            ctx[f"aws:RequestTag/{TAG}"] = P
                            ctx["aws:TagKeys"] = [TAG]
                        # Every existing resource carries the install tag (the
                        # stack's tag); the resource being created does not yet.
                        if arn != "*" and not arn.startswith(("UNUSED", "UNMAPPED")) and (tagged or not (h == "create" and is_create_of_self(a, ty))):
                            ctx[f"aws:ResourceTag/{TAG}"] = P
                        if a in ("iam:CreateRole", "iam:PutRolePolicy", "iam:AttachRolePolicy", "iam:DetachRolePolicy", "iam:DeleteRolePolicy", "iam:PutRolePermissionsBoundary"):
                            ctx["iam:PermissionsBoundary"] = BOUNDARY
                        key = (a, arn, json.dumps(ctx, sort_keys=True))
                        row = rows.setdefault(key, {"action": a, "arn": arn, "context": ctx, "why": set()})
                        row["why"].add(f"{ty} {h}")

    return rows, TYPES_SEEN


def load(session):
    cfn = session.client("cloudformation", region_name=REG)
    for ty in sorted(TYPES):
        s = json.loads(cfn.describe_type(Type="RESOURCE", TypeName=ty)["Schema"])
        SCHEMA[ty] = s
    for svc in SERVICES:
        with urllib.request.urlopen(f"https://servicereference.us-east-1.amazonaws.com/v1/{svc}/{svc}.json") as f:
            d = json.load(f)
        SVC[d["Name"]] = {"actions": {a["Name"].lower(): a for a in d["Actions"]}, "resources": {r["Name"]: r for r in d["Resources"]},
                          "operations": {o["Name"].lower(): [f"{x['Service']}:{x['Name']}" for x in o.get("AuthorizedActions", [])] for o in d.get("Operations", [])}}


def simulate(session, deploy, boundary, rows):
    iam = session.client("iam")
    groups = collections.defaultdict(list)
    for i, r in enumerate(rows):
        groups[(r["arn"], json.dumps(r["context"], sort_keys=True))].append(i)
    D, B = json.dumps(deploy), json.dumps(boundary)

    def entries(ctx):
        return [{"ContextKeyName": k, "ContextKeyValues": v if isinstance(v, list) else [v], "ContextKeyType": "stringList" if isinstance(v, list) else "string"} for k, v in ctx.items()]

    def run(key):
        arn, ctx = key[0], json.loads(key[1])
        res = {}

        def sim(chunk):
            for attempt in range(8):
                try:
                    for page in iam.get_paginator("simulate_custom_policy").paginate(
                        PolicyInputList=[D], PermissionsBoundaryPolicyInputList=[B], ActionNames=chunk, ResourceArns=[arn], ContextEntries=entries(ctx)):
                        for ev in page["EvaluationResults"]:
                            rr = ev.get("ResourceSpecificResults") or []
                            res[ev["EvalActionName"]] = rr[0]["EvalResourceDecision"] if rr else ev["EvalDecision"]
                    return
                except iam.exceptions.InvalidInputException as e:
                    # Some actions (apigateway:TagResource) can't share a call with others.
                    if len(chunk) > 1 and "different authorization" in str(e):
                        for a in chunk:
                            sim([a])
                        return
                    raise
                except iam.exceptions.ClientError as e:
                    if "Throttl" in str(e) or "Rate" in str(e):
                        time.sleep(2 ** attempt)
                        continue
                    raise

        actions = sorted({rows[i]["action"] for i in groups[key]})
        for j in range(0, len(actions), 40):
            sim(actions[j:j + 40])
        return key, res

    with cf.ThreadPoolExecutor(8) as ex:
        for key, res in ex.map(run, list(groups)):
            for i in groups[key]:
                rows[i]["simulator"] = res[rows[i]["action"]]


TYPES = set()


STS_ACTIONS = ["sts:AssumeRole", "sts:AssumeRoleWithWebIdentity", "sts:AssumeRoleWithSAML", "sts:TagSession", "sts:SetSourceIdentity"]
TAG_ACTIONS = ["iam:CreateRole", "iam:TagRole", "iam:UntagRole"]
RUNTIME_TAG = "coderhelm:role"
RUNTIME_IDENTITY = {"Version": "2012-10-17", "Statement": [{"Sid": "AllStsAndRoleTags", "Effect": "Allow", "Action": ["sts:*", *TAG_ACTIONS], "Resource": "*"}]}


def runtime_rows(names):
    """The boundary's own-account role rules: sts actions by each caller's role tag, and who may set that tag."""
    callers = {"gateway": "gateway", "worker": "worker", "log-analyzer": "log-analyzer", "lookalike tag": "Gateway", "untagged": None}
    roles = ["CoderHelmLogReader", "CoderhelmReadOnly", "CoderhelmReadOnly-2", "CoderHelmLogReaderAdmin", "team/CoderhelmReadOnly",
             f"cdk-hnb659fds-cfn-exec-role-{ACCT}-{REG}", f"cdk-hnb659fds-deploy-role-{ACCT}-{REG}", names["deployRoleName"], f"{P}-ServiceRole-ab12cd34", f"{P}-update-build"]
    rows = []
    for r in roles:
        for who, tag in callers.items():
            ctx = {"aws:ResourceAccount": ACCT, **({f"aws:PrincipalTag/{RUNTIME_TAG}": tag} if tag else {})}
            rows.append({"resource": f"arn:aws:iam::{ACCT}:role/{r}", "caller": who, "context": ctx, "names": STS_ACTIONS})
    # Another account's log reader: unchanged by the own-account rules.
    rows.append({"resource": "arn:aws:iam::999988887777:role/CoderHelmLogReader", "caller": "untagged", "context": {"aws:ResourceAccount": "999988887777"}, "names": STS_ACTIONS})
    # The tag: only the deploy role sets or removes it.
    other = f"arn:aws:iam::{ACCT}:role/{P}-update-build"
    for who, principal in (("deploy role", names["deployRoleArn"]), ("another install role", other)):
        for keys in ([RUNTIME_TAG], ["Coderhelm:Role"], ["other"]):
            rows.append({"resource": f"arn:aws:iam::{ACCT}:role/{P}-ServiceRole-ab12cd34", "caller": who,
                         "context": {"aws:PrincipalArn": principal, "aws:TagKeys": keys, "iam:PermissionsBoundary": names["boundaryArn"]}, "names": TAG_ACTIONS})
    return rows


def simulate_runtime(session, boundary, rows):
    iam = session.client("iam")
    I, B = json.dumps(RUNTIME_IDENTITY), json.dumps(boundary)
    for r in rows:
        ctx = [{"ContextKeyName": k, "ContextKeyValues": v if isinstance(v, list) else [v], "ContextKeyType": "stringList" if isinstance(v, list) else "string"} for k, v in r["context"].items()]
        res = {}
        for page in iam.get_paginator("simulate_custom_policy").paginate(
                PolicyInputList=[I], PermissionsBoundaryPolicyInputList=[B], ActionNames=r.pop("names"), ResourceArns=[r["resource"]], ContextEntries=ctx):
            for ev in page["EvaluationResults"]:
                rr = ev.get("ResourceSpecificResults") or []
                res[ev["EvalActionName"]] = rr[0]["EvalResourceDecision"] if rr else ev["EvalDecision"]
        r["actions"] = dict(sorted(res.items()))


def grouped(simulated):
    """Rows that share a resource and a context, as one row with each action's verdict."""
    g = {}
    for r in simulated:
        k = (r["arn"], json.dumps(r["context"], sort_keys=True))
        row = g.setdefault(k, {"resource": r["arn"], "context": r["context"], "actions": {}, "calls": set()})
        row["actions"][r["action"]] = r["simulator"]
        row["calls"].update(c.split(" ", 1)[0] + " " + c.rsplit(" ", 1)[-1] if c.startswith("Custom::") else c for c in r["why"])
    return [{**v, "actions": dict(sorted(v["actions"].items())), "calls": sorted(v["calls"])} for v in sorted(g.values(), key=lambda x: (x["resource"], json.dumps(x["context"], sort_keys=True)))]


NATIVE_JS = r"""
const fs = require("fs"), os = require("os"), path = require("path");
const { buildNative } = require("./dist/lib/native/build");
const { resolveValue, prodValues } = require("./dist/lib/native/resolve");
const { accessNames, boundaryPolicy } = require("./dist/lib/access");
const { deployRolePolicy, helperPolicy } = require("./dist/lib/native/parent");
const [ACCT, REG] = process.argv.slice(1);
const STUBS = fs.mkdtempSync(path.join(os.tmpdir(), "chsimn-"));
for (const d of ["gateway", "worker", "mcp"]) { fs.mkdirSync(path.join(STUBS, d), { recursive: true }); fs.writeFileSync(path.join(STUBS, d, d === "mcp" ? "handler.js" : "bootstrap"), "stub"); }
fs.mkdirSync(path.join(STUBS, "dash"), { recursive: true }); fs.writeFileSync(path.join(STUBS, "dash", "index.html"), "x");
process.env.GATEWAY_ZIP = path.join(STUBS, "gateway"); process.env.WORKER_ZIP = path.join(STUBS, "worker"); process.env.MCP_PROXY_DIR = path.join(STUBS, "mcp");
const rel = buildNative("v0.0.0", fs.mkdtempSync(path.join(os.tmpdir(), "chsimrel-")), { dashboardDir: path.join(STUBS, "dash") });
const values = prodValues(ACCT, REG);
const stackOf = (part) => "coderhelm-prod-" + part.replace(/([a-z])([A-Z])/g, "$1-$2").toLowerCase();
const imports = {};
for (const [part, t] of Object.entries(rel.children)) {
  for (const [p, v] of Object.entries(rel.parent.Resources[part].Properties.Parameters)) {
    const g = v["Fn::GetAtt"];
    if (!g) continue;
    const exp = rel.children[g[0]].Outputs[g[1].replace("Outputs.", "")].Export.Name;
    imports[part + "/" + p] = resolveValue(exp, { values });
  }
}
const out = { templates: { native: {} } };
for (const [part, t] of Object.entries(rel.children)) {
  const im = Object.fromEntries(Object.entries(imports).filter(([k]) => k.startsWith(part + "/")).map(([k, v]) => [k.split("/")[1], v]));
  out.templates.native[stackOf(part)] = { template: resolveValue(t, { values, imports: im }), tags: { "coderhelm:install": "coderhelm-prod" } };
}
// The parent's own roles and policies, as one more part.
out.templates.native["coderhelm-prod"] = { template: resolveValue({ Resources: Object.fromEntries(Object.entries(rel.parent.Resources).filter(([, r]) => r.Type.startsWith("AWS::"))) }, { values }), tags: {} };
const n = accessNames({ account: ACCT, region: REG, prefix: "coderhelm-prod", suffix: "-" + ACCT, secretName: "coderhelm/prod/secrets", id: "stack" });
out.names = n;
out.deploy = resolveValue(deployRolePolicy(accessNames({ partition: "${AWS::Partition}", account: "${AWS::AccountId}", region: "${AWS::Region}", prefix: "coderhelm-${Stage}", suffix: "-${AWS::AccountId}", secretName: "coderhelm/${Stage}/secrets", id: "stack" })).Statement.map((s) => JSON.parse(JSON.stringify(s).replace(/"([^"]*\$\{[^"]*)"/g, (m, x) => JSON.stringify({ "Fn::Sub": x })))), { values });
out.deploy = { Version: "2012-10-17", Statement: out.deploy };
out.boundary = boundaryPolicy(n, ["arn:aws:iam::" + ACCT + ":policy/coderhelm-prod-deploy-boundary-stack"]);
const helper = helperPolicy();
out.helper = JSON.parse(JSON.stringify(helper).replace(/\$\{Stage\}/g, "prod").replace(/\$\{StatusId\}/g, "0123456789abcdef").replace(/\$\{AWS::AccountId\}/g, ACCT).replace(/\$\{AWS::Region\}/g, REG).replace(/\$\{AWS::Partition\}/g, "aws").replace(/\$\{AWS::StackName\}/g, "coderhelm-prod"));
process.stdout.write(JSON.stringify(out));
"""


def helper_rows():
    """What the install helper may and may not do (infra/lib/native/parent.ts, helperPolicy)."""
    s3 = "arn:aws:s3:::"
    allowed = [
        ("s3:PutObject", f"{s3}{P}-release-{ACCT}/assets/a.zip"), ("s3:DeleteObject", f"{s3}{P}-release-{ACCT}/assets/a.zip"),
        ("s3:ListBucketVersions", f"{s3}{P}-data-{ACCT}"), ("s3:DeleteObjectVersion", f"{s3}{P}-data-{ACCT}/x"),
        ("s3:PutObject", f"{s3}coderhelm-setup-0123456789abcdef/status.json"),
        ("secretsmanager:PutSecretValue", f"{at('secretsmanager')}:secret:coderhelm/prod/secrets-AbCdEf"),
        ("dynamodb:DeleteTable", f"{at('dynamodb')}:table/{P}-traces"), ("dynamodb:BatchWriteItem", f"{at('dynamodb')}:table/{P}-settings"),
        # The main table has no suffix (the live test's failed delete).
        ("dynamodb:DescribeTable", f"{at('dynamodb')}:table/{P}"), ("dynamodb:DeleteTable", f"{at('dynamodb')}:table/{P}"),
        ("dynamodb:UpdateTable", f"{at('dynamodb')}:table/{P}"),
        # Final, with KeepDataOnDelete=false.
        ("secretsmanager:DeleteSecret", f"{at('secretsmanager')}:secret:coderhelm/prod/secrets-AbCdEf"),
        ("logs:DeleteLogGroup", f"{at('logs')}:log-group:/aws/lambda/{P}-Handler-cd8e80af"),
        ("logs:DeleteLogGroup", f"{at('logs')}:log-group:/aws/codebuild/{P}-release-check"),
        ("lambda:InvokeFunction", f"{at('lambda')}:function:{P}-updater"),
        ("logs:DeleteLogGroup", f"{at('logs')}:log-group:/aws/lambda/{P}-gateway"),
        ("cloudformation:DescribeStacks", f"{at('cloudformation')}:stack/{P}/0f0f0f0f-1111-2222-3333-444455556666"),
    ]
    denied = [
        ("s3:PutObject", f"{s3}{P}-data-{ACCT}/x"), ("s3:DeleteObject", f"{s3}someone-else/x"), ("s3:PutObject", f"{s3}coderhelm-other-release-{ACCT}/x"),
        ("secretsmanager:PutSecretValue", f"{at('secretsmanager')}:secret:coderhelm/other/secrets-AbCdEf"),
        ("dynamodb:DeleteTable", f"{at('dynamodb')}:table/orders"), ("dynamodb:DeleteTable", f"{at('dynamodb')}:table/coderhelm-other-traces"),
        ("dynamodb:DeleteTable", f"{at('dynamodb')}:table/{P}x"), ("dynamodb:DescribeTable", f"{at('dynamodb')}:table/coderhelm-other"),
        ("secretsmanager:DeleteSecret", f"{at('secretsmanager')}:secret:coderhelm/other/secrets-AbCdEf"),
        ("secretsmanager:DeleteSecret", f"{at('secretsmanager')}:secret:payments-AbCdEf"),
        ("lambda:InvokeFunction", f"{at('lambda')}:function:{P}-gateway"), ("lambda:UpdateFunctionCode", f"{at('lambda')}:function:{P}-updater"),
        ("logs:DeleteLogGroup", f"{at('logs')}:log-group:/aws/lambda/payments"), ("logs:CreateLogGroup", f"{at('logs')}:log-group:/aws/lambda/{P}-install-helper"),
        ("iam:CreateRole", f"arn:aws:iam::{ACCT}:role/{P}-x"), ("cloudformation:DeleteStack", f"{at('cloudformation')}:stack/{P}/0f0f0f0f-1111-2222-3333-444455556666"),
    ]
    return [{"action": a, "resource": r, "want": "allowed"} for a, r in allowed] + [{"action": a, "resource": r, "want": "denied"} for a, r in denied]


def simulate_helper(session, helper, boundary, rows):
    iam = session.client("iam")
    for r in rows:
        ev = iam.simulate_custom_policy(PolicyInputList=[json.dumps(helper)], PermissionsBoundaryPolicyInputList=[json.dumps(boundary)],
                                        ActionNames=[r["action"]], ResourceArns=[r["resource"]])["EvaluationResults"][0]
        rr = ev.get("ResourceSpecificResults") or []
        r["simulator"] = rr[0]["EvalResourceDecision"] if rr else ev["EvalDecision"]


def main():
    global BOUNDARY
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=None)
    ap.add_argument("--native", action="store_true", help="the native release: its parts, the auto-update role, the helper")
    args = ap.parse_args()
    import boto3
    session = boto3.Session()
    args.out = args.out or ("test/fixtures/native-sim.json" if args.native else "test/fixtures/deploy-role-sim.json")
    js = [NATIVE_JS, ACCT, REG] if args.native else [SYNTH_JS, ACCT, REG, LAUNCH_ID]
    synth = json.loads(subprocess.run(["node", "-e", *js], check=True, capture_output=True, text=True).stdout)
    T = synth["templates"]
    BOUNDARY = synth["names"]["boundaryArn"]
    for stacks in T.values():
        for s in stacks.values():
            for r in s["template"].get("Resources", {}).values():
                if r["Type"].startswith("AWS::") and r["Type"] != "AWS::CDK::Metadata":
                    TYPES.add(r["Type"])
    load(session)
    rows, types = build_rows(T)
    simulated, unused = [], []
    for v in rows.values():
        v["why"] = sorted(v["why"])
        if v["arn"].startswith("UNUSED"):
            unused.append({"action": v["action"], "calls": v["why"], "reason": v["arn"].split(":", 1)[1]})
        elif v["arn"].startswith("UNMAPPED"):
            sys.exit(f"no ARN for {v['action']} ({v['arn']})")
        else:
            simulated.append(v)
    simulated.sort(key=lambda x: (x["arn"], x["action"]))
    if args.native:
        # The caller is the auto-update role (CloudFormation uses it): conditions on aws:PrincipalArn see it.
        for v in simulated:
            v["context"] = {**v["context"], "aws:PrincipalArn": synth["names"]["deployRoleArn"]}
    simulate(session, synth["deploy"], synth["boundary"], simulated)
    runtime = runtime_rows(synth["names"])
    simulate_runtime(session, synth["boundary"], runtime)
    helper = []
    if args.native:
        helper = helper_rows()
        simulate_helper(session, synth["helper"], synth["boundary"], helper)
    collapsed = {}
    for u in unused:
        c = collapsed.setdefault((u["action"], u["reason"]), {"action": u["action"], "reason": u["reason"], "calls": set()})
        c["calls"].update(u["calls"])
    fixture = {
        "about": ("Generated by infra/scripts/deploy-role-sim.py --native: every IAM call CloudFormation's handlers make for a native install's parts and parent, by its auto-update role under the boundary, what the boundary leaves runtime roles, and what the install helper may and may not do, with AWS's IAM policy simulator verdicts."
                  if args.native else "Generated by infra/scripts/deploy-role-sim.py: every IAM call CloudFormation's handlers make for a hardened self-host install, with AWS's IAM policy simulator verdict for the policies below."),
        "account": ACCT, "region": REG, "prefix": P, "launchId": "" if args.native else LAUNCH_ID,
        "policies": {"deploy": synth["deploy"], "boundary": synth["boundary"]},
        "types": sorted(types),
        "rows": grouped(simulated),
        "runtime": {"identity": RUNTIME_IDENTITY, "rows": runtime},
        **({"helper": {"policy": synth["helper"], "rows": helper}} if args.native else {}),
        "notSimulated": sorted(({**c, "calls": sorted(c["calls"])} for c in collapsed.values()), key=lambda x: (x["action"], x["reason"])),
    }
    with open(args.out, "w") as f:
        # One row per line: a resource, the context its calls carry, and each action's verdict.
        head = {k: v for k, v in fixture.items() if k not in ("rows", "notSimulated", "runtime", "helper")}
        f.write(json.dumps(head, indent=1)[:-2] + ',\n "rows": [\n')
        f.write(",\n".join("  " + json.dumps(r, sort_keys=True) for r in fixture["rows"]))
        f.write('\n ],\n "runtime": {\n  "identity": ' + json.dumps(RUNTIME_IDENTITY, sort_keys=True) + ',\n  "rows": [\n')
        f.write(",\n".join("   " + json.dumps(r, sort_keys=True) for r in runtime))
        f.write('\n  ]\n },\n')
        if "helper" in fixture:
            f.write(' "helper": {\n  "policy": ' + json.dumps(fixture["helper"]["policy"], sort_keys=True) + ',\n  "rows": [\n')
            f.write(",\n".join("   " + json.dumps(r, sort_keys=True) for r in fixture["helper"]["rows"]))
            f.write('\n  ]\n },\n')
        f.write(' "notSimulated": [\n')
        f.write(",\n".join("  " + json.dumps(r, sort_keys=True) for r in fixture["notSimulated"]))
        f.write("\n ]\n}\n")
    c = collections.Counter(r["simulator"] for r in simulated)
    rc = collections.Counter(v for r in runtime for v in r["actions"].values())
    print(f"runtime: {len(runtime)} (role, caller) rows: {dict(rc)}")
    print(f"{len(simulated)} (action, resource) pairs simulated: {dict(c)}; {len(fixture['notSimulated'])} registry calls not simulated")


if __name__ == "__main__":
    main()
