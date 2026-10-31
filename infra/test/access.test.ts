// What a hardened install can do in its account (infra/lib/access.ts,
// docs/self-host/security.md), proved on the synthesized stacks with a small
// IAM evaluator: identity policy AND permissions boundary, explicit denies
// first, the condition operators these policies use. And an install without
// `access` synthesizes its update path exactly as before.
//
//   npm test   (compiles with tsc, then runs node --test)

import { test } from "node:test";
import * as assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import * as cdk from "aws-cdk-lib";
import { buildApp } from "../lib/app";
import { resolveConfig, ResolvedConfig, CoderhelmConfig } from "../lib/config";
import { AccessNames, accessNames, boundaryPolicy, BOUNDARY_SERVICES, OWN_READ_ROLES, RUNTIME_ROLE_TAG, RUNTIME_ROLES, deployPolicy, INSTALL_TAG, PolicyDocument, Statement, WINDOW_EXPIRES, WINDOW_USER_ID, windowPolicy, windowPolicyParts } from "../lib/access";
import { allowed, glob, list, Request } from "./iam-eval";
import { installName } from "../lib/hardening";
import { buildSpecSha256, canonicalJson, updateBuildSpecObject, UPDATE_BUILD_VARIABLES } from "../lib/updates-stack";
import { repoPath } from "../lib/paths";

const STUBS = fs.mkdtempSync(path.join(os.tmpdir(), "chacc-"));
for (const d of ["gateway", "worker", "mcp"]) {
  fs.mkdirSync(path.join(STUBS, d), { recursive: true });
  fs.writeFileSync(path.join(STUBS, d, d === "mcp" ? "handler.js" : "bootstrap"), "stub");
}
process.env.GATEWAY_ZIP = path.join(STUBS, "gateway");
process.env.WORKER_ZIP = path.join(STUBS, "worker");
process.env.MCP_PROXY_DIR = path.join(STUBS, "mcp");

const ACCOUNT = "111122223333";
const BASE: CoderhelmConfig = { account: ACCOUNT, region: "us-west-2", stage: "prod", email: { from: "a@example.org" }, admins: ["o@example.org"] };

function synth(over: Partial<CoderhelmConfig> = {}, opts: { accessStack?: boolean } = {}): Record<string, any> {
  const cfg = resolveConfig({ ...BASE, ...over });
  const app = new cdk.App({ outdir: fs.mkdtempSync(path.join(os.tmpdir(), "chaccout-")) });
  buildApp(app, cfg, opts);
  const out: Record<string, any> = {};
  for (const s of app.synth().stacks) out[s.stackName] = s.template;
  return out;
}

const hardened = (over: Partial<CoderhelmConfig> = {}) => synth({ access: { mode: "hardened", owner: "app" }, ...over }, { accessStack: true });
const names = (): AccessNames => resolveConfig({ ...BASE, access: { mode: "hardened", owner: "app" } }).access!;

// The IAM evaluator (test/iam-eval.ts) is checked against AWS's policy
// simulator by test/deploy-role-sim.test.ts.

/** A synthesized policy statement, its intrinsics made plain (Fn::Join of strings, Fn::GetAtt as a placeholder). */
function plain(v: any): any {
  if (typeof v === "string" || typeof v !== "object" || v === null) return v;
  if (Array.isArray(v)) return v.map(plain);
  if (v["Fn::Join"]) return (v["Fn::Join"][1] as any[]).map((x) => (typeof x === "string" ? x : String(plain(x)))).join(v["Fn::Join"][0]);
  if (v.Ref === "AWS::Partition") return "aws";
  if (v.Ref === "AWS::Region") return "us-west-2";
  if (v.Ref === "AWS::AccountId") return ACCOUNT;
  if (v["Fn::GetAtt"] || v.Ref || v["Fn::ImportValue"]) return `arn:aws:resolved:us-west-2:${ACCOUNT}:${JSON.stringify(v).replace(/[^A-Za-z0-9]/g, "")}`;
  return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, plain(x)]));
}

/** Every identity policy on a synthesized role (inline, AWS::IAM::Policy and managed ones in the same stack). */
function rolePolicies(t: any, logicalId: string): PolicyDocument[] {
  const r = t.Resources[logicalId];
  const docs: PolicyDocument[] = (r.Properties.Policies ?? []).map((p: any) => plain(p.PolicyDocument));
  for (const x of Object.values<any>(t.Resources)) {
    const roles = JSON.stringify(x.Properties?.Roles ?? []);
    if ((x.Type === "AWS::IAM::Policy" || x.Type === "AWS::IAM::ManagedPolicy") && roles.includes(`"${logicalId}"`)) docs.push(plain(x.Properties.PolicyDocument));
  }
  return docs;
}

function roleByName(stacks: Record<string, any>, name: string): { t: any; id: string; role: any } {
  for (const t of Object.values(stacks)) {
    for (const [id, r] of Object.entries<any>(t.Resources ?? {})) if (r.Type === "AWS::IAM::Role" && r.Properties.RoleName === name) return { t, id, role: r };
  }
  throw new Error(`no role ${name}`);
}

const n = names();
const BOUNDARY = boundaryPolicy(n);
const DEPLOY = deployPolicy(n);
const at = (svc: string, rest: string) => `arn:aws:${svc}:us-west-2:${ACCOUNT}:${rest}`;
const role = (name: string) => `arn:aws:iam::${ACCOUNT}:role/${name}`;
const SESSION = "AROAEXAMPLEROLEID01:AWSCodeBuild-1111";
const NOW = "2026-10-29T12:00:00Z";
const WINDOW = JSON.parse(JSON.stringify(windowPolicy(n)).split(WINDOW_USER_ID).join(SESSION).split(WINDOW_EXPIRES).join("2026-10-29T13:40:00Z")) as PolicyDocument;

// ── The boundary ─────────────────────────────────────────────────────────

test("boundary: even a role whose own policy allows everything stays inside it", () => {
  const everything: PolicyDocument = { Version: "2012-10-17", Statement: [{ Sid: "All", Effect: "Allow", Action: "*", Resource: "*" }] };
  const can = (action: string, resource = "*", context = {}) => allowed([everything], BOUNDARY, { action, resource, context });
  assert.ok(can("dynamodb:GetItem", at("dynamodb", "table/coderhelm-prod")));
  assert.ok(can("iam:CreateRole", role("coderhelm-prod-x"), { "iam:PermissionsBoundary": n.boundaryArn }));
  for (const [action, resource, context] of [
    ["iam:CreateUser", "*"],
    ["iam:CreateAccessKey", `arn:aws:iam::${ACCOUNT}:user/admin`],
    ["iam:CreateRole", role("coderhelm-prod-x"), {}],
    ["iam:CreateRole", role("coderhelm-prod-x"), { "iam:PermissionsBoundary": `arn:aws:iam::${ACCOUNT}:policy/other` }],
    ["iam:CreateRole", role("admin"), { "iam:PermissionsBoundary": n.boundaryArn }],
    ["iam:AttachRolePolicy", role("customer-admin"), { "iam:PermissionsBoundary": n.boundaryArn }],
    ["iam:PassRole", role("customer-admin")],
    ["iam:DeleteRolePermissionsBoundary", role("coderhelm-prod-x")],
    ["iam:CreatePolicyVersion", n.boundaryArn],
    ["iam:DeletePolicy", n.boundaryArn],
    ["iam:PutRolePolicy", n.deployRoleArn, { "iam:PermissionsBoundary": n.boundaryArn }],
    ["iam:UpdateAssumeRolePolicy", n.deployRoleArn],
    ["sts:AssumeRole", role("cdk-hnb659fds-cfn-exec-role-111122223333-us-west-2"), { "aws:ResourceAccount": ACCOUNT }],
    ["organizations:LeaveOrganization", "*"],
    ["account:CloseAccount", "*"],
    ["cloudtrail:StopLogging", "*"],
    ["guardduty:DeleteDetector", "*"],
    ["ec2:RunInstances", "*"],
    ["cloudformation:UpdateStack", at("cloudformation", "stack/coderhelm-prod-access/x")],
  ] as [string, string, Record<string, string>?][]) {
    assert.ok(!can(action, resource, context), `the boundary refuses ${action} on ${resource}`);
  }
  // A log reader role in another account can still be assumed (log reading).
  assert.ok(can("sts:AssumeRole", "arn:aws:iam::999988887777:role/coderhelm-log-reader", { "aws:ResourceAccount": "999988887777" }));
});

// An install in the same AWS account as the data it reads takes on
// CoderHelm's own read roles there. The boundary allows exactly that: only
// sts:AssumeRole, only on those two names, only by the gateway's and the
// worker's roles (their coderhelm:role tag).
test("boundary: in its own account, only the gateway and worker may assume only CoderHelm's two read roles", () => {
  const sts: PolicyDocument = { Version: "2012-10-17", Statement: [{ Sid: "AllSts", Effect: "Allow", Action: "sts:*", Resource: "*" }] };
  const can = (action: string, resource: string, tag?: string) =>
    allowed([sts], BOUNDARY, { action, resource, context: { "aws:ResourceAccount": resource.split(":")[4], ...(tag ? { [`aws:PrincipalTag/${RUNTIME_ROLE_TAG}`]: tag } : {}) } });
  for (const r of ["CoderHelmLogReader", "CoderhelmReadOnly"]) {
    for (const t of RUNTIME_ROLES) assert.ok(can("sts:AssumeRole", role(r), t), `${t} may assume ${r} here`);
    // Any other caller: another function's role, a build, an untagged role, a lookalike tag value.
    for (const t of ["log-analyzer", "update-build", "Gateway", "gateway-x", "", undefined]) assert.ok(!can("sts:AssumeRole", role(r), t), `${t ?? "no tag"} may not assume ${r}`);
    // Only AssumeRole: no other STS action on them, even from the gateway.
    for (const a of ["sts:TagSession", "sts:SetSourceIdentity", "sts:AssumeRoleWithWebIdentity", "sts:AssumeRoleWithSAML"]) assert.ok(!can(a, role(r), "gateway"), `${a} on ${r}`);
  }
  // Every other role here stays refused, even for the gateway and worker:
  // lookalike names, a path, other case, the CDK bootstrap, the deploy role, the install's own.
  for (const r of [
    "CoderhelmReadOnly-2", "CoderhelmReadOnlyAdmin", "CoderHelmLogReaderAdmin", "team/CoderhelmReadOnly", "coderhelmreadonly", "CoderHelmReadOnly",
    "cdk-hnb659fds-cfn-exec-role-111122223333-us-west-2", n.deployRoleName, "coderhelm-prod-ServiceRole-ab12cd34", "coderhelm-prod-update-build", "OrganizationAccountAccessRole",
  ]) {
    for (const t of RUNTIME_ROLES) {
      for (const a of ["sts:AssumeRole", "sts:TagSession", "sts:SetSourceIdentity", "sts:AssumeRoleWithWebIdentity"]) assert.ok(!can(a, role(r), t), `${t}: ${a} on ${r}`);
    }
  }
  // Other accounts are unchanged: the log reader there by name.
  assert.ok(can("sts:AssumeRole", "arn:aws:iam::999988887777:role/CoderHelmLogReader"));
});

test("boundary: fits IAM's managed policy limit (6,144 characters without whitespace) with room to spare", () => {
  const size = JSON.stringify(BOUNDARY).length;
  assert.ok(size < 5600, `boundary is ${size} characters`);
});

test("boundary: only the deploy role may set or remove the gateway and worker tag", () => {
  const everything: PolicyDocument = { Version: "2012-10-17", Statement: [{ Sid: "All", Effect: "Allow", Action: "*", Resource: "*" }] };
  const can = (action: string, keys: string[], principal: string) =>
    allowed([everything], BOUNDARY, { action, resource: role("coderhelm-prod-ServiceRole-ab12cd34"), context: { "aws:TagKeys": keys, "aws:PrincipalArn": principal, "iam:PermissionsBoundary": n.boundaryArn } });
  for (const a of ["iam:TagRole", "iam:UntagRole", "iam:CreateRole"]) {
    assert.ok(can(a, [RUNTIME_ROLE_TAG], n.deployRoleArn), `the deploy role: ${a}`);
    for (const keys of [[RUNTIME_ROLE_TAG], ["Coderhelm:Role"], ["other", RUNTIME_ROLE_TAG]]) {
      assert.ok(!can(a, keys, role("coderhelm-prod-update-build")), `another role: ${a} ${keys}`);
    }
    assert.ok(can(a, ["other"], role("coderhelm-prod-update-build")), `other tags unchanged: ${a}`);
  }
});

test("boundary: the read role names are the ones CoderHelm's own template and setup script create; only the gateway and worker roles carry the tag", () => {
  const tmpl = fs.readFileSync(repoPath("infra", "cfn", "coderhelm-log-reader.yaml"), "utf8");
  assert.match(tmpl, /^\s+RoleName: CoderHelmLogReader$/m);
  const aws = fs.readFileSync(repoPath("services", "common", "src", "live", "aws.rs"), "utf8");
  assert.match(aws, /pub const ROLE_PREFIX: &str = "CoderhelmReadOnly";/);
  assert.match(aws, /^ROLE=\{ROLE_PREFIX\}$/m, "the setup script creates exactly ROLE_PREFIX");
  assert.match(aws, /pub const LOG_READER_ROLE: &str = "CoderHelmLogReader";/);
  assert.deepEqual(OWN_READ_ROLES, ["CoderHelmLogReader", "CoderhelmReadOnly"]);
  const stacks = hardened();
  const tagged: Record<string, string> = {};
  for (const t of Object.values<any>(stacks)) {
    const res = t.Resources ?? {};
    for (const [id, r] of Object.entries<any>(res)) {
      if (r.Type !== "AWS::IAM::Role") continue;
      const tag = (r.Properties.Tags ?? []).find((x: any) => x.Key === RUNTIME_ROLE_TAG);
      if (!tag) continue;
      const fn = Object.values<any>(res).find((f) => f.Type === "AWS::Lambda::Function" && JSON.stringify(f.Properties.Role).includes(`"${id}"`));
      tagged[tag.Value] = fn?.Properties.FunctionName;
    }
  }
  assert.deepEqual(tagged, { gateway: "coderhelm-prod-gateway", worker: "coderhelm-prod-worker" });
});

test("boundary: allows every action every synthesized CoderHelm role is given", () => {
  const stacks = hardened();
  const missing = new Set<string>();
  for (const t of Object.values(stacks)) {
    for (const r of Object.values<any>(t.Resources ?? {})) {
      const docs = r.Type === "AWS::IAM::Role" ? (r.Properties.Policies ?? []).map((p: any) => p.PolicyDocument) : r.Type === "AWS::IAM::Policy" || r.Type === "AWS::IAM::ManagedPolicy" ? [r.Properties.PolicyDocument] : [];
      if (r.Properties?.ManagedPolicyName === n.boundaryName) continue;
      for (const d of docs) for (const s of d.Statement) {
        if (s.Effect !== "Allow") continue;
        for (const a of list(s.Action)) if (!BOUNDARY.Statement.some((b) => b.Effect === "Allow" && list(b.Action).some((x) => glob(x, a, true) || glob(a, x, true)))) missing.add(a);
      }
    }
  }
  assert.deepEqual([...missing], [], "actions the boundary would silently refuse");
});

// ── The deploy role ──────────────────────────────────────────────────────

test("deploy role: creates and changes only the install's roles, only with the boundary", () => {
  const can = (action: string, resource: string, context = {}) => allowed([DEPLOY], BOUNDARY, { action, resource, context });
  const withBoundary = { "iam:PermissionsBoundary": n.boundaryArn };
  assert.ok(can("iam:CreateRole", role("coderhelm-prod-ServiceRole-ab12cd34"), withBoundary));
  assert.ok(can("iam:PutRolePolicy", role("coderhelm-prod-ServiceRole-ab12cd34"), withBoundary));
  assert.ok(can("iam:PassRole", role("coderhelm-prod-ServiceRole-ab12cd34")));
  // Even AdministratorAccess on one of its roles is capped by the boundary that role carries.
  assert.ok(can("iam:AttachRolePolicy", role("coderhelm-prod-x"), { ...withBoundary, "iam:PolicyARN": "arn:aws:iam::aws:policy/AdministratorAccess" }));
  for (const [action, resource, context] of [
    ["iam:CreateRole", role("coderhelm-prod-x"), {}],
    ["iam:CreateRole", role("admin-for-me"), withBoundary],
    ["iam:PassRole", role("OrganizationAccountAccessRole")],
    ["iam:PassRole", role("cdk-hnb659fds-cfn-exec-role-111122223333-us-west-2")],
    ["iam:CreateUser", `arn:aws:iam::${ACCOUNT}:user/x`],
    ["iam:DeleteRolePermissionsBoundary", role("coderhelm-prod-x")],
    ["iam:CreatePolicyVersion", n.boundaryArn],
    ["iam:PutRolePolicy", n.deployRoleArn, withBoundary],
    ["lambda:UpdateFunctionCode", at("lambda", "function:payroll")],
    ["s3:DeleteBucket", "arn:aws:s3:::company-backups"],
    ["dynamodb:DeleteTable", at("dynamodb", "table/orders")],
    ["kms:ScheduleKeyDeletion", at("kms", "key/1234"), {}],
    ["kms:ScheduleKeyDeletion", at("kms", "key/1234"), { [`aws:ResourceTag/${INSTALL_TAG}`]: "coderhelm-prod" }],
    ["kms:DisableKey", at("kms", "key/1234"), { [`aws:ResourceTag/${INSTALL_TAG}`]: "coderhelm-prod" }],
    ["kms:PutKeyPolicy", at("kms", "key/1234"), { [`aws:ResourceTag/${INSTALL_TAG}`]: "someone-else" }],
    ["kms:TagResource", at("kms", "key/1234"), { [`aws:RequestTag/${INSTALL_TAG}`]: "someone-else" }],
    ["cognito-idp:UpdateUserPool", at("cognito-idp", "userpool/us-west-2_abc"), {}],
    ["cloudfront:UpdateDistribution", `arn:aws:cloudfront::${ACCOUNT}:distribution/E123`, {}],
    ["acm:DeleteCertificate", `arn:aws:acm:us-east-1:${ACCOUNT}:certificate/x`, {}],
    ["events:PutRule", at("events", "rule/billing")],
    ["codebuild:UpdateProject", at("codebuild", "project/deploy-prod")],
    ["ssm:PutParameter", at("ssm", "parameter/prod/db-password")],
    ["cloudtrail:DeleteTrail", "*"],
    ["sts:AssumeRole", role("cdk-hnb659fds-deploy-role-111122223333-us-west-2"), { "aws:ResourceAccount": ACCOUNT }],
  ] as [string, string, Record<string, string>?][]) {
    assert.ok(!can(action, resource, context), `the deploy role can't ${action} on ${resource}`);
  }
  assert.ok(can("kms:PutKeyPolicy", at("kms", "key/1234"), { [`aws:ResourceTag/${INSTALL_TAG}`]: "coderhelm-prod" }), "its own tagged key");
  // Creating with the install's tag needs TagResource before the key exists
  // (docs/self-host/security.md says what that leaves open).
  assert.ok(can("kms:TagResource", "*", { [`aws:RequestTag/${INSTALL_TAG}`]: "coderhelm-prod" }));
  assert.ok(can("kms:CreateKey", "*", { [`aws:RequestTag/${INSTALL_TAG}`]: "coderhelm-prod" }));
  assert.ok(!can("kms:CreateKey", "*", {}), "only tagged for the install");
  assert.ok(can("lambda:UpdateFunctionCode", at("lambda", "function:coderhelm-prod-gateway")));
});

test("deploy role: covers every resource the synthesized app creates (by name or tag)", () => {
  const stacks = hardened();
  const service: Record<string, string> = {};
  for (const t of Object.values(stacks)) for (const r of Object.values<any>(t.Resources ?? {})) {
    const m = /^AWS::([A-Za-z0-9]+)::/.exec(r.Type);
    if (m) service[r.Type] = m[1];
  }
  const prefix: Record<string, string> = {
    ApiGatewayV2: "apigateway", CloudFront: "cloudfront", CloudWatch: "cloudwatch", CodeBuild: "codebuild", Cognito: "cognito-idp",
    DynamoDB: "dynamodb", Events: "events", IAM: "iam", KMS: "kms", Lambda: "lambda", Logs: "logs", S3: "s3", SES: "ses", SNS: "sns",
    SQS: "sqs", SSM: "ssm", Scheduler: "scheduler",
  };
  for (const [type, svc] of Object.entries(service)) {
    const p = prefix[svc];
    assert.ok(p, `${type}: add its service to this map and to deployPolicy`);
    assert.ok(DEPLOY.Statement.some((s) => s.Effect === "Allow" && list(s.Action).some((a) => a.startsWith(`${p}:`))), `${type}: the deploy role has ${p}`);
  }
  // Names: every named resource starts with the install's prefix, so name-scoped statements cover it.
  const NAME: Record<string, string> = {
    "AWS::IAM::Role": "RoleName", "AWS::IAM::ManagedPolicy": "ManagedPolicyName", "AWS::Lambda::Function": "FunctionName", "AWS::Lambda::LayerVersion": "LayerName",
    "AWS::DynamoDB::GlobalTable": "TableName", "AWS::S3::Bucket": "BucketName", "AWS::SQS::Queue": "QueueName", "AWS::SNS::Topic": "TopicName",
    "AWS::Events::Rule": "Name", "AWS::CodeBuild::Project": "Name", "AWS::CloudWatch::Alarm": "AlarmName", "AWS::SSM::Parameter": "Name",
    "AWS::Logs::LogGroup": "LogGroupName", "AWS::Scheduler::ScheduleGroup": "Name", "AWS::SES::ConfigurationSet": "Name", "AWS::KMS::Alias": "AliasName",
  };
  for (const t of Object.values(stacks)) for (const [id, r] of Object.entries<any>(t.Resources ?? {})) {
    const prop = NAME[r.Type];
    if (!prop) continue;
    const v = r.Properties?.[prop];
    assert.equal(typeof v, "string", `${id} (${r.Type}) has a fixed name`);
    assert.match(v, /^(coderhelm-prod|\/coderhelm-prod\/|\/aws\/(lambda|codebuild)\/coderhelm-prod-|alias\/coderhelm-prod-)/, `${id}: ${v}`);
  }
});

// ── The deploy window ────────────────────────────────────────────────────

test("idle: the update build's role holds no deploy rights and can't change anything", () => {
  const stacks = hardened();
  const { t, id, role: r } = roleByName(stacks, "coderhelm-prod-update-build");
  assert.equal(r.Properties.PermissionsBoundary, n.boundaryArn);
  const idle = rolePolicies(t, id);
  const can = (action: string, resource: string, context: Record<string, string> = {}) => allowed(idle, BOUNDARY, { action, resource, context: { "aws:userid": SESSION, "aws:CurrentTime": NOW, ...context } });
  for (const [action, resource, context] of [
    ["cloudformation:CreateChangeSet", at("cloudformation", "stack/coderhelm-prod-api/x"), { "cloudformation:RoleArn": n.deployRoleArn }],
    ["cloudformation:UpdateStack", at("cloudformation", "stack/coderhelm-prod-api/x")],
    ["iam:PassRole", n.deployRoleArn],
    ["iam:CreateRole", role("coderhelm-prod-x"), { "iam:PermissionsBoundary": n.boundaryArn }],
    ["iam:PutRolePolicy", role("coderhelm-prod-update-build"), { "iam:PermissionsBoundary": n.boundaryArn }],
    ["sts:AssumeRole", role("cdk-hnb659fds-deploy-role-111122223333-us-west-2"), { "aws:ResourceAccount": ACCOUNT }],
    ["s3:PutObject", `arn:aws:s3:::${n.assetsBucket}/x`],
    ["secretsmanager:GetSecretValue", at("secretsmanager", "secret:coderhelm/prod/secrets-AbCdEf")],
    ["ssm:PutParameter", at("ssm", "parameter/coderhelm-prod/installer/config")],
    ["lambda:InvokeFunction", at("lambda", "function:coderhelm-prod-worker")],
    ["lambda:UpdateFunctionCode", at("lambda", "function:coderhelm-prod-gateway")],
  ] as [string, string, Record<string, string>?][]) {
    assert.ok(!can(action, resource, context), `idle: ${action} on ${resource}`);
  }
  assert.ok(can("lambda:InvokeFunction", at("lambda", "function:coderhelm-prod-updater")), "it may ask for its window and report");
  assert.ok(!JSON.stringify(idle).includes("cdk-"), "no CDK bootstrap roles");
});

test("window: only the build's own session, only until its end, only boundary-scoped deploy rights", () => {
  const stacks = hardened();
  const { t, id } = roleByName(stacks, "coderhelm-prod-update-build");
  const policies = [...rolePolicies(t, id), WINDOW];
  const can = (action: string, resource: string, context: Record<string, string> = {}, who = SESSION, when = NOW) =>
    allowed(policies, BOUNDARY, { action, resource, context: { "aws:userid": who, "aws:CurrentTime": when, ...context } });
  const stack = at("cloudformation", "stack/coderhelm-prod-api/x");
  const viaDeployRole = { "cloudformation:RoleArn": n.deployRoleArn };
  assert.ok(can("cloudformation:CreateChangeSet", stack, viaDeployRole));
  assert.ok(can("iam:PassRole", n.deployRoleArn, { "iam:PassedToService": "cloudformation.amazonaws.com" }));
  assert.ok(can("s3:PutObject", `arn:aws:s3:::${n.assetsBucket}/abc.zip`));
  assert.ok(can("ssm:PutParameter", at("ssm", "parameter/coderhelm-prod/installer/state")));
  // Not another session of the same role (a second build), not after the end.
  assert.ok(!can("cloudformation:CreateChangeSet", stack, viaDeployRole, "AROAEXAMPLEROLEID01:AWSCodeBuild-2222"));
  assert.ok(!can("cloudformation:CreateChangeSet", stack, viaDeployRole, SESSION, "2026-10-29T13:41:00Z"));
  // Stacks only through the deploy role, never the access stack or anything else.
  assert.ok(!can("cloudformation:CreateChangeSet", stack, {}), "without a role CloudFormation would use the stack's old one");
  assert.ok(!can("cloudformation:CreateChangeSet", stack, { "cloudformation:RoleArn": role("cdk-hnb659fds-cfn-exec-role-111122223333-us-west-2") }));
  assert.ok(!can("cloudformation:CreateChangeSet", at("cloudformation", "stack/coderhelm-prod-access/x"), viaDeployRole));
  assert.ok(!can("cloudformation:CreateChangeSet", at("cloudformation", "stack/payroll/x"), viaDeployRole));
  for (const [action, resource, context] of [
    ["iam:PassRole", role("coderhelm-prod-ServiceRole-ab12cd34"), { "iam:PassedToService": "lambda.amazonaws.com" }],
    ["iam:PassRole", n.deployRoleArn, { "iam:PassedToService": "lambda.amazonaws.com" }],
    ["iam:CreateRole", role("coderhelm-prod-x"), { "iam:PermissionsBoundary": n.boundaryArn }],
    ["iam:CreateUser", "*"],
    ["lambda:UpdateFunctionCode", at("lambda", "function:coderhelm-prod-gateway")],
    ["s3:PutObject", "arn:aws:s3:::company-backups/x"],
    ["sts:AssumeRole", role("cdk-hnb659fds-deploy-role-111122223333-us-west-2"), { "aws:ResourceAccount": ACCOUNT }],
  ] as [string, string, Record<string, string>?][]) {
    assert.ok(!can(action, resource, context), `window: ${action} on ${resource}`);
  }
});

test("window: the updater gets its policy in parts that fit standard parameters, and the buildspec it checks", () => {
  const stacks = hardened();
  const u = stacks["coderhelm-prod-updates"];
  const params = Object.values<any>(u.Resources).filter((r) => r.Type === "AWS::SSM::Parameter" && String(r.Properties.Name).startsWith("/coderhelm-prod/access/window-policy/"));
  const parts = windowPolicyParts(n);
  assert.equal(params.length, parts.length);
  for (const p of params) assert.ok(p.Properties.Value.length <= 4096, "standard tier");
  assert.deepEqual(params.flatMap((p) => JSON.parse(p.Properties.Value)), windowPolicy(n).Statement);
  const updater = Object.values<any>(u.Resources).find((r) => r.Type === "AWS::Lambda::Function" && r.Properties.FunctionName === "coderhelm-prod-updater");
  const w = JSON.parse(updater.Properties.Environment.Variables.DEPLOY_WINDOW);
  const cfg = resolveConfig({ ...BASE, access: { mode: "hardened", owner: "app" } });
  assert.equal(w.buildspec, buildSpecSha256(updateBuildSpecObject(cfg)));
  const project = Object.values<any>(u.Resources).find((r) => r.Type === "AWS::CodeBuild::Project" && r.Properties.Name === "coderhelm-prod-update");
  assert.equal(w.buildspec, buildSpecSha256(JSON.parse(project.Properties.Source.BuildSpec)), "the hash of what CodeBuild stores");
  assert.deepEqual(w.variables, UPDATE_BUILD_VARIABLES);
  assert.deepEqual(project.Properties.Environment.EnvironmentVariables.map((v: any) => v.Name).sort(), [...UPDATE_BUILD_VARIABLES].sort());
  assert.equal(w.parts, parts.length);
  assert.equal(canonicalJson({ b: [1, { d: "é", c: null }], a: "x\n" }), '{"a":"x\\n","b":[1,{"c":null,"d":"é"}]}', "equal to canonical_json in lambda/updater/handler.py");
  // The build asks for its window after the signature checks, before deploying, and closes it when it ends.
  const spec = updateBuildSpecObject(cfg) as any;
  const cmds: string[] = spec.phases.build.commands;
  const ask = cmds.findIndex((c) => c.includes('"action":"open_window"'));
  assert.ok(ask > cmds.findIndex((c) => c.includes("cosign verify-blob")) && ask < cmds.findIndex((c) => c.includes("coderhelm-selfhost update")));
  assert.ok(spec.phases.post_build.commands[0].includes("delete-role-policy --role-name coderhelm-prod-update-build --policy-name deploy-window"));
});

// ── Every role, every policy ─────────────────────────────────────────────

test("hardened: every role carries the boundary; no AdministratorAccess, no *:*, PassRole and role writes scoped", () => {
  for (const owner of ["app", "launch"] as const) {
    const stacks = synth({ access: owner === "app" ? { mode: "hardened", owner } : { mode: "hardened", owner, id: "1a2b3c4d" } }, { accessStack: true });
    const boundary = resolveConfig({ ...BASE, access: owner === "app" ? { mode: "hardened", owner } : { mode: "hardened", owner, id: "1a2b3c4d" } }).access!.boundaryArn;
    let roles = 0;
    for (const [name, t] of Object.entries(stacks)) {
      const text = JSON.stringify(t);
      assert.ok(!text.includes("AdministratorAccess"), `${name}: no AdministratorAccess`);
      assert.ok(!text.includes("cdk-hnb659fds"), `${name}: nothing of the CDK bootstrap`);
      assert.ok(!t.Parameters?.BootstrapVersion, `${name}: no bootstrap version check`);
      for (const [id, r] of Object.entries<any>(t.Resources ?? {})) {
        if (r.Type === "AWS::IAM::Role") {
          roles++;
          assert.equal(r.Properties.PermissionsBoundary, boundary, `${name}/${id} carries the boundary`);
          for (const m of r.Properties.ManagedPolicyArns ?? []) assert.match(JSON.stringify(m), /AWSLambdaBasicExecutionRole/, `${name}/${id}: ${JSON.stringify(m)}`);
        }
        const docs = r.Type === "AWS::IAM::Role" ? (r.Properties.Policies ?? []).map((p: any) => p.PolicyDocument) : ["AWS::IAM::Policy", "AWS::IAM::ManagedPolicy"].includes(r.Type) ? [r.Properties.PolicyDocument] : [];
        for (const d of docs) for (const s of d.Statement) {
          if (s.Effect !== "Allow") continue;
          const actions = list(s.Action);
          assert.ok(!actions.some((a) => a === "*" || a === "*:*" || a === "iam:*"), `${name}/${id}: ${JSON.stringify(actions)}`);
          if (actions.some((a) => /^iam:(PassRole|\*)$/.test(a))) {
            for (const res of list(s.Resource).map((x) => JSON.stringify(x))) assert.match(res, /role\/coderhelm-prod-|PluginSchedulerRole|DeployRole/, `${name}/${id}: PassRole on ${res}`);
          }
          if (actions.some((a) => ["iam:CreateRole", "iam:PutRolePolicy", "iam:AttachRolePolicy", "iam:PutRolePermissionsBoundary"].includes(a))) {
            assert.equal(s.Condition?.StringEquals?.["iam:PermissionsBoundary"], boundary, `${name}/${id}: role writes only with the boundary`);
          }
        }
      }
    }
    assert.ok(roles >= 18, `${owner}: ${roles} roles checked`);
  }
});

test("hardened: assets go to the install's own bucket, with the CLI's credentials (no bootstrap roles)", () => {
  const cfg = resolveConfig({ ...BASE, access: { mode: "hardened", owner: "launch", id: "1a2b3c4d" } });
  const app = new cdk.App({ outdir: fs.mkdtempSync(path.join(os.tmpdir(), "chaccasm-")) });
  buildApp(app, cfg);
  const asm = app.synth();
  assert.ok(!asm.stacks.some((s) => s.stackName === "coderhelm-prod-access"), "a launch-made install has no access stack");
  for (const s of asm.stacks) {
    const props = s.manifest.properties as any;
    assert.equal(props.assumeRoleArn, undefined, `${s.stackName}: no role to assume`);
    assert.equal(props.cloudFormationExecutionRoleArn, undefined, `${s.stackName}: the CLI passes --role-arn`);
    assert.equal(props.requiresBootstrapStackVersion, undefined);
    assert.match(String(props.stackTemplateAssetObjectUrl ?? `s3://${cfg.access!.assetsBucket}/`), new RegExp(`^s3://${cfg.access!.assetsBucket}/`));
    assert.equal(props.tags[INSTALL_TAG], "coderhelm-prod");
  }
  // Updates skip the admin's access stack.
  process.env.CODERHELM_ACCESS_STACK = "skip";
  try {
    const app2 = new cdk.App({ outdir: fs.mkdtempSync(path.join(os.tmpdir(), "chaccasm-")) });
    buildApp(app2, resolveConfig({ ...BASE, access: { mode: "hardened", owner: "app" } }));
    assert.ok(!app2.node.children.some((c) => (c as cdk.Stack).stackName === "coderhelm-prod-access"));
  } finally {
    delete process.env.CODERHELM_ACCESS_STACK;
  }
});

test("hardened: config refuses what it can't deploy", () => {
  assert.throws(() => resolveConfig({ ...BASE, access: { mode: "hardened", owner: "launch" } as any }), /access.id/);
  assert.throws(() => resolveConfig({ ...BASE, region: "eu-west-1", domain: { hostname: "ch.example.org", hostedZone: "example.org" }, access: { mode: "hardened", owner: "app" } }), /outside us-east-1 needs domain.certificateArn/);
  assert.throws(() => resolveConfig({ ...BASE, updates: { enabled: false }, access: { mode: "hardened", owner: "app" } }), /needs the updater/);
  assert.equal(installName("coderhelm-abcdefghijklmnopqrst", "coderhelm-abcdefghijklmnopqrst-web", "a/b/CustomCDKBucketDeployment8693BB64968944B69AAFB0CC9EB8756C/ServiceRole/Resource", 64).length <= 64, true);
});

// ── Installs without `access`: exactly as before ─────────────────────────

test("without access: the update path synthesizes exactly as before (test/fixtures/legacy-update-path.json)", () => {
  // The update build's role, buildspec and project, the release check and
  // the updater, as main synthesized them before hardened installs. A
  // deliberate change to an older install's update path updates the fixture.
  const fixture = JSON.parse(fs.readFileSync(repoPath("infra", "test", "fixtures", "legacy-update-path.json"), "utf8"));
  const R = synth()["coderhelm-prod-updates"].Resources;
  const now: Record<string, unknown> = {};
  const STAMP = ["CODERHELM_VERSION", "CODERHELM_COMMIT", "CODERHELM_RELEASED_AT"];
  for (const [k, v] of Object.entries<any>(R)) {
    if (v.Type === "AWS::CodeBuild::Project") now[`project:${v.Properties.Name}`] = { BuildSpec: JSON.parse(v.Properties.Source.BuildSpec), Environment: v.Properties.Environment, ServiceRole: v.Properties.ServiceRole, TimeoutInMinutes: v.Properties.TimeoutInMinutes, ConcurrentBuildLimit: v.Properties.ConcurrentBuildLimit };
    if (v.Type === "AWS::IAM::Role") now[`role:${k}`] = { Properties: v.Properties };
    if (v.Type === "AWS::IAM::Policy") now[`policy:${k}`] = { Properties: v.Properties };
    if (v.Type === "AWS::Lambda::Function") {
      const p = { ...v.Properties };
      delete p.Code;
      now[`function:${k}`] = { Properties: p };
    }
  }
  const strip = (x: any) => {
    const y = JSON.parse(JSON.stringify(x));
    for (const f of Object.keys(y).filter((k) => k.startsWith("function:"))) for (const s of STAMP) delete y[f].Properties.Environment?.Variables?.[s];
    return y;
  };
  assert.deepEqual(strip(now), strip(fixture));
});

test("without access: no boundary, no names or tags added, the CDK bootstrap as before", () => {
  for (const [name, t] of Object.entries(synth())) {
    const text = JSON.stringify(t);
    assert.ok(!text.includes("PermissionsBoundary"), name);
    assert.ok(!text.includes(INSTALL_TAG), name);
    assert.ok(t.Parameters?.BootstrapVersion, `${name}: the CDK bootstrap's version check, as before`);
  }
  assert.ok(!Object.keys(synth()).includes("coderhelm-prod-access"));
});

test("the boundary's services list has no wildcard service and no IAM", () => {
  for (const a of BOUNDARY_SERVICES) assert.ok(!a.startsWith("iam:") && a !== "*" && a !== "*:*", a);
});
