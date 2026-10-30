// Every IAM call CloudFormation makes to create, tag, update and delete each
// resource of a hardened self-host install, checked against the deploy
// role's policy and its permissions boundary.
//
// test/fixtures/deploy-role-sim.json comes from infra/scripts/deploy-role-sim.py:
// it synthesizes the install (with no domain, and with a domain and Bedrock),
// takes each resource's handler permissions from its CloudFormation registry
// schema, builds the ARN each call hits from the template's names, and
// records AWS's IAM policy simulator verdict on every call. CI has no AWS
// credentials, so these tests replay that table with test/iam-eval.ts:
// first that the evaluator gives the simulator's verdict on the policies
// the simulator saw, then that today's policies allow every call.
//
// After adding a resource type or changing infra/lib/access.ts, run the
// script again (read-only in AWS: DescribeType and SimulateCustomPolicy).
//
//   npm test   (compiles with tsc, then runs node --test)

import { test } from "node:test";
import * as assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import * as cdk from "aws-cdk-lib";
import { buildApp } from "../lib/app";
import { resolveConfig } from "../lib/config";
import { accessNames, boundaryPolicy, deployPolicy, PolicyDocument } from "../lib/access";
import { decision } from "./iam-eval";

type Verdict = "allowed" | "explicitDeny" | "implicitDeny";
interface Row { resource: string; context: Record<string, string | string[]>; actions: Record<string, Verdict>; calls: string[] }
interface Fixture {
  account: string; region: string; prefix: string; launchId: string;
  policies: { deploy: PolicyDocument; boundary: PolicyDocument };
  types: string[];
  rows: Row[];
  notSimulated: { action: string; reason: string; calls: string[] }[];
}

const FIXTURE: Fixture = JSON.parse(fs.readFileSync(path.join(__dirname, "..", "..", "test", "fixtures", "deploy-role-sim.json"), "utf8"));

/**
 * Calls the registry lists that the deploy role refuses on purpose, each
 * with why a CoderHelm install never makes it.
 */
const REFUSED: Record<string, { verdict: Verdict; why: string }> = {
  "iam:DeleteRolePermissionsBoundary": { verdict: "explicitDeny", why: "every role keeps the boundary; CloudFormation calls this only when a template drops a role's PermissionsBoundary, which the boundary forbids" },
  "kms:DisableKey": { verdict: "explicitDeny", why: "only for a key with Enabled: false; CoderHelm never disables its keys" },
  "kms:ScheduleKeyDeletion": { verdict: "explicitDeny", why: "every key is DeletionPolicy and UpdateReplacePolicy Retain, so CloudFormation never deletes one" },
  "iam:CreateServiceLinkedRole": { verdict: "explicitDeny", why: "DynamoDB's replication role is for replicas in other regions (one region here); Cognito's email role is for SES email (the pools use Cognito's own email)" },
  "cloudfront:CreateConnectionGroup": { verdict: "implicitDeny", why: "only for a distribution in a connection group (multi-tenant); CoderHelm's distribution is a plain one" },
  "cloudwatch:PutMetricData": { verdict: "implicitDeny", why: "the global table handler's own metrics; real installs created every table without it" },
  "logs:CreateLogDelivery": { verdict: "implicitDeny", why: "only for a log group delivery configuration; none is set" },
  "logs:PutResourcePolicy": { verdict: "implicitDeny", why: "only for a log group ResourcePolicyDocument; none is set" },
  "logs:DeleteResourcePolicy": { verdict: "implicitDeny", why: "only when a log group ResourcePolicyDocument is removed; none is set" },
  "logs:PutIndexPolicy": { verdict: "implicitDeny", why: "only for log group FieldIndexPolicies; none is set" },
  "logs:DeleteIndexPolicy": { verdict: "implicitDeny", why: "only when log group FieldIndexPolicies are removed; none is set" },
  "logs:PutBearerTokenAuthentication": { verdict: "implicitDeny", why: "only for BearerTokenAuthenticationEnabled on a log group; not set" },
};

const pairs = () => FIXTURE.rows.flatMap((r) => Object.entries(r.actions).map(([action, verdict]) => ({ action, verdict, resource: r.resource, context: r.context, calls: r.calls })));

test("the fixture is AWS's own verdict on thousands of calls, none of them on a real account", () => {
  assert.ok(pairs().length > 2000, `${pairs().length} pairs`);
  assert.equal(FIXTURE.account, "111122223333");
  const accounts = new Set([...JSON.stringify(FIXTURE).matchAll(/:(\d{12}):/g)].map((m) => m[1]));
  assert.deepEqual([...accounts].sort(), ["111122223333", "753240598075"], "the example account, and the public Lambda adapter layer's");
});

test("the local IAM evaluator gives AWS's policy simulator verdict on every call", () => {
  const { deploy, boundary } = FIXTURE.policies;
  const wrong = pairs()
    .filter((p) => decision([deploy], boundary, p) !== p.verdict)
    .map((p) => `${p.action} on ${p.resource}: simulator ${p.verdict}, evaluator ${decision([deploy], boundary, p)}`);
  assert.deepEqual(wrong, []);
});

test("the deploy role may make every call CloudFormation makes for a self-host install", () => {
  const n = accessNames({ account: FIXTURE.account, region: FIXTURE.region, prefix: FIXTURE.prefix, suffix: `-${FIXTURE.account}`, secretName: "coderhelm/prod/secrets", id: FIXTURE.launchId });
  const deploy = deployPolicy(n);
  const boundary = boundaryPolicy(n);
  const gaps: string[] = [];
  for (const p of pairs()) {
    const got = decision([deploy], boundary, p);
    if (got === "allowed") continue;
    const ok = REFUSED[p.action];
    if (!ok || ok.verdict !== got) gaps.push(`${p.action} on ${p.resource} (${p.calls.join(", ")}): ${got}`);
  }
  assert.deepEqual(gaps, []);
  // Each refusal still matches a call the registry lists, so the list can't go stale.
  for (const a of Object.keys(REFUSED)) assert.ok(pairs().some((p) => p.action === a), `${a} is no longer a call`);
});

test("the fixture covers every resource type a hardened install synthesizes", () => {
  const stubs = fs.mkdtempSync(path.join(os.tmpdir(), "chsim-"));
  for (const d of ["gateway", "worker", "mcp"]) {
    fs.mkdirSync(path.join(stubs, d), { recursive: true });
    fs.writeFileSync(path.join(stubs, d, d === "mcp" ? "handler.js" : "bootstrap"), "stub");
  }
  process.env.GATEWAY_ZIP = path.join(stubs, "gateway");
  process.env.WORKER_ZIP = path.join(stubs, "worker");
  process.env.MCP_PROXY_DIR = path.join(stubs, "mcp");
  const base = { access: { mode: "hardened" as const, owner: "launch" as const, id: FIXTURE.launchId }, account: FIXTURE.account, region: FIXTURE.region, stage: "prod", email: { from: "coderhelm@example.org" }, admins: ["owner@example.org"] };
  const zone = { [`hosted-zone:account=${FIXTURE.account}:domainName=example.org:region=${FIXTURE.region}`]: { Id: "/hostedzone/ZTEST000001", Name: "example.org." } };
  const configs: [any, Record<string, unknown>][] = [
    [base, {}],
    [{ ...base, domain: { hostname: "helm.example.org", hostedZone: "example.org" }, models: { provider: "bedrock" }, email: { from: "helm@example.org" }, integrations: { googleSignIn: true } }, zone],
  ];
  const missing = new Set<string>();
  for (const [c, context] of configs) {
    const app = new cdk.App({ outdir: fs.mkdtempSync(path.join(os.tmpdir(), "chsimout-")), context });
    buildApp(app, resolveConfig(c));
    for (const s of app.synth().stacks) {
      for (const r of Object.values<any>(s.template.Resources ?? {})) {
        if (r.Type.startsWith("AWS::") && r.Type !== "AWS::CDK::Metadata" && !FIXTURE.types.includes(r.Type)) missing.add(r.Type);
      }
    }
  }
  assert.deepEqual([...missing], [], "run infra/scripts/deploy-role-sim.py for the new resource types");
});
