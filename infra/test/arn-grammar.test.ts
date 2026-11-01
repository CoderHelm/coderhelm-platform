// Every IAM policy a release creates, checked against the grammar IAM
// enforces when it creates the policy (CreatePolicy, PutRolePolicy). AWS's
// policy simulator doesn't check it, and neither does Access Analyzer for a
// wildcard service: v0.8.8's boundary named `arn:*:*:*:*:*/coderhelm-prod-*`,
// the simulator allowed every call, and every launch then failed with
// "Resource vendor must be fully qualified and cannot contain regexes".
//
// A Resource is "*" or an ARN of at least six parts, its partition a real
// one or "*", its service spelled out, and a resource part that names
// something (`hostedzone/` with an empty zone id doesn't).

import { test } from "node:test";
import * as assert from "node:assert/strict";
import * as cdk from "aws-cdk-lib";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { buildApp } from "../lib/app";
import { CoderhelmConfig, resolveConfig } from "../lib/config";
import { accessNames, boundaryPolicy, deployPolicy } from "../lib/access";
import { buildNative } from "../lib/native/build";
import { prodValues, resolveValue } from "../lib/native/resolve";

const PARTITIONS = new Set(["aws", "aws-cn", "aws-us-gov", "aws-iso", "aws-iso-b", "aws-iso-e", "aws-iso-f", "*"]);

/** Why IAM would refuse this Resource ("" when it wouldn't). */
export function arnProblem(r: string): string {
  if (r === "*") return "";
  if (!r.startsWith("arn:")) return "not an ARN";
  const parts = r.split(":");
  if (parts.length < 6) return "fewer than six parts";
  if (!PARTITIONS.has(parts[1])) return `partition ${parts[1]}`;
  if (!/^[a-z0-9][a-z0-9-]*$/.test(parts[2])) return `service ${parts[2] || "(empty)"} isn't spelled out`;
  const resource = parts.slice(5).join(":");
  if (!resource) return "empty resource";
  if (/[/:]$/.test(resource)) return "resource part names nothing after its type";
  return "";
}

/** Every Resource and NotResource string in every identity policy of a template (intrinsics left as they are skipped). */
function policyResources(t: any): { where: string; arn: string }[] {
  const out: { where: string; arn: string }[] = [];
  const doc = (where: string, d: any) => {
    for (const st of [d?.Statement ?? []].flat()) {
      if (!st || typeof st !== "object" || st.Ref === "AWS::NoValue") continue;
      for (const k of ["Resource", "NotResource"]) {
        for (const r of [st[k] ?? []].flat()) if (typeof r === "string") out.push({ where: `${where} ${st.Sid ?? ""} ${k}`, arn: r });
      }
    }
  };
  for (const [id, r] of Object.entries(t.Resources ?? {}) as [string, any][]) {
    const p = r.Properties ?? {};
    if (r.Type === "AWS::IAM::ManagedPolicy" || r.Type === "AWS::IAM::Policy") doc(id, p.PolicyDocument);
    if (r.Type === "AWS::IAM::Role") for (const pol of p.Policies ?? []) doc(`${id}/${pol.PolicyName}`, pol.PolicyDocument);
  }
  return out;
}

function problems(templates: [string, any][]): string[] {
  return templates.flatMap(([name, t]) => policyResources(t).map((x) => [x, arnProblem(x.arn)] as const).filter(([, p]) => p).map(([x, p]) => `${name} ${x.where}: ${x.arn} (${p})`));
}

test("the grammar check refuses what IAM refuses", () => {
  // v0.8.8's boundary.
  for (const r of ["arn:*:*:*:*:*/coderhelm-prod-*", "arn:*:*:*:*:*:coderhelm-prod-*", "arn:*:*:*:*:coderhelm-prod-*"]) assert.match(arnProblem(r), /service/, r);
  assert.match(arnProblem("arn:aws:route53:::hostedzone/"), /names nothing/);
  assert.match(arnProblem("arn:aws:s3"), /six/);
  assert.match(arnProblem("arn:${AWS::Partition}:s3:::x"), /partition/);
  for (const r of ["*", "arn:*:lambda:*:*:function:coderhelm-prod-*", "arn:aws:s3:::coderhelm-prod-x/*", "arn:aws:route53:::hostedzone/Z0123", "arn:aws:apigateway:us-east-1::/apis/*"]) assert.equal(arnProblem(r), "", r);
});

test("the boundary and deploy policy, with the longest names and with none of the optional ones", () => {
  for (const over of [{}, { emailIdentity: "helm@example.org", zoneId: "Z0123456789EXAMPLE" }]) {
    const n = { ...accessNames({ account: "111122223333", region: "ap-southeast-2", prefix: "coderhelm-abcdefghijklmnopqrst", suffix: "-111122223333", secretName: "coderhelm/abcdefghijklmnopqrst/secrets", id: "stack" }), ...over };
    const t = { Resources: { B: { Type: "AWS::IAM::ManagedPolicy", Properties: { PolicyDocument: boundaryPolicy(n) } }, D: { Type: "AWS::IAM::ManagedPolicy", Properties: { PolicyDocument: deployPolicy(n) } } } };
    assert.deepEqual(problems([["access", t]]), []);
  }
});

// ── A native release, as CloudFormation sees it for one install ─────────

const STUBS = fs.mkdtempSync(path.join(os.tmpdir(), "charn-"));
for (const d of ["gateway", "worker", "mcp"]) {
  fs.mkdirSync(path.join(STUBS, d), { recursive: true });
  fs.writeFileSync(path.join(STUBS, d, d === "mcp" ? "handler.js" : "bootstrap"), "stub");
}
const DASH = path.join(STUBS, "dash");
fs.mkdirSync(path.join(DASH, "_next"), { recursive: true });
fs.writeFileSync(path.join(DASH, "index.html"), "<html></html>");
fs.writeFileSync(path.join(DASH, "_next", "a.js"), "x");
process.env.GATEWAY_ZIP = path.join(STUBS, "gateway");
process.env.WORKER_ZIP = path.join(STUBS, "worker");
process.env.MCP_PROXY_DIR = path.join(STUBS, "mcp");

const rel = buildNative("v9.8.7", fs.mkdtempSync(path.join(os.tmpdir(), "charnrel-")), { dashboardDir: DASH });

test("every policy of a native release, auto-update on, with and without a domain zone", () => {
  for (const zone of ["", "Z0123456789EXAMPLE"]) {
    const values = { ...prodValues("111122223333", "us-east-1"), DomainZoneId: zone, AutoUpdate: "on", Hostname: zone ? "helm.example.org" : "", DomainCertificateArn: zone ? "arn:aws:acm:us-east-1:111122223333:certificate/abc" : "" };
    const conditions = { AutoUpdateOn: true, HasDomainZone: zone !== "", HasHostname: zone !== "", HasDomainCert: zone !== "", HasEmailFrom: false };
    const all: [string, any][] = [["coderhelm.json", rel.parent], ...Object.entries(rel.children)].map(([name, t]) => [`${name} (zone ${zone || "none"})`, resolveValue(t, { values, conditions })]);
    // The auto-update role is there, with or without its DNS statement.
    const deploy = (all[0][1].Resources.DeployRole.Properties.Policies[0].PolicyDocument.Statement as any[]).filter((s) => s.Ref !== "AWS::NoValue");
    assert.equal(deploy.some((s) => s.Sid === "DnsRecords"), zone !== "", `DnsRecords with zone ${zone || "none"}`);
    assert.ok(policyResources(all[0][1]).length > 50);
    assert.deepEqual(problems(all), []);
  }
});

// ── A hardened install from the CLI (the multi-stack layout) ────────────

function synth(over: Partial<CoderhelmConfig>): [string, any][] {
  const cfg = resolveConfig({ account: "111122223333", region: "us-west-2", stage: "prod", email: { from: "a@example.org" }, admins: ["o@example.org"], access: { mode: "hardened", owner: "app" }, ...over });
  const app = new cdk.App({ outdir: fs.mkdtempSync(path.join(os.tmpdir(), "charnout-")) });
  buildApp(app, cfg, { accessStack: true });
  return app.synth().stacks.map((s) => [s.stackName, s.template]);
}

test("every policy of a hardened install, with and without its own domain", () => {
  const ctx = (region: string) => ({ [`hosted-zone:account=111122223333:domainName=example.org:region=${region}`]: { Id: "/hostedzone/ZTEST000001", Name: "example.org." } });
  const plain = synth({});
  assert.ok(plain.some(([, t]) => policyResources(t).length > 0));
  assert.deepEqual(problems(plain), []);
  const cfg = { domain: { hostname: "helm.example.org", hostedZone: "example.org", certificateArn: "arn:aws:acm:us-east-1:111122223333:certificate/abc" } } as Partial<CoderhelmConfig>;
  const app = new cdk.App({ outdir: fs.mkdtempSync(path.join(os.tmpdir(), "charnout-")), context: { ...ctx("us-west-2"), ...ctx("us-east-1") } });
  buildApp(app, resolveConfig({ account: "111122223333", region: "us-west-2", stage: "prod", email: { from: "a@example.org" }, admins: ["o@example.org"], access: { mode: "hardened", owner: "app" }, ...cfg }), { accessStack: true });
  assert.deepEqual(problems(app.synth().stacks.map((s) => [s.stackName, s.template])), []);
});

test("the updater may create the parts' nested change sets, only from the staged release", () => {
  // CloudFormation creates a change set on every part as the caller; without
  // this the updater's first update or domain move fails on the first part.
  const updates: any = (rel.children as any).Updates;
  const docs = Object.values<any>(updates.Resources).filter((r) => r.Type === "AWS::IAM::Policy").map((r) => r.Properties.PolicyDocument.Statement).flat();
  const st = docs.find((s: any) => s.Sid === "ProposePartUpdates");
  assert.ok(st, "ProposePartUpdates is on the updater");
  assert.equal(st.Action, "cloudformation:CreateChangeSet");
  assert.match(JSON.stringify(st.Resource), /stack\/.*-\*\/\*/);
  assert.match(JSON.stringify(st.Condition.StringLikeIfExists["cloudformation:TemplateUrl"]), /-release-.*\/releases\/v\*\.\*\.\*\/stacks\/\*\.json/);
  assert.ok(st.Condition.StringEqualsIfExists["cloudformation:RoleArn"], "only through the deploy role");
});
