// The native release's IAM, as AWS's policy simulator judged it
// (test/fixtures/native-sim.json, from `infra/scripts/deploy-role-sim.py
// --native`, read-only in AWS): every call CloudFormation makes for the
// parent and its parts through the auto-update role under the boundary,
// what the boundary leaves runtime roles in the install's own account, and
// what the install helper may and may not do. CI has no AWS credentials, so
// these tests replay the table with test/iam-eval.ts and check that today's
// policies are the ones the simulator saw.
//
// After changing infra/lib/access.ts or infra/lib/native/parent.ts, run the
// script again with --native.

import { test } from "node:test";
import * as assert from "node:assert/strict";
import * as fs from "node:fs";
import * as path from "node:path";
import { accessNames, boundaryPolicy, PolicyDocument } from "../lib/access";
import { deployRolePolicy, helperPolicy } from "../lib/native/parent";
import { prodValues, resolveValue } from "../lib/native/resolve";
import { decision } from "./iam-eval";

type Verdict = "allowed" | "explicitDeny" | "implicitDeny";
const F = JSON.parse(fs.readFileSync(path.join(__dirname, "..", "..", "test", "fixtures", "native-sim.json"), "utf8"));

/** Calls the registry lists that the auto-update role refuses on purpose. */
const REFUSED: Record<string, Verdict> = {
  "iam:DeleteRolePermissionsBoundary": "explicitDeny", // every role keeps the boundary
  "kms:DisableKey": "explicitDeny", // it never disables a key
  "kms:ScheduleKeyDeletion": "explicitDeny", // it never deletes data
  "iam:CreateServiceLinkedRole": "explicitDeny", // no replicas in other regions, no SES email for Cognito
  "secretsmanager:CreateSecret": "implicitDeny", // the secret is made at launch with the admin's permissions
  "secretsmanager:DeleteSecret": "implicitDeny", // it never deletes data
  "secretsmanager:ReplicateSecretToRegions": "implicitDeny",
  "secretsmanager:RemoveRegionsFromReplication": "implicitDeny",
  "cloudfront:CreateConnectionGroup": "implicitDeny",
  "cloudwatch:PutMetricData": "implicitDeny",
  "logs:CreateLogDelivery": "implicitDeny",
  "logs:PutResourcePolicy": "implicitDeny",
  "logs:DeleteResourcePolicy": "implicitDeny",
  "logs:PutIndexPolicy": "implicitDeny",
  "logs:DeleteIndexPolicy": "implicitDeny",
  "logs:PutBearerTokenAuthentication": "implicitDeny",
};

const pairs = () => (F.rows as any[]).flatMap((r) => Object.entries(r.actions as Record<string, Verdict>).map(([action, verdict]) => ({ action, verdict, resource: r.resource, context: r.context })));

/** Today's policies, for stage prod in the fixture's account and region. */
function today(): { deploy: PolicyDocument; boundary: PolicyDocument; helper: PolicyDocument } {
  const values = prodValues(F.account, F.region);
  const subNames = accessNames({ partition: "${AWS::Partition}", account: "${AWS::AccountId}", region: "${AWS::Region}", prefix: "coderhelm-${Stage}", suffix: "-${AWS::AccountId}", secretName: "coderhelm/${Stage}/secrets", id: "stack" });
  const wrap = (v: any): any => (typeof v === "string" ? (v.includes("${") ? { "Fn::Sub": v } : v) : Array.isArray(v) ? v.map(wrap) : v && typeof v === "object" ? Object.fromEntries(Object.entries(v).map(([k, x]) => [k, wrap(x)])) : v);
  const deploy = resolveValue(wrap(deployRolePolicy(subNames)), { values });
  const helper = JSON.parse(JSON.stringify(helperPolicy()).replace(/\$\{Stage\}/g, "prod").replace(/\$\{StatusId\}/g, "0123456789abcdef").replace(/\$\{AWS::AccountId\}/g, F.account).replace(/\$\{AWS::Region\}/g, F.region).replace(/\$\{AWS::Partition\}/g, "aws").replace(/\$\{AWS::StackName\}/g, "coderhelm-prod"));
  const boundary = boundaryPolicy(accessNames({ account: F.account, region: F.region, prefix: "coderhelm-prod", suffix: `-${F.account}`, secretName: "coderhelm/prod/secrets", id: "stack" }), [`arn:aws:iam::${F.account}:policy/coderhelm-prod-deploy-boundary-stack`]);
  return { deploy, boundary, helper };
}

test("the fixture is AWS's verdict on thousands of calls, for the native policies as they are today", () => {
  assert.ok(pairs().length > 3000, `${pairs().length} pairs`);
  assert.match(F.about, /--native/);
  const t = today();
  assert.deepEqual(F.policies.deploy, t.deploy, "the auto-update role changed: run deploy-role-sim.py --native again");
  assert.deepEqual(F.policies.boundary, t.boundary, "the boundary changed: run deploy-role-sim.py --native again");
  assert.deepEqual(F.helper.policy, t.helper, "the helper's policy changed: run deploy-role-sim.py --native again");
});

test("the local evaluator gives the simulator's verdict on every call", () => {
  const { deploy, boundary } = F.policies;
  const wrong = pairs().filter((p) => decision([deploy], boundary, p) !== p.verdict).map((p) => `${p.action} on ${p.resource}: simulator ${p.verdict}`);
  assert.deepEqual(wrong, []);
});

/**
 * The auto-update role may not change the boundary or itself (the boundary's
 * TheBoundaryStays and TheDeployRoleStays): a release that changes either
 * waits for an admin (docs/self-host/security.md, "A known limit").
 */
const ITS_OWN = ["arn:aws:iam::111122223333:policy/coderhelm-prod-boundary-stack", "arn:aws:iam::111122223333:policy/coderhelm-prod-deploy-boundary-stack", "arn:aws:iam::111122223333:role/coderhelm-prod-deploy-stack"];

test("the auto-update role may make every call CloudFormation makes, except the refusals on purpose", () => {
  const gaps = pairs().filter((p) => p.verdict !== "allowed" && REFUSED[p.action] !== p.verdict && !(ITS_OWN.includes(p.resource) && p.verdict === "explicitDeny")).map((p) => `${p.action} on ${p.resource}: ${p.verdict}`);
  assert.deepEqual(gaps, []);
  for (const a of Object.keys(REFUSED)) assert.ok(pairs().some((p) => p.action === a), `${a} is no longer a call`);
  // Its delete of the stack (CloudFormation keeps using it once it applied an update) removes the
  // boundary: it may delete it, never edit it (the security review's finding on this branch).
  const verdict = (a: string, res: string) => pairs().find((p) => p.action === a && p.resource === res)?.verdict;
  const main = "arn:aws:iam::111122223333:policy/coderhelm-prod-boundary-stack";
  assert.equal(verdict("iam:DeletePolicy", main), "allowed");
  assert.equal(verdict("iam:DeletePolicyVersion", main), "allowed");
  assert.equal(verdict("iam:CreatePolicyVersion", main), "explicitDeny");
  // ... and never create a policy under that name again (else: delete it, make an "Allow *" one of the same name, give it to a role).
  assert.equal(verdict("iam:CreatePolicy", main), "explicitDeny");
  assert.equal(verdict("iam:CreatePolicy", "arn:aws:iam::111122223333:policy/coderhelm-prod-deploy-boundary-stack"), "explicitDeny");
  assert.ok(pairs().every((p) => p.context["aws:PrincipalArn"] === "arn:aws:iam::111122223333:role/coderhelm-prod-deploy-stack"), "the simulated caller is the auto-update role");
  // And it really can't change either of them.
  for (const r of ITS_OWN) assert.ok(pairs().some((p) => p.resource === r && p.verdict === "explicitDeny" && /^iam:(CreatePolicyVersion|PutRolePolicy)$/.test(p.action)), r);
});

test("the helper may do what it needs and nothing on another install's or anyone else's resources", () => {
  const bad = (F.helper.rows as any[]).filter((r) => (r.simulator === "allowed") !== (r.want === "allowed")).map((r) => `${r.action} on ${r.resource}: ${r.simulator}, want ${r.want}`);
  assert.deepEqual(bad, []);
  assert.ok(F.helper.rows.filter((r: any) => r.want === "denied").length >= 10);
  // The live test's delete failed on the main table (coderhelm-prod, no suffix): now allowed, and only that one.
  const row = (a: string, res: string) => (F.helper.rows as any[]).find((r) => r.action === a && r.resource.endsWith(res));
  for (const a of ["dynamodb:DescribeTable", "dynamodb:DeleteTable", "dynamodb:UpdateTable"]) assert.equal(row(a, ":table/coderhelm-prod")?.simulator, "allowed", a);
  assert.notEqual(row("dynamodb:DeleteTable", ":table/coderhelm-prodx")?.simulator, "allowed");
  // Final deletes the install's secret (KeepDataOnDelete=false), never another.
  assert.equal(row("secretsmanager:DeleteSecret", ":secret:coderhelm/prod/secrets-AbCdEf")?.simulator, "allowed");
  assert.notEqual(row("secretsmanager:DeleteSecret", ":secret:coderhelm/other/secrets-AbCdEf")?.simulator, "allowed");
});

test("runtime roles: only the gateway and worker may take on the two read roles in the install's own account", () => {
  const allowed = (F.runtime.rows as any[]).flatMap((r) => Object.entries(r.actions).filter(([a, v]) => v === "allowed" && a.startsWith("sts:")).map(([a]) => `${r.caller}: ${a} on ${r.resource.split(":").slice(4).join(":")}`)).sort();
  assert.deepEqual(allowed, [
    "gateway: sts:AssumeRole on 111122223333:role/CoderHelmLogReader",
    "gateway: sts:AssumeRole on 111122223333:role/CoderhelmReadOnly",
    "untagged: sts:AssumeRole on 999988887777:role/CoderHelmLogReader",
    "worker: sts:AssumeRole on 111122223333:role/CoderHelmLogReader",
    "worker: sts:AssumeRole on 111122223333:role/CoderhelmReadOnly",
  ]);
});
