// The updater role's own policy and boundary, as a native release gives them
// to one install (stage prod), pinned in test/fixtures/updater-role.json.
//
// lambda/tests/test_updater_iam.py drives the updater through every change
// it makes to the install's stack (a version update, a held update, a
// switch or private network) and evaluates each AWS call it makes, and each
// one CloudFormation then makes as the caller (the parts' nested change
// sets, the staged templates it reads), against this file. v0.8.94's
// switches made their change set with UsePreviousTemplate: no template URL,
// so ProposeReleaseUpdate's cloudformation:TemplateUrl condition refused it.
//
// The checks below are this evaluator's (test/iam-eval.ts) verdicts on a few
// calls; the Python test gives the same verdicts on them first, so the two
// evaluators can't drift apart.
//
// After changing the updater's policy (lib/updates-stack.ts) or the boundary:
//   UPDATE_FIXTURES=1 npm test   (then run the Python tests)

import { test } from "node:test";
import * as assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { PolicyDocument } from "../lib/access";
import { buildNative } from "../lib/native/build";
import { prodValues, resolveValue } from "../lib/native/resolve";
import { decision, Request } from "./iam-eval";

const FIXTURE = path.join(__dirname, "..", "..", "test", "fixtures", "updater-role.json");
const ACCOUNT = "111122223333";
const REGION = "us-east-1";

const STUBS = fs.mkdtempSync(path.join(os.tmpdir(), "updrole-"));
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

function rendered() {
  const rel: any = buildNative("v9.8.7", fs.mkdtempSync(path.join(os.tmpdir(), "updrolerel-")), { dashboardDir: DASH });
  const P = rel.parent.Resources;
  const values = prodValues(ACCOUNT, REGION);
  // The deploy role as the parent names it (Updates gets its ARN).
  const deployRole = `arn:aws:iam::${ACCOUNT}:role/${resolveValue(P.DeployRole.Properties.RoleName, { values })}`;
  values.DeployRoleArn = deployRole;
  const updates = rel.children.Updates;
  const R = updates.Resources;
  const fnId = Object.keys(R).find((k) => R[k].Type === "AWS::Lambda::Function" && resolveValue(R[k].Properties.FunctionName, { values }) === "coderhelm-prod-updater");
  assert.ok(fnId, "the updater function");
  const roleId = R[fnId!].Properties.Role["Fn::GetAtt"][0];
  const role = R[roleId];
  const identity: PolicyDocument[] = Object.values<any>(R)
    .filter((r) => (r.Type === "AWS::IAM::Policy" || r.Type === "AWS::IAM::ManagedPolicy") && JSON.stringify(r.Properties.Roles ?? []).includes(`"${roleId}"`))
    .map((r) => resolveValue(r.Properties.PolicyDocument, { values }) as any);
  for (const p of role.Properties.Policies ?? []) identity.push(resolveValue(p.PolicyDocument, { values }) as any);
  assert.ok(identity.length > 0, "the updater role's policies");
  const boundaryArn = resolveValue(role.Properties.PermissionsBoundary, { values }) as string;
  const boundaryId = Object.keys(P).find((k) => P[k].Type === "AWS::IAM::ManagedPolicy" && boundaryArn.endsWith(`:policy/${resolveValue(P[k].Properties.ManagedPolicyName, { values })}`));
  assert.ok(boundaryId, `the updater role's boundary ${boundaryArn}`);
  const boundary = resolveValue(P[boundaryId!].Properties.PolicyDocument, { values }) as any as PolicyDocument;

  const root = `arn:aws:cloudformation:${REGION}:${ACCOUNT}:stack/coderhelm-prod/abc`;
  const part = `arn:aws:cloudformation:${REGION}:${ACCOUNT}:stack/coderhelm-prod-Api-1A2B3C/def`;
  const bucket = `https://coderhelm-prod-release-${ACCOUNT}.s3.${REGION}.amazonaws.com/releases`;
  const cs = (resource: string, context: Request["context"]): Request => ({ action: "cloudformation:CreateChangeSet", resource, context });
  const requests: Request[] = [
    cs(root, { "cloudformation:TemplateUrl": "https://coderhelm-public.s3.amazonaws.com/launch/v1.2.3/coderhelm.json", "cloudformation:RoleArn": deployRole }),
    cs(root, { "cloudformation:TemplateUrl": `${bucket}/v1.2.3/held/coderhelm.json`, "cloudformation:RoleArn": deployRole }),
    // A change set with no template URL (UsePreviousTemplate): refused.
    cs(root, { "cloudformation:RoleArn": deployRole }),
    cs(root, { "cloudformation:TemplateUrl": "https://example.org/launch/v1.2.3/coderhelm.json", "cloudformation:RoleArn": deployRole }),
    cs(root, { "cloudformation:TemplateUrl": "https://coderhelm-public.s3.amazonaws.com/launch/v1.2.3/coderhelm.json", "cloudformation:RoleArn": `arn:aws:iam::${ACCOUNT}:role/other` }),
    cs(part, { "cloudformation:TemplateUrl": `${bucket}/v1.2.3/stacks/Api.json`, "cloudformation:RoleArn": deployRole }),
    cs(part, { "cloudformation:RoleArn": deployRole }),
    cs(part, { "cloudformation:TemplateUrl": "https://example.org/x.json" }),
    { action: "cloudformation:ExecuteChangeSet", resource: root },
    { action: "cloudformation:DeleteChangeSet", resource: root },
    { action: "iam:PassRole", resource: deployRole, context: { "iam:PassedToService": "cloudformation.amazonaws.com" } },
    { action: "s3:GetObject", resource: `arn:aws:s3:::coderhelm-prod-release-${ACCOUNT}/releases/v1.2.3/stacks/Api.json` },
  ];
  const checks = requests.map((r) => ({ request: r, verdict: decision(identity, boundary, r) }));
  return { account: ACCOUNT, region: REGION, stack: "coderhelm-prod", deployRoleArn: deployRole, boundaryArn, identity, boundary, checks };
}

test("the updater role's policy and boundary are the ones the updater's calls are checked against", () => {
  const now = JSON.parse(JSON.stringify(rendered()));
  if (process.env.UPDATE_FIXTURES === "1") fs.writeFileSync(FIXTURE, JSON.stringify(now, null, 2) + "\n");
  const pinned = JSON.parse(fs.readFileSync(FIXTURE, "utf8"));
  assert.deepEqual(pinned, now, "the updater role changed: run UPDATE_FIXTURES=1 npm test, then the Python tests (lambda/tests/test_updater_iam.py)");
  // A change set without a template URL is never one the updater may make.
  const noUrl = now.checks.find((c: any) => c.request.resource.includes("/coderhelm-prod/") && c.request.action === "cloudformation:CreateChangeSet" && !c.request.context["cloudformation:TemplateUrl"]);
  assert.equal(noUrl.verdict, "implicitDeny");
});
