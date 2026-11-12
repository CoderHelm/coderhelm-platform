// The deploy role's own policy and boundary, in the two forms an install can
// have them, pinned in test/fixtures/deploy-role.json:
//
// - "pinned": what the parent template carries (access-pinned.json), which is
//   what an install has until an AWS admin applies a newer access stack. It
//   has none of the private networks' statements (access.ts
//   PRIVATE_NETWORK_DEPLOY_*).
// - "current": what this release's access.json writes to the deploy role and
//   its boundary once an admin applies it.
//
// lambda/tests/test_updater_preflight.py makes the change set a private
// network adds (the parent's Reach<n> resources, pinned below as the release
// renders them) and runs the updater's preflight against both: an install on
// "pinned" is refused before anything starts (it would fail on
// ec2:DescribeSecurityGroups as the deploy role), one on "current" is allowed.
//
// The checks below are this evaluator's (test/iam-eval.ts) verdicts on the
// calls CloudFormation's handlers make for a network; the Python test gives
// the same verdicts on them first, so the two evaluators can't drift apart.
//
// After changing the deploy policy, the boundary or the network resources:
//   UPDATE_FIXTURES=1 npm test   (then run the Python tests)

import { test } from "node:test";
import * as assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { INSTALL_TAG, PolicyDocument } from "../lib/access";
import { ACCESS_FILE } from "../lib/native/parent";
import { buildNative } from "../lib/native/build";
import { prodValues, resolveValue } from "../lib/native/resolve";
import { decision, Request } from "./iam-eval";

const FIXTURE = path.join(__dirname, "..", "..", "test", "fixtures", "deploy-role.json");
const ACCOUNT = "111122223333";
const REGION = "us-east-1";

const STUBS = fs.mkdtempSync(path.join(os.tmpdir(), "deprole-"));
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
  const out = fs.mkdtempSync(path.join(os.tmpdir(), "deprolerel-"));
  const rel: any = buildNative("v9.8.7", out, { dashboardDir: DASH });
  const P = rel.parent.Resources;
  const access = JSON.parse(fs.readFileSync(path.join(out, ACCESS_FILE), "utf8")).Resources.Access.Properties;
  const values: Record<string, string> = { ...prodValues(ACCOUNT, REGION), EmailFrom: "", EmailIdentity: "example.org" };
  const r = (v: unknown) => resolveValue(v as any, { values }) as any;
  const roleName = r(P.DeployRole.Properties.RoleName) as string;
  const deployRoleArn = `arn:aws:iam::${ACCOUNT}:role/${roleName}`;
  const boundaryArn = `arn:aws:iam::${ACCOUNT}:policy/${r(P.DeployBoundary.Properties.ManagedPolicyName)}`;
  const pinned = {
    identity: (P.DeployRole.Properties.Policies as any[]).map((p) => r(p.PolicyDocument)) as PolicyDocument[],
    boundary: r(P.DeployBoundary.Properties.PolicyDocument) as PolicyDocument,
  };
  const current = { identity: [r(access.DeployRole.Document)] as PolicyDocument[], boundary: r(access.DeployBoundary.Document) as PolicyDocument };
  // What one private network adds to the parent: each resource's type, and the security group's tags.
  const network: Record<string, { type: string; tags?: { Key: string; Value: string }[] }> = {};
  for (const id of ["Reach1Group", "Reach1Logs", "Reach1Role", "Reach1"]) {
    assert.ok(P[id], id);
    network[id] = { type: P[id].Type, ...(P[id].Properties.Tags && P[id].Type === "AWS::EC2::SecurityGroup" ? { tags: r(P[id].Properties.Tags) } : {}) };
  }
  assert.ok(P.Reach1.Properties.VpcConfig, "the network's function runs in the VPC");
  const ec2 = `arn:aws:ec2:${REGION}:${ACCOUNT}`;
  const tag = { [`aws:RequestTag/${INSTALL_TAG}`]: "coderhelm-prod", "aws:PrincipalArn": deployRoleArn };
  const requests: Request[] = [
    { action: "ec2:DescribeSecurityGroups", resource: "*" },
    { action: "ec2:DescribeVpcs", resource: "*" },
    { action: "ec2:DescribeSubnets", resource: "*" },
    { action: "ec2:CreateSecurityGroup", resource: `${ec2}:vpc/vpc-0a1b2c3d4e5f60718` },
    { action: "ec2:CreateSecurityGroup", resource: `${ec2}:security-group/*` },
    { action: "ec2:AuthorizeSecurityGroupEgress", resource: `${ec2}:security-group/*` },
    { action: "ec2:RevokeSecurityGroupEgress", resource: `${ec2}:security-group/*` },
    { action: "ec2:CreateTags", resource: `${ec2}:security-group/*`, context: tag },
    { action: "logs:CreateLogGroup", resource: `arn:aws:logs:${REGION}:${ACCOUNT}:log-group:/aws/lambda/coderhelm-prod-reach-1` },
  ];
  const checks = requests.map((q) => ({
    request: q,
    pinned: decision(pinned.identity, pinned.boundary, q),
    current: decision(current.identity, current.boundary, q),
  }));
  return { account: ACCOUNT, region: REGION, stack: "coderhelm-prod", deployRoleArn, boundaryArn, installTag: INSTALL_TAG, pinned, current, network, checks };
}

test("the deploy role's policy and boundary, as the parent pins them and as access.json writes them, are the ones the preflight is checked against", () => {
  const now = JSON.parse(JSON.stringify(rendered()));
  if (process.env.UPDATE_FIXTURES === "1") fs.writeFileSync(FIXTURE, JSON.stringify(now, null, 2) + "\n");
  const pinned = JSON.parse(fs.readFileSync(FIXTURE, "utf8"));
  assert.deepEqual(pinned, now, "the deploy role changed: run UPDATE_FIXTURES=1 npm test, then the Python tests (lambda/tests/test_updater_preflight.py)");
  // The install that hasn't applied the access stack can't make a network: the deploy role
  // may not read security groups (v0.8.95's rollback, ec2:DescribeSecurityGroups).
  const describe = now.checks.find((c: any) => c.request.action === "ec2:DescribeSecurityGroups");
  assert.equal(describe.pinned, "implicitDeny");
  assert.equal(describe.current, "allowed");
  // Every call a network needs is allowed once the access stack is applied.
  for (const c of now.checks) assert.equal(c.current, "allowed", JSON.stringify(c.request));
});
