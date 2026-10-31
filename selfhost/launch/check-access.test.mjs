// selfhost/check-access.mjs against a fake `aws` (a script on PATH answering
// from a table): a hardened install passes; an admin role, a missing
// boundary, an open window or an idle role with deploy rights fail. And the
// check only ever reads.
//
//   node --test selfhost/launch/*.test.mjs
import { test } from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const SCRIPT = fileURLToPath(new URL("../check-access.mjs", import.meta.url));
const ACCOUNT = "111122223333";
const B = `arn:aws:iam::${ACCOUNT}:policy/coderhelm-prod-boundary-1a2b3c4d`;
const DEPLOY = `arn:aws:iam::${ACCOUNT}:role/coderhelm-prod-deploy-1a2b3c4d`;

function healthy() {
  const role = (name, extra = {}) => ({ RoleName: name, Arn: `arn:aws:iam::${ACCOUNT}:role/${name}`, PermissionsBoundary: { PermissionsBoundaryArn: B }, ...extra });
  return {
    roles: {
      "coderhelm-prod-deploy-1a2b3c4d": role("coderhelm-prod-deploy-1a2b3c4d", { AssumeRolePolicyDocument: { Statement: [{ Effect: "Allow", Principal: { Service: "cloudformation.amazonaws.com" }, Action: "sts:AssumeRole" }] } }),
      "coderhelm-prod-update-build": role("coderhelm-prod-update-build"),
      "coderhelm-prod-installer-1a2b3c4d": role("coderhelm-prod-installer-1a2b3c4d"),
      "coderhelm-prod-ServiceRole-ab12cd34": role("coderhelm-prod-ServiceRole-ab12cd34"),
      "coderhelm-prod-ServiceRole-0000aaaa": role("coderhelm-prod-ServiceRole-0000aaaa", { Tags: [{ Key: "coderhelm:role", Value: "gateway" }] }),
      "coderhelm-prod-ServiceRole-1111bbbb": role("coderhelm-prod-ServiceRole-1111bbbb", { Tags: [{ Key: "coderhelm:role", Value: "worker" }] }),
    },
    functions: { "coderhelm-prod-gateway": "coderhelm-prod-ServiceRole-0000aaaa", "coderhelm-prod-worker": "coderhelm-prod-ServiceRole-1111bbbb" },
    // What the boundary allows in this account, as "<caller tag>|<action>|<role>" (the simulator's answer).
    boundaryAllows: ["gateway", "worker"].flatMap((t) => ["CoderHelmLogReader", "CoderhelmReadOnly"].map((r) => `${t}|sts:AssumeRole|${r}`)),
    attached: { "coderhelm-prod-ServiceRole-ab12cd34": [{ PolicyArn: "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole" }] },
    inline: { "coderhelm-prod-update-build": { idle: { Statement: [{ Effect: "Allow", Action: "lambda:InvokeFunction", Resource: "arn:aws:lambda:x" }] } } },
    stacks: [{ StackName: "coderhelm-prod-api", RoleARN: DEPLOY, StackStatus: "UPDATE_COMPLETE" }, { StackName: "other-app", RoleARN: "arn:aws:iam::1:role/x", StackStatus: "CREATE_COMPLETE" }],
    allowedForBuilds: [],
    deployDecisions: {},
    // CodeBuild projects by the role they run as. The installer's is named after the Launch Stack.
    projects: {
      "coderhelm-prod-update": `arn:aws:iam::${ACCOUNT}:role/coderhelm-prod-update-build`,
      "my-launch-stack-installer": `arn:aws:iam::${ACCOUNT}:role/coderhelm-prod-installer-1a2b3c4d`,
    },
    builds: [
      { project: "coderhelm-prod-update", id: "coderhelm-prod-update:0001", status: "SUCCEEDED" },
      { project: "my-launch-stack-installer", id: "my-launch-stack-installer:0001", status: "SUCCEEDED" },
    ],
  };
}

/** A fake aws CLI: answers from state.json, logs every call. */
function fakeAws(state) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "chcheck-"));
  fs.writeFileSync(path.join(dir, "state.json"), JSON.stringify(state));
  fs.writeFileSync(path.join(dir, "aws"), `#!/usr/bin/env node
const fs = require("fs"), path = require("path");
const s = JSON.parse(fs.readFileSync(path.join(__dirname, "state.json"), "utf8"));
const a = process.argv.slice(2);
fs.appendFileSync(path.join(__dirname, "calls.log"), a.join(" ") + "\\n");
const arg = (k) => a[a.indexOf(k) + 1];
const out = (v) => { process.stdout.write(JSON.stringify(v)); process.exit(0); };
const none = () => { process.stderr.write("An error occurred (NoSuchEntity) when calling: not found"); process.exit(254); };
const k = a[0] + " " + a[1];
if (k === "sts get-caller-identity") out({ Account: "${ACCOUNT}" });
if (k === "ssm get-parameter") out({ Parameter: { Value: JSON.stringify({ access: { mode: "hardened", owner: "launch", id: "1a2b3c4d" } }) } });
if (k === "iam list-roles") out({ Roles: Object.values(s.roles).concat([{ RoleName: "cdk-hnb659fds-deploy-role-${ACCOUNT}-us-east-1" }, { RoleName: "someone-else" }]) });
if (k === "iam get-role") { const r = s.roles[arg("--role-name")]; r ? out({ Role: r }) : none(); }
if (k === "iam list-attached-role-policies") out({ AttachedPolicies: s.attached[arg("--role-name")] || [] });
if (k === "iam list-role-policies") out({ PolicyNames: Object.keys(s.inline[arg("--role-name")] || {}) });
if (k === "iam get-role-policy") { const p = (s.inline[arg("--role-name")] || {})[arg("--policy-name")]; p ? out({ PolicyDocument: p }) : none(); }
if (k === "cloudformation describe-stacks") out({ Stacks: s.stacks });
if (k === "iam get-policy") out({ Policy: { DefaultVersionId: "v3" } });
if (k === "iam get-policy-version") out({ PolicyVersion: { Document: { Version: "2012-10-17", Statement: [] } } });
if (k === "cloudformation describe-stack-resources") {
  const st = arg("--stack-name"), f = st === "coderhelm-prod-api" ? "coderhelm-prod-gateway" : st === "coderhelm-prod-worker" ? "coderhelm-prod-worker" : null;
  const id = st === "coderhelm-prod-api" ? "GatewayServiceRole162DEA77" : "WorkerServiceRole8543584E";
  out({ StackResources: f && (s.functions || {})[f] ? [{ ResourceType: "AWS::IAM::Role", LogicalResourceId: "OtherServiceRole1", PhysicalResourceId: "coderhelm-prod-ServiceRole-ab12cd34" }, { ResourceType: "AWS::IAM::Role", LogicalResourceId: id, PhysicalResourceId: s.functions[f] }] : [] });
}
if (k === "iam simulate-custom-policy") {
  const all = a.join(" "), tag = (all.match(/aws:PrincipalTag[/]coderhelm:role,ContextKeyValues=([^,]+)/) || [])[1] || "";
  const action = arg("--action-names"), role = arg("--resource-arns").split(":role/").pop();
  if (action.startsWith("iam:")) out({ EvaluationResults: [{ EvalActionName: action, EvalDecision: s.tagGuardMissing || all.includes("deploy-1a2b3c4d") ? "allowed" : "explicitDeny" }] });
  out({ EvaluationResults: [{ EvalActionName: action, EvalDecision: s.boundaryAllows.includes(tag + "|" + action + "|" + role) ? "allowed" : "explicitDeny" }] });
}
if (k === "codebuild list-projects" && s.projectsDenied) { process.stderr.write("An error occurred (AccessDeniedException) when calling the ListProjects operation"); process.exit(254); }
if (k === "codebuild list-projects") out({ projects: Object.keys(s.projects || {}).concat(["someone-elses-build"]) });
if (k === "codebuild batch-get-projects") out({ projects: a.slice(a.indexOf("--names") + 1).filter((n) => !n.startsWith("--")).map((n) => ({ name: n, serviceRole: (s.projects || {})[n] || "arn:aws:iam::1:role/other" })) });
if (k === "codebuild list-builds-for-project") out({ ids: (s.builds || []).filter((b) => b.project === arg("--project-name")).map((b) => b.id) });
if (k === "codebuild batch-get-builds") { const ids = a.slice(a.indexOf("--ids") + 1).filter((n) => !n.startsWith("--")); out({ builds: (s.builds || []).filter((b) => ids.includes(b.id)).map((b) => ({ id: b.id, buildStatus: b.status })) }); }
if (k === "iam simulate-principal-policy") {
  const who = arg("--policy-source-arn"), action = arg("--action-names");
  let d = "implicitDeny";
  const all = a.join(" ");
  const tagged = all.match(/aws:PrincipalTag[/]coderhelm:role,ContextKeyValues=([^,]+)/);
  if (action === "sts:AssumeRole" && tagged) d = s.boundaryAllows.includes(tagged[1] + "|sts:AssumeRole|" + arg("--resource-arns").split(":role/").pop()) ? "allowed" : "explicitDeny";
  else if (who.includes("deploy")) d = s.deployDecisions[action] || ((action === "lambda:UpdateFunctionCode" && all.includes("function:coderhelm-prod-")) || (action === "iam:CreateRole" && all.includes("role/coderhelm-prod-check") && all.includes("iam:PermissionsBoundary")) ? "allowed" : "implicitDeny");
  else if (s.allowedForBuilds.includes(action)) d = "allowed";
  out({ EvaluationResults: [{ EvalActionName: action, EvalDecision: d }] });
}
process.stderr.write("unexpected: " + a.join(" ")); process.exit(1);
`, { mode: 0o755 });
  return dir;
}

function check(state) {
  const dir = fakeAws(state);
  const r = spawnSync(process.execPath, [SCRIPT, "--stage", "prod", "--region", "us-east-1"], { encoding: "utf8", env: { ...process.env, PATH: `${dir}:${process.env.PATH}` } });
  const calls = fs.existsSync(path.join(dir, "calls.log")) ? fs.readFileSync(path.join(dir, "calls.log"), "utf8").trim().split("\n") : [];
  return { code: r.status, out: r.stdout + r.stderr, calls };
}

const row = (out, name) => out.split("\n").find((l) => l.includes(name)) ?? "";

test("a hardened install passes every row, and the check only reads", () => {
  const { code, out, calls } = check(healthy());
  assert.match(row(out, "every CoderHelm role carries the boundary"), /^PASS/);
  assert.match(row(out, "no role has AdministratorAccess"), /^PASS/);
  assert.match(row(out, "deploy role's trust allows CloudFormation only"), /^PASS/);
  assert.match(row(out, "CDK bootstrap roles are not used"), /^PASS.*not CoderHelm's/);
  assert.match(row(out, "coderhelm-prod-update-build: no deploy rights while idle"), /^PASS/);
  assert.match(row(out, "coderhelm-prod-installer-1a2b3c4d: no deploy window while idle"), /^PASS/);
  assert.match(row(out, "refused IAM users"), /^PASS/);
  assert.match(row(out, "no other role in this account can be assumed"), /^PASS.*expected exception: the gateway and worker may assume CoderHelmLogReader and CoderhelmReadOnly/);
  assert.match(row(out, "only the gateway's and worker's roles carry coderhelm:role"), /^PASS.*gateway: coderhelm-prod-ServiceRole-0000aaaa/);
  assert.match(row(out, "the gateway and worker roles: only the read roles"), /^PASS.*gateway: CoderHelmLogReader allowed, CoderhelmReadOnly allowed/);
  assert.match(row(out, "read roles in this account only read"), /^PASS.*none in this account/);
  for (const c of calls) assert.match(c, /^(sts get-caller-identity|ssm get-parameter|iam (list-|get-|simulate-principal-policy|simulate-custom-policy)|cloudformation describe-stack(s|-resources)|codebuild (list-|batch-get-))/, `read-only: ${c}`);
  assert.doesNotMatch(out, /^FAIL/m, out);
  assert.equal(code, 0);
});

test("an admin role, a role without the boundary, *:* and an old stack role each fail their row", () => {
  const s = healthy();
  s.attached["coderhelm-prod-installer-1a2b3c4d"] = [{ PolicyArn: "arn:aws:iam::aws:policy/AdministratorAccess" }];
  delete s.roles["coderhelm-prod-ServiceRole-ab12cd34"].PermissionsBoundary;
  s.inline["coderhelm-prod-ServiceRole-ab12cd34"] = { all: { Statement: [{ Effect: "Allow", Action: "*", Resource: "*" }] } };
  s.stacks.push({ StackName: "coderhelm-prod-web", RoleARN: `arn:aws:iam::${ACCOUNT}:role/cdk-hnb659fds-cfn-exec-role-${ACCOUNT}-us-east-1`, StackStatus: "UPDATE_COMPLETE" });
  const { code, out } = check(s);
  assert.equal(code, 1);
  assert.match(row(out, "no role has AdministratorAccess"), /^FAIL.*installer-1a2b3c4d/);
  assert.match(row(out, "every CoderHelm role carries the boundary"), /^FAIL.*ServiceRole-ab12cd34/);
  assert.match(row(out, "no policy allows *:*"), /^FAIL.*inline all/);
  assert.match(row(out, "every CoderHelm stack deploys through the deploy role"), /^FAIL.*coderhelm-prod-web/);
  assert.match(row(out, "CDK bootstrap roles are not used"), /^FAIL.*coderhelm-prod-web/);
});

test("an idle build role with deploy rights or an open window fails", () => {
  const s = healthy();
  s.allowedForBuilds = ["iam:PassRole"];
  s.inline["coderhelm-prod-update-build"]["deploy-window"] = { Statement: [] };
  const { code, out } = check(s);
  assert.equal(code, 1);
  assert.match(row(out, "coderhelm-prod-update-build: no deploy rights while idle"), /^FAIL.*iam:PassRole/);
  assert.match(row(out, "coderhelm-prod-update-build: no deploy window while idle"), /^FAIL.*nothing running/);
});

// 10/30: an update build started a minute before the check. It held its
// window while it downloaded and synthesized the release, so no stack was
// changing yet, and the check (which only looked at stacks) said "nothing
// running". The window belongs to the running build: a PASS that says so.
test("a window held by a running build passes with the build's id; the installer's project is found by its role", () => {
  const s = healthy();
  s.inline["coderhelm-prod-update-build"]["deploy-window"] = { Statement: [] };
  s.inline["coderhelm-prod-installer-1a2b3c4d"] = { "deploy-window": { Statement: [] } };
  s.builds.unshift(
    { project: "coderhelm-prod-update", id: "coderhelm-prod-update:0002", status: "IN_PROGRESS" },
    { project: "my-launch-stack-installer", id: "my-launch-stack-installer:0002", status: "IN_PROGRESS" },
  );
  // Every stack is settled: the build has not reached CloudFormation yet.
  assert.ok(s.stacks.every((x) => !/_IN_PROGRESS$/.test(x.StackStatus)));
  const { code, out, calls } = check(s);
  assert.match(row(out, "coderhelm-prod-update-build: no deploy window while idle"), /^PASS.*window open for running build coderhelm-prod-update:0002/);
  assert.match(row(out, "coderhelm-prod-installer-1a2b3c4d: no deploy window while idle"), /^PASS.*window open for running build my-launch-stack-installer:0002/);
  assert.ok(!calls.some((c) => c.includes("someone-elses-build") && c.startsWith("codebuild list-builds")), "only the build roles' projects");
  assert.doesNotMatch(out, /^FAIL/m, out);
  assert.equal(code, 0);
});

test("a window with only finished builds, or with builds that can't be listed, fails", () => {
  const s = healthy();
  s.inline["coderhelm-prod-update-build"]["deploy-window"] = { Statement: [] };
  s.stacks[0].StackStatus = "UPDATE_IN_PROGRESS";
  let { out } = check(s);
  assert.match(row(out, "coderhelm-prod-update-build: no deploy window while idle"), /^FAIL.*stack is changing, with no build running/);
  ({ out } = check({ ...s, projectsDenied: true }));
  assert.match(row(out, "coderhelm-prod-update-build: no deploy window while idle"), /^FAIL.*could not be listed.*AccessDenied/);
});

test("a deploy role that could make an IAM user fails", () => {
  const s = healthy();
  s.deployDecisions = { "iam:CreateUser": "allowed" };
  const { out } = check(s);
  assert.match(row(out, "refused IAM users"), /^FAIL.*iam:CreateUser/);
});

// Same-account data sources: the boundary may let the gateway and worker
// take on CoderHelm's two read roles here, and nothing broader.
test("a boundary from before the read-role exception passes and says so", () => {
  const s = healthy();
  s.boundaryAllows = [];
  for (const r of Object.values(s.roles)) delete r.Tags;
  const { code, out } = check(s);
  assert.match(row(out, "no other role in this account can be assumed"), /^PASS.*no exception/);
  assert.match(row(out, "only the gateway's and worker's roles carry coderhelm:role"), /^PASS.*no role carries it yet/);
  assert.equal(code, 0, out);
});

test("a boundary that lets the gateway assume another role here, or more than AssumeRole, or an untagged role in, fails", () => {
  for (const extra of ["gateway|sts:AssumeRole|coderhelm-prod-deploy-1a2b3c4d", "worker|sts:AssumeRole|CoderhelmReadOnly-2", "gateway|sts:TagSession|CoderhelmReadOnly", "|sts:AssumeRole|CoderHelmLogReader", "log-analyzer|sts:AssumeRole|CoderhelmReadOnly"]) {
    const s = healthy();
    s.boundaryAllows.push(extra);
    const { code, out } = check(s);
    assert.equal(code, 1, extra);
    assert.match(row(out, "no other role in this account can be assumed"), /^FAIL/, extra);
  }
});

test("another role carrying the gateway or worker tag fails", () => {
  const s = healthy();
  s.roles["coderhelm-prod-ServiceRole-ab12cd34"].Tags = [{ Key: "Coderhelm:Role", Value: "worker" }];
  const { code, out } = check(s);
  assert.equal(code, 1);
  assert.match(row(out, "only the gateway's and worker's roles carry coderhelm:role"), /^FAIL.*ServiceRole-ab12cd34/);
});

test("a boundary with the exception but without the tag guard fails", () => {
  const s = healthy();
  s.tagGuardMissing = true;
  const { code, out } = check(s);
  assert.equal(code, 1);
  assert.match(row(out, "no other role in this account can be assumed"), /^FAIL.*a role other than the deploy role: iam:TagRole with tag coderhelm:role/);
});

test("read roles here: one that only reads with an external id passes; no external id, another account, a managed policy or a role-passing statement fails", () => {
  const trust = (extra = {}) => ({ Statement: [{ Effect: "Allow", Principal: { AWS: "arn:aws:iam::111122223333:root" }, Action: "sts:AssumeRole", Condition: { StringEquals: { "sts:ExternalId": "x" } }, ...extra }] });
  const ok = healthy();
  ok.roles.CoderhelmReadOnly = { RoleName: "CoderhelmReadOnly", Arn: "arn:aws:iam::111122223333:role/CoderhelmReadOnly", AssumeRolePolicyDocument: trust() };
  ok.inline.CoderhelmReadOnly = { "coderhelm-read-logs": { Statement: [{ Effect: "Allow", Action: ["logs:FilterLogEvents", "logs:StartQuery"], Resource: "*" }] } };
  let r = check(ok);
  assert.match(row(r.out, "read roles in this account only read"), /^PASS.*CoderhelmReadOnly: external id required/);
  const cases = [
    [(s) => { s.roles.CoderhelmReadOnly.AssumeRolePolicyDocument = trust({ Condition: {} }); }, /no sts:ExternalId/],
    [(s) => { s.roles.CoderhelmReadOnly.AssumeRolePolicyDocument = trust({ Principal: { AWS: "arn:aws:iam::999988887777:root" } }); }, /trust names/],
    [(s) => { s.attached.CoderhelmReadOnly = [{ PolicyArn: "arn:aws:iam::aws:policy/ReadOnlyAccess" }]; }, /managed policy/],
    [(s) => { s.inline.CoderhelmReadOnly.more = { Statement: [{ Effect: "Allow", Action: "iam:PassRole", Resource: "*" }] }; }, /allows iam:passrole/],
    [(s) => { s.inline.CoderhelmReadOnly.more = { Statement: [{ Effect: "Allow", Action: "s3:*", Resource: "*" }] }; }, /allows s3:\*/],
  ];
  for (const [change, why] of cases) {
    const s = JSON.parse(JSON.stringify(ok));
    change(s);
    r = check(s);
    assert.equal(r.code, 1, String(why));
    assert.match(row(r.out, "read roles in this account only read"), new RegExp("^FAIL.*" + why.source), String(why));
  }
});
