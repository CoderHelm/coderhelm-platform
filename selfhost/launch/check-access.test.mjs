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
    },
    attached: { "coderhelm-prod-ServiceRole-ab12cd34": [{ PolicyArn: "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole" }] },
    inline: { "coderhelm-prod-update-build": { idle: { Statement: [{ Effect: "Allow", Action: "lambda:InvokeFunction", Resource: "arn:aws:lambda:x" }] } } },
    stacks: [{ StackName: "coderhelm-prod-api", RoleARN: DEPLOY, StackStatus: "UPDATE_COMPLETE" }, { StackName: "other-app", RoleARN: "arn:aws:iam::1:role/x", StackStatus: "CREATE_COMPLETE" }],
    allowedForBuilds: [],
    deployDecisions: {},
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
if (k === "iam simulate-principal-policy") {
  const who = arg("--policy-source-arn"), action = arg("--action-names");
  let d = "implicitDeny";
  const all = a.join(" ");
  if (who.includes("deploy")) d = s.deployDecisions[action] || ((action === "lambda:UpdateFunctionCode" && all.includes("function:coderhelm-prod-")) || (action === "iam:CreateRole" && all.includes("role/coderhelm-prod-check") && all.includes("iam:PermissionsBoundary")) ? "allowed" : "implicitDeny");
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
  for (const c of calls) assert.match(c, /^(sts get-caller-identity|ssm get-parameter|iam (list-|get-|simulate-principal-policy)|cloudformation describe-stacks)/, `read-only: ${c}`);
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

test("a deploy role that could make an IAM user fails", () => {
  const s = healthy();
  s.deployDecisions = { "iam:CreateUser": "allowed" };
  const { out } = check(s);
  assert.match(row(out, "refused IAM users"), /^FAIL.*iam:CreateUser/);
});
