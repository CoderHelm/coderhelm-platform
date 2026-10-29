// Hardened installs (config `access`, docs/self-host/security.md): they
// deploy through their own deploy role and never bootstrap; an install
// without `access` runs every command exactly as before.
//
//   cd installer && npm test
import { test } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { main } from "../lib/cli.mjs";
import { initialSecret } from "../lib/secrets.mjs";
import { Runner } from "../lib/runner.mjs";
import { accessOf } from "../lib/util.mjs";
import { FakeRunner, quietAsk } from "./fake.mjs";

const ACCOUNT = "111122223333";
const LAUNCH = { mode: "hardened", owner: "launch", id: "1a2b3c4d" };
const DEPLOY_ROLE = `arn:aws:iam::${ACCOUNT}:role/coderhelm-prod-deploy-1a2b3c4d`;

function setup(over = {}, version = "v1.0.0") {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "chacc-"));
  const file = path.join(dir, "coderhelm.config.json");
  fs.writeFileSync(file, JSON.stringify({
    account: ACCOUNT, region: "us-west-2", stage: "prod", domain: null, models: { provider: "anthropic" },
    email: { from: "helm@example.org" }, admins: ["me@example.org"], telemetry: { enabled: false }, ...over,
  }));
  fs.mkdirSync(path.join(dir, ".coderhelm"));
  fs.writeFileSync(path.join(dir, ".coderhelm", "state.json"), JSON.stringify({ version, installId: "i" }));
  return { dir, file };
}

function answers(extra = {}) {
  const stacks = ["database", "storage", "api", "worker", "streaming", "web", "email", "monitoring"].map((s) => ({ StackName: `coderhelm-prod-${s}`, StackStatus: "UPDATE_COMPLETE" }));
  return {
    "aws --version": "aws-cli/2.17.0 Python/3",
    "sts get-caller-identity": { Account: ACCOUNT, Arn: `arn:aws:iam::${ACCOUNT}:user/me` },
    "tar --version": "bsdtar 3.5",
    "list-stacks": { StackSummaries: stacks },
    "get-function-configuration": { State: "Active", LastUpdateStatus: "Successful" },
    "get-queue-url": { QueueUrl: "https://q" },
    "get-queue-attributes": { Attributes: { ApproximateNumberOfMessages: "0" } },
    "get-secret-value": JSON.stringify(JSON.stringify({ ...initialSecret(), github_app_id: "1", github_private_key: "k" })),
    "sesv2 get-account": { ProductionAccessEnabled: true },
    ...extra,
  };
}

/** A release source; `hardened` says whether its CDK app knows hardened installs. */
const source = (calls, hardened = (v) => true) => async (_ctx, version) => {
  calls.push(version);
  const infra = fs.mkdtempSync(path.join(os.tmpdir(), "chinfra-"));
  fs.mkdirSync(path.join(infra, "lib"));
  fs.mkdirSync(path.join(infra, "node_modules"));
  if (hardened(version)) fs.writeFileSync(path.join(infra, "lib", "access.ts"), "// access");
  return { version, infra, dashboard: null, env: {} };
};

const run = (argv, deps) => main(argv, { log: () => {}, interactive: false, ask: quietAsk(), ...deps });
const cdkCalls = (runner) => runner.commands.filter((c) => c.cmd === "npx" && c.args[1] === "cdk");

test("access: the names follow infra/lib/access.ts", () => {
  const a = accessOf({ account: ACCOUNT, region: "us-west-2", stage: "prod", access: LAUNCH });
  assert.deepEqual(a, {
    owner: "launch",
    deployRoleArn: DEPLOY_ROLE,
    boundaryArn: `arn:aws:iam::${ACCOUNT}:policy/coderhelm-prod-boundary-1a2b3c4d`,
    assetsBucket: `coderhelm-prod-assets-1a2b3c4d-${ACCOUNT}`,
    accessStackName: "coderhelm-prod-access",
  });
  assert.equal(accessOf({ account: ACCOUNT, region: "us-west-2", stage: "prod" }), null);
  assert.equal(accessOf({ account: ACCOUNT, region: "us-gov-west-1", stage: "prod", access: { mode: "hardened", owner: "app" } }).deployRoleArn,
    `arn:aws-us-gov:iam::${ACCOUNT}:role/coderhelm-prod-deploy`);
});

test("without access: deploy bootstraps and deploys exactly as before, no role, no access stack setting", async () => {
  const { file } = setup();
  const runner = new Runner({ dryRun: true, log: () => {} });
  await run(["deploy", "--dry-run", "--config", file, "--version", "v1.2.3", "--skip-github-app"], { runner });
  const lines = cdkCalls(runner).map((c) => c.args.slice(1).join(" "));
  assert.deepEqual(lines.map((l) => l.split(" ").slice(0, 2).join(" ")), ["cdk bootstrap", "cdk deploy"]);
  assert.equal(lines[1], `cdk deploy --all --require-approval never --progress events --outputs-file ${path.join(path.dirname(file), ".coderhelm", "outputs.json")}`);
  // The dry-run Runner records the names of the variables it sets.
  for (const c of cdkCalls(runner)) assert.ok(!c.env.includes("CODERHELM_ACCESS_STACK"), "no new environment for old installs");
});

test("without access: an update deploys and rolls back exactly as before", async () => {
  const { file, dir } = setup();
  let n = 0;
  const runner = new FakeRunner(answers({ "get-function-configuration": () => (n++ < 3 ? { State: "Active", LastUpdateStatus: "Successful" } : { State: "Active", LastUpdateStatus: "Failed" }) }));
  const calls = [];
  await run(["update", "--config", file, "--to", "v1.1.0", "--yes", "--auto-rollback", "--result-file", path.join(dir, "r.json")],
    { runner, resolveSource: source(calls, () => false), log: () => {} }).catch(() => null);
  assert.deepEqual(calls, ["v1.1.0", "v1.0.0"]);
  const deploys = cdkCalls(runner).map((c) => c.args.slice(1).join(" "));
  assert.deepEqual(deploys, ["cdk deploy --all --require-approval never --progress events --outputs-file " + path.join(dir, ".coderhelm", "outputs.json")].concat(
    ["cdk deploy --all --require-approval never --progress events --outputs-file " + path.join(dir, ".coderhelm", "outputs.json")]));
  assert.equal(JSON.parse(fs.readFileSync(path.join(dir, "r.json"), "utf8")).status, "rolled_back");
});

test("hardened: deploy never bootstraps, deploys through the install's deploy role, and skips the access stack", async () => {
  const { file } = setup({ access: LAUNCH });
  const runner = new Runner({ dryRun: true, log: () => {} });
  await run(["deploy", "--dry-run", "--config", file, "--version", "v1.2.3", "--skip-github-app"], { runner });
  const calls = cdkCalls(runner);
  assert.ok(!calls.some((c) => c.args.includes("bootstrap")), "no CDK bootstrap");
  const dep = calls.find((c) => c.args.includes("deploy"));
  assert.deepEqual(dep.args.slice(-2), ["--role-arn", DEPLOY_ROLE]);
  assert.ok(dep.env.includes("CODERHELM_ACCESS_STACK"));
});

test("hardened: update and uninstall go through the deploy role too", async () => {
  const { file, dir } = setup({ access: LAUNCH });
  const runner = new FakeRunner(answers());
  await run(["update", "--config", file, "--to", "v1.1.0", "--yes", "--result-file", path.join(dir, "r.json")], { runner, resolveSource: source([]) });
  for (const c of cdkCalls(runner).filter((x) => x.args.includes("deploy"))) assert.ok(c.args.includes(DEPLOY_ROLE));
  const r2 = new FakeRunner(answers());
  await run(["uninstall", "--config", file, "--yes"], { runner: r2, resolveSource: source([]) });
  const destroy = cdkCalls(r2).find((c) => c.args.includes("destroy"));
  assert.deepEqual(destroy.args.slice(-2), ["--role-arn", DEPLOY_ROLE]);
});

test("hardened: going to a release from before hardening needs an admin, and an auto-rollback to one stops instead", async () => {
  const { file, dir } = setup({ access: LAUNCH }, "v1.1.0");
  fs.writeFileSync(path.join(dir, ".coderhelm", "state.json"), JSON.stringify({ version: "v1.1.0", previousVersion: "v1.0.0", versions: ["v1.0.0", "v1.1.0"] }));
  const old = (v) => v !== "v1.0.0";
  const runner = new FakeRunner(answers());
  await assert.rejects(() => run(["rollback", "--config", file, "--to", "v1.0.0", "--yes"], { runner, resolveSource: source([], old) }), /from before hardened installs.*needs an admin/s);
  assert.ok(!cdkCalls(runner).some((c) => c.args.includes("deploy")), "nothing deployed");

  let n = 0;
  const sick = new FakeRunner(answers({ "get-function-configuration": () => (n++ < 4 ? { State: "Active", LastUpdateStatus: "Successful" } : { State: "Active", LastUpdateStatus: "Failed" }) }));
  const calls = [];
  fs.writeFileSync(path.join(dir, ".coderhelm", "state.json"), JSON.stringify({ version: "v1.0.0" }));
  await run(["update", "--config", file, "--to", "v1.2.0", "--yes", "--auto-rollback", "--result-file", path.join(dir, "r.json")],
    { runner: sick, resolveSource: source(calls, old) }).catch(() => null);
  assert.deepEqual(calls, ["v1.2.0", "v1.0.0"]);
  assert.equal(cdkCalls(sick).filter((c) => c.args.includes("deploy")).length, 1, "the old release is not deployed");
  const r = JSON.parse(fs.readFileSync(path.join(dir, "r.json"), "utf8"));
  assert.equal(r.status, "failed");
  assert.match(r.reason, /needs an admin/);
});

test("access harden: the access stack with admin credentials, every stack once as admin, then through the deploy role", async () => {
  const { file } = setup({}, "v1.1.0");
  const runner = new FakeRunner(answers({ "codebuild list-projects": { projects: [] } }));
  await run(["access", "harden", "--config", file, "--yes"], { runner, resolveSource: source([]) });
  const cfg = JSON.parse(fs.readFileSync(file, "utf8"));
  assert.deepEqual(cfg.access, { mode: "hardened", owner: "app" });
  const steps = cdkCalls(runner).map((c) => [c.args.slice(1).join(" "), c.env.CODERHELM_ACCESS_STACK]);
  const role = `arn:aws:iam::${ACCOUNT}:role/coderhelm-prod-deploy`;
  assert.deepEqual(steps, [
    ["cdk deploy coderhelm-prod-access --exclusively --require-approval never", "include"],
    ["cdk deploy --all --require-approval never --progress events", "skip"],
    [`cdk deploy --all --require-approval never --force --role-arn ${role}`, "skip"],
  ]);
  const saved = runner.lines().find((l) => l.includes("ssm put-parameter") && l.includes("/coderhelm-prod/installer/config"));
  assert.ok(saved && saved.includes('"owner":"app"'), "the account's copy says hardened before the stacks change");
});

test("access harden: a failed first step leaves the config as it was", async () => {
  const { file } = setup({}, "v1.1.0");
  const runner = new FakeRunner(answers({ "coderhelm-prod-access": { code: 1, stderr: "AccessDenied" } }));
  runner.run = ((orig) => async (cmd, args, opts) => {
    const r = await orig.call(runner, cmd, args, opts);
    if (cmd === "npx" && args.includes("coderhelm-prod-access")) throw new Error("cdk deploy failed: AccessDenied");
    return r;
  })(runner.run);
  await assert.rejects(() => run(["access", "harden", "--config", file, "--yes"], { runner, resolveSource: source([]) }), /before changing CoderHelm's stacks/);
  assert.equal(JSON.parse(fs.readFileSync(file, "utf8")).access, undefined);
});

test("access harden: takes AdministratorAccess off the old Launch Stack installer role of this install only", async () => {
  const { file } = setup({}, "v1.1.0");
  const project = (name, stage, role) => ({
    name, serviceRole: `arn:aws:iam::${ACCOUNT}:role/${role}`,
    source: { buildspec: "... node launch-bootstrap.mjs ..." },
    environment: { environmentVariables: [{ name: "STAGE", value: stage }] },
  });
  const runner = new FakeRunner(answers({
    "codebuild list-projects": { projects: ["other-installer", "launch-installer", "unrelated"] },
    "codebuild batch-get-projects": { projects: [project("other-installer", "dev", "dev-role"), project("launch-installer", "prod", "launch-InstallerRole-X")] },
    "list-attached-role-policies": { AttachedPolicies: [{ PolicyArn: "arn:aws:iam::aws:policy/AdministratorAccess" }] },
  }));
  await run(["access", "harden", "--config", file, "--yes"], { runner, resolveSource: source([]) });
  const detach = runner.lines().filter((l) => l.includes("detach-role-policy"));
  assert.deepEqual(detach, ["aws iam detach-role-policy --role-name launch-InstallerRole-X --policy-arn arn:aws:iam::aws:policy/AdministratorAccess"]);
});

test("access status says how the install deploys", async () => {
  const logs = [];
  await run(["access", "--config", setup().file], { log: (m) => logs.push(m) });
  await run(["access", "status", "--config", setup({ access: LAUNCH }).file], { log: (m) => logs.push(m) });
  assert.match(logs[0], /not hardened/);
  assert.match(logs[1], /Launch Stack[\s\S]*coderhelm-prod-deploy-1a2b3c4d/);
});
