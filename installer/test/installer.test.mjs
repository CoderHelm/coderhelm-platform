// The installer, run against a fake AWS: what each command does, in order,
// and what it never does (print a secret, deploy in a dry run, send
// telemetry when it is off).
//
//   cd installer && npm test
import { test } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import http from "node:http";
import { main } from "../lib/cli.mjs";
import * as cmd from "../lib/commands.mjs";
import { buildManifest, convertCode, EVENTS, formPage, newAppUrl, PERMISSIONS, runManifestFlow, secretKeysFromApp } from "../lib/github-app.mjs";
import { parseSums, verifySignature, verifySums } from "../lib/release.mjs";
import { initialSecret, hasGithubApp } from "../lib/secrets.mjs";
import { checkPrereqs } from "../lib/prereqs.mjs";
import { redactArg, Runner } from "../lib/runner.mjs";
import { derived, parseArgs, semverCompare, sha256File } from "../lib/util.mjs";
import { FakeRunner, quietAsk } from "./fake.mjs";

const ACCOUNT = "111122223333";

function tmpdir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), "chinst-"));
}

function awsAnswers(extra = {}) {
  return {
    "aws --version": "aws-cli/2.17.0 Python/3",
    "sts get-caller-identity": { Account: ACCOUNT, Arn: `arn:aws:iam::${ACCOUNT}:user/me` },
    "tar --version": "bsdtar 3.5",
    ...extra,
  };
}

function writeConfig(dir, over = {}) {
  const cfg = {
    account: ACCOUNT,
    region: "us-west-2",
    stage: "prod",
    domain: null,
    models: { provider: "anthropic" },
    email: { from: "helm@example.org" },
    admins: ["me@example.org"],
    telemetry: { enabled: false },
    ...over,
  };
  const file = path.join(dir, "coderhelm.config.json");
  fs.writeFileSync(file, JSON.stringify(cfg));
  return file;
}

async function run(argv, deps) {
  const logs = [];
  const code = await main(argv, { log: (m) => logs.push(String(m)), interactive: false, ask: quietAsk(), ...deps });
  return { code, out: logs.join("\n") };
}

// ── CLI basics ──────────────────────────────────────────────────────────

test("help and unknown commands", async () => {
  assert.equal((await run(["help"])).code, 0);
  const bad = await run(["fly"]);
  assert.equal(bad.code, 2);
  assert.match(bad.out, /Unknown command "fly"/);
});

test("args: flags with and without values, camelCase", () => {
  assert.deepEqual(parseArgs(["deploy", "--dry-run", "--config", "a.json", "--to=v1.2.0"]), {
    _: ["deploy"],
    dryRun: true,
    config: "a.json",
    to: "v1.2.0",
  });
});

// ── prerequisites ───────────────────────────────────────────────────────

test("prerequisites: old Node, AWS CLI v1, wrong account", async () => {
  const r = new FakeRunner(awsAnswers({ "aws --version": "aws-cli/1.29.0" }));
  const { problems } = await checkPrereqs(r, { account: "999999999999", nodeVersion: "18.19.0" });
  assert.ok(problems.some((p) => /Node.js 20/.test(p)));
  assert.ok(problems.some((p) => /version 2/.test(p)));
  const r2 = new FakeRunner(awsAnswers());
  const { problems: p2 } = await checkPrereqs(r2, { account: "999999999999" });
  assert.ok(p2.some((p) => /credentials are for account 111122223333/.test(p)));
  const r3 = new FakeRunner(awsAnswers({ "sts get-caller-identity": { code: 255 } }));
  const { problems: p3 } = await checkPrereqs(r3, {});
  assert.ok(p3.some((p) => /No AWS credentials/.test(p)));
});

// ── init ────────────────────────────────────────────────────────────────

test("init writes a config from flags (non-interactive)", async () => {
  const dir = tmpdir();
  const file = path.join(dir, "coderhelm.config.json");
  const runner = new FakeRunner(awsAnswers());
  const { code } = await run(
    ["init", "--config", file, "--region", "eu-west-1", "--admin", "me@example.org", "--provider", "bedrock", "--telemetry", "off"],
    { runner },
  );
  assert.equal(code, 0);
  const cfg = JSON.parse(fs.readFileSync(file, "utf8"));
  assert.equal(cfg.account, ACCOUNT, "account from the caller identity");
  assert.equal(cfg.region, "eu-west-1");
  assert.equal(cfg.domain, null);
  assert.equal(cfg.models.provider, "bedrock");
  assert.equal(cfg.email.from, "coderhelm@example.org");
  assert.deepEqual(cfg.admins, ["me@example.org"]);
  assert.deepEqual(cfg.signup, { domains: ["example.org"] });
  assert.equal(cfg.telemetry.enabled, false, "telemetry is off unless asked for");
  // Running it again does not overwrite.
  await assert.rejects(() => main(["init", "--config", file], { runner, log: () => {}, interactive: false, ask: quietAsk() }), /already exists/);
});

test("init --dry-run writes nothing", async () => {
  const dir = tmpdir();
  const file = path.join(dir, "coderhelm.config.json");
  const { out } = await run(["init", "--dry-run", "--config", file, "--admin", "me@example.org", "--account", ACCOUNT], {
    runner: new FakeRunner(awsAnswers(), { dryRun: true }),
  });
  assert.ok(!fs.existsSync(file));
  assert.match(out, /\[dry run\] write/);
});

test("init with a domain asks for the zone", async () => {
  const cfg = cmd.buildConfig({
    account: ACCOUNT, region: "us-east-1", stage: "prod", hostname: "helm.example.org", zone: "example.org",
    provider: "anthropic", admin: "me@example.org", emailFrom: "helm@example.org",
  });
  assert.deepEqual(cfg.domain, { hostname: "helm.example.org", hostedZone: "example.org" });
  assert.deepEqual(cmd.quickCheck(cfg), []);
  assert.ok(cmd.quickCheck({ ...cfg, layout: "split" }).some((p) => /split/.test(p)));
});

// ── deploy (dry run) ────────────────────────────────────────────────────

test("deploy --dry-run from a release: prints the plan, runs nothing, never shows a secret", async () => {
  const dir = tmpdir();
  const file = writeConfig(dir, { region: "eu-west-1", domain: { hostname: "helm.example.org", hostedZone: "example.org" } });
  const runner = new Runner({ dryRun: true, log: () => {} });
  const { code, out } = await run(["deploy", "--dry-run", "--config", file, "--version", "v1.2.3", "--skip-github-app"], { runner });
  assert.equal(code, 0);
  const lines = runner.commands.map((c) => [c.cmd, ...c.args].join(" "));
  const order = [
    "aws --version",
    "aws sts get-caller-identity",
    "aws secretsmanager get-secret-value --secret-id coderhelm/prod/secrets",
    "aws secretsmanager create-secret --name coderhelm/prod/secrets",
    "npm ci",
    `npx --yes cdk bootstrap aws://${ACCOUNT}/eu-west-1`,
    `npx --yes cdk bootstrap aws://${ACCOUNT}/us-east-1`,
    "npx --yes cdk deploy --all --require-approval never",
  ];
  let at = -1;
  for (const want of order) {
    const i = lines.findIndex((l, j) => j > at && l.startsWith(want));
    assert.ok(i > at, `expected "${want}" after step ${at}:\n${lines.join("\n")}`);
    at = i;
  }
  assert.match(out, /download release v1.2.3/);
  assert.match(out, /https:\/\/helm\.example\.org\/\?signup=1/);
  const deployCmd = runner.commands.find((c) => c.args.includes("deploy"));
  for (const k of ["CODERHELM_CONFIG", "GATEWAY_ZIP", "WORKER_ZIP", "MCP_PROXY_DIR", "CODERHELM_DASHBOARD_DIR"]) {
    assert.ok(deployCmd.env.includes(k), `cdk deploy gets ${k}`);
  }
  const create = runner.commands.find((c) => c.args.includes("create-secret"));
  assert.ok(create.args.some((a) => a.startsWith("file://")), "secret values go in a file, not argv");
  assert.ok(!lines.join("\n").includes(initialSecret().jwt_secret));
  assert.ok(!fs.existsSync(path.join(dir, ".coderhelm", "state.json")), "a dry run saves no state");
});

test("deploy from a checkout (--source) uses its infra and no release download", async () => {
  const dir = tmpdir();
  const file = writeConfig(dir);
  const runner = new Runner({ dryRun: true, log: () => {} });
  const src = path.resolve(path.dirname(new URL(import.meta.url).pathname), "..", "..");
  const { out } = await run(["deploy", "--dry-run", "--config", file, "--source", src, "--skip-github-app"], { runner });
  assert.doesNotMatch(out, /download release/);
  const deployCmd = runner.commands.find((c) => c.args.includes("deploy"));
  assert.equal(deployCmd.cwd, path.join(src, "infra"));
  assert.ok(!runner.commands.some((c) => c.args.some((a) => a.includes("us-east-1"))), "no extra bootstrap without a domain");
});

test("deploy refuses a config for another account before touching anything", async () => {
  const dir = tmpdir();
  const file = writeConfig(dir, { account: "999999999999" });
  const runner = new FakeRunner(awsAnswers());
  await assert.rejects(() => main(["deploy", "--config", file], { runner, log: () => {}, interactive: false, ask: quietAsk() }), /credentials are for account/);
  assert.ok(!runner.lines().some((l) => /secretsmanager|cdk/.test(l)));
});

test("deploy without a config says to run init", async () => {
  await assert.rejects(() => main(["deploy", "--config", path.join(tmpdir(), "nope.json")], { log: () => {}, interactive: false, ask: quietAsk() }), /Run "coderhelm-selfhost init"/);
});

// ── GitHub App manifest flow ────────────────────────────────────────────

test("manifest: URLs, permissions and events", () => {
  const m = buildManifest({ publicUrl: "https://d1.cloudfront.net/", name: "CoderHelm prod 3333", redirectUrl: "http://127.0.0.1:5000/callback" });
  assert.equal(m.url, "https://d1.cloudfront.net");
  assert.equal(m.hook_attributes.url, "https://d1.cloudfront.net/webhooks/github");
  assert.deepEqual(m.callback_urls, ["https://d1.cloudfront.net/auth/github/callback"]);
  assert.equal(m.redirect_url, "http://127.0.0.1:5000/callback");
  assert.equal(m.public, false);
  assert.equal(m.default_permissions.contents, "write");
  assert.equal(m.default_permissions.pull_requests, "write");
  assert.deepEqual(m.default_permissions, PERMISSIONS);
  assert.ok(m.default_events.includes("pull_request_review_thread"));
  assert.deepEqual(m.default_events, EVENTS);
  assert.equal(newAppUrl(null, "s1"), "https://github.com/settings/apps/new?state=s1");
  assert.equal(newAppUrl("acme", "s1"), "https://github.com/organizations/acme/settings/apps/new?state=s1");
  const page = formPage({ name: '"><script>' }, newAppUrl("acme", "s"));
  assert.ok(!page.includes('"><script>'), "the manifest is escaped in the form");
});

test("manifest: the code becomes the secret's keys", async () => {
  const fakeFetch = async (url, init) => {
    assert.equal(url, "https://api.github.com/app-manifests/abc12345/conversions");
    assert.equal(init.method, "POST");
    return { ok: true, json: async () => ({ id: 42, slug: "coderhelm-prod", pem: "PEM", webhook_secret: "W", client_id: "Iv1", client_secret: "CS", html_url: "https://github.com/apps/coderhelm-prod" }) };
  };
  const app = await convertCode("abc12345", fakeFetch);
  assert.deepEqual(secretKeysFromApp(app), {
    github_app_id: "42", github_private_key: "PEM", github_webhook_secret: "W",
    github_client_id: "Iv1", github_client_secret: "CS", github_app_slug: "coderhelm-prod",
  });
  assert.ok(hasGithubApp(secretKeysFromApp(app)));
  assert.ok(!hasGithubApp(initialSecret()));
  await assert.rejects(() => convertCode("../../evil", fakeFetch), /unexpected code/);
  await assert.rejects(() => convertCode("abc12345", async () => ({ ok: false, status: 404 })), /refused/);
});

test("manifest flow end to end: local page, GitHub's redirect, state checked", async () => {
  const get = (url) =>
    new Promise((resolve, reject) => {
      http.get(url, (res) => {
        let body = "";
        res.on("data", (d) => (body += d));
        res.on("end", () => resolve({ status: res.statusCode, body }));
      }).on("error", reject);
    });
  const fakeFetch = async () => ({ ok: true, json: async () => ({ id: 7, slug: "s", pem: "P", webhook_secret: "W", client_id: "C", client_secret: "S", html_url: "https://github.com/apps/s" }) });
  const result = await runManifestFlow({
    publicUrl: "https://helm.example.org",
    name: "CoderHelm test",
    org: "acme",
    log: () => {},
    fetchImpl: fakeFetch,
    open: async (local) => {
      const page = await get(local);
      assert.equal(page.status, 200);
      const state = page.body.match(/state=([0-9a-f]+)/)[1];
      assert.match(page.body, /organizations\/acme\/settings\/apps\/new/);
      assert.match(page.body, /webhooks\/github/);
      const wrong = await get(`${local}callback?code=abcdefgh&state=nope`);
      assert.equal(wrong.status, 400, "a callback with another state is refused");
      const ok = await get(`${local}callback?code=abcdefgh&state=${state}`);
      assert.equal(ok.status, 200);
    },
  });
  assert.equal(result.id, 7);
});

test("github-app stores the keys, then restarts the services on the new secret version", async () => {
  const dir = tmpdir();
  const file = writeConfig(dir);
  fs.mkdirSync(path.join(dir, ".coderhelm"));
  fs.writeFileSync(path.join(dir, ".coderhelm", "state.json"), JSON.stringify({ version: "source", publicUrl: "https://d9.cloudfront.net", installId: "x" }));
  let stored = null;
  const runner = new FakeRunner(awsAnswers({
    "get-secret-value": JSON.stringify(JSON.stringify(initialSecret())),
    "put-secret-value": (args) => {
      const f = args.find((a) => a.startsWith("file://")).slice(7);
      stored = JSON.parse(fs.readFileSync(f, "utf8"));
      return { VersionId: "v-123" };
    },
  }));
  const logs = [];
  const src = path.resolve(path.dirname(new URL(import.meta.url).pathname), "..", "..");
  await main(["github-app", "--config", file, "--source", src], {
    runner,
    log: (m) => logs.push(m),
    interactive: false,
    ask: quietAsk(),
    manifestFlow: async ({ publicUrl, name }) => {
      assert.equal(publicUrl, "https://d9.cloudfront.net");
      assert.equal(name, "CoderHelm prod 3333");
      return { id: 9, slug: "ch", pem: "PEM-SECRET", webhook_secret: "WS", client_id: "CI", client_secret: "CSEC", html_url: "https://github.com/apps/ch" };
    },
  });
  assert.equal(stored.github_app_id, "9");
  assert.equal(stored.github_private_key, "PEM-SECRET");
  assert.ok(stored.jwt_secret, "existing keys are kept");
  const redeploy = runner.commands.find((c) => c.args.includes("deploy"));
  assert.equal(redeploy.env.CODERHELM_SECRETS_REVISION, "v-123");
  const shown = logs.join("\n") + runner.lines().join("\n");
  assert.ok(!shown.includes("PEM-SECRET") && !shown.includes("CSEC"), "credentials are never printed");
  assert.match(logs.join("\n"), /github.com\/apps\/ch\/installations\/new/);
});

// ── upgrade / rollback ──────────────────────────────────────────────────

test("migration notes: only the releases crossed, in order", () => {
  const all = [
    { version: "v1.3.0", notes: "c", breaking: true },
    { version: "v1.1.0", notes: "a" },
    { version: "v1.2.0", notes: "b" },
    { version: "v2.0.0", notes: "d" },
  ];
  assert.deepEqual(cmd.migrationNotes(all, "v1.1.0", "v1.3.0").map((n) => n.version), ["v1.2.0", "v1.3.0"]);
  assert.deepEqual(cmd.migrationNotes(all, undefined, "v1.1.0").map((n) => n.version), ["v1.1.0"]);
  assert.equal(semverCompare("v1.10.0", "v1.9.0"), 1);
  assert.equal(semverCompare("1.2.0", "v1.2.0"), 0);
});

test("upgrade refuses to go backwards; rollback needs a recorded version", async () => {
  const dir = tmpdir();
  const file = writeConfig(dir);
  fs.mkdirSync(path.join(dir, ".coderhelm"));
  fs.writeFileSync(path.join(dir, ".coderhelm", "state.json"), JSON.stringify({ version: "v1.4.0" }));
  const deps = { runner: new FakeRunner(awsAnswers()), log: () => {}, interactive: false, ask: quietAsk() };
  await assert.rejects(() => main(["upgrade", "--config", file, "--to", "v1.3.0"], deps), /older than v1.4.0/);
  await assert.rejects(() => main(["rollback", "--config", file], deps), /No earlier version/);
  const same = await run(["upgrade", "--config", file, "--to", "v1.4.0"], { runner: deps.runner });
  assert.match(same.out, /Already on v1.4.0/);
});

test("update --dry-run: health check, deploy with live progress, health check; no bootstrap", async () => {
  const dir = tmpdir();
  const file = writeConfig(dir);
  const runner = new Runner({ dryRun: true, log: () => {} });
  await run(["update", "--dry-run", "--config", file, "--to", "v2.0.0"], { runner });
  const lines = runner.commands.map((c) => [c.cmd, ...c.args].join(" "));
  const firstHealth = lines.findIndex((l) => l.includes("cloudformation list-stacks"));
  const dep = lines.findIndex((l) => l.includes("cdk deploy --all") && l.includes("--progress events"));
  const lastHealth = lines.map((l, i) => (l.includes("cloudformation list-stacks") ? i : -1)).filter((i) => i >= 0).pop();
  assert.ok(firstHealth >= 0 && dep > firstHealth && lastHealth > dep, lines.join("\n"));
  assert.ok(!lines.some((l) => l.includes("cdk bootstrap")), "updates never bootstrap");
});

/** A fake AWS where health passes `healthyCalls` times, then functions report a failed update. */
function healthyThen(healthyCalls) {
  let n = 0;
  const stacks = ["database", "storage", "api", "worker", "streaming", "web", "email", "monitoring"].map((s) => ({ StackName: `coderhelm-prod-${s}`, StackStatus: "UPDATE_COMPLETE" }));
  return new FakeRunner(awsAnswers({
    "list-stacks": { StackSummaries: stacks },
    "get-function-configuration": () => (n++ < healthyCalls ? { State: "Active", LastUpdateStatus: "Successful" } : { State: "Active", LastUpdateStatus: "Failed" }),
    "get-queue-url": { QueueUrl: "https://q" },
    "get-queue-attributes": { Attributes: { ApproximateNumberOfMessages: "0" } },
    "get-secret-value": JSON.stringify(JSON.stringify({ ...initialSecret(), github_app_id: "1", github_private_key: "k" })),
    "sesv2 get-account": { ProductionAccessEnabled: true },
  }));
}

function stateDir(version) {
  const dir = tmpdir();
  const file = writeConfig(dir);
  fs.mkdirSync(path.join(dir, ".coderhelm"));
  fs.writeFileSync(path.join(dir, ".coderhelm", "state.json"), JSON.stringify({ version, installId: "i" }));
  return { dir, file };
}

const fakeSource = (calls) => async (_ctx, version) => {
  calls.push(version);
  return { version, infra: path.join(tmpdir(), "infra"), dashboard: null, env: {} };
};

test("update: an unhealthy new release is rolled back automatically, and the result says so", async () => {
  const { dir, file } = stateDir("v1.0.0");
  const runner = healthyThen(3); // the 3 function checks before the update pass, after fail
  const sources = [];
  const result = path.join(dir, "result.json");
  const { code } = await run(["update", "--config", file, "--to", "v1.1.0", "--yes", "--auto-rollback", "--result-file", result],
    { runner, resolveSource: fakeSource(sources) });
  assert.equal(code, 1);
  assert.deepEqual(sources, ["v1.1.0", "v1.0.0"], "the previous release is deployed again");
  const deploys = runner.commands.filter((c) => c.args.includes("deploy") && c.args.includes("--all"));
  assert.equal(deploys.length, 2);
  const r = JSON.parse(fs.readFileSync(result, "utf8"));
  assert.equal(r.status, "rolled_back");
  assert.match(r.reason, /health check failed after deploying v1.1.0; back on v1.0.0/);
});

test("update: a healthy release succeeds; an unhealthy install isn't touched", async () => {
  const { dir, file } = stateDir("v1.0.0");
  const result = path.join(dir, "r.json");
  const ok = await run(["update", "--config", file, "--to", "v1.1.0", "--yes", "--result-file", result], { runner: healthyThen(99), resolveSource: fakeSource([]) });
  assert.equal(ok.code, 0);
  assert.equal(JSON.parse(fs.readFileSync(result, "utf8")).status, "succeeded");

  const sick = stateDir("v1.0.0");
  const runner = healthyThen(0);
  await assert.rejects(
    () => main(["update", "--config", sick.file, "--to", "v1.1.0", "--yes", "--result-file", path.join(sick.dir, "r.json")],
      { runner, resolveSource: fakeSource([]), log: () => {}, interactive: false, ask: quietAsk() }),
    /isn't healthy before the update/,
  );
  assert.ok(!runner.commands.some((c) => c.args.includes("deploy")), "nothing deployed");
  assert.equal(JSON.parse(fs.readFileSync(path.join(sick.dir, "r.json"), "utf8")).status, "failed");
});

test("signature: a bad signature refuses the release; --require-signature refuses an unverifiable one", async () => {
  const dir = tmpdir();
  fs.writeFileSync(path.join(dir, "SHA256SUMS"), "x");
  fs.writeFileSync(path.join(dir, "SHA256SUMS.sigstore.json"), "{}");
  const paths = { dir };
  const bad = new FakeRunner({ "cosign version": "v2.4.1", "cosign verify-blob": { code: 1, stderr: "none of the expected identities matched" } });
  await assert.rejects(() => verifySignature(paths, bad, { log: () => {} }), /signature does not verify/);
  const good = new FakeRunner({ "cosign version": "v2.4.1", "cosign verify-blob": "Verified OK" });
  assert.equal(await verifySignature(paths, good, { log: () => {} }), "cosign");
  const verify = good.commands.find((c) => c.args[0] === "verify-blob");
  assert.ok(verify.args.join(" ").includes("release.yml@refs/tags/v"), "only this repository's release workflow may sign");
  const none = new FakeRunner({ "cosign version": { code: 127 }, "gh --version": { code: 127 } });
  await assert.rejects(() => verifySignature(paths, none, { require: true, log: () => {} }), /--require-signature/);
  assert.equal(await verifySignature(paths, none, { log: () => {} }), null);
  const ghBad = new FakeRunner({ "cosign version": { code: 127 }, "gh --version": "gh 2", "gh attestation verify": { code: 1 } });
  fs.rmSync(path.join(dir, "SHA256SUMS.sigstore.json"));
  await assert.rejects(() => verifySignature(paths, ghBad, { log: () => {} }), /no valid build attestation/);
});

test("auto-update on with a window: saved in the config and sent to the running updater", async () => {
  const { file } = stateDir("v1.0.0");
  const runner = new FakeRunner({ "lambda invoke": "{}" });
  await run(["auto-update", "on", "--config", file, "--window", "Sat 22:00", "--timezone", "Europe/Berlin", "--channel", "stable", "--majors", "ask"], { runner });
  const cfg = JSON.parse(fs.readFileSync(file, "utf8"));
  assert.deepEqual(cfg.updates, { auto: true, window: "Sat 22:00", channel: "stable", majors: "ask", timezone: "Europe/Berlin" });
  const inv = runner.commands.find((c) => c.args.includes("invoke"));
  assert.ok(inv.args.includes("coderhelm-prod-updater"));
  const payload = JSON.parse(inv.args[inv.args.indexOf("--payload") + 1]);
  assert.deepEqual(payload.settings, { auto: true, channel: "stable", day: "sat", hour: 22, timezone: "Europe/Berlin", majors: "ask" });
  await assert.rejects(() => main(["auto-update", "on", "--config", file, "--window", "soon"], { runner, log: () => {}, interactive: false, ask: quietAsk() }), /--window/);
  await run(["auto-update", "off", "--config", file], { runner });
  assert.equal(JSON.parse(fs.readFileSync(file, "utf8")).updates.auto, false);
  assert.deepEqual(cmd.parseWindow("daily 02:00"), { day: "daily", hour: 2 });
});

test("version shows the installer, the installed and the latest release", async () => {
  const { file } = stateDir("v1.0.0");
  const fetchImpl = async () => ({ ok: true, json: async () => ({ tag_name: "v1.2.0" }) });
  const { out } = await run(["version", "--config", file], { fetchImpl, runner: new FakeRunner() });
  assert.match(out, /Installed\s+v1\.0\.0/);
  assert.match(out, /Latest\s+v1\.2\.0/);
  assert.match(out, /update --to v1\.2\.0/);
});

// ── health / backup / uninstall ─────────────────────────────────────────

test("health reports each check and fails on a broken one", async () => {
  const dir = tmpdir();
  const file = writeConfig(dir);
  const stacks = ["database", "storage", "api", "worker", "streaming", "web", "email", "monitoring"].map((s) => ({ StackName: `coderhelm-prod-${s}`, StackStatus: "UPDATE_COMPLETE" }));
  const ok = new FakeRunner(awsAnswers({
    "list-stacks": { StackSummaries: stacks },
    "get-function-configuration": { State: "Active", LastUpdateStatus: "Successful" },
    "get-queue-url": { QueueUrl: "https://q" },
    "get-queue-attributes": { Attributes: { ApproximateNumberOfMessages: "0" } },
    "get-secret-value": JSON.stringify(JSON.stringify({ ...initialSecret(), github_app_id: "1", github_private_key: "k" })),
    "sesv2 get-account": { ProductionAccessEnabled: true },
  }));
  const good = await run(["health", "--config", file], { runner: ok });
  assert.equal(good.code, 0, good.out);
  assert.match(good.out, /All checks passed/);

  const broken = new FakeRunner(awsAnswers({
    "list-stacks": { StackSummaries: [...stacks.slice(1), { StackName: "coderhelm-prod-database", StackStatus: "UPDATE_ROLLBACK_FAILED" }] },
    "get-function-configuration": { State: "Active", LastUpdateStatus: "Successful" },
    "get-queue-url": { QueueUrl: "https://q" },
    "get-queue-attributes": { Attributes: { ApproximateNumberOfMessages: "3" } },
    "get-secret-value": JSON.stringify(JSON.stringify(initialSecret())),
    "sesv2 get-account": { ProductionAccessEnabled: false },
  }));
  const bad = await run(["health", "--config", file], { runner: broken });
  assert.equal(bad.code, 1);
  assert.match(bad.out, /FAIL\s+Stacks.*UPDATE_ROLLBACK_FAILED/);
  assert.match(bad.out, /FAIL\s+Dead-letter queue\s+3 failed/);
  assert.match(bad.out, /FAIL\s+GitHub App keys/);
  assert.match(bad.out, /SES sandbox/);
});

test("backup covers this install's tables only", async () => {
  const dir = tmpdir();
  const file = writeConfig(dir);
  const runner = new FakeRunner(awsAnswers({
    "list-tables": { TableNames: ["coderhelm-prod", "coderhelm-prod-runs", "coderhelm-prod-settings", "coderhelm-dev-runs", "other"] },
  }));
  await run(["backup", "--config", file], { runner });
  const backed = runner.commands.filter((c) => c.args.includes("create-backup")).map((c) => c.args[c.args.indexOf("--table-name") + 1]);
  assert.deepEqual(backed, ["coderhelm-prod", "coderhelm-prod-runs", "coderhelm-prod-settings"]);
});

test("uninstall needs --yes when not interactive, and destroys with cdk", async () => {
  const dir = tmpdir();
  const file = writeConfig(dir);
  await assert.rejects(() => main(["uninstall", "--config", file], { runner: new FakeRunner(awsAnswers()), log: () => {}, interactive: false, ask: quietAsk() }), /--yes/);
  const runner = new Runner({ dryRun: true, log: () => {} });
  const src = path.resolve(path.dirname(new URL(import.meta.url).pathname), "..", "..");
  const { out } = await run(["uninstall", "--dry-run", "--yes", "--config", file, "--source", src], { runner });
  assert.ok(runner.commands.some((c) => c.args.join(" ").includes("cdk destroy --all --force")));
  assert.match(out, /Kept on purpose/);
});

// ── telemetry ───────────────────────────────────────────────────────────

test("telemetry: off by default, on/off edits the config, pings carry no identity", async () => {
  const dir = tmpdir();
  const file = writeConfig(dir, { telemetry: undefined });
  const status = await run(["telemetry", "status", "--config", file]);
  assert.match(status.out, /off/);
  await run(["telemetry", "on", "--config", file]);
  assert.equal(JSON.parse(fs.readFileSync(file, "utf8")).telemetry.enabled, true);
  await run(["telemetry", "off", "--config", file]);
  assert.equal(JSON.parse(fs.readFileSync(file, "utf8")).telemetry.enabled, false);

  const body = cmd.telemetryPayload({ region: "eu-west-1", models: { provider: "bedrock" }, account: ACCOUNT, email: { from: "a@b.c" } }, { installId: "id-1", version: "v1.0.0" }, "install");
  assert.deepEqual(Object.keys(body).sort(), ["event", "install_id", "layout", "provider", "region", "version"]);
  assert.ok(!JSON.stringify(body).includes(ACCOUNT));

  let sent = 0;
  const ctx = { runner: { dryRun: false }, log: () => {}, fetchImpl: async () => { sent++; return { ok: true }; } };
  assert.equal(await cmd.telemetryPing(ctx, { telemetry: { enabled: false } }, { installId: "x" }, "install"), false);
  assert.equal(await cmd.telemetryPing(ctx, {}, { installId: "x" }, "install"), false);
  assert.equal(sent, 0, "nothing is sent unless telemetry is on");
  await cmd.telemetryPing(ctx, { telemetry: { enabled: true }, region: "us-east-1" }, { installId: "x" }, "health");
  assert.equal(sent, 1);
  const failing = { ...ctx, fetchImpl: async () => { throw new Error("offline"); } };
  assert.equal(await cmd.telemetryPing(failing, { telemetry: { enabled: true } }, { installId: "x" }, "health"), true, "a failed ping never fails a command");
});

// ── releases and secrets ────────────────────────────────────────────────

test("release checksums are verified", () => {
  const dir = tmpdir();
  fs.writeFileSync(path.join(dir, "gateway.zip"), "abc");
  const sum = sha256File(path.join(dir, "gateway.zip"));
  const sums = parseSums(`${sum}  gateway.zip\n${"0".repeat(64)} *worker.zip\n`);
  assert.equal(sums["gateway.zip"], sum);
  assert.equal(sums["worker.zip"], "0".repeat(64));
  verifySums(dir, sums, ["gateway.zip"]);
  fs.writeFileSync(path.join(dir, "gateway.zip"), "tampered");
  assert.throws(() => verifySums(dir, sums, ["gateway.zip"]), /does not match its checksum/);
  assert.throws(() => verifySums(dir, {}, ["gateway.zip"]), /no entry/);
});

test("names derived like the CDK app; printed commands hide secrets", () => {
  assert.deepEqual(derived({ stage: "prod", account: ACCOUNT }), {
    prefix: "coderhelm-prod", secretName: "coderhelm/prod/secrets", publicUrlParameter: "/coderhelm-prod/public-url", suffix: `-${ACCOUNT}`,
  });
  assert.equal(derived({ stage: "x", account: ACCOUNT, resourcePrefix: "acme-helm" }).secretName, "acme-helm/secrets");
  assert.equal(redactArg("--secret-string={...}"), "--secret-string <hidden>");
  assert.equal(redactArg("file:///tmp/coderhelm-secret-1/secret.json"), "file://<hidden>");
  const s = initialSecret();
  assert.equal(s.jwt_secret.length, 64);
  assert.notEqual(initialSecret().jwt_secret, s.jwt_secret);
});

test("pull writes the config and state kept in the account", async () => {
  const dir = tmpdir();
  const file = path.join(dir, "coderhelm.config.json");
  const cfg = { account: ACCOUNT, region: "us-west-2", stage: "prod", email: { from: "a@b.co" }, admins: ["a@b.co"] };
  const runner = new FakeRunner({
    "/coderhelm-prod/installer/config": JSON.stringify(JSON.stringify(cfg)),
    "/coderhelm-prod/installer/state": JSON.stringify(JSON.stringify({ version: "v1.0.0", publicUrl: "https://x" })),
  });
  await run(["pull", "--stage", "prod", "--region", "us-west-2", "--config", file], { runner });
  assert.deepEqual(JSON.parse(fs.readFileSync(file, "utf8")), cfg);
  assert.equal(JSON.parse(fs.readFileSync(path.join(dir, ".coderhelm", "state.json"), "utf8")).version, "v1.0.0");
  await assert.rejects(() => main(["pull"], { log: () => {}, interactive: false, ask: quietAsk() }), /--stage and --region/);
});

test("an app made by hand: flags and environment, never argv secrets", () => {
  const dir = tmpdir();
  const pem = path.join(dir, "app.pem");
  fs.writeFileSync(pem, "-----BEGIN RSA PRIVATE KEY-----\nX\n-----END RSA PRIVATE KEY-----\n");
  const keys = cmd.manualAppKeys({ appId: "123", clientId: "Iv1.a", privateKeyFile: pem }, { GITHUB_WEBHOOK_SECRET: "w", GITHUB_CLIENT_SECRET: "c" });
  assert.equal(keys.github_app_id, "123");
  assert.match(keys.github_private_key, /PRIVATE KEY/);
  assert.throws(() => cmd.manualAppKeys({ appId: "x" }, {}), /--app-id[\s\S]*GITHUB_CLIENT_SECRET/);
});
