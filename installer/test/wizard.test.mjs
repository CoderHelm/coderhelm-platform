// The guided install with scripted answers: the happy path, going back,
// picking up where it stopped, answers it refuses, and no terminal at all.
//
//   cd installer && npm test
import { test } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { main } from "../lib/cli.mjs";
import { bedrockPrefixFor, checkAnthropicKey, configFromAnswers, estimateMonthlyCost, progress, STEPS, wizardFile } from "../lib/wizard.mjs";
import { FakeRunner } from "./fake.mjs";

const ACCOUNT = "111122223333";

function aws(extra = {}) {
  return new FakeRunner({
    "aws --version": "aws-cli/2.17.0",
    "sts get-caller-identity": { Account: ACCOUNT, Arn: `arn:aws:iam::${ACCOUNT}:user/me` },
    "tar --version": "bsdtar",
    "configure get region": "eu-west-1",
    "route53 list-hosted-zones": { HostedZones: [{ Name: "example.org.", Config: { PrivateZone: false } }, { Name: "internal.", Config: { PrivateZone: true } }] },
    "sesv2 get-email-identity": { code: 254 },
    "sesv2 get-account": { ProductionAccessEnabled: false },
    "get-foundation-model-availability": { authorizationStatus: "AUTHORIZED", agreementAvailability: { status: "AVAILABLE" } },
    ...extra,
  });
}

/**
 * Answers in order: each entry is [part of the question, answer]. A question
 * that doesn't match the next entry fails the test, so the flow is checked
 * too. Questions after the script runs out get their suggested answer.
 */
function script(entries) {
  const asked = [];
  const queue = [...entries];
  const next = (q, def) => {
    asked.push(q);
    if (!queue.length) return def;
    const [part, answer] = queue[0];
    if (!q.includes(part)) throw new Error(`expected a question about "${part}", got "${q}"`);
    queue.shift();
    return answer;
  };
  return { asked, left: queue, ask: async (q, def) => next(q, def), confirm: async (q, def) => next(q, def), close() {} };
}

function tmp() {
  return fs.mkdtempSync(path.join(os.tmpdir(), "chwiz-"));
}

async function run(argv, deps) {
  const logs = [];
  const code = await main(argv, { log: (m) => logs.push(String(m)), ...deps });
  return { code, out: logs.join("\n") };
}

test("happy path: every step, a domain from Route 53, Bedrock, auto-update, a cost summary, saved", async () => {
  const dir = tmp();
  const file = path.join(dir, "coderhelm.config.json");
  const s = script([
    ["AWS account", ACCOUNT],
    ["AWS region", "eu-west-1"],
    ["short name", "prod"],
    ["Hostname", "helm.example.org"],
    ["anthropic or bedrock", "bedrock"],
    ["prefix", "eu."],
    ["Your email", "owner@example.org"],
    ["Emails come from", "helm@example.org"],
    ["email domain", "example.org"],
    ["Slack or Teams", ""],
    ["Sign in with Google", "no"],
    ["automatically", "yes"],
    ["Maintenance window", "Sat 22:00"],
    ["Time zone", "Europe/Berlin"],
    ["Major releases", "ask"],
    ["deploy now", "no"],
  ]);
  const { out } = await run(["init", "--config", file], { runner: aws(), ask: s, interactive: true });
  assert.deepEqual(s.left, [], "every scripted question was asked");
  const cfg = JSON.parse(fs.readFileSync(file, "utf8"));
  assert.deepEqual(cfg.domain, { hostname: "helm.example.org", hostedZone: "example.org" });
  assert.deepEqual(cfg.models, { provider: "bedrock", bedrockInferencePrefix: "eu." });
  assert.deepEqual(cfg.updates, { auto: true, window: "Sat 22:00", timezone: "Europe/Berlin", majors: "ask" });
  assert.equal(cfg.telemetry.enabled, false, "telemetry stays off unless asked for");
  for (let i = 0; i < STEPS.length; i++) assert.ok(out.includes(`Step ${i + 1} of ${STEPS.length}`), `progress for step ${i + 1}`);
  assert.match(out, /Route 53 zones in this account: example\.org/);
  assert.doesNotMatch(out, /internal/, "private zones are not offered");
  assert.match(out, /SES sandbox/);
  assert.match(out, /Estimated AWS cost per month/);
  assert.match(out, /Deploy when you are ready/);
});

test("back returns to the previous step and keeps going", async () => {
  const dir = tmp();
  const file = path.join(dir, "coderhelm.config.json");
  const s = script([
    ["AWS account", ACCOUNT],
    ["AWS region", "us-east-1"],
    ["short name", "prod"],
    ["Hostname", "back"], // back to "Where to install"
    ["AWS account", ACCOUNT],
    ["AWS region", "us-west-2"],
    ["short name", "team"],
    ["Hostname", ""],
    ["anthropic or bedrock", "anthropic"],
    ["Anthropic API key", ""],
    ["Your email", "owner@example.org"],
  ]);
  await run(["init", "--config", file, "--deploy", "no"], { runner: aws(), ask: s, interactive: true });
  const cfg = JSON.parse(fs.readFileSync(file, "utf8"));
  assert.equal(cfg.region, "us-west-2");
  assert.equal(cfg.stage, "team");
  assert.equal(cfg.domain, null);
});

test("answers it refuses are asked again, with the reason", async () => {
  const dir = tmp();
  const file = path.join(dir, "coderhelm.config.json");
  const s = script([
    ["AWS account", "12345"],
    ["AWS account", ACCOUNT],
    ["AWS region", "Europe"],
    ["AWS region", "eu-west-1"],
    ["short name", "Prod!"],
    ["short name", "prod"],
    ["Hostname", "Not A Host"],
    ["Hostname", ""],
    ["anthropic or bedrock", "openai"],
    ["anthropic or bedrock", "anthropic"],
    ["Anthropic API key", "sk-bad"],
    ["anthropic or bedrock", "anthropic"],
    ["Anthropic API key", ""],
    ["Your email", "nope"],
    ["Your email", "owner@example.org"],
  ]);
  const fetchImpl = async () => ({ status: 401 });
  const { out } = await run(["init", "--config", file, "--deploy", "no"], { runner: aws(), ask: s, interactive: true, fetchImpl });
  assert.deepEqual(s.left, []);
  for (const reason of ["A 12-digit account id.", "A region such as us-east-1.", "Lowercase letters, digits and hyphens", "A lowercase hostname", "anthropic or bedrock", "Anthropic refused that key.", "An email address."]) {
    assert.ok(out.includes(reason), reason);
  }
});

test("resume: a stopped run picks up at the first unfinished step", async () => {
  const dir = tmp();
  const file = path.join(dir, "coderhelm.config.json");
  const first = script([
    ["AWS account", ACCOUNT],
    ["AWS region", "us-east-1"],
    ["short name", "prod"],
    ["Hostname", ""],
  ]);
  // Stops at the model question (the script ends with an error there).
  first.ask = (orig => async (q, d) => {
    if (q.includes("anthropic or bedrock")) throw new Error("interrupted");
    return orig(q, d);
  })(first.ask);
  await assert.rejects(() => main(["init", "--config", file], { runner: aws(), ask: first, interactive: true, log: () => {} }), /interrupted/);
  const saved = JSON.parse(fs.readFileSync(wizardFile({ config: file }), "utf8"));
  assert.deepEqual(saved.done, ["prereqs", "account", "domain"]);
  assert.ok(!fs.existsSync(file), "no config until the end");

  const second = script([
    ["anthropic or bedrock", "anthropic"],
    ["Anthropic API key", ""],
    ["Your email", "owner@example.org"],
  ]);
  const { out } = await run(["init", "--config", file, "--deploy", "no"], { runner: aws(), ask: second, interactive: true });
  assert.match(out, /Picking up at step 4, "Claude access"/);
  assert.ok(!second.asked.some((q) => q.includes("AWS region")), "finished steps are not asked again");
  assert.equal(JSON.parse(fs.readFileSync(file, "utf8")).region, "us-east-1");
  // Finished: running it again doesn't overwrite the config.
  await assert.rejects(() => main(["init", "--config", file], { runner: aws(), log: () => {}, interactive: false }), /already exists/);
});

test("no terminal: flags answer everything; a bad flag stops with the reason", async () => {
  const dir = tmp();
  const file = path.join(dir, "coderhelm.config.json");
  await run(["init", "--yes", "--config", file, "--region", "us-east-1", "--admin", "owner@example.org", "--provider", "anthropic",
    "--auto-update", "on", "--window", "daily 02:00", "--timezone", "UTC", "--majors", "auto"], { runner: aws(), interactive: false });
  const cfg = JSON.parse(fs.readFileSync(file, "utf8"));
  assert.equal(cfg.domain, null, "no domain is picked without a terminal unless given");
  assert.deepEqual(cfg.updates, { auto: true, window: "daily 02:00", timezone: "UTC", majors: "auto" });

  const dir2 = tmp();
  await assert.rejects(
    () => main(["init", "--yes", "--config", path.join(dir2, "c.json"), "--region", "mars-1", "--admin", "owner@example.org"], { runner: aws(), log: () => {}, interactive: false }),
    /AWS region: A region such as us-east-1/,
  );
  // --answers <file> works the same way.
  const answers = path.join(dir2, "answers.json");
  fs.writeFileSync(answers, JSON.stringify({ region: "us-west-2", admin: "a@example.org", provider: "bedrock", bedrockPrefix: "us." }));
  await run(["init", "--yes", "--config", path.join(dir2, "c2.json"), "--answers", answers], { runner: aws(), interactive: false });
  assert.equal(JSON.parse(fs.readFileSync(path.join(dir2, "c2.json"), "utf8")).models.provider, "bedrock");
});

test("not signed in: offers the profiles and an SSO sign-in", async () => {
  const dir = tmp();
  const file = path.join(dir, "coderhelm.config.json");
  let signedIn = false;
  const runner = aws({
    "sts get-caller-identity": () => (signedIn ? { Account: ACCOUNT, Arn: "arn" } : { code: 255 }),
    "configure list-profiles": "dev\nprod-admin\n",
    "configure get sso_session": "my-sso",
    "sso login": () => {
      signedIn = true;
      return "";
    },
  });
  const s = script([
    ["AWS profile", "prod-admin"],
    ["aws sso login", "yes"],
    ["AWS account", ACCOUNT],
  ]);
  const { out } = await run(["init", "--config", file, "--deploy", "no", "--region", "us-east-1", "--stage", "prod", "--domain", "", "--provider", "bedrock", "--bedrock-prefix", "us.", "--admin", "a@example.org"],
    { runner, ask: s, interactive: true });
  assert.match(out, /Profiles on this machine: dev, prod-admin/);
  assert.ok(runner.commands.some((c) => c.args.join(" ") === "sso login --profile prod-admin"));
  assert.equal(process.env.AWS_PROFILE, "prod-admin");
  delete process.env.AWS_PROFILE;
});

test("Bedrock access is checked per model; without a terminal a missing model stops the install with the fix", async () => {
  const dir = tmp();
  const runner = aws({ "get-foundation-model-availability": { authorizationStatus: "NOT_AUTHORIZED" } });
  const err = await main(["init", "--yes", "--config", path.join(dir, "c.json"), "--region", "eu-west-1", "--admin", "a@example.org", "--provider", "bedrock"], { runner, interactive: false, log: () => {} }).catch((e) => e);
  assert.ok(err instanceof Error);
  assert.match(err.message, /FAIL\s+Claude models enabled in Bedrock/);
  assert.match(err.message, /Claude Opus 5\.5 \(anthropic\.claude-opus-5-5\)/);
  assert.match(err.message, /bedrock\/home\?region=eu-west-1#\/modelaccess/);
  assert.doesNotMatch(err.message, /\n\s+at /, "no stack trace in the message");
  // --skip-checks lets it through (the doctor still reports it later).
  const ok = await run(["init", "--yes", "--config", path.join(dir, "d.json"), "--region", "eu-west-1", "--admin", "a@example.org", "--provider", "bedrock", "--skip-checks", "bedrock"], { runner, interactive: false });
  assert.equal(ok.code, 0);
});

test("helpers: progress bar, prefixes, key check, cost, config", async () => {
  assert.equal(progress(0, 8, "X"), "\nStep 1 of 8 [██░░░░░░░░░░░░░░] X");
  assert.equal(bedrockPrefixFor("eu-central-1"), "eu.");
  assert.equal(bedrockPrefixFor("ap-southeast-2"), "apac.");
  assert.equal(bedrockPrefixFor("ca-central-1"), "");
  assert.equal(await checkAnthropicKey({ fetchImpl: async () => ({ status: 200 }) }, "k"), true);
  assert.equal(await checkAnthropicKey({ fetchImpl: async () => ({ status: 401 }) }, "k"), false);
  assert.equal(await checkAnthropicKey({ fetchImpl: async () => { throw new Error("offline"); } }, "k"), null);
  const c = estimateMonthlyCost({ provider: "bedrock" });
  assert.ok(c.low > 0 && c.high > c.low);
  assert.match(c.model, /Bedrock/);
  const cfg = configFromAnswers({ account: ACCOUNT, region: "us-east-1", stage: "prod", provider: "anthropic", admin: "a@b.co", emailFrom: "x@b.co", auto: false, updatesWebhook: "https://hooks.example.org/1" });
  assert.deepEqual(cfg.updates, { auto: false, webhookUrl: "https://hooks.example.org/1" });
  assert.equal(cfg.domain, null);
});
