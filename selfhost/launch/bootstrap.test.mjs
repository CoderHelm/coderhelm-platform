// The Launch Stack installer's decisions, without AWS:
//   node --test selfhost/launch/*.test.mjs
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  bedrockPrefixFor, configFromEnv, createProgress, explainFailure, logGroup, logUrl, redact, run, secretName,
  statusDoc, withSetupToken,
} from "./bootstrap.mjs";

const ENV = {
  AWS_REGION: "eu-west-1",
  STAGE: "prod",
  VERSION: "v0.7.0",
  ADMIN_EMAIL: "Ada@Example.com",
  EMAIL_FROM: "",
  MODEL_PROVIDER: "bedrock",
  BEDROCK_PREFIX: "auto",
  TELEMETRY: "off",
  STATUS_BUCKET: "coderhelm-setup-abc",
  SETUP_TOKEN: "a".repeat(40),
};

test("config: no DNS by default, the admin's own address sends email", () => {
  const cfg = configFromEnv(ENV, { account: "123456789012" });
  assert.equal(cfg.domain, null);
  assert.deepEqual(cfg.email, { from: "ada@example.com", identity: "ada@example.com" });
  assert.deepEqual(cfg.admins, ["ada@example.com"]);
  assert.deepEqual(cfg.models, { provider: "bedrock", bedrockInferencePrefix: "eu." });
  assert.equal(cfg.telemetry.enabled, false);
  assert.equal(cfg.signup, undefined);
});

test("config: a domain in a hosted zone gets a domain identity", () => {
  const cfg = configFromEnv({ ...ENV, HOSTNAME_ARG: "ch.example.com", HOSTED_ZONE: "example.com.", EMAIL_FROM: "bot@example.com", SIGNUP_DOMAIN: "Example.com" }, { account: "123456789012" });
  assert.deepEqual(cfg.domain, { hostname: "ch.example.com", hostedZone: "example.com" });
  assert.equal(cfg.email.identity, "example.com");
  assert.deepEqual(cfg.signup, { domains: ["example.com"] });
});

test("config: anthropic has no Bedrock prefix; explicit prefixes are kept", () => {
  assert.deepEqual(configFromEnv({ ...ENV, MODEL_PROVIDER: "anthropic" }, { account: "123456789012" }).models, { provider: "anthropic" });
  assert.equal(configFromEnv({ ...ENV, BEDROCK_PREFIX: "" }, { account: "123456789012" }).models.bedrockInferencePrefix, "");
});

test("config: plain sentences for settings that can't work", () => {
  assert.throws(() => configFromEnv({ ...ENV, ADMIN_EMAIL: "nope" }, { account: "123456789012" }), /admin email/);
  assert.throws(() => configFromEnv({ ...ENV, HOSTNAME_ARG: "ch.other.com", HOSTED_ZONE: "example.com" }, { account: "123456789012" }), /not inside the hosted zone/);
  assert.throws(() => configFromEnv({ ...ENV, HOSTNAME_ARG: "ch.example.com" }, { account: "123456789012" }), /hosted zone/);
  assert.throws(() => configFromEnv(ENV, { account: "x" }), /account number/);
});

test("bedrock prefix by region", () => {
  assert.equal(bedrockPrefixFor("us-east-1"), "us.");
  assert.equal(bedrockPrefixFor("ca-central-1"), "us.");
  assert.equal(bedrockPrefixFor("eu-central-1"), "eu.");
  assert.equal(bedrockPrefixFor("ap-southeast-2"), "apac.");
  assert.equal(bedrockPrefixFor("sa-east-1"), "global.");
});

test("the public progress file carries no account numbers, ARNs or emails", () => {
  const s = redact("User arn:aws:iam::123456789012:role/x in 123456789012 by ada@example.com failed");
  assert.doesNotMatch(s, /123456789012|ada@/);
  const doc = statusDoc({ phase: "failed", message: "role arn:aws:iam::123456789012:role/x" }, new Date(0));
  assert.equal(doc.schema, 1);
  assert.equal(doc.updatedAt, "1970-01-01T00:00:00.000Z");
  assert.doesNotMatch(JSON.stringify(doc), /123456789012/);
  assert.throws(() => statusDoc({ phase: "nope" }));
});

test("progress follows cdk deploy output", () => {
  const p = createProgress();
  assert.equal(p.feed("coderhelm-prod-database: deploying... [1/10]"), "step");
  assert.equal(p.feed("\x1b[32m ✅  coderhelm-prod-database\x1b[0m"), "done");
  assert.equal(p.feed("coderhelm-prod-api: deploying... [3/10]"), "step");
  assert.equal(p.feed("12:01:02 | CREATE_FAILED | AWS::S3::Bucket | Data  coderhelm-prod-data already exists"), "error");
  assert.equal(p.feed("random line"), null);
  assert.equal(p.step, 3);
  assert.equal(p.steps, 10);
  assert.deepEqual(p.done, ["coderhelm-prod-database"]);
  assert.equal(p.errors.length, 1);
});

test("failures come with a plain sentence and a fix", () => {
  assert.match(explainFailure(["CREATE_FAILED bucket already exists"]).message, /same name/);
  assert.match(explainFailure(["is not authorized to perform: iam:CreateRole"]).fix, /administrator/);
  assert.match(explainFailure(["sha256sum: WARNING: 1 computed checksum did NOT match"]).message, /checksums/);
  assert.match(explainFailure(["weird thing"]).message, /weird thing/);
});

test("the setup key is stored once, never over an existing GitHub App", () => {
  const t = "b".repeat(32);
  assert.deepEqual(withSetupToken({ jwt_secret: "j" }, t), { jwt_secret: "j", setup_token: t });
  assert.equal(withSetupToken({ github_app_id: "1", github_private_key: "k" }, t), null);
  assert.equal(withSetupToken({ setup_token: t }, t), null);
  assert.equal(withSetupToken({}, "short"), null);
  assert.equal(withSetupToken({}, ""), null);
  assert.equal(secretName("prod"), "coderhelm/prod/secrets");
});

test("the log link points at the kept log group", () => {
  assert.equal(logGroup({ CODEBUILD_BUILD_ID: "coderhelm-prod-installer:1234-abcd" }), "/aws/codebuild/coderhelm-prod-installer");
  assert.equal(logGroup({ STAGE: "dev" }), "/aws/codebuild/coderhelm-dev-installer");
  assert.equal(logUrl("us-east-1", "/aws/codebuild/coderhelm-prod-installer"), "https://us-east-1.console.aws.amazon.com/cloudwatch/home?region=us-east-1#logsV2:log-groups/log-group/$252Faws$252Fcodebuild$252Fcoderhelm-prod-installer");
});

/** Fakes for every side effect; records what happened. */
function fakes(over = {}) {
  const calls = { status: [], config: null, secret: null };
  const deps = {
    account: async () => "123456789012",
    writeStatus: async (d) => void calls.status.push(d),
    installCli: async () => ({ code: 0 }),
    pull: async () => ({ code: 1 }),
    writeConfig: async (c) => void (calls.config = c),
    deploy: async (_v, onLine) => {
      for (const l of ["coderhelm-prod-database: deploying... [1/2]", " ✅  coderhelm-prod-database", "coderhelm-prod-web: deploying... [2/2]", " ✅  coderhelm-prod-web"]) onLine(l);
      return { code: 0 };
    },
    readState: () => ({ publicUrl: "https://d111.cloudfront.net" }),
    readSecret: async () => ({ jwt_secret: "j", github_app_id: "" }),
    putSecret: async (_n, v) => void (calls.secret = v) || true,
    ...over,
  };
  return { deps, calls };
}

test("run: a fresh install writes the config, deploys, stores the key, ends done with the URL", async () => {
  const { deps, calls } = fakes();
  const doc = await run({ env: ENV, deps, log: () => {} });
  assert.equal(doc.phase, "done");
  assert.equal(doc.url, "https://d111.cloudfront.net");
  assert.equal(calls.config.stage, "prod");
  assert.equal(calls.secret.setup_token, ENV.SETUP_TOKEN);
  assert.deepEqual([...new Set(calls.status.map((s) => s.phase))], ["preparing", "deploying", "finishing", "done"]);
  assert.ok(calls.status.some((s) => s.phase === "deploying" && s.step === 1 && s.steps === 2), "per-part progress");
  for (const s of calls.status) assert.doesNotMatch(JSON.stringify(s), /ada@|123456789012|aaaaaaaa/);
});

test("run: an existing install of the same name is upgraded, not re-configured", async () => {
  const { deps, calls } = fakes({ pull: async () => ({ code: 0 }) });
  const doc = await run({ env: ENV, deps, log: () => {} });
  assert.equal(doc.phase, "done");
  assert.equal(calls.config, null);
});

test("run: a failed deploy is a failed status with the fix", async () => {
  const { deps } = fakes({
    deploy: async (_v, onLine) => {
      onLine("coderhelm-prod-database: deploying... [1/9]");
      onLine("CREATE_FAILED | AWS::S3::Bucket | coderhelm-prod-data already exists");
      return { code: 1 };
    },
  });
  const doc = await run({ env: ENV, deps, log: () => {} });
  assert.equal(doc.phase, "failed");
  assert.match(doc.message, /same name/);
  assert.match(doc.fix, /different install name/);
  assert.equal(doc.step, 1);
});

test("run: bad settings fail before deploying", async () => {
  let deployed = false;
  const { deps } = fakes({ deploy: async () => ((deployed = true), { code: 0 }) });
  const doc = await run({ env: { ...ENV, ADMIN_EMAIL: "x" }, deps, log: () => {} });
  assert.equal(doc.phase, "failed");
  assert.equal(deployed, false);
});

test("run: a progress-file write failure never stops the install", async () => {
  const { deps } = fakes({ writeStatus: async () => { throw new Error("denied"); } });
  const doc = await run({ env: ENV, deps, log: () => {} });
  assert.equal(doc.phase, "done");
});

test("run: no setup key, no secret write", async () => {
  const { deps, calls } = fakes();
  await run({ env: { ...ENV, SETUP_TOKEN: "" }, deps, log: () => {} });
  assert.equal(calls.secret, null);
});
