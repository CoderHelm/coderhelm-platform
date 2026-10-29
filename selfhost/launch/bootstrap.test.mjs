// The Launch Stack installer's decisions, without AWS:
//   node --test selfhost/launch/*.test.mjs
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  bedrockPrefixFor, configFromEnv, createProgress, domainPlan, explainFailure, findZone, isDelegated, logGroup, logUrl,
  publicDomain, redact, refusal, relaunchConfig, run, secretName, statusDoc, updatesFromEnv, withSetupToken,
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
  // Two-factor sign-in required from the start.
  const tf = configFromEnv({ ...ENV, REQUIRE_TWO_FACTOR: "on" }, { account: "123456789012" });
  assert.deepEqual(tf.signup, { requireTwoFactor: true });
  assert.equal(configFromEnv({ ...ENV, REQUIRE_TWO_FACTOR: "off" }, { account: "123456789012" }).signup, undefined);
});

test("config: email through CoderHelm only when chosen, with no SES identity to verify", () => {
  assert.equal(configFromEnv(ENV, { account: "123456789012" }).email.transport, undefined);
  const cfg = configFromEnv({ ...ENV, EMAIL_TRANSPORT: "coderhelm" }, { account: "123456789012" });
  assert.deepEqual(cfg.email, { from: "ada@example.com", identity: null, transport: "coderhelm" });
  assert.equal(configFromEnv({ ...ENV, EMAIL_TRANSPORT: "smtp" }, { account: "123456789012" }).email.transport, undefined);
});

test("config: anthropic has no Bedrock prefix; explicit prefixes are kept", () => {
  assert.deepEqual(configFromEnv({ ...ENV, MODEL_PROVIDER: "anthropic" }, { account: "123456789012" }).models, { provider: "anthropic" });
  assert.equal(configFromEnv({ ...ENV, BEDROCK_PREFIX: "" }, { account: "123456789012" }).models.bedrockInferencePrefix, "");
});

test("config: plain sentences for settings that can't work", () => {
  assert.throws(() => configFromEnv({ ...ENV, ADMIN_EMAIL: "nope" }, { account: "123456789012" }), /admin email/);
  assert.throws(() => configFromEnv({ ...ENV, HOSTNAME_ARG: "ch.other.com", HOSTED_ZONE: "example.com" }, { account: "123456789012" }), /not inside the hosted zone/);
  assert.throws(() => configFromEnv({ ...ENV, HOSTNAME_ARG: "not a host" }, { account: "123456789012" }), /not a hostname/);
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
  assert.match(explainFailure(["is not authorized to perform: iam:CreateRole"]).message, /AWS refused iam:CreateRole/);
  assert.match(explainFailure(["is not authorized to perform: iam:CreateRole"]).fix, /Delete this stack/);
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

// ── Your own domain ───────────────────────────────────────────────────────

const ZONES = [
  { Id: "/hostedzone/ZROOT", Name: "example.com.", Config: { PrivateZone: false } },
  { Id: "/hostedzone/ZPRIV", Name: "ch.example.com.", Config: { PrivateZone: true } },
];

test("domain: the zone is found in the account; private zones and other domains never match", () => {
  assert.deepEqual(findZone(ZONES, "ch.example.com"), { name: "example.com", id: "ZROOT" });
  assert.equal(findZone(ZONES, "ch.example.net"), null);
  assert.equal(findZone(ZONES, "badexample.com"), null);
  assert.ok(isDelegated(["ns-1.awsdns-01.org.", "NS-2.awsdns-02.com"], ["ns-2.awsdns-02.com", "ns-1.awsdns-01.org"]));
  assert.ok(!isDelegated(["ns-1.awsdns-01.org"], ["dns1.registrar.example"]));
  assert.ok(!isDelegated([], []));
});

test("domain: a delegated Route 53 zone is set up by the deploy; anything else waits for its certificate", () => {
  const host = { ...ENV, HOSTNAME_ARG: "ch.example.com" };
  assert.deepEqual(domainPlan(ENV), { mode: "none" });
  assert.deepEqual(domainPlan(host, { name: "example.com", id: "ZROOT", delegated: true }), { mode: "route53", hostname: "ch.example.com", zone: "example.com", zoneId: "ZROOT" });
  assert.deepEqual(domainPlan(host, { name: "example.com", id: "ZROOT", delegated: false }), { mode: "external", hostname: "ch.example.com" });
  assert.deepEqual(domainPlan(host, null), { mode: "external", hostname: "ch.example.com" });
  // Route 53: the domain is in the config with the zone id (no lookup at synth).
  const r53 = configFromEnv(host, { account: "123456789012", found: { name: "example.com", id: "ZROOT", delegated: true } });
  assert.deepEqual(r53.domain, { hostname: "ch.example.com", hostedZone: "example.com", hostedZoneId: "ZROOT" });
  // Elsewhere: the AWS address first.
  assert.equal(configFromEnv(host, { account: "123456789012" }).domain, null);
});

test("domain: the progress file holds the records to add and nothing else about the account", () => {
  const d = publicDomain({ hostname: "CH.example.com.", mode: "external", state: "validating", certificateArn: "arn:aws:acm:us-east-1:123456789012:certificate/x",
    records: [{ purpose: "certificate", name: "_a1.ch.example.com.", type: "CNAME", value: "_b2.acm-validations.aws." }, { purpose: "address", name: "ch.example.com", type: "CNAME", value: "d111.cloudfront.net" }] });
  assert.deepEqual(d, { hostname: "ch.example.com", mode: "external", state: "validating", records: [
    { purpose: "certificate", name: "_a1.ch.example.com", type: "CNAME", value: "_b2.acm-validations.aws" },
    { purpose: "address", name: "ch.example.com", type: "CNAME", value: "d111.cloudfront.net" },
  ] });
  assert.equal(publicDomain(null), null);
  assert.equal(statusDoc({ phase: "done" }).domain, null);
});

test("run: another DNS provider: the AWS address now, the certificate requested, its records in the progress file", async () => {
  const asked = [];
  const { deps, calls } = fakes({
    findZone: async () => null,
    setDomain: async (stage, host) => (asked.push([stage, host]), { ok: true, state: "validating", records: [{ purpose: "certificate", name: "_a1.ch.example.com", type: "CNAME", value: "_b2.acm-validations.aws" }] }),
  });
  const doc = await run({ env: { ...ENV, HOSTNAME_ARG: "ch.example.com" }, deps, log: () => {} });
  assert.equal(doc.phase, "done");
  assert.equal(doc.url, "https://d111.cloudfront.net");
  assert.equal(calls.config.domain, null);
  assert.deepEqual(asked, [["prod", "ch.example.com"]]);
  assert.equal(doc.domain.mode, "external");
  assert.equal(doc.domain.records[0].value, "_b2.acm-validations.aws");
});

test("run: a Route 53 zone in the account: the deploy does it all, nothing to add", async () => {
  const { deps, calls } = fakes({
    findZone: async () => ({ name: "example.com", id: "ZROOT", delegated: true }),
    setDomain: async () => assert.fail("no certificate request"),
    readState: () => ({ publicUrl: "https://ch.example.com" }),
  });
  const doc = await run({ env: { ...ENV, HOSTNAME_ARG: "ch.example.com" }, deps, log: () => {} });
  assert.equal(calls.config.domain.hostedZone, "example.com");
  assert.deepEqual(doc.domain, { hostname: "ch.example.com", mode: "route53", state: "live", records: [] });
  assert.equal(doc.url, "https://ch.example.com");
});

test("run: a certificate request that fails still finishes the install, marked for Settings", async () => {
  const { deps } = fakes({ findZone: async () => null, setDomain: async () => ({ ok: false, error: "enter a domain you own" }) });
  const doc = await run({ env: { ...ENV, HOSTNAME_ARG: "ch.example.com" }, deps, log: () => {} });
  assert.equal(doc.phase, "done");
  assert.equal(doc.domain.state, "failed");
});

test("updates: off by default; on with a window and time zone from the stack", () => {
  assert.equal(configFromEnv(ENV, { account: "123456789012" }).updates, undefined);
  assert.equal(updatesFromEnv({ AUTO_UPDATE: "off", UPDATE_WINDOW: "Sun 03:00", UPDATE_TIMEZONE: "UTC" }), null);
  assert.deepEqual(configFromEnv({ ...ENV, AUTO_UPDATE: "on", UPDATE_WINDOW: "daily 02:00", UPDATE_TIMEZONE: "Europe/Berlin" }, { account: "123456789012" }).updates,
    { auto: true, window: "daily 02:00", timezone: "Europe/Berlin" });
  assert.throws(() => updatesFromEnv({ UPDATE_WINDOW: "whenever" }), /maintenance window/);
  assert.throws(() => updatesFromEnv({ UPDATE_TIMEZONE: "Mars" }), /time zone/);
});

test("run: a VERSION that isn't a release tag stops before anything is installed", async () => {
  for (const bad of ["latest", "v1.2.3;id", "../v1", ""]) {
    let installed = false;
    const { deps } = fakes({ installCli: async () => ((installed = true), { code: 0 }) });
    const doc = await run({ env: { ...ENV, VERSION: bad }, deps, log: () => {} });
    assert.equal(doc.phase, "failed", bad);
    assert.equal(installed, false, bad);
  }
});

// ── Hardened installs (ACCESS_ID from the Launch Stack) ─────────────────

test("hardened: the launch's id makes the config hardened; without it nothing changes", () => {
  assert.deepEqual(configFromEnv({ ...ENV, ACCESS_ID: "1a2b3c4d" }, { account: "123456789012" }).access, { mode: "hardened", owner: "launch", id: "1a2b3c4d" });
  assert.equal(configFromEnv(ENV, { account: "123456789012" }).access, undefined);
  assert.equal(configFromEnv({ ...ENV, ACCESS_ID: "not-an-id" }, { account: "123456789012" }).access, undefined);
});

test("hardened: a Route 53 zone outside us-east-1 goes through the updater's certificate, inside it through the deploy", () => {
  const found = { name: "example.com", id: "Z1", delegated: true };
  assert.equal(domainPlan({ ...ENV, ACCESS_ID: "1a2b3c4d", HOSTNAME_ARG: "ch.example.com" }, found).mode, "external");
  assert.equal(domainPlan({ ...ENV, ACCESS_ID: "1a2b3c4d", AWS_REGION: "us-east-1", HOSTNAME_ARG: "ch.example.com" }, found).mode, "route53");
  assert.equal(domainPlan({ ...ENV, HOSTNAME_ARG: "ch.example.com" }, found).mode, "route53", "unchanged without hardening");
});

test("hardened: a second launch moves a hardened install to this stack's id, and refuses one from before", () => {
  const env = { ...ENV, ACCESS_ID: "99887766" };
  assert.deepEqual(relaunchConfig({ stage: "prod", access: { mode: "hardened", owner: "launch", id: "1a2b3c4d" } }, env).access.id, "99887766");
  assert.throws(() => relaunchConfig({ stage: "prod" }, env), /set up before hardened installs.*access harden/);
  assert.deepEqual(relaunchConfig({ stage: "prod" }, ENV), { stage: "prod" }, "no change without hardening");
});

test("hardened: a refusal names the action and the resource, never the account", () => {
  const line = "CREATE_FAILED | AWS::IAM::Role | GatewayServiceRole Resource handler returned message: \"User: arn:aws:sts::123456789012:assumed-role/coderhelm-prod-deploy-1a2b3c4d/AWSCloudFormation is not authorized to perform: iam:CreateRole on resource: arn:aws:iam::123456789012:role/coderhelm-prod-ServiceRole-ab12cd34 because no identity-based policy allows the iam:CreateRole action\"";
  assert.equal(refusal(line), "AWS refused iam:CreateRole on role/coderhelm-prod-ServiceRole-ab12cd34.");
  const { message, fix } = explainFailure([line]);
  assert.equal(message, "AWS refused iam:CreateRole on role/coderhelm-prod-ServiceRole-ab12cd34.");
  assert.match(fix, /Delete this stack/);
  const doc = statusDoc({ phase: "failed", message, fix });
  assert.doesNotMatch(JSON.stringify(doc), /123456789012/);
});

test("run: a second hardened launch of a hardened install writes the config with this launch's id", async () => {
  let written = null;
  const { deps } = fakes({ pull: async () => ({ code: 0 }), readConfig: () => ({ stage: "prod", access: { mode: "hardened", owner: "launch", id: "1a2b3c4d" } }), writeConfig: async (c) => void (written = c) });
  const doc = await run({ env: { ...ENV, ACCESS_ID: "99887766" }, deps, log: () => {} });
  assert.equal(doc.phase, "done");
  assert.equal(written.access.id, "99887766");
  const old = fakes({ pull: async () => ({ code: 0 }), readConfig: () => ({ stage: "prod" }) });
  const refused = await run({ env: { ...ENV, ACCESS_ID: "99887766" }, deps: old.deps, log: () => {} });
  assert.equal(refused.phase, "failed");
  assert.match(refused.message, /set up before hardened installs/);
});
