// The deployment config: what a valid file looks like, what each mistake
// says, and the defaults a self-hosted deployment gets.
//
//   npm test   (compiles with tsc, then runs node --test)

import { test } from "node:test";
import * as assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import {
  parseWindow,
  CoderhelmConfig,
  ConfigError,
  chooseConfigFile,
  loadConfigFile,
  resolveConfig,
  substituteEnv,
  validateConfig,
} from "../lib/config";

const INFRA = path.join(__dirname, "..", "..");

function base(over: Partial<CoderhelmConfig> = {}): CoderhelmConfig {
  return {
    account: "123456789012",
    region: "us-east-1",
    stage: "prod",
    email: { from: "coderhelm@example.com" },
    admins: ["admin@example.com"],
    ...over,
  };
}

function problems(c: unknown): string[] {
  return validateConfig(c);
}

function hasProblem(c: unknown, re: RegExp) {
  const p = problems(c);
  assert.ok(p.some((x) => re.test(x)), `expected a problem matching ${re}, got:\n${p.join("\n")}`);
}

test("the smallest self-host config is valid", () => {
  assert.deepEqual(problems(base()), []);
});

test("the shipped example configs are valid", () => {
  for (const f of ["selfhost.example.json", "selfhost.domain.example.json"]) {
    const raw = JSON.parse(fs.readFileSync(path.join(INFRA, "config", f), "utf8"));
    assert.deepEqual(problems(raw), [], f);
  }
});

test("the hosted configs are valid once the account is filled in", () => {
  for (const f of ["hosted.prod.json", "local.json"]) {
    const cfg = loadConfigFile(path.join(INFRA, "config", f), { CDK_DEFAULT_ACCOUNT: "654654210434" });
    assert.equal(cfg.layout, "split", f);
    assert.equal(cfg.suffix, "", f);
  }
});

test("self-host defaults: single origin, no domain, Anthropic, retained data, account-suffixed global names", () => {
  const r = resolveConfig(base());
  assert.equal(r.layout, "single-origin");
  assert.equal(r.prefix, "coderhelm-prod");
  assert.equal(r.suffix, "-123456789012");
  assert.equal(r.appUrl, null);
  assert.equal(r.apiUrl, null);
  assert.equal(r.publicUrlParameter, "/coderhelm-prod/public-url");
  assert.equal(r.retainData, true);
  assert.equal(r.models.provider, "anthropic");
  assert.equal(r.dataBucketName, "coderhelm-prod-data-123456789012");
  assert.equal(r.sandboxBucketName, "coderhelm-prod-sandbox-123456789012");
  assert.equal(r.cognitoDomainPrefix, "coderhelm-prod-123456789012");
  assert.equal(r.secretName, "coderhelm/prod/secrets");
  assert.equal(r.email.identity, "example.com");
  assert.equal(r.email.supportInbox, "coderhelm@example.com");
  assert.equal(r.email.cognitoUsesSes, false);
  assert.equal(r.integrations.googleSignIn, false);
  assert.equal(r.integrations.sandbox, true);
  assert.equal(r.reviewerEvalsRepo, null);
  assert.equal(r.publicBucketName, null);
  assert.equal(r.siteUrl, null);
});

test("a custom domain makes the URL known at synth", () => {
  const r = resolveConfig(base({ domain: { hostname: "ch.example.com", hostedZone: "example.com" } }));
  assert.equal(r.appUrl, "https://ch.example.com");
  assert.equal(r.apiUrl, "https://ch.example.com");
});

test("admins can always sign up; addresses and domains are lowercased and deduplicated", () => {
  const r = resolveConfig(
    base({ admins: ["Admin@Example.com"], signup: { emails: ["admin@example.com", "B@x.io"], domains: ["Example.COM"] } })
  );
  assert.deepEqual(r.signup.emails, ["admin@example.com", "b@x.io"]);
  assert.deepEqual(r.signup.domains, ["example.com"]);
});

test("a custom prefix also names the secret", () => {
  const r = resolveConfig(base({ resourcePrefix: "acme-helm" }));
  assert.equal(r.secretName, "acme-helm/secrets");
  assert.equal(r.dataBucketName, "acme-helm-data-123456789012");
});

test("an empty suffix keeps global names bare", () => {
  const r = resolveConfig(base({ globalNameSuffix: "" }));
  assert.equal(r.dataBucketName, "coderhelm-prod-data");
});

test("every problem is reported at once", () => {
  const p = problems({ account: "12", region: "nowhere", stage: "Prod!" });
  assert.ok(p.length >= 4, p.join("\n"));
});

test("not an object", () => {
  assert.deepEqual(problems([]), ["the config must be a JSON object"]);
  assert.deepEqual(problems(null), ["the config must be a JSON object"]);
});

test("account, region and stage", () => {
  hasProblem(base({ account: "1234" }), /^account:/);
  hasProblem(base({ account: 123456789012 as unknown as string }), /^account:/);
  hasProblem(base({ region: "useast1" }), /^region:/);
  hasProblem(base({ stage: "Prod" }), /^stage:/);
  hasProblem(base({ stage: "a".repeat(21) }), /^stage:/);
  assert.deepEqual(problems(base({ region: "us-gov-west-1" })), []);
});

test("unknown settings are flagged (typos)", () => {
  hasProblem({ ...base(), emial: {} }, /unknown setting "emial"/);
});

test("single-origin needs an admin", () => {
  hasProblem(base({ admins: [] }), /^admins: add at least one/);
  hasProblem(base({ admins: ["not-an-email"] }), /^admins: a list of email/);
});

test("domain checks", () => {
  hasProblem(base({ domain: { hostname: "ch.example.com" } }), /set hostedZone .* or certificateArn/);
  hasProblem(base({ domain: { hostname: "ch.other.com", hostedZone: "example.com" } }), /is not inside the zone/);
  hasProblem(base({ domain: { hostname: "CH.example.com", hostedZone: "example.com" } }), /domain.hostname/);
  hasProblem(
    base({ domain: { hostname: "ch.example.com", certificateArn: "arn:aws:acm:eu-west-1:123456789012:certificate/abc" } }),
    /us-east-1/
  );
  hasProblem(base({ domain: { hostname: "ch.example.com", hostedZone: "example.com", hostedZoneId: "nope" } }), /hostedZoneId/);
  assert.deepEqual(
    problems(base({ domain: { hostname: "ch.example.com", certificateArn: "arn:aws:acm:us-east-1:123456789012:certificate/0a1b-2c" } })),
    []
  );
  assert.deepEqual(problems(base({ domain: { hostname: "example.com", hostedZone: "example.com" } })), []);
});

test("layout checks", () => {
  hasProblem(base({ layout: "both" as never }), /^layout:/);
  hasProblem(base({ layout: "split" }), /^split: required/);
  hasProblem(base({ split: {} as never }), /only used with layout "split"/);
  hasProblem(
    base({
      layout: "split",
      split: { zone: null, appUrl: "https://a", apiUrl: "https://b", apiDomain: "api.x.com", corsOrigins: [], streamCorsOrigins: [] },
    }),
    /split.apiDomain: needs split.zone/
  );
  hasProblem(
    base({
      layout: "split",
      domain: { hostname: "a.b.com", hostedZone: "b.com" },
      split: { zone: null, appUrl: "https://a", apiUrl: "https://b", corsOrigins: [], streamCorsOrigins: [] },
    }),
    /^domain: only used with layout "single-origin"/
  );
});

test("Google sign-in needs a domain in single-origin", () => {
  hasProblem(base({ integrations: { googleSignIn: true } }), /googleSignIn: needs a domain/);
  assert.deepEqual(
    problems(base({ domain: { hostname: "ch.example.com", hostedZone: "example.com" }, integrations: { googleSignIn: true } })),
    []
  );
});

test("migrateUsersFrom: a pool and client of another CoderHelm, self-host only", () => {
  const ok = { region: "us-east-1", userPoolId: "us-east-1_AbC123", clientId: "abc123def456" };
  assert.deepEqual(problems(base({ migrateUsersFrom: ok })), []);
  assert.deepEqual(problems(base({ migrateUsersFrom: { ...ok, clientSecretKey: "migrate_client_secret" } })), []);
  assert.deepEqual(problems(base({ migrateUsersFrom: null })), []);
  hasProblem(base({ migrateUsersFrom: { ...ok, region: "mars" } }), /migrateUsersFrom.region/);
  hasProblem(base({ migrateUsersFrom: { ...ok, userPoolId: "pool" } }), /migrateUsersFrom.userPoolId/);
  hasProblem(base({ migrateUsersFrom: { ...ok, clientId: "Not An Id" } }), /migrateUsersFrom.clientId/);
  hasProblem(base({ migrateUsersFrom: { ...ok, clientSecretKey: "a b" } }), /migrateUsersFrom.clientSecretKey/);
  hasProblem(base({ migrateUsersFrom: { ...ok, password: "x" } as never }), /migrateUsersFrom.password: unknown/);
  hasProblem(
    base({
      layout: "split",
      split: { zone: null, appUrl: "https://a", apiUrl: "https://b", corsOrigins: [], streamCorsOrigins: [] },
      migrateUsersFrom: ok,
    }),
    /migrateUsersFrom: only for a self-hosted install/
  );
});

test("models", () => {
  hasProblem(base({ models: { provider: "openai" as never } }), /models.provider/);
  hasProblem(base({ models: { primary: "Claude Opus" } }), /models.primary/);
  hasProblem(base({ models: { bedrockInferencePrefix: "usa" } }), /bedrockInferencePrefix/);
  hasProblem(base({ models: { bedrockModelIds: { "claude-x": "meta.llama" } } }), /bedrockModelIds.claude-x/);
  assert.deepEqual(
    problems(base({ models: { provider: "bedrock", bedrockInferencePrefix: "", bedrockModelIds: { "claude-haiku-4-5": "anthropic.claude-haiku-4-5-20251001-v1:0" } } })),
    []
  );
});

test("email", () => {
  hasProblem({ ...base(), email: undefined }, /^email.from: required/);
  hasProblem(base({ email: { from: "nope" } }), /^email.from: an email/);
  hasProblem(base({ email: { from: "a@example.com", identity: "other.com" } }), /does not cover the sender/);
  hasProblem(base({ email: { from: "a@example.com", identity: "b@example.com" } }), /does not cover the sender/);
  hasProblem(base({ email: { from: "a@example.com", privacyUrl: "privacy" } }), /email.privacyUrl/);
  assert.deepEqual(problems(base({ email: { from: "a@mail.example.com", identity: "example.com" } })), []);
  assert.deepEqual(problems(base({ email: { from: "a@example.com", identity: "a@example.com" } })), []);
  assert.deepEqual(problems(base({ email: { from: "a@example.com", identity: null } })), []);
});

test("integrations are booleans; telemetry endpoint is https", () => {
  hasProblem(base({ integrations: { sandbox: "yes" as never } }), /integrations.sandbox: true or false/);
  hasProblem(base({ telemetry: { endpoint: "http://x" } }), /telemetry.endpoint/);
  hasProblem(base({ telemetry: { enabled: "on" as never } }), /telemetry.enabled/);
});

test("names that would break AWS limits", () => {
  // The longest prefix and suffix the patterns allow still fit S3's 63.
  assert.deepEqual(problems(base({ resourcePrefix: "a" + "b".repeat(30), globalNameSuffix: "-" + "c".repeat(20) })), []);
  hasProblem(base({ resourcePrefix: "my-aws-helm" }), /sign-in domain prefix/);
});

test("resolveConfig throws a ConfigError listing the problems", () => {
  assert.throws(() => resolveConfig(base({ account: "x" })), (e: unknown) => e instanceof ConfigError && /account/.test(e.message));
});

test("${ENV} references are filled from the environment; a missing one is an error", () => {
  assert.equal(substituteEnv('{"a":"${X}"}', { X: "1" }), '{"a":"1"}');
  assert.throws(() => substituteEnv('{"a":"${MISSING_ONE}"}', {}), /MISSING_ONE/);
});

test("loadConfigFile reports bad JSON clearly", () => {
  const f = path.join(fs.mkdtempSync(path.join(os.tmpdir(), "chcfg-")), "c.json");
  fs.writeFileSync(f, "{ nope");
  assert.throws(() => loadConfigFile(f), /not valid JSON/);
});

test("which config file: -c config, then CODERHELM_CONFIG, then the hosted files", () => {
  assert.equal(chooseConfigFile({ contextConfig: "a.json", envConfig: "b.json", infraDir: "/i" }).file, path.resolve("a.json"));
  assert.equal(chooseConfigFile({ envConfig: "b.json", infraDir: "/i" }).file, path.resolve("b.json"));
  assert.equal(chooseConfigFile({ infraDir: "/i" }).file, "/i/config/hosted.prod.json");
  assert.deepEqual(chooseConfigFile({ stage: "dev", infraDir: "/i" }), { file: "/i/config/local.json", stage: "dev" });
});

test("updates settings", () => {
  assert.deepEqual(parseWindow("Sun 03:00"), { day: "sun", hour: 3 });
  assert.deepEqual(parseWindow("daily 23:00"), { day: "daily", hour: 23 });
  assert.equal(parseWindow("Sun 3pm"), null);
  assert.equal(parseWindow("Sun 24:00"), null);
  hasProblem(base({ updates: { window: "whenever" } }), /updates.window/);
  hasProblem(base({ updates: { channel: "beta" as never } }), /updates.channel/);
  hasProblem(base({ updates: { majors: "never" as never } }), /updates.majors/);
  hasProblem(base({ updates: { webhookUrl: "http://x" } }), /updates.webhookUrl/);
  const r = resolveConfig(base());
  assert.equal(r.updates.enabled, true);
  assert.deepEqual(r.updates.defaults, { auto: false, channel: "stable", day: "sun", hour: 3, timezone: "UTC", majors: "ask" });
});

test("signup.requireTwoFactor: off unless set, and only a boolean", () => {
  const base = (over: Record<string, unknown> = {}) =>
    resolveConfig({ account: "111122223333", region: "us-west-2", stage: "prod", email: { from: "a@example.org" }, admins: ["a@example.org"], ...over } as any);
  assert.equal(base().signup.requireTwoFactor, false);
  assert.equal(base({ signup: { requireTwoFactor: true } }).signup.requireTwoFactor, true);
  assert.throws(() => base({ signup: { requireTwoFactor: "yes" } }), /requireTwoFactor/);
});
