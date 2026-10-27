// Every check's pass and fail output, with a fake AWS CLI, a fake fetch
// and a fake DNS resolver: no real cloud calls. Then the guided install's
// "fail, fix, check again" loop and the doctor end to end.
//
//   cd installer && npm test
import { test } from "node:test";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import * as C from "../lib/checks.mjs";
import { gate, BACK, CheckFailed } from "../lib/gate.mjs";
import { main } from "../lib/cli.mjs";
import { PERMISSIONS, EVENTS } from "../lib/github-app.mjs";
import { FakeRunner } from "./fake.mjs";

const ACCOUNT = "111122223333";
const ctx = (answers = {}, extra = {}) => ({ runner: new FakeRunner(answers), log: () => {}, flags: {}, ...extra });
const noArrows = (r) => assert.doesNotMatch(JSON.stringify(r), /[\u2192\u21D2\u279C\u25B6]/);

/** A problem result has all three parts, plainly. */
function assertExplained(r, status = C.FAIL) {
  assert.equal(r.status, status, JSON.stringify(r));
  assert.ok(r.what && r.what.length < 300, "what is one short sentence");
  assert.ok(r.why, "why it matters");
  assert.ok(r.fix && (r.fix.command || r.fix.url), "a command to copy or a link");
  noArrows(r);
}

const json = (status, body) => ({ status, ok: status < 300, json: async () => body, text: async () => JSON.stringify(body) });

// ── This machine and AWS ────────────────────────────────────────────────

test("node and the AWS CLI", async () => {
  assert.equal(C.checkNode("22.3.0").status, C.PASS);
  assertExplained(C.checkNode("18.19.0"));
  assert.equal((await C.checkAwsCli(ctx({ "aws --version": "aws-cli/2.17.0 Python/3" }))).status, C.PASS);
  const v1 = await C.checkAwsCli(ctx({ "aws --version": "aws-cli/1.29.0 Python/3" }));
  assertExplained(v1);
  assert.match(v1.what, /version 1/);
  assertExplained(await C.checkAwsCli(ctx({ "aws --version": { code: 127 } })));
});

test("sign-in: expired SSO says to sign in again, no credentials says how to set them up", async () => {
  const ok = await C.checkSignIn(ctx({ "sts get-caller-identity": { Account: ACCOUNT, Arn: "arn:aws:iam::1:user/me" } }));
  assert.equal(ok.result.status, C.PASS);
  assert.equal(ok.identity.Account, ACCOUNT);
  const expired = await C.checkSignIn(ctx({ "sts get-caller-identity": { code: 255, stderr: "Error when retrieving token from sso: Token has expired and refresh failed" } }), { profile: "prod" });
  assertExplained(expired.result);
  assert.equal(expired.result.fix.command, "aws sso login --profile prod");
  const none = await C.checkSignIn(ctx({ "sts get-caller-identity": { code: 253, stderr: "Unable to locate credentials" } }));
  assert.equal(none.result.fix.command, "aws configure sso");
  assertExplained(C.checkAccountMatch({ Account: "999999999999" }, ACCOUNT));
  assert.equal(C.checkAccountMatch({ Account: ACCOUNT }, ACCOUNT).status, C.PASS);
});

test("permissions: simulated against the role (with its path), each missing action listed", async () => {
  const sso = { Account: ACCOUNT, Arn: `arn:aws:sts::${ACCOUNT}:assumed-role/AWSReservedSSO_Dev_abc/me@example.org` };
  const allowed = ctx({
    "iam get-role": { Role: { Arn: `arn:aws:iam::${ACCOUNT}:role/aws-reserved/sso.amazonaws.com/AWSReservedSSO_Dev_abc` } },
    "simulate-principal-policy": { EvaluationResults: C.NEEDED_ACTIONS.map((a) => ({ EvalActionName: a, EvalDecision: "allowed" })) },
  });
  assert.equal((await C.checkPermissions(allowed, sso)).status, C.PASS);
  const sim = allowed.runner.commands.find((c) => c.args.includes("simulate-principal-policy"));
  assert.ok(sim.args.includes(`arn:aws:iam::${ACCOUNT}:role/aws-reserved/sso.amazonaws.com/AWSReservedSSO_Dev_abc`), "the role ARN with its path");

  const denied = ctx({
    "simulate-principal-policy": { EvaluationResults: C.NEEDED_ACTIONS.map((a) => ({ EvalActionName: a, EvalDecision: ["iam:PassRole", "ecr:CreateRepository"].includes(a) ? "implicitDeny" : "allowed" })) },
  });
  const r = await C.checkPermissions(denied, { Account: ACCOUNT, Arn: `arn:aws:iam::${ACCOUNT}:user/dev` });
  assertExplained(r);
  assert.match(r.what, /2 things the install does: iam:PassRole, ecr:CreateRepository/);

  const cantSimulate = ctx({ "simulate-principal-policy": { code: 254, stderr: "AccessDenied: not authorized to perform iam:SimulatePrincipalPolicy" } });
  const w = await C.checkPermissions(cantSimulate, { Account: ACCOUNT, Arn: `arn:aws:iam::${ACCOUNT}:user/dev` });
  assert.equal(w.status, C.WARN, "can't check is a warning, not a failure");
  assert.match(w.what, /SimulatePrincipalPolicy/);
});

test("region: missing services and Bedrock are named; unknown regions too", async () => {
  const services = (names) => ({ Parameters: names.map((n) => ({ Name: `/x/${n}`, Value: n })) });
  assert.equal((await C.checkRegion(ctx({ "get-parameters-by-path": services(C.NEEDED_SERVICES) }), "us-east-1")).status, C.PASS);
  const r = await C.checkRegion(ctx({ "get-parameters-by-path": services(C.NEEDED_SERVICES.filter((s) => s !== "codebuild")) }), "il-central-1", { bedrock: true });
  assert.equal(r.status, C.FAIL);
  assert.match(r.what, /lacks codebuild, bedrock/);
  const bogus = await C.checkRegion(ctx({ "get-parameters-by-path": { Parameters: [] }, "describe-regions": ["us-east-1", "eu-west-1"] }), "mars-east-1");
  assert.match(bogus.what, /isn't an AWS region/);
});

test("Bedrock: each model not enabled is named, with the console link for the region", async () => {
  const ok = ctx({ "get-foundation-model-availability": { authorizationStatus: "AUTHORIZED", agreementAvailability: { status: "AVAILABLE" } } });
  assert.equal((await C.checkBedrock(ok, "eu-west-1")).status, C.PASS);
  const off = ctx({ "model-id anthropic.claude-opus-5-5": { authorizationStatus: "NOT_AUTHORIZED" }, "model-id anthropic.claude-sonnet-5-5": { authorizationStatus: "AUTHORIZED" } });
  const r = await C.checkBedrock(off, "eu-west-1");
  assertExplained(r);
  assert.match(r.what, /Claude Opus 5\.5 \(anthropic\.claude-opus-5-5\)/);
  assert.doesNotMatch(r.what, /Sonnet/);
  assert.equal(r.fix.url, "https://console.aws.amazon.com/bedrock/home?region=eu-west-1#/modelaccess");
});

test("CDK bootstrap: present passes, missing offers to run it, a failed stack says to delete it first", async () => {
  assert.equal((await C.checkBootstrap(ctx({ "describe-stacks": { Stacks: [{ StackStatus: "UPDATE_COMPLETE" }] } }), ACCOUNT, "us-east-1")).status, C.PASS);
  const missing = await C.checkBootstrap(ctx(), ACCOUNT, "eu-west-1");
  assertExplained(missing);
  assert.equal(missing.fix.command, `npx -y cdk bootstrap aws://${ACCOUNT}/eu-west-1`);
  assert.deepEqual(missing.action.command, ["npx", ["--yes", "cdk", "bootstrap", `aws://${ACCOUNT}/eu-west-1`]]);
  assert.equal((await C.checkBootstrap(ctx(), ACCOUNT, "eu-west-1", { missing: C.WARN })).status, C.WARN);
  const broken = await C.checkBootstrap(ctx({ "describe-stacks": { Stacks: [{ StackStatus: "UPDATE_ROLLBACK_FAILED" }] } }), ACCOUNT, "eu-west-1");
  assert.equal(broken.status, C.FAIL);
  assert.match(broken.fix.text, /Delete the CDKToolkit stack/);
});

// ── Domain and DNS ──────────────────────────────────────────────────────

test("hosted zone and NS delegation: expected and current name servers are both shown", async () => {
  const ns = ["ns-1.awsdns-01.org", "ns-2.awsdns-02.co.uk", "ns-3.awsdns-03.com", "ns-4.awsdns-04.net"];
  const answers = {
    "list-hosted-zones-by-name": { HostedZones: [{ Name: "example.org.", Id: "/hostedzone/Z123", Config: { PrivateZone: false } }] },
    "get-hosted-zone": { DelegationSet: { NameServers: ns } },
  };
  const c = ctx(answers, { dns: { resolveNs: async () => ns.map((n) => n.toUpperCase() + ".") } });
  const z = await C.checkHostedZone(c, "example.org");
  assert.equal(z.result.status, C.PASS);
  assert.equal((await C.checkDelegation(c, "example.org", z.zoneId)).status, C.PASS, "case and trailing dots don't matter");

  const wrong = ctx(answers, { dns: { resolveNs: async () => ["dns1.registrar.example", "dns2.registrar.example"] } });
  const r = await C.checkDelegation(wrong, "example.org", "/hostedzone/Z123");
  assertExplained(r);
  assert.match(r.what, /dns1\.registrar\.example/);
  assert.match(r.fix.command, /example\.org\. NS ns-1\.awsdns-01\.org\./);
  assert.equal(r.fix.command.split("\n").length, 4);

  const none = ctx(answers, { dns: { resolveNs: async () => { throw Object.assign(new Error("queryNs ENOTFOUND"), { code: "ENOTFOUND" }); } } });
  assert.match((await C.checkDelegation(none, "example.org", "/hostedzone/Z123")).what, /no name servers/);

  const missing = await C.checkHostedZone(ctx({ "list-hosted-zones-by-name": { HostedZones: [{ Name: "other.org.", Id: "/hostedzone/Z9" }] } }), "example.org");
  assertExplained(missing.result);
  assert.match(missing.result.fix.command, /create-hosted-zone --name example\.org/);
});

test("certificate: region, name, pending validation with its records, issued", async () => {
  const arn = `arn:aws:acm:us-east-1:${ACCOUNT}:certificate/abc-123`;
  assertExplained(await C.checkCertificate(ctx(), `arn:aws:acm:eu-west-1:${ACCOUNT}:certificate/x`, "helm.example.org"));
  const pending = ctx({ "describe-certificate": { Certificate: { DomainName: "helm.example.org", Status: "PENDING_VALIDATION", DomainValidationOptions: [{ ResourceRecord: { Name: "_x.helm.example.org.", Type: "CNAME", Value: "_y.acm-validations.aws." } }] } } });
  const p = await C.checkCertificate(pending, arn, "helm.example.org");
  assert.equal(p.status, C.WARN);
  assert.equal(p.fix.command, "_x.helm.example.org. CNAME _y.acm-validations.aws.");
  const other = ctx({ "describe-certificate": { Certificate: { DomainName: "www.example.org", Status: "ISSUED" } } });
  assert.match((await C.checkCertificate(other, arn, "helm.example.org")).what, /not helm\.example\.org/);
  const wildcard = ctx({ "describe-certificate": { Certificate: { DomainName: "*.example.org", Status: "ISSUED" } } });
  assert.equal((await C.checkCertificate(wildcard, arn, "helm.example.org")).status, C.PASS);
  assertExplained(await C.checkCertificate(ctx(), arn, "helm.example.org"));
});

// ── Email ───────────────────────────────────────────────────────────────

test("SES: unverified domain shows the DKIM records; the sandbox is a warning with the link", async () => {
  const pending = ctx({ "get-email-identity": { VerifiedForSendingStatus: false, DkimAttributes: { Status: "PENDING", Tokens: ["t1", "t2", "t3"] } } });
  const r = await C.checkSesIdentity(pending, "helm@example.org", "eu-west-1");
  assertExplained(r);
  assert.equal(r.fix.command, ["t1", "t2", "t3"].map((t) => `${t}._domainkey.example.org CNAME ${t}.dkim.amazonses.com`).join("\n"));
  const good = ctx({ "get-email-identity": { VerifiedForSendingStatus: true, DkimAttributes: { Status: "SUCCESS" } } });
  assert.equal((await C.checkSesIdentity(good, "helm@example.org", "eu-west-1")).status, C.PASS);
  assert.equal((await C.checkSesIdentity(ctx(), "helm@example.org", "eu-west-1", { zone: "example.org" })).status, C.PASS, "the deploy verifies a Route 53 zone");
  assert.equal((await C.checkSesIdentity(ctx(), "helm@other.org", "eu-west-1")).status, C.WARN);
  const sandbox = await C.checkSesSandbox(ctx({ "sesv2 get-account": { ProductionAccessEnabled: false } }), "eu-west-1");
  assertExplained(sandbox, C.WARN);
  assert.match(sandbox.fix.url, /region=eu-west-1#\/account/);
  assert.equal((await C.checkSesSandbox(ctx({ "sesv2 get-account": { ProductionAccessEnabled: true } }), "eu-west-1")).status, C.PASS);
});

// ── Model provider ──────────────────────────────────────────────────────

test("Anthropic key: a real one-token call; refused, wrong model, no credit, busy, offline", async () => {
  let sent;
  const ok = await C.checkAnthropicKey({ fetchImpl: async (url, init) => ((sent = { url, body: JSON.parse(init.body) }), json(200, {})) }, "sk-ant-x");
  assert.equal(ok.status, C.PASS);
  assert.equal(sent.url, "https://api.anthropic.com/v1/messages");
  assert.equal(sent.body.max_tokens, 1);
  assertExplained(await C.checkAnthropicKey({ fetchImpl: async () => json(401, {}) }, "k"));
  assert.match((await C.checkAnthropicKey({ fetchImpl: async () => json(404, {}) }, "k")).what, /can't use claude-sonnet-5-5/);
  assert.match((await C.checkAnthropicKey({ fetchImpl: async () => json(400, { error: { message: "Your credit balance is too low" } }) }, "k")).what, /no credit/);
  assert.equal((await C.checkAnthropicKey({ fetchImpl: async () => json(529, {}) }, "k")).status, C.WARN);
  assert.equal((await C.checkAnthropicKey({ fetchImpl: async () => { throw new Error("offline"); } }, "k")).status, C.WARN);
  assert.equal(await C.checkAnthropicKey({}, ""), null);
});

test("Bedrock call: a one-token converse; access denied links to model access", async () => {
  const ok = ctx({ "bedrock-runtime converse": { output: { message: { content: [{ text: "O" }] } } } });
  assert.equal((await C.checkBedrockCall(ok, "us-east-1", "us.")).status, C.PASS);
  assert.ok(ok.runner.lines()[0].includes("--model-id us.anthropic.claude-sonnet-5-5"));
  assert.ok(ok.runner.lines()[0].includes('"maxTokens":1'));
  const denied = await C.checkBedrockCall(ctx({ converse: { code: 254, stderr: "An error occurred (AccessDeniedException): You don't have access to the model" } }), "us-east-1", "us.");
  assertExplained(denied);
  assert.match(denied.fix.url, /modelaccess/);
});

// ── GitHub App ──────────────────────────────────────────────────────────

const { privateKey } = crypto.generateKeyPairSync("rsa", { modulusLength: 2048 });
const PEM = privateKey.export({ type: "pkcs1", format: "pem" });
const SECRET = { github_app_id: "42", github_private_key: PEM, github_webhook_secret: "w", github_client_id: "Iv1.x", github_client_secret: "s" };
const APP = { id: 42, slug: "coderhelm-prod", html_url: "https://github.com/apps/coderhelm-prod", owner: { login: "acme", type: "Organization" }, permissions: { ...PERMISSIONS }, events: [...EVENTS] };

/** A fake GitHub: routes by path; records the JWTs it was sent. */
function github(over = {}) {
  const seen = [];
  const routes = {
    "/app": json(200, APP),
    "/app/hook/config": json(200, { url: "https://helm.example.org/webhooks/github" }),
    "/app/hook/deliveries?per_page=10": json(200, [{ status_code: 200, event: "issues", delivered_at: "2026-10-25T09:00:00Z" }]),
    "/app/installations?per_page=100": json(200, [{ id: 1 }]),
    oauth: json(200, { error: "bad_verification_code" }),
    ...over,
  };
  const fetchImpl = async (url, init = {}) => {
    seen.push({ url, auth: init.headers?.authorization });
    if (url.includes("login/oauth")) return routes.oauth;
    return routes[url.replace("https://api.github.com", "")] ?? json(404, {});
  };
  return { fetchImpl, seen };
}

test("GitHub: the stored keys sign a JWT GitHub accepts, and everything passes", async () => {
  const gh = github();
  const results = await C.checkGithubApp({ fetchImpl: gh.fetchImpl }, SECRET, "https://helm.example.org");
  assert.deepEqual(results.map((r) => [r.id, r.status]), [
    ["github_keys", "pass"], ["github_app", "pass"], ["github_permissions", "pass"], ["github_webhook", "pass"], ["github_installations", "pass"], ["github_oauth", "pass"],
  ]);
  const jwt = gh.seen[0].auth.replace("Bearer ", "");
  const [, payload] = jwt.split(".");
  assert.equal(JSON.parse(Buffer.from(payload, "base64url")).iss, "42");
  const verified = crypto.createVerify("RSA-SHA256").update(jwt.split(".").slice(0, 2).join(".")).verify(crypto.createPublicKey(privateKey), Buffer.from(jwt.split(".")[2], "base64url"));
  assert.ok(verified, "a valid RS256 signature");
});

test("GitHub: an install on CoderHelm's app (hybrid) isn't failed for having no app of its own", async () => {
  const r = await C.checkGithubApp({ fetchImpl: github().fetchImpl }, { github_app_id: "" }, "https://h", { github_mode: "hosted" });
  assert.deepEqual(r.map((x) => [x.id, x.status]), [["github_keys", "pass"]]);
  const own = await C.checkGithubApp({ fetchImpl: github().fetchImpl }, { github_app_id: "" }, "https://h", { github_mode: "own" });
  assert.equal(own[0].status, "fail");
});

test("GitHub: no app, refused keys, missing permissions, wrong webhook, bad secret, no installs, bad OAuth client", async () => {
  const none = await C.checkGithubApp({ fetchImpl: github().fetchImpl }, { github_app_id: "" }, "https://h");
  assertExplained(none[0]);
  assert.match(none[0].fix.command, /github-app$/);

  const refused = await C.checkGithubApp({ fetchImpl: github({ "/app": json(401, {}) }).fetchImpl }, SECRET, "https://helm.example.org");
  assertExplained(refused[1]);
  assert.match(refused[1].fix.command, /github-app --force/);

  const weak = { ...APP, permissions: { ...PERMISSIONS, workflows: "read" }, events: EVENTS.filter((e) => e !== "workflow_run") };
  delete weak.permissions.checks;
  const perms = (await C.checkGithubApp({ fetchImpl: github({ "/app": json(200, weak) }).fetchImpl }, SECRET, "https://helm.example.org")).find((r) => r.id === "github_permissions");
  assertExplained(perms);
  assert.match(perms.what, /Workflows: write \(now read\)/);
  assert.match(perms.what, /Checks: read \(missing\)/);
  assert.match(perms.what, /workflow_run/);
  assert.equal(perms.fix.url, "https://github.com/organizations/acme/settings/apps/coderhelm-prod/permissions");

  const hook = (await C.checkGithubApp({ fetchImpl: github({ "/app/hook/config": json(200, { url: "https://old.example.org/webhooks/github" }) }).fetchImpl }, SECRET, "https://helm.example.org")).find((r) => r.id === "github_webhook");
  assertExplained(hook);
  assert.match(hook.fix.text, /https:\/\/helm\.example\.org\/webhooks\/github/);

  const secret = (await C.checkGithubApp({ fetchImpl: github({ "/app/hook/deliveries?per_page=10": json(200, [{ status_code: 401, event: "issues" }]) }).fetchImpl }, SECRET, "https://helm.example.org")).find((r) => r.id === "github_webhook");
  assertExplained(secret);
  assert.match(secret.what, /webhook secret doesn't match/);

  const quiet = (await C.checkGithubApp({ fetchImpl: github({ "/app/hook/deliveries?per_page=10": json(200, []) }).fetchImpl }, SECRET, "https://helm.example.org")).find((r) => r.id === "github_webhook");
  assert.equal(quiet.status, C.WARN);

  const installs = (await C.checkGithubApp({ fetchImpl: github({ "/app/installations?per_page=100": json(200, []) }).fetchImpl }, SECRET, "https://helm.example.org")).find((r) => r.id === "github_installations");
  assertExplained(installs);
  assert.equal(installs.fix.url, "https://github.com/apps/coderhelm-prod/installations/new");

  const oauth = (await C.checkGithubApp({ fetchImpl: github({ oauth: json(200, { error: "incorrect_client_credentials" }) }).fetchImpl }, SECRET, "https://helm.example.org")).find((r) => r.id === "github_oauth");
  assertExplained(oauth);
});

test("webhooks for notices: a removed Slack hook fails, an empty-payload 400 passes", async () => {
  assert.equal((await C.checkWebhook({ fetchImpl: async () => json(400, {}) }, "https://hooks.slack.com/services/T/B/x")).status, C.PASS);
  const gone = await C.checkWebhook({ fetchImpl: async () => json(404, {}) }, "https://hooks.slack.com/services/T/B/x");
  assertExplained(gone);
  assert.match(gone.title, /Slack/);
  assert.equal(await C.checkWebhook({}, ""), null);
});

// ── An install's health ─────────────────────────────────────────────────

test("health checks: stacks, functions, DLQ and URLs each say what to do", async () => {
  const cfg = { account: ACCOUNT, region: "us-west-2", stage: "prod" };
  const stacks = await C.checkStacks(ctx({ "list-stacks": { StackSummaries: [{ StackName: "coderhelm-prod-api", StackStatus: "UPDATE_ROLLBACK_FAILED" }] } }), cfg);
  assertExplained(stacks);
  assert.match(stacks.what, /coderhelm-prod-api \(UPDATE_ROLLBACK_FAILED\)/);
  assertExplained(await C.checkStacks(ctx({ "list-stacks": { StackSummaries: [] } }), cfg));
  const fns = await C.checkFunctions(ctx({ "get-function-configuration": { State: "Failed", LastUpdateStatus: "Failed", StateReason: "bad zip" } }), cfg);
  assert.equal(fns.length, 3);
  assert.match(fns[1].what, /coderhelm-prod-worker is Failed\/Failed: bad zip/);
  const dlq = await C.checkDlq(ctx({ "get-queue-url": { QueueUrl: "q" }, "get-queue-attributes": { Attributes: { ApproximateNumberOfMessages: "4" } } }), cfg);
  assert.equal(dlq.status, C.WARN);

  const dnsFail = await C.checkUrls({ ...ctx(), fetchImpl: async () => { throw Object.assign(new TypeError("fetch failed"), { cause: { code: "ENOTFOUND" } }); } }, "https://helm.example.org", cfg);
  assertExplained(dnsFail[0]);
  assert.match(dnsFail[0].what, /doesn't resolve/);
  const tlsFail = await C.checkUrls({ ...ctx(), fetchImpl: async () => { throw Object.assign(new TypeError("fetch failed"), { cause: { code: "ERR_TLS_CERT_ALTNAME_INVALID" } }); } }, "https://helm.example.org", cfg);
  assert.match(tlsFail[0].what, /certificate/);
  const up = await C.checkUrls({ ...ctx(), fetchImpl: async (u) => json(u.endsWith("/api/me") ? 401 : 200, {}) }, "https://helm.example.org", cfg);
  assert.deepEqual(up.map((r) => r.status), [C.PASS, C.PASS]);
});

test("formatting: what, why and how to fix, each command and link on its own line", () => {
  const text = C.formatResults([
    C.pass("a", "Alpha", "fine"),
    C.problem(C.FAIL, "b", "Bravo", { what: "B is off.", why: "It matters.", fix: { text: "Turn it on:", command: "one\ntwo", url: "https://x" } }),
  ]);
  assert.equal(text, [
    "  PASS  Alpha  (fine)",
    "  FAIL  Bravo",
    "        What's wrong: B is off.",
    "        Why it matters: It matters.",
    "        How to fix: Turn it on:",
    "          one",
    "          two",
    "          https://x",
  ].join("\n"));
  assert.deepEqual(C.tally([C.pass("a", "A"), null, C.problem(C.WARN, "w", "W", {})]), { pass: 1, warn: 1, fail: 0, skip: 0, ok: true });
});

// ── Fail, fix, check again ──────────────────────────────────────────────

test("gate: a failed check waits; after the fix, Enter checks again and it moves on", async () => {
  let fixed = false;
  const asked = [];
  const logs = [];
  const ask = { ask: async (q) => { asked.push(q); fixed = true; return ""; } };
  const results = await gate({ interactive: true, ask, log: (m) => logs.push(m), runner: new FakeRunner() }, async () => [
    fixed ? C.pass("x", "The thing") : C.problem(C.FAIL, "x", "The thing", { what: "It's off.", why: "Needed.", fix: { command: "turn-on" } }),
  ]);
  assert.equal(results[0].status, C.PASS);
  assert.equal(asked.length, 1);
  assert.match(asked[0], /press Enter to check again/);
  const out = logs.join("\n");
  assert.match(out, /FAIL  The thing[\s\S]*turn-on[\s\S]*Checking again[\s\S]*PASS  The thing/);
});

test("gate: skip goes on, back goes back, an action runs before checking again", async () => {
  const failing = async () => [C.problem(C.FAIL, "x", "X", { what: "w", why: "y", fix: { command: "c" } })];
  const base = { interactive: true, log: () => {}, runner: new FakeRunner() };
  assert.equal((await gate({ ...base, ask: { ask: async () => "skip" } }, failing))[0].status, C.FAIL);
  assert.equal(await gate({ ...base, ask: { ask: async () => "back" } }, failing), BACK);

  let bootstrapped = false;
  const runner = new FakeRunner({ "cdk bootstrap": () => ((bootstrapped = true), "") });
  const res = await gate({ ...base, runner, ask: { ask: async (q) => (q.startsWith("Run the bootstrap") ? "yes" : "") } }, async () =>
    [bootstrapped ? C.pass("cdk", "CDK") : await C.checkBootstrap({ runner: new FakeRunner() }, ACCOUNT, "us-east-1")]);
  assert.equal(res[0].status, C.PASS);
  assert.ok(runner.lines().some((l) => l === `npx --yes cdk bootstrap aws://${ACCOUNT}/us-east-1`));
});

test("gate without a terminal: stops with the explanation, unless --skip-checks names the check", async () => {
  const failing = async () => [C.problem(C.FAIL, "bedrock", "Bedrock", { what: "Off.", why: "Needed.", fix: { url: "https://x" } })];
  await assert.rejects(() => gate({ interactive: false, log: () => {}, flags: {} }, failing), (e) => e instanceof CheckFailed && /What's wrong: Off\.[\s\S]*https:\/\/x/.test(e.message));
  assert.equal((await gate({ interactive: false, log: () => {}, flags: { skipChecks: "bedrock" } }, failing)).length, 1);
});

test("guided install: a Bedrock model is off, the person enables it and presses Enter, the install goes on", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "chchk-"));
  const file = path.join(dir, "coderhelm.config.json");
  let enabled = false;
  const runner = new FakeRunner({
    "aws --version": "aws-cli/2.17.0",
    "sts get-caller-identity": { Account: ACCOUNT, Arn: `arn:aws:iam::${ACCOUNT}:user/me` },
    "tar --version": "bsdtar",
    "describe-stacks": { Stacks: [{ StackStatus: "CREATE_COMPLETE" }] },
    "get-foundation-model-availability": () => ({ authorizationStatus: enabled ? "AUTHORIZED" : "NOT_AUTHORIZED" }),
    "sesv2 get-account": { ProductionAccessEnabled: true },
  });
  const asked = [];
  const answers = [
    ["AWS account", ACCOUNT], ["AWS region", "us-east-1"], ["short name", "prod"], ["Hostname", ""],
    ["anthropic or bedrock", "bedrock"], ["prefix", "us."],
    ["press Enter to check again", () => { enabled = true; return ""; }],
    ["Your email", "owner@example.org"],
  ];
  const ask = {
    ask: async (q, def) => {
      asked.push(q);
      const i = answers.findIndex(([k]) => q.includes(k));
      if (i < 0) return def;
      const [, a] = answers.splice(i, 1)[0];
      return typeof a === "function" ? a() : a;
    },
    confirm: async (q, d) => d,
    close() {},
  };
  const logs = [];
  const code = await main(["init", "--config", file, "--deploy", "no"], { runner, ask, interactive: true, log: (m) => logs.push(String(m)) });
  assert.equal(code, 0);
  const out = logs.join("\n");
  assert.match(out, /FAIL  Claude models enabled in Bedrock[\s\S]*modelaccess[\s\S]*Checking again[\s\S]*PASS  Claude models enabled in Bedrock/);
  assert.equal(JSON.parse(fs.readFileSync(file, "utf8")).models.provider, "bedrock");
  assert.doesNotMatch(out, /\n\s+at .*\.mjs/, "no stack traces");
  noArrows(out);
});

// ── doctor ──────────────────────────────────────────────────────────────

function doctorFixture({ broken = false } = {}) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "chdoc-"));
  const file = path.join(dir, "coderhelm.config.json");
  fs.writeFileSync(file, JSON.stringify({
    account: ACCOUNT, region: "us-east-1", stage: "prod",
    domain: { hostname: "helm.example.org", hostedZone: "example.org" },
    models: { provider: "bedrock", bedrockInferencePrefix: "us." },
    email: { from: "helm@example.org" }, admins: ["me@example.org"],
  }));
  fs.mkdirSync(path.join(dir, ".coderhelm"));
  fs.writeFileSync(path.join(dir, ".coderhelm", "state.json"), JSON.stringify({ version: "v0.7.0", publicUrl: "https://helm.example.org" }));
  const ns = ["ns-1.awsdns-01.org"];
  const stacks = ["database", "storage", "api", "worker", "streaming", "web", "email", "monitoring"].map((s) => ({ StackName: `coderhelm-prod-${s}`, StackStatus: "UPDATE_COMPLETE" }));
  const runner = new FakeRunner({
    "aws --version": "aws-cli/2.17.0",
    "sts get-caller-identity": { Account: ACCOUNT, Arn: `arn:aws:iam::${ACCOUNT}:user/me` },
    "simulate-principal-policy": { EvaluationResults: C.NEEDED_ACTIONS.map((a) => ({ EvalActionName: a, EvalDecision: broken && a === "iam:PassRole" ? "implicitDeny" : "allowed" })) },
    "get-parameters-by-path": { Parameters: [...C.NEEDED_SERVICES, "bedrock"].map((n) => ({ Name: n, Value: n })) },
    "describe-stacks": { Stacks: [{ StackStatus: "UPDATE_COMPLETE" }] },
    "list-hosted-zones-by-name": { HostedZones: [{ Name: "example.org.", Id: "/hostedzone/Z1", Config: { PrivateZone: false } }] },
    "get-hosted-zone": { DelegationSet: { NameServers: ns } },
    "list-stacks": { StackSummaries: stacks },
    "get-function-configuration": { State: "Active", LastUpdateStatus: "Successful" },
    "get-queue-url": { QueueUrl: "q" },
    "get-queue-attributes": { Attributes: { ApproximateNumberOfMessages: "0" } },
    "get-secret-value": JSON.stringify(JSON.stringify({ ...SECRET, jwt_secret: "j" })),
    "get-email-identity": { VerifiedForSendingStatus: true, DkimAttributes: { Status: "SUCCESS" } },
    "sesv2 get-account": { ProductionAccessEnabled: true },
    "get-foundation-model-availability": { authorizationStatus: "AUTHORIZED" },
    "bedrock-runtime converse": { output: {} },
  });
  const gh = github(broken ? { "/app/hook/deliveries?per_page=10": json(200, [{ status_code: 401, event: "push" }]) } : {});
  const fetchImpl = async (url, init) => (url.startsWith("https://helm.example.org") ? json(url.endsWith("/api/me") ? 401 : 200, {}) : gh.fetchImpl(url, init));
  return { dir, file, runner, fetchImpl, dns: { resolveNs: async () => ns } };
}

test("doctor: everything passes, it says the install is ready, with the URL and next steps", async () => {
  const f = doctorFixture();
  const logs = [];
  const code = await main(["doctor", "--config", f.file], { runner: f.runner, fetchImpl: f.fetchImpl, dns: f.dns, log: (m) => logs.push(String(m)), interactive: false });
  const out = logs.join("\n");
  assert.equal(code, 0, out);
  for (const g of ["This machine and AWS", "Region and CDK", "Web address", "Install health", "GitHub", "Email", "Claude"]) assert.match(out, new RegExp(`\n${g}\n`));
  assert.doesNotMatch(out, /FAIL/);
  assert.match(out, /Your install is ready\.[\s\S]*Dashboard: https:\/\/helm\.example\.org[\s\S]*Sign up as the first admin/);
  const saved = JSON.parse(fs.readFileSync(path.join(f.dir, ".coderhelm", "doctor.json"), "utf8"));
  assert.equal(saved.ok, true);
});

test("doctor: failures exit 1 with each fix; never 'ready'; --json for scripts", async () => {
  const f = doctorFixture({ broken: true });
  const logs = [];
  const code = await main(["doctor", "--config", f.file], { runner: f.runner, fetchImpl: f.fetchImpl, dns: f.dns, log: (m) => logs.push(String(m)), interactive: false });
  const out = logs.join("\n");
  assert.equal(code, 1);
  assert.match(out, /FAIL  AWS permissions for the install\n\s+What's wrong: .*iam:PassRole/);
  assert.match(out, /FAIL  GitHub webhook reaches this install[\s\S]*webhook secret doesn't match/);
  assert.doesNotMatch(out, /Your install is ready/);
  assert.match(out, /Fix what failed above, then check again: npx -y github:CoderHelm\/coderhelm-platform doctor/);

  const j = [];
  await main(["doctor", "--config", f.file, "--json"], { runner: f.runner, fetchImpl: f.fetchImpl, dns: f.dns, log: (m) => j.push(String(m)), interactive: false });
  const report = JSON.parse(j.join("\n"));
  assert.equal(report.ok, false);
  assert.ok(report.groups.flatMap((g) => g.results).some((r) => r.id === "github_webhook" && r.status === "fail"));
});

test("doctor without a config says where to run it or how to fetch the config", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "chdoc-"));
  const logs = [];
  const code = await main(["doctor", "--config", path.join(dir, "coderhelm.config.json")], { log: (m) => logs.push(String(m)), handleErrors: true, interactive: false, runner: new FakeRunner() });
  assert.equal(code, 1);
  const out = logs.join("\n");
  assert.match(out, /pull --stage <name> --region <region>/);
  assert.match(out, /Full details: .*\.coderhelm\/logs\/doctor-.*\.log/);
  assert.doesNotMatch(out, /\n\s+at /, "no stack trace without --verbose");
});

// ── Update preflight and failures ───────────────────────────────────────

test("update preflight: low disk and a broken config stop it with the fix; failures say what to do", async () => {
  const { preflightProblems, afterFailure } = await import("../lib/commands.mjs");
  const cfg = { account: ACCOUNT, region: "us-east-1", stage: "prod", email: { from: "a@b.co" }, admins: ["a@b.co"] };
  assert.deepEqual(preflightProblems(cfg, { freeBytes: 50 * 1024 ** 3 }), []);
  const low = preflightProblems(cfg, { freeBytes: 0.4 * 1024 ** 3 });
  assert.match(low[0].what, /Only 0\.4 GB free/);
  assert.match(low[0].fix, /CODERHELM_HOME/);
  assert.match(preflightProblems({ ...cfg, admins: [] }, { freeBytes: Infinity })[0].what, /admins/);
  const rolled = afterFailure("v0.8.0", "v0.7.0", true);
  assert.match(rolled, /went back to v0\.7\.0\. Your data is untouched/);
  assert.match(rolled, /doctor[\s\S]*update --to v0\.8\.0/);
  assert.match(afterFailure("v0.8.0", "v0.7.0", false), /rollback --to v0\.7\.0/);
  noArrows(rolled);
});

test("update: the preflight runs first and a full disk stops it before anything changes", async () => {
  const f = doctorFixture();
  const logs = [];
  const runner = f.runner;
  await assert.rejects(
    () => main(["update", "--config", f.file, "--to", "v0.8.0", "--yes"], { runner, log: (m) => logs.push(String(m)), interactive: false, freeBytes: 1024 ** 3, resolveSource: async () => { throw new Error("should not download"); } }),
    /can't start: 1 problem above/,
  );
  assert.match(logs.join("\n"), /Before the update\n  FAIL  Only 1\.0 GB free[\s\S]*How to fix:/);
  assert.ok(!runner.lines().some((l) => l.includes("cdk deploy")));
});
