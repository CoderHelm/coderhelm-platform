// Synthesizes whole deployments from configs and checks what each one gets:
// the hosted layout keeps its domains; a fresh self-host config with no
// domain has nothing of the hosted deployment in it and serves everything
// from one CloudFront URL; Bedrock, regions and optional parts follow the
// config.
//
//   npm test   (compiles with tsc, then runs node --test)

import { test } from "node:test";
import * as assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import * as cdk from "aws-cdk-lib";
import { Template } from "aws-cdk-lib/assertions";
import { buildApp } from "../lib/app";
import { CoderhelmConfig, loadConfigFile, resolveConfig, ResolvedConfig } from "../lib/config";
import { API_PATHS, STREAM_PATH, runtimeConfigScript } from "../lib/web-stack";

const INFRA = path.join(__dirname, "..", "..");

// Stub Lambda code: synth only hashes the directories.
const STUBS = fs.mkdtempSync(path.join(os.tmpdir(), "chsynth-"));
for (const d of ["gateway", "worker", "mcp"]) {
  fs.mkdirSync(path.join(STUBS, d), { recursive: true });
  fs.writeFileSync(path.join(STUBS, d, d === "mcp" ? "handler.js" : "bootstrap"), "stub");
}
process.env.GATEWAY_ZIP = path.join(STUBS, "gateway");
process.env.WORKER_ZIP = path.join(STUBS, "worker");
process.env.MCP_PROXY_DIR = path.join(STUBS, "mcp");
delete process.env.MODEL_ID;
delete process.env.LIGHT_MODEL_ID;

function zoneContext(account: string, region: string, zone: string) {
  return { [`hosted-zone:account=${account}:domainName=${zone}:region=${region}`]: { Id: "/hostedzone/ZTEST000001", Name: `${zone}.` } };
}

/** Every stack's template, by stack name. */
function synth(cfg: ResolvedConfig, context: Record<string, unknown> = {}): Record<string, Template> {
  const app = new cdk.App({ outdir: fs.mkdtempSync(path.join(os.tmpdir(), "chout-")), context });
  buildApp(app, cfg);
  const out: Record<string, Template> = {};
  for (const s of app.node.children) {
    if (s instanceof cdk.Stack) out[s.stackName] = Template.fromStack(s);
  }
  return out;
}

function json(t: Template): string {
  return JSON.stringify(t.toJSON());
}

function envOf(t: Template, functionName: string): Record<string, unknown> {
  const fns = t.findResources("AWS::Lambda::Function");
  for (const r of Object.values(fns)) {
    const name = (r as any).Properties.FunctionName;
    if (name === functionName) return (r as any).Properties.Environment.Variables;
  }
  throw new Error(`no function ${functionName}`);
}

function selfHost(over: Partial<CoderhelmConfig> = {}): ResolvedConfig {
  return resolveConfig({
    account: "111122223333",
    region: "us-west-2",
    stage: "prod",
    email: { from: "coderhelm@example.org" },
    admins: ["owner@example.org"],
    ...over,
  });
}

test("hosted prod: the same stacks as before, with its domains, and no web stack", () => {
  const cfg = loadConfigFile(path.join(INFRA, "config", "hosted.prod.json"), { CDK_DEFAULT_ACCOUNT: "654654210434" });
  const t = synth(cfg, zoneContext("654654210434", "us-east-1", "coderhelm.com"));
  assert.deepEqual(Object.keys(t).sort(), [
    "coderhelm-prod-api",
    "coderhelm-prod-database",
    "coderhelm-prod-email",
    "coderhelm-prod-log-analyzer",
    "coderhelm-prod-monitoring",
    "coderhelm-prod-storage",
    "coderhelm-prod-streaming",
    "coderhelm-prod-worker",
  ]);
  const gw = envOf(t["coderhelm-prod-api"], "coderhelm-prod-gateway");
  assert.equal(gw.APP_URL, "https://app.coderhelm.com");
  assert.equal(gw.API_URL, "https://api.coderhelm.com");
  assert.equal(gw.SES_FROM_ADDRESS, "noreply@coderhelm.com");
  assert.equal(gw.PUBLIC_URL_PARAMETER, undefined);
  t["coderhelm-prod-api"].hasResourceProperties("AWS::ApiGatewayV2::DomainName", { DomainName: "api.coderhelm.com" });
  t["coderhelm-prod-api"].resourceCountIs("AWS::Cognito::UserPoolIdentityProvider", 1);
  t["coderhelm-prod-worker"].hasResourceProperties("AWS::IAM::Role", { RoleName: "coderhelm-prod-reviewer-evals" });
  t["coderhelm-prod-storage"].hasResourceProperties("AWS::S3::Bucket", { BucketName: "coderhelm-prod-data" });
  t["coderhelm-prod-log-analyzer"].hasResourceProperties("AWS::S3::Bucket", { BucketName: "coderhelm-public" });
  // No allowlist entries come from the hosted config.
  assert.ok(!json(t["coderhelm-prod-database"]).includes("batchWriteItem"));
});

test("fresh self-host config without a domain: nothing hosted-specific, one CloudFront URL", () => {
  const cfg = selfHost();
  const t = synth(cfg);
  assert.ok(t["coderhelm-prod-web"], "web stack");
  for (const [name, tpl] of Object.entries(t)) {
    const s = json(tpl);
    assert.ok(!s.includes("coderhelm.com"), `${name} mentions the hosted domain`);
    assert.ok(!s.includes("654654210434"), `${name} mentions the hosted account`);
    assert.ok(!s.includes("repo:CoderHelm/coderhelm-platform"), `${name} trusts the hosted repository's CI`);
    assert.ok(!s.includes("AWS::Route53::RecordSet"), `${name} creates DNS records`);
    assert.ok(!s.includes("AWS::CertificateManager::Certificate"), `${name} creates certificates`);
  }

  // Globally unique names carry the account.
  t["coderhelm-prod-storage"].hasResourceProperties("AWS::S3::Bucket", { BucketName: "coderhelm-prod-data-111122223333" });
  t["coderhelm-prod-worker"].hasResourceProperties("AWS::S3::Bucket", { BucketName: "coderhelm-prod-sandbox-111122223333" });
  t["coderhelm-prod-api"].hasResourceProperties("AWS::Cognito::UserPoolDomain", { Domain: "coderhelm-prod-111122223333" });

  // The services find their URL in SSM, and may read it.
  for (const [stack, fn] of [
    ["coderhelm-prod-api", "coderhelm-prod-gateway"],
    ["coderhelm-prod-worker", "coderhelm-prod-worker"],
    ["coderhelm-prod-streaming", "coderhelm-prod-streaming"],
  ]) {
    const env = envOf(t[stack], fn);
    assert.equal(env.APP_URL, undefined, fn);
    assert.equal(env.PUBLIC_URL_PARAMETER, "/coderhelm-prod/public-url", fn);
    assert.equal(env.PLATFORM_ADMIN_EMAILS, "owner@example.org", fn);
    assert.equal(env.SITE_URL, "", fn);
    assert.equal(env.MODEL_PROVIDER, undefined, fn);
    assert.ok(json(t[stack]).includes("parameter/coderhelm-prod/public-url"), `${fn} may read the parameter`);
  }
  t["coderhelm-prod-web"].hasResourceProperties("AWS::SSM::Parameter", { Name: "/coderhelm-prod/public-url" });

  // One distribution: the stream first, then the API paths, then the dashboard.
  const dist = Object.values(t["coderhelm-prod-web"].findResources("AWS::CloudFront::Distribution"))[0] as any;
  const paths = dist.Properties.DistributionConfig.CacheBehaviors.map((b: any) => b.PathPattern);
  assert.deepEqual(paths, [STREAM_PATH, ...API_PATHS]);
  assert.equal(dist.Properties.DistributionConfig.Aliases, undefined);

  // No Google sign-in, no OAuth client, Cognito's own email.
  t["coderhelm-prod-api"].resourceCountIs("AWS::Cognito::UserPoolIdentityProvider", 0);
  const pool = Object.values(t["coderhelm-prod-api"].findResources("AWS::Cognito::UserPool"))[0] as any;
  assert.equal(pool.Properties.EmailConfiguration.EmailSendingAccount, "COGNITO_DEFAULT");
  // Same origin: no CORS on the API.
  const api = Object.values(t["coderhelm-prod-api"].findResources("AWS::ApiGatewayV2::Api"))[0] as any;
  assert.equal(api.Properties.CorsConfiguration, undefined);

  // Hosted-only pieces are absent.
  assert.equal(t["coderhelm-prod-worker"].findResources("AWS::IAM::Role", { Properties: { RoleName: "coderhelm-prod-reviewer-evals" } }) && Object.keys(t["coderhelm-prod-worker"].findResources("AWS::IAM::Role", { Properties: { RoleName: "coderhelm-prod-reviewer-evals" } })).length, 0);
  assert.equal(Object.keys(t["coderhelm-prod-log-analyzer"].findResources("AWS::S3::Bucket")).length, 0);

  // The admin can sign up.
  assert.ok(json(t["coderhelm-prod-database"]).includes("EMAIL#owner@example.org"));

  // The email identity is the sender's domain, and emails link to the CloudFront URL.
  t["coderhelm-prod-email"].hasResourceProperties("AWS::SES::EmailIdentity", { EmailIdentity: "example.org" });
  assert.ok(json(t["coderhelm-prod-email"]).includes("Fn::ImportValue"), "email templates link to the web stack's URL");
});

test("self-host with a domain in another region and Bedrock", () => {
  const cfg = selfHost({
    region: "eu-west-1",
    domain: { hostname: "helm.example.org", hostedZone: "example.org" },
    models: { provider: "bedrock", bedrockInferencePrefix: "eu." },
    email: { from: "helm@example.org" },
    integrations: { googleSignIn: true },
  });
  const t = synth(cfg, {
    ...zoneContext("111122223333", "eu-west-1", "example.org"),
    ...zoneContext("111122223333", "us-east-1", "example.org"),
  });
  // CloudFront's certificate lives in us-east-1.
  assert.ok(t["coderhelm-prod-web-certificate"], "certificate stack");
  t["coderhelm-prod-web-certificate"].hasResourceProperties("AWS::CertificateManager::Certificate", { DomainName: "helm.example.org" });
  t["coderhelm-prod-web"].resourceCountIs("AWS::Route53::RecordSet", 2);

  const gw = envOf(t["coderhelm-prod-api"], "coderhelm-prod-gateway");
  assert.equal(gw.APP_URL, "https://helm.example.org");
  assert.equal(gw.MODEL_PROVIDER, "bedrock");
  assert.equal(gw.BEDROCK_INFERENCE_PREFIX, "eu.");
  const worker = json(t["coderhelm-prod-worker"]);
  assert.ok(worker.includes("bedrock:InvokeModelWithResponseStream"));
  assert.ok(worker.includes("foundation-model/anthropic.*"));

  // Google sign-in calls back to the API on the one hostname.
  t["coderhelm-prod-api"].resourceCountIs("AWS::Cognito::UserPoolIdentityProvider", 1);
  t["coderhelm-prod-api"].hasResourceProperties("AWS::Cognito::UserPoolClient", {
    CallbackURLs: ["https://helm.example.org/auth/google/callback"],
  });
  // The sender's domain is the zone: SES gets its DKIM records there.
  t["coderhelm-prod-email"].resourceCountIs("AWS::Route53::RecordSet", 3);
});

test("optional parts can be turned off", () => {
  const t = synth(selfHost({ integrations: { sandbox: false, logAnalyzer: false, weeklySummary: false } }));
  t["coderhelm-prod-worker"].resourceCountIs("AWS::CodeBuild::Project", 0);
  assert.equal(t["coderhelm-prod-log-analyzer"], undefined);
  const env = envOf(t["coderhelm-prod-worker"], "coderhelm-prod-worker");
  assert.equal(env.SANDBOX_PROJECT_NAME, undefined);
  assert.equal(env.WEEKLY_SUMMARY_ENABLED, "false");
  t["coderhelm-prod-api"].hasResourceProperties("AWS::Events::Rule", { Name: "coderhelm-prod-weekly-summary", State: "DISABLED" });
});

test("the sandbox build role has no S3 access: builds use the URLs the worker signs", () => {
  const t = synth(selfHost())["coderhelm-prod-worker"];
  const projects = t.findResources("AWS::CodeBuild::Project");
  assert.equal(Object.keys(projects).length, 1);
  const role = JSON.stringify(Object.values(projects)[0].Properties.ServiceRole);
  const roleId = role.match(/"Fn::GetAtt":\["([^"]+)"/)?.[1];
  assert.ok(roleId, role);
  for (const [id, pol] of Object.entries(t.findResources("AWS::IAM::Policy"))) {
    if (!JSON.stringify(pol.Properties.Roles ?? []).includes(roleId!)) continue;
    const actions = JSON.stringify(pol.Properties.PolicyDocument.Statement.map((st: { Action: unknown }) => st.Action));
    assert.ok(!/s3:/i.test(actions), `${id} gives the sandbox build role S3 actions: ${actions}`);
  }
  // The buildspec fetches and uploads through presigned URLs, never the CLI.
  const spec = JSON.stringify(Object.values(projects)[0].Properties.Source);
  assert.ok(spec.includes("SANDBOX_GET_URL") && spec.includes("SANDBOX_PUT_URL"), spec);
  assert.ok(!spec.includes("aws s3 cp"), spec);
});

test("a dashboard build is uploaded with the runtime config", () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "chdash-"));
  fs.writeFileSync(path.join(dir, "index.html"), "<html></html>");
  fs.mkdirSync(path.join(dir, "_next"));
  fs.writeFileSync(path.join(dir, "_next", "a.js"), "x");
  const t = synth(selfHost({ dashboard: { assetDir: dir } }));
  const deployments = t["coderhelm-prod-web"].findResources("Custom::CDKBucketDeployment");
  assert.equal(Object.keys(deployments).length, 2);
  assert.throws(() => synth(selfHost({ dashboard: { assetDir: STUBS } })), /has no index.html/);
});

test("updates: on by default for self-host, with a least-privilege build role; off removes them", () => {
  const cfg = selfHost({ updates: { auto: true, window: "Sat 22:00", timezone: "Europe/Berlin", majors: "ask", webhookUrl: "https://hooks.example.org/x" } });
  const t = synth(cfg);
  const u = t["coderhelm-prod-updates"];
  assert.ok(u, "updates stack");
  u.hasResourceProperties("AWS::CodeBuild::Project", { Name: "coderhelm-prod-update", ConcurrentBuildLimit: 1 });
  u.hasResourceProperties("AWS::Events::Rule", { Name: "coderhelm-prod-update-check", ScheduleExpression: "rate(1 hour)" });
  const updater = envOf(u, "coderhelm-prod-updater");
  assert.deepEqual(JSON.parse(updater.UPDATE_DEFAULTS as string), { auto: true, channel: "stable", day: "sat", hour: 22, timezone: "Europe/Berlin", majors: "ask" });
  assert.equal(updater.ADMIN_EMAILS, "owner@example.org");
  assert.equal(updater.UPDATE_WEBHOOK_URL, "https://hooks.example.org/x");
  assert.ok(updater.CODERHELM_CONFIG_FINGERPRINT);
  // The build deploys only through the CDK bootstrap roles: no admin policy.
  const role = Object.values(u.findResources("AWS::IAM::Role", { Properties: { RoleName: "coderhelm-prod-update-build" } }))[0] as any;
  assert.equal(role.Properties.ManagedPolicyArns, undefined);
  const policies = JSON.stringify(u.findResources("AWS::IAM::Policy"));
  assert.ok(policies.includes(":role/cdk-*"));
  assert.ok(!policies.includes('"Action":"*"'));
  // The build checks the release signature before installing it.
  const spec = JSON.stringify(u.findResources("AWS::CodeBuild::Project"));
  assert.ok(spec.includes("cosign verify-blob"));
  assert.ok(spec.includes("--require-signature --auto-rollback"));
  // The gateway knows the updater and may call it.
  assert.equal(envOf(t["coderhelm-prod-api"], "coderhelm-prod-gateway").UPDATER_FUNCTION_NAME, "coderhelm-prod-updater");
  assert.ok(json(t["coderhelm-prod-api"]).includes("function:coderhelm-prod-updater"));

  const off = synth(selfHost({ updates: { enabled: false } }));
  assert.equal(off["coderhelm-prod-updates"], undefined);
  assert.equal(envOf(off["coderhelm-prod-api"], "coderhelm-prod-gateway").UPDATER_FUNCTION_NAME, undefined);
});

test("the release stamp reaches the services and the dashboard", () => {
  process.env.CODERHELM_VERSION = "v1.2.3";
  process.env.CODERHELM_COMMIT = "abc1234";
  try {
    const t = synth(selfHost());
    const gw = envOf(t["coderhelm-prod-api"], "coderhelm-prod-gateway");
    assert.equal(gw.CODERHELM_VERSION, "v1.2.3");
    assert.equal(gw.CODERHELM_COMMIT, "abc1234");
    assert.match(String(gw.CODERHELM_CONFIG_FINGERPRINT), /^[0-9a-f]{12}$/);
    assert.match(runtimeConfigScript(selfHost()), /"version":"v1\.2\.3"/, "runtime-config.js carries the version");
  } finally {
    delete process.env.CODERHELM_VERSION;
    delete process.env.CODERHELM_COMMIT;
  }
  // The hosted deployment is not stamped (its templates stay as they are).
  const hosted = loadConfigFile(path.join(INFRA, "config", "hosted.prod.json"), { CDK_DEFAULT_ACCOUNT: "654654210434" });
  assert.equal(hosted.updates.enabled, false);
});

/** Does an HTTP API route key (`GET /api/{proxy+}`) take this request? */
function routeTakes(routeKey: string, method: string, requestPath: string): boolean {
  const [m, pattern] = routeKey.split(" ");
  if (m !== method && m !== "ANY") return false;
  const want = pattern.split("/");
  const got = requestPath.split("/");
  for (let i = 0; i < want.length; i++) {
    if (want[i] === "{proxy+}") return got.length > i;
    if (got[i] === undefined) return false;
    if (/^\{[^}]+\}$/.test(want[i])) continue;
    if (want[i] !== got[i]) return false;
  }
  return want.length === got.length;
}

test("every public API path, and the OpenAPI document itself, has an HTTP API route to the gateway", () => {
  const cfg = loadConfigFile(path.join(INFRA, "config", "hosted.prod.json"), { CDK_DEFAULT_ACCOUNT: "654654210434" });
  const api = synth(cfg, zoneContext("654654210434", "us-east-1", "coderhelm.com"))["coderhelm-prod-api"];
  const keys = Object.values(api.findResources("AWS::ApiGatewayV2::Route")).map((r: any) => String(r.Properties.RouteKey));
  const spec = JSON.parse(fs.readFileSync(path.join(INFRA, "..", "docs", "api", "openapi.json"), "utf8"));
  const wanted: [string, string][] = [["GET", "/openapi.json"]];
  for (const [p, item] of Object.entries<Record<string, unknown>>(spec.paths)) {
    for (const m of Object.keys(item)) {
      // A path parameter stands for one segment: fill it with a sample value.
      wanted.push([m.toUpperCase(), p.replace(/\{[^}]+\}/g, "x")]);
    }
  }
  const missing = wanted.filter(([m, p]) => !keys.some((k) => routeTakes(k, m, p)));
  assert.deepEqual(missing, [], `no HTTP API route for: ${missing.map((w) => w.join(" ")).join(", ")}`);
  // The matcher itself.
  assert.ok(routeTakes("GET /api/{proxy+}", "GET", "/api/runs/x"));
  assert.ok(!routeTakes("GET /api/{proxy+}", "GET", "/api"));
  assert.ok(!routeTakes("POST /webhooks/github", "POST", "/webhooks/ci/tests/results"));
});

test("self-host serves the OpenAPI document through CloudFront", () => {
  assert.ok(API_PATHS.includes("/openapi.json"));
});

test("the manifest landing: self-host only, may write only this install's secret", () => {
  const t = synth(selfHost({ models: { provider: "bedrock" } }));
  const api = t["coderhelm-prod-api"];
  const gw = envOf(api, "coderhelm-prod-gateway");
  assert.equal(gw.SETUP_ENDPOINTS, "1");
  // Every service knows it is self-hosted, so no link falls back to the hosted addresses.
  for (const [stack, fn] of [["coderhelm-prod-api", "coderhelm-prod-gateway"], ["coderhelm-prod-worker", "coderhelm-prod-worker"]] as const) {
    assert.equal(envOf(t[stack], fn).SELF_HOSTED, "1", fn);
  }
  const policies = JSON.stringify(api.findResources("AWS::IAM::Policy"));
  assert.ok(policies.includes("SetupStoresGithubAppKeys"));
  assert.ok(policies.includes("secret:coderhelm/prod/secrets-??????"), "only this install's secret");

  // The updater may restart exactly the functions that read the secret.
  const u = t["coderhelm-prod-updates"];
  assert.equal(envOf(u, "coderhelm-prod-updater").REFRESH_FUNCTIONS, "coderhelm-prod-gateway,coderhelm-prod-streaming,coderhelm-prod-worker");
  const up = JSON.stringify(u.findResources("AWS::IAM::Policy"));
  assert.ok(up.includes("RestartSecretReaders"));
  assert.ok(up.includes("function:coderhelm-prod-worker"));

  const hostedCfg = loadConfigFile(path.join(INFRA, "config", "hosted.prod.json"), { CDK_DEFAULT_ACCOUNT: "654654210434" });
  const hosted = synth(hostedCfg, zoneContext("654654210434", "us-east-1", "coderhelm.com"));
  assert.equal(envOf(hosted["coderhelm-prod-api"], "coderhelm-prod-gateway").SETUP_ENDPOINTS, undefined);
  assert.equal(envOf(hosted["coderhelm-prod-api"], "coderhelm-prod-gateway").SELF_HOSTED, undefined);
  assert.ok(!JSON.stringify(hosted["coderhelm-prod-api"].findResources("AWS::IAM::Policy")).includes("SetupStoresGithubAppKeys"));
  // The install guide reads launch/latest.json from the public bucket in the browser.
  hosted["coderhelm-prod-log-analyzer"].hasResourceProperties("AWS::S3::Bucket", {
    BucketName: "coderhelm-public",
    CorsConfiguration: { CorsRules: [{ AllowedMethods: ["GET", "HEAD"], AllowedOrigins: ["*"] }] },
  });
});

test("the release publisher: only v* tag builds of the platform repo, only launch/* in the public bucket", () => {
  const cfg = loadConfigFile(path.join(INFRA, "config", "hosted.prod.json"), { CDK_DEFAULT_ACCOUNT: "654654210434" });
  const t = synth(cfg, zoneContext("654654210434", "us-east-1", "coderhelm.com"));
  const w = t["coderhelm-prod-worker"];
  const role = Object.values(w.findResources("AWS::IAM::Role", { Properties: { RoleName: "coderhelm-prod-release-publisher" } }))[0] as any;
  assert.ok(role, "the role exists on hosted");
  assert.equal(role.Properties.MaxSessionDuration, 3600);
  const [st] = role.Properties.AssumeRolePolicyDocument.Statement;
  assert.equal(st.Action, "sts:AssumeRoleWithWebIdentity");
  assert.deepEqual(st.Condition, {
    StringEquals: { "token.actions.githubusercontent.com:aud": "sts.amazonaws.com" },
    StringLike: { "token.actions.githubusercontent.com:sub": "repo:CoderHelm/coderhelm-platform:ref:refs/tags/v*" },
  });
  const policies = Object.values(w.findResources("AWS::IAM::Policy")) as any[];
  const mine = policies.filter((p) => JSON.stringify(p.Properties.Roles).includes("ReleasePublisherRole"));
  assert.equal(mine.length, 1);
  assert.deepEqual(mine[0].Properties.PolicyDocument.Statement, [
    { Action: "s3:PutObject", Effect: "Allow", Resource: "arn:aws:s3:::coderhelm-public/launch/*" },
  ]);
  w.hasOutput("ReleasePublisherRoleArn", {});
  // Self-host installs have no publisher.
  const sh = synth(selfHost());
  assert.ok(!json(sh["coderhelm-prod-worker"]).includes("release-publisher"));
});

// ── Your own domain (docs/self-host/domains.md) ────────────────────────────

const CERT = "arn:aws:acm:us-east-1:111122223333:certificate/0f0e0d0c-1111-2222-3333-444455556666";

function outputs(t: Template): Record<string, any> {
  return (t.toJSON() as any).Outputs ?? {};
}

test("domain: none, the CloudFront address, and its hostname kept for a domain added later", () => {
  const t = synth(selfHost());
  const web = t["coderhelm-prod-web"];
  web.hasResourceProperties("AWS::SSM::Parameter", { Name: "/coderhelm-prod/distribution-domain" });
  web.resourceCountIs("AWS::Route53::RecordSet", 0);
  web.resourceCountIs("AWS::CertificateManager::Certificate", 0);
  assert.equal(outputs(web).DnsRecord, undefined);
  assert.equal(t["coderhelm-prod-web-certificate"], undefined);
  assert.equal(envOf(t["coderhelm-prod-api"], "coderhelm-prod-gateway").GOOGLE_SIGN_IN, "false");
});

test("domain: Route 53 in us-east-1, certificate and records made by the deploy", () => {
  const cfg = selfHost({ region: "us-east-1", domain: { hostname: "helm.example.org", hostedZone: "example.org" } });
  const t = synth(cfg, zoneContext("111122223333", "us-east-1", "example.org"));
  const web = t["coderhelm-prod-web"];
  web.hasResourceProperties("AWS::CertificateManager::Certificate", { DomainName: "helm.example.org", ValidationMethod: "DNS" });
  web.resourceCountIs("AWS::Route53::RecordSet", 2); // A and AAAA aliases
  web.hasResourceProperties("AWS::CloudFront::Distribution", { DistributionConfig: { Aliases: ["helm.example.org"] } });
  assert.equal(t["coderhelm-prod-web-certificate"], undefined, "no separate certificate stack in us-east-1");
  assert.equal(outputs(web).DnsRecord, undefined);
});

test("domain: another DNS provider, the certificate given, no records, the record to add as an output", () => {
  const cfg = selfHost({ domain: { hostname: "helm.example.org", certificateArn: CERT } });
  const t = synth(cfg);
  const web = t["coderhelm-prod-web"];
  web.resourceCountIs("AWS::CertificateManager::Certificate", 0);
  web.resourceCountIs("AWS::Route53::RecordSet", 0);
  web.hasResourceProperties("AWS::CloudFront::Distribution", {
    DistributionConfig: { Aliases: ["helm.example.org"], ViewerCertificate: { AcmCertificateArn: CERT } },
  });
  assert.ok(outputs(web).DnsRecord, "DnsRecord output");
  assert.ok(JSON.stringify(outputs(web).DnsRecord).includes("helm.example.org CNAME "));
  assert.equal(t["coderhelm-prod-web-certificate"], undefined);
  assert.equal(envOf(t["coderhelm-prod-api"], "coderhelm-prod-gateway").APP_URL, "https://helm.example.org");
});

test("domain: added later in Route 53 (the updater's certificate and the zone id): records, no lookup, no certificate stack", () => {
  const cfg = selfHost({
    region: "eu-west-1",
    domain: { hostname: "helm.example.org", hostedZone: "example.org", hostedZoneId: "Z0123456789ABC", certificateArn: CERT },
  });
  // No zone context: the zone id skips the lookup.
  const t = synth(cfg);
  const web = t["coderhelm-prod-web"];
  web.resourceCountIs("AWS::CertificateManager::Certificate", 0);
  web.resourceCountIs("AWS::Route53::RecordSet", 2);
  web.hasResourceProperties("AWS::Route53::RecordSet", { HostedZoneId: "Z0123456789ABC", Name: "helm.example.org.", Type: "A" });
  assert.equal(t["coderhelm-prod-web-certificate"], undefined);
  assert.equal(outputs(web).DnsRecord, undefined);
});

test("domain: the updater may request the certificate, add only the validation CNAME, and checks every 5 minutes", () => {
  const t = synth(selfHost({ integrations: { googleSignIn: false } }));
  const u = t["coderhelm-prod-updates"];
  u.hasResourceProperties("AWS::Events::Rule", { Name: "coderhelm-prod-domain-check", ScheduleExpression: "rate(5 minutes)" });
  const updater = envOf(u, "coderhelm-prod-updater");
  assert.equal(updater.DISTRIBUTION_DOMAIN_PARAMETER, "/coderhelm-prod/distribution-domain");
  const policies = JSON.stringify(u.findResources("AWS::IAM::Policy"));
  assert.ok(policies.includes("acm:RequestCertificate"));
  assert.ok(policies.includes('"aws:RequestedRegion":"us-east-1"'));
  assert.ok(policies.includes("route53:ChangeResourceRecordSetsRecordTypes"));
  assert.ok(policies.includes('"_*"'));
  assert.ok(policies.includes("parameter/coderhelm-prod/distribution-domain"));
  // The build applies a domain change with the release that runs.
  const spec = JSON.stringify(u.findResources("AWS::CodeBuild::Project"));
  assert.ok(spec.includes("coderhelm-selfhost domain apply --yes --require-signature"));
});

test("CORS: the gateway answers browsers, not API Gateway; the docs origin never gets credentials", () => {
  // Hosted (split): no API Gateway CORS setting (it would drop the gateway's
  // own headers), the dashboard's origins reach the gateway, and every
  // preflight is routed to it.
  const hostedCfg = loadConfigFile(path.join(INFRA, "config", "hosted.prod.json"), { CDK_DEFAULT_ACCOUNT: "654654210434" });
  const hosted = synth(hostedCfg, zoneContext("654654210434", "us-east-1", "coderhelm.com"))["coderhelm-prod-api"];
  const api = Object.values(hosted.findResources("AWS::ApiGatewayV2::Api"))[0] as any;
  assert.equal(api.Properties.CorsConfiguration, undefined);
  const keys = Object.values(hosted.findResources("AWS::ApiGatewayV2::Route")).map((r: any) => String(r.Properties.RouteKey));
  assert.ok(keys.includes("OPTIONS /{proxy+}"), "preflights reach the gateway");
  assert.ok(keys.some((k) => routeTakes(k, "GET", "/hybrid/names/acme")), "the name check has a route");
  const env = envOf(hosted, "coderhelm-prod-gateway");
  assert.equal(env.CORS_ORIGINS, hostedCfg.split!.corsOrigins.join(","));
  assert.ok(!String(env.CORS_ORIGINS).includes("docs.coderhelm.com"), "the docs origin is never a credentialed origin");

  // Self-host (single-origin): nothing at API Gateway or CloudFront touches
  // CORS, so the setup routes' own `*` reaches the docs install guide.
  const t = synth(selfHost());
  const selfApi = Object.values(t["coderhelm-prod-api"].findResources("AWS::ApiGatewayV2::Api"))[0] as any;
  assert.equal(selfApi.Properties.CorsConfiguration, undefined);
  assert.equal(envOf(t["coderhelm-prod-api"], "coderhelm-prod-gateway").CORS_ORIGINS, undefined);
  const dist = Object.values(t["coderhelm-prod-web"].findResources("AWS::CloudFront::Distribution"))[0] as any;
  for (const b of dist.Properties.DistributionConfig.CacheBehaviors) {
    if (b.PathPattern === STREAM_PATH) continue;
    assert.equal(b.ResponseHeadersPolicyId, undefined, `${b.PathPattern} rewrites the API's headers`);
  }
  assert.ok(API_PATHS.includes("/api/*"), "the setup routes go through the API behavior");
});
