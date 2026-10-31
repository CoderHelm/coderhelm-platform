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
import { Match, Template } from "aws-cdk-lib/assertions";
import { buildApp } from "../lib/app";
import { CoderhelmConfig, loadConfigFile, resolveConfig, ResolvedConfig } from "../lib/config";
import { API_PATHS, STREAM_PATH, runtimeConfigScript } from "../lib/web-stack";
import { releaseCheckBuildSpec, updateBuildSpec } from "../lib/updates-stack";
import { CHECK_VERSION, COSIGN_LINUX_AMD64_SHA256, VERIFY_SUMS, verifyTagCommands } from "../lib/release-verify";
import { accessNames, deployPolicy } from "../lib/access";

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
  // The evals call the models the way the worker does (streaming), on the
  // eval, reviewer and candidate models only.
  {
    const evalPolicy = JSON.stringify(
      Object.values(t["coderhelm-prod-worker"].findResources("AWS::IAM::Policy")).filter((p: any) =>
        JSON.stringify(p.Properties.Roles).includes("ReviewerEvalsRole"),
      ),
    );
    assert.ok(evalPolicy.includes("bedrock:InvokeModelWithResponseStream"));
    for (const m of ["claude-haiku-4-5-20251001-v1:0", "claude-opus-5-5", "claude-sonnet-5-5"]) {
      assert.ok(evalPolicy.includes(`inference-profile/us.anthropic.${m}`), m);
      assert.ok(evalPolicy.includes(`foundation-model/anthropic.${m}`), m);
    }
    assert.ok(!evalPolicy.includes("anthropic.*"), "named models only");
  }
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

  // Google sign-in on in an old config: a self-hosted install never gets it,
  // and the update removes the provider it had.
  t["coderhelm-prod-api"].resourceCountIs("AWS::Cognito::UserPoolIdentityProvider", 0);
  const gw = envOf(t["coderhelm-prod-api"], "coderhelm-prod-gateway");
  assert.equal(gw.GOOGLE_SIGN_IN, "false");
  assert.equal(gw.APP_URL, "https://helm.example.org");
  assert.equal(gw.MODEL_PROVIDER, "bedrock");
  assert.equal(gw.BEDROCK_INFERENCE_PREFIX, "eu.");
  const worker = json(t["coderhelm-prod-worker"]);
  assert.ok(worker.includes("bedrock:InvokeModelWithResponseStream"));
  assert.ok(worker.includes("foundation-model/anthropic.*"));

  // The sender's domain is the zone: SES gets its DKIM records there.
  t["coderhelm-prod-email"].resourceCountIs("AWS::Route53::RecordSet", 3);
});

test("optional parts can be turned off", () => {
  const t = synth(selfHost({ integrations: { sandbox: false, logAnalyzer: false, weeklySummary: false } }));
  t["coderhelm-prod-worker"].resourceCountIs("AWS::CodeBuild::Project", 0);
  assert.equal(t["coderhelm-prod-log-analyzer"], undefined);
  const env = envOf(t["coderhelm-prod-worker"], "coderhelm-prod-worker");
  assert.equal(env.SANDBOX_PROJECT_NAME, undefined);
  // The weekly summary is the install admin's choice at runtime (Settings,
  // Emails): the schedule stays, and the config starts it off.
  assert.equal(env.WEEKLY_SUMMARY_ENABLED, "true");
  assert.equal(env.OPTIONAL_EMAIL_DEFAULTS, "");
  t["coderhelm-prod-api"].hasResourceProperties("AWS::Events::Rule", { Name: "coderhelm-prod-weekly-summary", State: "ENABLED" });
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
  const commands: string[] = [...Object.values((updateBuildSpec(cfg) as any).spec.phases)].flatMap((p: any) => p.commands);
  const verifyAt = commands.indexOf(VERIFY_SUMS);
  assert.ok(verifyAt >= 0, "exact identity: release.yml on refs/tags/$VERSION");
  assert.ok(commands.indexOf(CHECK_VERSION) >= 0 && commands.indexOf(CHECK_VERSION) < verifyAt, "VERSION must be a release tag first");
  assert.ok(verifyAt < commands.findIndex((c) => c.startsWith("npm install")), "verified before the installer is installed");
  const tagAt = commands.indexOf(verifyTagCommands()[0]);
  assert.ok(tagAt > 0 && tagAt < verifyAt, "the tag (maintainer key, on main) is checked before the signature");
  for (const line of verifyTagCommands()) assert.ok(commands.includes(line), line);
  assert.doesNotMatch(commands.join("\n"), /identity-regexp|insecure-ignore|ignore-tlog/);
  assert.ok(commands.some((c) => c.includes(COSIGN_LINUX_AMD64_SHA256)), "cosign pinned by its hash");
  assert.ok(commands.includes("npm install -g --ignore-scripts ./coderhelm-selfhost.tgz"));
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

test("AWS connections: a self-hosted install's roles trust its own account, from its own release's template", () => {
  process.env.CODERHELM_VERSION = "v1.2.3";
  try {
    const t = synth(selfHost());
    for (const [stack, fn] of [
      ["coderhelm-prod-api", "coderhelm-prod-gateway"],
      ["coderhelm-prod-worker", "coderhelm-prod-worker"],
      ["coderhelm-prod-streaming", "coderhelm-prod-streaming"],
    ]) {
      const env = envOf(t[stack], fn);
      // This install's own account (the stack's), never CoderHelm's.
      assert.equal(env.CODERHELM_ACCOUNT_ID, "111122223333", fn);
      assert.equal(env.LOG_READER_TEMPLATE_URL, "https://coderhelm-public.s3.amazonaws.com/launch/v1.2.3/coderhelm-log-reader.yaml", fn);
      assert.equal(env.RESOURCE_PREFIX, "coderhelm-prod", fn);
    }
    const analyzer = envOf(t["coderhelm-prod-log-analyzer"], "coderhelm-prod-log-analyzer");
    assert.equal(analyzer.CODERHELM_ACCOUNT_ID, "111122223333");
  } finally {
    delete process.env.CODERHELM_VERSION;
  }
  // A development build (no release) and an operator's own template.
  const dev = envOf(synth(selfHost())["coderhelm-prod-api"], "coderhelm-prod-gateway");
  assert.equal(dev.LOG_READER_TEMPLATE_URL, "https://coderhelm-public.s3.amazonaws.com/cfn/coderhelm-log-reader.yaml");
  const own = envOf(synth(selfHost({ logReaderTemplateUrl: "https://my-bucket.s3.amazonaws.com/coderhelm-log-reader.yaml" }))["coderhelm-prod-api"], "coderhelm-prod-gateway");
  assert.equal(own.LOG_READER_TEMPLATE_URL, "https://my-bucket.s3.amazonaws.com/coderhelm-log-reader.yaml");

  // The gateway, the worker and the analyzer may take on a team's role
  // (in any account, the install's own included), and nothing else of it.
  const t = synth(selfHost());
  for (const stack of ["coderhelm-prod-api", "coderhelm-prod-worker", "coderhelm-prod-log-analyzer"]) {
    const s = json(t[stack]);
    assert.ok(s.includes("arn:aws:iam::*:role/CoderHelmLogReader"), `${stack} may assume the log reader role`);
  }
  // The role's name prefix the trust policy is narrowed to matches the
  // stacks whose generated role names start with it.
  for (const name of Object.keys(t)) assert.ok(name.startsWith("coderhelm-prod-"), name);
});

test("AWS connections: the role and update templates are released with each version", () => {
  const wf = fs.readFileSync(path.join(INFRA, "..", ".github", "workflows", "release.yml"), "utf8");
  for (const f of ["coderhelm-log-reader.yaml", "coderhelm-log-reader-update.yaml"]) {
    assert.ok(wf.includes(`cp infra/cfn/${f} dist/${f}`), `${f} is built into the release`);
    assert.ok(wf.includes(`dist/${f}`) && wf.includes(f + " "), `${f} is in SHA256SUMS`);
  }
  // Published write-once under launch/<tag>/, which the publisher role may write.
  assert.ok(wf.includes('for f in coderhelm-log-reader.yaml coderhelm-log-reader-update.yaml; do put "dist/$f" "launch/$TAG/$f" text/yaml; done'));
  assert.ok(wf.includes('put dist/coderhelm.json "launch/$TAG/coderhelm.json" application/json'), "the native parent template is published");
  assert.ok(wf.includes("--if-none-match '*'"), "never replaced");
  const role = fs.readFileSync(path.join(INFRA, "cfn", "coderhelm-log-reader.yaml"), "utf8");
  assert.ok(role.includes('"sts:ExternalId": !Ref ExternalId'));
  assert.ok(role.includes('"aws:PrincipalArn": !Sub "arn:aws:iam::${CoderHelmAccountId}:role/${TrustedRolePrefix}*"'));
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

test("single origin: every public route the gateway serves outside /api reaches it through CloudFront", () => {
  const main = fs.readFileSync(path.join(INFRA, "..", "services", "gateway", "src", "main.rs"), "utf8");
  // Top-level routers, and the paths of the routers nested there.
  const nested = [...main.matchAll(/\.nest\("(\/[a-z-]+)",\s*([a-z_]+)\)/g)].map((m) => ({ prefix: m[1], router: m[2] }));
  assert.ok(nested.length >= 6, "found the top-level routers");
  const covered = (p: string) =>
    API_PATHS.some((a) => (a.endsWith("/*") ? p.startsWith(a.slice(0, -1)) : a.endsWith("*") ? p.startsWith(a.slice(0, -1)) : p === a));
  // Hosted-only: the hosted API's hybrid relay (a self-hosted install never serves it).
  const hostedOnly = ["/hybrid"];
  for (const { prefix, router } of nested) {
    if (hostedOnly.includes(prefix)) continue;
    const body = main.slice(main.indexOf(`let ${router} = Router::new()`));
    const end = body.indexOf(";");
    const paths = [...body.slice(0, end).matchAll(/\.route\(\s*"([^"]+)"/g)].map((m) => prefix + m[1]);
    for (const p of paths.length ? paths : [prefix + "/x"]) {
      assert.ok(covered(p.replace(/:[a-z_]+/g, "x")), `${p} isn't in API_PATHS: on a single-origin install it would reach the dashboard's bucket`);
    }
  }
  // The dashboard's own plugin pages still go to the dashboard.
  assert.ok(!covered("/plugins/installed"));
  assert.ok(covered("/plugins/approve"));
  assert.ok(covered("/agent-approvals/link"));
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

test("the release roles: only release.yml on a v* tag in the approved release environment, or auto-release.yml on main in release-auto; templates never replaced; latest.json alone", () => {
  const cfg = loadConfigFile(path.join(INFRA, "config", "hosted.prod.json"), { CDK_DEFAULT_ACCOUNT: "654654210434" });
  const t = synth(cfg, zoneContext("654654210434", "us-east-1", "coderhelm.com"));
  const w = t["coderhelm-prod-worker"];
  const policies = Object.values(w.findResources("AWS::IAM::Policy")) as any[];
  const trust = {
    StringEquals: {
      "token.actions.githubusercontent.com:aud": "sts.amazonaws.com",
      // A job in the release environment, which needs a reviewer's approval...
      "token.actions.githubusercontent.com:sub": "repo:CoderHelm/coderhelm-platform:environment:release",
    },
    StringLike: {
      // ...that is release.yml itself, run on a v* tag: another workflow naming the environment gets nothing.
      "token.actions.githubusercontent.com:job_workflow_ref": "CoderHelm/coderhelm-platform/.github/workflows/release.yml@refs/tags/v*",
      "token.actions.githubusercontent.com:ref": "refs/tags/v*",
    },
  };
  // An automatic release: auto-release.yml itself, on main, in the environment that admits main alone.
  const autoTrust = {
    StringEquals: {
      "token.actions.githubusercontent.com:aud": "sts.amazonaws.com",
      "token.actions.githubusercontent.com:sub": "repo:CoderHelm/coderhelm-platform:environment:release-auto",
      "token.actions.githubusercontent.com:job_workflow_ref": "CoderHelm/coderhelm-platform/.github/workflows/auto-release.yml@refs/heads/main",
      "token.actions.githubusercontent.com:ref": "refs/heads/main",
    },
  };
  const statementsOf = (roleName: string, logicalPrefix: string) => {
    const role = Object.values(w.findResources("AWS::IAM::Role", { Properties: { RoleName: roleName } }))[0] as any;
    assert.ok(role, `${roleName} exists on hosted`);
    assert.equal(role.Properties.MaxSessionDuration, 3600);
    const sts = role.Properties.AssumeRolePolicyDocument.Statement;
    assert.equal(sts.length, 2, "exactly two ways in");
    for (const st of sts) {
      assert.equal(st.Action, "sts:AssumeRoleWithWebIdentity");
      assert.equal(st.Effect, "Allow");
      assert.ok(JSON.stringify(st.Principal.Federated).includes("token.actions.githubusercontent.com"), JSON.stringify(st.Principal));
    }
    assert.deepEqual(sts[0].Condition, trust);
    assert.deepEqual(sts[1].Condition, autoTrust);
    assert.ok(!JSON.stringify(sts).includes("StringLike\":{\"token.actions.githubusercontent.com:sub"), "no wildcard sub");
    const mine = policies.filter((p) => JSON.stringify(p.Properties.Roles).includes(logicalPrefix));
    assert.equal(mine.length, 1);
    return mine[0].Properties.PolicyDocument.Statement;
  };
  assert.deepEqual(statementsOf("coderhelm-prod-release-publisher", "ReleasePublisherRole"), [
    {
      Action: "s3:PutObject", Effect: "Allow", Resource: "arn:aws:s3:::coderhelm-public/launch/v*/*",
      Condition: { Null: { "s3:if-none-match": "false" } },
    },
  ]);
  assert.deepEqual(statementsOf("coderhelm-prod-release-latest", "ReleaseLatestRole"), [
    { Action: "s3:PutObject", Effect: "Allow", Resource: "arn:aws:s3:::coderhelm-public/launch/latest.json", Condition: { Null: { "s3:if-none-match": "false" } } },
    { Action: "s3:PutObject", Effect: "Allow", Resource: "arn:aws:s3:::coderhelm-public/launch/latest.json", Condition: { Null: { "s3:if-match": "false" } } },
  ]);
  w.hasOutput("ReleasePublisherRoleArn", {});
  w.hasOutput("ReleaseLatestRoleArn", {});
  // The workflow writes templates only with If-None-Match, and latest.json only with the second role.
  const wf = fs.readFileSync(path.join(INFRA, "..", ".github", "workflows", "release.yml"), "utf8");
  const puts = wf.split("\n").filter((l) => /s3api put-object|aws s3 cp/.test(l));
  assert.ok(puts.length >= 3);
  for (const l of puts) assert.match(l, /--if-none-match '\*'|--if-match "\$etag"/, l);
  assert.ok(!/aws s3 cp/.test(wf), "no unconditional writes");
  assert.ok(wf.indexOf("role/coderhelm-prod-release-latest") > wf.indexOf("role/coderhelm-prod-release-publisher"));
  // auto-release.yml: the same conditional writes, in release-auto, after it checked the published release.
  const auto = fs.readFileSync(path.join(INFRA, "..", ".github", "workflows", "auto-release.yml"), "utf8");
  const autoPuts = auto.split("\n").filter((l) => /s3api put-object|aws s3 cp/.test(l));
  assert.ok(autoPuts.length >= 3);
  for (const l of autoPuts) assert.match(l, /--if-none-match '\*'|--if-match "\$etag"/, l);
  assert.ok(!/aws s3 cp/.test(auto));
  assert.ok(auto.includes("environment: release-auto"));
  assert.ok(auto.indexOf("Check the published release") < auto.indexOf("role/coderhelm-prod-release-publisher"));
  assert.ok(auto.indexOf("role/coderhelm-prod-release-latest") > auto.indexOf("role/coderhelm-prod-release-publisher"));
  // Self-host installs have no publisher.
  const sh = synth(selfHost());
  assert.ok(!json(sh["coderhelm-prod-worker"]).includes("release-publisher"));
  assert.ok(!json(sh["coderhelm-prod-worker"]).includes("release-latest"));
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

// ── Switching the model provider (Settings, System, Models) ──────────────

/** Every action any IAM policy or role in these templates allows. */
function allowedActions(t: Record<string, Template>): string[] {
  const out: string[] = [];
  const walk = (v: unknown) => {
    if (Array.isArray(v)) v.forEach(walk);
    else if (v && typeof v === "object") {
      for (const [k, x] of Object.entries(v)) {
        if (k === "Action") out.push(...(Array.isArray(x) ? x : [x]).map(String));
        else walk(x);
      }
    }
  };
  for (const tpl of Object.values(t)) walk(tpl.toJSON().Resources);
  return out;
}

test("a self-host that starts on Anthropic keys may still call Bedrock, and nothing may accept terms", () => {
  const t = synth(selfHost({ models: { provider: "anthropic" } }));
  // No MODEL_PROVIDER: the first-boot default is the Anthropic API.
  assert.equal(envOf(t["coderhelm-prod-api"], "coderhelm-prod-gateway").MODEL_PROVIDER, undefined);
  // Every function that calls Claude may call it on Bedrock, on Anthropic models and inference profiles only.
  for (const stack of ["coderhelm-prod-api", "coderhelm-prod-worker", "coderhelm-prod-streaming", "coderhelm-prod-log-analyzer"]) {
    const policies = Object.values(t[stack].findResources("AWS::IAM::Policy"));
    const st = policies
      .flatMap((p: any) => p.Properties.PolicyDocument.Statement)
      .find((s: any) => s.Sid === "CallClaudeOnBedrock");
    assert.ok(st, `${stack} may call Claude on Bedrock`);
    assert.deepEqual(st.Action, ["bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream"]);
    const res = JSON.stringify(st.Resource);
    assert.ok(res.includes("foundation-model/anthropic.*"), stack);
    assert.ok(res.includes("inference-profile/*"), stack);
    assert.ok(!res.includes('"*"'), `${stack}: never every resource`);
  }
  // The install never subscribes in AWS Marketplace or accepts a model agreement or use case form.
  const actions = allowedActions(t);
  for (const a of actions) {
    assert.ok(!/^aws-marketplace:/i.test(a), a);
    assert.ok(!/bedrock:(CreateFoundationModelAgreement|PutUseCaseForModelAccess|\*)/i.test(a), a);
  }
  // The updater asks the worker for the model check, and knows the config's models.
  const u = t["coderhelm-prod-updates"];
  const env = envOf(u, "coderhelm-prod-updater");
  assert.equal(env.WORKER_FUNCTION_NAME, "coderhelm-prod-worker");
  assert.equal(env.CONFIGURED_MODELS, "claude-opus-5-5,claude-sonnet-5-5");
  const up = JSON.stringify(u.findResources("AWS::IAM::Policy"));
  assert.ok(up.includes("ModelCheckThroughWorker"));
  assert.ok(!up.includes("bedrock:"), "the updater itself never calls a model");
});

test("the hosted deployment on Anthropic keys gets no Bedrock model access", () => {
  const hostedCfg = loadConfigFile(path.join(INFRA, "config", "hosted.prod.json"), { CDK_DEFAULT_ACCOUNT: "654654210434" });
  if (hostedCfg.models.provider === "bedrock") return;
  const hosted = synth(hostedCfg, zoneContext("654654210434", "us-east-1", "coderhelm.com"));
  const api = JSON.stringify(hosted["coderhelm-prod-api"].findResources("AWS::IAM::Policy"));
  assert.ok(!api.includes("CallClaudeOnBedrock"));
});

test("the release check reads models.json only after the signature check, and can only report back", () => {
  const cfg = selfHost();
  const commands: string[] = [...Object.values((releaseCheckBuildSpec(cfg) as any).spec.phases)].flatMap((p: any) => p.commands);
  const verifyAt = commands.indexOf(VERIFY_SUMS);
  assert.ok(verifyAt > 0, "the same exact signature check as the update build");
  assert.ok(commands.indexOf(CHECK_VERSION) < verifyAt);
  for (const line of verifyTagCommands()) assert.ok(commands.indexOf(line) > 0 && commands.indexOf(line) < verifyAt, line);
  const readAt = commands.findIndex((c) => c.includes("-o models.json"));
  assert.ok(readAt > verifyAt, "models.json only after the signature check");
  assert.ok(commands[readAt].includes("grep ' models.json$' SHA256SUMS | sha256sum -c -"), "and only when its checksum matches");
  assert.ok(!commands.some((c) => /npm install|coderhelm-selfhost|cdk /.test(c)), "nothing installed or deployed");
  assert.ok(commands.some((c) => c.includes('action:"models_verified"')));
  // The updater believes the report only from the build's own exported checksums (lambda/updater, confirm_check).
  assert.deepEqual((releaseCheckBuildSpec(cfg) as any).spec.env["exported-variables"], ["TEMPLATE_SHA256", "MODELS_SHA256"]);
  assert.ok(commands.some((c) => c.includes("build:process.env.CODEBUILD_BUILD_ID")));
  const t = synth(cfg);
  const u = t["coderhelm-prod-updates"];
  const roles = u.findResources("AWS::IAM::Role");
  const check = Object.entries(roles).find(([, r]: any) => r.Properties.RoleName === "coderhelm-prod-release-check");
  assert.ok(check, "its own role");
  const policies = Object.values(u.findResources("AWS::IAM::Policy")).filter((p: any) => JSON.stringify(p.Properties.Roles).includes(check![0]));
  const actions = policies.flatMap((p: any) => p.Properties.PolicyDocument.Statement.flatMap((s: any) => [].concat(s.Action)));
  // Only reporting back (plus CodeBuild's own logs and reports): no deploy, no secret, no assumed role.
  assert.ok(actions.every((a) => a === "lambda:InvokeFunction" || a.startsWith("logs:") || /^codebuild:(Create|Update|BatchPut)/.test(a)), JSON.stringify(actions));
  assert.ok(!actions.some((a) => /sts:|secretsmanager:|ssm:|cloudformation:|bedrock:/.test(a)));
  assert.equal(envOf(u, "coderhelm-prod-updater").RELEASE_CHECK_PROJECT_NAME, "coderhelm-prod-release-check");
});

test("sign-in: self-host checks every new account in Cognito (pre-sign-up trigger); MFA stays optional with authenticator apps", () => {
  const t = synth(selfHost({ signup: { domains: ["Example.org"], emails: ["b@example.org"], requireTwoFactor: true } }));
  const api = t["coderhelm-prod-api"];
  // Optional in Cognito: the "required" switch is CoderHelm's (an admin turns
  // it on at runtime), and Google sign-ins never pass Cognito MFA anyway.
  api.hasResourceProperties("AWS::Cognito::UserPool", {
    MfaConfiguration: "OPTIONAL",
    EnabledMfas: ["SOFTWARE_TOKEN_MFA"],
  });
  const pool = Object.values(api.findResources("AWS::Cognito::UserPool"))[0] as any;
  const pre = pool.Properties.LambdaConfig.PreSignUp;
  assert.ok(pre, "a pre-sign-up trigger");
  const fns = api.findResources("AWS::Lambda::Function");
  const [fnId, fn] = Object.entries(fns).find(([, r]) => (r as any).Properties.FunctionName === "coderhelm-prod-pre-signup") as [string, any];
  assert.deepEqual(pre, { "Fn::GetAtt": [fnId, "Arn"] });
  const env = fn.Properties.Environment.Variables;
  assert.ok(env.SETTINGS_TABLE_NAME && env.USERS_TABLE_NAME);
  // Cognito may call it; it only reads.
  api.hasResourceProperties("AWS::Lambda::Permission", { Principal: "cognito-idp.amazonaws.com", FunctionName: { "Fn::GetAtt": [fnId, "Arn"] } });
  const policies = JSON.stringify(api.findResources("AWS::IAM::Policy"));
  const preRolePolicy = Object.values(api.findResources("AWS::IAM::Policy")).find((p: any) =>
    JSON.stringify(p.Properties.Roles).includes(fn.Properties.Role["Fn::GetAtt"][0]),
  ) as any;
  const actions = JSON.stringify(preRolePolicy.Properties.PolicyDocument.Statement.map((s: any) => s.Action));
  assert.ok(actions.includes("dynamodb:GetItem") && actions.includes("dynamodb:Query"), actions);
  assert.ok(!/PutItem|UpdateItem|DeleteItem|BatchWrite/.test(actions), `pre-sign-up may only read: ${actions}`);
  assert.ok(policies.length > 0);

  // The first boot's list: the admin, an invite, a domain and the rule.
  const db = json(t["coderhelm-prod-database"]);
  for (const want of ['"EMAIL#owner@example.org"},"kind":{"S":"admin"}', '"EMAIL#b@example.org"},"kind":{"S":"invite"}', '"EMAIL#*@example.org"},"kind":{"S":"domain"}', '"TWO_FACTOR"},"kind":{"S":"rule"}', '"require":{"BOOL":true}']) {
    assert.ok(db.replace(/\\/g, "").includes(want), want);
  }
});

test("sign-in: hosted keeps its open pool (no pre-sign-up trigger) and optional MFA", () => {
  const cfg = loadConfigFile(path.join(INFRA, "config", "hosted.prod.json"), { CDK_DEFAULT_ACCOUNT: "654654210434" });
  const api = synth(cfg, zoneContext("654654210434", "us-east-1", "coderhelm.com"))["coderhelm-prod-api"];
  const pool = Object.values(api.findResources("AWS::Cognito::UserPool"))[0] as any;
  assert.equal(pool.Properties.MfaConfiguration, "OPTIONAL");
  assert.equal(pool.Properties.LambdaConfig.PreSignUp, undefined);
  assert.ok(pool.Properties.LambdaConfig.CustomMessage);
});

test("worker queues: visibility 6x the worker timeout, redrive count 5", () => {
  const api = synth(selfHost())["coderhelm-prod-api"];
  for (const name of ["tickets", "ci-fix", "feedback", "webhooks"]) {
    const queues = api.findResources("AWS::SQS::Queue", {
      Properties: { QueueName: `coderhelm-prod-${name}` },
    });
    const q = (Object.values(queues)[0] as any).Properties;
    assert.equal(q.VisibilityTimeout, 5400, name);
    assert.equal(q.RedrivePolicy.maxReceiveCount, 5, name);
  }
});

test("outbound webhooks: one queue, wired to the worker, sendable by every emitter", () => {
  const t = synth(selfHost());
  const api = t["coderhelm-prod-api"];
  const queues = api.findResources("AWS::SQS::Queue", {
    Properties: { QueueName: "coderhelm-prod-webhooks" },
  });
  const ids = Object.keys(queues);
  assert.equal(ids.length, 1, "one webhooks queue");
  const q = (queues[ids[0]] as any).Properties;
  // AWS: visibility at least 6x the worker's 900 s, redrive count at least 5.
  assert.equal(q.VisibilityTimeout, 5400);
  assert.equal(q.RedrivePolicy.maxReceiveCount, 5);
  // Every function that emits or sends knows the queue.
  const gw = envOf(api, "coderhelm-prod-gateway");
  assert.ok(gw.WEBHOOK_QUEUE_URL, "gateway");
  const worker = t["coderhelm-prod-worker"];
  assert.ok(envOf(worker, "coderhelm-prod-worker").WEBHOOK_QUEUE_URL, "worker");
  // The worker reads it one message at a time, with a concurrency cap.
  const sources = Object.values(worker.findResources("AWS::Lambda::EventSourceMapping")) as any[];
  const fromWebhooks = sources.filter((s) =>
    JSON.stringify(s.Properties.EventSourceArn).includes("WebhookQueue")
  );
  assert.equal(fromWebhooks.length, 1, "the worker consumes the webhooks queue");
  assert.equal(fromWebhooks[0].Properties.BatchSize, 1);
  assert.equal(fromWebhooks[0].Properties.ScalingConfig.MaximumConcurrency, 5);
  // Gateway, worker, streaming and the log analyzer may send to it.
  for (const [stack, fn] of [
    ["coderhelm-prod-api", "Gateway"],
    ["coderhelm-prod-worker", "Worker"],
    ["coderhelm-prod-streaming", "Streaming"],
    ["coderhelm-prod-log-analyzer", "Analyzer"],
  ]) {
    // A large role's statements spill into a managed "overflow" policy.
    const policies = JSON.stringify([
      t[stack].findResources("AWS::IAM::Policy"),
      t[stack].findResources("AWS::IAM::ManagedPolicy"),
    ]);
    assert.ok(
      policies.includes("WebhookQueue") && policies.includes("sqs:SendMessage"),
      `${fn} can send webhook events`
    );
  }
  // The template for the admins' email when an endpoint is turned off.
  assert.ok(json(t["coderhelm-prod-email"]).includes("coderhelm-prod-webhook-disabled"));
});

test("the update build may run the installer's model check: ask the worker, read the SYSTEM setting only", () => {
  const u = synth(selfHost())["coderhelm-prod-updates"];
  const statements = Object.values(u.findResources("AWS::IAM::Policy")).flatMap((p: any) => p.Properties.PolicyDocument.Statement);
  const invoke = statements.find((s: any) => s.Sid === "InstallerModelCheck" && JSON.stringify(s.Resource).includes("coderhelm-prod-worker"));
  assert.ok(invoke);
  const read = statements.find((s: any) => s.Sid === "ModelCheckSetting");
  assert.deepEqual(read.Action, "dynamodb:GetItem");
  assert.deepEqual(read.Condition, { "ForAllValues:StringEquals": { "dynamodb:LeadingKeys": ["SYSTEM"] } });
});

// ── Optional email and CoderHelm's mail relay ──────────────────────────────

test("self-host: optional email starts off, own SES, and nothing that mails CoderHelm", () => {
  const cfg = loadConfigFile(path.join(INFRA, "config", "selfhost.example.json"), {});
  assert.equal(cfg.integrations.weeklySummary, false, "the example config starts it off");
  assert.equal(selfHost().integrations.weeklySummary, false, "and so does a config that says nothing");
  assert.equal(cfg.email.transport, "ses");
  assert.equal(cfg.mailRelay, null);
  const t = synth(cfg);
  for (const fn of ["coderhelm-prod-worker", "coderhelm-prod-gateway"]) {
    const stack = fn.endsWith("worker") ? "coderhelm-prod-worker" : "coderhelm-prod-api";
    const env = envOf(t[stack], fn);
    assert.equal(env.DEPLOYMENT_KIND, "self-hosted", fn);
    assert.equal(env.OPTIONAL_EMAIL_DEFAULTS, "", fn);
    assert.equal(env.EMAIL_TRANSPORT_DEFAULT, "ses", fn);
    assert.equal(env.HYBRID_EMAIL_FROM, undefined, fn);
    // No CoderHelm address anywhere a service could send to.
    for (const [k, v] of Object.entries(env)) {
      assert.ok(!/@(?:[a-z0-9-]+\.)*coderhelm\.com/i.test(String(v)), `${fn} ${k}=${v} names a CoderHelm address`);
    }
  }
  // The email templates and every stack: no CoderHelm mailbox in a self-hosted install.
  for (const [name, tpl] of Object.entries(t)) {
    assert.ok(!/@(?:[a-z0-9-]+\.)*coderhelm\.com/i.test(json(tpl)), `${name} names a CoderHelm address`);
    assert.equal(Object.keys(tpl.findResources("AWS::SNS::Topic")).filter((k) => k.includes("MailRelay")).length, 0, name);
  }
  t["coderhelm-prod-api"].hasResourceProperties("AWS::Events::Rule", { Name: "coderhelm-prod-weekly-summary", State: "ENABLED" });
  // The install's own SES events reach its worker (counted by kind for
  // Deliveries), through the default bus, matched by the set's name.
  t["coderhelm-prod-email"].hasResourceProperties("AWS::SES::ConfigurationSetEventDestination", {
    EventDestination: Match.objectLike({
      Enabled: true,
      MatchingEventTypes: Match.arrayWith(["send", "delivery", "bounce", "complaint", "reject"]),
      EventBridgeDestination: Match.anyValue(),
    }),
  });
  t["coderhelm-prod-api"].hasResourceProperties("AWS::Events::Rule", {
    Name: "coderhelm-prod-email-events",
    EventPattern: { source: ["aws.ses"], detail: { mail: { tags: { "ses:configuration-set": ["coderhelm-prod-emails"] } } } },
  });
  assert.equal(envOf(t["coderhelm-prod-api"], "coderhelm-prod-gateway").COGNITO_EMAIL_SENDER, "cognito_default");
});

test("self-host: a config that turns the weekly summary on, or chooses CoderHelm's mail, starts the install that way", () => {
  const t = synth(selfHost({ integrations: { weeklySummary: true }, email: { from: "coderhelm@example.org", transport: "coderhelm" } }));
  const env = envOf(t["coderhelm-prod-worker"], "coderhelm-prod-worker");
  assert.equal(env.OPTIONAL_EMAIL_DEFAULTS, "weekly_summary");
  assert.equal(env.EMAIL_TRANSPORT_DEFAULT, "coderhelm");
  assert.throws(() => selfHost({ email: { from: "a@example.org", transport: "smtp" as never } }), /email.transport/);
});

test("self-host: the organization names every email, by either route", () => {
  const t = synth(selfHost({ email: { from: "coderhelm@example.org", organization: " Acme " } }));
  for (const [stack, fn] of [["coderhelm-prod-worker", "coderhelm-prod-worker"], ["coderhelm-prod-api", "coderhelm-prod-gateway"], ["coderhelm-prod-updates", "coderhelm-prod-updater"]]) {
    const env = envOf(t[stack], fn);
    assert.equal(env.ORGANIZATION_NAME, "Acme", fn);
    assert.equal(env.EMAIL_FROM_NAME, "CoderHelm", fn);
  }
  const plain = envOf(synth(selfHost())["coderhelm-prod-worker"], "coderhelm-prod-worker");
  assert.equal(plain.ORGANIZATION_NAME, undefined, "no organization: the install's own address names it");
  assert.throws(() => selfHost({ email: { from: "a@example.org", organization: "Acme\r\nBcc: x@example.org" } }), /email.organization/);
  assert.throws(() => selfHost({ email: { from: "a@example.org", organization: "x".repeat(61) } }), /email.organization/);
  assert.throws(() => selfHost({ email: { from: "a@example.org", fromName: "\"Acme\" <x@example.org>" } }), /email.fromName/);
});

test("hosted: the weekly summary stays on for everyone, and the mail relay sends only as notify.coderhelm.com", () => {
  const cfg = loadConfigFile(path.join(INFRA, "config", "hosted.prod.json"), { CDK_DEFAULT_ACCOUNT: "654654210434" });
  assert.equal(cfg.integrations.weeklySummary, true);
  assert.deepEqual(cfg.mailRelay, { domain: "notify.coderhelm.com", from: "notifications@notify.coderhelm.com" });
  const t = synth(cfg, zoneContext("654654210434", "us-east-1", "coderhelm.com"));
  const api = t["coderhelm-prod-api"];
  const worker = envOf(t["coderhelm-prod-worker"], "coderhelm-prod-worker");
  assert.equal(worker.DEPLOYMENT_KIND, "hosted");
  assert.equal(worker.WEEKLY_SUMMARY_ENABLED, "true");
  assert.equal(worker.OPTIONAL_EMAIL_DEFAULTS, undefined);
  api.hasResourceProperties("AWS::Events::Rule", { Name: "coderhelm-prod-weekly-summary", State: "ENABLED" });
  const gw = envOf(api, "coderhelm-prod-gateway");
  assert.equal(gw.HYBRID_EMAIL_FROM, "notifications@notify.coderhelm.com");
  assert.equal(gw.HYBRID_EMAIL_CONFIG_SET, "coderhelm-prod-relay");
  // Its own identity with Easy DKIM and a custom MAIL FROM.
  api.hasResourceProperties("AWS::SES::EmailIdentity", {
    EmailIdentity: "notify.coderhelm.com",
    DkimSigningAttributes: Match.absent(),
    MailFromAttributes: { MailFromDomain: "bounce.notify.coderhelm.com", BehaviorOnMxFailure: "REJECT_MESSAGE" },
  });
  // SPF, DMARC reject, MAIL FROM MX, three DKIM CNAMEs.
  const recs = Object.values(api.findResources("AWS::Route53::RecordSet")).map((r: any) => r.Properties);
  const txt = (name: string) => recs.filter((p) => p.Type === "TXT" && p.Name === name).flatMap((p) => p.ResourceRecords);
  assert.ok(txt("_dmarc.notify.coderhelm.com.").some((v: string) => v.includes("p=reject")), "DMARC rejects");
  assert.ok(txt("notify.coderhelm.com.").some((v: string) => v.includes("include:amazonses.com -all")), "SPF");
  assert.ok(txt("bounce.notify.coderhelm.com.").some((v: string) => v.includes("include:amazonses.com -all")), "MAIL FROM SPF");
  assert.ok(recs.some((p) => p.Type === "MX" && p.Name === "bounce.notify.coderhelm.com."), "MAIL FROM MX");
  const dkim = recs.filter((p) => p.Type === "CNAME" && JSON.stringify(p.Name).includes("DkimDNSTokenName"));
  assert.equal(dkim.length, 3, "Easy DKIM");
  // Full names as SES gives them: the zone is never appended a second time.
  for (const p of dkim) assert.ok(!JSON.stringify(p.Name).includes("coderhelm.com."), `DKIM name has the zone appended: ${JSON.stringify(p.Name)}`);
  // Every event reaches the worker through SNS, raw (bounces and complaints suppress; all are counted).
  api.hasResourceProperties("AWS::SES::ConfigurationSetEventDestination", {
    ConfigurationSetName: Match.anyValue(),
    EventDestination: Match.objectLike({ MatchingEventTypes: Match.arrayWith(["send", "delivery", "bounce", "complaint", "reject"]) }),
  });
  api.hasResourceProperties("AWS::SNS::Subscription", { Protocol: "sqs", RawMessageDelivery: true });
  // Sending as the relay: only its identity and configuration set, only from its address.
  for (const [stack, role] of [["coderhelm-prod-api", api], ["coderhelm-prod-worker", t["coderhelm-prod-worker"]]] as const) {
    const sts = Object.values(role.findResources("AWS::IAM::Policy")).flatMap((p: any) => p.Properties.PolicyDocument.Statement);
    const relay = sts.filter((st: any) => st.Sid === "MailRelaySendsOnlyAsTheRelay");
    assert.equal(relay.length, 1, stack);
    assert.deepEqual(relay[0].Condition, { StringEquals: { "ses:FromAddress": "notifications@notify.coderhelm.com" } });
    assert.ok(JSON.stringify(relay[0].Resource).includes("identity/notify.coderhelm.com"), stack);
    assert.ok(!JSON.stringify(relay[0].Resource).includes("identity/coderhelm.com"), stack);
  }
});

// The hardened deploy role (infra/lib/access.ts) is all CloudFormation has.
// Every resource a self-host install creates must be one it may create, and
// tag where the resource carries tags: a missing tag action fails the
// install mid-way (an HTTP API stage did, on a real install).
const SERVICE_OF: Record<string, string> = {
  "AWS::ApiGatewayV2": "apigateway", "AWS::CertificateManager": "acm", "AWS::CloudFront": "cloudfront", "AWS::CloudWatch": "cloudwatch",
  "AWS::CodeBuild": "codebuild", "AWS::Cognito": "cognito-idp", "AWS::DynamoDB": "dynamodb", "AWS::Events": "events", "AWS::IAM": "iam",
  "AWS::KMS": "kms", "AWS::Lambda": "lambda", "AWS::Logs": "logs", "AWS::Route53": "route53", "AWS::S3": "s3", "AWS::SES": "ses",
  "AWS::SNS": "sns", "AWS::SQS": "sqs", "AWS::SSM": "ssm", "AWS::Scheduler": "scheduler",
};
// Resource types that take tags (stack tags reach them), with the piece of
// their ARN a grant must cover: a tag allowed on functions doesn't reach an
// event source mapping (a real install was refused there).
const TAGGABLE: Record<string, string> = {
  "AWS::ApiGatewayV2::Api": "::/apis", "AWS::ApiGatewayV2::Stage": "::/apis", "AWS::ApiGatewayV2::DomainName": "::/domainnames",
  "AWS::CertificateManager::Certificate": ":certificate/", "AWS::CloudFront::Distribution": ":distribution/", "AWS::CodeBuild::Project": ":project/",
  "AWS::Cognito::UserPool": ":userpool/", "AWS::DynamoDB::Table": ":table/", "AWS::DynamoDB::GlobalTable": ":table/", "AWS::IAM::Role": ":role/",
  "AWS::IAM::ManagedPolicy": ":policy/", "AWS::KMS::Key": ":key/", "AWS::Lambda::Function": ":function:",
  "AWS::Lambda::EventSourceMapping": ":event-source-mapping:", "AWS::Logs::LogGroup": ":log-group:", "AWS::S3::Bucket": ":::",
  "AWS::SNS::Topic": ":sns:", "AWS::SQS::Queue": ":sqs:", "AWS::SSM::Parameter": ":parameter/", "AWS::Events::Rule": ":rule/",
  "AWS::SES::ConfigurationSet": ":configuration-set/", "AWS::SES::EmailIdentity": ":identity/",
};
test("the deploy role may create and tag every resource a self-host install creates", () => {
  const n = accessNames({ account: "111122223333", region: "eu-west-1", prefix: "coderhelm-prod", suffix: "-111122223333", secretName: "coderhelm-prod" });
  const allowed = deployPolicy(n).Statement.filter((st) => st.Effect === "Allow").flatMap((st) => [st.Action ?? []].flat());
  const may = (svc: string, re: RegExp) => allowed.some((a) => a === `${svc}:*` || (a.startsWith(`${svc}:`) && re.test(a.slice(svc.length + 1))));
  // A tag grant that reaches this kind of resource: the action, and a
  // Resource of "*" or one naming that kind of ARN.
  const allows = deployPolicy(n).Statement.filter((st) => st.Effect === "Allow");
  const mayTag = (svc: string, kind: string) => allows.some((st) =>
    [st.Action ?? []].flat().some((a) => a === `${svc}:*` || (a.startsWith(`${svc}:`) && /Tag/.test(a))) &&
    [st.Resource ?? []].flat().some((r) => r === "*" || r.includes(kind)));
  const templates = [
    synth(selfHost()),
    synth(selfHost({ region: "eu-west-1", domain: { hostname: "helm.example.org", hostedZone: "example.org" }, email: { from: "helm@example.org" } }), {
      ...zoneContext("111122223333", "eu-west-1", "example.org"),
      ...zoneContext("111122223333", "us-east-1", "example.org"),
    }),
  ].flatMap((t) => Object.values(t));
  const gaps = new Set<string>();
  for (const t of templates) {
    for (const r of Object.values(t.toJSON().Resources ?? {}) as any[]) {
      const type: string = r.Type;
      if (type.startsWith("Custom::") || type.startsWith("AWS::CloudFormation::") || type === "AWS::CDK::Metadata") continue;
      const svc = SERVICE_OF[type.split("::").slice(0, 2).join("::")];
      if (!svc) { gaps.add(`${type}: no service mapped (add it to SERVICE_OF and the deploy role)`); continue; }
      if (!may(svc, /^(Create|Put|Post|POST|Request|Change|\*)/)) gaps.add(`${type}: ${svc} create`);
      // The installer tags each stack, and CloudFormation passes stack tags
      // to every resource that takes them, Tags property or not.
      if (TAGGABLE[type] && !mayTag(svc, TAGGABLE[type])) gaps.add(`${type}: ${svc} tagging on ${TAGGABLE[type]}`);
    }
  }
  assert.deepEqual([...gaps].sort(), []);
});
