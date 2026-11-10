// What AWS Security Hub checks on an install, as the templates set it
// (docs/self-host/security.md, "Security Hub"): the chat stream behind
// CloudFront only, access logs, TLS everywhere, tables and the user pool
// protected from deletion, supported runtimes. Self-host (CLI) and native
// templates both; the hosted deployment where it shares the stack.
//
//   npm test

import { test } from "node:test";
import * as assert from "node:assert/strict";
import * as cdk from "aws-cdk-lib";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { Template } from "aws-cdk-lib/assertions";
import { buildApp } from "../lib/app";
import { loadConfigFile, resolveConfig, ResolvedConfig } from "../lib/config";
import { buildNative } from "../lib/native/build";
import { HELPER_TABLE_KEY_ACTIONS, helperPolicy } from "../lib/native/parent";
import { ACCESS_LOG_DAYS } from "../lib/storage-stack";
import { CLOUDFRONT_LOG_REGIONS } from "../lib/web-stack";

const INFRA = path.join(__dirname, "..", "..");
const STUBS = fs.mkdtempSync(path.join(os.tmpdir(), "chsec-"));
for (const d of ["gateway", "worker", "mcp"]) {
  fs.mkdirSync(path.join(STUBS, d), { recursive: true });
  fs.writeFileSync(path.join(STUBS, d, d === "mcp" ? "handler.js" : "bootstrap"), "stub");
}
const DASH = path.join(STUBS, "dash");
fs.mkdirSync(path.join(DASH, "_next"), { recursive: true });
fs.writeFileSync(path.join(DASH, "index.html"), "<html></html>");
fs.writeFileSync(path.join(DASH, "_next", "a.js"), "x");
process.env.GATEWAY_ZIP = path.join(STUBS, "gateway");
process.env.WORKER_ZIP = path.join(STUBS, "worker");
process.env.MCP_PROXY_DIR = path.join(STUBS, "mcp");

function synth(cfg: ResolvedConfig, context: Record<string, unknown> = {}): Record<string, any> {
  const app = new cdk.App({ outdir: fs.mkdtempSync(path.join(os.tmpdir(), "chsecout-")), context });
  buildApp(app, cfg);
  const out: Record<string, any> = {};
  for (const s of app.node.children) if (s instanceof cdk.Stack) out[s.stackName] = Template.fromStack(s).toJSON();
  return out;
}

const selfHost = synth(resolveConfig({
  account: "111122223333",
  region: "us-west-2",
  stage: "prod",
  email: { from: "coderhelm@example.org" },
  admins: ["owner@example.org"],
  integrations: { googleSignIn: false, sandbox: true, logAnalyzer: true, weeklySummary: false },
}));
const hosted = synth(
  loadConfigFile(path.join(INFRA, "config", "hosted.prod.json"), { CDK_DEFAULT_ACCOUNT: "654654210434" }),
  { "hosted-zone:account=654654210434:domainName=coderhelm.com:region=us-east-1": { Id: "/hostedzone/ZTEST000001", Name: "coderhelm.com." } },
);
const rel = buildNative("v9.8.7", fs.mkdtempSync(path.join(os.tmpdir(), "chsecrel-")), { dashboardDir: DASH, commit: "0123456789abcdef0123456789abcdef01234567", releasedAt: "2026-11-01T12:00:00Z" });
const native: Record<string, any> = { parent: rel.parent, ...rel.children };

type R = { id: string; where: string; r: any };
function resources(templates: Record<string, any>, type: string): R[] {
  const out: R[] = [];
  for (const [where, t] of Object.entries(templates)) {
    for (const [id, r] of Object.entries<any>(t.Resources ?? {})) if (r.Type === type) out.push({ id, where, r });
  }
  return out;
}
const one = (rs: R[], what: string): R => {
  assert.equal(rs.length, 1, `${what}: ${rs.map((x) => `${x.where}.${x.id}`).join(", ")}`);
  return rs[0];
};
const named = (rs: R[], part: string): R => one(rs.filter((x) => JSON.stringify(x.r.Properties?.BucketName ?? x.r.Properties?.TableName ?? "").includes(part)), part);

const INSTALLS: [string, Record<string, any>][] = [["self-host", selfHost], ["native", native]];

test("Lambda.1: the chat stream's function URL takes only signed calls; CloudFront signs them (OAC)", () => {
  for (const [name, t] of INSTALLS) {
    const url = one(resources(t, "AWS::Lambda::Url"), `${name} function URL`);
    assert.equal(url.r.Properties.AuthType, "AWS_IAM", name);
    assert.equal(url.r.Properties.InvokeMode, "RESPONSE_STREAM", name);
    const perms = resources(t, "AWS::Lambda::Permission").map((p) => p.r.Properties);
    // Nothing lets the public in (the URL's old FunctionUrlAuthType NONE grants).
    for (const p of perms) assert.notEqual(p.Principal, "*", `${name}: a public permission ${JSON.stringify(p)}`);
    const cf = perms.filter((p) => p.Principal === "cloudfront.amazonaws.com");
    assert.deepEqual(cf.map((p) => p.Action).sort(), ["lambda:InvokeFunction", "lambda:InvokeFunctionUrl"], name);
    for (const p of cf) assert.match(JSON.stringify(p.SourceArn), /:distribution\//, `${name}: scoped to the distribution`);
    assert.equal(cf.find((p) => p.Action === "lambda:InvokeFunction")!.InvokedViaFunctionUrl, true, name);

    const oac = resources(t, "AWS::CloudFront::OriginAccessControl").map((x) => x.r.Properties.OriginAccessControlConfig);
    assert.ok(oac.some((c) => c.OriginAccessControlOriginType === "lambda" && c.SigningBehavior === "always" && c.SigningProtocol === "sigv4"), name);
    const dist = one(resources(t, "AWS::CloudFront::Distribution"), `${name} distribution`).r.Properties.DistributionConfig;
    const lambdaOrigin = dist.Origins.find((o: any) => JSON.stringify(o.DomainName).includes("Fn::Split"));
    assert.ok(lambdaOrigin?.OriginAccessControlId, `${name}: the stream origin uses OAC`);
    for (const p of ["/api/plans/chat/stream", "/api/brain/stream"]) {
      const b = dist.CacheBehaviors.find((x: any) => x.PathPattern === p);
      assert.deepEqual(b.TargetOriginId, lambdaOrigin.Id, p);
      assert.ok(b.AllowedMethods.includes("POST"), p);
      // No caching; every viewer header (the stream token, x-amz-content-sha256), cookie and query string reaches it.
      assert.equal(b.CachePolicyId, "4135ea2d-6df8-44a3-9df3-4b5a84be39ad", p);
      assert.equal(b.OriginRequestPolicyId, "b689b0a8-53d0-40ab-baf2-68738e2966ac", p);
    }
  }
  // CoderHelm's own deployment answers browsers on its stream URL directly: unchanged.
  const h = one(resources(hosted, "AWS::Lambda::Url"), "hosted function URL");
  assert.equal(h.r.Properties.AuthType, "NONE");
});

test("S3.5, S3.9, S3.13: every bucket TLS-only with a lifecycle; every bucket but the log bucket logs into it", () => {
  for (const [name, t] of INSTALLS) {
    const buckets = resources(t, "AWS::S3::Bucket");
    const policies = resources(t, "AWS::S3::BucketPolicy");
    for (const b of buckets) {
      const label = `${name} ${b.where}.${b.id}`;
      const pol = policies.find((p) => p.where === b.where && JSON.stringify(p.r.Properties.Bucket) === JSON.stringify({ Ref: b.id }));
      assert.ok(pol, `${label}: no bucket policy`);
      assert.ok(pol!.r.Properties.PolicyDocument.Statement.some((s: any) => s.Effect === "Deny" && s.Condition?.Bool?.["aws:SecureTransport"] === "false"), `${label}: not TLS-only`);
      assert.ok(b.r.Properties.LifecycleConfiguration?.Rules?.length, `${label}: no lifecycle`);
      const logs = JSON.stringify(b.r.Properties.BucketName ?? "").includes("-logs");
      if (logs) assert.equal(b.r.Properties.LoggingConfiguration, undefined, label);
      // The release bucket (native parent) is made before any part, so before the log bucket.
      else if (b.where !== "parent") assert.match(JSON.stringify(b.r.Properties.LoggingConfiguration), /-logs.*"LogFilePrefix":"s3\/[a-z]+\/"/, `${label}: no access logs`);
    }
  }
});

test("the log bucket: SSE-S3, private, logs kept 90 days, removed with the stack (writes denied, then emptied)", () => {
  for (const [name, t] of [...INSTALLS, ["hosted", hosted] as [string, Record<string, any>]]) {
    const b = named(resources(t, "AWS::S3::Bucket"), "-logs");
    assert.equal(b.where.endsWith("storage") || b.where === "Storage", true, `${name}: in the storage stack`);
    const p = b.r.Properties;
    assert.equal(p.BucketEncryption.ServerSideEncryptionConfiguration[0].ServerSideEncryptionByDefault.SSEAlgorithm, "AES256");
    assert.deepEqual(p.PublicAccessBlockConfiguration, { BlockPublicAcls: true, BlockPublicPolicy: true, IgnorePublicAcls: true, RestrictPublicBuckets: true });
    // CloudFront's standard logs write with an ACL; the install still owns every object.
    assert.equal(p.OwnershipControls.Rules[0].ObjectOwnership, "BucketOwnerPreferred");
    assert.equal(p.AccessControl, undefined, "no ACL grant of its own");
    assert.equal(p.LifecycleConfiguration.Rules[0].ExpirationInDays, ACCESS_LOG_DAYS);
    assert.equal(b.r.DeletionPolicy, "Delete", `${name}: logs aren't kept data`);
    assert.ok(resources(t, "Custom::S3AutoDeleteObjects").some((x) => x.where === b.where && JSON.stringify(x.r.Properties.BucketName) === JSON.stringify({ Ref: b.id })), name);
    const pol = resources(t, "AWS::S3::BucketPolicy").find((x) => x.where === b.where && JSON.stringify(x.r.Properties.Bucket) === JSON.stringify({ Ref: b.id }))!;
    const delivery = pol.r.Properties.PolicyDocument.Statement.find((s: any) => s.Sid === "S3ServerAccessLogsFromThisInstall");
    assert.deepEqual(delivery.Principal, { Service: "logging.s3.amazonaws.com" });
    assert.ok(delivery.Condition.StringEquals["aws:SourceAccount"]);
    assert.match(JSON.stringify(delivery.Condition.ArnLike["aws:SourceArn"]), /:s3:::coderhelm-(prod|\$\{Stage\})-\*/);
  }
  // Native: a part that logs into it depends on the storage part.
  for (const part of ["Web", "Worker"]) {
    const r = rel.parent.Resources[part];
    assert.ok(r.DependsOn.includes("Storage") || JSON.stringify(r.Properties.Parameters).includes('"Storage"'), part);
  }
});

test("the release bucket (native parent): TLS-only, a lifecycle, only the helper writes", () => {
  const R = rel.parent.Resources;
  const st = R.AssetsBucketPolicy.Properties.PolicyDocument.Statement;
  assert.equal(st[0].Sid, "OnlyTheHelperWritesReleaseFiles");
  assert.deepEqual(st[1].Condition, { Bool: { "aws:SecureTransport": "false" } });
  assert.equal(st[1].Effect, "Deny");
  assert.deepEqual(R.AssetsBucket.Properties.LifecycleConfiguration.Rules[0].AbortIncompleteMultipartUpload, { DaysAfterInitiation: 1 });
});

test("CloudFront.5, CloudFront.15: standard logs to the log bucket; the newest TLS policy on a domain", () => {
  const self = one(resources(selfHost, "AWS::CloudFront::Distribution"), "self-host").r.Properties.DistributionConfig;
  assert.deepEqual(self.Logging.Prefix, "cloudfront/");
  assert.equal(self.Logging.IncludeCookies, false);
  assert.match(JSON.stringify(self.Logging.Bucket), /coderhelm-prod-logs-111122223333\.s3\./);
  // Native: one template for every region, and CloudFront delivers no standard
  // logs to a bucket in an opt-in region, so the region decides.
  const nd = native.Web.Resources.Distribution830FAC52.Properties.DistributionConfig;
  assert.equal(nd.Logging["Fn::If"][0], "CloudFrontLogsRegion");
  assert.equal(nd.Logging["Fn::If"][1].Prefix, "cloudfront/");
  assert.deepEqual(nd.Logging["Fn::If"][2], { Ref: "AWS::NoValue" });
  const regions = JSON.stringify(native.Web.Conditions.CloudFrontLogsRegion).match(/"(?:[a-z]+-)+\d"/g)!.map((r) => JSON.parse(r)).sort();
  assert.deepEqual(regions, [...CLOUDFRONT_LOG_REGIONS].sort());
  for (const r of ["af-south-1", "il-central-1", "eu-south-2", "me-central-1", "ap-east-1", "mx-central-1"]) assert.ok(!regions.includes(r), r);
  // A self-host install in an opt-in region: no standard logs (the update would fail).
  const optIn = synth(resolveConfig({ account: "111122223333", region: "eu-south-2", stage: "prod", email: { from: "coderhelm@example.org" }, admins: ["owner@example.org"] }));
  assert.equal(one(resources(optIn, "AWS::CloudFront::Distribution"), "opt-in").r.Properties.DistributionConfig.Logging, undefined);
  // Native: the domain's certificate, when there is one.
  assert.match(JSON.stringify(native.Web.Resources.Distribution830FAC52.Properties.DistributionConfig.ViewerCertificate), /"MinimumProtocolVersion":"TLSv1.2_2025"/);
  const withDomain = synth(resolveConfig({
    account: "111122223333", region: "us-east-1", stage: "prod", email: { from: "coderhelm@example.org" }, admins: ["owner@example.org"],
    domain: { hostname: "coderhelm.example.org", certificateArn: "arn:aws:acm:us-east-1:111122223333:certificate/00000000-0000-0000-0000-000000000000" },
  } as any));
  const vc = one(resources(withDomain, "AWS::CloudFront::Distribution"), "domain").r.Properties.DistributionConfig.ViewerCertificate;
  assert.equal(vc.MinimumProtocolVersion, "TLSv1.2_2025");
});

test("DynamoDB.2, DynamoDB.6: every table has point-in-time recovery and deletion protection", () => {
  for (const [name, t] of [["self-host", selfHost], ["hosted", hosted]] as [string, Record<string, any>][]) {
    const tables = resources(t, "AWS::DynamoDB::GlobalTable");
    assert.ok(tables.length >= 20, `${name}: ${tables.length} tables`);
    for (const x of tables) {
      const rep = x.r.Properties.Replicas[0];
      assert.equal(rep.PointInTimeRecoverySpecification?.PointInTimeRecoveryEnabled, true, `${name} ${x.id}: PITR`);
      assert.equal(rep.DeletionProtectionEnabled, true, `${name} ${x.id}: deletion protection`);
    }
  }
  // Native: deletion protection follows KeepDataOnDelete (the uninstall turns it off with it).
  for (const x of resources(native, "AWS::DynamoDB::GlobalTable")) {
    const rep = x.r.Properties.Replicas[0];
    assert.equal(rep.PointInTimeRecoverySpecification?.PointInTimeRecoveryEnabled, true, x.id);
    assert.deepEqual(rep.DeletionProtectionEnabled, { "Fn::If": ["NativeKeepData", true, false] }, x.id);
  }
});

test("Cognito.3, Cognito.6: a strong password policy and deletion protection on the user pool", () => {
  for (const [name, t] of [...INSTALLS, ["hosted", hosted] as [string, Record<string, any>]]) {
    const p = one(resources(t, "AWS::Cognito::UserPool"), name).r.Properties;
    assert.deepEqual(p.Policies.PasswordPolicy, {
      MinimumLength: 8, RequireLowercase: true, RequireUppercase: true, RequireNumbers: true, RequireSymbols: true, TemporaryPasswordValidityDays: 7,
    }, name);
    // Optional TOTP: an admin can require two-factor for everyone (Settings, Sign-in).
    assert.equal(p.MfaConfiguration, "OPTIONAL", name);
    assert.deepEqual(p.EnabledMfas, ["SOFTWARE_TOKEN_MFA"], name);
    if (name === "native") assert.deepEqual(p.DeletionProtection, { "Fn::If": ["NativeKeepData", "ACTIVE", "INACTIVE"] });
    else assert.equal(p.DeletionProtection, "ACTIVE", name);
  }
});

test("Lambda.2: every function on a supported runtime (no Node.js 20 or older)", () => {
  for (const [name, t] of [...INSTALLS, ["hosted", hosted] as [string, Record<string, any>]]) {
    for (const f of resources(t, "AWS::Lambda::Function")) {
      const rt = f.r.Properties.Runtime;
      if (rt === undefined) continue;
      assert.doesNotMatch(String(rt), /^nodejs(1\d|20)\.x$|^python3\.(\d|1[01])$/, `${name} ${f.where}.${f.id}: ${rt}`);
    }
    const proxy = resources(t, "AWS::Lambda::Function").find((f) => JSON.stringify(f.r.Properties.FunctionName ?? "").includes("-mcp-proxy"))!;
    assert.equal(proxy.r.Properties.Runtime, "nodejs22.x", name);
  }
  assert.match(fs.readFileSync(path.join(INFRA, "..", "lambda", "mcp-proxy", "package.json"), "utf8"), /--target=node22/);
});

test("SES.3: email is only sent over TLS", () => {
  for (const [name, t] of [...INSTALLS, ["hosted", hosted] as [string, Record<string, any>]]) {
    const sets = resources(t, "AWS::SES::ConfigurationSet");
    assert.ok(sets.length >= 1, name);
    // The install's own set; on hosted, the mail relay's too.
    for (const c of sets) assert.equal(c.r.Properties.DeliveryOptions?.TlsPolicy, "REQUIRE", `${name} ${c.where}.${c.id}`);
  }
});

test("KMS.2: the install helper decrypts with the table key by its ARN, never with every key", () => {
  const R = rel.parent.Resources;
  const decrypts = (st: any) => ([] as string[]).concat(st.Action ?? []).some((a) => /^kms:(\*|Decrypt|ReEncrypt\*|ReEncryptFrom)$/.test(a));
  // Its own inline policy uses no key at all.
  for (const s of helperPolicy().Statement) assert.ok(!decrypts(s), `helper ${s.Sid}`);
  const p = R.HelperTableKey;
  assert.equal(p.Type, "AWS::IAM::RolePolicy");
  assert.deepEqual(p.Properties.RoleName, { Ref: "HelperRole" });
  assert.deepEqual(p.Properties.PolicyDocument.Statement, [
    { Sid: "TableKey", Effect: "Allow", Action: HELPER_TABLE_KEY_ACTIONS, Resource: { "Fn::GetAtt": ["Database", "Outputs.TableKeyArn"] } },
  ]);
  // That output is the table key itself, and the allowlist is written only once the policy is there.
  const out = rel.children.Database.Outputs.TableKeyArn.Value["Fn::GetAtt"];
  assert.equal(out[1], "Arn");
  const key = rel.children.Database.Resources[out[0]];
  assert.equal(key.Type, "AWS::KMS::Key");
  assert.equal(key.Properties.Description, "Coderhelm DynamoDB encryption key");
  assert.deepEqual(R.Allowlist.DependsOn, ["HelperTableKey"]);
  // No role the release makes (its parts included) decrypts with every key; the
  // pinned deploy role, which creates the keys, is the one exception (security.md).
  for (const [name, t] of Object.entries<any>(native)) {
    for (const [id, r] of Object.entries<any>(t.Resources ?? {})) {
      if (id === "DeployRole") continue;
      const docs = r.Type === "AWS::IAM::Role" ? (r.Properties.Policies ?? []).map((x: any) => x.PolicyDocument)
        : r.Type === "AWS::IAM::Policy" || r.Type === "AWS::IAM::RolePolicy" ? [r.Properties.PolicyDocument] : [];
      for (const d of docs) {
        for (const s of ([] as any[]).concat(d.Statement)) {
          if (s.Effect !== "Allow" || !decrypts(s)) continue;
          for (const res of ([] as any[]).concat(s.Resource)) {
            const txt = JSON.stringify(res);
            assert.ok(res !== "*" && !/:key\/\*"?$/.test(txt.replace(/"\]+\}*$/, '"')), `${name}.${id}: ${txt}`);
          }
        }
      }
    }
  }
});
