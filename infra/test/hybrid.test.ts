// The hosted side of hybrid self-hosting and what installs get from it:
// the provisioning API may change only {name}.coderhelm.com CNAMEs (and the
// setup-page alias), never a reserved name; every record CoderHelm itself
// serves in the zone is reserved; the wildcard fallback is A/AAAA only;
// installs serve their own 404; nothing trusts customer subdomains.
//
//   npm test

import { test } from "node:test";
import * as assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import * as cdk from "aws-cdk-lib";
import { Template } from "aws-cdk-lib/assertions";
import { buildApp } from "../lib/app";
import { loadConfigFile, resolveConfig, ResolvedConfig } from "../lib/config";
import { FALLBACK_FUNCTION, reservedNames } from "../lib/hybrid";

const INFRA = path.join(__dirname, "..", "..");
const STUBS = fs.mkdtempSync(path.join(os.tmpdir(), "chhybrid-"));
for (const d of ["gateway", "worker", "mcp"]) {
  fs.mkdirSync(path.join(STUBS, d), { recursive: true });
  fs.writeFileSync(path.join(STUBS, d, d === "mcp" ? "handler.js" : "bootstrap"), "stub");
}
process.env.GATEWAY_ZIP = path.join(STUBS, "gateway");
process.env.WORKER_ZIP = path.join(STUBS, "worker");
process.env.MCP_PROXY_DIR = path.join(STUBS, "mcp");

const ACCOUNT = "654654210434";
const context = { [`hosted-zone:account=${ACCOUNT}:domainName=coderhelm.com:region=us-east-1`]: { Id: "/hostedzone/ZTEST000001", Name: "coderhelm.com." } };

function synth(cfg: ResolvedConfig): Record<string, Template> {
  const app = new cdk.App({ outdir: fs.mkdtempSync(path.join(os.tmpdir(), "chout-")), context });
  buildApp(app, cfg);
  const out: Record<string, Template> = {};
  for (const s of app.node.children) if (s instanceof cdk.Stack) out[s.stackName] = Template.fromStack(s);
  return out;
}

const hostedCfg = () => loadConfigFile(path.join(INFRA, "config", "hosted.prod.json"), { CDK_DEFAULT_ACCOUNT: ACCOUNT });
let hostedCache: Record<string, Template> | null = null;
const hosted = () => (hostedCache ??= synth(hostedCfg()));

function statements(t: Template): any[] {
  const out: any[] = [];
  for (const type of ["AWS::IAM::Policy", "AWS::IAM::ManagedPolicy"]) {
    for (const r of Object.values(t.findResources(type))) {
      out.push(...((r as any).Properties.PolicyDocument.Statement as any[]));
    }
  }
  return out;
}

function str(v: unknown): string {
  return JSON.stringify(v);
}

test("the provisioning API may change only CNAMEs (and the setup alias) under the zone, in that zone only", () => {
  const api = hosted()["coderhelm-prod-api"];
  const all = statements(api).filter((s) => [].concat(s.Action).some((a: string) => a.startsWith("route53:Change")));
  assert.equal(all.length, 3, "one allow and two denies");
  const allow = all.find((s) => s.Effect === "Allow");
  const deny = all.find((s) => s.Effect === "Deny" && s.Condition["ForAnyValue:StringEquals"]);
  const deep = all.find((s) => s.Effect === "Deny" && s.Condition["ForAnyValue:StringLike"]);
  assert.equal(allow.Action, "route53:ChangeResourceRecordSets");
  assert.match(str(allow.Resource), /hostedzone\/ZTEST000001/);
  assert.ok(!str(allow.Resource).includes("hostedzone/*"), "never every zone");
  assert.deepEqual(allow.Condition["ForAllValues:StringEquals"]["route53:ChangeResourceRecordSetsRecordTypes"], ["CNAME", "A", "AAAA"]);
  assert.deepEqual(allow.Condition["ForAllValues:StringLike"]["route53:ChangeResourceRecordSetsNormalizedRecordNames"], ["*.coderhelm.com"]);
  const denied: string[] = deny.Condition["ForAnyValue:StringEquals"]["route53:ChangeResourceRecordSetsNormalizedRecordNames"];
  for (const n of ["coderhelm.com", "*.coderhelm.com", "api.coderhelm.com", "app.coderhelm.com", "auth.coderhelm.com", "docs.coderhelm.com", "stream.coderhelm.com", "www.coderhelm.com", "mail.coderhelm.com", "status.coderhelm.com", "admin.coderhelm.com", "careers.coderhelm.com"]) {
    assert.ok(denied.includes(n), `${n} is denied`);
  }
  // IAM's `*` crosses dots, so the allow alone would let a change reach
  // deeper names: under a reserved host (`x.api.coderhelm.com`, a
  // `_acme-challenge.app.coderhelm.com` that could get a certificate for the
  // dashboard), mail records (`_domainkey`, `_dmarc`), or three labels deep.
  // A separate deny fences those (a second condition in the same statement
  // would AND with the first and fence nothing).
  assert.ok(deep, "a deny for deeper names");
  const like: string[] = deep.Condition["ForAnyValue:StringLike"]["route53:ChangeResourceRecordSetsNormalizedRecordNames"];
  const matches = (pattern: string, name: string) =>
    new RegExp("^" + pattern.split("*").map((p) => p.replace(/[.?+^$()[\]{}|\\]/g, "\\$&")).join(".*") + "$").test(name);
  for (const n of ["x.api.coderhelm.com", "_acme-challenge.app.coderhelm.com", "_0123456789abcdef0123456789abcdef.app.coderhelm.com", "s1._domainkey.coderhelm.com", "_dmarc.coderhelm.com", "_dmarc.example.coderhelm.com", "a.b.example.coderhelm.com", "_acme-challenge.example.coderhelm.com", "_mta-sts.coderhelm.com"]) {
    assert.ok(like.some((p) => matches(p, n)), `${n} is fenced`);
  }
  for (const n of ["example.coderhelm.com", "_0123456789abcdef0123456789abcdef.example.coderhelm.com"]) {
    assert.ok(!like.some((p) => matches(p, n)), `${n} stays writable`);
  }
  // No other statement anywhere grants record changes.
  for (const t of Object.values(hosted())) {
    for (const s of statements(t)) {
      if (s === allow || s === deny || s === deep) continue;
      assert.ok(![].concat(s.Action).some((a: string) => a === "route53:ChangeResourceRecordSets" || a === "route53:*") || t !== api, "only the hybrid statements");
    }
  }
});

test("every record CoderHelm serves in the zone is reserved (a new one must be added to reserved_names.json)", () => {
  const { zoneRecords } = reservedNames();
  let seen = 0;
  for (const [stack, t] of Object.entries(hosted())) {
    for (const r of Object.values(t.findResources("AWS::Route53::RecordSet"))) {
      const p = (r as any).Properties;
      const name = String(typeof p.Name === "string" ? p.Name : str(p.Name)).replace(/\.$/, "");
      if (!name.endsWith("coderhelm.com")) continue;
      seen++;
      const label = name.slice(0, -"coderhelm.com".length).replace(/\.$/, "");
      if (label === "*") continue; // the fallback wildcard
      if (label === "" || label.startsWith("_")) continue; // apex and service records can't be claimed anyway
      const first = label.split(".").pop()!;
      assert.ok(zoneRecords.includes(first), `${stack}: ${name} isn't in reserved_names.json zone_records`);
    }
  }
  assert.ok(seen >= 4, "found the hosted records (api, auth, stream, wildcard)");
});

test("the wildcard is A and AAAA only, to the fallback site with a *.coderhelm.com certificate", () => {
  const api = hosted()["coderhelm-prod-api"];
  const recs = Object.values(api.findResources("AWS::Route53::RecordSet")).map((r: any) => r.Properties);
  const wild = recs.filter((p) => String(p.Name).startsWith("*.coderhelm.com"));
  assert.deepEqual(wild.map((p) => p.Type).sort(), ["A", "AAAA"]);
  assert.ok(wild.every((p) => p.AliasTarget), "aliases to CloudFront");
  // Explicit records stay explicit (a wildcard never replaces them).
  for (const p of recs) {
    if (String(p.Name).startsWith("*.")) continue;
    assert.ok(!String(p.Name).includes("*"), `${p.Name} is explicit`);
  }
  api.hasResourceProperties("AWS::CertificateManager::Certificate", { DomainName: "*.coderhelm.com", ValidationMethod: "DNS" });
  const dists = Object.values(api.findResources("AWS::CloudFront::Distribution")).map((r: any) => r.Properties.DistributionConfig);
  const fb = dists.find((d) => str(d.Aliases).includes("*.coderhelm.com"));
  assert.ok(fb, "the fallback distribution");
  assert.match(str(fb.DefaultCacheBehavior.AllowedMethods), /GET/);
  assert.ok(!str(fb.DefaultCacheBehavior.AllowedMethods).includes("POST"), "read-only");
  const env = Object.values(api.findResources("AWS::Lambda::Function")).map((r: any) => r.Properties).find((p) => p.FunctionName === "coderhelm-prod-gateway").Environment.Variables;
  assert.ok(env.HYBRID_ZONE_ID && env.HYBRID_FALLBACK_DOMAIN && env.HYBRID_ZONE_NAME === "coderhelm.com");
  assert.ok(!env.HYBRID_URL, "the hosted side doesn't talk to itself");
});

test("the fallback site keeps the viewer's host and asks only for the fallback page", () => {
  const handler = new Function(`${FALLBACK_FUNCTION}; return handler;`)();
  const out = handler({ request: { uri: "/some/path", querystring: { a: { value: "1" } }, headers: { host: { value: "example.coderhelm.com" } } } });
  assert.equal(out.uri, "/hybrid/fallback");
  assert.equal(out.headers["x-coderhelm-host"].value, "example.coderhelm.com");
  assert.equal(out.headers["x-coderhelm-path"].value, "/some/path");
  assert.deepEqual(out.querystring, {});
});

test("hosted CORS trusts exact origins only, never a coderhelm.com wildcard", () => {
  const cfg = hostedCfg();
  for (const o of [...cfg.split!.corsOrigins, ...cfg.split!.streamCorsOrigins]) {
    assert.ok(!o.includes("*"), `${o} is exact`);
    assert.match(o, /^https:\/\/(app\.|www\.)?coderhelm\.com$/);
  }
  const { zoneRecords } = reservedNames();
  for (const o of cfg.split!.corsOrigins) {
    const host = new URL(o).hostname;
    const label = host === "coderhelm.com" ? "" : host.split(".")[0];
    assert.ok(label === "" || zoneRecords.includes(label), `${host} can never be claimed`);
  }
});

test("relay bodies held for retries are gone after two days", () => {
  const storage = hosted()["coderhelm-prod-storage"];
  const bucket = Object.values(storage.findResources("AWS::S3::Bucket"))[0] as any;
  const rule = bucket.Properties.LifecycleConfiguration.Rules.find((r: any) => r.Prefix === "relay/");
  assert.equal(rule.ExpirationInDays, 2);
});

test("a self-hosted install: relay intake, its own 404, no hosted-side parts", () => {
  const cfg = resolveConfig({ account: "111122223333", region: "us-west-2", stage: "prod", email: { from: "coderhelm@example.org" }, admins: ["owner@example.org"] });
  const t = synth(cfg);
  const api = t["coderhelm-prod-api"];
  const routes = Object.values(api.findResources("AWS::ApiGatewayV2::Route")).map((r: any) => r.Properties.RouteKey);
  assert.ok(routes.includes("POST /webhooks/relay") && routes.includes("GET /webhooks/relay/proof"));
  assert.ok(!routes.some((r: string) => r.includes("/hybrid/")), "no hosted API on an install");
  for (const s of statements(api)) assert.ok(![].concat(s.Action).some((a: string) => a.startsWith("route53:Change")), "no zone rights");
  const env = Object.values(api.findResources("AWS::Lambda::Function")).map((r: any) => r.Properties).find((p) => p.FunctionName === "coderhelm-prod-gateway").Environment.Variables;
  // The default hosted API is built in; nothing of the hosted deployment is in an install's config.
  assert.equal(env.HYBRID_URL, undefined);
  const web = t["coderhelm-prod-web"];
  const dist = Object.values(web.findResources("AWS::CloudFront::Distribution"))[0] as any;
  assert.deepEqual(dist.Properties.DistributionConfig.CustomErrorResponses, [
    { ErrorCachingMinTTL: 60, ErrorCode: 404, ResponseCode: 404, ResponsePagePath: "/_errors/404.html" },
  ]);
  assert.match(str(web.toJSON()), /s3:ListBucket/, "missing files are 404s, not 403s");
  // hybridUrl: null turns hybrid off.
  const off = synth(resolveConfig({ account: "111122223333", region: "us-west-2", stage: "prod", email: { from: "coderhelm@example.org" }, admins: ["owner@example.org"], hybridUrl: null }));
  const offEnv = Object.values(off["coderhelm-prod-api"].findResources("AWS::Lambda::Function")).map((r: any) => r.Properties).find((p) => p.FunctionName === "coderhelm-prod-gateway").Environment.Variables;
  assert.equal(offEnv.HYBRID_URL, "off");
  const staging = synth(resolveConfig({ account: "111122223333", region: "us-west-2", stage: "prod", email: { from: "coderhelm@example.org" }, admins: ["owner@example.org"], hybridUrl: "https://api.staging.example.org" }));
  const stEnv = Object.values(staging["coderhelm-prod-api"].findResources("AWS::Lambda::Function")).map((r: any) => r.Properties).find((p) => p.FunctionName === "coderhelm-prod-gateway").Environment.Variables;
  assert.equal(stEnv.HYBRID_URL, "https://api.staging.example.org");
});

test("hybrid config is checked", () => {
  assert.throws(() => resolveConfig({ account: "111122223333", region: "us-west-2", stage: "prod", email: { from: "a@example.org" }, hybrid: { zone: "coderhelm.com" } }), /layout split/);
  assert.throws(() => resolveConfig({ account: "111122223333", region: "us-east-1", stage: "prod", email: { from: "a@example.org" }, hybridUrl: "http://insecure.example.org" }), /hybridUrl/);
  assert.equal(hostedCfg().hybrid!.zone, "coderhelm.com");
  assert.equal(hostedCfg().hybridUrl, null);
});

test("the name check: every 15 minutes, the worker reads the zone and parks with the API's own fences", () => {
  const api = hosted()["coderhelm-prod-api"];
  const rule = Object.values(api.findResources("AWS::Events::Rule")).find((r: any) => r.Properties.Name === "coderhelm-prod-hybrid-name-check") as any;
  assert.ok(rule, "the schedule exists on the hosted side");
  assert.equal(rule.Properties.ScheduleExpression, "rate(15 minutes)");
  assert.match(str(rule.Properties.Targets), /hybrid_name_check/);

  const worker = hosted()["coderhelm-prod-worker"];
  const st = statements(worker);
  const read = st.find((s) => s.Sid === "HybridNameCheckRead");
  assert.equal(read.Action, "route53:ListResourceRecordSets");
  assert.match(str(read.Resource), /hostedzone\/ZTEST000001/);
  const change = st.filter((s) => ([] as string[]).concat(s.Action).includes("route53:ChangeResourceRecordSets"));
  // The same three statements the provisioning API has: never the apex, the
  // wildcard, a reserved name or anything deeper.
  const apiChange = statements(api).filter((s) => ([] as string[]).concat(s.Action).includes("route53:ChangeResourceRecordSets"));
  assert.deepEqual(change.map((s) => s.Sid).sort(), ["HybridNameRecords", "HybridNeverDeeper", "HybridNeverReserved"]);
  assert.deepEqual(change.map((s) => str(s.Condition)).sort(), apiChange.map((s) => str(s.Condition)).sort());
  const env = Object.values(worker.findResources("AWS::Lambda::Function")).map((r: any) => r.Properties).find((p) => p.FunctionName === "coderhelm-prod-worker").Environment.Variables;
  assert.equal(env.HYBRID_ZONE_ID, "ZTEST000001");
  assert.ok(env.HYBRID_FALLBACK_DOMAIN, "the fallback site's address");

  const mon = hosted()["coderhelm-prod-monitoring"];
  const alarms = Object.values(mon.findResources("AWS::CloudWatch::Alarm")).map((r: any) => r.Properties.AlarmName);
  assert.ok(alarms.includes("coderhelm-prod-hybrid-name-parked") && alarms.includes("coderhelm-prod-hybrid-name-unknown-record"));
});

test("a self-hosted install has no name check and no zone rights in its worker", () => {
  const t = synth(resolveConfig({ account: "111122223333", region: "us-west-2", stage: "prod", email: { from: "coderhelm@example.org" }, admins: ["owner@example.org"] }));
  assert.ok(!Object.values(t["coderhelm-prod-api"].findResources("AWS::Events::Rule")).some((r: any) => r.Properties.Name === "coderhelm-prod-hybrid-name-check"));
  for (const s of statements(t["coderhelm-prod-worker"])) assert.ok(![].concat(s.Action).some((a: string) => a.startsWith("route53:")), "no zone rights");
  const alarms = Object.values(t["coderhelm-prod-monitoring"].findResources("AWS::CloudWatch::Alarm")).map((r: any) => r.Properties.AlarmName);
  assert.ok(!alarms.some((a: string) => a.includes("hybrid-name")));
});
