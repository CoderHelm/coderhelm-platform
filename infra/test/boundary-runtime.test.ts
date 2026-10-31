// Every call a native install's roles are given, on the resources they name,
// stays inside the permissions boundary (infra/lib/access.ts): each role's
// identity statements resolved to the ARNs CloudFormation would create for
// stage prod (a table's name, a key's tag, an import's value from the part
// that exports it), then judged with the boundary by test/iam-eval.ts. A
// statement on "*" is judged on what the code calls it on (WHERE_STAR).
// And what the boundary refuses: other people's resources, by service.
//
//   npm test

import { test } from "node:test";
import * as assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { accessNames, boundaryPolicy, INSTALL_TAG, PolicyDocument } from "../lib/access";
import { buildNative } from "../lib/native/build";
import { allowed, list } from "./iam-eval";

const STUBS = fs.mkdtempSync(path.join(os.tmpdir(), "chbrt-"));
for (const d of ["gateway", "worker", "mcp"]) {
  fs.mkdirSync(path.join(STUBS, d), { recursive: true });
  fs.writeFileSync(path.join(STUBS, d, d === "mcp" ? "handler.js" : "bootstrap"), "stub");
}
const DASH = path.join(STUBS, "dash");
fs.mkdirSync(path.join(DASH, "_next"), { recursive: true });
fs.writeFileSync(path.join(DASH, "index.html"), "<html></html>");
process.env.GATEWAY_ZIP = path.join(STUBS, "gateway");
process.env.WORKER_ZIP = path.join(STUBS, "worker");
process.env.MCP_PROXY_DIR = path.join(STUBS, "mcp");

const rel = buildNative("v9.8.7", fs.mkdtempSync(path.join(os.tmpdir(), "chbrtrel-")), { dashboardDir: DASH });

const ACCOUNT = "111122223333";
const REGION = "us-east-1";
const P = "coderhelm-prod";
const EMAIL = "owner@example.org";
const ZONE = "Z0123456789ABCDEFGHIJ";
const OTHER = "444455556666";

const n = { ...accessNames({ account: ACCOUNT, region: REGION, prefix: P, suffix: `-${ACCOUNT}`, secretName: "coderhelm/prod/secrets", id: "stack" }), emailIdentity: EMAIL, zoneId: ZONE };
const BOUNDARY = boundaryPolicy(n, [`arn:aws:iam::${ACCOUNT}:policy/${P}-deploy-boundary-stack`]);
const EVERYTHING: PolicyDocument = { Version: "2012-10-17", Statement: [{ Sid: "All", Effect: "Allow", Action: "*", Resource: "*" }] };
const inside = (action: string, resource: string, context: Record<string, string | string[]> = {}) => allowed([EVERYTHING], BOUNDARY, { action, resource, context });

const PARAMS: Record<string, string> = {
  Stage: "prod", "AWS::Region": REGION, "AWS::AccountId": ACCOUNT, "AWS::Partition": "aws", "AWS::URLSuffix": "amazonaws.com",
  "AWS::StackName": P, RootStackName: P, AssetsBucket: `${P}-release-${ACCOUNT}`, EmailFromResolved: EMAIL, AdminEmail: EMAIL,
  DeployRoleArn: n.deployRoleArn, DomainZoneId: ZONE, StatusId: "0123456789abcdef",
};

interface Resolved { arn: string; tags?: Record<string, string> }

/** A child resource's Ref and its ARN, from its type and name (or, named by id, its logical id and tags). */
function describe(child: string, id: string): { ref: string; arn: string; tags?: Record<string, string>; attrs: Record<string, string> } {
  const t = rel.children[child];
  const r = t.Resources[id];
  if (!r) throw new Error(`${child}: no resource ${id}`);
  const p = r.Properties ?? {};
  const s = (v: any) => str(child, v);
  const at = (svc: string) => `arn:aws:${svc}:${REGION}:${ACCOUNT}`;
  const tags: Record<string, string> = {};
  const rawTags = p.Tags ?? p.UserPoolTags ?? p.DistributionConfig?.Tags;
  if (Array.isArray(rawTags)) for (const x of rawTags) tags[x.Key] = s(x.Value);
  else if (rawTags && typeof rawTags === "object") for (const [k, v] of Object.entries(rawTags)) tags[k] = s(v);
  switch (r.Type) {
    case "AWS::DynamoDB::GlobalTable": case "AWS::DynamoDB::Table": {
      const name = s(p.TableName); const arn = `${at("dynamodb")}:table/${name}`;
      return { ref: name, arn, attrs: { Arn: arn, StreamArn: `${arn}/stream/2026-01-01T00:00:00.000` } };
    }
    case "AWS::SQS::Queue": { const name = s(p.QueueName); const arn = `${at("sqs")}:${name}`; return { ref: `https://sqs.${REGION}.amazonaws.com/${ACCOUNT}/${name}`, arn, attrs: { Arn: arn, QueueName: name } }; }
    case "AWS::SNS::Topic": { const arn = `${at("sns")}:${s(p.TopicName)}`; return { ref: arn, arn, attrs: { TopicArn: arn } }; }
    case "AWS::S3::Bucket": { const name = s(p.BucketName); const arn = `arn:aws:s3:::${name}`; return { ref: name, arn, attrs: { Arn: arn, DomainName: `${name}.s3.amazonaws.com`, RegionalDomainName: `${name}.s3.${REGION}.amazonaws.com` } }; }
    case "AWS::Lambda::Function": { const name = s(p.FunctionName); const arn = `${at("lambda")}:function:${name}`; return { ref: name, arn, attrs: { Arn: arn } }; }
    case "AWS::Lambda::LayerVersion": { const arn = `${at("lambda")}:layer:${s(p.LayerName)}:1`; return { ref: arn, arn, attrs: {} }; }
    case "AWS::KMS::Key": { const arn = `${at("kms")}:key/${id.toLowerCase()}`; return { ref: id.toLowerCase(), arn, tags, attrs: { Arn: arn, KeyId: id.toLowerCase() } }; }
    case "AWS::Cognito::UserPool": { const pid = `${REGION}_${id}`; const arn = `${at("cognito-idp")}:userpool/${pid}`; return { ref: pid, arn, tags, attrs: { Arn: arn, ProviderName: `cognito-idp.${REGION}.amazonaws.com/${pid}` } }; }
    case "AWS::CloudFront::Distribution": { const did = `E${id.toUpperCase().slice(0, 12)}`; const arn = `arn:aws:cloudfront::${ACCOUNT}:distribution/${did}`; return { ref: did, arn, tags, attrs: { Id: did, DomainName: `${did}.cloudfront.net` } }; }
    case "AWS::CodeBuild::Project": { const name = s(p.Name); const arn = `${at("codebuild")}:project/${name}`; return { ref: name, arn, attrs: { Arn: arn } }; }
    case "AWS::Logs::LogGroup": { const name = s(p.LogGroupName); const arn = `${at("logs")}:log-group:${name}:*`; return { ref: name, arn, attrs: { Arn: arn } }; }
    case "AWS::SecretsManager::Secret": { const arn = `${at("secretsmanager")}:secret:${s(p.Name)}-AbCdEf`; return { ref: arn, arn, attrs: { Id: arn } }; }
    case "AWS::SSM::Parameter": { const name = s(p.Name); return { ref: name, arn: `${at("ssm")}:parameter${name}`, attrs: { Value: "x" } }; }
    case "AWS::IAM::Role": { const name = s(p.RoleName); const arn = `arn:aws:iam::${ACCOUNT}:role/${name}`; return { ref: name, arn, attrs: { Arn: arn } }; }
    case "AWS::Scheduler::ScheduleGroup": { const arn = `${at("scheduler")}:schedule-group/${s(p.Name)}`; return { ref: s(p.Name), arn, attrs: { Arn: arn } }; }
    case "AWS::Events::Rule": { const arn = `${at("events")}:rule/${s(p.Name)}`; return { ref: s(p.Name), arn, attrs: { Arn: arn } }; }
    case "AWS::ApiGatewayV2::Api": return { ref: "abcd1234", arn: `arn:aws:apigateway:${REGION}::/apis/abcd1234`, tags, attrs: { ApiEndpoint: "https://abcd1234.execute-api.amazonaws.com", ApiId: "abcd1234" } };
    case "AWS::Cognito::UserPoolClient": return { ref: "client123", arn: "", attrs: {} };
    default: throw new Error(`${child}.${id}: no ARN for ${r.Type}`);
  }
}

/** A value with every intrinsic resolved to a string (imports through the part that exports them). */
function str(child: string, v: any): string {
  if (typeof v === "string") return v;
  if (typeof v === "number") return String(v);
  if (!v || typeof v !== "object") throw new Error(`${child}: can't resolve ${JSON.stringify(v)}`);
  const t = rel.children[child];
  if ("Ref" in v) {
    const x = v.Ref;
    if (x in PARAMS) return PARAMS[x];
    if (t.Parameters?.[x] && x.startsWith("Imp")) return imported(child, x);
    if (t.Resources?.[x]) return describe(child, x).ref;
    throw new Error(`${child}: Ref ${x}`);
  }
  if ("Fn::GetAtt" in v) {
    const [id, attr] = v["Fn::GetAtt"];
    const d = describe(child, id);
    if (!(attr in d.attrs)) throw new Error(`${child}: GetAtt ${id}.${attr}`);
    return d.attrs[attr];
  }
  if ("Fn::Join" in v) return (v["Fn::Join"][1] as any[]).map((x) => str(child, x)).join(v["Fn::Join"][0]);
  if ("Fn::Sub" in v) {
    const [text, vars] = typeof v["Fn::Sub"] === "string" ? [v["Fn::Sub"], {}] : v["Fn::Sub"];
    return text.replace(/\$\{([^}]+)\}/g, (_: string, name: string) => {
      if (name in vars) return str(child, vars[name]);
      if (name.includes(".")) { const [id, attr] = name.split("."); return str(child, { "Fn::GetAtt": [id, attr] }); }
      return str(child, { Ref: name });
    });
  }
  if ("Fn::Select" in v) { const [i, arr] = v["Fn::Select"]; const xs = Array.isArray(arr) ? arr : strList(child, arr); return str(child, xs[Number(i)]); }
  if ("Fn::If" in v) return str(child, v["Fn::If"][1]);
  throw new Error(`${child}: can't resolve ${JSON.stringify(v).slice(0, 200)}`);
}

function strList(child: string, v: any): string[] {
  if ("Fn::Split" in v) return str(child, v["Fn::Split"][1]).split(v["Fn::Split"][0]);
  throw new Error(`${child}: can't resolve a list ${JSON.stringify(v).slice(0, 200)}`);
}

/** An import parameter's value: the parent feeds it from another part's output. */
function imported(child: string, param: string): string {
  const feed = rel.parent.Resources[child].Properties.Parameters[param];
  const [from, out] = feed["Fn::GetAtt"];
  const name = out.replace(/^Outputs\./, "");
  return str(from, rel.children[from].Outputs[name].Value);
}

/** The tags of the resource behind an ARN a child names (a key, user pool or distribution), for aws:ResourceTag. */
function tagsOf(arn: string): Record<string, string> {
  for (const [child, t] of Object.entries(rel.children)) {
    for (const [id, r] of Object.entries<any>(t.Resources)) {
      if (!["AWS::KMS::Key", "AWS::Cognito::UserPool", "AWS::CloudFront::Distribution"].includes(r.Type)) continue;
      const d = describe(child, id);
      if (arn === d.arn || arn.startsWith(d.arn + "/")) return d.tags ?? {};
    }
  }
  return {};
}

/**
 * What the code calls a "*" statement on, by action: the install's own
 * resources (its email identity, log groups, distribution, certificate) or
 * calls that take no resource.
 */
const DIST = (() => {
  const [id] = Object.entries<any>(rel.children.Web.Resources).find(([, r]) => r.Type === "AWS::CloudFront::Distribution")!;
  return describe("Web", id);
})();
const WHERE_STAR: Record<string, { resource: string; context?: Record<string, string | string[]> }[]> = {
  "ses:SendEmail": [{ resource: `arn:aws:ses:${REGION}:${ACCOUNT}:identity/${EMAIL}` }, { resource: `arn:aws:ses:${REGION}:${ACCOUNT}:identity/someone@example.com` }, { resource: `arn:aws:ses:${REGION}:${ACCOUNT}:configuration-set/${P}-email` }],
  "ses:SendTemplatedEmail": [{ resource: `arn:aws:ses:${REGION}:${ACCOUNT}:identity/${EMAIL}` }],
  "ses:GetAccount": [{ resource: "*" }],
  "ses:GetEmailIdentity": [{ resource: `arn:aws:ses:${REGION}:${ACCOUNT}:identity/${EMAIL}` }],
  "ses:GetEmailTemplate": [{ resource: `arn:aws:ses:${REGION}:${ACCOUNT}:template/${P}-welcome` }],
  "logs:PutRetentionPolicy": [{ resource: `arn:aws:logs:${REGION}:${ACCOUNT}:log-group:/aws/lambda/${P}-gateway` }],
  "logs:DeleteRetentionPolicy": [{ resource: `arn:aws:logs:${REGION}:${ACCOUNT}:log-group:/aws/lambda/${P}-gateway` }],
  "cloudfront:CreateInvalidation": [{ resource: DIST.arn, context: { [`aws:ResourceTag/${INSTALL_TAG}`]: P } }],
  "cloudfront:GetInvalidation": [{ resource: DIST.arn, context: { [`aws:ResourceTag/${INSTALL_TAG}`]: P } }],
  "acm:RequestCertificate": [{ resource: "*", context: { "aws:RequestTag/coderhelm": P, "aws:TagKeys": ["coderhelm"] } }],
  "route53:ListHostedZones": [{ resource: "*" }],
  "route53:GetHostedZone": [{ resource: `arn:aws:route53:::hostedzone/${ZONE}` }, { resource: "arn:aws:route53:::hostedzone/ZOTHERZONE" }],
};

/** Every Allow statement of every role in the parts and the parent, with the role's name. */
function roleStatements(): { role: string; child: string; action: string; resource: any }[] {
  const out: { role: string; child: string; action: string; resource: any }[] = [];
  for (const [child, t] of Object.entries(rel.children)) {
    for (const [id, r] of Object.entries<any>(t.Resources)) {
      let docs: [string, any][] = [];
      if (r.Type === "AWS::IAM::Role") docs = (r.Properties.Policies ?? []).map((p: any) => [id, p.PolicyDocument]);
      else if (r.Type === "AWS::IAM::Policy" || (r.Type === "AWS::IAM::ManagedPolicy" && !JSON.stringify(r.Properties.ManagedPolicyName ?? "").includes("boundary"))) {
        docs = (r.Properties.Roles ?? [{ Ref: id }]).map((x: any) => [x.Ref ?? id, r.Properties.PolicyDocument]);
      }
      for (const [role, d] of docs) {
        for (const st of d.Statement) {
          if (st.Effect !== "Allow") continue;
          for (const action of list(st.Action)) for (const resource of [].concat(st.Resource)) out.push({ role, child, action, resource });
        }
      }
    }
  }
  return out;
}

/** An action pattern as one concrete call ("dynamodb:Get*" -> "dynamodb:GetX"). */
const concrete = (a: string) => a.replace(/\*/g, "X");

test("every call each role of a native install is given, on what it names, is inside the boundary", () => {
  const outside: string[] = [];
  let judged = 0;
  for (const s of roleStatements()) {
    if (s.resource === "*") {
      const uses = WHERE_STAR[s.action];
      if (!uses) { outside.push(`${s.child}.${s.role}: ${s.action} on "*" has no WHERE_STAR entry`); continue; }
      for (const u of uses) { judged++; if (!inside(s.action, u.resource, u.context)) outside.push(`${s.child}.${s.role}: ${s.action} on ${u.resource}`); }
      continue;
    }
    let arn: string;
    try { arn = str(s.child, s.resource); } catch (e) { outside.push(`${s.child}.${s.role}: ${(e as Error).message}`); continue; }
    // A role in another account (the log reader there) or AWS's own models and layers.
    const context: Record<string, string | string[]> = {};
    const tags = tagsOf(arn);
    for (const [k, v] of Object.entries(tags)) context[`aws:ResourceTag/${k}`] = v;
    if (arn.startsWith("arn:aws:iam::")) {
      const acct = arn.split(":")[4];
      context["aws:ResourceAccount"] = acct === "*" ? OTHER : acct;
      if (s.action.startsWith("sts:")) arn = arn.replace("::*:", `::${OTHER}:`);
    }
    judged++;
    if (!inside(concrete(s.action), arn.replace(/\$\{[^}]+\}/g, "x"), context)) outside.push(`${s.child}.${s.role}: ${s.action} on ${arn}`);
  }
  assert.deepEqual(outside, []);
  assert.ok(judged > 300, `${judged} calls judged`);
});

// ── What it refuses: the account's other resources ──────────────────────

const ELSE = (svc: string, rest: string) => `arn:aws:${svc}:${REGION}:${ACCOUNT}:${rest}`;
const REFUSED: [string, string, Record<string, string | string[]>?][] = [
  // Data in other tables, buckets, parameters, secrets and functions.
  ["dynamodb:GetItem", ELSE("dynamodb", "table/orders")],
  ["dynamodb:Scan", ELSE("dynamodb", "table/coderhelm-prod2-runs")],
  ["dynamodb:PutResourcePolicy", ELSE("dynamodb", "table/orders")],
  ["s3:GetObject", "arn:aws:s3:::customer-data/report.csv"],
  ["s3:GetObject", `arn:aws:s3:::customer-data/${P}-x`],
  ["s3:PutBucketPolicy", "arn:aws:s3:::customer-data"],
  ["ssm:GetParameter", ELSE("ssm", "parameter/prod/db-password")],
  ["ssm:GetParameter", ELSE("ssm", "parameter/coderhelm-prod2/x")],
  ["secretsmanager:GetSecretValue", ELSE("secretsmanager", "secret:prod/db-AbCdEf")],
  ["secretsmanager:PutResourcePolicy", ELSE("secretsmanager", "secret:prod/db-AbCdEf")],
  ["lambda:GetFunction", ELSE("lambda", "function:payments")],
  ["lambda:ListFunctions", "*"],
  ["lambda:UpdateFunctionCode", ELSE("lambda", "function:payments")],
  ["lambda:AddPermission", ELSE("lambda", "function:payments")],
  // Builds: another project's build would run as that project's role.
  ["codebuild:StartBuild", ELSE("codebuild", "project/deploy-prod")],
  ["codebuild:UpdateProject", ELSE("codebuild", "project/deploy-prod")],
  ["codebuild:RetryBuild", ELSE("codebuild", "project/deploy-prod")],
  ["codebuild:StartBuildBatch", ELSE("codebuild", "project/deploy-prod")],
  // Resource policies that would open something to another account.
  ["sqs:SetQueueAttributes", ELSE("sqs", "orders")],
  ["sqs:AddPermission", ELSE("sqs", "orders")],
  ["sns:SetTopicAttributes", ELSE("sns", "alerts")],
  ["sns:Subscribe", ELSE("sns", "alerts")],
  ["logs:PutSubscriptionFilter", ELSE("logs", "log-group:/aws/lambda/payments")],
  ["logs:PutResourcePolicy", "*"],
  ["events:PutRule", ELSE("events", "rule/nightly")],
  // Keys, user pools, distributions, certificates that aren't the install's.
  ["kms:PutKeyPolicy", ELSE("kms", "key/1111"), { "aws:ResourceTag/team": "payments" }],
  ["kms:CreateGrant", ELSE("kms", "key/1111")],
  ["kms:ScheduleKeyDeletion", ELSE("kms", "key/1111")],
  ["kms:DisableKey", ELSE("kms", "key/1111"), { [`aws:ResourceTag/${INSTALL_TAG}`]: "coderhelm-prod2" }],
  ["kms:TagResource", ELSE("kms", "key/1111"), { [`aws:RequestTag/${INSTALL_TAG}`]: P, "aws:TagKeys": [INSTALL_TAG] }],
  ["cognito-idp:ListUsers", ELSE("cognito-idp", "userpool/us-east-1_abc")],
  ["cognito-idp:CreateUserPoolClient", ELSE("cognito-idp", "userpool/us-east-1_abc")],
  ["cognito-idp:TagResource", ELSE("cognito-idp", "userpool/us-east-1_abc"), { [`aws:RequestTag/${INSTALL_TAG}`]: P }],
  ["cloudfront:UpdateDistribution", `arn:aws:cloudfront::${ACCOUNT}:distribution/EOTHER`],
  ["cloudfront:TagResource", `arn:aws:cloudfront::${ACCOUNT}:distribution/EOTHER`, { [`aws:RequestTag/${INSTALL_TAG}`]: P }],
  ["acm:DeleteCertificate", "arn:aws:acm:us-east-1:111122223333:certificate/other"],
  ["acm:ExportCertificate", "arn:aws:acm:us-east-1:111122223333:certificate/other", { "aws:ResourceTag/coderhelm": P }],
  // DNS in other zones, and email identities other than the install's.
  ["route53:ChangeResourceRecordSets", "arn:aws:route53:::hostedzone/ZOTHERZONE", { "route53:ChangeResourceRecordSetsNormalizedRecordNames": ["www.example.com"], "route53:ChangeResourceRecordSetsRecordTypes": ["A"] }],
  ["route53:DeleteHostedZone", "arn:aws:route53:::hostedzone/ZOTHERZONE"],
  ["ses:DeleteEmailIdentity", `arn:aws:ses:${REGION}:${ACCOUNT}:identity/example.com`],
  ["ses:PutEmailIdentityFeedbackAttributes", `arn:aws:ses:${REGION}:${ACCOUNT}:identity/example.com`],
  ["ses:CreateEmailIdentityPolicy", `arn:aws:ses:${REGION}:${ACCOUNT}:identity/example.com`],
  // Another install's stacks, and other people's.
  ["cloudformation:UpdateStack", ELSE("cloudformation", "stack/coderhelm-prod2/x")],
  ["cloudformation:CreateChangeSet", ELSE("cloudformation", "stack/coderhelm-prodx-Api-1/x")],
  ["cloudformation:DeleteStack", ELSE("cloudformation", "stack/payments/x")],
  // Roles in other accounts other than the read roles.
  ["sts:AssumeRole", `arn:aws:iam::${OTHER}:role/OrganizationAccountAccessRole`, { "aws:ResourceAccount": OTHER }],
  ["sts:AssumeRole", `arn:aws:iam::${OTHER}:role/admin`, { "aws:ResourceAccount": OTHER }],
  // No APIs other than HTTP APIs (REST APIs are left alone).
  ["apigateway:PATCH", `arn:aws:apigateway:${REGION}::/restapis/abc`],
];

test("the boundary refuses the account's other resources, service by service", () => {
  const wrongly = REFUSED.filter(([a, r, c]) => inside(a, r, c ?? {})).map(([a, r]) => `${a} on ${r}`);
  assert.deepEqual(wrongly, []);
});

test("and allows the same calls on the install's own", () => {
  const own: [string, string, Record<string, string | string[]>?][] = [
    ["dynamodb:GetItem", ELSE("dynamodb", `table/${P}-runs`)],
    ["dynamodb:Query", ELSE("dynamodb", `table/${P}/index/by-team`)],
    ["s3:GetObject", `arn:aws:s3:::${P}-data-${ACCOUNT}/runs/x`],
    ["ssm:GetParameter", ELSE("ssm", `parameter/${P}/public-url`)],
    ["secretsmanager:GetSecretValue", ELSE("secretsmanager", "secret:coderhelm/prod/secrets-AbCdEf")],
    ["lambda:InvokeFunction", ELSE("lambda", `function:${P}-worker`)],
    ["codebuild:StartBuild", ELSE("codebuild", `project/${P}-release-check`)],
    ["sqs:SendMessage", ELSE("sqs", `${P}-tickets`)],
    ["kms:PutKeyPolicy", ELSE("kms", "key/2222"), { [`aws:ResourceTag/${INSTALL_TAG}`]: P }],
    ["kms:TagResource", ELSE("kms", "key/2222"), { [`aws:ResourceTag/${INSTALL_TAG}`]: P, [`aws:RequestTag/${INSTALL_TAG}`]: P }],
    ["kms:CreateKey", "*", { [`aws:RequestTag/${INSTALL_TAG}`]: P }],
    ["cognito-idp:AdminCreateUser", ELSE("cognito-idp", "userpool/us-east-1_own"), { [`aws:ResourceTag/${INSTALL_TAG}`]: P }],
    ["acm:DeleteCertificate", "arn:aws:acm:us-east-1:111122223333:certificate/own", { "aws:ResourceTag/coderhelm": P }],
    ["route53:ChangeResourceRecordSets", `arn:aws:route53:::hostedzone/${ZONE}`, { "route53:ChangeResourceRecordSetsRecordTypes": ["A"] }],
    ["route53:ChangeResourceRecordSets", "arn:aws:route53:::hostedzone/ZOTHERZONE", { "route53:ChangeResourceRecordSetsNormalizedRecordNames": ["_abc.ci.example.com"], "route53:ChangeResourceRecordSetsRecordTypes": ["CNAME"] }],
    ["ses:CreateEmailIdentity", `arn:aws:ses:${REGION}:${ACCOUNT}:identity/${EMAIL}`],
    ["cloudformation:UpdateStack", ELSE("cloudformation", `stack/${P}/x`)],
    ["cloudformation:CreateChangeSet", ELSE("cloudformation", `stack/${P}-Api-1ABC/x`)],
    ["sts:AssumeRole", `arn:aws:iam::${OTHER}:role/CoderHelmLogReader`, { "aws:ResourceAccount": OTHER }],
  ];
  const refused = own.filter(([a, r, c]) => !inside(a, r, c ?? {})).map(([a, r]) => `${a} on ${r}`);
  assert.deepEqual(refused, []);
});
