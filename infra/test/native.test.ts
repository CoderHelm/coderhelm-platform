// A native release (infra/lib/native, docs DESIGN): one parent template and
// one nested template per part, synthesized from the same CDK app, with the
// release's files pinned by sha256.
//
// Checks: no sentinel survives; every template is within CloudFormation's
// limits; every child parameter is fed by the parent; data follows
// KeepDataOnDelete; every role carries the boundary; the helper's and the
// auto-update role's permissions are what security.md says; the update
// build and deploy window are gone; and, offline, that today's install
// nests into the parent with every table, bucket, key and user pool kept
// (same logical id, same create-only properties).
//
//   npm test

import { test } from "node:test";
import * as assert from "node:assert/strict";
import * as cdk from "aws-cdk-lib";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { buildApp } from "../lib/app";
import { resolveConfig } from "../lib/config";
import { buildNative } from "../lib/native/build";
import { ASSETS_BUCKET_SENTINEL, REGION_SENTINEL, STAGE_SENTINEL } from "../lib/native/config";
import { DATA_TYPES, LIMITS, checkLimits, findSentinels, sha256File, transformChild } from "../lib/native/package";
import { DEPLOY_BOUNDARY_ARN, declaredLogGroups, deployRolePolicy, helperPolicy, nativeNames } from "../lib/native/parent";

const STUBS = fs.mkdtempSync(path.join(os.tmpdir(), "chnat-"));
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

const OUT = fs.mkdtempSync(path.join(os.tmpdir(), "chnatrel-"));
const rel = buildNative("v9.8.7", OUT, { dashboardDir: DASH });
const P = rel.parent;
const R = P.Resources;

const all = (): [string, any][] => [["coderhelm.json", P], ...Object.entries(rel.children)];

test("no sentinel survives anywhere, and the package reports no problem", () => {
  assert.deepEqual(rel.problems, []);
  for (const [name, t] of all()) {
    assert.deepEqual(findSentinels(t), [], name);
    const text = JSON.stringify(t);
    for (const s of [STAGE_SENTINEL, REGION_SENTINEL, ASSETS_BUCKET_SENTINEL]) assert.ok(!text.includes(s), `${name} has ${s}`);
  }
});

test("each template is within CloudFormation's limits (1 MB, 500 resources), with room", () => {
  for (const [name, t] of all()) {
    assert.deepEqual(checkLimits(name, t), []);
    assert.ok(Buffer.byteLength(JSON.stringify(t)) < LIMITS.templateBytes / 2, `${name} is over half the limit`);
    assert.ok(Object.keys(t.Resources).length < 250, `${name} has over half the resource limit`);
  }
  // A synthetic over-limit template is caught.
  const big = { Resources: Object.fromEntries(Array.from({ length: 501 }, (_, i) => [`R${i}`, { Type: "AWS::SNS::Topic" }])) };
  assert.equal(checkLimits("big", big).length, 1);
});

test("the parent pins every published file by sha256, and the files on disk match", () => {
  const files = R.ReleaseAssets.Properties.Files as { Key: string; Dest: string; Sha256: string }[];
  const listed = fs.readFileSync(path.join(OUT, "files.txt"), "utf8").trim().split("\n");
  assert.deepEqual([...files.map((f) => f.Key), "coderhelm.json"].sort(), [...listed].sort());
  for (const f of files) assert.equal(sha256File(path.join(OUT, f.Key)), f.Sha256, f.Key);
  assert.equal(R.ReleaseAssets.Properties.BaseUrl, "https://coderhelm-public.s3.amazonaws.com/launch/v9.8.7");
  // Every Lambda code key in a child is a file the copy resource verifies.
  const dests = new Set(files.map((f) => f.Dest));
  for (const [name, t] of Object.entries<any>(rel.children)) {
    for (const r of Object.values<any>(t.Resources)) {
      const key = r.Properties?.Code?.S3Key ?? r.Properties?.Content?.S3Key;
      if (typeof key === "string") assert.ok(dests.has(key), `${name}: ${key} is not copied`);
    }
  }
  // Children come from the install's own bucket, after the copy.
  for (const name of Object.keys(rel.children)) {
    assert.match(JSON.stringify(R[name].Properties.TemplateURL), /coderhelm-\$\{Stage\}-release-\$\{AWS::AccountId\}\.s3\.\$\{AWS::Region\}\.amazonaws\.com\/releases\/v9\.8\.7\/stacks\//);
    assert.ok(R[name].DependsOn.includes("ReleaseAssets"), name);
  }
});

test("every parameter a child declares is fed by the parent", () => {
  for (const [name, t] of Object.entries<any>(rel.children)) {
    const fed = Object.keys(R[name].Properties.Parameters).sort();
    const want = Object.keys(t.Parameters ?? {}).filter((p) => t.Parameters[p].Default === undefined).sort();
    for (const p of want) assert.ok(fed.includes(p), `${name}.${p} is not fed`);
    for (const p of fed) assert.ok(p in t.Parameters, `${name} is fed ${p}, which it doesn't declare`);
  }
  // Imports came out of exports: no Fn::ImportValue left, the producers keep their exports.
  for (const [name, t] of Object.entries<any>(rel.children)) assert.ok(!JSON.stringify(t).includes("Fn::ImportValue"), name);
  assert.ok(Object.values<any>(rel.children.Database.Outputs).some((o) => o.Export), "producers keep their exports (migration)");
});

test("data follows KeepDataOnDelete, deletion protection too, keys wait 7 days", () => {
  let tables = 0;
  for (const [name, t] of Object.entries<any>(rel.children)) {
    for (const [id, r] of Object.entries<any>(t.Resources)) {
      if (r.Type === "AWS::KMS::Key") assert.equal(r.Properties.PendingWindowInDays, 7, `${name}.${id}`);
      if (r.Type.startsWith("AWS::DynamoDB::") || r.Type === "AWS::Cognito::UserPool" || r.Type === "AWS::KMS::Key") {
        assert.deepEqual(r.DeletionPolicy, { Ref: "DataDeletionPolicy" }, `${name}.${id}`);
        assert.deepEqual(r.UpdateReplacePolicy, { Ref: "DataReplacePolicy" }, `${name}.${id}`);
        assert.equal(t.Transform, "AWS::LanguageExtensions", name);
      }
      if (r.Type === "AWS::DynamoDB::GlobalTable") {
        tables++;
        for (const rep of r.Properties.Replicas) if ("DeletionProtectionEnabled" in rep) assert.deepEqual(rep.DeletionProtectionEnabled, { "Fn::If": ["NativeKeepData", true, false] });
      }
      // Nothing is plainly retained (every kept thing follows the switch).
      assert.notEqual(r.DeletionPolicy, "Retain", `${name}.${id} is always retained`);
    }
  }
  assert.ok(tables >= 15, `only ${tables} tables`);
  assert.deepEqual(R.Database.Properties.Parameters.DataDeletionPolicy, { "Fn::If": ["NativeKeepData", "RetainExceptOnCreate", "Delete"] });
  assert.deepEqual(R.Database.Properties.Parameters.DataReplacePolicy, { "Fn::If": ["NativeKeepData", "Retain", "Delete"] });
  assert.equal(P.Parameters.KeepDataOnDelete.Default, "true");
  // Kept by CloudFormation; Final deletes it with KeepDataOnDelete=false.
  assert.equal(R.InstallSecret.DeletionPolicy, "RetainExceptOnCreate");
  assert.equal(R.InstallSecret.UpdateReplacePolicy, "Retain");
  assert.deepEqual(R.Final.Properties.SecretId, { Ref: "InstallSecret" });
  assert.deepEqual(R.Final.Properties.KeepData, { Ref: "KeepDataOnDelete" });
  // HybridLeave and SetupKey read the secret on delete: deleted before Final.
  assert.deepEqual(R.HybridLeave.DependsOn, ["Final"]);
  assert.deepEqual(R.SetupKey.DependsOn, ["Final"]);
  // The secret's template never changes, so an update never regenerates it.
  assert.ok(!JSON.stringify(R.InstallSecret.Properties.GenerateSecretString).includes("Ref"));
  // The sweep reads every data table and bucket, so it is deleted before them.
  const sweep = JSON.stringify(R.Sweep.Properties);
  assert.match(sweep, /Database.*Outputs\.NativeDataTables/);
  assert.match(sweep, /Storage.*Outputs\.NativeDataBuckets/);
  // Final is deleted after every part.
  for (const name of Object.keys(rel.children)) assert.ok(R[name].DependsOn.includes("Final"), name);
});

test("every role carries the boundary; the boundary is the parent's own", () => {
  for (const [name, t] of all()) {
    for (const [id, r] of Object.entries<any>(t.Resources)) {
      if (r.Type !== "AWS::IAM::Role") continue;
      assert.ok(r.Properties.PermissionsBoundary, `${name}.${id} has no boundary`);
    }
  }
  // "-stack": never the boundary of an install hardened from the CLI (coderhelm-<name>-boundary).
  assert.equal(JSON.stringify(R.Boundary.Properties.ManagedPolicyName), JSON.stringify({ "Fn::Sub": "coderhelm-${Stage}-boundary-stack" }));
  assert.ok(JSON.stringify(rel.children.Api).includes(":policy/coderhelm-${Stage}-boundary-stack"));
  // No hyphen in an install's name: coderhelm-prod-* never also names another install.
  assert.equal(P.Parameters.Stage.AllowedPattern, "^[a-z][a-z0-9]{0,19}$");
});

test("only the helper writes release files; its deletes reach only this install's certificate and check records", () => {
  const bp = R.AssetsBucketPolicy.Properties.PolicyDocument.Statement[0];
  assert.equal(bp.Effect, "Deny");
  assert.deepEqual(bp.Condition, { ArnNotEquals: { "aws:PrincipalArn": { "Fn::GetAtt": ["HelperRole", "Arn"] } } });
  const doc = helperPolicy();
  const by = (sid: string) => doc.Statement.find((x) => x.Sid === sid)!;
  assert.deepEqual(by("CertificatesOnDelete").Condition, { StringEquals: { "aws:RequestedRegion": "us-east-1", "aws:ResourceTag/coderhelm": "coderhelm-${Stage}" } });
  const r53 = by("ValidationRecordsOnDelete").Condition!;
  assert.deepEqual(r53["ForAllValues:StringEquals"], { "route53:ChangeResourceRecordSetsActions": ["DELETE"], "route53:ChangeResourceRecordSetsRecordTypes": ["CNAME"] });
  assert.deepEqual(by("StatusFile").Resource, ["arn:${AWS::Partition}:s3:::coderhelm-setup-${StatusId}", "arn:${AWS::Partition}:s3:::coderhelm-setup-${StatusId}/*"]);
});

test("the helper may only do what security.md lists", () => {
  const doc = helperPolicy();
  for (const s of doc.Statement) {
    const actions = ([] as string[]).concat(s.Action ?? []);
    for (const a of actions) assert.ok(!a.endsWith(":*") && a !== "*", `${s.Sid}: ${a}`);
    if (s.Resource === "*") assert.ok(s.Condition || actions.every((a) => /:(Describe|List)/.test(a)), `${s.Sid} is unscoped`);
  }
  const role = R.HelperRole.Properties;
  assert.deepEqual(role.PermissionsBoundary, { Ref: "Boundary" });
  // Its log group exists and it can't make one: nothing re-creates a log after the delete.
  assert.ok(!JSON.stringify(doc).includes("logs:CreateLogGroup"));
  assert.ok(R.HelperLogs && R.Helper.Properties.LoggingConfig);
  // The helper's code is in this signed template.
  assert.match(R.Helper.Properties.Code.ZipFile, /Custom::ReleaseAssets/);
});

test("no update build, no deploy window; auto-update is a role only with AutoUpdate=on", () => {
  const u = rel.children.Updates;
  const types = Object.values<any>(u.Resources).map((r) => r.Type);
  assert.equal(types.filter((t) => t === "AWS::CodeBuild::Project").length, 1, "only the read-only release check");
  assert.ok(!JSON.stringify(u).includes("window-policy"));
  assert.ok(!JSON.stringify(P).includes("deploy-window"));
  assert.equal(R.DeployRole.Condition, "AutoUpdateOn");
  assert.equal(P.Parameters.AutoUpdate.Default, "off");
  assert.equal(R.DeployRole.Properties.AssumeRolePolicyDocument.Statement[0].Principal.Service, "cloudformation.amazonaws.com");
  const updater = Object.values<any>(u.Resources).find((r) => r.Type === "AWS::Lambda::Function" && JSON.stringify(r.Properties.FunctionName ?? "").includes("updater"));
  const env = updater.Properties.Environment.Variables;
  assert.deepEqual(env.NATIVE_STACK, { Ref: "RootStackName" });
  assert.deepEqual(env.DEPLOY_ROLE_ARN, { Ref: "DeployRoleArn" });
  const pol = JSON.stringify(Object.values<any>(u.Resources).filter((r) => r.Type === "AWS::IAM::Policy"));
  assert.ok(pol.includes("cloudformation:TemplateUrl"), "change sets only from release templates");
  assert.ok(pol.includes("https://coderhelm-public.s3.amazonaws.com/launch/v*.*.*/coderhelm.json"));
  assert.ok(pol.includes('"StringEqualsIfExists":{"cloudformation:RoleArn"'), "with auto-update, only the stack's own deploy role");
  assert.ok(pol.includes("cloudformation:GetTemplate"), "it compares what CloudFormation fetched with the verified template");
  assert.ok(pol.includes('"iam:PassedToService":"cloudformation.amazonaws.com"'));
  // The auto-update role: only this stack and its parts may use it.
  const trust = JSON.stringify(R.DeployRole.Properties.AssumeRolePolicyDocument);
  assert.match(trust, /aws:SourceArn/);
  assert.match(trust, /stack\/\$\{AWS::StackName\}-\*\/\*/);
  // The kept role carries its own copy of the boundary, kept with it; the
  // boundary every other role carries goes with the stack.
  assert.equal(R.Boundary.DeletionPolicy, "Delete");
  assert.deepEqual(R.DeployRole.Properties.PermissionsBoundary, { Ref: "DeployBoundary" });
  assert.equal(R.DeployBoundary.Condition, "AutoUpdateOn");
  assert.equal(R.DeployBoundary.DeletionPolicy, "Retain");
  assert.deepEqual(R.DeployBoundary.Properties.PolicyDocument, R.Boundary.Properties.PolicyDocument);
  // Neither copy can be edited by an install role.
  const stays = R.Boundary.Properties.PolicyDocument.Statement.find((x: any) => x.Sid === "TheBoundaryStays");
  const res = JSON.stringify(stays.Resource);
  assert.ok(res.includes(":policy/coderhelm-${Stage}-boundary-stack") && res.includes(":policy/coderhelm-${Stage}-deploy-boundary-stack"), res);
  assert.match(DEPLOY_BOUNDARY_ARN, /policy\/coderhelm-\$\{Stage\}-deploy-boundary-stack$/);
  // IAM's 6,144 characters, measured as IAM stores it: substitutions resolved
  // with the longest names an install can have (a 20-character stage).
  const unwrap = (v: any): any => (Array.isArray(v) ? v.map(unwrap) : v && typeof v === "object" ? ("Fn::Sub" in v ? unwrap(v["Fn::Sub"]) : Object.fromEntries(Object.entries(v).map(([k, x]) => [k, unwrap(x)]))) : typeof v === "string"
    ? v.replace(/\$\{Stage\}/g, "abcdefghijklmnopqrst").replace(/\$\{AWS::AccountId\}/g, "111122223333").replace(/\$\{AWS::Region\}/g, "ap-southeast-4").replace(/\$\{AWS::Partition\}/g, "aws-us-gov")
    : v);
  const size = JSON.stringify(unwrap(R.Boundary.Properties.PolicyDocument)).length;
  assert.ok(size < 6144, `the boundary is ${size} characters`);
  // The auto-update role: the deploy policy, the parent stack and its parts, the release files.
  const sids = deployRolePolicy(nativeNames()).Statement.map((s) => s.Sid);
  assert.ok(sids.includes("ParentStack") && sids.includes("ReleaseFiles"));
});

test("launch parameters: the install guide's, the keep-data choice, no installer", () => {
  for (const p of ["AdminEmail", "Stage", "ModelProvider", "Hostname", "KeepDataOnDelete", "AutoUpdate", "SetupToken", "StatusId"]) assert.ok(P.Parameters[p], p);
  assert.equal(P.Parameters.SetupToken.NoEcho, true);
  const types = new Set(Object.values<any>(R).map((r) => r.Type));
  assert.ok(!types.has("AWS::CodeBuild::Project"));
  assert.ok(!types.has("AWS::CloudFormation::WaitCondition"));
});

// ── The live test's findings (bugs 1 to 4) ────────────────────────────────

/** What CloudFormation does with a template that has no transform: evaluates conditions and Fn::If with the stack's parameters. */
function evaluate(v: any, params: Record<string, string>, conds: Record<string, any>): any {
  const cond = (name: string): boolean => evaluate(conds[name], params, conds);
  if (Array.isArray(v)) return v.map((x) => evaluate(x, params, conds));
  if (v && typeof v === "object") {
    if ("Ref" in v && v.Ref in params) return params[v.Ref];
    if ("Fn::Equals" in v) { const [a, b] = v["Fn::Equals"].map((x: any) => evaluate(x, params, conds)); return a === b; }
    if ("Fn::Not" in v) return !evaluate(v["Fn::Not"][0], params, conds);
    if ("Fn::And" in v) return v["Fn::And"].every((x: any) => evaluate(x, params, conds));
    if ("Fn::If" in v) { const [c, a, b] = v["Fn::If"]; return evaluate(cond(c) ? a : b, params, conds); }
    return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, evaluate(x, params, conds)]));
  }
  return v;
}

test("bug 2: the parent has no transform, so an update with the current template really changes the data policies", () => {
  // CloudFormation stores a template with AWS::LanguageExtensions already
  // resolved: v0.8.5's stored parent (a real one, from the live test) has the
  // keep choice frozen in, so "use the current template" kept every old value.
  const stored = JSON.parse(fs.readFileSync(path.join(__dirname, "..", "..", "test", "fixtures", "cfn-stored-v0.8.5-parent.json"), "utf8"));
  assert.equal(stored.Transform, undefined, "the transform is gone from what CloudFormation keeps");
  assert.equal(stored.Resources.Database.Properties.Parameters.DataDeletionPolicy, "RetainExceptOnCreate", "frozen: no parameter can change it");
  assert.deepEqual(stored.Conditions.NativeKeepData, { "Fn::Equals": ["true", "true"] });

  // Today's parent: no transform anywhere in it, every policy a literal.
  assert.equal(P.Transform, undefined);
  for (const [id, r] of Object.entries<any>(R)) {
    for (const k of ["DeletionPolicy", "UpdateReplacePolicy"]) if (k in r) assert.equal(typeof r[k], "string", `${id}.${k} must be a literal (no transform)`);
  }
  // CloudFormation keeps exactly this template, so a parameter change with the
  // current template re-evaluates it: KeepDataOnDelete reaches every part and
  // Final and Sweep, which re-read it on delete.
  const base = Object.fromEntries(Object.entries<any>(P.Parameters).map(([k, v]) => [k, String(v.Default ?? "")]));
  for (const [keep, want] of [["true", "RetainExceptOnCreate"], ["false", "Delete"]]) {
    const out = evaluate(R, { ...base, KeepDataOnDelete: keep }, P.Conditions);
    for (const name of Object.keys(rel.children)) {
      if (!("DataDeletionPolicy" in (R[name].Properties.Parameters ?? {}))) continue;
      assert.equal(out[name].Properties.Parameters.DataDeletionPolicy, want, `${name} with KeepDataOnDelete=${keep}`);
      assert.equal(out[name].Properties.Parameters.DataReplacePolicy, keep === "true" ? "Retain" : "Delete", name);
    }
    assert.equal(out.Final.Properties.KeepData, keep);
    assert.equal(out.Sweep.Properties.KeepData, keep);
    assert.equal(evaluate(P.Outputs.KeepDataOnDelete.Value, { ...base, KeepDataOnDelete: keep }, P.Conditions), keep);
  }
  // Each part reads its template from its URL on every update and applies
  // its own transform there: the parameter is what the data's policy follows.
  for (const [name, t] of Object.entries<any>(rel.children)) {
    if (!t.Parameters?.DataDeletionPolicy) continue;
    assert.equal(t.Transform, "AWS::LanguageExtensions", name);
  }
});

test("bug 1: a change set with the nested stacks can be built (each child URL resolves from parameters, and the helper stages it)", () => {
  const files = R.ReleaseAssets.Properties.Files as { Key: string; Dest: string; Sha256: string }[];
  const declared = new Set([...Object.keys(P.Parameters), "AWS::Region", "AWS::AccountId", "AWS::Partition", "AWS::URLSuffix"]);
  for (const name of Object.keys(rel.children)) {
    const url = R[name].Properties.TemplateURL;
    assert.deepEqual(Object.keys(url), ["Fn::Sub"], `${name}: one Fn::Sub string`);
    const text: string = url["Fn::Sub"];
    assert.equal(typeof text, "string", name);
    // Only parameters and pseudo parameters: CloudFormation resolves them
    // while it builds the change set; a resource (the old ${AssetsBucket})
    // it doesn't, and refused with "must start with https://".
    for (const m of text.matchAll(/\$\{([^}]+)\}/g)) assert.ok(declared.has(m[1]), `${name}: \${${m[1]}} is not a parameter`);
    const resolved = text.replace(/\$\{Stage\}/g, "prod").replace(/\$\{AWS::AccountId\}/g, "111122223333").replace(/\$\{AWS::Region\}/g, "us-east-1");
    assert.equal(resolved, `https://coderhelm-prod-release-111122223333.s3.us-east-1.amazonaws.com/releases/v9.8.7/stacks/${name}.json`);
    // The same bucket the parent creates, and a file the helper copies (staged before the change set).
    const dest = `releases/v9.8.7/stacks/${name}.json`;
    assert.ok(files.some((f) => f.Dest === dest && /^[0-9a-f]{64}$/.test(f.Sha256)), `${name}: not in the release files`);
  }
  assert.deepEqual(R.AssetsBucket.Properties.BucketName, { "Fn::Sub": "coderhelm-${Stage}-release-${AWS::AccountId}" });
  assert.deepEqual(R.Helper.Properties.Environment.Variables.ASSETS_BUCKET, { Ref: "AssetsBucket" });
  assert.match(R.Helper.Properties.Code.ZipFile, /def stage_release/);
  // The updater asks the helper to stage, and may read what it staged (CloudFormation reads with the caller's access).
  const u = rel.children.Updates;
  const updater = Object.values<any>(u.Resources).find((r) => r.Type === "AWS::Lambda::Function" && JSON.stringify(r.Properties.FunctionName ?? "").includes("updater"));
  assert.deepEqual(updater.Properties.Environment.Variables.NATIVE_HELPER_FUNCTION, { "Fn::Sub": "coderhelm-${Stage}-install-helper" });
  const pol = JSON.stringify(Object.values<any>(u.Resources).filter((r) => r.Type === "AWS::IAM::Policy"));
  assert.ok(pol.includes('"Sid":"StageReleaseFiles"') && pol.includes("function:coderhelm-${Stage}-install-helper"), "invoke the helper");
  const read = Object.values<any>(u.Resources).filter((r) => r.Type === "AWS::IAM::Policy").flatMap((r) => r.Properties.PolicyDocument.Statement).find((x: any) => x.Sid === "ReadStagedTemplates");
  assert.deepEqual(read.Action, "s3:GetObject");
  assert.ok(JSON.stringify(read.Resource).includes(":s3:::coderhelm-${Stage}-release-") && JSON.stringify(read.Resource).endsWith('"/releases/*"]]}'), JSON.stringify(read.Resource));
});

test("bug 3: the helper may delete the main table (coderhelm-<stage>, no suffix) as well as the others", () => {
  const st = helperPolicy().Statement.find((x) => x.Sid === "AllowlistAndTablesOnDelete")!;
  assert.deepEqual(st.Resource, ["arn:${AWS::Partition}:dynamodb:${AWS::Region}:${AWS::AccountId}:table/coderhelm-${Stage}", "arn:${AWS::Partition}:dynamodb:${AWS::Region}:${AWS::AccountId}:table/coderhelm-${Stage}-*"]);
  // Every table the sweep deletes is covered.
  const db = rel.children.Database;
  const names = Object.values<any>(db.Resources).filter((r) => r.Type === "AWS::DynamoDB::GlobalTable").map((r) => r.Properties.TableName["Fn::Sub"] ?? r.Properties.TableName);
  assert.ok(names.includes("coderhelm-${Stage}"), "the main table");
  for (const n of names) assert.ok(n === "coderhelm-${Stage}" || n.startsWith("coderhelm-${Stage}-"), n);
});

test("bug 4: Final deletes every declared log group by its exact name (Lambda recreates one after the delete)", () => {
  const names = declaredLogGroups(Object.entries<any>(rel.children).map(([name, template]) => ({ name, template } as any)));
  // Every log group a part declares: each function's and build project's.
  let declared = 0;
  for (const t of Object.values<any>(rel.children)) for (const r of Object.values<any>(t.Resources)) if (r.Type === "AWS::Logs::LogGroup") declared++;
  assert.equal(names.length, declared);
  assert.ok(names.length >= 12, `only ${names.length}`);
  assert.ok(names.includes("/aws/lambda/coderhelm-${Stage}-worker") && names.includes("/aws/codebuild/coderhelm-${Stage}-release-check"));
  // The two the live test found left behind.
  assert.ok(names.some((n) => /^\/aws\/lambda\/coderhelm-\$\{Stage\}-Handler-[0-9a-f]{8}$/.test(n)), "the S3 auto-delete provider");
  assert.ok(names.some((n) => /^\/aws\/lambda\/coderhelm-\$\{Stage\}-LogRetention/.test(n)), "the log retention provider");
  assert.deepEqual(R.Final.Properties.LogGroupNames, names.map((n) => ({ "Fn::Sub": n })));
  // Within what the helper may delete.
  const st = helperPolicy().Statement.find((x) => x.Sid === "InstallLogGroupsOnDelete")!;
  assert.ok(JSON.stringify(st.Resource).includes("log-group:/aws/lambda/coderhelm-${Stage}-*"));
  assert.ok(JSON.stringify(st.Resource).includes("log-group:/aws/codebuild/coderhelm-${Stage}-*"));
  // A name that isn't this install's own is refused at build time.
  assert.throws(() => declaredLogGroups([{ name: "X", template: { Resources: { L: { Type: "AWS::Logs::LogGroup", Metadata: { "coderhelm:native-log": true }, Properties: { LogGroupName: { Ref: "Something" } } } } } } as any]), /declared log group/);
});

test("the packager refuses a sentinel where intrinsics can't go, and unknown names", () => {
  const bad = { Resources: { [`X${STAGE_SENTINEL}`]: { Type: "AWS::SNS::Topic", Properties: { Tags: [{ Key: `k-${STAGE_SENTINEL}`, Value: "v" }] } } } };
  assert.throws(() => transformChild("Bad", { Resources: { T: { Type: "AWS::SNS::Topic", Properties: { Tags: [{ [`k${STAGE_SENTINEL}`]: 1 }] } } } }, {}), /name with the install in it/);
  assert.doesNotThrow(() => transformChild("Ok", bad, {})); // logical ids are renamed
  assert.throws(() => transformChild("Ref", { Resources: { T: { Type: "AWS::SNS::Topic", Properties: { TopicName: { Ref: "Mystery" } } } } }, {}), /unknown name Mystery/);
  assert.throws(() => transformChild("Imp", { Resources: { T: { Type: "AWS::SNS::Topic", Properties: { TopicName: { "Fn::ImportValue": "nope" } } } } }, {}), /unknown export/);
  const c = transformChild("Sub", { Resources: { T: { Type: "AWS::SNS::Topic", Properties: { TopicName: `coderhelm-${STAGE_SENTINEL}-x \${literal}` } } } }, {});
  assert.deepEqual(c.template.Resources.T.Properties.TopicName, { "Fn::Sub": "coderhelm-${Stage}-x ${!literal}" });
});

// ── Migration, offline: today's install nests with its data kept ──────────

/** A native value with the install's own values put in. */
function resolveFor(v: any, ctx: Record<string, string>): any {
  if (typeof v === "string") return v;
  if (Array.isArray(v)) return v.map((x) => resolveFor(x, ctx));
  if (v && typeof v === "object") {
    if ("Fn::Sub" in v && typeof v["Fn::Sub"] === "string") return v["Fn::Sub"].replace(/\$\{([^}!]+)\}/g, (_: string, k: string) => ctx[k] ?? `\${${k}}`).replace(/\$\{!/g, "${");
    if ("Ref" in v && ctx[v.Ref]) return ctx[v.Ref];
    if ("Fn::Join" in v) {
      const parts = resolveFor(v["Fn::Join"][1], ctx);
      return parts.every((p: any) => typeof p === "string") ? parts.join(v["Fn::Join"][0]) : { "Fn::Join": [v["Fn::Join"][0], parts] };
    }
    return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, resolveFor(x, ctx)]));
  }
  return v;
}

const CREATE_ONLY: Record<string, string[]> = {
  "AWS::DynamoDB::GlobalTable": ["TableName", "KeySchema", "LocalSecondaryIndexes"],
  "AWS::DynamoDB::Table": ["TableName", "KeySchema", "LocalSecondaryIndexes"],
  "AWS::S3::Bucket": ["BucketName"],
  "AWS::Cognito::UserPool": ["UsernameAttributes", "AliasAttributes", "Schema"],
  "AWS::KMS::Key": [],
  "AWS::SecretsManager::Secret": ["Name"],
};

test("migration: every table, bucket, key and user pool of today's install keeps its logical id and create-only properties", () => {
  const account = "111122223333";
  const region = "us-west-2";
  const cfg = resolveConfig({
    account, region, stage: "prod", email: { from: "a@example.org" }, admins: ["o@example.org"],
    access: { mode: "hardened", owner: "launch", id: "1a2b3c4d" }, dashboard: { assetDir: DASH },
  });
  const app = new cdk.App({ outdir: fs.mkdtempSync(path.join(os.tmpdir(), "chmig-")) });
  buildApp(app, cfg);
  const today: Record<string, any> = {};
  for (const s of app.synth().stacks) today[s.stackName] = s.template;
  const ctx = { Stage: "prod", "AWS::AccountId": account, "AWS::Region": region, "AWS::Partition": "aws" };
  let compared = 0;
  for (const [stackName, t] of Object.entries<any>(today)) {
    const part = stackName.replace("coderhelm-prod-", "").split("-").map((w) => w[0].toUpperCase() + w.slice(1)).join("");
    for (const [id, r] of Object.entries<any>(t.Resources)) {
      if (!(r.Type in CREATE_ONLY) || !DATA_TYPES.concat(["AWS::SecretsManager::Secret"]).includes(r.Type)) continue;
      if (r.Type === "AWS::S3::Bucket" && r.DeletionPolicy !== "Retain") continue; // not data (the dashboard bucket)
      const n = rel.children[part]?.Resources?.[id];
      assert.ok(n, `${stackName}.${id} (${r.Type}) has no counterpart in the native ${part} part`);
      assert.equal(n.Type, r.Type, `${stackName}.${id}`);
      for (const prop of CREATE_ONLY[r.Type]) {
        assert.deepEqual(resolveFor(n.Properties?.[prop], ctx), r.Properties?.[prop], `${stackName}.${id}.${prop} would replace it`);
      }
      compared++;
    }
  }
  assert.ok(compared >= 20, `compared only ${compared} data resources`);
});

// ── The bar: what a real "delete everything" left behind ─────────────────
//
// The v0.8.2 guided uninstall, run for real, left: a database stack in
// DELETE_FAILED on two GlobalTables; the Launch Stack; 19 tables, the
// assets, data and progress buckets; 9 KMS keys (several from earlier
// failed installs); 2 user pools of the same name; the secret; 10 log groups
// of CDK's helper functions and the starter; 4 installer SSM parameters;
// 4 roles and the boundary; 4 orphaned event source mappings; a certificate.
// Each line below is why Delete stack can't leave that piece.

test("the bar: every piece the real uninstall left is owned and removed by the stack", () => {
  const fns: string[] = [];
  for (const [name, t] of all()) {
    for (const [id, r] of Object.entries<any>(t.Resources)) {
      if (r.Type === "AWS::Lambda::Function") {
        // Log groups of every function, CDK's helpers included, are stack resources.
        const lg = r.Properties.LoggingConfig?.LogGroup;
        assert.ok(lg?.Ref && t.Resources[lg.Ref]?.Type === "AWS::Logs::LogGroup", `${name}.${id} logs to an undeclared log group`);
        assert.notEqual(t.Resources[lg.Ref].DeletionPolicy, "Retain", `${name}.${id}'s log group is kept`);
        fns.push(`${name}.${id}`);
      }
      // Nothing outside CloudFormation: no installer state, no window policy parameters.
      if (r.Type === "AWS::SSM::Parameter") assert.ok(!/installer|window-policy|launch-/.test(JSON.stringify(r.Properties.Name)), `${name}.${id}`);
      // No installer, starter or window roles.
      if (r.Type === "AWS::IAM::Role") assert.ok(!/installer|starter|window/.test(JSON.stringify(r.Properties.RoleName ?? "")), `${name}.${id}`);
    }
  }
  assert.ok(fns.length >= 12, `${fns.length} functions`);
  // Event source mappings are the Worker part's own resources (deleted with it, never orphaned).
  assert.ok(Object.values<any>(rel.children.Worker.Resources).filter((r) => r.Type === "AWS::Lambda::EventSourceMapping").length >= 4);
  // Data: deleted on a failed first launch (RetainExceptOnCreate), so earlier failed installs leave nothing.
  for (const [name, t] of Object.entries<any>(rel.children)) {
    for (const r of Object.values<any>(t.Resources)) {
      if (["AWS::KMS::Key", "AWS::Cognito::UserPool", "AWS::DynamoDB::GlobalTable"].includes(r.Type)) assert.deepEqual(r.DeletionPolicy, { Ref: "DataDeletionPolicy" }, name);
    }
  }
  // GlobalTables: the Sweep deletes them itself, with retries and waits, before CloudFormation does.
  assert.equal(R.Sweep.Type, "Custom::Sweep");
  assert.match(R.Helper.Properties.Code.ZipFile, /def delete_tables\(/);
  // The certificate the updater requested is deleted by Final, after the distribution is gone.
  assert.match(R.Helper.Properties.Code.ZipFile, /def delete_certificates\(/);
  // The progress bucket is the stack's (emptied by Final), not retained.
  assert.notEqual(R.StatusBucket.DeletionPolicy, "Retain");
  // One stack: no Launch Stack, no boundary or bucket left on purpose.
  assert.notEqual(R.Boundary.DeletionPolicy, "Retain");
  assert.notEqual(R.AssetsBucket.DeletionPolicy, "Retain");
});

test("bug 2: no doc tells anyone to change a parameter with the current template", () => {
  // On v0.8.4 and v0.8.5 that update reports UPDATE_COMPLETE and keeps every
  // old value (KeepDataOnDelete included): the docs re-supply the release's template.
  const dir = path.join(__dirname, "..", "..", "..", "docs", "self-host");
  for (const f of fs.readdirSync(dir).filter((x) => x.endsWith(".md"))) {
    const text = fs.readFileSync(path.join(dir, f), "utf8");
    assert.ok(!/use-previous-template|UsePreviousTemplate/.test(text), `${f} uses the previous template`);
    for (const m of text.matchAll(/[^.\n]*Use current template[^.\n]*/g)) assert.match(m[0], /[Dd]on't|not \*\*Use current/, `${f}: "${m[0].trim()}"`);
  }
  const un = fs.readFileSync(path.join(dir, "uninstall.md"), "utf8");
  assert.match(un, /--template-url "https:\/\/coderhelm-public\.s3\.amazonaws\.com\/launch\/\$V\/coderhelm\.json"/);
  assert.match(un, /OutputKey=='KeepDataOnDelete'/, "it checks the output before deleting");
});
