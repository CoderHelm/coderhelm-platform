// The deploy role's own access on a native install (infra/lib/native/parent.ts
// pinnedAccess, infra/lib/native/access-stack.ts, lambda/updater "held").
//
// Every update runs as the parent's deploy role, and the boundary forbids that
// role to change itself or a boundary; an AWS admin's Execute in the console
// runs as that role too (the stack's service role). So:
//
// - the parent carries Boundary, DeployBoundary and DeployRole exactly as
//   installs have them (v0.8.36 to v0.8.53), and a change to them fails here;
// - today's documents ship in access.json, which an AWS admin applies with
//   their own permissions, and no CoderHelm role can reach its writer;
// - every call CloudFormation makes for a release through the deploy role
//   (the simulator's table) is allowed under the pinned boundary too, so an
//   update that keeps an install's older boundary still installs;
// - a change to today's boundary or deploy policy is flagged (ACCESS_DOCS):
//   it reaches installs only through an admin, so it is a deliberate,
//   batched decision.
//
//   npm test

import { test } from "node:test";
import * as assert from "node:assert/strict";
import * as crypto from "node:crypto";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { accessNames, BOUNDARY_OPTION_READS, boundaryPolicy, PolicyDocument } from "../lib/access";
import { ACCESS_PARAMETERS, ACCESS_SHAPE, ACCESS_WRITER_NAME, buildAccess, READER_UPDATE_NAME, readerPolicy, writerPolicy } from "../lib/native/access-stack";
import { buildNative } from "../lib/native/build";
import { ACCESS_FILE, deployRolePolicy, nativeBoundaryDoc, nativeNames, pinnedAccess, subInstall } from "../lib/native/parent";
import { plainStatements } from "../lib/own-account-read";
import { sha256File } from "../lib/native/package";
import { prodValues, resolveValue } from "../lib/native/resolve";
import { decision } from "./iam-eval";

const STUBS = fs.mkdtempSync(path.join(os.tmpdir(), "chacc-"));
for (const d of ["gateway", "worker", "mcp"]) {
  fs.mkdirSync(path.join(STUBS, d), { recursive: true });
  fs.writeFileSync(path.join(STUBS, d, d === "mcp" ? "handler.js" : "bootstrap"), "stub");
}
const DASH = path.join(STUBS, "dash");
fs.mkdirSync(DASH, { recursive: true });
fs.writeFileSync(path.join(DASH, "index.html"), "<html></html>");
process.env.GATEWAY_ZIP = path.join(STUBS, "gateway");
process.env.WORKER_ZIP = path.join(STUBS, "worker");
process.env.MCP_PROXY_DIR = path.join(STUBS, "mcp");
const OUT = fs.mkdtempSync(path.join(os.tmpdir(), "chaccrel-"));
const rel = buildNative("v9.8.7", OUT, { dashboardDir: DASH, commit: "0".repeat(40), releasedAt: "2026-11-08T00:00:00Z" });
const R = rel.parent.Resources;
const ACCESS = JSON.parse(fs.readFileSync(path.join(OUT, ACCESS_FILE), "utf8"));

const canon = (v: unknown) => JSON.stringify(v, (_k, x) => (x && typeof x === "object" && !Array.isArray(x) ? Object.fromEntries(Object.entries(x).sort(([a], [b]) => a.localeCompare(b))) : x));
const sha = (v: unknown) => crypto.createHash("sha256").update(canon(v)).digest("hex");

/**
 * Today's boundary and deploy role policy, as their sha256. Last changed for
 * Amazon Inspector's findings and AWS Health events in the install's own
 * account (inspector2 list calls and BatchGetAccountStatus, health describe
 * calls; own-account-read.ts, OWN_OPTIONS), batched with the infrastructure
 * vulnerability reads before it, which no release has offered an admin yet. A change here
 * reaches no install by itself: an AWS admin applies it through the access
 * stack (Settings, System, Permissions). Before changing these two values,
 * batch it with whatever else needs the same admin step, and say so in the
 * release notes; the dashboard then offers it as one optional item.
 */
const ACCESS_DOCS = {
  // Private networks (access.ts BOUNDARY_OPTION_READS.PrivateNetworks, PRIVATE_NETWORK_DEPLOY_*),
  // then AWS WAF's reads for alarm investigations (wafv2:Get*, wafv2:List*).
  boundary: "9b357fb381000b22452ca2a44d34bd1a61ef81bbe05755b6731250139e9e8c36",
  deployRole: "c55fb9759c36f7df55c4d8d25de2ebd792238d91a144fcd82ca5e765566a5117",
};

test("the parent carries the deploy role and both boundaries exactly as installs have them, so no update ever changes them", () => {
  const pinned = pinnedAccess();
  for (const k of ["Boundary", "DeployBoundary", "DeployRole"] as const) assert.deepEqual(R[k], pinned[k], k);
  // The pinned file is v0.8.36 to v0.8.53's: a change to it holds every install's next update.
  const f = JSON.parse(fs.readFileSync(path.join(__dirname, "..", "..", "lib", "native", "access-pinned.json"), "utf8"));
  assert.equal(sha(f.resources), "bf5ec8ae9432f9468256ff208b3b40e782742f25a36b57f935740d583ce32c78", "access-pinned.json changed: every install's next update would wait for an AWS admin");
});

test("access.json is built, pinned by sha256 in the parent and published with the release", () => {
  const files = R.ReleaseAssets.Properties.Files as { Key: string; Dest: string; Sha256: string }[];
  const f = files.find((x) => x.Key === ACCESS_FILE)!;
  assert.ok(f, "access.json is in ReleaseAssets");
  assert.equal(f.Sha256, sha256File(path.join(OUT, ACCESS_FILE)));
  assert.equal(f.Dest, "releases/v9.8.7/access.json");
  assert.ok(rel.files.includes(ACCESS_FILE));
});

test("access.json writes today's boundary to both boundaries and today's deploy policy to the deploy role", () => {
  const p = ACCESS.Resources.Access.Properties;
  const n = nativeNames();
  const today = subInstall(boundaryPolicy(n, ["arn:${AWS::Partition}:iam::${AWS::AccountId}:policy/coderhelm-${Stage}-deploy-boundary-stack"]));
  assert.deepEqual(p.Boundary.Document, today);
  assert.deepEqual(p.DeployBoundary.Document, today);
  assert.deepEqual(p.Boundary.Arn, { "Fn::Sub": "arn:${AWS::Partition}:iam::${AWS::AccountId}:policy/coderhelm-${Stage}-boundary-stack" });
  assert.deepEqual(p.DeployBoundary.Arn, { "Fn::Sub": "arn:${AWS::Partition}:iam::${AWS::AccountId}:policy/coderhelm-${Stage}-deploy-boundary-stack" });
  assert.deepEqual(p.DeployRole.Document, subInstall(deployRolePolicy(n)));
  assert.deepEqual(p.DeployRole.RoleName, { "Fn::Sub": "coderhelm-${Stage}-deploy-stack" });
  // Every ${Name} the template uses is a parameter it declares (the Launch link fills them) or a pseudo parameter.
  const used = new Set([...JSON.stringify(ACCESS).matchAll(/\$\{([A-Za-z]+)\}/g)].map((m) => m[1]));
  for (const u of used) assert.ok(ACCESS_PARAMETERS.includes(u) || u === "EmailIdentity", u);
  assert.deepEqual(Object.keys(ACCESS.Parameters).sort(), [...ACCESS_PARAMETERS].sort());
  // Its writer's code is in the signed template.
  assert.match(ACCESS.Resources.Writer.Properties.Code.ZipFile, /def write_policy/);
});

test("today's boundary and deploy policy are the reviewed ones (a change is a deliberate, batched admin step)", () => {
  const n = nativeNames();
  const now = { boundary: sha(subInstall(boundaryPolicy(n, ["arn:${AWS::Partition}:iam::${AWS::AccountId}:policy/coderhelm-${Stage}-deploy-boundary-stack"]))), deployRole: sha(subInstall(deployRolePolicy(n))) };
  assert.deepEqual(now, ACCESS_DOCS,
    "This changes the permissions boundary or the deploy role's policy. No update can apply that: installs get it only when an AWS admin " +
    "applies the access stack (Settings, System, Permissions). Batch it with other such changes, then set ACCESS_DOCS in this test to the new values.");
});

/** A policy document for stage prod in the simulator fixture's account. */
const F = JSON.parse(fs.readFileSync(path.join(__dirname, "..", "..", "test", "fixtures", "native-sim.json"), "utf8"));
function resolved(v: unknown): PolicyDocument {
  const values: Record<string, string> = { ...prodValues(F.account, F.region), EmailFrom: "owner@example.org" };
  return resolveValue(v as any, { values, conditions: { HasEmailFrom: true } }) as PolicyDocument;
}

test("a release installs through the deploy role under the pinned boundary too: every call it may make today, it may make with the boundary installs have", () => {
  const pinned = pinnedAccess();
  const boundary = resolved((pinned.Boundary as any).Properties.PolicyDocument);
  const deploy = resolved((pinned.DeployRole as any).Properties.Policies[0].PolicyDocument);
  assert.ok(!JSON.stringify([boundary, deploy]).includes("Fn::"), "fully resolved");
  const pairs = (F.rows as any[]).flatMap((r) => Object.entries(r.actions as Record<string, string>).map(([action, verdict]) => ({ action, verdict, resource: r.resource, context: r.context })));
  const lost = pairs.filter((p) => p.verdict === "allowed" && decision([deploy], boundary, p) !== "allowed").map((p) => `${p.action} on ${p.resource}`);
  assert.deepEqual(lost, [], "the deploy needs more than the pinned boundary allows: such a release can't install until an admin applies the access stack");
});

test("no CoderHelm role can reach the access stack, its writer or the writer's role", () => {
  const names = { ...accessNames({ account: "111122223333", region: "us-east-1", prefix: "coderhelm-prod", suffix: "-111122223333", secretName: "coderhelm/prod/secrets", id: "stack" }), emailIdentity: "owner@example.org" };
  const boundary = boundaryPolicy(names, ["arn:aws:iam::111122223333:policy/coderhelm-prod-deploy-boundary-stack"]);
  const anything: PolicyDocument = { Version: "2012-10-17", Statement: [{ Sid: "All", Effect: "Allow", Action: "*", Resource: "*" }] };
  const writer = ACCESS_WRITER_NAME.replace("${Stage}", "prod");
  const calls: [string, string][] = [
    ["lambda:InvokeFunction", `arn:aws:lambda:us-east-1:111122223333:function:${writer}`],
    ["lambda:UpdateFunctionCode", `arn:aws:lambda:us-east-1:111122223333:function:${writer}`],
    ["iam:PassRole", `arn:aws:iam::111122223333:role/${writer}`],
    ["iam:UpdateAssumeRolePolicy", `arn:aws:iam::111122223333:role/${writer}`],
    ["iam:PutRolePolicy", `arn:aws:iam::111122223333:role/${writer}`],
    ["cloudformation:CreateChangeSet", "arn:aws:cloudformation:us-east-1:111122223333:stack/coderhelm-prod-access/abc"],
    ["cloudformation:ExecuteChangeSet", "arn:aws:cloudformation:us-east-1:111122223333:stack/coderhelm-prod-access/abc"],
    ["cloudformation:UpdateStack", "arn:aws:cloudformation:us-east-1:111122223333:stack/coderhelm-prod-access/abc"],
    ["cloudformation:DeleteStack", "arn:aws:cloudformation:us-east-1:111122223333:stack/coderhelm-prod-access/abc"],
  ];
  for (const [action, resource] of calls) {
    assert.notEqual(decision([anything], boundary, { action, resource, context: { "iam:PermissionsBoundary": names.boundaryArn } }), "allowed", `${action} on ${resource}`);
  }
  // The writer may write exactly the two boundaries and the deploy role's policy, nothing else.
  const w = resolveValue(writerPolicy(), { values: prodValues("111122223333", "us-east-1") }) as PolicyDocument;
  const can = (action: string, resource: string) => decision([w], null, { action, resource }) === "allowed";
  assert.ok(can("iam:CreatePolicyVersion", "arn:aws:iam::111122223333:policy/coderhelm-prod-boundary-stack"));
  assert.ok(can("iam:CreatePolicyVersion", "arn:aws:iam::111122223333:policy/coderhelm-prod-deploy-boundary-stack"));
  assert.ok(can("iam:PutRolePolicy", "arn:aws:iam::111122223333:role/coderhelm-prod-deploy-stack"));
  assert.ok(!can("iam:CreatePolicyVersion", "arn:aws:iam::111122223333:policy/coderhelm-prod-other"));
  assert.ok(!can("iam:PutRolePolicy", "arn:aws:iam::111122223333:role/coderhelm-prod-gateway"));
  assert.ok(!can("iam:CreateRole", "arn:aws:iam::111122223333:role/anything"));
});

test("the access stack has no service role and changes nothing on delete", () => {
  assert.equal(ACCESS.RoleARN, undefined);
  for (const r of Object.values<any>(ACCESS.Resources)) assert.equal(r.DeletionPolicy, undefined);
  assert.match(fs.readFileSync(path.join(__dirname, "..", "..", "..", "lambda", "native-access", "handler.py"), "utf8"), /On Delete it changes nothing/);
});

test("searching a connected Bedrock knowledge base arrives with the access stack, never with an update", () => {
  const values = prodValues("111122223333", "us-east-1");
  const kb = { action: "bedrock:Retrieve", resource: "arn:aws:bedrock:us-east-1:111122223333:knowledge-base/ABCDE12345" };
  const anything: PolicyDocument = { Version: "2012-10-17", Statement: [{ Sid: "All", Effect: "Allow", Action: "*", Resource: "*" }] };
  // The parent's boundary is the pinned one: an update leaves an install without Retrieve.
  const pinned = resolveValue((pinnedAccess().Boundary as any).Properties.PolicyDocument, { values, conditions: { HasEmailFrom: true } }) as PolicyDocument;
  assert.notEqual(decision([anything], pinned, kb), "allowed", "the pinned boundary must not change");
  // Today's boundary, which access.json writes when an AWS admin applies it, allows it.
  const today = resolveValue(ACCESS.Resources.Access.Properties.Boundary.Document, { values: { ...values, EmailFrom: "owner@example.org" }, conditions: { HasEmailFrom: true } }) as PolicyDocument;
  assert.equal(decision([anything], today, kb), "allowed");
  // Listing and reading a cited document were already allowed (bedrock:List*, bedrock:Get*).
  for (const action of ["bedrock:ListKnowledgeBases", "bedrock:GetDocumentContent"]) {
    assert.equal(decision([anything], pinned, { ...kb, action }), "allowed", action);
  }
});

test("the install's optional reads are parent parameters fed to the parts that read its own account, so a parameter-only change set switches them", () => {
  const P = rel.parent.Parameters;
  assert.equal(P.InfrastructureVulnerabilities.Default, "on");
  assert.equal(P.InspectorFindings.Default, "off");
  assert.equal(P.HealthEvents.Default, "off");
  for (const part of ["Api", "Worker"]) {
    for (const k of ["InfrastructureVulnerabilities", "InspectorFindings", "HealthEvents"]) {
      assert.deepEqual(R[part].Properties.Parameters[k], { Ref: k }, `${part}.${k}`);
    }
  }
});

// ── A newer release's access: an update of the stack, never a delete ─────

/** The resources an earlier release's access.json made (v0.8.59's, as published). */
const EARLIER = {
  WriterRole: { Type: "AWS::IAM::Role", RoleName: { "Fn::Sub": "CoderHelmAccess-${Stage}" } },
  Writer: { Type: "AWS::Lambda::Function", FunctionName: { "Fn::Sub": "CoderHelmAccess-${Stage}" } },
  Access: { Type: "Custom::Access" },
};

test("an admin's update of the access stack with a newer access.json changes every resource in place and runs the writer again", () => {
  const older = buildAccess("v9.8.6", "def write_policy(): pass\n");
  const newer = buildAccess("v9.8.7", "def write_policy(): pass  # newer\n");
  // The same logical ids, types and fixed names as every earlier release: nothing is replaced.
  // What a release adds (the reader update) is added beside them, and kept from then on (ACCESS_SHAPE).
  for (const id of Object.keys(EARLIER)) assert.ok(id in newer.Resources, id);
  assert.deepEqual(Object.keys(newer.Resources).sort(), Object.keys(ACCESS_SHAPE).sort());
  for (const [id, want] of Object.entries(EARLIER)) {
    const r = newer.Resources[id];
    assert.equal(r.Type, want.Type, id);
    assert.equal(r.Type, ACCESS_SHAPE[id as keyof typeof ACCESS_SHAPE].Type, id);
    if ("RoleName" in want) assert.deepEqual(r.Properties.RoleName, want.RoleName);
    if ("FunctionName" in want) assert.deepEqual(r.Properties.FunctionName, want.FunctionName);
    assert.equal(r.Condition, undefined, `${id} has no condition (one would delete it on update)`);
  }
  // The same parameters, so "Use existing values" is all an update asks.
  assert.deepEqual(Object.keys(newer.Parameters).sort(), Object.keys(older.Parameters).sort());
  // The custom resource's properties change with every release (its Version), so
  // CloudFormation sends the writer an Update, which writes what differs (lambda/tests, AccessForWhatIsUsed).
  assert.notDeepEqual(newer.Resources.Access.Properties, older.Resources.Access.Properties);
  assert.equal(newer.Resources.Access.Properties.Version, "v9.8.7");
  assert.equal(older.Resources.Access.Properties.Version, "v9.8.6");
  // The writer answers an Update the way it answers a Create.
  const code = fs.readFileSync(path.join(__dirname, "..", "..", "..", "lambda", "native-access", "handler.py"), "utf8");
  assert.match(code, /if event.get\("RequestType"\) in \("Create", "Update"\):/);
  assert.match(code, /"PhysicalResourceId": event.get\("PhysicalResourceId"\) or "access"/, "an Update keeps its physical id: never a replacement");
});

test("access.json names the reads only one switch uses, so the updater asks for them only while that switch is on", () => {
  assert.deepEqual(ACCESS.Metadata.CoderHelm.reads, JSON.parse(JSON.stringify(BOUNDARY_OPTION_READS)));
  assert.equal(ACCESS.Metadata.CoderHelm.kind, "access");
  // The updater's own copy for an access.json from before the metadata is the same.
  const updater = fs.readFileSync(path.join(__dirname, "..", "..", "..", "lambda", "updater", "handler.py"), "utf8");
  const block = updater.slice(updater.indexOf("OPTION_READS = {"), updater.indexOf("}", updater.indexOf("OPTION_READS = {")) + 1);
  for (const [opt, pats] of Object.entries(BOUNDARY_OPTION_READS)) {
    assert.ok(block.includes(`"${opt}"`), opt);
    for (const p of pats) assert.ok(block.includes(`"${p}"`), `${opt}: ${p}`);
  }
});

/**
 * What the install's AWS card checks (services/common/src/log_reader.rs
 * ABILITIES, every read a team doesn't choose) is what the release grants the
 * install's own read roles, the gateway's and the worker's, under today's
 * boundary: each action, on a resource of the type IAM applies it to
 * (services/common/src/live/iam_resource_types.json; an action with no
 * resource-level permissions, such as logs:StopQuery, only on "*").
 */
test("every read the install's permission check expects is granted to the roles that read, and the boundary allows it", () => {
  const rs = fs.readFileSync(path.join(__dirname, "..", "..", "..", "services", "common", "src", "log_reader.rs"), "utf8");
  const types = JSON.parse(fs.readFileSync(path.join(__dirname, "..", "..", "..", "services", "common", "src", "live", "iam_resource_types.json"), "utf8")).actions as Record<string, string[]>;
  const block = rs.slice(rs.indexOf("pub const ABILITIES"), rs.indexOf("pub const OPTIONAL"));
  const expected = new Set<string>();
  for (const ab of block.split("Ability {").slice(1)) {
    if (/chosen: true/.test(ab)) continue;
    for (const m of ab.matchAll(/"([a-z0-9-]+:[A-Za-z0-9]+)"/g)) expected.add(m[1]);
  }
  assert.ok(expected.has("logs:StopQuery") && expected.size > 20, `${expected.size} actions`);
  const acct = F.account, region = F.region;
  const resourceOf = (a: string) => {
    if (types[a]?.length === 1 && types[a][0] === "") return "*";
    if (/^logs:(StartQuery|GetQueryResults|FilterLogEvents)$/.test(a)) return `arn:aws:logs:${region}:${acct}:log-group:app`;
    if (a === "logs:GetLogEvents") return `arn:aws:logs:${region}:${acct}:log-group:app:log-stream:s`;
    if (a === "airflow:InvokeRestApi") return `arn:aws:airflow:${region}:${acct}:role/env/Viewer`;
    if (a === "cloudformation:DescribeStacks") return `arn:aws:cloudformation:${region}:${acct}:stack/CoderHelmLogReaderUpdate-abc/x`;
    return "*";
  };
  const boundary = resolved(nativeBoundaryDoc());
  for (const [part, id] of [["Api", "GatewayOwnAccountRead"], ["Worker", "WorkerOwnAccountRead"]]) {
    const doc = rel.children[part].Resources[id].Properties.PolicyDocument;
    const identity = { Version: "2012-10-17", Statement: plainStatements(resolved(doc).Statement) } as PolicyDocument;
    const refused: string[] = [];
    for (const a of expected) {
      // Its own role only (a GetAtt the resolver leaves as is): checked by synth.test.ts.
      if (a === "iam:SimulatePrincipalPolicy") continue;
      assert.ok(types[a], `${a}: add it to iam_resource_types.json`);
      const v = decision([identity], boundary, { action: a, resource: resourceOf(a), context: {} });
      if (v !== "allowed") refused.push(`${a} (${v})`);
    }
    assert.deepEqual(refused, [], `${part}.${id}`);
  }
});

// ── The reader update: the release brings its reads to CoderHelmLogReader here ──

test("after each update of the install's stack, the access stack's reader update brings the release's reads to CoderHelmLogReader in this account", () => {
  const r = ACCESS.Resources;
  // Its code is the signed template's, inline: lambda/native-reader/handler.py.
  assert.equal(r.Reader.Properties.Code.ZipFile, fs.readFileSync(path.join(__dirname, "..", "..", "..", "lambda", "native-reader", "handler.py"), "utf8"));
  assert.deepEqual(r.Reader.Properties.Environment.Variables.ROOT_STACK, { "Fn::Sub": "coderhelm-${Stage}" });
  assert.match(r.Reader.Properties.Code.ZipFile, /STACK = "CoderHelmLogReaderUpdate-release"/);
  assert.match(r.Reader.Properties.Code.ZipFile, /TEMPLATE = "coderhelm-log-reader-update.yaml"/);
  // It runs when the install's stack finishes an update (a version update or a switch), once a day, and at once.
  const pattern = r.ReaderAfterUpdate.Properties.EventPattern;
  assert.deepEqual(pattern.source, ["aws.cloudformation"]);
  assert.deepEqual(pattern.detail["status-details"].status, ["UPDATE_COMPLETE"]);
  assert.deepEqual(pattern.detail["stack-id"], [{ prefix: { "Fn::Sub": "arn:${AWS::Partition}:cloudformation:${AWS::Region}:${AWS::AccountId}:stack/coderhelm-${Stage}/" } }]);
  assert.equal(r.ReaderDaily.Properties.ScheduleExpression, "rate(1 day)");
  for (const rule of ["ReaderAfterUpdate", "ReaderDaily"]) assert.deepEqual(r[rule].Properties.Targets, [{ Id: "reader", Arn: { "Fn::GetAtt": ["Reader", "Arn"] } }]);
  assert.deepEqual(r.ReaderFromUpdate.Properties.SourceArn, { "Fn::GetAtt": ["ReaderAfterUpdate", "Arn"] });
  assert.deepEqual(r.ReaderFromDaily.Properties.SourceArn, { "Fn::GetAtt": ["ReaderDaily", "Arn"] });
  assert.equal(r.ReaderNow.Properties.Version, "v9.8.7");
  assert.equal(r.ReaderNow.Type, "Custom::ReaderUpdate");
});

test("the reader update may write only CoderHelmLogReader's policy, only through CloudFormation, only from a release's update template", () => {
  const p = resolveValue(readerPolicy(), { values: prodValues("111122223333", "us-east-1") }) as PolicyDocument;
  const role = "arn:aws:iam::111122223333:role/CoderHelmLogReader";
  const stack = "arn:aws:cloudformation:us-east-1:111122223333:stack/CoderHelmLogReaderUpdate-release/abc";
  const release = { "cloudformation:TemplateUrl": "https://coderhelm-public.s3.amazonaws.com/launch/v9.8.7/coderhelm-log-reader-update.yaml" };
  const via = { "aws:CalledVia": ["cloudformation.amazonaws.com"] };
  const can = (action: string, resource: string, context: Record<string, string | string[]> = {}) => decision([p], null, { action, resource, context }) === "allowed";
  // From the release's own update template: allowed.
  assert.ok(can("cloudformation:CreateStack", stack, release));
  assert.ok(can("cloudformation:UpdateStack", stack, release));
  // Any other template, or none: refused.
  for (const url of ["https://coderhelm-public.s3.amazonaws.com/launch/v9.8.7/coderhelm-log-reader.yaml", "https://example.com/launch/v9.8.7/coderhelm-log-reader-update.yaml", "https://coderhelm-public.s3.amazonaws.com/cfn/coderhelm-log-reader-update.yaml"]) {
    assert.ok(!can("cloudformation:UpdateStack", stack, { "cloudformation:TemplateUrl": url }), url);
  }
  assert.ok(!can("cloudformation:CreateStack", stack));
  // Any other stack: refused, the install's own included.
  for (const other of ["arn:aws:cloudformation:us-east-1:111122223333:stack/coderhelm-prod/abc", "arn:aws:cloudformation:us-east-1:111122223333:stack/CoderHelmLogReader/abc", "arn:aws:cloudformation:us-east-1:111122223333:stack/CoderHelmLogReaderUpdate-1a2b3c4d/abc"]) {
    assert.ok(!can("cloudformation:UpdateStack", other, release), other);
  }
  // The role's policy: through CloudFormation only, on that one role only.
  assert.ok(can("iam:PutRolePolicy", role, via));
  assert.ok(!can("iam:PutRolePolicy", role), "never directly");
  assert.ok(!can("iam:PutRolePolicy", "arn:aws:iam::111122223333:role/coderhelm-prod-worker", via));
  for (const a of ["iam:AttachRolePolicy", "iam:UpdateAssumeRolePolicy", "iam:CreateRole", "iam:PassRole", "iam:CreatePolicy", "iam:PutRolePermissionsBoundary", "iam:DeleteRolePermissionsBoundary"]) {
    assert.ok(!can(a, role, via), a);
  }
  // No IAM action outside that role (GetRole is the one read).
  for (const st of p.Statement) {
    const acts = ([] as string[]).concat(st.Action ?? []);
    if (acts.some((a) => a.startsWith("iam:"))) assert.equal(st.Resource, role, st.Sid);
  }
});

test("no CoderHelm role can reach the reader update, its role or its rules", () => {
  const names = { ...accessNames({ account: "111122223333", region: "us-east-1", prefix: "coderhelm-prod", suffix: "-111122223333", secretName: "coderhelm/prod/secrets", id: "stack" }), emailIdentity: "owner@example.org" };
  const boundary = boundaryPolicy(names, ["arn:aws:iam::111122223333:policy/coderhelm-prod-deploy-boundary-stack"]);
  const anything: PolicyDocument = { Version: "2012-10-17", Statement: [{ Sid: "All", Effect: "Allow", Action: "*", Resource: "*" }] };
  const reader = READER_UPDATE_NAME.replace("${Stage}", "prod");
  const calls: [string, string][] = [
    ["lambda:InvokeFunction", `arn:aws:lambda:us-east-1:111122223333:function:${reader}`],
    ["lambda:UpdateFunctionCode", `arn:aws:lambda:us-east-1:111122223333:function:${reader}`],
    ["lambda:UpdateFunctionConfiguration", `arn:aws:lambda:us-east-1:111122223333:function:${reader}`],
    ["lambda:AddPermission", `arn:aws:lambda:us-east-1:111122223333:function:${reader}`],
    ["iam:PassRole", `arn:aws:iam::111122223333:role/${reader}`],
    ["iam:PutRolePolicy", `arn:aws:iam::111122223333:role/${reader}`],
    ["iam:UpdateAssumeRolePolicy", `arn:aws:iam::111122223333:role/${reader}`],
    // And the reader role itself stays out of the install's reach, as before.
    ["iam:PutRolePolicy", "arn:aws:iam::111122223333:role/CoderHelmLogReader"],
    ["iam:AttachRolePolicy", "arn:aws:iam::111122223333:role/CoderHelmLogReader"],
    ["cloudformation:UpdateStack", "arn:aws:cloudformation:us-east-1:111122223333:stack/CoderHelmLogReaderUpdate-release/abc"],
  ];
  for (const [action, resource] of calls) {
    assert.notEqual(decision([anything], boundary, { action, resource, context: { "iam:PermissionsBoundary": names.boundaryArn } }), "allowed", `${action} on ${resource}`);
  }
});

test("a one-stack install's gateway and worker know the release keeps their own account's reader current", () => {
  for (const part of ["Api", "Worker"]) {
    const t = JSON.parse(fs.readFileSync(path.join(OUT, "stacks", `${part}.json`), "utf8"));
    const fns = Object.values<any>(t.Resources).filter((r) => r.Type === "AWS::Lambda::Function" && r.Properties?.Environment?.Variables?.INSTALL_OWN_ROLE_ARN);
    assert.ok(fns.length > 0, part);
    for (const f of fns) assert.equal(f.Properties.Environment.Variables.INSTALL_READER_KEPT, "1", part);
  }
});
