// The uninstall stack (infra/cfn/coderhelm-selfhost-uninstall.yaml): exactly
// what infra/lib/uninstall-stack.ts writes, confirms the install's name,
// checks the release like the installer, gives the build deletion rights
// only through its window, and removes itself by rolling back.
//
// And the window's scope (infra/lib/uninstall-cases.ts) with the local IAM
// evaluator: everything of coderhelm-prod may go, nothing else may.
//
//   npm test   (compiles with tsc, then runs node --test)

import { test } from "node:test";
import * as assert from "node:assert/strict";
import * as fs from "node:fs";
import * as cdk from "aws-cdk-lib";
import { Template } from "aws-cdk-lib/assertions";
import { CfnInclude } from "aws-cdk-lib/cloudformation-include";
import { repoPath } from "../lib/paths";
import { CHECK_VERSION, COSIGN_LINUX_AMD64_SHA256, installCosignCommands, VERIFY_SUMS, verifyTagCommands } from "../lib/release-verify";
import { buildspec, buildspecSha256, stackNames, storedParts, uninstallTemplateText, UNINSTALL_VARIABLES, windowSha256 } from "../lib/uninstall-stack";
import * as crypto from "node:crypto";
import { DATA_SID, expandWindow, TOKENS, uninstallWindowStatements, uninstallNames } from "../lib/uninstall-access";
import { casePolicies, CASE_NOW, CASE_USER_ID, CASE_EXPIRES, sessionCases, uninstallCases } from "../lib/uninstall-cases";
import { decision } from "./iam-eval";

const FILE = repoPath("infra", "cfn", "coderhelm-selfhost-uninstall.yaml");

function load(params: Record<string, string> = { Stage: "prod", ConfirmStage: "prod" }): any {
  const stack = new cdk.Stack(new cdk.App(), "Uninstall");
  new CfnInclude(stack, "T", { templateFile: FILE, parameters: params });
  return Template.fromStack(stack).toJSON();
}

test("the template is exactly what infra/lib/uninstall-stack.ts writes (scripts/uninstall-stack.sh)", () => {
  assert.equal(fs.readFileSync(FILE, "utf8"), uninstallTemplateText());
});

test("the install's name, typed twice: the stack's rule refuses a mismatch; keeping the data is the default", () => {
  const t = JSON.parse(uninstallTemplateText().split("\n").filter((l) => !l.startsWith("#")).join("\n"));
  assert.equal(t.Parameters.Stage.Default, undefined, "no default install to remove");
  assert.equal(t.Parameters.ConfirmStage.Default, undefined);
  assert.deepEqual(t.Rules.TheNameTwice.Assertions[0].Assert, { "Fn::Equals": [{ Ref: "ConfirmStage" }, { Ref: "Stage" }] });
  assert.equal(t.Parameters.Mode.Default, "keep");
  assert.deepEqual(t.Parameters.Mode.AllowedValues, ["keep", "everything"]);
  assert.match(t.Parameters.Mode.Description, /7-day minimum/);
  assert.equal(t.Parameters.Version.Default, "latest", "the release stamps its tag (selfhost/launch/stamp-template.mjs)");
  load();
});

test("the build checks the release exactly as the installer does before running anything from it", () => {
  const spec = buildspec();
  let last = -1;
  for (const line of [...installCosignCommands(), CHECK_VERSION, ...verifyTagCommands(), VERIFY_SUMS, "grep ' coderhelm-selfhost.tgz$' SHA256SUMS | sha256sum -c -"]) {
    const i = spec.indexOf(line);
    assert.ok(i > last, `in order: ${line.slice(0, 80)}`);
    last = i;
  }
  assert.ok(spec.includes(COSIGN_LINUX_AMD64_SHA256));
  assert.ok(spec.indexOf("sha256sum -c -", spec.indexOf(VERIFY_SUMS)) < spec.indexOf("npm install -g"), "checked, then installed");
  assert.ok(spec.indexOf("npm install -g") < spec.indexOf("aws lambda invoke"), "no window before the release checks out");
  assert.ok(spec.indexOf("aws lambda invoke") < spec.indexOf("coderhelm-selfhost uninstall --guided"));
  assert.doesNotMatch(spec, /identity-regexp|insecure-ignore|ignore-tlog|--offline/);
  for (const line of spec.split("\n").filter((l) => /\bcurl\b/.test(l))) assert.match(line, /curl --proto =https --proto-redir =https --tlsv1\.2/, line);
  // The window ends with the build; the stack is always answered FAILURE, so it rolls itself back.
  const post = spec.slice(spec.indexOf("post_build:"));
  assert.ok(post.includes("delete-role-policy") && post.includes("uninstall-window"));
  assert.match(post, /Status:"FAILURE"/);
  assert.doesNotMatch(post, /Status:ok\?|"SUCCESS"/);
});

test("the window function checks this template's buildspec, image and variables, and uses its own mode and name", () => {
  const t = JSON.parse(uninstallTemplateText().split("\n").filter((l) => !l.startsWith("#")).join("\n"));
  load();
  const v = t.Resources.WindowFunction.Properties.Environment.Variables;
  assert.equal(v.BUILDSPEC_SHA256, buildspecSha256());
  assert.equal(t.Resources.Project.Properties.Source.BuildSpec, buildspec());
  assert.equal(v.VARIABLES, t.Resources.Project.Properties.Environment.EnvironmentVariables.map((e: any) => e.Name).join(","));
  assert.equal(v.VARIABLES, UNINSTALL_VARIABLES.join(","));
  assert.equal(v.IMAGE, t.Resources.Project.Properties.Environment.Image);
  assert.deepEqual(v.MODE, { Ref: "Mode" });
  assert.deepEqual(v.STAGE, { Ref: "Stage" });
  const code: string = t.Resources.WindowFunction.Properties.Code.ZipFile;
  assert.equal(code, fs.readFileSync(repoPath("infra", "cfn", "uninstall", "window.py"), "utf8"));
  assert.match(code, /E\["MODE"\] != "everything"/);
  assert.match(code, /MAX_WINDOW = 9400/);
  const parts = Object.keys(t.Resources).filter((k) => k.startsWith("WindowPolicy"));
  assert.equal(v.PARTS, String(parts.length));
  // The statements are pinned: the window function refuses parameters someone changed.
  assert.equal(v.WINDOW_SHA256, windowSha256());
  assert.deepEqual(parts.map((k) => t.Resources[k].Properties.Value), storedParts());
  assert.deepEqual(v.STACK_ID, { Ref: "AWS::StackId" });
  assert.match(code, /CREATE_IN_PROGRESS/);
  assert.match(code, /debugSession/);
  assert.match(code, /AWSCodeBuild-/);
});

test("every role carries the uninstall boundary; the build is idle until its window; nothing allows a wildcard action", () => {
  const t = load();
  for (const [id, r] of Object.entries<any>(t.Resources)) {
    if (r.Type !== "AWS::IAM::Role") continue;
    assert.deepEqual(r.Properties.PermissionsBoundary, { Ref: "Boundary" }, `${id} carries the boundary`);
    assert.equal(r.Properties.ManagedPolicyArns, undefined, `${id}: no managed policies`);
    for (const p of r.Properties.Policies) for (const st of p.PolicyDocument.Statement) {
      for (const a of [st.Action].flat()) assert.ok(!/\*/.test(a), `${id}: ${a}`);
    }
  }
  const idle = t.Resources.BuildRole.Properties.Policies.flatMap((p: any) => p.PolicyDocument.Statement.flatMap((s: any) => [s.Action].flat()));
  assert.deepEqual(idle.sort(), ["iam:DeleteRolePolicy", "lambda:InvokeFunction", "logs:CreateLogStream", "logs:PutLogEvents", "s3:PutObject"]);
  for (const st of uninstallWindowStatements(stackNames()).filter((s) => s.Effect === "Allow")) {
    for (const a of [st.Action].flat()) assert.ok(a !== "*" && !/^[a-z0-9-]+:\*$/.test(String(a)), `${st.Sid}: ${a}`);
  }
});

test("the uninstaller removes itself by rolling back; only its log and progress bucket wait for the starter", () => {
  const t = load({ Stage: "prod", ConfirmStage: "prod", StatusId: "abcdefghijklmnop" });
  for (const [id, r] of Object.entries<any>(t.Resources)) {
    const keep = r.DeletionPolicy === "Retain";
    assert.equal(keep, ["StatusBucket", "BuildLogs"].includes(id), `${id}: ${r.DeletionPolicy ?? "Delete"}`);
  }
  assert.equal(t.Resources.Done.Properties.Timeout, "5400");
  assert.deepEqual(t.Resources.Project.Properties.LogsConfig.CloudWatchLogs.GroupName, { Ref: "BuildLogs" });
  for (const f of ["WindowFunction", "StarterFunction"]) assert.ok(t.Resources[f].Properties.LoggingConfig.LogGroup.Ref, `${f} logs to a group the stack removes`);
  const starter: string = t.Resources.StarterFunction.Properties.Code.ZipFile;
  assert.equal(starter, fs.readFileSync(repoPath("infra", "cfn", "uninstall", "starter.py"), "utf8"));
  assert.match(starter, /buildStatus"\) == "SUCCEEDED"/);
  assert.match(starter, /def respond\(event, context, status/);
  assert.doesNotMatch(starter, /^\s*import[^\n]*\bcfnresponse\b/m);
  // No role may create log groups: nothing the stack leaves behind is recreated.
  for (const r of ["BuildRole", "WindowRole", "StarterRole"]) assert.doesNotMatch(JSON.stringify(t.Resources[r]), /logs:CreateLogGroup/);
});

test("each window part fits a standard parameter with the longest names substituted", () => {
  // Stored as written (the window function fills in the names): each part fits as it is.
  for (const p of storedParts()) {
    assert.ok(p.length <= 4096, `${p.length}`);
    assert.doesNotMatch(p, /\$\{/, "no ${...} for CloudFormation to read");
  }
  assert.equal(windowSha256(), crypto.createHash("sha256").update(storedParts().join("\n")).digest("hex"));
});

test("a common install's window fits a role's inline policies with the idle policy", () => {
  const n = uninstallNames({ account: "111122223333", region: "us-east-1", stage: "prod", uid: "12345678", statusId: "abcdefghijklmnop" });
  const doc = expandWindow(uninstallWindowStatements(n), { userId: CASE_USER_ID, expires: CASE_EXPIRES, ids: ["11111111", "22222222"], launch: ["coderhelm-prod", "coderhelm-prod-2"], setup: ["s".repeat(16), "t".repeat(16)], siblings: ["coderhelm-prod-eu"], buckets: ["coderhelm-prod-dashboard-111122223333", "coderhelm-prod-sandbox-111122223333"], data: ["coderhelm-prod-data-111122223333"] }, "everything");
  assert.ok(JSON.stringify(doc).length <= 9400, `${JSON.stringify(doc).length}`);
  // No token left, and keep mode has no data statement.
  assert.doesNotMatch(JSON.stringify(doc), /__[A-Z_]+__/);
  const keep = expandWindow(uninstallWindowStatements(n), { userId: "u", expires: CASE_EXPIRES, ids: ["11111111"], launch: ["coderhelm-prod"], setup: [], siblings: [] }, "keep");
  assert.ok(!keep.Statement.some((s) => s.Sid.startsWith(DATA_SID)));
  assert.ok(!keep.Statement.some((s) => s.Sid === "OtherInstallsInsideThisName"), "no longer installs: no statement");
  assert.ok(Object.values(TOKENS).every((t) => !JSON.stringify(keep).includes(t)));
});

// ── The scope, with the local evaluator ────────────────────────────────

const ctx = (c: Record<string, string | string[]> = {}) => ({ "aws:userid": CASE_USER_ID, "aws:CurrentTime": CASE_NOW, ...c });

for (const mode of ["keep", "everything"] as const) {
  test(`${mode}: every call on the install's pieces is allowed, every other call refused`, () => {
    const { idle, window, boundary } = casePolicies(mode);
    const wrong = uninstallCases()
      .map((c) => ({ c, d: decision([idle, window], boundary, { action: c.action, resource: c.resource, context: ctx(c.context) }) }))
      .filter(({ c, d }) => (d === "allowed") !== c.allowed.includes(mode))
      .map(({ c, d }) => `${c.why} (${c.action} ${c.resource}): ${d}`);
    assert.deepEqual(wrong, []);
  });
}

test("another session of the build role, or the same after the window's end, may delete nothing", () => {
  const { idle, window, boundary } = casePolicies("everything");
  for (const c of sessionCases()) {
    if (c.action === "iam:DeleteRolePolicy" || c.action === "s3:PutObject") continue;
    for (const extra of [{ "aws:userid": "AROAEXAMPLEBUILDROLE1:AWSCodeBuild-another-build" }, { "aws:CurrentTime": "2026-10-30T13:41:00Z" }]) {
      const d = decision([idle, window], boundary, { action: c.action, resource: c.resource, context: { ...ctx(c.context), ...extra } });
      assert.equal(d, "explicitDeny", `${c.why} with ${JSON.stringify(extra)}`);
    }
  }
});
