// The Launch Stack template (infra/cfn/coderhelm-selfhost-launch.yaml) parses
// as CloudFormation, waits for the install, checks the release's signature,
// grants only what each piece needs, and keeps the progress file public-safe.
//
//   npm test   (compiles with tsc, then runs node --test)

import { test } from "node:test";
import * as assert from "node:assert/strict";
import * as cdk from "aws-cdk-lib";
import { Template } from "aws-cdk-lib/assertions";
import { CfnInclude } from "aws-cdk-lib/cloudformation-include";
import { repoPath } from "../lib/paths";
import { CHECK_VERSION, COSIGN_LINUX_AMD64_SHA256, installCosignCommands, VERIFY_SUMS, verifyTagCommands } from "../lib/release-verify";

const FILE = repoPath("infra", "cfn", "coderhelm-selfhost-launch.yaml");

function load(params: Record<string, string> = {}): Template {
  const stack = new cdk.Stack(new cdk.App(), "Launch");
  new CfnInclude(stack, "T", { templateFile: FILE, parameters: params });
  return Template.fromStack(stack);
}

test("parses, with the parameters the install guide fills in", () => {
  const t = load().toJSON();
  for (const p of ["AdminEmail", "Stage", "ModelProvider", "Version", "Hostname", "HostedZone", "EmailFrom", "Telemetry", "StatusId", "SetupToken"]) {
    assert.ok(t.Parameters[p], p);
  }
  assert.equal(t.Parameters.Telemetry.Default, "off", "telemetry is off by default");
  assert.equal(t.Parameters.Version.Default, "latest", "the release stamps its tag (selfhost/launch/stamp-template.mjs)");
  assert.equal(t.Parameters.ModelProvider.Default, "bedrock", "no API key needed to start");
  assert.equal(t.Parameters.Hostname.Default, "", "no DNS work by default");
  assert.equal(t.Parameters.EmailFrom.Default, "", "the admin's own address by default: no DNS work");
  assert.equal(t.Parameters.EmailTransport.Default, "ses", "email through CoderHelm only when chosen");
  assert.deepEqual(t.Parameters.EmailTransport.AllowedValues, ["ses", "coderhelm"]);
  assert.equal(t.Parameters.HostedZone.Default, "", "the zone is found in the account");
  assert.match(t.Parameters.Hostname.Description, /any other DNS provider/);
  assert.equal(t.Parameters.AutoUpdate.Default, "off", "updates wait for an admin by default");
  assert.equal(t.Parameters.UpdateWindow.Default, "Sun 03:00");
  assert.equal(t.Parameters.UpdateTimezone.Default, "UTC");
  // Quick-create links ignore NoEcho parameters, so the guide's values can't be NoEcho.
  assert.equal(t.Parameters.SetupToken.NoEcho, undefined);
  assert.equal(t.Parameters.StatusId.NoEcho, undefined);
});

test("only AdminEmail must be typed: every other parameter has a default", () => {
  const t = load().toJSON();
  const required = Object.entries(t.Parameters as Record<string, any>).filter(([, p]) => p.Default === undefined).map(([k]) => k);
  assert.deepEqual(required, ["AdminEmail"]);
});

test("the stack waits for the install and shows the URL and the way back to the guide", () => {
  const t = load();
  t.resourceCountIs("AWS::CloudFormation::WaitCondition", 1);
  t.resourceCountIs("AWS::CodeBuild::Project", 1);
  const outputs = t.toJSON().Outputs;
  assert.match(JSON.stringify(outputs.CoderHelm.Value), /InstallDone.*Data/);
  assert.match(JSON.stringify(outputs.InstallGuide.Value), /docs\.coderhelm\.com\/self-host\/install\/\?id=/);
});

test("the build checks the release's signature and checksums before running anything from it", () => {
  const t = load().toJSON();
  const spec: string = t.Resources.InstallerProject.Properties.Source.BuildSpec;
  const at = (s: string) => {
    const i = spec.indexOf(s);
    assert.ok(i >= 0, `buildspec has ${s}`);
    return i;
  };
  const verify = at("cosign verify-blob");
  at("sha256sum -c -");
  const sums = spec.lastIndexOf("sha256sum -c -");
  const runs = at("node launch-bootstrap.mjs");
  assert.ok(verify < sums && sums < runs, "verify, then checksums, then run");
  assert.ok(spec.includes(VERIFY_SUMS), "the exact identity: release.yml on refs/tags/$VERSION, GitHub's issuer");
  // The tag check, with the pinned maintainer keys, before the signature (it sets $COMMIT).
  let last = -1;
  for (const line of verifyTagCommands()) {
    const i = spec.indexOf(line);
    assert.ok(i > last, `the tag check, in order: ${line}`);
    last = i;
  }
  assert.ok(last < spec.indexOf(VERIFY_SUMS), "the tag is checked before the signature");
  assert.ok(VERIFY_SUMS.includes('--certificate-github-workflow-sha "$COMMIT"'));
  assert.ok(spec.indexOf(CHECK_VERSION) >= 0 && spec.indexOf(CHECK_VERSION) < verify, "VERSION is a release tag before it is used");
  for (const line of installCosignCommands()) assert.ok(spec.includes(line), `pinned cosign: ${line}`);
  assert.ok(spec.includes(COSIGN_LINUX_AMD64_SHA256));
  assert.ok(spec.includes("grep ' coderhelm-selfhost.tgz$' SHA256SUMS | sha256sum -c -"));
  assert.ok(spec.includes("grep ' launch-bootstrap.mjs$' SHA256SUMS | sha256sum -c -"));
});

test("no loose signature checks: no identity regexp, no skipped transparency log, HTTPS-only downloads", () => {
  const spec: string = load().toJSON().Resources.InstallerProject.Properties.Source.BuildSpec;
  assert.doesNotMatch(spec, /identity-regexp|insecure-ignore|ignore-tlog|--offline/);
  for (const line of spec.split("\n").filter((l) => /\bcurl\b/.test(l))) {
    assert.match(line, /curl --proto =https --proto-redir =https --tlsv1\.2/, line);
  }
  assert.ok(spec.includes('B="https://github.com/CoderHelm/coderhelm-platform/releases/download/$VERSION"'));
});

test("the starter may only start the installer and open the progress file", () => {
  const t = load().toJSON();
  const statements = t.Resources.StarterRole.Properties.Policies[0].PolicyDocument.Statement;
  assert.deepEqual(statements.map((s: any) => s.Action), ["codebuild:StartBuild", ["s3:PutBucketPolicy", "s3:PutObject"]]);
  const code: string = t.Resources.StarterFunction.Properties.Code.ZipFile;
  assert.match(code, /status\.json/);
  assert.doesNotMatch(code, /AdminEmail|ADMIN_EMAIL|SetupToken/, "nothing personal in the public file");
});

test("the progress bucket: only with a progress ID, kept after the stack, no public ACLs, expiring", () => {
  const without = load().toJSON();
  assert.equal(without.Resources.StatusBucket.Condition, "HasStatus", "no ID, no bucket");
  const t = load();
  t.hasResource("AWS::S3::Bucket", { DeletionPolicy: "Retain" });
  t.hasResourceProperties("AWS::S3::Bucket", {
    BucketName: { "Fn::Sub": "coderhelm-setup-${StatusId}" },
    PublicAccessBlockConfiguration: { BlockPublicAcls: true, IgnorePublicAcls: true },
    LifecycleConfiguration: { Rules: [{ ExpirationInDays: 30, Status: "Enabled" }] },
  });
});

test("the installer's log outlives a rollback (no log group resource; names follow the stack)", () => {
  const t = load();
  t.resourceCountIs("AWS::Logs::LogGroup", 0);
  t.hasResourceProperties("AWS::CodeBuild::Project", { Name: { "Fn::Sub": "${AWS::StackName}-installer" } });
});

test("the domain and update settings reach the installer", () => {
  const t = load();
  const env = JSON.stringify(t.findResources("AWS::CodeBuild::Project"));
  for (const v of ["HOSTNAME_ARG", "HOSTED_ZONE", "AUTO_UPDATE", "UPDATE_WINDOW", "UPDATE_TIMEZONE"]) assert.ok(env.includes(v), v);
});

test("inline functions answer CloudFormation themselves, never through the cfnresponse helper", () => {
  // The helper isn't provided on every runtime: a missing import crashed the
  // starter and left the stack waiting an hour for an answer that never came.
  const fs = require("node:fs") as typeof import("node:fs");
  for (const f of fs.readdirSync(repoPath("infra", "cfn"))) {
    if (!f.endsWith(".yaml")) continue;
    const src = fs.readFileSync(repoPath("infra", "cfn", f), "utf8");
    assert.doesNotMatch(src, /^\s*import[^\n]*\bcfnresponse\b/m, `${f} imports cfnresponse`);
    assert.doesNotMatch(src, /cfnresponse\.send\(/, `${f} calls cfnresponse.send`);
  }
  const t = load().toJSON();
  const code: string = t.Resources.StarterFunction.Properties.Code.ZipFile;
  assert.match(code, /def respond\(event, context, status/);
  assert.match(code, /urllib\.request\.Request\(event\["ResponseURL"\]/);
  assert.match(code, /respond\(event, context, "FAILED"/);
});
