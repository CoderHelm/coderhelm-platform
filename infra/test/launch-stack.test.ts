// The Launch Stack template (infra/cfn/coderhelm-selfhost-launch.yaml) parses
// as CloudFormation, waits for the install, and grants only what it needs.
//
//   npm test   (compiles with tsc, then runs node --test)

import { test } from "node:test";
import * as assert from "node:assert/strict";
import * as path from "node:path";
import * as cdk from "aws-cdk-lib";
import { Template } from "aws-cdk-lib/assertions";
import { CfnInclude } from "aws-cdk-lib/cloudformation-include";
import { repoPath } from "../lib/paths";

const FILE = repoPath("infra", "cfn", "coderhelm-selfhost-launch.yaml");

function load(): Template {
  const stack = new cdk.Stack(new cdk.App(), "Launch");
  new CfnInclude(stack, "T", { templateFile: FILE });
  return Template.fromStack(stack);
}

test("parses, with the parameters people fill in", () => {
  const t = load().toJSON();
  for (const p of ["AdminEmail", "EmailFrom", "Hostname", "HostedZone", "ModelProvider", "Version", "Telemetry"]) {
    assert.ok(t.Parameters[p], p);
  }
  assert.equal(t.Parameters.Telemetry.Default, "off", "telemetry is off by default");
  assert.equal(t.Parameters.Version.Default, "latest");
});

test("the stack waits for the install and shows the URL", () => {
  const t = load();
  t.resourceCountIs("AWS::CloudFormation::WaitCondition", 1);
  t.resourceCountIs("AWS::CodeBuild::Project", 1);
  const outputs = t.toJSON().Outputs;
  assert.match(JSON.stringify(outputs.CoderHelm.Value), /InstallDone.*Data/);
});

test("the starter may only start the installer build; the build checks the installer's checksum", () => {
  const t = load().toJSON();
  const starter = t.Resources.StarterRole.Properties.Policies[0].PolicyDocument.Statement;
  assert.deepEqual(starter.map((s: any) => s.Action), ["codebuild:StartBuild"]);
  const spec: string = t.Resources.InstallerProject.Properties.Source.BuildSpec;
  assert.match(spec, /sha256sum -c -/);
  assert.match(spec, /coderhelm-selfhost deploy --yes/);
  assert.ok(path.basename(FILE).endsWith(".yaml"));
});
