// CoderHelm's own deployment shares its AWS account with other products:
// its keys say themselves who may use them, its stacks can't be deleted by
// accident, its noreply mail can't leave through another sending path, and
// the hybrid master key reads from its own secret. Self-hosted installs keep
// the defaults (the account is theirs; uninstall deletes the stack).
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
import { loadConfigFile, ResolvedConfig } from "../lib/config";
import { SIGNING_LOCKED_ACTIONS } from "../lib/key-policy";

const INFRA = path.join(__dirname, "..", "..");
const STUBS = fs.mkdtempSync(path.join(os.tmpdir(), "chharden-"));
for (const d of ["gateway", "worker", "mcp"]) {
  fs.mkdirSync(path.join(STUBS, d), { recursive: true });
  fs.writeFileSync(path.join(STUBS, d, d === "mcp" ? "handler.js" : "bootstrap"), "stub");
}
process.env.GATEWAY_ZIP = path.join(STUBS, "gateway");
process.env.WORKER_ZIP = path.join(STUBS, "worker");
process.env.MCP_PROXY_DIR = path.join(STUBS, "mcp");

function synth(cfg: ResolvedConfig, context: Record<string, unknown> = {}) {
  const app = new cdk.App({ outdir: fs.mkdtempSync(path.join(os.tmpdir(), "chhout-")), context });
  buildApp(app, cfg);
  const out: Record<string, { stack: cdk.Stack; t: Template }> = {};
  for (const s of app.node.children) if (s instanceof cdk.Stack) out[s.stackName] = { stack: s, t: Template.fromStack(s) };
  return out;
}

const ACCOUNT = "654654210434";
const hostedCfg = loadConfigFile(path.join(INFRA, "config", "hosted.prod.json"), { CDK_DEFAULT_ACCOUNT: ACCOUNT });
const hosted = synth(hostedCfg, {
  [`hosted-zone:account=${ACCOUNT}:domainName=coderhelm.com:region=us-east-1`]: { Id: "/hostedzone/ZTEST000001", Name: "coderhelm.com." },
});
const selfCfg = loadConfigFile(path.join(INFRA, "config", "selfhost.example.json"), { CDK_DEFAULT_ACCOUNT: "111111111111" });
const selfhost = synth(selfCfg);

function keys(t: Template): any[] {
  return Object.values(t.findResources("AWS::KMS::Key"));
}
function allKeys(app: ReturnType<typeof synth>): any[] {
  return Object.values(app).flatMap(({ t }) => keys(t));
}
function policies(t: Template): any[] {
  // Large role policies overflow into managed policies; read both.
  return [
    ...Object.values(t.findResources("AWS::IAM::Policy")),
    ...Object.values(t.findResources("AWS::IAM::ManagedPolicy")),
  ].flatMap((p: any) => p.Properties.PolicyDocument.Statement);
}
const s = (v: unknown) => JSON.stringify(v);

test("hosted keys: no blanket account grant; managing and using each key are separate and named", () => {
  const ks = allKeys(hosted);
  assert.equal(ks.length, 4, "dynamo, secrets, s3 and the signing key");
  for (const k of ks) {
    const st: any[] = k.Properties.KeyPolicy.Statement;
    for (const x of st) {
      if (x.Effect === "Allow") {
        assert.ok(x.Condition?.ArnLike?.["aws:PrincipalArn"], `every allow names who: ${s(x)}`);
        assert.ok(x.Action !== "kms:*" && !(Array.isArray(x.Action) && x.Action.includes("kms:*")), "never kms:*");
      }
    }
    const manage = st.find((x) => x.Sid === "ManageKeyAdminsAndCloudFormationOnly");
    assert.ok(manage && !s(manage.Action).match(/Decrypt|Encrypt|Sign|GenerateDataKey/), "managing never uses the key");
    const use = st.find((x) => x.Sid === "UseKeyThisDeploymentOnly");
    assert.ok(s(use.Condition).includes("role/coderhelm-prod-*"), "only this deployment's roles use it");
    assert.equal(k.Properties.PendingWindowInDays, 30);
  }
});

test("hosted signing key: policy, deletion, disabling and grants are a person's alone", () => {
  const sign = allKeys(hosted).find((k) => k.Properties.KeySpec === "ECC_NIST_EDWARDS25519");
  assert.ok(sign);
  const deny = sign.Properties.KeyPolicy.Statement.find((x: any) => x.Sid === "SigningKeyChangesByPeopleOnly");
  assert.equal(deny.Effect, "Deny");
  assert.deepEqual([...deny.Action].sort(), [...SIGNING_LOCKED_ACTIONS].sort());
  assert.ok(s(deny.Condition).includes("AWSReservedSSO_AdministratorAccess"));
  assert.ok(!s(deny.Condition).includes("cfn-exec"), "CloudFormation isn't excepted");
  assert.equal(sign.Properties.BypassPolicyLockoutSafetyCheck, true);
  assert.ok(sign.Properties.KeyPolicy.Statement.some((x: any) => x.Sid === "OnlyCoderHelmSignersSign"), "the signer tag rule stays");
});

test("self-hosted keys keep CDK's default policy and no termination protection", () => {
  for (const k of allKeys(selfhost)) {
    assert.ok(!s(k.Properties.KeyPolicy).includes("UseKeyThisDeploymentOnly"));
    assert.equal(k.Properties.PendingWindowInDays, 30);
  }
  for (const { stack } of Object.values(selfhost)) assert.notEqual(stack.terminationProtection, true);
});

test("hosted stacks have termination protection", () => {
  const names = Object.keys(hosted);
  assert.ok(names.length >= 5);
  for (const [name, { stack }] of Object.entries(hosted)) assert.equal(stack.terminationProtection, true, name);
});

test("hosted noreply mail is limited to its identity, its configuration set and its templates", () => {
  const all = Object.values(hosted).flatMap(({ t }) => policies(t));
  const sends = all.filter((x) => s(x.Action).includes("ses:SendTemplatedEmail"));
  assert.ok(sends.length >= 1);
  for (const x of sends) {
    assert.ok(Array.isArray(x.Resource) && !x.Resource.includes("*"), s(x.Resource));
    assert.ok(s(x.Resource).includes("identity/coderhelm.com"));
    assert.ok(s(x.Resource).includes("configuration-set/coderhelm-prod-emails"));
  }
  const dests = Object.values(hosted["coderhelm-prod-email"].t.findResources("AWS::SES::ConfigurationSetEventDestination"));
  const events = dests.flatMap((d: any) => d.Properties.EventDestination.MatchingEventTypes);
  for (const e of ["bounce", "complaint", "reject"]) assert.ok(events.includes(e), `${e} is reported`);
});

test("cross-account reads always carry the private ID, except connecting's check", () => {
  for (const name of ["coderhelm-prod-api", "coderhelm-prod-worker"]) {
    const assumes = policies(hosted[name].t).filter((x) => x.Action === "sts:AssumeRole" && s(x.Resource).includes("Coderhelm"));
    assert.ok(assumes.length >= 1, name);
    for (const x of assumes) {
      if (x.Sid === "CheckRoleNeedsPrivateId") {
        assert.equal(x.Condition.StringEquals["sts:RoleSessionName"], "coderhelm-check");
        assert.ok(!s(x.Resource).includes("CoderHelmLogReader"));
      } else {
        assert.equal(x.Condition?.Null?.["sts:ExternalId"], "false", `${name}: ${s(x)}`);
      }
    }
  }
});

test("the hybrid master key reads from its own secret, and nothing grants the absent admin-key secret", () => {
  for (const name of ["coderhelm-prod-api", "coderhelm-prod-worker"]) {
    const t = hosted[name].t;
    assert.ok(s(t.toJSON()).includes("coderhelm/prod/hybrid-master"), `${name} knows the secret`);
    assert.ok(policies(t).some((x) => x.Sid === "ReadHybridMasterSecret"), `${name} may read it`);
  }
  const streaming = hosted["coderhelm-prod-streaming"];
  if (streaming) assert.ok(!s(streaming.t.toJSON()).includes("hybrid-master"), "streaming never gets it");
  assert.ok(!s(hosted["coderhelm-prod-worker"].t.toJSON()).includes("anthropic-admin-keys-"), "no grant on a secret that doesn't exist");
});
