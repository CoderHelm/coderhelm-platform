// The migration to one native stack (installer/lib/migrate.mjs) against an
// account held in memory: the parts are nested by import, the secret is
// imported, and the update runs only when no table, bucket, user pool, key
// or secret would be replaced or removed.
import { test } from "node:test";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import * as m from "../lib/migrate.mjs";

const RELEASE = { Resources: { ReleaseAssets: { Properties: { Files: [] } } } };
const BODY = JSON.stringify(RELEASE);
const SHA = crypto.createHash("sha256").update(BODY).digest("hex");

const A = "111122223333";
const R = "us-east-1";
const URL = "https://coderhelm-public.s3.amazonaws.com/launch/v1.2.0/coderhelm.json";
const CFG = { account: A, region: R, stage: "prod", admins: ["o@example.org"], email: { from: "o@example.org" }, models: { provider: "bedrock" }, updates: { auto: true, window: "daily 02:00", timezone: "Europe/Berlin" } };

test("the parts: this install's stacks, not another install's or the old Launch Stack's pieces", () => {
  const stacks = ["coderhelm-prod-database", "coderhelm-prod-api", "coderhelm-prod-log-analyzer", "coderhelm-prod-access", "coderhelm-prod-eu-api", "coderhelm-prod-eu-database", "coderhelm-prod-uninstall-x", "my-app"].map((StackName) => ({ StackName }));
  const parts = m.partStacks("coderhelm-prod", stacks, ["coderhelm-prod-eu"]).map((s) => s.StackName);
  assert.deepEqual(parts, ["coderhelm-prod-database", "coderhelm-prod-api", "coderhelm-prod-log-analyzer"]);
  assert.equal(m.partName("coderhelm-prod-log-analyzer", "coderhelm-prod"), "LogAnalyzer");
});

test("the import template nests each part as it is, retained, and adds the secret", () => {
  const t = m.importTemplate([{ logical: "Database", templateUrl: "https://b.s3.us-east-1.amazonaws.com/migrate/db.json", parameters: [{ ParameterKey: "X", ParameterValue: "1" }] }], { secret: { name: "coderhelm/prod/secrets" } });
  assert.equal(t.Resources.Database.DeletionPolicy, "Retain");
  assert.deepEqual(t.Resources.Database.Properties.Parameters, { X: "1" });
  assert.equal(t.Resources.InstallSecret.DeletionPolicy, "Retain");
  assert.equal(t.Resources.InstallSecret.Properties.Name, "coderhelm/prod/secrets");
});

test("the launch parameters come from the install's config", () => {
  const p = Object.fromEntries(m.nativeParameters(CFG).map((x) => [x.ParameterKey, x.ParameterValue]));
  assert.equal(p.AdminEmail, "o@example.org");
  assert.equal(p.EmailFrom, "");
  assert.equal(p.ModelProvider, "bedrock");
  assert.equal(p.AutoUpdate, "on");
  assert.equal(p.UpdateDay, "daily");
  assert.equal(p.UpdateHour, "2");
  assert.equal(p.KeepDataOnDelete, "true");
});

test("the gate: any replaced or removed data stops it; replaced functions and roles don't", () => {
  const ok = [{ part: "Api", Changes: [{ ResourceChange: { Action: "Modify", ResourceType: "AWS::Lambda::Function", Replacement: "True", LogicalResourceId: "Fn" } }, { ResourceChange: { Action: "Modify", ResourceType: "AWS::DynamoDB::GlobalTable", Replacement: "False", LogicalResourceId: "Table" } }] }];
  assert.deepEqual(m.blockers(ok), []);
  for (const rc of [
    { Action: "Modify", ResourceType: "AWS::DynamoDB::GlobalTable", Replacement: "True", LogicalResourceId: "Table" },
    { Action: "Modify", ResourceType: "AWS::Cognito::UserPool", Replacement: "Conditional", LogicalResourceId: "Pool" },
    { Action: "Remove", ResourceType: "AWS::S3::Bucket", LogicalResourceId: "Data" },
    { Action: "Remove", ResourceType: "AWS::KMS::Key", LogicalResourceId: "Key" },
    { Action: "Modify", ResourceType: "AWS::SecretsManager::Secret", Replacement: "True", LogicalResourceId: "InstallSecret" },
  ]) assert.equal(m.blockers([{ part: "X", Changes: [{ ResourceChange: rc }] }]).length, 1, rc.ResourceType);
});

test("replaced event source mappings and the log groups the update adds are found", () => {
  const cs = [{ part: "Worker", Changes: [
    { ResourceChange: { Action: "Remove", ResourceType: "AWS::Lambda::EventSourceMapping", PhysicalResourceId: "uuid-1" } },
    { ResourceChange: { Action: "Add", ResourceType: "AWS::Logs::LogGroup", LogicalResourceId: "PreSignUpFnLogs" } },
    { ResourceChange: { Action: "Add", ResourceType: "AWS::Logs::LogGroup", LogicalResourceId: "Other" } },
  ] }];
  assert.deepEqual(m.replacedEventSources(cs), ["uuid-1"]);
  const templates = { Worker: { Resources: {
    PreSignUpFnLogs: { Metadata: { "coderhelm:native-log": true }, Properties: { LogGroupName: { "Fn::Join": ["", ["/aws/lambda/", { "Fn::Sub": "coderhelm-${Stage}-pre-signup" }]] } } },
    Other: { Properties: { LogGroupName: "/x" } },
  } } };
  assert.deepEqual(m.addedLogGroups(cs, templates, "prod"), ["/aws/lambda/coderhelm-prod-pre-signup"]);
});

/** An account with the multi-stack install; `replace` makes the update replace a table. */
function fake({ replace = false } = {}) {
  const calls = [];
  const f = { fetched: RELEASE };
  const stacks = { "coderhelm-prod-database": "arn:cfn/db", "coderhelm-prod-api": "arn:cfn/api" };
  const aws = {
    async read(svc, op, args) {
      calls.push({ kind: "read", svc, op, args });
      const a = (k) => args[args.indexOf(k) + 1];
      if (op === "list-stacks") return { StackSummaries: Object.keys(stacks).map((StackName) => ({ StackName })) };
      if (op === "describe-stacks") {
        const n = a("--stack-name");
        if (n === "coderhelm-prod") return { Stacks: [{ StackStatus: calls.some((c) => c.op === "execute-change-set" && c.args.includes("coderhelm-migrate-update")) ? "UPDATE_COMPLETE" : "IMPORT_COMPLETE" }] };
        return { Stacks: [{ StackId: stacks[n], Parameters: [] }] };
      }
      if (op === "get-template") return args.includes("--change-set-name") ? { TemplateBody: f.fetched } : { TemplateBody: { Resources: {} } };
      if (op === "describe-secret") return { ARN: "arn:secret:coderhelm/prod/secrets-AbCdEf" };
      if (op === "describe-change-set") {
        const id = a("--change-set-name");
        if (id === "nested-db") return { Changes: [{ ResourceChange: { Action: "Modify", ResourceType: "AWS::DynamoDB::GlobalTable", Replacement: replace ? "True" : "False", LogicalResourceId: "Table" } }] };
        return { Status: "CREATE_COMPLETE", Changes: id.includes("update") ? [{ ResourceChange: { ResourceType: "AWS::CloudFormation::Stack", LogicalResourceId: "Database", ChangeSetId: "nested-db" } }] : [] };
      }
      if (op === "describe-log-groups") return { logGroups: [] };
      return null;
    },
    async write(svc, op, args) {
      calls.push({ kind: "write", svc, op, args });
      if (op === "create-change-set") return { Id: args[args.indexOf("--change-set-name") + 1] };
      return {};
    },
  };
  const fetchImpl = async (u) => ({ text: async () => BODY, json: async () => (u.endsWith("coderhelm.json") ? RELEASE : {}) });
  return Object.assign(f, { calls, ctx: { aws, log: () => {}, sleep: async () => {}, putObject: async () => {}, fetchImpl } });
}

test("migrate: nests, imports the secret, checks, and stops before running without --yes", async () => {
  const f = fake();
  const r = await m.migrate(f.ctx, { stage: "prod", region: R, account: A, templateUrl: URL, templateSha: SHA, cfg: CFG });
  assert.equal(r.ready, "coderhelm-migrate-update");
  const cs = f.calls.filter((c) => c.op === "create-change-set").map((c) => c.args[c.args.indexOf("--change-set-type") + 1]);
  assert.deepEqual(cs, ["IMPORT", "IMPORT", "UPDATE"]);
  assert.equal(f.calls.filter((c) => c.op === "execute-change-set").length, 2, "the two imports ran, the update did not");
  const nest = f.calls.find((c) => c.op === "create-change-set");
  assert.deepEqual(JSON.parse(nest.args[nest.args.indexOf("--resources-to-import") + 1]).map((x) => x.ResourceIdentifier.StackId), ["arn:cfn/db", "arn:cfn/api"]);
});

test("migrate: a replaced table stops it, deletes the change set, and names the table", async () => {
  const f = fake({ replace: true });
  await assert.rejects(m.migrate(f.ctx, { stage: "prod", region: R, account: A, templateUrl: URL, templateSha: SHA, cfg: CFG, yes: true }), /Database\.Table \(AWS::DynamoDB::GlobalTable\)/);
  assert.ok(f.calls.some((c) => c.op === "delete-change-set"));
  assert.equal(f.calls.filter((c) => c.op === "execute-change-set").length, 2, "never the update");
});

test("migrate: only a CoderHelm release template, only once", async () => {
  await assert.rejects(m.migrate(fake().ctx, { stage: "prod", region: R, account: A, templateUrl: "https://evil.example.com/coderhelm.json", cfg: CFG }), /not a CoderHelm release template/);
  const f = fake();
  const read = f.ctx.aws.read;
  f.ctx.aws.read = async (svc, op, args) => {
    if (op === "list-stacks") return { StackSummaries: [{ StackName: "coderhelm-prod" }] };
    if (op === "describe-stacks" && args.includes("coderhelm-prod")) return { Stacks: [{ Parameters: [{ ParameterKey: "SetupToken" }, { ParameterKey: "StatusId" }] }] };
    return read(svc, op, args);
  };
  await assert.rejects(m.migrate(f.ctx, { stage: "prod", region: R, account: A, templateUrl: URL, templateSha: SHA, cfg: CFG }), /earlier Launch Stack\. Delete it/);
});

test("migrate: a template that isn't the signed one is refused, before and after CloudFormation fetches it", async () => {
  await assert.rejects(m.migrate(fake().ctx, { stage: "prod", region: R, account: A, templateUrl: URL, templateSha: "0".repeat(64), cfg: CFG }), /doesn't match its signed checksum/);
  const f = fake();
  f.fetched = { Resources: { Evil: {} } };
  await assert.rejects(m.migrate(f.ctx, { stage: "prod", region: R, account: A, templateUrl: URL, templateSha: SHA, cfg: CFG }), /not the one checked against the signature/);
  assert.ok(f.calls.some((c) => c.op === "delete-change-set"));
});

test("migrate: run again after it stopped at the checked change set, it carries on", async () => {
  const f = fake();
  const read = f.ctx.aws.read;
  f.ctx.aws.read = async (svc, op, args) => {
    if (op === "list-stacks") return { StackSummaries: [{ StackName: "coderhelm-prod" }, { StackName: "coderhelm-prod-database" }] };
    if (op === "describe-stacks" && args[args.indexOf("--stack-name") + 1] === "coderhelm-prod" && !f.calls.some((c) => c.op === "execute-change-set")) return { Stacks: [{ Parameters: [] }] };
    if (op === "get-template" && !args.includes("--change-set-name") && args.includes("coderhelm-prod")) return { TemplateBody: { Resources: { Database: { Type: "AWS::CloudFormation::Stack" }, InstallSecret: { Type: "AWS::SecretsManager::Secret" } } } };
    return read(svc, op, args);
  };
  const r = await m.migrate(f.ctx, { stage: "prod", region: R, account: A, templateUrl: URL, templateSha: SHA, cfg: CFG, yes: true });
  assert.equal(r.migrated, true);
  const types = f.calls.filter((c) => c.op === "create-change-set").map((c) => c.args[c.args.indexOf("--change-set-type") + 1]);
  assert.deepEqual(types, ["UPDATE"], "no second import");
});
