// The streaming function's role allows every DynamoDB action its code was
// seen to call.
//
// infra/streaming-dynamodb.json lists what the stream routes call, by table.
// The gateway's tests (services/gateway/src/routes/stream_access.rs) drive
// both stream routes against an AWS stand-in that records each DynamoDB call
// and fail when one isn't in that file. This test synthesizes the streaming
// stack (self-hosted and hosted), resolves its role's policies to plain ARNs
// (cross-stack table ARNs included) and evaluates each listed action on each
// listed table, so a check added to a shared code path can't reach
// production with a role that refuses it.
//
//   npm test   (compiles with tsc, then runs node --test)

import { test } from "node:test";
import * as assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import * as cdk from "aws-cdk-lib";
import { buildApp } from "../lib/app";
import { loadConfigFile, resolveConfig, ResolvedConfig } from "../lib/config";
import { PolicyDocument, Statement } from "../lib/access";
import { allowed } from "./iam-eval";

const INFRA = path.join(__dirname, "..", "..");

const STUBS = fs.mkdtempSync(path.join(os.tmpdir(), "chstream-"));
for (const d of ["gateway", "worker", "mcp"]) {
  fs.mkdirSync(path.join(STUBS, d), { recursive: true });
  fs.writeFileSync(path.join(STUBS, d, d === "mcp" ? "handler.js" : "bootstrap"), "stub");
}
process.env.GATEWAY_ZIP = path.join(STUBS, "gateway");
process.env.WORKER_ZIP = path.join(STUBS, "worker");
process.env.MCP_PROXY_DIR = path.join(STUBS, "mcp");

/** `{ "users": ["dynamodb:GetItem"], "runs/index/x": [...] }` */
export function granted(): Record<string, string[]> {
  const file = JSON.parse(fs.readFileSync(path.join(INFRA, "streaming-dynamodb.json"), "utf8"));
  return file.dynamodb;
}

type Templates = Record<string, any>;

function synth(cfg: ResolvedConfig, context: Record<string, unknown> = {}): Templates {
  const app = new cdk.App({ outdir: fs.mkdtempSync(path.join(os.tmpdir(), "chstreamout-")), context });
  buildApp(app, cfg);
  const asm = app.synth();
  const out: Templates = {};
  for (const s of app.node.children) {
    if (s instanceof cdk.Stack) out[s.stackName] = asm.getStackByName(s.stackName).template;
  }
  return out;
}

/** A template value as a plain string: pseudo parameters, joins, and
 * references to tables in this stack or exported by another. */
function resolver(all: Templates, account: string, region: string) {
  const exports: Record<string, { stack: any; value: any }> = {};
  for (const t of Object.values(all)) {
    for (const o of Object.values<any>(t.Outputs ?? {})) {
      const name = o.Export?.Name;
      if (typeof name === "string") exports[name] = { stack: t, value: o.Value };
    }
  }
  const arnOf = (stack: any, logicalId: string, attr: string): string => {
    const r = stack.Resources?.[logicalId];
    if (!r) throw new Error(`no resource ${logicalId}`);
    if (r.Type === "AWS::DynamoDB::GlobalTable" || r.Type === "AWS::DynamoDB::Table") {
      const name = resolve(stack, r.Properties.TableName);
      if (attr === "Arn") return `arn:aws:dynamodb:${region}:${account}:table/${name}`;
    }
    // Anything else (a queue, a key, a bucket) can't be a table: a
    // placeholder that matches no table ARN.
    return `unresolved:${logicalId}.${attr}`;
  };
  function resolve(stack: any, v: any): string {
    if (typeof v === "string") return v;
    if (v?.Ref === "AWS::Region") return region;
    if (v?.Ref === "AWS::AccountId") return account;
    if (v?.Ref === "AWS::Partition") return "aws";
    if (v?.Ref === "AWS::URLSuffix") return "amazonaws.com";
    if (v?.Ref) return `unresolved:${v.Ref}`;
    if (v?.["Fn::Join"]) return v["Fn::Join"][1].map((p: any) => resolve(stack, p)).join(v["Fn::Join"][0]);
    if (v?.["Fn::GetAtt"]) return arnOf(stack, v["Fn::GetAtt"][0], v["Fn::GetAtt"][1]);
    if (v?.["Fn::ImportValue"]) {
      const name = resolve(stack, v["Fn::ImportValue"]);
      const e = exports[name];
      if (!e) throw new Error(`no export ${name}`);
      return resolve(e.stack, e.value);
    }
    return `unresolved:${JSON.stringify(v)}`;
  }
  return resolve;
}

/** The policies attached to the streaming function's role, resolved. */
function streamingPolicies(all: Templates, prefix: string, account: string, region: string): PolicyDocument[] {
  const stack = all[`${prefix}-streaming`];
  assert.ok(stack, "a streaming stack");
  const resolve = resolver(all, account, region);
  const fn = Object.values<any>(stack.Resources).find(
    (r) => r.Type === "AWS::Lambda::Function" && r.Properties.FunctionName === `${prefix}-streaming`,
  );
  assert.ok(fn, "the streaming function");
  const roleId = fn.Properties.Role["Fn::GetAtt"][0];
  const docs: PolicyDocument[] = [];
  for (const r of Object.values<any>(stack.Resources)) {
    if (r.Type !== "AWS::IAM::Policy" && r.Type !== "AWS::IAM::ManagedPolicy") continue;
    const roles = (r.Properties.Roles ?? []).map((x: any) => x.Ref);
    if (!roles.includes(roleId)) continue;
    const Statement: Statement[] = r.Properties.PolicyDocument.Statement.map((s: any, i: number) => {
      const out: any = { Sid: s.Sid ?? `s${i}`, Effect: s.Effect, Action: s.Action, NotAction: s.NotAction, Condition: s.Condition };
      if (s.Resource !== undefined) out.Resource = [s.Resource].flat().map((x: any) => resolve(stack, x));
      if (s.NotResource !== undefined) out.NotResource = [s.NotResource].flat().map((x: any) => resolve(stack, x));
      return out;
    });
    docs.push({ Version: "2012-10-17", Statement });
  }
  return docs;
}

/** The ARN of a table (or a table's index) as the file names it. */
export function tableArn(prefix: string, account: string, region: string, resource: string): string {
  const [table, ...rest] = resource.split("/");
  const name = table === "main" ? prefix : `${prefix}-${table}`;
  return `arn:aws:dynamodb:${region}:${account}:table/${[name, ...rest].join("/")}`;
}

function refusals(all: Templates, prefix: string, account: string, region: string): string[] {
  const docs = streamingPolicies(all, prefix, account, region);
  const out: string[] = [];
  for (const [resource, actions] of Object.entries(granted())) {
    for (const action of actions) {
      const arn = tableArn(prefix, account, region, resource);
      if (!allowed(docs, null, { action, resource: arn })) out.push(`${action} on ${arn}`);
    }
  }
  return out;
}

test("the file lists the member check's read of the users table", () => {
  assert.ok(granted().users?.includes("dynamodb:GetItem"), "users GetItem");
  for (const actions of Object.values(granted())) {
    for (const a of actions) assert.match(a, /^dynamodb:[A-Za-z]+$/);
  }
});

test("self-hosted: the streaming role allows every DynamoDB call its routes make", () => {
  const cfg = resolveConfig({
    account: "111122223333",
    region: "us-west-2",
    stage: "prod",
    email: { from: "coderhelm@example.org" },
    admins: ["owner@example.org"],
  });
  const all = synth(cfg);
  assert.deepEqual(refusals(all, cfg.prefix, "111122223333", "us-west-2"), []);
});

test("hosted: the streaming role allows every DynamoDB call its routes make", () => {
  const cfg = loadConfigFile(path.join(INFRA, "config", "hosted.prod.json"), { CDK_DEFAULT_ACCOUNT: "654654210434" });
  const all = synth(cfg, {
    "hosted-zone:account=654654210434:domainName=coderhelm.com:region=us-east-1": { Id: "/hostedzone/ZTEST000001", Name: "coderhelm.com." },
  });
  assert.deepEqual(refusals(all, cfg.prefix, "654654210434", "us-east-1"), []);
});

test("the check itself refuses a table the role doesn't grant", () => {
  const cfg = resolveConfig({
    account: "111122223333",
    region: "us-west-2",
    stage: "prod",
    email: { from: "coderhelm@example.org" },
    admins: ["owner@example.org"],
  });
  const docs = streamingPolicies(synth(cfg), cfg.prefix, "111122223333", "us-west-2");
  const at = (t: string) => tableArn(cfg.prefix, "111122223333", "us-west-2", t);
  // Granted: the member check, and Chat's own reads.
  assert.ok(allowed(docs, null, { action: "dynamodb:GetItem", resource: at("users") }));
  assert.ok(allowed(docs, null, { action: "dynamodb:Query", resource: at("runs") }));
  // Never granted to the stream: writing members, or the email tables.
  assert.ok(!allowed(docs, null, { action: "dynamodb:PutItem", resource: at("users") }));
  assert.ok(!allowed(docs, null, { action: "dynamodb:GetItem", resource: at("waitlist") }));
});
