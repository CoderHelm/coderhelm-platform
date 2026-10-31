// The uninstall (installer/lib/uninstall.mjs) against an AWS account held in
// memory: this install (coderhelm-prod), another install whose name starts
// with it (coderhelm-prod-eu), the uninstaller's own pieces, and things that
// aren't CoderHelm's at all. Every delete is checked to be this install's.
import { test } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import * as un from "../lib/uninstall.mjs";
import { main } from "../lib/cli.mjs";
import { FakeRunner, quietAsk } from "./fake.mjs";
import { signRequest } from "../lib/hybrid.mjs";

const A = "111122223333";
const R = "us-east-1";
const ID = "1a2b3c4d";
const OLD = "9f8e7d6c";
const arn = (svc, res, region = R) => `arn:aws:${svc}:${region}:${A}:${res}`;
const tag = (v) => [{ Key: un.INSTALL_TAG, Value: v }];
const HYBRID = { hybrid_install_id: "ins_abcdefghijklmnopqrstuvwx", hybrid_install_secret: "s3cret" };

/** An account with this install, a sibling install, the uninstaller and someone else's things. */
function world({ hybrid = false } = {}) {
  const w = {
    calls: [],
    stacks: {
      "coderhelm-prod": { id: `arn:aws:cloudformation:${R}:${A}:stack/coderhelm-prod/${ID}-0000-1111-2222-333344445555`, params: { Stage: "prod", SetupToken: "", AdminEmail: "a@example.org", StatusId: "abcdefghijklmnop" }, tags: [] },
      "coderhelm-prod-database": { tags: tag("coderhelm-prod"), role: `arn:aws:iam::${A}:role/coderhelm-prod-deploy-${ID}`, exports: ["coderhelm-prod-TableArn"] },
      "coderhelm-prod-api": { tags: tag("coderhelm-prod"), role: `arn:aws:iam::${A}:role/coderhelm-prod-deploy-${ID}`, exports: [] },
      "coderhelm-prod-eu-api": { tags: tag("coderhelm-prod-eu"), role: `arn:aws:iam::${A}:role/coderhelm-prod-eu-deploy-77777777`, exports: [] },
      "coderhelm-prod-uninstall": { tags: [], params: { Stage: "prod", ConfirmStage: "prod" } },
      "my-app": { tags: [] },
      CDKToolkit: { tags: [] },
    },
    imports: { "coderhelm-prod-TableArn": ["coderhelm-prod-api"] },
    buckets: {
      [`coderhelm-prod-data-${A}`]: { region: R, objects: 3 },
      [`coderhelm-prod-assets-${ID}-${A}`]: { region: R, objects: 2 },
      [`coderhelm-prod-assets-${OLD}-${A}`]: { region: R, objects: 0 },
      [`coderhelm-prod-assets-22223333-${A}`]: { region: "eu-west-1", objects: 0 },
      [`coderhelm-prod-eu-data-${A}`]: { region: R, objects: 5 },
      "coderhelm-setup-abcdefghijklmnop": { region: R, objects: 1 },
      "coderhelm-setup-uninstallprogres": { region: R, objects: 1 },
      [`cdk-hnb659fds-assets-${A}-${R}`]: { region: R, objects: 9 },
      "my-app-bucket": { region: R, objects: 9 },
    },
    tables: { "coderhelm-prod": tag("coderhelm-prod"), "coderhelm-prod-runs": tag("coderhelm-prod"), "coderhelm-prod-eu-runs": tag("coderhelm-prod-eu"), "coderhelm-prod-imposter": tag("someone-else"), "my-app-table": [] },
    backups: [{ TableName: "coderhelm-prod-runs", BackupName: "runs-1", BackupArn: arn("dynamodb", "table/coderhelm-prod-runs/backup/01"), BackupStatus: "AVAILABLE" },
      { TableName: "coderhelm-prod-eu-runs", BackupName: "eu-1", BackupArn: arn("dynamodb", "table/coderhelm-prod-eu-runs/backup/02"), BackupStatus: "AVAILABLE" }],
    keys: { k1: { tag: "coderhelm-prod", state: "Enabled" }, k2: { tag: "coderhelm-prod-eu", state: "Enabled" }, k3: { tag: null, state: "Enabled" } },
    aliases: [{ AliasName: "alias/coderhelm-prod-dynamo", TargetKeyId: "k1" }, { AliasName: "alias/coderhelm-prod-eu-dynamo", TargetKeyId: "k2" }, { AliasName: "alias/my-key", TargetKeyId: "k3" }],
    pools: { "us-east-1_prod": { Name: "coderhelm-prod-users", tag: "coderhelm-prod", Domain: "coderhelm-prod-auth", DeletionProtection: "ACTIVE" }, "us-east-1_eu": { Name: "coderhelm-prod-eu-users", tag: "coderhelm-prod-eu" } },
    secrets: ["coderhelm/prod/secrets", "coderhelm/prod-eu/secrets", "my-app/secret"],
    logGroups: ["/aws/lambda/coderhelm-prod-gateway", "/aws/codebuild/coderhelm-prod-installer", "/aws/lambda/coderhelm-prod-eu-gateway", "/aws/codebuild/coderhelm-prod-uninstall-5e6f7a8b", "/aws/lambda/my-app"],
    params: ["/coderhelm-prod/public-url", "/coderhelm-prod/launch-1a2b3c4d/window-policy/1", "/coderhelm-prod/uninstall-5e6f7a8b/window-policy/1"],
    roles: {
      [`coderhelm-prod-deploy-${ID}`]: { boundary: `arn:aws:iam::${A}:policy/coderhelm-prod-boundary-${ID}`, policies: ["deploy"] },
      [`coderhelm-prod-installer-${ID}`]: { boundary: `arn:aws:iam::${A}:policy/coderhelm-prod-boundary-${ID}`, policies: ["idle"], launch: true },
      [`coderhelm-prod-deploy-${OLD}`]: { boundary: `arn:aws:iam::${A}:policy/coderhelm-prod-boundary-${OLD}`, policies: ["deploy"] },
      "coderhelm-prod-leftoverprovider-abcd1234": { boundary: `arn:aws:iam::${A}:policy/coderhelm-prod-boundary-${ID}`, tags: tag("coderhelm-prod"), policies: ["x"] },
      "coderhelm-prod-deploy-22223333": { boundary: `arn:aws:iam::${A}:policy/coderhelm-prod-boundary-22223333`, policies: ["deploy"] },
      "coderhelm-prod-eu-deploy-77777777": { boundary: `arn:aws:iam::${A}:policy/coderhelm-prod-eu-boundary-77777777`, policies: ["deploy"] },
      "coderhelm-prod-uninstall-build-5e6f7a8b": { boundary: `arn:aws:iam::${A}:policy/coderhelm-prod-uninstall-boundary-5e6f7a8b`, policies: ["idle"] },
      "coderhelm-prod-someones": { boundary: null, policies: [] },
      Admin: { boundary: null, policies: ["all"] },
    },
    policies: [`coderhelm-prod-boundary-${ID}`, `coderhelm-prod-boundary-${OLD}`, "coderhelm-prod-boundary-22223333", "coderhelm-prod-eu-boundary-77777777", "coderhelm-prod-uninstall-boundary-5e6f7a8b"],
    certs: [`arn:aws:acm:us-east-1:${A}:certificate/updater-cert`],
    secretValue: hybrid ? HYBRID : { github_app_id: "1" },
  };
  w.read = async (svc, op, args = [], region) => {
    w.calls.push({ kind: "read", svc, op, args, region });
    const arg = (k) => args[args.indexOf(k) + 1];
    switch (`${svc} ${op}`) {
      case "sts get-caller-identity": return { Account: A };
      case "resourcegroupstaggingapi get-tag-values": return { TagValues: ["coderhelm-prod", "coderhelm-prod-eu", "someone-else"] };
      case "resourcegroupstaggingapi get-resources": {
        const want = arg("--tag-filters");
        if (want.startsWith("Key=coderhelm,")) return { ResourceTagMappingList: w.certs.map((c) => ({ ResourceARN: c })) };
        const v = want.split("Values=")[1];
        const out = [];
        for (const [n, t] of Object.entries(w.tables)) if (t[0]?.Value === v) out.push(arn("dynamodb", `table/${n}`));
        for (const [k, x] of Object.entries(w.keys)) if (x.tag === v) out.push(arn("kms", `key/${k}`));
        for (const [id, p] of Object.entries(w.pools)) if (p.tag === v) out.push(arn("cognito-idp", `userpool/${id}`));
        return { ResourceTagMappingList: out.map((a) => ({ ResourceARN: a })) };
      }
      case "secretsmanager list-secrets": return { SecretList: w.secrets.filter((s) => s.startsWith(args[1].split("Values=")[1])).map((Name) => ({ Name })) };
      case "secretsmanager get-secret-value": return w.secrets.includes(arg("--secret-id")) ? JSON.stringify(w.secretValue) : null;
      case "cloudformation list-stacks": return { StackSummaries: Object.keys(w.stacks).map((n) => ({ StackName: n, StackId: w.stacks[n].id ?? n })) };
      case "cloudformation describe-stacks": {
        const key = arg("--stack-name");
        const n = Object.keys(w.stacks).find((x) => x === key || w.stacks[x].id === key);
        if (!n) return null;
        const s = w.stacks[n];
        return { Stacks: [{ StackName: n, StackId: s.id ?? n, StackStatus: s.status ?? "CREATE_COMPLETE", RoleARN: s.role, Tags: s.tags, Parameters: Object.entries(s.params ?? {}).map(([ParameterKey, ParameterValue]) => ({ ParameterKey, ParameterValue })), Outputs: (s.exports ?? []).map((ExportName) => ({ ExportName })) }] };
      }
      case "cloudformation list-imports": return { Imports: (w.imports[arg("--export-name")] ?? []).filter((n) => w.stacks[n]) };
      case "s3api list-buckets": return { Buckets: Object.keys(w.buckets).map((Name) => ({ Name })) };
      case "s3api get-bucket-location": return { LocationConstraint: w.buckets[arg("--bucket")]?.region === "us-east-1" ? null : w.buckets[arg("--bucket")]?.region };
      case "s3api list-object-versions": return { Versions: Array.from({ length: w.buckets[arg("--bucket")]?.objects ?? 0 }, (_, i) => ({ Key: `k${i}`, VersionId: `v${i}` })) };
      case "dynamodb list-tables": return { TableNames: Object.keys(w.tables) };
      case "dynamodb list-tags-of-resource": return { Tags: w.tables[arg("--resource-arn").split("/").pop()] ?? [] };
      case "dynamodb list-backups": return { BackupSummaries: w.backups };
      case "kms list-aliases": return { Aliases: w.aliases };
      case "kms describe-key": return w.keys[arg("--key-id")] ? { KeyMetadata: { Arn: arn("kms", `key/${arg("--key-id")}`), KeyManager: "CUSTOMER", KeyState: w.keys[arg("--key-id")].state } } : null;
      case "kms list-resource-tags": return { Tags: w.keys[arg("--key-id")]?.tag ? [{ TagKey: un.INSTALL_TAG, TagValue: w.keys[arg("--key-id")].tag }].map((t) => ({ Key: t.TagKey, Value: t.TagValue })) : [] };
      case "cognito-idp list-user-pools": return { UserPools: Object.entries(w.pools).map(([Id, p]) => ({ Id, Name: p.Name })) };
      case "cognito-idp describe-user-pool": {
        const p = w.pools[arg("--user-pool-id")];
        return p ? { UserPool: { Name: p.Name, Domain: p.Domain, DeletionProtection: p.DeletionProtection, UserPoolTags: { [un.INSTALL_TAG]: p.tag } } } : null;
      }
      case "logs describe-log-groups": return { logGroups: w.logGroups.filter((g) => g.startsWith(arg("--log-group-name-prefix"))).map((logGroupName) => ({ logGroupName })) };
      case "ssm get-parameters-by-path": return { Parameters: w.params.filter((p) => p.startsWith(`${arg("--path")}/`)).map((Name) => ({ Name })) };
      case "iam list-policies": return { Policies: w.policies.map((PolicyName) => ({ PolicyName, Arn: `arn:aws:iam::${A}:policy/${PolicyName}` })) };
      case "iam list-roles": return { Roles: Object.keys(w.roles).map((RoleName) => ({ RoleName })) };
      case "iam get-role": {
        const r = w.roles[arg("--role-name")];
        return r ? { Role: { RoleName: arg("--role-name"), Tags: r.tags ?? [], ...(r.boundary ? { PermissionsBoundary: { PermissionsBoundaryArn: r.boundary } } : {}) } } : null;
      }
      case "iam list-role-policies": return { PolicyNames: w.roles[arg("--role-name")]?.policies ?? [] };
      case "iam list-attached-role-policies": return { AttachedPolicies: [] };
      case "iam list-policy-versions": return { Versions: [{ VersionId: "v1", IsDefaultVersion: true }] };
      default: return null;
    }
  };
  w.write = async (svc, op, args = [], region) => {
    w.calls.push({ kind: "write", svc, op, args, region });
    const arg = (k) => args[args.indexOf(k) + 1];
    const ok = { ok: true, error: "", out: "" };
    switch (`${svc} ${op}`) {
      case "cloudformation delete-stack": {
        const n = arg("--stack-name");
        const s = w.stacks[n];
        if (s?.role && arg("--role-arn") !== s.role) return { ok: false, error: "wrong role" };
        if ((s?.exports ?? []).some((e) => (w.imports[e] ?? []).some((i) => w.stacks[i]))) return { ok: false, error: `${n} is still imported` };
        delete w.stacks[n];
        return ok;
      }
      case "s3api delete-objects": w.buckets[arg("--bucket")].objects = 0; return ok;
      case "s3api delete-bucket": if (w.buckets[arg("--bucket")].objects) return { ok: false, error: "BucketNotEmpty" }; delete w.buckets[arg("--bucket")]; return ok;
      case "dynamodb delete-table": delete w.tables[arg("--table-name")]; return ok;
      case "dynamodb delete-backup": w.backups = w.backups.filter((b) => b.BackupArn !== arg("--backup-arn")); return ok;
      case "kms delete-alias": w.aliases = w.aliases.filter((a) => a.AliasName !== arg("--alias-name")); return ok;
      case "kms schedule-key-deletion": w.keys[arg("--key-id")].state = "PendingDeletion"; return { ...ok, out: JSON.stringify({ DeletionDate: "2026-11-06T00:00:00Z" }) };
      case "cognito-idp delete-user-pool": delete w.pools[arg("--user-pool-id")]; return ok;
      case "secretsmanager delete-secret": w.secrets = w.secrets.filter((s) => s !== arg("--secret-id")); return ok;
      case "logs delete-log-group": w.logGroups = w.logGroups.filter((g) => g !== arg("--log-group-name")); return ok;
      case "ssm delete-parameters": { const names = args.slice(args.indexOf("--names") + 1).filter((x) => !x.startsWith("--")); w.params = w.params.filter((p) => !names.includes(p)); return ok; }
      case "acm delete-certificate": w.certs = w.certs.filter((c) => c !== arg("--certificate-arn")); return ok;
      case "iam delete-role-policy": w.roles[arg("--role-name")].policies = w.roles[arg("--role-name")].policies.filter((p) => p !== arg("--policy-name")); return ok;
      case "iam delete-role": if (w.roles[arg("--role-name")].policies.length) return { ok: false, error: "DeleteConflict" }; delete w.roles[arg("--role-name")]; return ok;
      case "iam delete-policy": w.policies = w.policies.filter((p) => !arg("--policy-arn").endsWith(`/${p}`)); return ok;
      default: return ok;
    }
  };
  // The Launch Stack takes its own roles with it.
  const del = w.write;
  w.write = async (svc, op, args, region) => {
    const r = await del(svc, op, args, region);
    if (svc === "cloudformation" && op === "delete-stack" && args[1] === "coderhelm-prod") {
      for (const [n, x] of Object.entries(w.roles)) if (x.launch) delete w.roles[n];
      w.params = w.params.filter((p) => !p.includes("/launch-"));
    }
    return r;
  };
  w.dryRun = false;
  return w;
}

const writes = (w) => w.calls.filter((c) => c.kind === "write");
/** The resource a write names (its name, ARN or id). */
const target = (c) => {
  for (const k of ["--stack-name", "--bucket", "--table-name", "--backup-arn", "--alias-name", "--key-id", "--user-pool-id", "--secret-id", "--log-group-name", "--certificate-arn", "--role-name", "--policy-arn"]) {
    const i = c.args.indexOf(k);
    if (i >= 0) return c.args[i + 1];
  }
  if (c.op === "delete-parameters") return c.args.slice(c.args.indexOf("--names") + 1).join(" ");
  return "";
};

function isThisInstalls(t0) {
  const t = t0.split(A).join("<account>");
  if (/coderhelm-prod-eu|prod-eu|my-app|cdk-|CDKToolkit|Admin|uninstall|someone|imposter|22223333|my-key|k2|k3|us-east-1_eu|uninstallprogres/.test(t)) return false;
  return /coderhelm-prod|coderhelm\/prod\/|coderhelm-setup-abcdefghijklmnop|k1|us-east-1_prod|updater-cert/.test(t) || false;
}

const ctxFor = (w, extra = {}) => ({ aws: w, log: () => {}, sleep: async () => {}, runner: new FakeRunner(), fetchImpl: async () => ({ ok: false }), ...extra });

test("an install's names: its own, never a longer install's, never the uninstaller's", () => {
  const s = un.scopeOf({ stage: "prod", region: R, account: A, siblings: ["coderhelm-prod-eu"] });
  assert.ok(un.isOurName(s, "coderhelm-prod"));
  assert.ok(un.isOurName(s, "coderhelm-prod-runs"));
  assert.ok(!un.isOurName(s, "coderhelm-prod-eu"));
  assert.ok(!un.isOurName(s, "coderhelm-prod-eu-runs"));
  assert.ok(!un.isOurName(s, "coderhelm-prod-uninstall-build-5e6f7a8b"));
  assert.ok(!un.isOurName(s, "coderhelm-production"));
  assert.ok(!un.isOurName(s, "coderhelm-pro"));
  assert.ok(un.isLaunchName(s, "coderhelm-prod") && un.isLaunchName(s, "coderhelm-prod-2") && !un.isLaunchName(s, "coderhelm-prod-api"));
  assert.deepEqual(un.siblingsOf("coderhelm-prod", ["coderhelm-prod", "coderhelm-prod-eu", "coderhelm-production", "someone-else"]), ["coderhelm-prod-eu"]);
  assert.throws(() => un.scopeOf({ stage: "Prod", region: R, account: A }), /install name/);
  assert.throws(() => un.scopeOf({ stage: "prod", region: R, account: "x" }), /account/);
});

test("finds exactly this install's pieces in this region, and names what it leaves alone", async () => {
  const w = world();
  const ctx = ctxFor(w, { guided: true });
  const inv = await un.discover(ctx, un.scopeOf({ stage: "prod", region: R, account: A }), { strictTags: true, ownStatus: "uninstallprogres" });
  assert.deepEqual(inv.scope.siblings, ["coderhelm-prod-eu"]);
  assert.deepEqual(inv.launch.map((l) => l.name), ["coderhelm-prod"]);
  assert.deepEqual(inv.ids, [ID, OLD], "the Launch Stack's id and a kept assets bucket's id in this region; not eu-west-1's");
  assert.deepEqual(inv.stacks.map((s) => s.name).sort(), ["coderhelm-prod-api", "coderhelm-prod-database"]);
  assert.deepEqual(inv.tables.map((t) => t.name).sort(), ["coderhelm-prod", "coderhelm-prod-runs"]);
  assert.deepEqual(inv.backups.map((b) => b.name), ["coderhelm-prod-runs/runs-1"]);
  assert.deepEqual(inv.buckets.map((b) => b.name).sort(), [`coderhelm-prod-assets-${ID}-${A}`, `coderhelm-prod-assets-${OLD}-${A}`, `coderhelm-prod-data-${A}`, "coderhelm-setup-abcdefghijklmnop"].sort());
  assert.deepEqual(inv.keys.map((k) => k.id), ["k1"]);
  assert.deepEqual(inv.aliases.map((a) => a.name), ["alias/coderhelm-prod-dynamo"]);
  assert.deepEqual(inv.pools.map((p) => p.id), ["us-east-1_prod"]);
  assert.deepEqual(inv.secrets, ["coderhelm/prod/secrets"]);
  assert.deepEqual(inv.logGroups.sort(), ["/aws/codebuild/coderhelm-prod-installer", "/aws/lambda/coderhelm-prod-gateway"]);
  assert.deepEqual(inv.params, ["/coderhelm-prod/public-url", "/coderhelm-prod/launch-1a2b3c4d/window-policy/1"]);
  assert.deepEqual(inv.boundaries.map((b) => b.name).sort(), [`coderhelm-prod-boundary-${ID}`, `coderhelm-prod-boundary-${OLD}`]);
  assert.deepEqual(inv.roles.map((r) => r.name).sort(), [`coderhelm-prod-deploy-${ID}`, `coderhelm-prod-deploy-${OLD}`, `coderhelm-prod-installer-${ID}`, "coderhelm-prod-leftoverprovider-abcd1234"].sort());
  assert.ok(inv.skipped.some((x) => x.name === "coderhelm-prod-imposter" && /someone-else/.test(x.why)));
  assert.ok(inv.skipped.some((x) => x.name === "coderhelm-prod-someones"), "an unbounded, untagged role of the name is not touched");
  assert.ok(inv.skipped.some((x) => x.name === "coderhelm-prod-deploy-22223333"), "another region's Launch Stack pieces are not touched");
  assert.equal(writes(w).length, 0, "finding is read-only");
});

test("keep my data: removes CoderHelm, keeps tables, data bucket, keys, user pool and secret; touches nothing else", async () => {
  const w = world();
  const ctx = ctxFor(w, { guided: true });
  const doc = await un.runUninstall(ctx, { stage: "prod", region: R, account: A, mode: "keep", ownStatus: "uninstallprogres" });
  assert.equal(doc.phase, "done", JSON.stringify(doc.problems.concat(doc.left)));
  for (const c of writes(w)) assert.ok(isThisInstalls(target(c)), `${c.svc} ${c.op} ${target(c)}`);
  assert.ok(!writes(w).some((c) => /delete-table|delete-backup|schedule-key-deletion|delete-user-pool|delete-secret|delete-alias/.test(c.op)));
  assert.ok(w.buckets[`coderhelm-prod-data-${A}`] && w.tables["coderhelm-prod-runs"] && w.keys.k1.state === "Enabled" && w.pools["us-east-1_prod"] && w.secrets.includes("coderhelm/prod/secrets"));
  assert.ok(!w.stacks["coderhelm-prod-api"] && !w.stacks["coderhelm-prod"] && !w.roles[`coderhelm-prod-deploy-${ID}`] && !w.policies.includes(`coderhelm-prod-boundary-${ID}`));
  assert.ok(doc.kept.some((k) => k.kind === "table") && doc.kept.some((k) => k.kind === "data bucket"));
  // The rest of the account is as it was.
  assert.ok(w.stacks["coderhelm-prod-eu-api"] && w.stacks["coderhelm-prod-uninstall"] && w.stacks.CDKToolkit && w.buckets["coderhelm-setup-uninstallprogres"]);
  assert.ok(w.roles["coderhelm-prod-uninstall-build-5e6f7a8b"] && w.roles.Admin && w.roles["coderhelm-prod-someones"]);
  assert.ok(w.logGroups.includes("/aws/codebuild/coderhelm-prod-uninstall-5e6f7a8b") && w.params.includes("/coderhelm-prod/uninstall-5e6f7a8b/window-policy/1"));
});

test("delete everything: the data goes too, KMS keys on AWS's 7-day minimum, and the check finds nothing left", async () => {
  const w = world();
  const ctx = ctxFor(w, { guided: true });
  const docs = [];
  const doc = await un.runUninstall(ctx, { stage: "prod", region: R, account: A, mode: "everything", ownStatus: "uninstallprogres", write: async (d) => docs.push(d) });
  assert.equal(doc.phase, "done", JSON.stringify(doc.problems.concat(doc.left)));
  for (const c of writes(w)) assert.ok(isThisInstalls(target(c)), `${c.svc} ${c.op} ${target(c)}`);
  const sched = writes(w).find((c) => c.op === "schedule-key-deletion");
  assert.deepEqual(sched.args.slice(sched.args.indexOf("--pending-window-in-days"), sched.args.indexOf("--pending-window-in-days") + 2), ["--pending-window-in-days", "7"]);
  assert.deepEqual(doc.scheduled, [{ kind: "KMS key", name: "k1", date: "2026-11-06T00:00:00Z" }]);
  assert.deepEqual(doc.left, []);
  assert.ok(!w.buckets[`coderhelm-prod-data-${A}`] && !w.tables["coderhelm-prod-runs"] && !w.pools["us-east-1_prod"] && !w.secrets.includes("coderhelm/prod/secrets"));
  assert.ok(w.buckets[`coderhelm-prod-eu-data-${A}`] && w.tables["coderhelm-prod-eu-runs"] && w.keys.k2.state === "Enabled" && w.backups.some((b) => b.TableName === "coderhelm-prod-eu-runs"));
  // A protected user pool is unprotected, its domain removed, then deleted.
  const i = (op) => w.calls.findIndex((c) => c.op === op);
  assert.ok(i("update-user-pool") < i("delete-user-pool-domain") && i("delete-user-pool-domain") < i("delete-user-pool"));
  // Data only after every stack is gone; the boundary after its roles; the Launch Stack before its kept pieces.
  const last = (op, name) => w.calls.map((c, j) => [c, j]).filter(([c]) => c.op === op && (!name || target(c) === name)).pop()?.[1] ?? -1;
  assert.ok(last("delete-stack", "coderhelm-prod-api") < i("delete-table"));
  assert.ok(last("delete-role") < last("delete-policy"));
  // The progress file: phases in order, no account numbers.
  const phases = [...new Set(docs.map((d) => d.phase))];
  assert.deepEqual(phases, ["preparing", "stacks", "data", "launch", "cleanup", "verifying", "done"]);
  assert.ok(!JSON.stringify(docs).includes(A), "no account number in the public file");
});

test("stacks go dependents first, through the install's own deploy role", async () => {
  const w = world();
  const ctx = ctxFor(w, { guided: true });
  await un.runUninstall(ctx, { stage: "prod", region: R, account: A, mode: "keep" });
  const order = writes(w).filter((c) => c.op === "delete-stack").map(target);
  assert.ok(order.indexOf("coderhelm-prod-api") < order.indexOf("coderhelm-prod-database"), order.join(","));
  for (const c of writes(w).filter((x) => x.op === "delete-stack" && target(x) !== "coderhelm-prod")) {
    assert.equal(c.args[c.args.indexOf("--role-arn") + 1], `arn:aws:iam::${A}:role/coderhelm-prod-deploy-${ID}`);
  }
  const launch = writes(w).find((c) => c.op === "delete-stack" && target(c) === "coderhelm-prod");
  assert.ok(!launch.args.includes("--role-arn"), "the Launch Stack goes with the uninstaller's own (windowed) credentials");
});

test("guided: a stack not deployed through the install's deploy role is refused and listed, and the data stays", async () => {
  const w = world();
  w.stacks["coderhelm-prod-api"].role = undefined;
  const ctx = ctxFor(w, { guided: true });
  const doc = await un.runUninstall(ctx, { stage: "prod", region: R, account: A, mode: "everything" });
  assert.equal(doc.phase, "failed");
  assert.ok(doc.problems.some((p) => p.name === "coderhelm-prod-api" && /deploy role/.test(p.error)));
  assert.ok(w.tables["coderhelm-prod-runs"], "nothing of the data goes while a stack that uses it stays");
});

test("hybrid: the name, links and broker key are released first; links are counted for the warning", async () => {
  const w = world({ hybrid: true });
  const seen = [];
  const fetchImpl = async (url, opts = {}) => {
    seen.push({ url, method: opts.method ?? "GET", deletes: writes(w).length, headers: opts.headers });
    if (url.endsWith("/hybrid/status")) return { ok: true, json: async () => ({ name: "acme", github: { links: [{ installation_id: 1 }, { installation_id: 2 }] }, jira: { link: { site: "acme.atlassian.net" } } }) };
    return { ok: true, json: async () => ({ ok: true }) };
  };
  const ctx = ctxFor(w, { guided: true, fetchImpl });
  const doc = await un.runUninstall(ctx, { stage: "prod", region: R, account: A, mode: "keep" });
  const del = seen.find((s) => s.method === "DELETE");
  assert.ok(del && del.url.endsWith("/hybrid/installs"));
  assert.equal(del.deletes, 0, "released before anything is deleted");
  const ts = del.headers["x-coderhelm-timestamp"];
  assert.equal(del.headers["x-coderhelm-signature"], signRequest("s3cret", Number(ts), "DELETE", "/hybrid/installs"));
  assert.deepEqual(doc.hybrid, { registered: true, released: true, github: 2, jira: true });
});

test("the progress file: known phases only, lists capped, no ARNs or account numbers", () => {
  const d = un.statusDoc({ phase: "done", message: `removed arn:aws:iam::${A}:role/x and coderhelm-prod-data-${A} for a@example.org`, removed: [{ kind: "bucket", name: `coderhelm-prod-data-${A}` }] });
  assert.ok(!JSON.stringify(d).includes(A) && !JSON.stringify(d).includes("a@example.org"));
  assert.throws(() => un.statusDoc({ phase: "deleting-everything" }), /unknown phase/);
});

// ── The terminal ────────────────────────────────────────────────────────

function cfgDir() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "chun-"));
  const file = path.join(dir, "coderhelm.config.json");
  fs.writeFileSync(file, JSON.stringify({ account: A, region: R, stage: "prod", email: { from: "a@example.org" }, admins: ["a@example.org"] }));
  return file;
}

async function cli(argv, deps) {
  const out = [];
  let code;
  try {
    code = await main(argv, { log: (m) => out.push(String(m)), interactive: false, ask: quietAsk(), sleep: async () => {}, fetchImpl: async () => ({ ok: false }), ...deps });
  } catch (e) {
    return { code: 1, out: out.join("\n"), error: e };
  }
  return { code, out: out.join("\n") };
}

test("terminal: lists what goes, asks for the install name, and refuses a wrong one", async () => {
  const file = cfgDir();
  const w = world();
  const noYes = await cli(["uninstall", "--config", file], { aws: w });
  assert.match(String(noYes.error), /--yes/);
  assert.match(noYes.out, /This removes CoderHelm prod/);
  assert.match(noYes.out, /Kept \(your data/);
  assert.match(noYes.out, /Not touched: the other installs coderhelm-prod-eu/);
  assert.equal(writes(w).length, 0);
  const wrong = await cli(["uninstall", "--config", file], { aws: w, interactive: true, ask: quietAsk({ "Type the install name": "prod-eu" }) });
  assert.match(String(wrong.error), /didn't match/);
  assert.equal(writes(w).length, 0, "a wrong name deletes nothing");
});

test("terminal: --everything with the name typed removes it all; --dry-run and --verify change nothing", async () => {
  const file = cfgDir();
  const dry = world();
  const r0 = await cli(["uninstall", "--everything", "--config", file], { aws: dry, runner: new FakeRunner({}, { dryRun: true }) });
  assert.equal(r0.code, 0, r0.out + r0.error);
  assert.match(r0.out, /Dry run: nothing was deleted/);
  assert.match(r0.out, /7 days/);
  assert.equal(writes(dry).length, 0);

  const w = world();
  const r1 = await cli(["uninstall", "--delete-data", "--config", file], { aws: w, interactive: true, ask: quietAsk({ "Type the install name (prod)": "prod" }) });
  assert.equal(r1.code, 0, r1.out + String(r1.error?.message));
  assert.match(r1.out, /CoderHelm and its data are removed/);
  assert.match(r1.out, /KMS key k1: scheduled for deletion on 2026-11-06/);
  for (const c of writes(w)) assert.ok(isThisInstalls(target(c)), `${c.svc} ${c.op} ${target(c)}`);

  const v = await cli(["uninstall", "--verify", "--stage", "prod", "--region", R], { aws: w });
  assert.equal(v.code, 0, v.out);
  assert.match(v.out, /nothing left/);
  assert.match(v.out, /scheduled for deletion/);
  const before = writes(w).length;
  const v2 = await cli(["uninstall", "--verify", "--stage", "prod", "--region", R], { aws: world() });
  assert.equal(v2.code, 1);
  assert.match(v2.out, /pieces left/);
  assert.equal(writes(w).length, before);
});
