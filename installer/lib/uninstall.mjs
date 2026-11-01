// Finds, and on request removes, what is left of one CoderHelm install in
// its AWS account, from a terminal with the person's own credentials:
// `coderhelm-selfhost leftovers [--delete]` after Delete stack (a native
// install), and `uninstall [--everything]` for an install from before
// native releases. It finds the install's pieces the same way and touch only what is
// the install's (docs/self-host/uninstall.md):
//   - names: coderhelm-<stage>, coderhelm-<stage>-..., coderhelm/<stage>/...,
//     /coderhelm-<stage>/..., never those of another install whose name
//     starts with this one's (coderhelm-prod-eu inside coderhelm-prod-*),
//     never the uninstaller's own (coderhelm-<stage>-uninstall-*);
//   - the install's tag (coderhelm:install = coderhelm-<stage>) wherever a
//     resource has one; a resource of the install's name tagged for another
//     install is never touched;
//   - this region only (IAM pieces by their Launch Stack id, found here).
//
// Order: hybrid first (release the name, hand GitHub and Jira back), then
// a native install's own stack (coderhelm-<stage>: its custom resources
// empty the buckets and take the data when KeepDataOnDelete=false, and it
// deletes its parts in order), or an older install's stacks through its
// deploy role (dependents first), then the data (delete everything only),
// then the Launch Stack, then what is left by name, then a read-only check
// of anything still there. Running it again picks up from any partial state.
import { leaveHybrid, registration, DEFAULT_HOSTED_URL, signRequest } from "./hybrid.mjs";
import { Runner } from "./runner.mjs";

export const INSTALL_TAG = "coderhelm:install";
/** The tag the updater puts on a certificate it requests (lambda/updater). */
export const UPDATER_CERT_TAG = "coderhelm";
export const STAGE_RE = /^[a-z][a-z0-9-]{0,19}$/;
export const MODES = ["keep", "everything"];
/** What "keep my data" keeps. */
export const DATA_KINDS = ["table", "backup", "data bucket", "KMS key", "KMS alias", "user pool", "secret"];
/** AWS's shortest wait before a KMS key is deleted. */
export const KEY_DELETION_DAYS = 7;

const ALIVE = ["CREATE_COMPLETE", "CREATE_FAILED", "CREATE_IN_PROGRESS", "ROLLBACK_COMPLETE", "ROLLBACK_FAILED", "ROLLBACK_IN_PROGRESS", "UPDATE_COMPLETE",
  "UPDATE_FAILED", "UPDATE_IN_PROGRESS", "UPDATE_ROLLBACK_COMPLETE", "UPDATE_ROLLBACK_FAILED", "UPDATE_ROLLBACK_IN_PROGRESS", "UPDATE_COMPLETE_CLEANUP_IN_PROGRESS",
  "UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS", "DELETE_FAILED", "DELETE_IN_PROGRESS", "IMPORT_COMPLETE", "IMPORT_ROLLBACK_COMPLETE", "REVIEW_IN_PROGRESS"];

// ── Scope: what is this install's ───────────────────────────────────────

export function partitionOf(region) {
  return /^us-gov-/.test(region) ? "aws-us-gov" : /^cn-/.test(region) ? "aws-cn" : "aws";
}

export function scopeOf({ stage, region, account, siblings = [] }) {
  if (!STAGE_RE.test(String(stage ?? ""))) throw new Error(`"${stage}" is not an install name (lowercase letters, digits and hyphens).`);
  if (!/^\d{12}$/.test(String(account ?? ""))) throw new Error("The AWS account number could not be read.");
  if (!/^[a-z]{2}(-gov)?-[a-z]+-\d$/.test(String(region ?? ""))) throw new Error(`"${region}" is not an AWS region.`);
  return { stage, region, account: String(account), partition: partitionOf(region), prefix: `coderhelm-${stage}`, siblings: [...siblings] };
}

/** Another install whose name starts with this one's plus a hyphen. */
export function siblingsOf(prefix, names) {
  return [...new Set(names)].filter((x) => typeof x === "string" && x.startsWith(`${prefix}-`) && /^coderhelm-[a-z][a-z0-9-]{0,19}$/.test(x)).sort();
}

/** A name of this install: coderhelm-<stage> or coderhelm-<stage>-..., not another install's, not the uninstaller's. */
export function isOurName(scope, name) {
  const n = String(name ?? "");
  const P = scope.prefix;
  if (n !== P && !n.startsWith(`${P}-`)) return false;
  if (n.startsWith("coderhelm--")) return false;
  return !scope.siblings.some((s) => n === s || n.startsWith(`${s}-`));
}

/** A path name (log group, parameter) whose last segment set starts with one of ours. */
function ourAfter(scope, text, lead) {
  return text.startsWith(lead) && isOurName(scope, text.slice(lead.length).split(/[/:]/)[0]);
}

/** The install guide's Launch Stack names: coderhelm-<stage> and coderhelm-<stage>-<n>. */
export function isLaunchName(scope, name) {
  return name === scope.prefix || new RegExp(`^${scope.prefix.replace(/-/g, "\\-")}-\\d{1,3}$`).test(name);
}

const tagOf = (tags, key) => (tags ?? []).find((t) => (t.Key ?? t.key) === key)?.Value ?? (tags ?? []).find((t) => (t.Key ?? t.key) === key)?.value;

// ── AWS ─────────────────────────────────────────────────────────────────

/** Reads always run (a dry run still shows the plan); writes go through the runner, which prints them in a dry run. */
export function awsClient(ctx, region) {
  const reader = ctx.runner.dryRun ? new Runner({ log: () => {} }) : ctx.runner;
  const at = (args, r) => [...args, "--region", r ?? region, "--output", "json"];
  return {
    dryRun: !!ctx.runner.dryRun,
    async read(svc, op, args = [], r) {
      return reader.json("aws", [svc, op, ...at(args, r)], { allowFail: true });
    },
    async write(svc, op, args = [], r) {
      const res = await ctx.runner.run("aws", [svc, op, ...at(args, r)], { capture: true, allowFail: true });
      return { ok: res.code === 0, error: String(res.stderr ?? "").trim(), out: res.stdout };
    },
  };
}

// ── Finding the install ─────────────────────────────────────────────────

async function pages(aws, svc, op, args, key) {
  const r = await aws.read(svc, op, args);
  return r?.[key] ?? [];
}

/** Every piece of the install in this region, read-only. */
export async function discover(ctx, base, { mode = "keep", installStatus = "", ownStatus = "", strictTags = false, fetchImpl } = {}) {
  const aws = ctx.aws;
  const { prefix: P, account: A, region } = base;
  // Other installs whose names start with this one's: by their tag and secret here, their
  // Launch Stacks (Stage) here and their permissions boundaries anywhere (IAM is global).
  const tagValues = await pages(aws, "resourcegroupstaggingapi", "get-tag-values", ["--key", INSTALL_TAG], "TagValues");
  const secretNames = (await pages(aws, "secretsmanager", "list-secrets", ["--filters", "Key=name,Values=coderhelm/"], "SecretList")).map((s) => s.Name);
  const policies = await pages(aws, "iam", "list-policies", ["--scope", "Local"], "Policies");
  const summaries = await pages(aws, "cloudformation", "list-stacks", ["--stack-status-filter", ...ALIVE], "StackSummaries");
  const described = [];
  for (const s of summaries) {
    if (s.StackName !== P && !s.StackName.startsWith(`${P}-`)) continue;
    const d = (await aws.read("cloudformation", "describe-stacks", ["--stack-name", s.StackId ?? s.StackName]))?.Stacks?.[0];
    if (d) described.push({ d, params: Object.fromEntries((d.Parameters ?? []).map((p) => [p.ParameterKey, p.ParameterValue ?? ""])) });
  }
  const isLaunch = (params) => "SetupToken" in params && "AdminEmail" in params;
  const names = [
    ...tagValues,
    ...secretNames.map((n) => n.split("/")).filter((p) => p.length >= 3 && p[0] === "coderhelm").map((p) => `coderhelm-${p[1]}`),
    ...described.filter((x) => isLaunch(x.params) && x.params.Stage !== base.stage).map((x) => `coderhelm-${x.params.Stage}`),
    ...policies.map((p) => String(p.PolicyName).match(/^(coderhelm-[a-z][a-z0-9-]{0,19})-boundary-[0-9a-f]{8}$/)?.[1]).filter(Boolean),
  ];
  const scope = { ...base, siblings: siblingsOf(P, names) };
  const inv = { scope, mode, anchored: false, launch: [], ids: [], stacks: [], esms: [], tables: [], backups: [], buckets: [], keys: [], aliases: [], pools: [], secrets: [], logGroups: [], params: [], certs: [], roles: [], boundaries: [], skipped: [] };

  // Stacks: the Launch Stack(s), and the install's own (tagged; untagged only for a terminal uninstall of an older install).
  for (const { d, params } of described) {
    if (isLaunch(params)) {
      // A Launch Stack is this install's only by its Stage; another install's is never touched, whatever its name.
      if (params.Stage !== scope.stage) inv.skipped.push({ kind: "stack", name: d.StackName, why: `the Launch Stack of ${params.Stage}` });
      else if (!isLaunchName(scope, d.StackName)) inv.skipped.push({ kind: "stack", name: d.StackName, why: "a Launch Stack under a name the install guide doesn't use: delete it in CloudFormation" });
      else if (strictTags && d.RoleARN) inv.skipped.push({ kind: "stack", name: d.StackName, why: "a Launch Stack with a service role: delete it in CloudFormation" });
      else inv.launch.push({ name: d.StackName, id: String(d.StackId).split("/")[2].slice(0, 8), statusId: /^[a-z0-9]{16}$/.test(params.StatusId ?? "") ? params.StatusId : "" });
      continue;
    }
    if (!isOurName(scope, d.StackName)) continue;
    const tag = tagOf(d.Tags, INSTALL_TAG);
    if (tag === P || (tag === undefined && !strictTags)) {
      inv.stacks.push({ name: d.StackName, id: d.StackId, roleArn: d.RoleARN ?? null, status: d.StackStatus, tagged: tag === P, exports: (d.Outputs ?? []).map((o) => o.ExportName).filter(Boolean) });
    } else if (tag !== undefined) inv.skipped.push({ kind: "stack", name: d.StackName, why: `tagged for ${tag}` });
  }
  const ids = new Set(inv.launch.map((l) => l.id));

  // Buckets, by exact name.
  const buckets = (await aws.read("s3api", "list-buckets", []))?.Buckets ?? [];
  const assetRe = new RegExp(`^${P.replace(/-/g, "\\-")}-assets-([0-9a-f]{8})-${A}$`);
  for (const b of buckets) {
    const m = b.Name.match(assetRe);
    if (!m) continue;
    const loc = await aws.read("s3api", "get-bucket-location", ["--bucket", b.Name]);
    if ((loc?.LocationConstraint || "us-east-1") === region) ids.add(m[1]);
  }
  inv.ids = [...ids].sort();
  const bucketNames = new Set(buckets.map((b) => b.Name));
  // S3 names carry no region: the stacks' own buckets only when they are in this region.
  const want = [...inv.ids.map((id) => [`${P}-assets-${id}-${A}`, "bucket"])];
  for (const [name, kind] of [[`${P}-dashboard-${A}`, "bucket"], [`${P}-sandbox-${A}`, "bucket"], [`${P}-data-${A}`, "data bucket"]]) {
    if (!bucketNames.has(name)) continue;
    const loc = await aws.read("s3api", "get-bucket-location", ["--bucket", name]);
    if ((loc?.LocationConstraint || "us-east-1") === region) want.push([name, kind]);
    else inv.skipped.push({ kind, name, why: `in ${loc?.LocationConstraint || "another region"}, not ${region}` });
  }
  const setups = new Set(inv.launch.map((l) => l.statusId).filter(Boolean));
  if (/^[a-z0-9]{16}$/.test(installStatus) && (await progressIsOurs(fetchImpl, installStatus, scope))) setups.add(installStatus);
  setups.delete(ownStatus);
  for (const id of setups) want.push([`coderhelm-setup-${id}`, "progress bucket"]);
  for (const [name, kind] of want) if (bucketNames.has(name)) inv.buckets.push({ name, kind });

  // Everything tagged for the install in this region (and the updater's certificate in us-east-1).
  const tagged = await pages(aws, "resourcegroupstaggingapi", "get-resources", ["--tag-filters", `Key=${INSTALL_TAG},Values=${P}`], "ResourceTagMappingList");
  inv.tagged = tagged.map((t) => t.ResourceARN);
  const certs = await aws.read("resourcegroupstaggingapi", "get-resources", ["--tag-filters", `Key=${UPDATER_CERT_TAG},Values=${P}`, "--resource-type-filters", "acm:certificate"], "us-east-1");
  inv.certs = (certs?.ResourceTagMappingList ?? []).map((t) => t.ResourceARN);
  // Event source mappings of the install's functions (a failed delete can orphan them).
  for (const m of await pages(aws, "lambda", "list-event-source-mappings", [], "EventSourceMappings")) {
    const fn = String(m.FunctionArn ?? "").split(":function:")[1]?.split(":")[0] ?? "";
    if (isOurName(scope, fn)) inv.esms.push({ uuid: m.UUID, name: `${fn} <- ${String(m.EventSourceArn ?? "").split(":").pop()}` });
  }

  // Tables and their backups.
  for (const t of await pages(aws, "dynamodb", "list-tables", [], "TableNames")) {
    if (!isOurName(scope, t)) continue;
    const arn = `arn:${scope.partition}:dynamodb:${region}:${A}:table/${t}`;
    const tags = (await aws.read("dynamodb", "list-tags-of-resource", ["--resource-arn", arn]))?.Tags ?? [];
    const tag = tagOf(tags, INSTALL_TAG);
    if (tag === P || (tag === undefined && !strictTags)) inv.tables.push({ name: t, arn });
    else inv.skipped.push({ kind: "table", name: t, why: tag ? `tagged for ${tag}` : "not tagged for this install" });
  }
  for (const b of await pages(aws, "dynamodb", "list-backups", [], "BackupSummaries")) {
    if (isOurName(scope, b.TableName) && b.BackupStatus !== "DELETED") inv.backups.push({ name: `${b.TableName}/${b.BackupName}`, arn: b.BackupArn });
  }

  // KMS keys (tagged, or behind one of the install's aliases) and aliases.
  const aliases = (await pages(aws, "kms", "list-aliases", [], "Aliases")).filter((a) => isOurName(scope, String(a.AliasName).replace(/^alias\//, "")));
  inv.aliases = aliases.map((a) => ({ name: a.AliasName, key: a.TargetKeyId }));
  const keyIds = new Set(inv.tagged.filter((a) => a.includes(":key/")).map((a) => a.split("/").pop()));
  // An older install's keys carry no tag: behind its aliases, and only for an install from before hardening (no Launch Stack ids).
  if (!strictTags && !inv.ids.length) for (const a of aliases) if (a.TargetKeyId) keyIds.add(a.TargetKeyId);
  for (const id of keyIds) {
    const k = (await aws.read("kms", "describe-key", ["--key-id", id]))?.KeyMetadata;
    if (!k || k.KeyManager !== "CUSTOMER") continue;
    const tag = tagOf((await aws.read("kms", "list-resource-tags", ["--key-id", id]))?.Tags, INSTALL_TAG);
    if (tag !== undefined && tag !== P) continue;
    inv.keys.push({ id, arn: k.Arn, state: k.KeyState, deletionDate: k.DeletionDate ?? null });
  }

  // User pools.
  const poolIds = new Set(inv.tagged.filter((a) => a.includes(":userpool/")).map((a) => a.split("/").pop()));
  if (!strictTags) for (const p of await pages(aws, "cognito-idp", "list-user-pools", ["--max-results", "60"], "UserPools")) if (p.Name === `${P}-users`) poolIds.add(p.Id);
  for (const id of poolIds) {
    const p = (await aws.read("cognito-idp", "describe-user-pool", ["--user-pool-id", id]))?.UserPool;
    if (!p) continue;
    const tag = p.UserPoolTags?.[INSTALL_TAG];
    if (tag !== undefined && tag !== P) continue;
    inv.pools.push({ id, name: p.Name, domain: p.Domain ?? null, customDomain: p.CustomDomain ?? null, protected: p.DeletionProtection === "ACTIVE" });
  }

  // The secret(s): coderhelm/<stage>/...
  inv.secrets = secretNames.filter((n) => n.startsWith(`coderhelm/${scope.stage}/`));

  // Log groups and parameters named for the install.
  for (const lead of ["/aws/lambda/", "/aws/codebuild/"]) {
    for (const g of await pages(aws, "logs", "describe-log-groups", ["--log-group-name-prefix", `${lead}${P}`], "logGroups")) {
      if (ourAfter(scope, g.logGroupName, lead)) inv.logGroups.push(g.logGroupName);
    }
  }
  for (const p of await pages(aws, "ssm", "get-parameters-by-path", ["--path", `/${P}`, "--recursive"], "Parameters")) {
    if (p.Name.startsWith(`/${P}/`)) inv.params.push(p.Name);
  }

  // IAM (global): the boundaries and roles of this region's Launch Stacks, and roles tagged for the install.
  const boundaryRe = new RegExp(`^${P.replace(/-/g, "\\-")}-boundary-([0-9a-f]{8})$`);
  for (const pol of policies) {
    const m = pol.PolicyName.match(boundaryRe);
    if (m && ids.has(m[1])) inv.boundaries.push({ name: pol.PolicyName, arn: pol.Arn, id: m[1] });
  }
  // A native install's auto-update role and boundaries are kept on delete (IAM refuses to delete a policy a
  // role still carries). Their names have no region (one install per name per account), so only when this
  // region's coderhelm-<stage> stack is gone and the kept role trusts that stack, or the boundary is unused.
  let nativeAnchored = false;
  if (!described.some((x) => x.d.StackName === P)) {
    const nr = (await aws.read("iam", "get-role", ["--role-name", `${P}-deploy-stack`]))?.Role;
    nativeAnchored = !!nr && JSON.stringify(nr.AssumeRolePolicyDocument ?? "").includes(`:cloudformation:${region}:${scope.account}:stack/${P}/`);
    for (const pol of policies) {
      if (pol.PolicyName !== `${P}-boundary-stack` && pol.PolicyName !== `${P}-deploy-boundary-stack`) continue;
      const carried = nativeAnchored && nr?.PermissionsBoundary?.PermissionsBoundaryArn === pol.Arn;
      // Unused for over an hour: not one another region's install is creating right now (its boundary comes before its roles).
      const old = Date.now() - Date.parse(pol.CreateDate ?? "") > 3600_000;
      const unused = old && Number(pol.PermissionsBoundaryUsageCount ?? 1) === 0 && Number(pol.AttachmentCount ?? 1) === 0;
      if (carried || unused) inv.boundaries.push({ name: pol.PolicyName, arn: pol.Arn, id: "stack" });
      else inv.skipped.push({ kind: "permissions boundary", name: pol.PolicyName, why: "a native install's boundary still in use, and not by this region's deleted stack" });
    }
  }
  const boundaryArns = new Set(inv.boundaries.map((b) => b.arn));
  // A native install's roles carry coderhelm-<stage>-boundary-stack (or the auto-update role's
  // -deploy-boundary-stack copy): this install's while its stack is in this region, or once it
  // is deleted and the kept role trusts this region's stack.
  const rootHere = described.find((x) => x.d.StackName === P && ("KeepDataOnDelete" in x.params || !isLaunch(x.params)));
  const nativeBoundaryArns = new Set(rootHere || nativeAnchored
    ? policies.filter((x) => x.PolicyName === `${P}-boundary-stack` || x.PolicyName === `${P}-deploy-boundary-stack`).map((x) => x.Arn)
    : []);
  for (const r of await pages(aws, "iam", "list-roles", [], "Roles")) {
    if (!isOurName(scope, r.RoleName)) continue;
    const g = (await aws.read("iam", "get-role", ["--role-name", r.RoleName]))?.Role;
    if (!g) continue;
    const tag = tagOf(g.Tags, INSTALL_TAG);
    const boundary = g.PermissionsBoundary?.PermissionsBoundaryArn ?? null;
    const byId = inv.ids.some((id) => r.RoleName.endsWith(`-${id}`));
    // IAM is global: a role is this install's only by a boundary or a Launch Stack id found in this region, never by its tag alone.
    if ((boundary && (boundaryArns.has(boundary) || nativeBoundaryArns.has(boundary))) || byId) {
      inv.roles.push({ name: r.RoleName, boundary, tagged: tag === P });
    } else inv.skipped.push({ kind: "role", name: r.RoleName, why: tag && tag !== P ? `tagged for ${tag}` : "carries none of this region's install boundaries" });
  }
  // Something of the install must be in this region (a Launch Stack, its tag or its secret) before anything is removed.
  // CloudFront and Route 53 are global: in us-east-1 they show for any region's install, so they don't count.
  const regional = inv.tagged.some((a) => a.includes(`:${region}:`));
  // A native install's own stack, coderhelm-<stage> (its parts are its nested stacks): removed
  // as one, never taken apart, so it is listed apart from the Launch Stacks and the parts.
  inv.root = rootHere
    ? { name: P, status: rootHere.d.StackStatus, keepData: String(rootHere.params.KeepDataOnDelete ?? "true").toLowerCase() !== "false", roleArn: rootHere.d.RoleARN ?? null }
    : null;
  if (inv.root) {
    inv.launch = inv.launch.filter((l) => l.name !== P);
    inv.stacks = inv.stacks.filter((s) => s.name !== P);
  }
  inv.anchored = inv.launch.length > 0 || !!inv.root || regional || inv.secrets.length > 0 || nativeAnchored;
  return inv;
}

/** A native install's stack that still runs (not being deleted, not failed to delete): nothing of it is a leftover. */
export function runningRoot(inv) {
  return inv.root && !String(inv.root.status).startsWith("DELETE_") ? inv.root : null;
}

/** The install guide's progress file says it is this install's (it is public). */
async function progressIsOurs(fetchImpl, id, scope) {
  try {
    const r = await (fetchImpl ?? fetch)(`https://coderhelm-setup-${id}.s3.${scope.region}.amazonaws.com/status.json`, { signal: AbortSignal.timeout(6000) });
    if (!r.ok) return false;
    const d = await r.json();
    return d?.stage === scope.stage && d?.region === scope.region;
  } catch {
    return false;
  }
}

// ── What will go ────────────────────────────────────────────────────────

/** Everything the uninstall removes and keeps, by kind, for people to read before they confirm. */
export function plan(inv) {
  const remove = [];
  const keep = [];
  const data = inv.mode === "everything";
  const add = (kind, name, isData = false) => (isData && !data ? keep : remove).push({ kind, name });
  if (inv.root) add("Launch Stack", inv.root.name);
  for (const s of inv.stacks) add("stack", s.name);
  for (const l of inv.launch) add("Launch Stack", l.name);
  for (const t of inv.tables) add("table", t.name, true);
  for (const b of inv.backups) add("backup", b.name, true);
  for (const b of inv.buckets) add(b.kind, b.name, b.kind === "data bucket");
  for (const k of inv.keys) add("KMS key", k.id, true);
  for (const a of inv.aliases) add("KMS alias", a.name, true);
  for (const p of inv.pools) add("user pool", p.name, true);
  for (const s of inv.secrets) add("secret", s, true);
  for (const g of inv.logGroups) add("log group", g);
  for (const p of inv.params) add("parameter", p);
  for (const r of inv.roles) add("role", r.name);
  for (const m of inv.esms ?? []) add("event source mapping", m.name);
  for (const b of inv.boundaries) add("permissions boundary", b.name);
  // A certificate the updater requested is in us-east-1 for any region's install: listed for you, not removed.
  const notRemoved = inv.certs.map((c) => ({ kind: "certificate", name: c.split("/").pop(), why: "in us-east-1 (ACM), tagged coderhelm = " + inv.scope.prefix + "; delete it in ACM once no install of this name uses it" }));
  return { remove, keep, notRemoved };
}

// ── Removing it ─────────────────────────────────────────────────────────

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** The stacks, dependents first: a stack goes once no remaining stack imports its exports. */
export async function stackWaves(aws, stacks) {
  const importers = new Map();
  for (const s of stacks) {
    const by = new Set();
    for (const e of s.exports ?? []) for (const i of (await aws.read("cloudformation", "list-imports", ["--export-name", e]))?.Imports ?? []) by.add(i);
    importers.set(s.name, by);
  }
  const left = new Set(stacks.map((s) => s.name));
  const waves = [];
  while (left.size) {
    const wave = [...left].filter((n) => ![...importers.get(n)].some((i) => i !== n && left.has(i)));
    // A cycle (never in CoderHelm): delete the rest together and let CloudFormation say why.
    const next = wave.length ? wave : [...left];
    waves.push(next.sort());
    for (const n of next) left.delete(n);
  }
  return waves.map((w) => w.map((n) => stacks.find((s) => s.name === n)));
}

/**
 * Why a stack's delete failed: its first DELETE_FAILED with a reason, and
 * for a nested part that failed, that part's own first failure (which
 * names the resource and AWS's reason, like a bucket with versions left).
 */
async function deleteFailure(aws, events, name) {
  const failedEv = events.find((e) => e.ResourceStatus === "DELETE_FAILED" && e.LogicalResourceId !== name && !/cancelled/i.test(e.ResourceStatusReason ?? ""))
    ?? events.find((e) => e.ResourceStatus === "DELETE_FAILED");
  const why = failedEv?.ResourceStatusReason ?? "DELETE_FAILED";
  if (failedEv?.ResourceType === "AWS::CloudFormation::Stack" && failedEv.PhysicalResourceId) {
    const inner = (await aws.read("cloudformation", "describe-stack-events", ["--stack-name", failedEv.PhysicalResourceId]))?.StackEvents ?? [];
    const e = inner.find((x) => x.ResourceStatus === "DELETE_FAILED" && x.ResourceType !== "AWS::CloudFormation::Stack");
    if (e) return `${failedEv.LogicalResourceId}: ${e.LogicalResourceId}: ${e.ResourceStatusReason ?? "DELETE_FAILED"}`;
  }
  return why;
}

async function waitGone(ctx, names, onTick) {
  if (ctx.aws.dryRun) return {};
  const failed = {};
  const left = new Set(names);
  for (let i = 0; left.size && i < (ctx.maxPolls ?? 360); i++) {
    for (const n of [...left]) {
      const d = (await ctx.aws.read("cloudformation", "describe-stacks", ["--stack-name", n]))?.Stacks?.[0];
      if (!d || d.StackStatus === "DELETE_COMPLETE") left.delete(n);
      else if (d.StackStatus === "DELETE_FAILED") {
        const ev = (await ctx.aws.read("cloudformation", "describe-stack-events", ["--stack-name", n]))?.StackEvents ?? [];
        failed[n] = await deleteFailure(ctx.aws, ev, n);
        left.delete(n);
      }
    }
    if (left.size) {
      await onTick?.([...left]);
      await (ctx.sleep ?? sleep)(ctx.pollMs ?? 10_000);
    }
  }
  for (const n of left) failed[n] = "still deleting after an hour";
  return failed;
}

/** What AWS says is temporary: a table still settling, throttling, limits. */
const TABLE_RETRYABLE = /ResourceInUse|LimitExceeded|Throttl|InternalServerError|being (updated|created)/i;

/**
 * Deletes a table and waits until it is gone. A GlobalTable delete can
 * report a failure while the table is still going away (seen in a real
 * uninstall: CheckpointsTable and TracesTable failed, then vanished a
 * minute later), so a temporary refusal is retried with backoff and the
 * table is only reported when it is still there at the end, with AWS's
 * own reason.
 */
export async function deleteTable(ctx, name) {
  const aws = ctx.aws;
  if (aws.dryRun) return { ok: true };
  let reason = "";
  let delay = ctx.pollMs ?? 5_000;
  for (let i = 0; i < (ctx.maxPolls ?? 120); i++) {
    const d = (await aws.read("dynamodb", "describe-table", ["--table-name", name]))?.Table;
    if (!d) return { ok: true };
    if (d.TableStatus !== "DELETING") {
      if (d.DeletionProtectionEnabled) await aws.write("dynamodb", "update-table", ["--table-name", name, "--no-deletion-protection-enabled"]);
      const res = await aws.write("dynamodb", "delete-table", ["--table-name", name]);
      if (!res.ok) {
        if (/ResourceNotFound/.test(res.error)) return { ok: true };
        reason = res.error;
        if (!TABLE_RETRYABLE.test(res.error) && !d.DeletionProtectionEnabled) return { ok: false, error: res.error };
        ctx.log?.(`  ${name}: ${res.error.split("\n")[0]} (trying again)`);
      }
    } else reason = "still deleting";
    await (ctx.sleep ?? sleep)(delay);
    delay = Math.min(delay * 2, 30_000);
  }
  return { ok: false, error: `still there after waiting: ${reason}` };
}

async function emptyBucket(aws, bucket) {
  for (let i = 0; i < 10_000; i++) {
    const r = await aws.read("s3api", "list-object-versions", ["--bucket", bucket, "--max-items", "1000"]);
    const objs = [...(r?.Versions ?? []), ...(r?.DeleteMarkers ?? [])].map((o) => ({ Key: o.Key, VersionId: o.VersionId }));
    if (!objs.length) return true;
    const res = await aws.write("s3api", "delete-objects", ["--bucket", bucket, "--delete", JSON.stringify({ Objects: objs, Quiet: true })]);
    if (!res.ok || aws.dryRun) return res.ok;
  }
  return false;
}

/**
 * Removes the install. `report` collects what went, what stayed and why;
 * `onStep` gets each phase for the progress file. Never throws for one
 * resource: each refusal is listed and the rest goes on.
 */
export async function remove(ctx, inv0, { onStep = async () => {}, hybrid = null } = {}) {
  let inv = inv0;
  const aws = ctx.aws;
  const { scope } = inv;
  const everything = inv.mode === "everything";
  const report = { removed: [], kept: [], scheduled: [], problems: [], hybrid: null };
  const gone = (kind, name) => report.removed.push({ kind, name });
  const fail = (kind, name, error) => report.problems.push({ kind, name, error: String(error).slice(0, 300) });
  const run = async (kind, name, svc, op, args, r) => {
    const res = await aws.write(svc, op, args, r);
    if (res.ok) gone(kind, name);
    else fail(kind, name, res.error);
    return res.ok;
  };
  const deployRoles = new Set(inv.ids.map((id) => `arn:${scope.partition}:iam::${scope.account}:role/${scope.prefix}-deploy-${id}`));

  // 1. Hybrid: release the name, hand GitHub and Jira back to CoderHelm, revoke the broker key.
  if (hybrid) report.hybrid = await hybrid(onStep);

  // 2a. A native install: its own stack, as one. With "everything", every bucket of the install is
  //     emptied first (CloudFormation refuses a bucket with object versions left); the data bucket
  //     only then, never when data is kept. The parts go with the stack, in its order.
  if (inv.root) {
    for (const b of inv.buckets) {
      if (b.kind === "progress bucket" || (b.kind === "data bucket" && !everything)) continue;
      if (!(await emptyBucket(aws, b.name))) fail(b.kind, b.name, "could not empty it");
    }
    const args = ["--stack-name", inv.root.name];
    if (inv.root.roleArn) args.push("--role-arn", inv.root.roleArn);
    await onStep({ phase: "stacks", message: `Deleting ${inv.root.name}`, step: 0, steps: 1 });
    const res = await aws.write("cloudformation", "delete-stack", args);
    if (!res.ok) {
      fail("Launch Stack", inv.root.name, res.error);
      return report;
    }
    const failed = await waitGone(ctx, [inv.root.name], () => onStep({ phase: "stacks", message: `Waiting for ${inv.root.name} and its parts`, step: 0, steps: 1 }));
    if (failed[inv.root.name]) {
      // Its first failure, with AWS's reason: deleting it again (running this again) picks up there.
      fail("Launch Stack", inv.root.name, failed[inv.root.name]);
      return report;
    }
    gone("Launch Stack", inv.root.name);
    // What the stack kept (its data with KeepDataOnDelete=true) and anything it left, found again.
    const after = await discover(ctx, { ...scope, siblings: inv.scope.siblings }, { mode: inv.mode, installStatus: ctx.installStatus ?? "", ownStatus: ctx.ownStatus ?? "", strictTags: !!ctx.guided, fetchImpl: ctx.fetchImpl });
    inv = { ...after, launch: after.launch };
  }

  // 2b. An older install's stacks, through its deploy role, dependents first.
  const waves = await stackWaves(aws, inv.stacks);
  const total = inv.stacks.length;
  let done = 0;
  for (const wave of waves) {
    const started = [];
    for (const s of wave) {
      const args = ["--stack-name", s.name];
      if (s.roleArn && deployRoles.has(s.roleArn)) args.push("--role-arn", s.roleArn);
      else if (ctx.guided) {
        fail("stack", s.name, "it isn't deployed through this install's deploy role, so the uninstaller may not delete it; delete it in CloudFormation or uninstall from a terminal");
        continue;
      } else if (s.roleArn) args.push("--role-arn", s.roleArn);
      const res = await aws.write("cloudformation", "delete-stack", args);
      if (res.ok) started.push(s.name);
      else fail("stack", s.name, res.error);
    }
    await onStep({ phase: "stacks", message: `Deleting ${started.join(", ") || "stacks"}`, step: done, steps: total });
    const failed = await waitGone(ctx, started, (left) => onStep({ phase: "stacks", message: `Waiting for ${left.join(", ")}`, step: done + started.length - left.length, steps: total }));
    for (const n of started) (failed[n] ? fail("stack", n, failed[n]) : gone("stack", n));
    done += wave.length;
    // A stack that didn't go keeps what depends on it in place: stop before the data.
    if (report.problems.some((p) => p.kind === "stack")) return report;
  }

  // 3. The data (delete everything only).
  if (everything) {
    await onStep({ phase: "data", message: "Deleting the tables, the data bucket, the user pool and the secret" });
    for (const t of inv.tables) {
      const r = await deleteTable(ctx, t.name);
      if (r.ok) gone("table", t.name);
      else fail("table", t.name, r.error);
    }
    for (const b of inv.backups) await run("backup", b.name, "dynamodb", "delete-backup", ["--backup-arn", b.arn]);
    for (const b of inv.buckets.filter((x) => x.kind === "data bucket")) {
      if (await emptyBucket(aws, b.name)) await run(b.kind, b.name, "s3api", "delete-bucket", ["--bucket", b.name]);
      else fail(b.kind, b.name, "could not empty it");
    }
    for (const p of inv.pools) {
      if (p.protected) await aws.write("cognito-idp", "update-user-pool", ["--user-pool-id", p.id, "--deletion-protection", "INACTIVE"]);
      for (const d of [p.domain, p.customDomain].filter(Boolean)) await aws.write("cognito-idp", "delete-user-pool-domain", ["--domain", d, "--user-pool-id", p.id]);
      await run("user pool", p.name, "cognito-idp", "delete-user-pool", ["--user-pool-id", p.id]);
    }
    for (const s of inv.secrets) await run("secret", s, "secretsmanager", "delete-secret", ["--secret-id", s, "--force-delete-without-recovery"]);
    for (const a of inv.aliases) await run("KMS alias", a.name, "kms", "delete-alias", ["--alias-name", a.name]);
    for (const k of inv.keys) {
      if (k.state === "PendingDeletion") {
        report.scheduled.push({ kind: "KMS key", name: k.id, date: k.deletionDate });
        continue;
      }
      const res = await aws.write("kms", "schedule-key-deletion", ["--key-id", k.id, "--pending-window-in-days", String(KEY_DELETION_DAYS)]);
      if (res.ok) {
        let date = null;
        try {
          date = JSON.parse(res.out || "{}").DeletionDate ?? null;
        } catch {}
        report.scheduled.push({ kind: "KMS key", name: k.id, date });
      } else fail("KMS key", k.id, res.error);
    }
  } else {
    for (const x of plan(inv).keep) report.kept.push(x);
  }

  // 4. The Launch Stack(s) that installed it.
  if (inv.launch.length) {
    await onStep({ phase: "launch", message: "Deleting the Launch Stack" });
    const started = [];
    for (const l of inv.launch) if ((await aws.write("cloudformation", "delete-stack", ["--stack-name", l.name])).ok) started.push(l.name);
    const failed = await waitGone(ctx, started);
    for (const l of inv.launch) (started.includes(l.name) && !failed[l.name] ? gone("Launch Stack", l.name) : fail("Launch Stack", l.name, failed[l.name] ?? "refused"));
  }

  // 5. What is left by name: logs, parameters, certificates, buckets, roles, then the boundaries.
  await onStep({ phase: "cleanup", message: "Removing the logs, parameters, buckets, roles and the permissions boundary" });
  const now = await discover(ctx, { ...scope, siblings: [] }, { mode: inv.mode, installStatus: ctx.installStatus ?? "", ownStatus: ctx.ownStatus ?? "", strictTags: !!ctx.guided, fetchImpl: ctx.fetchImpl });
  for (const g of now.logGroups) await run("log group", g, "logs", "delete-log-group", ["--log-group-name", g]);
  for (const m of now.esms ?? []) await run("event source mapping", m.name, "lambda", "delete-event-source-mapping", ["--uuid", m.uuid]);
  for (let i = 0; i < now.params.length; i += 10) {
    const names = now.params.slice(i, i + 10);
    const res = await aws.write("ssm", "delete-parameters", ["--names", ...names]);
    for (const n of names) (res.ok ? gone("parameter", n) : fail("parameter", n, res.error));
  }
  for (const b of now.buckets.filter((x) => x.kind !== "data bucket")) {
    if (await emptyBucket(aws, b.name)) await run(b.kind, b.name, "s3api", "delete-bucket", ["--bucket", b.name]);
    else fail(b.kind, b.name, "could not empty it");
  }
  for (const r of now.roles) {
    for (const p of (await aws.read("iam", "list-role-policies", ["--role-name", r.name]))?.PolicyNames ?? []) await aws.write("iam", "delete-role-policy", ["--role-name", r.name, "--policy-name", p]);
    for (const p of (await aws.read("iam", "list-attached-role-policies", ["--role-name", r.name]))?.AttachedPolicies ?? []) await aws.write("iam", "detach-role-policy", ["--role-name", r.name, "--policy-arn", p.PolicyArn]);
    await run("role", r.name, "iam", "delete-role", ["--role-name", r.name]);
  }
  for (const b of now.boundaries) {
    for (const v of (await aws.read("iam", "list-policy-versions", ["--policy-arn", b.arn]))?.Versions ?? []) {
      if (!v.IsDefaultVersion) await aws.write("iam", "delete-policy-version", ["--policy-arn", b.arn, "--version-id", v.VersionId]);
    }
    await run("permissions boundary", b.name, "iam", "delete-policy", ["--policy-arn", b.arn]);
  }
  return report;
}

/** What is still there afterwards: anything named or tagged for the install, by kind (keys waiting for deletion apart). */
export function leftovers(inv) {
  const { remove: left } = plan({ ...inv, mode: "everything" });
  const scheduled = inv.keys.filter((k) => k.state === "PendingDeletion").map((k) => ({ kind: "KMS key", name: k.id, date: k.deletionDate }));
  const keys = new Set(scheduled.map((s) => s.name));
  const listed = new Set([...left.map((l) => l.name), ...inv.pools.map((p) => p.id), ...inv.keys.map((k) => k.id)]);
  // Anything else still carrying the install's tag.
  const other = (inv.tagged ?? []).filter((a) => !a.includes(":key/") && ![...listed].some((n) => a.endsWith(n) || a.includes(`/${n}`) || a.includes(`:${n}`))).map((a) => ({ kind: "tagged", name: a }));
  return { left: [...left.filter((l) => !(l.kind === "KMS key" && keys.has(l.name))), ...other], scheduled };
}

// ── Hybrid ──────────────────────────────────────────────────────────────

/** What CoderHelm's hosted side forwards to this install (its GitHub installations and Jira site), when it says. */
export async function hybridLinks(ctx, secret, hostedUrl = DEFAULT_HOSTED_URL) {
  const reg = registration(secret);
  if (!reg) return null;
  const path = "/hybrid/status";
  const ts = Math.floor(Date.now() / 1000);
  try {
    const r = await (ctx.fetchImpl ?? fetch)(`${hostedUrl}${path}`, {
      headers: { "x-coderhelm-install": reg.id, "x-coderhelm-timestamp": String(ts), "x-coderhelm-signature": signRequest(reg.secret, ts, "GET", path), "user-agent": "coderhelm-selfhost" },
      signal: AbortSignal.timeout(8000),
    });
    if (!r.ok) return { registered: true };
    const d = await r.json();
    const github = Array.isArray(d?.github?.links) ? d.github.links.length : null;
    return { registered: true, name: d?.name ?? null, github, jira: !!d?.jira?.link };
  } catch {
    return { registered: true };
  }
}

/** Leaves hybrid before anything is deleted: the name, the GitHub and Jira links and the broker key. */
export async function deregister(ctx, scope, hostedUrl, onStep = async () => {}) {
  const out = await ctx.aws.read("secretsmanager", "get-secret-value", ["--secret-id", `coderhelm/${scope.stage}/secrets`, "--query", "SecretString"]);
  let secret = null;
  try {
    secret = typeof out === "string" ? JSON.parse(out) : null;
  } catch {}
  if (!registration(secret)) return { registered: false };
  await onStep({ phase: "hybrid", message: "Releasing the CoderHelm address, and handing GitHub and Jira back to CoderHelm" });
  const links = await hybridLinks(ctx, secret, hostedUrl);
  const released = await leaveHybrid({ ...ctx, log: ctx.log ?? (() => {}) }, secret, hostedUrl);
  return { ...links, registered: true, released };
}

// ── The progress file (guided) ──────────────────────────────────────────

export const PHASES = ["starting", "preparing", "hybrid", "stacks", "data", "launch", "cleanup", "verifying", "done", "failed"];

/** Account numbers, ARNs and emails out of anything that goes in the public progress file. */
export function redact(text) {
  return String(text ?? "")
    .replace(/arn:aws[a-z-]*:[^\s"'`]+/g, (a) => a.split(":").slice(5).join(":").replace(/\b\d{12}\b/g, "<account>"))
    .replace(/\b\d{12}\b/g, "<account>")
    .replace(/[^\s@"'<>]+@[^\s@"'<>]+\.[a-z]{2,}/gi, "<email>")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 300);
}

const list = (xs, n = 300) => (xs ?? []).slice(0, n).map((x) => ({ kind: x.kind, name: redact(x.name), ...(x.date ? { date: String(x.date) } : {}), ...(x.error ? { error: redact(x.error) } : {}), ...(x.why ? { why: redact(x.why) } : {}) }));

export function statusDoc(f, now = new Date()) {
  if (!PHASES.includes(f.phase)) throw new Error(`unknown phase ${f.phase}`);
  return {
    schema: 1,
    kind: "uninstall",
    phase: f.phase,
    message: redact(f.message ?? ""),
    mode: f.mode ?? null,
    stage: f.stage ?? null,
    region: f.region ?? null,
    step: Number.isInteger(f.step) ? f.step : null,
    steps: Number.isInteger(f.steps) ? f.steps : null,
    removed: list(f.removed),
    kept: list(f.kept),
    scheduled: list(f.scheduled),
    left: list(f.left),
    notRemoved: list(f.notRemoved),
    problems: list(f.problems, 50),
    hybrid: f.hybrid ? { registered: !!f.hybrid.registered, released: !!f.hybrid.released, github: Number.isInteger(f.hybrid.github) ? f.hybrid.github : null, jira: !!f.hybrid.jira } : null,
    fix: f.fix ? redact(f.fix) : null,
    logUrl: f.logUrl ?? null,
    updatedAt: now.toISOString(),
  };
}

// ── The whole run ───────────────────────────────────────────────────────

/**
 * Finds, removes and checks. Resolves with the final progress document
 * (phase done or failed); `write` gets every progress document.
 */
export async function runUninstall(ctx, { stage, region, account, mode, installStatus = "", ownStatus = "", hostedUrl, write = async () => {} } = {}) {
  if (!MODES.includes(mode)) throw new Error(`mode must be keep or everything, not ${mode}`);
  const base = { stage, region, mode };
  const status = async (fields) => {
    const d = statusDoc({ ...base, ...fields });
    try {
      await write(d);
    } catch (e) {
      ctx.log?.(`(could not update the progress file: ${e.message})`);
    }
    return d;
  };
  let scope;
  try {
    scope = scopeOf({ stage, region, account });
  } catch (e) {
    return status({ phase: "failed", message: e.message, fix: "Launch the uninstaller again with the install's name." });
  }
  ctx.installStatus = installStatus;
  ctx.ownStatus = ownStatus;
  await status({ phase: "preparing", message: "Finding the install's pieces" });
  const inv = await discover(ctx, scope, { mode, installStatus, ownStatus, strictTags: !!ctx.guided, fetchImpl: ctx.fetchImpl });
  const found = plan(inv);
  if (!inv.anchored || (!found.remove.length && !found.keep.length)) {
    return status({ phase: "failed", message: `Nothing of the install ${stage} is in ${region}: no Launch Stack, no resource tagged for it and no secret. Nothing was deleted.`, fix: "Check the install name and the region, then launch the uninstaller again." });
  }
  const report = await remove(ctx, inv, {
    onStep: (s) => status({ ...s, removed: [] }),
    hybrid: (onStep) => deregister(ctx, inv.scope, hostedUrl, onStep),
  });
  await status({ phase: "verifying", message: "Checking nothing is left", removed: report.removed });
  const after = await discover(ctx, scope, { mode, installStatus, ownStatus, strictTags: !!ctx.guided, fetchImpl: ctx.fetchImpl });
  const { left, scheduled } = leftovers(after);
  const keptNames = new Set(report.kept.map((k) => `${k.kind}:${k.name}`));
  const unexpected = left.filter((l) => !keptNames.has(`${l.kind}:${l.name}`));
  const ok = !report.problems.length && !unexpected.length;
  return status({
    phase: ok ? "done" : "failed",
    message: ok
      ? mode === "everything" ? "CoderHelm and its data are removed from this account." : "CoderHelm is removed from this account. Its data is kept."
      : `Most of CoderHelm is removed, but ${report.problems.length + unexpected.length} piece${report.problems.length + unexpected.length === 1 ? "" : "s"} could not be.`,
    fix: ok ? null : "Launch the uninstaller again: it picks up where it stopped. Each piece left is listed with AWS's reason.",
    removed: report.removed,
    kept: report.kept,
    scheduled: [...report.scheduled, ...scheduled.filter((s) => !report.scheduled.some((r) => r.name === s.name))],
    left: unexpected,
    notRemoved: plan(after).notRemoved,
    problems: report.problems,
    hybrid: report.hybrid,
  });
}
