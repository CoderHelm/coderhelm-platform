// Moves an install from the multi-stack layout (coderhelm-<stage>-database,
// -api, ...) to one native stack, coderhelm-<stage>, without touching its
// data (docs/self-host/migrate.md):
//
//   1. each part stack's current template goes to a private bucket of the
//      install's (S3 is where CloudFormation reads nested templates from);
//   2. the parent stack is created by an IMPORT change set that nests the
//      existing part stacks as they are (DeletionPolicy Retain), so no
//      table, bucket, user pool or key changes owner one by one;
//   3. a second IMPORT adds the existing secret to the parent;
//   4. the UPDATE change set to the release's coderhelm.json is created with
//      nested change sets and checked: if any table, bucket, user pool, key
//      or secret would be replaced or removed, it stops there and deletes
//      the change set (the install keeps running as before, now nested);
//   5. only then is it executed.
//
// Event source mappings that the update replaces are deleted right before
// it runs: Lambda refuses a second mapping for the same queue and function,
// and messages wait in their queues for the few minutes in between.
import crypto from "node:crypto";
import { Runner } from "./runner.mjs";

const canonical = (v) => (Array.isArray(v) ? v.map(canonical) : v && typeof v === "object" ? Object.fromEntries(Object.keys(v).sort().map((k) => [k, canonical(v[k])])) : v);
const shaOf = (v) => crypto.createHash("sha256").update(JSON.stringify(canonical(v))).digest("hex");

export const DATA_TYPES = ["AWS::DynamoDB::GlobalTable", "AWS::DynamoDB::Table", "AWS::S3::Bucket", "AWS::KMS::Key", "AWS::Cognito::UserPool", "AWS::SecretsManager::Secret"];
export const CAPABILITIES = ["CAPABILITY_IAM", "CAPABILITY_NAMED_IAM", "CAPABILITY_AUTO_EXPAND"];
const NOT_PARTS = ["access", "uninstall", "launch", "migrate"];
const LIVE = /_COMPLETE$/;

/** `coderhelm-prod-log-analyzer` -> `LogAnalyzer` (the parent's logical id, infra/lib/native/package.ts). */
export function partName(stackName, prefix) {
  return stackName.slice(prefix.length + 1).split("-").map((w) => w.charAt(0).toUpperCase() + w.slice(1)).join("");
}

/** The part stacks of an install: coderhelm-<stage>-<part>, not another install's, not the access or uninstall stacks. */
export function partStacks(prefix, stacks, siblings = []) {
  return stacks
    .filter((s) => s.StackName.startsWith(`${prefix}-`) && !s.ParentId)
    .filter((s) => !siblings.some((x) => s.StackName === x || s.StackName.startsWith(`${x}-`)))
    .filter((s) => !NOT_PARTS.some((n) => s.StackName.slice(prefix.length + 1).startsWith(n)))
    .filter((s) => !/-(installer|starter|window|deploy)-/.test(s.StackName));
}

/** The parent that nests the part stacks exactly as they are. */
export function importTemplate(parts, { secret = null } = {}) {
  const Resources = {};
  for (const p of parts) {
    Resources[p.logical] = {
      Type: "AWS::CloudFormation::Stack",
      DeletionPolicy: "Retain",
      UpdateReplacePolicy: "Retain",
      Properties: {
        TemplateURL: p.templateUrl,
        ...(p.parameters?.length ? { Parameters: Object.fromEntries(p.parameters.map((x) => [x.ParameterKey, x.ParameterValue])) } : {}),
      },
    };
  }
  if (secret) {
    Resources.InstallSecret = {
      Type: "AWS::SecretsManager::Secret",
      DeletionPolicy: "Retain",
      UpdateReplacePolicy: "Retain",
      Properties: { Name: secret.name, Description: secret.description ?? "CoderHelm: GitHub App keys and the session signing key" },
    };
  }
  return { AWSTemplateFormatVersion: "2010-09-09", Description: "CoderHelm: the install's parts, nested as they are (coderhelm-selfhost migrate)", Resources };
}

/** The native parameters, from the install's config. */
export function nativeParameters(cfg, { keepData = true } = {}) {
  const v = (k, x) => ({ ParameterKey: k, ParameterValue: String(x ?? "") });
  const from = cfg.email?.from ?? "";
  const admin = (cfg.admins ?? [])[0] ?? "";
  const u = cfg.updates ?? {};
  const [day, time] = String(u.window ?? "Sun 03:00").toLowerCase().split(/\s+/);
  return [
    v("Stage", cfg.stage),
    v("AdminEmail", admin),
    v("EmailFrom", from === admin ? "" : from),
    v("EmailTransport", cfg.email?.transport ?? "ses"),
    v("ModelProvider", cfg.models?.provider ?? "anthropic"),
    v("BedrockInferencePrefix", cfg.models?.bedrockInferencePrefix === undefined ? "auto" : cfg.models.bedrockInferencePrefix || "none"),
    v("Hostname", cfg.domain?.hostname ?? ""),
    v("DomainCertificateArn", cfg.domain?.certificateArn ?? ""),
    v("DomainZoneId", cfg.domain?.hostedZoneId ?? ""),
    v("SignupDomain", (cfg.signup?.domains ?? [])[0] ?? ""),
    v("RequireTwoFactor", cfg.signup?.requireTwoFactor ? "on" : "off"),
    v("AutoUpdate", u.auto ? "on" : "off"),
    v("UpdateDay", (day ?? "sun").slice(0, day === "daily" ? 5 : 3)),
    v("UpdateHour", String(Number((time ?? "03:00").split(":")[0]))),
    v("UpdateTimezone", u.timezone ?? "UTC"),
    v("KeepDataOnDelete", keepData ? "true" : "false"),
  ];
}

/** Every change that would replace or remove data: the migration's gate. */
export function blockers(changeSets) {
  const out = [];
  for (const cs of changeSets) {
    for (const c of cs.Changes ?? []) {
      const rc = c.ResourceChange ?? {};
      if (!DATA_TYPES.includes(rc.ResourceType)) continue;
      const replace = rc.Replacement === "True" || rc.Replacement === "Conditional";
      if (rc.Action === "Remove" || replace) {
        out.push({ part: cs.part ?? "parent", id: rc.LogicalResourceId, type: rc.ResourceType, action: rc.Action, replacement: rc.Replacement ?? "False", physical: rc.PhysicalResourceId ?? "" });
      }
    }
  }
  return out;
}

/** Event source mappings an update replaces (Remove + Add on the same function). */
export function replacedEventSources(changeSets) {
  const ids = [];
  for (const cs of changeSets) {
    for (const c of cs.Changes ?? []) {
      const rc = c.ResourceChange ?? {};
      if (rc.ResourceType === "AWS::Lambda::EventSourceMapping" && (rc.Action === "Remove" || rc.Replacement === "True") && rc.PhysicalResourceId) ids.push(rc.PhysicalResourceId);
    }
  }
  return ids;
}

/**
 * The log groups the update adds (declared for every function, so Delete
 * stack removes them) that Lambda already made by itself for the install:
 * CloudFormation can't create a log group that exists, so they are deleted
 * just before the update (their old log lines go with them; it says so).
 */
export function addedLogGroups(changeSets, templates, stage) {
  const names = [];
  for (const cs of changeSets) {
    const t = templates[cs.part];
    if (!t) continue;
    for (const c of cs.Changes ?? []) {
      const rc = c.ResourceChange ?? {};
      if (rc.ResourceType !== "AWS::Logs::LogGroup" || rc.Action !== "Add") continue;
      const r = t.Resources?.[rc.LogicalResourceId];
      if (!r?.Metadata?.["coderhelm:native-log"]) continue;
      const join = r.Properties?.LogGroupName?.["Fn::Join"];
      if (!join) continue;
      const fn = join[1][1];
      const text = typeof fn === "string" ? fn : typeof fn?.["Fn::Sub"] === "string" ? fn["Fn::Sub"] : null;
      if (text && !/\$\{(?!Stage\})/.test(text)) names.push(`/aws/lambda/${text.replace(/\$\{Stage\}/g, stage)}`);
    }
  }
  return [...new Set(names)].sort();
}

export function awsClient(ctx, region) {
  const at = (args) => [...args, "--region", region, "--output", "json"];
  return {
    async read(svc, op, args = []) {
      return ctx.runner.json("aws", [svc, op, ...at(args)], { allowFail: true });
    },
    async write(svc, op, args = []) {
      const res = await ctx.runner.run("aws", [svc, op, ...at(args)], { capture: true, allowFail: true });
      if (res.code !== 0) throw new Error(`aws ${svc} ${op}: ${String(res.stderr ?? "").trim()}`);
      try {
        return JSON.parse(res.stdout || "{}");
      } catch {
        return {};
      }
    },
  };
}

async function waitFor(ctx, check, what, { tries = 360 } = {}) {
  for (let i = 0; i < tries; i++) {
    const r = await check();
    if (r.done) return r;
    if (r.failed) throw new Error(`${what}: ${r.failed}`);
    await (ctx.sleep ?? ((ms) => new Promise((x) => setTimeout(x, ms))))(ctx.pollMs ?? 10_000);
  }
  throw new Error(`${what}: still waiting after an hour`);
}

async function stackFailure(aws, stack) {
  const ev = (await aws.read("cloudformation", "describe-stack-events", ["--stack-name", stack]))?.StackEvents ?? [];
  const f = ev.filter((e) => /_FAILED$/.test(e.ResourceStatus) && !/cancelled/.test(e.ResourceStatusReason ?? "")).pop();
  return f ? `${f.LogicalResourceId}: ${f.ResourceStatusReason}` : "";
}

async function runChangeSet(ctx, aws, stack, args, label) {
  const cs = await aws.write("cloudformation", "create-change-set", ["--stack-name", stack, ...args]);
  const id = cs.Id;
  const d = await waitFor(ctx, async () => {
    const x = await aws.read("cloudformation", "describe-change-set", ["--change-set-name", id]);
    if (x?.Status === "CREATE_COMPLETE") return { done: true, cs: x };
    if (x?.Status === "FAILED") return { failed: x.StatusReason ?? "the change set failed" };
    return {};
  }, `${label} change set`);
  return { id, cs: d.cs };
}

async function execute(ctx, aws, stack, id, label, want) {
  await aws.write("cloudformation", "execute-change-set", ["--change-set-name", id]);
  await waitFor(ctx, async () => {
    const s = (await aws.read("cloudformation", "describe-stacks", ["--stack-name", stack]))?.Stacks?.[0];
    if (s?.StackStatus === want) return { done: true };
    if (s && LIVE.test(s.StackStatus) && s.StackStatus !== want) return { failed: `${s.StackStatus}: ${await stackFailure(aws, stack)}` };
    if (s && /FAILED$/.test(s.StackStatus)) return { failed: `${s.StackStatus}: ${await stackFailure(aws, stack)}` };
    return {};
  }, label);
}

/**
 * The migration. `opts`: stage, region, account, version, cfg (the install's
 * config), templateUrl (the release's coderhelm.json), yes, log.
 */
export async function migrate(ctx, opts) {
  const { stage, region, account, templateUrl, cfg } = opts;
  const log = ctx.log ?? console.log;
  const aws = ctx.aws ?? awsClient(ctx, region);
  const prefix = `coderhelm-${stage}`;
  if (!/^[a-z][a-z0-9]{0,19}$/.test(stage)) {
    // One-stack names have no hyphen, so coderhelm-<name>-* never also names another install.
    throw new Error(`"${stage}" has a hyphen, which one-stack installs don't allow: install fresh under a new name and move the data (docs/self-host/migrate.md, "If migrate can't run").`);
  }
  if (!/^https:\/\/coderhelm-public\.s3\.amazonaws\.com\/launch\/v[^/]+\/coderhelm\.json$/.test(templateUrl)) {
    throw new Error(`${templateUrl} is not a CoderHelm release template`);
  }
  // The release's parent template, checked against its signed sha256 before it is used.
  const body = await (await (ctx.fetchImpl ?? fetch)(templateUrl)).text();
  if (!opts.templateSha || crypto.createHash("sha256").update(body).digest("hex") !== opts.templateSha) {
    throw new Error("The release's template doesn't match its signed checksum: not migrating.");
  }
  const release = JSON.parse(body);
  const summaries = (await aws.read("cloudformation", "list-stacks", ["--stack-status-filter", "CREATE_COMPLETE", "UPDATE_COMPLETE", "UPDATE_ROLLBACK_COMPLETE", "IMPORT_COMPLETE"]))?.StackSummaries ?? [];
  let resume = false;
  if (summaries.some((s) => s.StackName === prefix)) {
    // A migration that stopped before its update (no --yes, or the check): carry on from there.
    // An earlier Launch Stack of this name: it must go first (its boundary, role and bucket stay).
    const d = (await aws.read("cloudformation", "describe-stacks", ["--stack-name", prefix]))?.Stacks?.[0] ?? {};
    const params = new Set((d.Parameters ?? []).map((p) => p.ParameterKey));
    const t = (await aws.read("cloudformation", "get-template", ["--stack-name", prefix, "--template-stage", "Original"]))?.TemplateBody;
    const tpl = typeof t === "string" ? JSON.parse(t) : t ?? {};
    const kinds = new Set(Object.values(tpl.Resources ?? {}).map((r) => r.Type));
    const nestedOnly = [...kinds].every((k) => k === "AWS::CloudFormation::Stack" || k === "AWS::SecretsManager::Secret");
    if (params.has("KeepDataOnDelete")) throw new Error(`${prefix} is already one stack: nothing to migrate.`);
    if (params.has("SetupToken") || params.has("StatusId") || !nestedOnly) {
      throw new Error(`${prefix} is the earlier Launch Stack. Delete it in CloudFormation first (it keeps its boundary, deploy role and assets bucket, and CoderHelm keeps running), then run migrate again.`);
    }
    resume = true;
    log(`${prefix} exists from a migration that stopped before its update: carrying on.`);
  }
  // Another install whose name starts with this one's (coderhelm-prod-eu next to coderhelm-prod).
  const siblings = summaries.map((s) => s.StackName).filter((n) => n.startsWith(`${prefix}-`) && n.endsWith("-database") && n !== `${prefix}-database`).map((n) => n.slice(0, -"-database".length));
  const parts = partStacks(prefix, summaries, siblings);
  if (!parts.length) throw new Error(`No stacks named ${prefix}-* in ${region}: nothing to migrate.`);
  log(`Parts of ${prefix}: ${parts.map((p) => p.StackName).join(", ")}`);

  const bucket = `${prefix}-migrate-${account}`;
  if (!resume) {
  // 1. Their current templates, in a private bucket of the install's.
  try {
    await aws.write("s3api", "create-bucket", ["--bucket", bucket, ...(region === "us-east-1" ? [] : ["--create-bucket-configuration", `LocationConstraint=${region}`])]);
    await aws.write("s3api", "put-public-access-block", ["--bucket", bucket, "--public-access-block-configuration", "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"]);
  } catch (e) {
    if (!/BucketAlreadyOwnedByYou/.test(e.message)) throw e;
  }
  const nested = [];
  for (const s of parts) {
    const d = (await aws.read("cloudformation", "describe-stacks", ["--stack-name", s.StackName]))?.Stacks?.[0];
    const body = (await aws.read("cloudformation", "get-template", ["--stack-name", s.StackName, "--template-stage", "Original"]))?.TemplateBody;
    const key = `migrate/${s.StackName}.json`;
    const text = typeof body === "string" ? body : JSON.stringify(body);
    await ctx.putObject(bucket, key, text, region);
    nested.push({ logical: partName(s.StackName, prefix), stackId: d.StackId, name: s.StackName, parameters: d.Parameters ?? [], templateUrl: `https://${bucket}.s3.${region}.amazonaws.com/${key}` });
  }

  // 2. The parent, nesting them as they are.
  const t1 = importTemplate(nested);
  await ctx.putObject(bucket, "migrate/parent-1.json", JSON.stringify(t1), region);
  const toImport = nested.map((n) => ({ ResourceType: "AWS::CloudFormation::Stack", LogicalResourceId: n.logical, ResourceIdentifier: { StackId: n.stackId } }));
  log("Nesting the parts in the new parent stack (an import; nothing in them changes)");
  const i1 = await runChangeSet(ctx, aws, prefix, ["--change-set-name", "coderhelm-migrate-nest", "--change-set-type", "IMPORT", "--template-url", `https://${bucket}.s3.${region}.amazonaws.com/migrate/parent-1.json`, "--resources-to-import", JSON.stringify(toImport), "--capabilities", ...CAPABILITIES], "nest");
  await execute(ctx, aws, prefix, i1.id, "nesting the parts", "IMPORT_COMPLETE");

  // 3. The secret, imported into the parent.
  const secretName = `coderhelm/${stage}/secrets`;
  const sec = await aws.read("secretsmanager", "describe-secret", ["--secret-id", secretName]);
  if (sec?.ARN) {
    const t2 = importTemplate(nested, { secret: { name: secretName, description: sec.Description } });
    await ctx.putObject(bucket, "migrate/parent-2.json", JSON.stringify(t2), region);
    log("Adding the install's secret to the parent stack (an import)");
    const i2 = await runChangeSet(ctx, aws, prefix, ["--change-set-name", "coderhelm-migrate-secret", "--change-set-type", "IMPORT", "--template-url", `https://${bucket}.s3.${region}.amazonaws.com/migrate/parent-2.json`, "--resources-to-import", JSON.stringify([{ ResourceType: "AWS::SecretsManager::Secret", LogicalResourceId: "InstallSecret", ResourceIdentifier: { Id: sec.ARN } }]), "--capabilities", ...CAPABILITIES], "secret");
    await execute(ctx, aws, prefix, i2.id, "importing the secret", "IMPORT_COMPLETE");
  }
  }

  // 4. The update to the release, checked before it runs.
  const params = nativeParameters(cfg).map((p) => `ParameterKey=${p.ParameterKey},ParameterValue=${p.ParameterValue.replace(/,/g, "\\,")}`);
  log(`Preparing the update to ${templateUrl}`);
  const existing = await aws.read("cloudformation", "describe-change-set", ["--change-set-name", "coderhelm-migrate-update", "--stack-name", prefix]);
  if (existing?.ChangeSetId) await aws.write("cloudformation", "delete-change-set", ["--change-set-name", existing.ChangeSetId]);
  const up = await runChangeSet(ctx, aws, prefix, ["--change-set-name", "coderhelm-migrate-update", "--change-set-type", "UPDATE", "--template-url", templateUrl, "--include-nested-stacks", "--capabilities", ...CAPABILITIES, "--parameters", ...params], "update");
  // What CloudFormation fetched is the template checked above.
  const fetched = (await aws.read("cloudformation", "get-template", ["--stack-name", prefix, "--change-set-name", up.id, "--template-stage", "Original"]))?.TemplateBody;
  if (shaOf(typeof fetched === "string" ? JSON.parse(fetched) : fetched) !== shaOf(release)) {
    await aws.write("cloudformation", "delete-change-set", ["--change-set-name", up.id]);
    throw new Error("The template CloudFormation fetched is not the one checked against the signature: not migrating.");
  }
  const all = [{ ...up.cs, part: "parent" }];
  for (const c of up.cs.Changes ?? []) {
    const rc = c.ResourceChange ?? {};
    if (rc.ResourceType === "AWS::CloudFormation::Stack" && rc.ChangeSetId) {
      all.push({ ...(await aws.read("cloudformation", "describe-change-set", ["--change-set-name", rc.ChangeSetId])), part: rc.LogicalResourceId });
    }
  }
  const bad = blockers(all);
  if (bad.length) {
    await aws.write("cloudformation", "delete-change-set", ["--change-set-name", up.id]);
    const lines = bad.map((b) => `  ${b.part}.${b.id} (${b.type}): ${b.action}${b.replacement !== "False" ? `, replacement ${b.replacement}` : ""}`);
    const e = new Error(`Not migrated: the update would replace or remove data:\n${lines.join("\n")}\nNothing was changed except the nesting; the install keeps running. See docs/self-host/migrate.md ("If the check stops").`);
    e.blockers = bad;
    throw e;
  }
  const esm = replacedEventSources(all);
  // The release's part templates (to name the log groups the update adds).
  const templates = {};
  for (const f of release.Resources.ReleaseAssets.Properties.Files.filter((x) => x.Key.startsWith("stacks/"))) {
    templates[f.Key.slice("stacks/".length, -".json".length)] = await (await (ctx.fetchImpl ?? fetch)(templateUrl.replace(/coderhelm\.json$/, f.Key))).json();
  }
  const logs = [];
  for (const name of addedLogGroups(all, templates, stage)) {
    const found = (await aws.read("logs", "describe-log-groups", ["--log-group-name-prefix", name]))?.logGroups ?? [];
    if (found.some((g) => g.logGroupName === name)) logs.push(name);
  }
  if (logs.length) log(`Log groups Lambda made by itself, which the stack now owns (deleted just before the update, with their old lines): ${logs.join(", ")}`);
  log(`The check passed: no table, bucket, user pool, key or secret is replaced or removed (${all.length} change sets read).`);
  if (!opts.yes) return { ready: up.id, eventSources: esm, logGroups: logs };

  // 5. Run it.
  for (const uuid of esm) {
    log(`Removing event source mapping ${uuid} (the update makes it again)`);
    await aws.write("lambda", "delete-event-source-mapping", ["--uuid", uuid]);
  }
  for (const name of logs) await aws.write("logs", "delete-log-group", ["--log-group-name", name]);
  log("Updating to the native release (about 20 minutes)");
  await execute(ctx, aws, prefix, up.id, "the update", "UPDATE_COMPLETE");
  log(`${prefix} is one stack now. Run "coderhelm-selfhost leftovers --stage ${stage} --region ${region}" for the old Launch Stack's pieces (its boundary, deploy role and assets bucket) and the migration bucket ${bucket}.`);
  return { migrated: true, bucket };
}

export function runner() {
  return new Runner({});
}
