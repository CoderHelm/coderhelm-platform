/**
 * Turns the native synth (./config.ts) into a release: one parent template
 * (./parent.ts), one child template per part, and the code assets, each
 * with its sha256 (docs: DESIGN, "1. Release artifacts").
 *
 * For each child template:
 * - the stage, region and assets-bucket sentinels become `${Stage}`,
 *   `${AWS::Region}` and `${AssetsBucket}` (Fn::Sub); a sentinel anywhere
 *   intrinsics aren't allowed (keys, conditions) is an error;
 * - `Fn::ImportValue` of another part's export becomes a parameter the parent
 *   feeds with `Fn::GetAtt <Part>.Outputs.<Id>`; the producer keeps its
 *   export, so a migrated install never removes an export still in use;
 * - every Ref to a launch parameter becomes a declared parameter;
 * - data (tables, retained buckets, keys, the user pool) follows
 *   KeepDataOnDelete through DataDeletionPolicy / DataReplacePolicy, with
 *   the AWS::LanguageExtensions transform that allows Refs there; deletion
 *   protection follows the same switch; keys wait 7 days;
 * - the data tables' and buckets' names are outputs, for the parent's Sweep.
 */
import * as childProcess from "child_process";
import * as crypto from "crypto";
import * as fs from "fs";
import * as path from "path";
import { ASSETS_BUCKET_SENTINEL, REGION_SENTINEL, STAGE_SENTINEL } from "./config";

export type Json = any;

/** Launch parameters a child may Ref (the parent declares and resolves them). */
export const INSTALL_PARAMS = [
  "AdminEmail", "EmailFromResolved", "BedrockPrefixResolved", "EmailTransport", "ModelProvider",
  "UpdateDefaultsJson", "DeployRoleArn", "RootStackName",
];
const PSEUDO = new Set(["AWS::AccountId", "AWS::Region", "AWS::Partition", "AWS::URLSuffix", "AWS::StackName", "AWS::StackId", "AWS::NoValue", "AWS::NotificationARNs"]);

/** Types whose data follows KeepDataOnDelete (when CDK retained them). */
export const DATA_TYPES = ["AWS::DynamoDB::GlobalTable", "AWS::DynamoDB::Table", "AWS::S3::Bucket", "AWS::KMS::Key", "AWS::Cognito::UserPool"];

export const KEEP_CONDITION = "NativeKeepData";

export interface Child {
  /** The part's logical id in the parent, e.g. `LogAnalyzer`. */
  name: string;
  template: Json;
  /** Parameters it needs: install parameters and imports (param -> producer part, output id). */
  installParams: string[];
  imports: Record<string, { part: string; output: string }>;
  /** Parameters CDK declared itself (e.g. the web part's domain). */
  ownParams: string[];
  /** Parts it depends on (from the assembly). */
  dependsOn: string[];
  hasTables: boolean;
  hasBuckets: boolean;
}

/** `coderhelm-<sentinel>-log-analyzer` -> `LogAnalyzer`. */
export function partName(stackName: string): string {
  const short = stackName.replace(`coderhelm-${STAGE_SENTINEL}-`, "");
  return short.split("-").map((w) => w.charAt(0).toUpperCase() + w.slice(1)).join("");
}

const SENTINELS: [string, string, string | null][] = [
  [STAGE_SENTINEL, "${Stage}", "Stage"],
  [REGION_SENTINEL, "${AWS::Region}", null],
  [ASSETS_BUCKET_SENTINEL, "${AssetsBucket}", "AssetsBucket"],
];

function hasSentinel(s: string): boolean {
  return SENTINELS.some(([x]) => s.includes(x));
}

/** A string inside an Fn::Sub template: sentinels to variables. */
function subText(s: string, used: Set<string>): string {
  let out = s;
  for (const [x, v, p] of SENTINELS) {
    if (out.includes(x)) {
      out = out.split(x).join(v);
      if (p) used.add(p);
    }
  }
  return out;
}

/** A plain string: unchanged, or an Fn::Sub with `${` escaped first. */
function rewriteString(s: string, used: Set<string>): Json {
  if (!hasSentinel(s)) return s;
  return { "Fn::Sub": subText(s.split("${").join("${!"), used) };
}

export function findSentinels(v: Json, where = ""): string[] {
  const found: string[] = [];
  const walk = (x: Json, p: string) => {
    if (typeof x === "string") {
      if (hasSentinel(x)) found.push(p);
    } else if (Array.isArray(x)) x.forEach((y, i) => walk(y, `${p}[${i}]`));
    else if (x && typeof x === "object") {
      for (const [k, y] of Object.entries(x)) {
        if (hasSentinel(k)) found.push(`${p}/${k} (key)`);
        walk(y, `${p}/${k}`);
      }
    }
  };
  walk(v, where);
  return found;
}

export interface ExportRef {
  part: string;
  output: string;
}

/** Every export of every part, by its name as synthesized. */
export function exportMap(templates: Record<string, Json>): Record<string, ExportRef> {
  const out: Record<string, ExportRef> = {};
  for (const [part, t] of Object.entries(templates)) {
    for (const [id, o] of Object.entries((t.Outputs ?? {}) as Record<string, Json>)) {
      const name = o?.Export?.Name;
      if (typeof name === "string") out[name] = { part, output: id };
    }
  }
  return out;
}

function importParam(exportName: string): string {
  return `Imp${crypto.createHash("sha256").update(exportName).digest("hex").slice(0, 12)}`;
}

/**
 * CDK builds some logical ids from construct paths that hold the stack's
 * name (an event source for another part's queue): the sentinel becomes
 * "Install" there, and every Ref, GetAtt and DependsOn follows.
 */
export function renameLogicalIds(t: Json): Record<string, string> {
  const map: Record<string, string> = {};
  for (const id of Object.keys(t.Resources ?? {})) if (id.includes(STAGE_SENTINEL)) map[id] = id.split(STAGE_SENTINEL).join("Install");
  if (!Object.keys(map).length) return map;
  t.Resources = Object.fromEntries(Object.entries(t.Resources).map(([k, v]) => [map[k] ?? k, v]));
  const fix = (x: Json): Json => {
    if (Array.isArray(x)) return x.map(fix);
    if (!x || typeof x !== "object") return x;
    if (typeof x.Ref === "string" && map[x.Ref]) return { Ref: map[x.Ref] };
    if (Array.isArray(x["Fn::GetAtt"]) && map[x["Fn::GetAtt"][0]]) return { "Fn::GetAtt": [map[x["Fn::GetAtt"][0]], ...x["Fn::GetAtt"].slice(1)] };
    const out: Json = {};
    for (const [k, v] of Object.entries(x)) {
      if (k === "DependsOn") out[k] = Array.isArray(v) ? v.map((d: string) => map[d] ?? d) : (map[v as string] ?? v);
      else out[k] = fix(v);
    }
    return out;
  };
  t.Resources = fix(t.Resources);
  t.Outputs = fix(t.Outputs ?? {});
  return map;
}

export function transformChild(name: string, input: Json, known: Record<string, ExportRef>, dependsOn: string[] = []): Child {
  const t: Json = JSON.parse(JSON.stringify(input));
  renameLogicalIds(t);
  // The packager's own parameters count as declared (added below when used).
  const declared = new Set<string>([...Object.keys(t.Parameters ?? {}), ...Object.keys(t.Resources ?? {}), "DataDeletionPolicy", "DataReplacePolicy"]);
  const ownParams = Object.keys(t.Parameters ?? {});
  const used = new Set<string>();
  const install = new Set<string>();
  const imports: Record<string, ExportRef> = {};
  const problems: string[] = [];

  const rewrite = (x: Json): Json => {
    if (typeof x === "string") return rewriteString(x, used);
    if (Array.isArray(x)) return x.map(rewrite);
    if (!x || typeof x !== "object") return x;
    const keys = Object.keys(x);
    if (keys.length === 1) {
      const [k] = keys;
      const v = x[k];
      if (k === "Ref") {
        if (!declared.has(v) && !PSEUDO.has(v)) {
          if (INSTALL_PARAMS.includes(v)) install.add(v);
          else problems.push(`Ref to an unknown name ${v}`);
        }
        return x;
      }
      if (k === "Fn::GetAtt" || k === "Condition") return x;
      if (k === "Fn::ImportValue") {
        if (typeof v !== "string" || !known[v]) {
          problems.push(`import of an unknown export ${JSON.stringify(v)}`);
          return x;
        }
        if (known[v].part === name) problems.push(`${name} imports its own export ${v}`);
        const p = importParam(v);
        imports[p] = known[v];
        return { Ref: p };
      }
      if (k === "Fn::Sub") {
        if (typeof v === "string") return { "Fn::Sub": subText(v, used) };
        return { "Fn::Sub": [subText(v[0], used), rewrite(v[1])] };
      }
    }
    const out: Json = {};
    for (const [k, v] of Object.entries(x)) {
      if (hasSentinel(k)) problems.push(`a name with the install in it: ${k}`);
      out[k] = rewrite(v);
    }
    return out;
  };

  // Every function's log group is a resource of the stack, so Delete stack
  // removes it (CDK's own helpers, LogRetention, BucketDeployment and the
  // custom-resource providers, otherwise leave /aws/lambda/<name> behind).
  for (const [id, r] of Object.entries((t.Resources ?? {}) as Record<string, Json>)) {
    if (r.Type !== "AWS::Lambda::Function" || r.Properties?.LoggingConfig?.LogGroup) continue;
    const fn = r.Properties?.FunctionName;
    if (fn === undefined) throw new Error(`${name}.${id}: a function without a name (the install's names make its log group findable)`);
    const logId = `${id}Logs`.slice(0, 255);
    t.Resources[logId] = {
      Type: "AWS::Logs::LogGroup",
      Metadata: { "coderhelm:native-log": true },
      Properties: { LogGroupName: { "Fn::Join": ["", ["/aws/lambda/", fn]] }, RetentionInDays: 90 },
    };
    r.Properties.LoggingConfig = { ...(r.Properties.LoggingConfig ?? {}), LogGroup: { Ref: logId } };
    declared.add(logId);
  }

  // Data follows KeepDataOnDelete.
  const tables: Json[] = [];
  const buckets: Json[] = [];
  let usesPolicy = false;
  for (const [id, r] of Object.entries((t.Resources ?? {}) as Record<string, Json>)) {
    if (r.Metadata?.["coderhelm:native-log"]) r.Metadata = { "coderhelm:native-log": true };
    else delete r.Metadata;
    if (r.Type === "AWS::KMS::Key") r.Properties = { ...r.Properties, PendingWindowInDays: 7 };
    const retained = r.DeletionPolicy === "Retain" || r.DeletionPolicy === "RetainExceptOnCreate";
    const isData = DATA_TYPES.includes(r.Type) && (retained || r.Type.startsWith("AWS::DynamoDB::") || r.Type === "AWS::Cognito::UserPool" || r.Type === "AWS::KMS::Key");
    if (!isData) continue;
    usesPolicy = true;
    r.DeletionPolicy = { Ref: "DataDeletionPolicy" };
    r.UpdateReplacePolicy = { Ref: "DataReplacePolicy" };
    const keep = (a: Json, b: Json) => ({ "Fn::If": [KEEP_CONDITION, a, b] });
    if (r.Type === "AWS::DynamoDB::GlobalTable") {
      tables.push({ Ref: id });
      for (const rep of r.Properties?.Replicas ?? []) if ("DeletionProtectionEnabled" in rep) rep.DeletionProtectionEnabled = keep(true, false);
    }
    if (r.Type === "AWS::DynamoDB::Table") {
      tables.push({ Ref: id });
      if ("DeletionProtectionEnabled" in (r.Properties ?? {})) r.Properties.DeletionProtectionEnabled = keep(true, false);
    }
    if (r.Type === "AWS::Cognito::UserPool" && r.Properties?.DeletionProtection) r.Properties.DeletionProtection = keep("ACTIVE", "INACTIVE");
    if (r.Type === "AWS::S3::Bucket") buckets.push({ Ref: id });
  }

  const out: Json = { AWSTemplateFormatVersion: "2010-09-09" };
  if (usesPolicy) out.Transform = "AWS::LanguageExtensions";
  out.Description = String(t.Description ?? `CoderHelm: the ${name} part`).split(STAGE_SENTINEL).join("the install");
  const conditions = t.Conditions ? JSON.parse(JSON.stringify(t.Conditions)) : {};
  for (const p of findSentinels(conditions, "Conditions")) problems.push(`the install's name in a condition: ${p}`);
  // Conditions may Ref launch parameters too (they are declared like any other).
  const refs = (x: Json): void => {
    if (Array.isArray(x)) x.forEach(refs);
    else if (x && typeof x === "object") {
      if (typeof x.Ref === "string" && !declared.has(x.Ref) && !PSEUDO.has(x.Ref)) {
        if (INSTALL_PARAMS.includes(x.Ref)) install.add(x.Ref);
        else problems.push(`a condition Refs an unknown name ${x.Ref}`);
      }
      Object.values(x).forEach(refs);
    }
  };
  refs(conditions);
  const resources = rewrite(t.Resources ?? {});
  const outputs = rewrite(t.Outputs ?? {});
  if (tables.length) outputs.NativeDataTables = { Value: { "Fn::Join": [",", tables] } };
  if (buckets.length) outputs.NativeDataBuckets = { Value: { "Fn::Join": [",", buckets] } };
  if (JSON.stringify(resources).includes(`"${KEEP_CONDITION}"`)) conditions[KEEP_CONDITION] = { "Fn::Equals": [{ Ref: "DataReplacePolicy" }, "Retain"] };
  const params: Json = { ...(t.Parameters ?? {}) };
  if (used.has("Stage")) params.Stage = { Type: "String" };
  if (used.has("AssetsBucket")) params.AssetsBucket = { Type: "String" };
  if (usesPolicy) {
    params.DataDeletionPolicy = { Type: "String", AllowedValues: ["RetainExceptOnCreate", "Delete"] };
    params.DataReplacePolicy = { Type: "String", AllowedValues: ["Retain", "Delete"] };
  }
  for (const p of [...install].sort()) params[p] = { Type: "String" };
  for (const p of Object.keys(imports).sort()) params[p] = { Type: "String" };
  out.Parameters = params;
  if (Object.keys(conditions).length) out.Conditions = conditions;
  out.Resources = resources;
  if (Object.keys(outputs).length) out.Outputs = outputs;
  for (const p of findSentinels(out)) problems.push(`sentinel left at ${p}`);
  if (problems.length) throw new Error(`${name}: ${problems.join("; ")}`);
  return {
    name,
    template: out,
    installParams: [...install].sort(),
    imports,
    ownParams,
    dependsOn,
    hasTables: tables.length > 0,
    hasBuckets: buckets.length > 0,
  };
}

export interface Asset {
  /** Where the release publishes it, relative to launch/<version>/. */
  key: string;
  /** Where the install keeps it in its own bucket. */
  dest: string;
  file: string;
  sha256: string;
}

export function sha256File(file: string): string {
  return crypto.createHash("sha256").update(fs.readFileSync(file)).digest("hex");
}

/** Zips a directory (sorted, no extra attributes) with the system zip. */
export function zipDir(dir: string, out: string): void {
  fs.rmSync(out, { force: true });
  const files = childProcess.execFileSync("find", [".", "-type", "f"], { cwd: dir, encoding: "utf8" }).split("\n").filter(Boolean).sort();
  childProcess.execFileSync("zip", ["-X", "-q", "-D", out, "-@"], { cwd: dir, input: files.join("\n") });
}

export interface Assembly {
  children: Child[];
  assets: Asset[];
}

/** Reads a synthesized assembly: the children, transformed, and the assets, packaged into outDir/assets. */
export function readAssembly(assemblyDir: string, outDir: string): Assembly {
  const manifest = JSON.parse(fs.readFileSync(path.join(assemblyDir, "manifest.json"), "utf8"));
  const stacks: { id: string; name: string; template: Json; deps: string[] }[] = [];
  const assets = new Map<string, Asset>();
  fs.mkdirSync(path.join(outDir, "assets"), { recursive: true });
  for (const [id, a] of Object.entries(manifest.artifacts as Record<string, Json>)) {
    if (a.type !== "aws:cloudformation:stack") continue;
    const template = JSON.parse(fs.readFileSync(path.join(assemblyDir, a.properties.templateFile), "utf8"));
    stacks.push({ id, name: partName(id), template, deps: (a.dependencies ?? []).filter((d: string) => !d.endsWith(".assets")) });
  }
  for (const f of fs.readdirSync(assemblyDir).filter((x) => x.endsWith(".assets.json"))) {
    const m = JSON.parse(fs.readFileSync(path.join(assemblyDir, f), "utf8"));
    for (const [, a] of Object.entries((m.files ?? {}) as Record<string, Json>)) {
      const dest = (Object.values(a.destinations)[0] as Json).objectKey as string;
      if (!dest.startsWith("assets/")) continue; // the templates themselves
      if (assets.has(dest)) continue;
      const src = path.join(assemblyDir, a.source.path);
      const out = path.join(outDir, dest);
      if (a.source.packaging === "zip" && fs.statSync(src).isDirectory()) zipDir(src, out);
      else fs.copyFileSync(src, out);
      assets.set(dest, { key: dest, dest, file: out, sha256: sha256File(out) });
    }
  }
  const templates = Object.fromEntries(stacks.map((s) => [s.name, s.template]));
  const known = exportMap(templates);
  const children = stacks.map((s) => transformChild(s.name, s.template, known, s.deps.map(partName)));
  return { children, assets: [...assets.values()].sort((a, b) => a.key.localeCompare(b.key)) };
}

/** CloudFormation's limits (nested stacks keep each template under them). */
export const LIMITS = { templateBytes: 1_000_000, resources: 500, parameters: 200, outputs: 200 };

export function checkLimits(name: string, t: Json): string[] {
  const p: string[] = [];
  const bytes = Buffer.byteLength(JSON.stringify(t));
  if (bytes > LIMITS.templateBytes) p.push(`${name}: ${bytes} bytes (limit ${LIMITS.templateBytes})`);
  const n = Object.keys(t.Resources ?? {}).length;
  if (n > LIMITS.resources) p.push(`${name}: ${n} resources (limit ${LIMITS.resources})`);
  if (Object.keys(t.Parameters ?? {}).length > LIMITS.parameters) p.push(`${name}: too many parameters`);
  if (Object.keys(t.Outputs ?? {}).length > LIMITS.outputs) p.push(`${name}: too many outputs`);
  return p;
}
