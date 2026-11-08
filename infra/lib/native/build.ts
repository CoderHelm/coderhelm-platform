/**
 * Builds a native release (docs: DESIGN):
 *
 *   node dist/lib/native/build.js <version> <outDir>
 *
 * with GATEWAY_ZIP, WORKER_ZIP, MCP_PROXY_DIR and CODERHELM_DASHBOARD_DIR
 * set as for any synth, and CODERHELM_COMMIT / CODERHELM_RELEASED_AT for the
 * release's stamp (the release workflow sets them from selfhost/release.json;
 * without them, the commit is the version's tag in this checkout, or HEAD,
 * and the release date is now). Writes, under outDir (the layout of
 * coderhelm-public/launch/<version>/):
 *
 *   coderhelm.json          the parent template (Launch Stack opens it)
 *   access.json             the access stack's template (an AWS admin applies
 *                           newer boundary and deploy role permissions with it)
 *   stacks/<Part>.json      one per part
 *   assets/<hash>.zip       the code, by CDK's content hash
 *   files.txt               every file, relative, for SHA256SUMS
 */
import * as cdk from "aws-cdk-lib";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { execFileSync } from "child_process";
import { buildApp } from "../app";
import { repoPath } from "../paths";
import { nativeConfig } from "./config";
import { Json, checkLimits, readAssembly, sha256File } from "./package";
import { buildAccess } from "./access-stack";
import { ACCESS_FILE, buildParent, childKey } from "./parent";

export interface NativeRelease {
  parent: Json;
  children: Record<string, Json>;
  files: string[];
  problems: string[];
}

/** The commit a release is built from: its tag in this checkout, else HEAD, else unknown. */
function gitCommit(version: string): string | null {
  for (const ref of [`${version}^{commit}`, "HEAD"]) {
    try {
      const out = execFileSync("git", ["-C", repoPath(), "rev-parse", "--verify", "--quiet", ref], { encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] }).trim();
      if (/^[0-9a-f]{40}$/.test(out)) return out;
    } catch {
      // not a git checkout, or no such ref
    }
  }
  return null;
}

/**
 * The stamp every service of a native install carries (infra/lib/release.ts):
 * the version, the commit and the release date. A native release has no
 * selfhost/release.json in its checkout (the workflow writes that file into
 * the source archive), so all three are set here, or an install would report
 * its version alone.
 */
export function nativeStamp(version: string, opts: { commit?: string; releasedAt?: string } = {}, env: NodeJS.ProcessEnv = process.env, now = new Date()): Record<string, string> {
  const commit = opts.commit || env.CODERHELM_COMMIT || gitCommit(version);
  const releasedAt = opts.releasedAt || env.CODERHELM_RELEASED_AT || now.toISOString().replace(/\.\d{3}Z$/, "Z");
  return { CODERHELM_VERSION: version, ...(commit ? { CODERHELM_COMMIT: commit } : {}), CODERHELM_RELEASED_AT: releasedAt };
}

export function buildNative(version: string, outDir: string, opts: { dashboardDir?: string; assemblyDir?: string; commit?: string; releasedAt?: string } = {}): NativeRelease {
  if (!/^v\d+\.\d+\.\d+(-[0-9A-Za-z.-]+)?$/.test(version)) throw new Error(`${version} is not a release version`);
  const assemblyDir = opts.assemblyDir ?? fs.mkdtempSync(path.join(os.tmpdir(), "chnative-"));
  const stamp = nativeStamp(version, opts);
  const keys = ["CODERHELM_VERSION", "CODERHELM_COMMIT", "CODERHELM_RELEASED_AT"];
  const prev = Object.fromEntries(keys.map((k) => [k, process.env[k]]));
  for (const k of keys) {
    if (stamp[k]) process.env[k] = stamp[k];
    else delete process.env[k];
  }
  try {
    const app = new cdk.App({ outdir: assemblyDir, analyticsReporting: false });
    buildApp(app, nativeConfig({ dashboardDir: opts.dashboardDir ?? process.env.CODERHELM_DASHBOARD_DIR }));
    app.synth();
  } finally {
    for (const k of keys) {
      if (prev[k] === undefined) delete process.env[k];
      else process.env[k] = prev[k];
    }
  }
  fs.rmSync(outDir, { recursive: true, force: true });
  fs.mkdirSync(path.join(outDir, "stacks"), { recursive: true });
  const { children, assets } = readAssembly(assemblyDir, outDir);
  const childSha: Record<string, string> = {};
  const out: Record<string, Json> = {};
  const problems: string[] = [];
  for (const c of children) {
    const file = path.join(outDir, childKey(version, c.name).key);
    fs.writeFileSync(file, JSON.stringify(c.template));
    childSha[c.name] = sha256File(file);
    out[c.name] = c.template;
    problems.push(...checkLimits(c.name, c.template));
  }
  const helperCode = fs.readFileSync(repoPath("lambda/native-helper/handler.py"), "utf8");
  const access = buildAccess(version);
  fs.writeFileSync(path.join(outDir, ACCESS_FILE), JSON.stringify(access, null, 1));
  problems.push(...checkLimits(ACCESS_FILE, access));
  const accessSha = sha256File(path.join(outDir, ACCESS_FILE));
  const parent = buildParent({ version, children, assets, childSha, helperCode, accessSha });
  fs.writeFileSync(path.join(outDir, "coderhelm.json"), JSON.stringify(parent, null, 1));
  problems.push(...checkLimits("coderhelm.json", parent));
  const files = ["coderhelm.json", ACCESS_FILE, ...children.map((c) => childKey(version, c.name).key), ...assets.map((a) => a.key)];
  fs.writeFileSync(path.join(outDir, "files.txt"), files.join("\n") + "\n");
  return { parent, children: out, files, problems };
}

if (require.main === module) {
  const [version, outDir] = process.argv.slice(2);
  const r = buildNative(version, path.resolve(outDir));
  if (r.problems.length) {
    console.error(r.problems.join("\n"));
    process.exit(1);
  }
  console.log(`${r.files.length} files in ${outDir}`);
}
