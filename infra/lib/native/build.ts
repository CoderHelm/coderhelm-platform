/**
 * Builds a native release (docs: DESIGN):
 *
 *   node dist/lib/native/build.js <version> <outDir>
 *
 * with GATEWAY_ZIP, WORKER_ZIP, MCP_PROXY_DIR and CODERHELM_DASHBOARD_DIR
 * set as for any synth. Writes, under outDir (the layout of
 * coderhelm-public/launch/<version>/):
 *
 *   coderhelm.json          the parent template (Launch Stack opens it)
 *   stacks/<Part>.json      one per part
 *   assets/<hash>.zip       the code, by CDK's content hash
 *   files.txt               every file, relative, for SHA256SUMS
 */
import * as cdk from "aws-cdk-lib";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { buildApp } from "../app";
import { repoPath } from "../paths";
import { nativeConfig } from "./config";
import { Json, checkLimits, readAssembly, sha256File } from "./package";
import { buildParent, childKey } from "./parent";

export interface NativeRelease {
  parent: Json;
  children: Record<string, Json>;
  files: string[];
  problems: string[];
}

export function buildNative(version: string, outDir: string, opts: { dashboardDir?: string; assemblyDir?: string } = {}): NativeRelease {
  if (!/^v\d+\.\d+\.\d+(-[0-9A-Za-z.-]+)?$/.test(version)) throw new Error(`${version} is not a release version`);
  const assemblyDir = opts.assemblyDir ?? fs.mkdtempSync(path.join(os.tmpdir(), "chnative-"));
  const prev = process.env.CODERHELM_VERSION;
  process.env.CODERHELM_VERSION = version;
  try {
    const app = new cdk.App({ outdir: assemblyDir, analyticsReporting: false });
    buildApp(app, nativeConfig({ dashboardDir: opts.dashboardDir ?? process.env.CODERHELM_DASHBOARD_DIR }));
    app.synth();
  } finally {
    if (prev === undefined) delete process.env.CODERHELM_VERSION;
    else process.env.CODERHELM_VERSION = prev;
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
  const parent = buildParent({ version, children, assets, childSha, helperCode });
  fs.writeFileSync(path.join(outDir, "coderhelm.json"), JSON.stringify(parent, null, 1));
  problems.push(...checkLimits("coderhelm.json", parent));
  const files = ["coderhelm.json", ...children.map((c) => childKey(version, c.name).key), ...assets.map((a) => a.key)];
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
