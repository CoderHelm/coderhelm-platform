// Releases: which version to install, and its files. A release is a tag
// (vX.Y.Z) on GitHub with these assets, built by .github/workflows/release.yml:
//
//   gateway.zip, worker.zip     the Rust Lambdas (bootstrap), ARM64
//   mcp-proxy.zip               the MCP proxy Lambda
//   dashboard.tar.gz            the dashboard (static export, no API URL baked in)
//   source.tar.gz               this repository at the tag (the CDK app)
//   SHA256SUMS                  checksums of all of the above
//
// Every file is checked against SHA256SUMS before use. The release is also
// signed with GitHub artifact attestations (build provenance); `verify`
// checks that with the GitHub CLI when it is installed.
import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import { REPO, sha256File } from "./util.mjs";

export const ASSETS = ["gateway.zip", "worker.zip", "mcp-proxy.zip", "dashboard.tar.gz", "source.tar.gz"];

export function releasesDir() {
  return process.env.CODERHELM_HOME
    ? path.join(process.env.CODERHELM_HOME, "releases")
    : path.join(os.homedir(), ".coderhelm", "releases");
}

/** The latest release tag, from GitHub. */
export async function latestVersion(fetchImpl = fetch) {
  const r = await fetchImpl(`https://api.github.com/repos/${REPO}/releases/latest`, {
    headers: { accept: "application/vnd.github+json", "user-agent": "coderhelm-selfhost" },
  });
  if (!r.ok) throw new Error(`Could not look up the latest release (GitHub answered ${r.status}).`);
  const j = await r.json();
  return j.tag_name;
}

/** Parses SHA256SUMS ("<hex>  <file>" per line). */
export function parseSums(text) {
  const out = {};
  for (const line of text.split("\n")) {
    const m = line.trim().match(/^([0-9a-f]{64})\s+\*?(.+)$/);
    if (m) out[m[2].trim()] = m[1];
  }
  return out;
}

/** Throws unless every asset matches its checksum. */
export function verifySums(dir, sums, names = ASSETS) {
  for (const n of names) {
    if (!sums[n]) throw new Error(`SHA256SUMS has no entry for ${n}`);
    const got = sha256File(path.join(dir, n));
    if (got !== sums[n]) throw new Error(`${n} does not match its checksum (got ${got}, expected ${sums[n]}). Delete ${dir} and try again.`);
  }
}

/**
 * Downloads a release into ~/.coderhelm/releases/<tag> (once), verifies
 * it, and unpacks the source and dashboard. Returns the paths deploy needs.
 */
export async function fetchRelease(tag, { runner, fetchImpl = fetch, log = console.log }) {
  const dir = path.join(releasesDir(), tag);
  const done = path.join(dir, ".ready");
  const paths = releasePaths(dir);
  if (runner.dryRun) {
    log(`  [dry run] download release ${tag} into ${dir} and check SHA256SUMS`);
    return paths;
  }
  if (fs.existsSync(done)) return paths;
  fs.mkdirSync(dir, { recursive: true });
  const base = `https://github.com/${REPO}/releases/download/${tag}`;
  for (const n of ["SHA256SUMS", ...ASSETS]) {
    log(`  downloading ${n}`);
    const r = await fetchImpl(`${base}/${n}`, { redirect: "follow" });
    if (!r.ok) throw new Error(`Could not download ${n} for ${tag} (${r.status}). Is ${tag} a CoderHelm release?`);
    fs.writeFileSync(path.join(dir, n), Buffer.from(await r.arrayBuffer()));
  }
  verifySums(dir, parseSums(fs.readFileSync(path.join(dir, "SHA256SUMS"), "utf8")));
  fs.mkdirSync(paths.source, { recursive: true });
  fs.mkdirSync(paths.dashboard, { recursive: true });
  await runner.run("tar", ["-xzf", path.join(dir, "source.tar.gz"), "-C", paths.source, "--strip-components=1"]);
  await runner.run("tar", ["-xzf", path.join(dir, "dashboard.tar.gz"), "-C", paths.dashboard]);
  fs.writeFileSync(done, new Date().toISOString());
  return paths;
}

export function releasePaths(dir) {
  return {
    dir,
    source: path.join(dir, "source"),
    infra: path.join(dir, "source", "infra"),
    gatewayZip: path.join(dir, "gateway.zip"),
    workerZip: path.join(dir, "worker.zip"),
    mcpProxyZip: path.join(dir, "mcp-proxy.zip"),
    dashboard: path.join(dir, "dashboard"),
  };
}

/** Checks the release's build provenance with `gh attestation verify`, when gh is there. */
export async function verifyAttestation(paths, runner, log = console.log) {
  const gh = await runner.run("gh", ["--version"], { capture: true, allowFail: true }).catch(() => null);
  if (!gh || gh.code !== 0) {
    log("  (GitHub CLI not found: checksums verified, provenance not checked)");
    return false;
  }
  for (const n of ["gateway.zip", "worker.zip"]) {
    await runner.run("gh", ["attestation", "verify", path.join(paths.dir, n), "--repo", REPO], { capture: true });
  }
  log("  provenance verified (built by this repository's release workflow)");
  return true;
}
