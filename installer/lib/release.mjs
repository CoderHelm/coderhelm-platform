// Releases: which version to install, and its files. A release is a tag
// (vX.Y.Z) on GitHub with these assets, built by .github/workflows/release.yml:
//
//   gateway.zip, worker.zip     the Rust Lambdas (bootstrap), ARM64
//   mcp-proxy.zip               the MCP proxy Lambda
//   dashboard.tar.gz            the dashboard (static export, no API URL baked in)
//   source.tar.gz               this repository at the tag (the CDK app)
//   SHA256SUMS                  checksums of all of the above
//   SHA256SUMS.sigstore.json    cosign's keyless signature of SHA256SUMS,
//                               made by this repository's release workflow
//
// Every file is checked against SHA256SUMS before use, and SHA256SUMS
// against its signature (cosign) or the files' GitHub build attestations
// (gh) when either tool is installed. `--require-signature` (the update
// build always passes it) refuses a release that can't be verified.
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
  // The signature is optional to download; verifySignature decides if it is needed.
  const sig = await fetchImpl(`${base}/${SIGNATURE}`, { redirect: "follow" }).catch(() => null);
  if (sig?.ok) fs.writeFileSync(path.join(dir, SIGNATURE), Buffer.from(await sig.arrayBuffer()));
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

export const SIGNATURE = "SHA256SUMS.sigstore.json";
/** Who may have signed a release: this repository's release workflow, on a tag. */
export const SIGNER_IDENTITY = `^https://github.com/${REPO}/.github/workflows/release.yml@refs/tags/v`;
export const SIGNER_ISSUER = "https://token.actions.githubusercontent.com";

const has = async (runner, cmd, args) => {
  const r = await runner.run(cmd, args, { capture: true, allowFail: true }).catch(() => null);
  return !!r && r.code === 0;
};

/**
 * Checks that the release was built and signed by this repository's release
 * workflow: SHA256SUMS against its cosign signature, or the artifacts'
 * GitHub attestations. Returns how it was verified, or null when no tool
 * was there; throws on a bad signature, and when `require` is set and it
 * couldn't be checked.
 */
export async function verifySignature(paths, runner, { require = false, log = console.log } = {}) {
  const sigFile = path.join(paths.dir, SIGNATURE);
  if (fs.existsSync(sigFile) && (await has(runner, "cosign", ["version"]))) {
    const r = await runner.run("cosign", [
      "verify-blob", "--bundle", sigFile,
      "--certificate-identity-regexp", SIGNER_IDENTITY,
      "--certificate-oidc-issuer", SIGNER_ISSUER,
      path.join(paths.dir, "SHA256SUMS"),
    ], { capture: true, allowFail: true });
    if (r.code !== 0) throw new Error(`The release's signature does not verify: not installing it.${r.stderr ? `\n${r.stderr.trim()}` : ""}`);
    log("  signature verified (cosign: signed by this repository's release workflow)");
    return "cosign";
  }
  if (await has(runner, "gh", ["--version"])) {
    for (const n of ["gateway.zip", "worker.zip", "source.tar.gz"]) {
      const r = await runner.run("gh", ["attestation", "verify", path.join(paths.dir, n), "--repo", REPO], { capture: true, allowFail: true });
      if (r.code !== 0) throw new Error(`${n} has no valid build attestation from ${REPO}: not installing it.`);
    }
    log("  provenance verified (gh: built by this repository's release workflow)");
    return "attestation";
  }
  if (require) {
    throw new Error("Can't verify the release's signature (install cosign or the GitHub CLI), and --require-signature is set: not installing it.");
  }
  log("  (cosign and gh not found: checksums verified, signature not checked)");
  return null;
}
