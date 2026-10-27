// Releases: which version to install, and its files. A release is a tag
// (vX.Y.Z) on GitHub with these assets, built by .github/workflows/release.yml:
//
//   gateway.zip, worker.zip     the Rust Lambdas (bootstrap), ARM64
//   mcp-proxy.zip               the MCP proxy Lambda
//   dashboard.tar.gz            the dashboard (static export, no API URL baked in)
//   source.tar.gz               this repository at the tag (the CDK app)
//   models.json                 the models the release calls (newer releases)
//   SHA256SUMS                  checksums of all of the above
//   SHA256SUMS.sigstore.json    cosign's keyless signature of SHA256SUMS,
//                               made by this repository's release workflow
//
// Nothing from a release is used until it checks out: its tag must be an
// annotated tag signed with a maintainer key pinned in selfhost/maintainers.json,
// on a commit on main (lib/source-tag.mjs); every file must match SHA256SUMS;
// and SHA256SUMS must carry a signature made by exactly this repository's
// release.yml, on the release's own tag, from that same commit, with a GitHub
// Actions certificate logged in Rekor (cosign), or else every file must carry
// a GitHub build attestation from that same workflow, tag and commit (gh).
// No git, no cosign and no gh, no bundle, or any mismatch: the release is refused.
// There is no switch that skips this; deploy from a checkout (--source) to
// run code that isn't a signed release.
import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import { REPO, sha256File } from "./util.mjs";
import { checkBundleIdentity } from "./sigstore.mjs";
import { verifySourceTag } from "./source-tag.mjs";

export const ASSETS = ["gateway.zip", "worker.zip", "mcp-proxy.zip", "dashboard.tar.gz", "source.tar.gz"];

export function releasesDir() {
  return process.env.CODERHELM_HOME
    ? path.join(process.env.CODERHELM_HOME, "releases")
    : path.join(os.homedir(), ".coderhelm", "releases");
}

/** A release tag: vX.Y.Z, or vX.Y.Z-pre (also selfhost/launch/stamp-template.mjs). */
export const TAG_RE = /^v\d+\.\d+\.\d+(-[0-9A-Za-z.-]+)?$/;

export function checkTag(tag) {
  if (typeof tag !== "string" || !TAG_RE.test(tag)) throw new Error(`${JSON.stringify(tag)} is not a CoderHelm release (vX.Y.Z).`);
  return tag;
}

/** The latest release tag, from GitHub. */
export async function latestVersion(fetchImpl = fetch) {
  const r = await fetchImpl(`https://api.github.com/repos/${REPO}/releases/latest`, {
    headers: { accept: "application/vnd.github+json", "user-agent": "coderhelm-selfhost" },
  });
  if (!r.ok) throw new Error(`Could not look up the latest release (GitHub answered ${r.status}).`);
  const j = await r.json();
  if (j.draft || j.prerelease) throw new Error("GitHub's latest release is a draft or a pre-release: pass --to vX.Y.Z.");
  return checkTag(j.tag_name);
}

/**
 * Where release files may come from: GitHub's release download URL for this
 * repository, over HTTPS, and the hosts GitHub redirects those downloads to.
 */
export const DOWNLOAD_HOSTS = ["github.com", "objects.githubusercontent.com", "release-assets.githubusercontent.com"];

export function releaseUrl(tag, name) {
  return `https://github.com/${REPO}/releases/download/${checkTag(tag)}/${name}`;
}

/** Throws unless a (possibly redirected) download came from an expected HTTPS host. */
export function checkDownloadUrl(url) {
  let u;
  try {
    u = new URL(url);
  } catch {
    throw new Error(`Refusing a download from ${url}: not a URL.`);
  }
  if (u.protocol !== "https:" || !DOWNLOAD_HOSTS.includes(u.hostname) || u.username || u.password || u.port) {
    throw new Error(`Refusing a download from ${u.origin}: releases come only from GitHub over HTTPS.`);
  }
  if (u.hostname === "github.com" && !u.pathname.startsWith(`/${REPO}/releases/download/`)) {
    throw new Error(`Refusing a download from ${u.href}: not a ${REPO} release.`);
  }
  return u.href;
}

async function download(fetchImpl, tag, name) {
  const url = releaseUrl(tag, name);
  const r = await fetchImpl(url, { redirect: "follow" });
  if (!r.ok) throw new Error(`Could not download ${name} for ${tag} (${r.status}). Is ${tag} a CoderHelm release?`);
  checkDownloadUrl(r.url || url);
  return Buffer.from(await r.arrayBuffer());
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
  checkTag(tag);
  const dir = path.join(releasesDir(), tag);
  const done = path.join(dir, ".ready");
  const paths = releasePaths(dir);
  if (runner.dryRun) {
    log(`  [dry run] download release ${tag} into ${dir} and check SHA256SUMS`);
    return paths;
  }
  // A release downloaded before is checked again: its files may have changed since.
  if (fs.existsSync(done)) {
    const sums = parseSums(fs.readFileSync(path.join(dir, "SHA256SUMS"), "utf8"));
    verifySums(dir, sums);
    const { method, commit } = await verifySignature(paths, runner, { tag, log });
    paths.models = await releaseModels(dir, sums, { runner, fetchImpl, tag, method, commit, log });
    return paths;
  }
  fs.rmSync(dir, { recursive: true, force: true });
  fs.mkdirSync(dir, { recursive: true });
  for (const n of ["SHA256SUMS", SIGNATURE, ...ASSETS]) {
    log(`  downloading ${n}`);
    fs.writeFileSync(path.join(dir, n), await download(fetchImpl, tag, n));
  }
  // Checked before anything is unpacked or run.
  const sums = parseSums(fs.readFileSync(path.join(dir, "SHA256SUMS"), "utf8"));
  verifySums(dir, sums);
  const { method, commit } = await verifySignature(paths, runner, { tag, log });
  paths.models = await releaseModels(dir, sums, { runner, fetchImpl, tag, method, commit, log });
  fs.mkdirSync(paths.source, { recursive: true });
  fs.mkdirSync(paths.dashboard, { recursive: true });
  await runner.run("tar", ["-xzf", path.join(dir, "source.tar.gz"), "-C", paths.source, "--strip-components=1"]);
  await runner.run("tar", ["-xzf", path.join(dir, "dashboard.tar.gz"), "-C", paths.dashboard]);
  fs.writeFileSync(done, new Date().toISOString());
  return paths;
}

export const MODELS = "models.json";

/**
 * The release's model manifest (models.json: the models it calls, with their
 * Bedrock ids per inference profile prefix), read only after the signature
 * check passed, only when the signed SHA256SUMS lists it, and only when it
 * matches its checksum (verified with gh, it must carry its own build
 * attestation too). null for a signed release that lists none (releases
 * from before model manifests). Throws on any mismatch.
 */
export async function releaseModels(dir, sums, { runner, fetchImpl = fetch, tag, method, commit, log = console.log }) {
  if (!method) throw new Error("The release's signature wasn't checked: not reading its model list.");
  if (!sums[MODELS]) return null;
  const file = path.join(dir, MODELS);
  if (!fs.existsSync(file)) {
    log(`  downloading ${MODELS}`);
    fs.writeFileSync(file, await download(fetchImpl, tag, MODELS));
  }
  verifySums(dir, sums, [MODELS]);
  if (method === "attestation") {
    const r = await runner.run("gh", attestationArgs(tag, file, commit), { capture: true, allowFail: true });
    if (r.code !== 0) throw new Error(`${MODELS} has no valid build attestation from ${SIGNER_WORKFLOW} on ${tag}: not using it.`);
  }
  let m;
  try {
    m = JSON.parse(fs.readFileSync(file, "utf8"));
  } catch {
    throw new Error(`${MODELS} of ${tag} isn't valid JSON.`);
  }
  if (!m || !Array.isArray(m.calls) || (m.version !== undefined && m.version !== tag)) {
    throw new Error(`${MODELS} of ${tag} isn't a model list for this release.`);
  }
  return m;
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
export const SIGNER_ISSUER = "https://token.actions.githubusercontent.com";
export const SIGNER_WORKFLOW = `${REPO}/.github/workflows/release.yml`;

/** The one certificate identity a release's signature may have: release.yml, run on that release's tag. */
export function signerIdentity(tag) {
  return `https://github.com/${SIGNER_WORKFLOW}@refs/tags/${checkTag(tag)}`;
}

const checkCommit = (commit) => {
  if (!/^[0-9a-f]{40}$/.test(String(commit))) throw new Error("no source commit to check the release against");
  return commit;
};

/** cosign's arguments: the exact identity, issuer, repository, ref and commit (and Rekor, which cosign requires by default). */
export function cosignArgs(tag, bundle, file, commit) {
  return [
    "verify-blob", "--bundle", bundle,
    "--certificate-identity", signerIdentity(tag),
    "--certificate-oidc-issuer", SIGNER_ISSUER,
    "--certificate-github-workflow-repository", REPO,
    "--certificate-github-workflow-ref", `refs/tags/${tag}`,
    "--certificate-github-workflow-sha", checkCommit(commit),
    file,
  ];
}

/** gh's arguments: the attestation must come from release.yml in this repository, on the tag and commit, on GitHub's runners. */
export function attestationArgs(tag, file, commit) {
  return [
    "attestation", "verify", file,
    "--repo", REPO,
    "--signer-workflow", SIGNER_WORKFLOW,
    "--source-ref", `refs/tags/${checkTag(tag)}`,
    "--source-digest", checkCommit(commit),
    "--cert-oidc-issuer", SIGNER_ISSUER,
    "--deny-self-hosted-runners",
  ];
}

const has = async (runner, cmd, args) => {
  const r = await runner.run(cmd, args, { capture: true, allowFail: true }).catch(() => null);
  return !!r && r.code === 0;
};

/**
 * Checks that the release was built and signed by this repository's release
 * workflow on `tag`, from the commit a maintainer signed: the tag itself is
 * checked first (signed with a pinned maintainer key, on main), then the
 * signature bundle's certificate must name exactly that workflow, tag and
 * commit, then SHA256SUMS is checked against it with cosign, or every asset
 * against its GitHub attestation with gh. Throws on any mismatch, a missing
 * bundle, or when a needed tool is missing; returns how it was verified
 * ({method: "cosign" | "attestation", commit}).
 * (`require` is accepted for older callers: it is always required now.)
 */
export async function verifySignature(paths, runner, { tag, log = console.log, remote } = {}) {
  checkTag(tag);
  const sigFile = path.join(paths.dir, SIGNATURE);
  if (!fs.existsSync(sigFile)) throw new Error(`The release has no signature (${SIGNATURE}): not installing it.`);
  let commit;
  try {
    commit = await verifySourceTag(tag, { runner, log, ...(remote ? { remote } : {}) });
  } catch (e) {
    throw new Error(`The release's tag doesn't check out: ${e.message}. Not installing it.`);
  }
  try {
    checkBundleIdentity(fs.readFileSync(sigFile, "utf8"), {
      identity: signerIdentity(tag),
      issuer: SIGNER_ISSUER,
      repositoryUri: `https://github.com/${REPO}`,
      ref: `refs/tags/${tag}`,
      commit,
    });
  } catch (e) {
    throw new Error(`The release's signature is not CoderHelm's release workflow for ${tag}: ${e.message}. Not installing it.`);
  }
  if (await has(runner, "cosign", ["version"])) {
    const r = await runner.run("cosign", cosignArgs(tag, sigFile, path.join(paths.dir, "SHA256SUMS"), commit), { capture: true, allowFail: true });
    if (r.code !== 0) throw new Error(`The release's signature does not verify: not installing it.${r.stderr ? `\n${r.stderr.trim()}` : ""}`);
    log(`  signature verified (cosign: ${SIGNER_WORKFLOW} on ${tag}, in the Rekor log)`);
    return { method: "cosign", commit };
  }
  if (await has(runner, "gh", ["--version"])) {
    for (const n of ASSETS) {
      const r = await runner.run("gh", attestationArgs(tag, path.join(paths.dir, n), commit), { capture: true, allowFail: true });
      if (r.code !== 0) throw new Error(`${n} has no valid build attestation from ${SIGNER_WORKFLOW} on ${tag}: not installing it.`);
    }
    log(`  provenance verified (gh: built by ${SIGNER_WORKFLOW} on ${tag})`);
    return { method: "attestation", commit };
  }
  throw new Error("Can't check the release's signature: install cosign (https://docs.sigstore.dev/cosign/system_config/installation/) or the GitHub CLI (gh), then run this again. Nothing was installed.");
}
