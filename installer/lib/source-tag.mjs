// The release's tag, checked in git itself: before a release's signature is
// trusted, its tag must be an annotated tag named exactly vX.Y.Z, on a commit
// that is on main, and vouched for in one of two ways:
//
//   maintainer  the tag is signed with one of the maintainer SSH keys pinned
//               in selfhost/maintainers.json (a release cut by hand);
//   automatic   the tag carries a keyless Sigstore signature (the "release
//               attestation" block in its message) made by exactly
//               .github/workflows/auto-release.yml on refs/heads/main, run on
//               the tag's own commit, over a statement naming this
//               repository, the tag and that commit (a release cut after
//               main's deploy). No key is stored for this path: the
//               certificate names the workflow, and only main's reviewed copy
//               of that workflow on main can get it.
//
// The commit the tag points to is returned, and the release's signing
// certificate must name that same commit (lib/sigstore.mjs), so the release
// workflow that signed is the one in that reviewed commit, not a release.yml
// changed and pushed under a new tag.
//
// It needs git (2.34 or newer, for SSH signatures) and ssh-keygen (OpenSSH
// 8.2 or newer), and for an automatic release cosign or the GitHub CLI. Any
// missing, or any check failing: the release is refused.
// The user's own git configuration is not read (no global or system config),
// so a local allowedSignersFile, GPG keyring or signing program can't make a
// signature pass.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { REPO } from "./util.mjs";
import { checkTag } from "./release.mjs";
import { checkBundleIdentity } from "./sigstore.mjs";

export const SOURCE_REMOTE = `https://github.com/${REPO}.git`;

const here = path.dirname(fileURLToPath(import.meta.url));

/**
 * The pinned maintainer keys. The release build copies selfhost/maintainers.json
 * into the installer package (lib/maintainers.json); a checkout reads it from
 * selfhost/.
 */
export function maintainersFile() {
  for (const f of [path.join(here, "maintainers.json"), path.join(here, "..", "..", "selfhost", "maintainers.json")]) {
    if (fs.existsSync(f)) return f;
  }
  throw new Error("The pinned maintainer keys (selfhost/maintainers.json) are missing: can't check the release's tag.");
}

const KEY_RE = /^(ssh-ed25519|ecdsa-sha2-nistp256|ecdsa-sha2-nistp384|ecdsa-sha2-nistp521|sk-ssh-ed25519@openssh\.com|sk-ecdsa-sha2-nistp256@openssh\.com|ssh-rsa) [A-Za-z0-9+/]+={0,3}$/;
const LOGIN_RE = /^[A-Za-z0-9-]{1,39}$/;

/** Parses maintainers.json; throws unless every entry is a login and one public key. */
export function parseMaintainers(text) {
  let m;
  try {
    m = JSON.parse(text);
  } catch {
    throw new Error("maintainers.json is not valid JSON");
  }
  if (!m || !Array.isArray(m.keys) || m.keys.length === 0) throw new Error("maintainers.json lists no keys");
  for (const k of m.keys) {
    if (!k || !LOGIN_RE.test(String(k.login))) throw new Error(`maintainers.json: ${JSON.stringify(k?.login)} is not a GitHub login`);
    if (!KEY_RE.test(String(k.key))) throw new Error(`maintainers.json: the key for ${k.login} is not an SSH public key (type and base64, no comment)`);
  }
  return m.keys;
}

/** ssh-keygen's allowed signers file: one line per key, for git signatures only. */
export function allowedSigners(keys) {
  return keys.map((k) => `${k.login} namespaces="git" ${k.key}`).join("\n") + "\n";
}

/** The workflow that cuts automatic releases, and the only ref it may sign from. */
export const AUTO_RELEASE_WORKFLOW = `${REPO}/.github/workflows/auto-release.yml`;
export const AUTO_RELEASE_REF = "refs/heads/main";
export const AUTO_RELEASE_IDENTITY = `https://github.com/${AUTO_RELEASE_WORKFLOW}@${AUTO_RELEASE_REF}`;
const ISSUER = "https://token.actions.githubusercontent.com";

/** The lines around the base64 Sigstore bundle in an automatic release's tag message. */
export const ATTESTATION_BEGIN = "-----BEGIN CODERHELM RELEASE ATTESTATION-----";
export const ATTESTATION_END = "-----END CODERHELM RELEASE ATTESTATION-----";

/**
 * What auto-release.yml signs for a tag: this repository, the tag and the
 * commit, byte for byte (the workflow, release.yml, the update build and
 * install.sh write the same four lines).
 */
export function attestationPayload(tag, commit) {
  checkTag(tag);
  if (!/^[0-9a-f]{40}$/.test(String(commit))) throw new Error("no commit for the release attestation");
  return `coderhelm-release-tag v1\nrepository ${REPO}\ntag ${tag}\ncommit ${commit}\n`;
}

/** The Sigstore bundle (JSON text) in a tag message, or null when it has none. Throws on a malformed block. */
export function attestationBundle(tagText) {
  const lines = tagText.split("\n");
  const b = lines.filter((l) => l === ATTESTATION_BEGIN).length;
  const e = lines.filter((l) => l === ATTESTATION_END).length;
  if (b === 0 && e === 0) return null;
  if (b !== 1 || e !== 1) throw new Error("its release attestation block is malformed (one begin and one end line)");
  const from = lines.indexOf(ATTESTATION_BEGIN);
  const to = lines.indexOf(ATTESTATION_END);
  if (to < from) throw new Error("its release attestation block is malformed (end before begin)");
  const b64 = lines.slice(from + 1, to).join("");
  if (!/^[A-Za-z0-9+/]+={0,2}$/.test(b64)) throw new Error("its release attestation is not base64");
  return Buffer.from(b64, "base64").toString("utf8");
}

/** cosign's arguments for an automatic release's tag: exactly auto-release.yml on main, run on the tag's commit. */
export function attestationCosignArgs(bundleFile, payloadFile, commit) {
  return [
    "verify-blob", "--bundle", bundleFile,
    "--certificate-identity", AUTO_RELEASE_IDENTITY,
    "--certificate-oidc-issuer", ISSUER,
    "--certificate-github-workflow-repository", REPO,
    "--certificate-github-workflow-ref", AUTO_RELEASE_REF,
    "--certificate-github-workflow-sha", commit,
    payloadFile,
  ];
}

/** gh's arguments: GitHub's attestation of the same statement, from auto-release.yml on main and the tag's commit. */
export function attestationGhArgs(payloadFile, commit) {
  return [
    "attestation", "verify", payloadFile,
    "--repo", REPO,
    "--signer-workflow", AUTO_RELEASE_WORKFLOW,
    "--source-ref", AUTO_RELEASE_REF,
    "--source-digest", commit,
    "--cert-oidc-issuer", ISSUER,
    "--deny-self-hosted-runners",
  ];
}

const has = async (runner, cmd, args) => {
  const r = await runner.run(cmd, args, { capture: true, allowFail: true }).catch(() => null);
  return !!r && r.code === 0;
};

/** Git with no global, system or user configuration. */
const GIT_ENV = { GIT_CONFIG_GLOBAL: "/dev/null", GIT_CONFIG_NOSYSTEM: "1", GIT_TERMINAL_PROMPT: "0", HOME: os.tmpdir() };

/** The header of a tag object (up to the first blank line) as fields. */
export function tagHeader(text) {
  const head = text.split("\n\n")[0];
  const fields = {};
  for (const line of head.split("\n")) {
    const sp = line.indexOf(" ");
    if (sp > 0 && !(line.slice(0, sp) in fields)) fields[line.slice(0, sp)] = line.slice(sp + 1);
  }
  return fields;
}

/**
 * Checks `tag` in `remote` (this repository on GitHub) and returns the
 * commit it points to: annotated, named `tag`, on main, and either signed
 * by a maintainer key or attested by auto-release.yml on main for exactly
 * this tag and commit. Throws, naming the check, on anything else.
 */
export async function verifySourceTag(tag, { runner, remote = SOURCE_REMOTE, keysFile = maintainersFile(), log = () => {} }) {
  checkTag(tag);
  const keys = parseMaintainers(fs.readFileSync(keysFile, "utf8"));
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "ch-tag-"));
  try {
    const git = (args, opts = {}) => runner.run("git", ["-C", dir, ...args], { capture: true, allowFail: true, env: GIT_ENV, ...opts });
    const r0 = await runner.run("git", ["init", "-q", "--bare", dir], { capture: true, allowFail: true, env: GIT_ENV }).catch((e) => ({ code: 127, stderr: e.message }));
    if (r0.code !== 0) throw new Error(`git is needed to check the release's tag (${(r0.stderr || "").trim() || "git init failed"})`);
    // The tag and main's commits (no trees or files: small and quick).
    const f = await git(["fetch", "-q", "--no-tags", "--filter=tree:0", remote, `+refs/tags/${tag}:refs/tags/${tag}`, "+refs/heads/main:refs/heads/main"]);
    if (f.code !== 0) throw new Error(`could not fetch the tag ${tag} and main from ${remote}${f.stderr ? `: ${f.stderr.trim()}` : ""}`);
    const type = (await git(["cat-file", "-t", `refs/tags/${tag}`])).stdout.trim();
    if (type !== "tag") throw new Error(`${tag} is a lightweight tag, not an annotated, signed one`);
    const body = (await git(["cat-file", "tag", `refs/tags/${tag}`])).stdout;
    const h = tagHeader(body);
    if (h.tag !== tag) throw new Error(`the tag object under ${tag} is named ${JSON.stringify(h.tag)}`);
    if (h.type !== "commit") throw new Error(`${tag} points to a ${h.type}, not a commit`);
    const commit = (await git(["rev-parse", "--verify", `refs/tags/${tag}^{commit}`])).stdout.trim();
    if (!/^[0-9a-f]{40}$/.test(commit)) throw new Error(`could not read the commit ${tag} points to`);
    const m = await git(["merge-base", "--is-ancestor", commit, "refs/heads/main"]);
    if (m.code !== 0) throw new Error(`${tag} (${commit}) is not on main`);

    // A maintainer's SSH signature, when the tag has one, decides: it must verify.
    if (body.includes("\n-----BEGIN SSH SIGNATURE-----\n")) {
      const signers = path.join(dir, "allowed_signers");
      fs.writeFileSync(signers, allowedSigners(keys));
      const v = await git(["-c", "gpg.format=ssh", "-c", `gpg.ssh.allowedSignersFile=${signers}`, "-c", "gpg.ssh.program=ssh-keygen", "verify-tag", "--raw", tag]);
      if (v.code !== 0) {
        throw new Error(`${tag} is not signed by a CoderHelm maintainer key (selfhost/maintainers.json)${v.stderr ? `: ${v.stderr.trim().split("\n").slice(-1)[0]}` : ""}`);
      }
      log(`  tag verified (${tag} signed by a maintainer key, commit ${commit.slice(0, 12)} on main)`);
      return commit;
    }

    // Otherwise the tag must be an automatic release's: auto-release.yml on main signed this tag and commit.
    const bundle = attestationBundle(body);
    if (bundle === null) throw new Error(`${tag} has no SSH signature and no release attestation`);
    try {
      checkBundleIdentity(bundle, { identity: AUTO_RELEASE_IDENTITY, issuer: ISSUER, repositoryUri: `https://github.com/${REPO}`, ref: AUTO_RELEASE_REF, commit });
    } catch (e) {
      throw new Error(`${tag}'s release attestation is not from ${AUTO_RELEASE_WORKFLOW} on main for commit ${commit}: ${e.message}`);
    }
    const bundleFile = path.join(dir, "tag.sigstore.json");
    const payloadFile = path.join(dir, `${tag}.release-tag`);
    fs.writeFileSync(bundleFile, bundle);
    fs.writeFileSync(payloadFile, attestationPayload(tag, commit));
    if (await has(runner, "cosign", ["version"])) {
      const r = await runner.run("cosign", attestationCosignArgs(bundleFile, payloadFile, commit), { capture: true, allowFail: true });
      if (r.code !== 0) throw new Error(`${tag}'s release attestation does not verify for this tag and commit${r.stderr ? `: ${r.stderr.trim().split("\n").slice(-1)[0]}` : ""}`);
    } else if (await has(runner, "gh", ["--version"])) {
      const r = await runner.run("gh", attestationGhArgs(payloadFile, commit), { capture: true, allowFail: true });
      if (r.code !== 0) throw new Error(`${tag} has no valid GitHub attestation from ${AUTO_RELEASE_WORKFLOW} on main for this tag and commit`);
    } else {
      throw new Error("cosign or the GitHub CLI (gh) is needed to check an automatic release's tag");
    }
    log(`  tag verified (${tag} released by ${AUTO_RELEASE_WORKFLOW} on main, commit ${commit.slice(0, 12)} on main)`);
    return commit;
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
}
