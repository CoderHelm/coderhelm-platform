// The release's tag, checked in git itself: before a release's signature is
// trusted, its tag must be an annotated tag named exactly vX.Y.Z, signed with
// one of the maintainer SSH keys pinned in selfhost/maintainers.json, on a
// commit that is on main. The commit it points to is returned, and the
// signing certificate must name that same commit (lib/sigstore.mjs), so the
// release workflow that signed is the one in the commit a maintainer signed,
// not a release.yml changed and pushed under a new tag.
//
// It needs git (2.34 or newer, for SSH signatures) and ssh-keygen (OpenSSH
// 8.2 or newer). Either missing, or any check failing: the release is refused.
// The user's own git configuration is not read (no global or system config),
// so a local allowedSignersFile, GPG keyring or signing program can't make a
// signature pass.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { REPO } from "./util.mjs";
import { checkTag } from "./release.mjs";

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
 * commit it points to. Throws, naming the check, on anything else.
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
    if (!body.includes("\n-----BEGIN SSH SIGNATURE-----\n")) throw new Error(`${tag} has no SSH signature`);
    const signers = path.join(dir, "allowed_signers");
    fs.writeFileSync(signers, allowedSigners(keys));
    const v = await git(["-c", "gpg.format=ssh", "-c", `gpg.ssh.allowedSignersFile=${signers}`, "-c", "gpg.ssh.program=ssh-keygen", "verify-tag", "--raw", tag]);
    if (v.code !== 0) {
      throw new Error(`${tag} is not signed by a CoderHelm maintainer key (selfhost/maintainers.json)${v.stderr ? `: ${v.stderr.trim().split("\n").slice(-1)[0]}` : ""}`);
    }
    const commit = (await git(["rev-parse", "--verify", `refs/tags/${tag}^{commit}`])).stdout.trim();
    if (!/^[0-9a-f]{40}$/.test(commit)) throw new Error(`could not read the commit ${tag} points to`);
    const m = await git(["merge-base", "--is-ancestor", commit, "refs/heads/main"]);
    if (m.code !== 0) throw new Error(`${tag} (${commit}) is not on main`);
    log(`  tag verified (${tag} signed by a maintainer key, commit ${commit.slice(0, 12)} on main)`);
    return commit;
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
}
