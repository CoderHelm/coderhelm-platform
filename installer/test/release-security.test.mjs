// Release verification: nothing that merely looks like a CoderHelm release
// gets installed. Signers come from test/fixtures/signers.json (stand-in
// Fulcio certificates; make-signers.sh regenerates them); cosign and gh are
// stubbed, and each test checks the exact identity they are asked for.
import { test } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import {
  ASSETS, attestationArgs, checkDownloadUrl, cosignArgs, fetchRelease, latestVersion, signerIdentity, SIGNATURE, verifySignature,
} from "../lib/release.mjs";
import { certificateClaims, checkBundleIdentity, encodeOid, readBundle } from "../lib/sigstore.mjs";
import { checkRollbackTarget, ranVersions } from "../lib/commands.mjs";
import { FakeRunner } from "./fake.mjs";

const here = path.dirname(fileURLToPath(import.meta.url));
const SIGNERS = JSON.parse(fs.readFileSync(path.join(here, "fixtures", "signers.json"), "utf8"));
const TAG = "v1.2.3";
// The commit the stand-in certificates name (fixtures/make-signers.sh).
const COMMIT = "1".repeat(40);
const IDENTITY = "https://github.com/CoderHelm/coderhelm-platform/.github/workflows/release.yml@refs/tags/v1.2.3";

/** A cosign 2.x bundle around a signer's certificate (with or without its Rekor entry). */
function bundle(signer, { rekor = true } = {}) {
  return JSON.stringify({
    base64Signature: "c2ln",
    cert: Buffer.from(SIGNERS[signer]).toString("base64"),
    ...(rekor ? { rekorBundle: { SignedEntryTimestamp: "c2V0", Payload: { logIndex: 1, integratedTime: 1 } } } : {}),
  });
}

/** A Sigstore v0.3 bundle (cosign 3, --new-bundle-format). */
function newBundle(signer, { tlog = true } = {}) {
  const der = new crypto.X509Certificate(SIGNERS[signer]).raw.toString("base64");
  return JSON.stringify({
    mediaType: "application/vnd.dev.sigstore.bundle.v0.3+json",
    verificationMaterial: { certificate: { rawBytes: der }, tlogEntries: tlog ? [{ logIndex: "1", inclusionProof: { logIndex: "1" } }] : [] },
  });
}

function tmpdir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), "ch-rel-"));
}

const sha = (b) => crypto.createHash("sha256").update(b).digest("hex");

/** A release on disk: assets, SHA256SUMS over them, and a bundle from `signer`. */
function releaseDir(signer = "good", { withBundle = true } = {}) {
  const dir = tmpdir();
  const lines = [];
  for (const n of ASSETS) {
    const body = Buffer.from(`contents of ${n}`);
    fs.writeFileSync(path.join(dir, n), body);
    lines.push(`${sha(body)}  ${n}`);
  }
  fs.writeFileSync(path.join(dir, "SHA256SUMS"), lines.join("\n") + "\n");
  if (withBundle) fs.writeFileSync(path.join(dir, SIGNATURE), bundle(signer));
  return { dir };
}

/** git's answers for a release tag that checks out (lib/source-tag.mjs), with overrides. */
function sourceTag({ commit = COMMIT, name = TAG, type = "tag", signature = true, ...over } = {}) {
  const body = `object ${commit}\ntype commit\ntag ${name}\ntagger M <m@example.com> 1 +0000\n\n${name}\n` +
    (signature ? "-----BEGIN SSH SIGNATURE-----\nU1NIU0lH\n-----END SSH SIGNATURE-----\n" : "");
  return {
    "init -q --bare": "",
    "fetch -q --no-tags": "",
    "cat-file -t": type,
    "cat-file tag": body,
    "verify-tag": "Good \"git\" signature",
    "rev-parse --verify": commit,
    "merge-base --is-ancestor": "",
    ...over,
  };
}
const withSource = (answers, src = {}) => new FakeRunner({ ...sourceTag(src), ...answers });
const cosignOk = (src) => withSource({ "cosign version": "v2.4.1", "cosign verify-blob": "Verified OK" }, src);
const quiet = { tag: TAG, log: () => {} };

// ── the signer's identity ───────────────────────────────────────────────

test("the certificate reader finds the identity and Fulcio's extensions", () => {
  assert.deepEqual([...encodeOid("1.3.6.1.4.1.57264.1.8")], [0x2b, 6, 1, 4, 1, 0x83, 0xbf, 0x30, 1, 8]);
  const c = certificateClaims(readBundle(bundle("good")).der);
  assert.deepEqual(c.identities, [IDENTITY]);
  assert.equal(c.issuer, "https://token.actions.githubusercontent.com");
  assert.equal(c.repository, "https://github.com/CoderHelm/coderhelm-platform");
  assert.equal(c.ref, "refs/tags/v1.2.3");
  assert.equal(readBundle(newBundle("good")).tlog, true);
});

test("a real GitHub Actions bundle (cli/cli's build provenance, public Sigstore) reads the same way", () => {
  // fixtures/real-github-actions-bundle.json: from `gh attestation download` on a GitHub CLI release.
  const text = fs.readFileSync(path.join(here, "fixtures", "real-github-actions-bundle.json"), "utf8");
  const { tlog, der } = readBundle(text);
  assert.equal(tlog, true);
  const c = certificateClaims(der);
  assert.deepEqual(c.identities, ["https://github.com/cli/cli/.github/workflows/deployment.yml@refs/heads/trunk"]);
  assert.equal(c.issuer, "https://token.actions.githubusercontent.com");
  assert.equal(c.repository, "https://github.com/cli/cli");
  // A genuine Sigstore signature, just not CoderHelm's release workflow on a tag: refused.
  assert.equal(c.sourceDigest, "4f2cfe92e3499f8581bf39db77e0a1431fa7b55c");
  assert.equal(c.workflowSha, c.sourceDigest);
  assert.throws(() => checkBundleIdentity(text, {
    identity: IDENTITY, issuer: "https://token.actions.githubusercontent.com",
    repositoryUri: "https://github.com/CoderHelm/coderhelm-platform", ref: "refs/tags/v1.2.3", commit: COMMIT,
  }), /signed by https:\/\/github\.com\/cli\/cli.*built from https:\/\/github\.com\/cli\/cli.*refs\/heads\/trunk.*built from commit 4f2cfe/);
});

test("the identity is exact: this repository's release.yml on the release's own tag", () => {
  assert.equal(signerIdentity(TAG), IDENTITY);
  assert.throws(() => signerIdentity("v1.2.3/../../x"), /not a CoderHelm release/);
  assert.throws(() => signerIdentity("main"), /not a CoderHelm release/);
  const args = cosignArgs(TAG, "b.json", "SHA256SUMS", COMMIT);
  assert.equal(args[args.indexOf("--certificate-github-workflow-sha") + 1], COMMIT);
  assert.throws(() => cosignArgs(TAG, "b.json", "SHA256SUMS"), /no source commit/);
  assert.equal(args[args.indexOf("--certificate-identity") + 1], IDENTITY);
  assert.equal(args[args.indexOf("--certificate-oidc-issuer") + 1], "https://token.actions.githubusercontent.com");
  assert.equal(args[args.indexOf("--certificate-github-workflow-repository") + 1], "CoderHelm/coderhelm-platform");
  assert.equal(args[args.indexOf("--certificate-github-workflow-ref") + 1], "refs/tags/v1.2.3");
  assert.ok(!args.some((a) => /regexp|insecure|ignore-tlog|offline/.test(a)), "no loose match and no skipped transparency log");
  const gh = attestationArgs(TAG, "f", COMMIT);
  for (const f of ["--repo", "--signer-workflow", "--source-ref", "--source-digest", "--cert-oidc-issuer", "--deny-self-hosted-runners"]) assert.ok(gh.includes(f), f);
  assert.equal(gh[gh.indexOf("--source-digest") + 1], COMMIT);
  assert.throws(() => attestationArgs(TAG, "f"), /no source commit/);
  assert.equal(gh[gh.indexOf("--source-ref") + 1], "refs/tags/v1.2.3");
});

for (const [signer, why] of [
  ["otherRepo", /evil\/coderhelm-platform/],
  ["otherWorkflow", /ci\.yml/],
  ["branch", /refs\/heads\/main/],
  ["otherTag", /refs\/tags\/v1\.0\.0/],
  ["otherIssuer", /accounts\.google\.com/],
  // The exact identity, but built from another commit than the signed tag's:
  // a release.yml changed and pushed under a new tag gets this certificate.
  ["otherCommit", /built from commit 2{40}, not 1{40}.*workflow file came from commit 2{40}.*ran on commit 2{40}/],
  ["noCommit", /built from commit \(none named\)/],
]) {
  test(`a valid-looking signature from the wrong signer is refused (${signer})`, async () => {
    assert.throws(() => checkBundleIdentity(bundle(signer), {
      identity: IDENTITY, issuer: "https://token.actions.githubusercontent.com",
      repositoryUri: "https://github.com/CoderHelm/coderhelm-platform", ref: "refs/tags/v1.2.3", commit: COMMIT,
    }), why);
    // Even when cosign (stubbed) would say yes, the release is refused first.
    const runner = cosignOk();
    await assert.rejects(() => verifySignature(releaseDir(signer), runner, quiet), /not CoderHelm's release workflow for v1\.2\.3/);
    assert.ok(!runner.commands.some((c) => c.args[0] === "verify-blob"), "cosign is never asked");
  });
}

test("a signature without a Rekor transparency log entry is refused", async () => {
  const p = releaseDir();
  fs.writeFileSync(path.join(p.dir, SIGNATURE), bundle("good", { rekor: false }));
  await assert.rejects(() => verifySignature(p, cosignOk(), quiet), /no transparency log/);
  fs.writeFileSync(path.join(p.dir, SIGNATURE), newBundle("good", { tlog: false }));
  await assert.rejects(() => verifySignature(p, cosignOk(), quiet), /no transparency log/);
});

test("a missing or broken bundle is refused", async () => {
  await assert.rejects(() => verifySignature(releaseDir("good", { withBundle: false }), cosignOk(), quiet), /has no signature/);
  const p = releaseDir();
  fs.writeFileSync(path.join(p.dir, SIGNATURE), "{}");
  await assert.rejects(() => verifySignature(p, cosignOk(), quiet), /no signing certificate/);
  fs.writeFileSync(path.join(p.dir, SIGNATURE), "not json");
  await assert.rejects(() => verifySignature(p, cosignOk(), quiet), /not valid JSON/);
});

test("the right signer: cosign checks the signature with the exact identity", async () => {
  const runner = cosignOk();
  assert.deepEqual(await verifySignature(releaseDir(), runner, quiet), { method: "cosign", commit: COMMIT });
  const v = runner.commands.find((c) => c.args[0] === "verify-blob");
  assert.equal(v.args[v.args.indexOf("--certificate-identity") + 1], IDENTITY);
  assert.equal(v.args[v.args.indexOf("--certificate-github-workflow-sha") + 1], COMMIT);
  // The tag is checked before the signature, against the pinned keys, in git with no user config.
  const lines = runner.lines();
  const verifyTag = lines.findIndex((l) => l.includes(" verify-tag "));
  assert.ok(verifyTag >= 0 && verifyTag < lines.findIndex((l) => l.includes("verify-blob")));
  assert.match(lines[verifyTag], /gpg\.ssh\.allowedSignersFile=\S+allowed_signers/);
  const env = runner.commands[verifyTag].env;
  assert.equal(env.GIT_CONFIG_GLOBAL, "/dev/null");
  assert.equal(env.GIT_CONFIG_NOSYSTEM, "1");
});

test("tampered SHA256SUMS: cosign says no, the release is refused", async () => {
  const p = releaseDir();
  fs.appendFileSync(path.join(p.dir, "SHA256SUMS"), `${"0".repeat(64)}  extra.zip\n`);
  // cosign checks the signature over the file's bytes; the stub answers as cosign would.
  const runner = withSource({ "cosign version": "v2.4.1", "cosign verify-blob": { code: 1, stderr: "invalid signature when validating ASN.1 encoded signature" } });
  await assert.rejects(() => verifySignature(p, runner, quiet), /signature does not verify/);
});

test("no cosign and no gh: refused (no unchecked install, with or without --require-signature)", async () => {
  const none = withSource({ "cosign version": { code: 127 }, "gh --version": { code: 127 } });
  await assert.rejects(() => verifySignature(releaseDir(), none, quiet), /install cosign .* or the GitHub CLI/);
  await assert.rejects(() => verifySignature(releaseDir(), none, { ...quiet, require: false }), /Nothing was installed/);
});

test("gh only: every asset needs an attestation from release.yml on the tag", async () => {
  const ok = withSource({ "cosign version": { code: 127 }, "gh --version": "gh 2", "gh attestation verify": "ok" });
  assert.equal((await verifySignature(releaseDir(), ok, quiet)).method, "attestation");
  const verified = ok.commands.filter((c) => c.args[0] === "attestation").map((c) => path.basename(c.args[2]));
  assert.deepEqual(verified, ASSETS);
  for (const c of ok.commands.filter((c) => c.args[0] === "attestation")) assert.equal(c.args[c.args.indexOf("--source-digest") + 1], COMMIT);
  const bad = withSource({ "cosign version": { code: 127 }, "gh --version": "gh 2", "gh attestation verify": { code: 1 } });
  await assert.rejects(() => verifySignature(releaseDir(), bad, quiet), /no valid build attestation from .*release\.yml on v1\.2\.3/);
  // gh doesn't make a bad bundle acceptable.
  await assert.rejects(() => verifySignature(releaseDir("branch"), ok, quiet), /not CoderHelm's release workflow/);
});

// ── downloads ───────────────────────────────────────────────────────────

/** A fake GitHub: serves a release's files (optionally redirected elsewhere). */
function fakeGithub(files, { redirectTo } = {}) {
  const asked = [];
  const fetchImpl = async (url) => {
    asked.push(url);
    const name = url.split("/").pop();
    if (!(name in files)) return { ok: false, status: 404 };
    return { ok: true, status: 200, url: redirectTo ? `${redirectTo}/${name}` : url, arrayBuffer: async () => Buffer.from(files[name]) };
  };
  return { fetchImpl, asked };
}

function releaseFiles(signer = "good") {
  const p = releaseDir(signer);
  return Object.fromEntries(["SHA256SUMS", SIGNATURE, ...ASSETS].map((n) => [n, fs.readFileSync(path.join(p.dir, n))]));
}

test("downloads: only GitHub's release URLs for this repository, over HTTPS", () => {
  assert.ok(checkDownloadUrl("https://github.com/CoderHelm/coderhelm-platform/releases/download/v1.2.3/SHA256SUMS"));
  assert.ok(checkDownloadUrl("https://release-assets.githubusercontent.com/github-production-release-asset/1/abc?sig=x"));
  for (const bad of [
    "http://github.com/CoderHelm/coderhelm-platform/releases/download/v1.2.3/SHA256SUMS",
    "https://github.com/evil/coderhelm-platform/releases/download/v1.2.3/SHA256SUMS",
    "https://evil.example.com/SHA256SUMS",
    "https://github.com.evil.example/x",
    "https://user:pw@github.com/CoderHelm/coderhelm-platform/releases/download/v1.2.3/x",
  ]) assert.throws(() => checkDownloadUrl(bad), /Refusing a download/, bad);
});

test("fetchRelease: a file changed after signing is refused before anything is unpacked", async () => {
  process.env.CODERHELM_HOME = tmpdir();
  const files = releaseFiles();
  files["worker.zip"] = Buffer.from("swapped after the release was signed");
  const runner = cosignOk();
  await assert.rejects(() => fetchRelease(TAG, { runner, fetchImpl: fakeGithub(files).fetchImpl, log: () => {} }), /worker\.zip does not match its checksum/);
  assert.ok(!runner.commands.some((c) => c.cmd === "tar"), "nothing unpacked");
});

test("fetchRelease: no bundle on the release means no install", async () => {
  process.env.CODERHELM_HOME = tmpdir();
  const files = releaseFiles();
  delete files[SIGNATURE];
  await assert.rejects(() => fetchRelease(TAG, { runner: cosignOk(), fetchImpl: fakeGithub(files).fetchImpl, log: () => {} }), /Could not download SHA256SUMS\.sigstore\.json/);
});

test("fetchRelease: a redirect off GitHub is refused; a signed release is checked before it is unpacked", async () => {
  process.env.CODERHELM_HOME = tmpdir();
  const evil = fakeGithub(releaseFiles(), { redirectTo: "https://mirror.example.com" });
  await assert.rejects(() => fetchRelease(TAG, { runner: cosignOk(), fetchImpl: evil.fetchImpl, log: () => {} }), /Refusing a download from https:\/\/mirror\.example\.com/);

  process.env.CODERHELM_HOME = tmpdir();
  const runner = cosignOk();
  const gh = fakeGithub(releaseFiles());
  await fetchRelease(TAG, { runner, fetchImpl: gh.fetchImpl, log: () => {} });
  assert.ok(gh.asked.every((u) => u.startsWith("https://github.com/CoderHelm/coderhelm-platform/releases/download/v1.2.3/")));
  const order = runner.commands.map((c) => (c.args[0] === "verify-blob" ? "verify" : c.cmd === "tar" ? "tar" : null)).filter(Boolean);
  assert.deepEqual(order, ["verify", "tar", "tar"]);
  // Run again from the cache: checked again.
  await fetchRelease(TAG, { runner, fetchImpl: gh.fetchImpl, log: () => {} });
  assert.equal(runner.commands.filter((c) => c.args[0] === "verify-blob").length, 2);
});

test("fetchRelease: a tag that isn't a release tag is refused before any download", async () => {
  const gh = fakeGithub({});
  await assert.rejects(() => fetchRelease("../../etc", { runner: cosignOk(), fetchImpl: gh.fetchImpl, log: () => {} }), /not a CoderHelm release/);
  assert.equal(gh.asked.length, 0);
});

test("latest release: drafts, pre-releases and odd tags are refused", async () => {
  const answer = (j) => async () => ({ ok: true, json: async () => j });
  assert.equal(await latestVersion(answer({ tag_name: "v1.2.3" })), "v1.2.3");
  await assert.rejects(() => latestVersion(answer({ tag_name: "v1.2.3", prerelease: true })), /pre-release/);
  await assert.rejects(() => latestVersion(answer({ tag_name: "main; rm -rf /" })), /not a CoderHelm release/);
});

// ── no downgrade ────────────────────────────────────────────────────────

test("rollback: only to a release this install ran before", () => {
  const state = { version: "v1.4.0", previousVersion: "v1.3.0", versions: ["v1.1.0", "v1.3.0"] };
  assert.deepEqual(ranVersions(state), ["v1.1.0", "v1.3.0", "v1.4.0"]);
  assert.doesNotThrow(() => checkRollbackTarget(state, "v1.3.0"));
  assert.doesNotThrow(() => checkRollbackTarget(state, "v1.1.0"));
  assert.throws(() => checkRollbackTarget(state, "v1.2.0"), /not a release this install ran before/);
  assert.throws(() => checkRollbackTarget(state, "v1.4.0"), /not a release this install ran before/);
  assert.throws(() => checkRollbackTarget({ version: "v1.4.0" }, "v1.0.0"), /none recorded/);
});

// ── models.json: read only after the signature check ─────────────────────

import { MODELS, releaseModels } from "../lib/release.mjs";

const MANIFEST_BODY = Buffer.from(JSON.stringify({ schema: 1, version: TAG, calls: ["claude-opus-5-5"], bedrock: {} }));

/** A release whose signed SHA256SUMS lists models.json. */
function filesWithModels(body = MANIFEST_BODY) {
  const files = releaseFiles();
  files[MODELS] = body;
  files.SHA256SUMS = Buffer.from(String(files.SHA256SUMS) + `${sha(MANIFEST_BODY)}  ${MODELS}\n`);
  return files;
}

test("models.json is downloaded only after the signature verifies, and must match the signed checksum", async () => {
  process.env.CODERHELM_HOME = tmpdir();
  const runner = cosignOk();
  const gh = fakeGithub(filesWithModels());
  const paths = await fetchRelease(TAG, { runner, fetchImpl: gh.fetchImpl, log: () => {} });
  assert.deepEqual(paths.models.calls, ["claude-opus-5-5"]);
  const verifiedAt = runner.commands.findIndex((c) => c.args[0] === "verify-blob");
  assert.ok(verifiedAt >= 0);
  assert.equal(gh.asked.filter((u) => u.endsWith(`/${MODELS}`)).length, 1);

  // Changed after signing: refused.
  process.env.CODERHELM_HOME = tmpdir();
  const swapped = fakeGithub(filesWithModels(Buffer.from(JSON.stringify({ schema: 1, version: TAG, calls: [] }))));
  await assert.rejects(() => fetchRelease(TAG, { runner: cosignOk(), fetchImpl: swapped.fetchImpl, log: () => {} }), /models\.json does not match its checksum/);

  // A signature that doesn't verify: models.json is never fetched.
  process.env.CODERHELM_HOME = tmpdir();
  const bad = withSource({ "cosign version": "v2.4.1", "cosign verify-blob": { code: 1, stderr: "no" } });
  const gh2 = fakeGithub(filesWithModels());
  await assert.rejects(() => fetchRelease(TAG, { runner: bad, fetchImpl: gh2.fetchImpl, log: () => {} }), /does not verify/);
  assert.ok(!gh2.asked.some((u) => u.endsWith(`/${MODELS}`)));
});

test("models.json: a release that doesn't list it has none; with gh it needs its own attestation; never without a check", async () => {
  process.env.CODERHELM_HOME = tmpdir();
  const plain = await fetchRelease(TAG, { runner: cosignOk(), fetchImpl: fakeGithub(releaseFiles()).fetchImpl, log: () => {} });
  assert.equal(plain.models, null);

  const dir = tmpdir();
  fs.writeFileSync(path.join(dir, MODELS), MANIFEST_BODY);
  const sums = { [MODELS]: sha(MANIFEST_BODY) };
  const noAttestation = new FakeRunner({ "gh attestation verify": { code: 1 } });
  await assert.rejects(() => releaseModels(dir, sums, { runner: noAttestation, tag: TAG, method: "attestation", commit: COMMIT, log: () => {} }), /no valid build attestation/);
  const attested = new FakeRunner({ "gh attestation verify": "ok" });
  assert.equal((await releaseModels(dir, sums, { runner: attested, tag: TAG, method: "attestation", commit: COMMIT, log: () => {} })).version, TAG);
  assert.equal(attested.commands[0].args[attested.commands[0].args.indexOf("--source-digest") + 1], COMMIT);
  await assert.rejects(() => releaseModels(dir, sums, { runner: attested, tag: TAG, method: undefined, log: () => {} }), /signature wasn't checked/);
  // Another release's list is refused.
  await assert.rejects(() => releaseModels(dir, sums, { runner: attested, tag: "v9.9.9", method: "cosign", log: () => {} }), /isn't a model list for this release/);
});

// ── the release's tag: signed by a pinned maintainer key, on main ─────────

for (const [name, src, why] of [
  ["a lightweight tag", { type: "commit" }, /lightweight tag/],
  ["a tag object named for another version", { name: "v1.0.0" }, /tag object under v1\.2\.3 is named "v1\.0\.0"/],
  ["an unsigned (or GPG-signed) tag", { signature: false }, /has no SSH signature/],
  ["a signature from a key that isn't pinned", { "verify-tag": { code: 1, stderr: "No principal matched." } }, /not signed by a CoderHelm maintainer key.*No principal matched/],
  ["a commit that isn't on main", { "merge-base --is-ancestor": { code: 1 } }, /\(1{40}\) is not on main/],
  ["a tag GitHub doesn't have", { "fetch -q --no-tags": { code: 128, stderr: "couldn't find remote ref" } }, /could not fetch the tag v1\.2\.3/],
  ["no git", { "init -q --bare": { code: 127, stderr: "git: not found" } }, /git is needed/],
]) {
  test(`the release's tag is refused: ${name} (cosign is never asked)`, async () => {
    const runner = cosignOk(src);
    await assert.rejects(() => verifySignature(releaseDir(), runner, quiet), new RegExp(`tag doesn't check out: .*${why.source}`));
    assert.ok(!runner.commands.some((c) => c.args[0] === "verify-blob"), "cosign is never asked");
  });
}

test("the signed tag's commit must be the certificate's: a tag on another commit is refused", async () => {
  const runner = cosignOk({ commit: "3".repeat(40) });
  await assert.rejects(() => verifySignature(releaseDir(), runner, quiet), /built from commit 1{40}, not 3{40}/);
  assert.ok(!runner.commands.some((c) => c.args[0] === "verify-blob"));
});

import { allowedSigners, parseMaintainers, verifySourceTag } from "../lib/source-tag.mjs";
import { Runner } from "../lib/runner.mjs";
import { execFileSync } from "node:child_process";

test("maintainers.json: the pinned keys parse into an allowed signers file for git only", () => {
  const keys = parseMaintainers(fs.readFileSync(path.join(here, "..", "..", "selfhost", "maintainers.json"), "utf8"));
  assert.ok(keys.length >= 1);
  for (const line of allowedSigners(keys).trim().split("\n")) assert.match(line, /^[A-Za-z0-9-]+ namespaces="git" ssh-[a-z0-9-]+ [A-Za-z0-9+/]+=*$/);
  assert.throws(() => parseMaintainers("{}"), /lists no keys/);
  assert.throws(() => parseMaintainers(JSON.stringify({ keys: [{ login: "a", key: "ssh-ed25519 AAAA comment" }] })), /not an SSH public key/);
  assert.throws(() => parseMaintainers(JSON.stringify({ keys: [{ login: "a b", key: "ssh-ed25519 AAAA" }] })), /not a GitHub login/);
});

// Real git and ssh-keygen: a repository with main, a feature branch and tags
// signed by a maintainer key and by a stranger's key.
test("source tag, with real git: only a maintainer-signed annotated tag on main passes", async () => {
  const root = tmpdir();
  const env = { ...process.env, GIT_CONFIG_GLOBAL: "/dev/null", GIT_CONFIG_NOSYSTEM: "1", GIT_AUTHOR_NAME: "M", GIT_AUTHOR_EMAIL: "m@example.com", GIT_COMMITTER_NAME: "M", GIT_COMMITTER_EMAIL: "m@example.com" };
  const sh = (cmd, args, cwd = root) => execFileSync(cmd, args, { cwd, env, encoding: "utf8" }).trim();
  for (const k of ["maintainer", "stranger"]) sh("ssh-keygen", ["-q", "-t", "ed25519", "-N", "", "-C", "", "-f", path.join(root, k)]);
  const pub = (k) => fs.readFileSync(path.join(root, `${k}.pub`), "utf8").trim().split(" ").slice(0, 2).join(" ");
  const keysFile = path.join(root, "maintainers.json");
  fs.writeFileSync(keysFile, JSON.stringify({ keys: [{ login: "maintainer", key: pub("maintainer") }] }));
  const repo = path.join(root, "repo");
  fs.mkdirSync(repo);
  const git = (...a) => sh("git", a, repo);
  git("init", "-q", "-b", "main");
  git("commit", "-q", "--allow-empty", "-m", "on main");
  const onMain = git("rev-parse", "HEAD");
  const sign = (key, ...a) => git("-c", "gpg.format=ssh", "-c", `user.signingkey=${path.join(root, key)}`, "tag", "-s", ...a);
  sign("maintainer", "v1.2.3", "-m", "v1.2.3");
  sign("stranger", "v1.2.4", "-m", "v1.2.4");
  git("tag", "v1.2.5");
  git("tag", "-a", "v1.2.6", "-m", "unsigned");
  // A maintainer-signed tag object for v1.0.0, put under another version's name.
  sign("maintainer", "v1.0.0", "-m", "v1.0.0");
  git("update-ref", "refs/tags/v1.2.7", git("rev-parse", "refs/tags/v1.0.0"));
  git("checkout", "-q", "-b", "feature");
  git("commit", "-q", "--allow-empty", "-m", "not on main");
  sign("maintainer", "v1.2.8", "-m", "v1.2.8");
  git("checkout", "-q", "main");

  const runner = new Runner({ log: () => {} });
  const check = (tag) => verifySourceTag(tag, { runner, remote: repo, keysFile });
  assert.equal(await check("v1.2.3"), onMain);
  await assert.rejects(() => check("v1.2.4"), /not signed by a CoderHelm maintainer key/);
  await assert.rejects(() => check("v1.2.5"), /lightweight tag/);
  await assert.rejects(() => check("v1.2.6"), /has no SSH signature/);
  await assert.rejects(() => check("v1.2.7"), /named "v1\.0\.0"/);
  await assert.rejects(() => check("v1.2.8"), /is not on main/);
  await assert.rejects(() => check("v9.9.9"), /could not fetch the tag v9\.9\.9/);
  // The stranger's key pinned instead: the maintainer's tag no longer passes.
  fs.writeFileSync(keysFile, JSON.stringify({ keys: [{ login: "stranger", key: pub("stranger") }] }));
  await assert.rejects(() => check("v1.2.3"), /not signed by a CoderHelm maintainer key/);
  assert.equal(await check("v1.2.4"), onMain);
});

test("install.sh pins the same maintainer keys, checks the tag first and gives cosign and gh the commit", () => {
  const sh = fs.readFileSync(path.join(here, "..", "..", "selfhost", "install.sh"), "utf8");
  const keys = parseMaintainers(fs.readFileSync(path.join(here, "..", "..", "selfhost", "maintainers.json"), "utf8"));
  const pinned = sh.match(/^SIGNERS='([^']*)'$/m);
  assert.ok(pinned, "install.sh has SIGNERS");
  assert.equal(pinned[1] + "\n", allowedSigners(keys));
  const tagAt = sh.indexOf(" verify-tag ");
  assert.ok(tagAt > 0 && tagAt < sh.indexOf("cosign verify-blob") && tagAt < sh.indexOf("gh attestation verify"));
  assert.ok(sh.includes('--certificate-github-workflow-sha "$COMMIT"'));
  assert.ok(sh.includes('--source-digest "$COMMIT"'));
  assert.ok(sh.indexOf("merge-base --is-ancestor") < sh.indexOf("cosign verify-blob"));
});
