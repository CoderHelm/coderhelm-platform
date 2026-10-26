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

const cosignOk = () => new FakeRunner({ "cosign version": "v2.4.1", "cosign verify-blob": "Verified OK" });
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
  assert.throws(() => checkBundleIdentity(text, {
    identity: IDENTITY, issuer: "https://token.actions.githubusercontent.com",
    repositoryUri: "https://github.com/CoderHelm/coderhelm-platform", ref: "refs/tags/v1.2.3",
  }), /signed by https:\/\/github\.com\/cli\/cli.*built from https:\/\/github\.com\/cli\/cli.*refs\/heads\/trunk/);
});

test("the identity is exact: this repository's release.yml on the release's own tag", () => {
  assert.equal(signerIdentity(TAG), IDENTITY);
  assert.throws(() => signerIdentity("v1.2.3/../../x"), /not a CoderHelm release/);
  assert.throws(() => signerIdentity("main"), /not a CoderHelm release/);
  const args = cosignArgs(TAG, "b.json", "SHA256SUMS");
  assert.equal(args[args.indexOf("--certificate-identity") + 1], IDENTITY);
  assert.equal(args[args.indexOf("--certificate-oidc-issuer") + 1], "https://token.actions.githubusercontent.com");
  assert.equal(args[args.indexOf("--certificate-github-workflow-repository") + 1], "CoderHelm/coderhelm-platform");
  assert.equal(args[args.indexOf("--certificate-github-workflow-ref") + 1], "refs/tags/v1.2.3");
  assert.ok(!args.some((a) => /regexp|insecure|ignore-tlog|offline/.test(a)), "no loose match and no skipped transparency log");
  const gh = attestationArgs(TAG, "f");
  for (const f of ["--repo", "--signer-workflow", "--source-ref", "--cert-oidc-issuer", "--deny-self-hosted-runners"]) assert.ok(gh.includes(f), f);
  assert.equal(gh[gh.indexOf("--source-ref") + 1], "refs/tags/v1.2.3");
});

for (const [signer, why] of [
  ["otherRepo", /evil\/coderhelm-platform/],
  ["otherWorkflow", /ci\.yml/],
  ["branch", /refs\/heads\/main/],
  ["otherTag", /refs\/tags\/v1\.0\.0/],
  ["otherIssuer", /accounts\.google\.com/],
]) {
  test(`a valid-looking signature from the wrong signer is refused (${signer})`, async () => {
    assert.throws(() => checkBundleIdentity(bundle(signer), {
      identity: IDENTITY, issuer: "https://token.actions.githubusercontent.com",
      repositoryUri: "https://github.com/CoderHelm/coderhelm-platform", ref: "refs/tags/v1.2.3",
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
  assert.equal(await verifySignature(releaseDir(), runner, quiet), "cosign");
  const v = runner.commands.find((c) => c.args[0] === "verify-blob");
  assert.equal(v.args[v.args.indexOf("--certificate-identity") + 1], IDENTITY);
});

test("tampered SHA256SUMS: cosign says no, the release is refused", async () => {
  const p = releaseDir();
  fs.appendFileSync(path.join(p.dir, "SHA256SUMS"), `${"0".repeat(64)}  extra.zip\n`);
  // cosign checks the signature over the file's bytes; the stub answers as cosign would.
  const runner = new FakeRunner({ "cosign version": "v2.4.1", "cosign verify-blob": { code: 1, stderr: "invalid signature when validating ASN.1 encoded signature" } });
  await assert.rejects(() => verifySignature(p, runner, quiet), /signature does not verify/);
});

test("no cosign and no gh: refused (no unchecked install, with or without --require-signature)", async () => {
  const none = new FakeRunner({ "cosign version": { code: 127 }, "gh --version": { code: 127 } });
  await assert.rejects(() => verifySignature(releaseDir(), none, quiet), /install cosign .* or the GitHub CLI/);
  await assert.rejects(() => verifySignature(releaseDir(), none, { ...quiet, require: false }), /Nothing was installed/);
});

test("gh only: every asset needs an attestation from release.yml on the tag", async () => {
  const ok = new FakeRunner({ "cosign version": { code: 127 }, "gh --version": "gh 2", "gh attestation verify": "ok" });
  assert.equal(await verifySignature(releaseDir(), ok, quiet), "attestation");
  const verified = ok.commands.filter((c) => c.args[0] === "attestation").map((c) => path.basename(c.args[2]));
  assert.deepEqual(verified, ASSETS);
  const bad = new FakeRunner({ "cosign version": { code: 127 }, "gh --version": "gh 2", "gh attestation verify": { code: 1 } });
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
