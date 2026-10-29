// The update build's and the Launch Stack's tag check (infra/lib/release-verify.ts),
// run as bash against a real git repository: only an annotated tag named
// $VERSION, on main, and either signed with a pinned maintainer key or
// attested by auto-release.yml on main for that tag and commit, passes, and it
// sets $COMMIT to the commit cosign must then find in the signing certificate.

import { test } from "node:test";
import * as assert from "node:assert/strict";
import { execFileSync, spawnSync } from "child_process";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { ATTESTATION_BEGIN, ATTESTATION_END, maintainerKeys, RELEASE_REPO, VERIFY_SUMS, verifyTagCommands } from "../lib/release-verify";

test("the pinned maintainer keys are SSH public keys; a broken file is refused", () => {
  const keys = maintainerKeys();
  assert.ok(keys.length >= 1);
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "ch-keys-"));
  const f = path.join(dir, "m.json");
  fs.writeFileSync(f, JSON.stringify({ keys: [] }));
  assert.throws(() => maintainerKeys(f), /lists no maintainer keys/);
  fs.writeFileSync(f, JSON.stringify({ keys: [{ login: "x", key: "ssh-ed25519 AAAA x@y" }] }));
  assert.throws(() => maintainerKeys(f), /not an SSH public key/);
  fs.writeFileSync(f, JSON.stringify({ keys: [{ login: "x'; rm -rf /", key: "ssh-ed25519 AAAA" }] }));
  assert.throws(() => maintainerKeys(f), /not a GitHub login/);
});

test("cosign is given the tag's commit", () => {
  assert.match(VERIFY_SUMS, /--certificate-github-workflow-sha "\$COMMIT"/);
  assert.doesNotMatch(VERIFY_SUMS, /identity-regexp|insecure|ignore-tlog|offline/);
});

test("the build's tag check, with real git: only a maintainer-signed annotated tag on main passes", () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "ch-tagcheck-"));
  const env = {
    ...process.env, GIT_CONFIG_GLOBAL: "/dev/null", GIT_CONFIG_NOSYSTEM: "1",
    GIT_AUTHOR_NAME: "M", GIT_AUTHOR_EMAIL: "m@example.com", GIT_COMMITTER_NAME: "M", GIT_COMMITTER_EMAIL: "m@example.com",
  };
  const sh = (cmd: string, args: string[], cwd = root) => execFileSync(cmd, args, { cwd, env, encoding: "utf8" }).trim();
  for (const k of ["maintainer", "stranger"]) sh("ssh-keygen", ["-q", "-t", "ed25519", "-N", "", "-C", "", "-f", path.join(root, k)]);
  const pub = (k: string) => fs.readFileSync(path.join(root, `${k}.pub`), "utf8").trim().split(" ").slice(0, 2).join(" ");
  const repo = path.join(root, "repo");
  fs.mkdirSync(repo);
  const git = (...a: string[]) => sh("git", a, repo);
  git("init", "-q", "-b", "main");
  git("commit", "-q", "--allow-empty", "-m", "on main");
  const onMain = git("rev-parse", "HEAD");
  const sign = (key: string, ...a: string[]) => git("-c", "gpg.format=ssh", "-c", `user.signingkey=${path.join(root, key)}`, "tag", "-s", ...a);
  sign("maintainer", "v1.2.3", "-m", "v1.2.3");
  sign("stranger", "v1.2.4", "-m", "v1.2.4");
  git("tag", "v1.2.5");
  git("tag", "-a", "v1.2.6", "-m", "unsigned");
  sign("maintainer", "v1.0.0", "-m", "v1.0.0");
  git("update-ref", "refs/tags/v1.2.7", git("rev-parse", "refs/tags/v1.0.0"));
  git("checkout", "-q", "-b", "feature");
  git("commit", "-q", "--allow-empty", "-m", "not on main");
  sign("maintainer", "v1.2.8", "-m", "v1.2.8");
  git("checkout", "-q", "main");

  const lines = verifyTagCommands([{ login: "maintainer", key: pub("maintainer") }])
    .map((l) => l.split(`https://github.com/${RELEASE_REPO}.git`).join(repo));
  const run = (version: string) => {
    const r = spawnSync("bash", ["-c", ["set -euo pipefail", ...lines, 'echo "COMMIT=$COMMIT"'].join("\n")], {
      env: { ...env, VERSION: version }, encoding: "utf8",
    });
    return { code: r.status, out: `${r.stdout}${r.stderr}` };
  };
  const ok = run("v1.2.3");
  assert.equal(ok.code, 0, ok.out);
  assert.match(ok.out, new RegExp(`COMMIT=${onMain}`));
  for (const [v, why] of [
    ["v1.2.4", /not signed by a CoderHelm maintainer key/],
    ["v1.2.5", /not an annotated tag/],
    ["v1.2.6", /has no SSH signature and no release attestation/],
    ["v1.2.7", /named for another version/],
    ["v1.2.8", /is not on main/],
    ["v9.9.9", /couldn't find remote ref|not our ref|fatal/],
  ] as const) {
    const r = run(v);
    assert.notEqual(r.code, 0, `${v} must fail: ${r.out}`);
    assert.match(r.out, why, v);
  }
});

// cosign stood in for: the "bundle" names who signed (workflow identity, ref,
// commit) and the statement signed, and the stand-in says yes only when the
// flags it is given match them and the file is that statement, as cosign does
// with a real certificate and signature.
const FAKE_COSIGN = `#!/usr/bin/env node
const a = process.argv.slice(2);
const v = (f) => a[a.indexOf(f) + 1];
const fs = require("fs");
const b = JSON.parse(fs.readFileSync(v("--bundle"), "utf8"));
const ok = v("--certificate-identity") === b.identity && v("--certificate-github-workflow-ref") === b.ref
  && v("--certificate-github-workflow-sha") === b.sha && v("--certificate-oidc-issuer") === "https://token.actions.githubusercontent.com"
  && fs.readFileSync(a[a.length - 1], "utf8") === b.statement;
process.exit(ok ? 0 : 1);
`;

test("the build's tag check, with real git: an automatic release's tag needs auto-release.yml on main for that tag and commit", () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "ch-autotag-"));
  const env = {
    ...process.env, GIT_CONFIG_GLOBAL: "/dev/null", GIT_CONFIG_NOSYSTEM: "1",
    GIT_AUTHOR_NAME: "M", GIT_AUTHOR_EMAIL: "m@example.com", GIT_COMMITTER_NAME: "M", GIT_COMMITTER_EMAIL: "m@example.com",
  };
  const sh = (cmd: string, args: string[], cwd = root) => execFileSync(cmd, args, { cwd, env, encoding: "utf8" }).trim();
  sh("ssh-keygen", ["-q", "-t", "ed25519", "-N", "", "-C", "", "-f", path.join(root, "maintainer")]);
  const pub = fs.readFileSync(path.join(root, "maintainer.pub"), "utf8").trim().split(" ").slice(0, 2).join(" ");
  const repo = path.join(root, "repo");
  fs.mkdirSync(repo);
  const git = (...a: string[]) => sh("git", a, repo);
  git("init", "-q", "-b", "main");
  git("commit", "-q", "--allow-empty", "-m", "on main");
  const onMain = git("rev-parse", "HEAD");
  const AUTO = `https://github.com/${RELEASE_REPO}/.github/workflows/auto-release.yml@refs/heads/main`;
  const statement = (tag: string, sha: string) => `coderhelm-release-tag v1\nrepository ${RELEASE_REPO}\ntag ${tag}\ncommit ${sha}\n`;
  const tagWith = (name: string, b: object | null, { blocks = 1 } = {}) => {
    const b64 = b ? Buffer.from(JSON.stringify(b)).toString("base64").replace(/(.{76})/g, "$1\n").trim() : "";
    const block = `${ATTESTATION_BEGIN}\n${b64}\n${ATTESTATION_END}\n`;
    const f = path.join(root, `${name}.msg`);
    fs.writeFileSync(f, `${name}\n\nReleased automatically.\n\n${b ? block.repeat(blocks) : ""}`);
    git("tag", "-a", name, "-F", f);
  };
  const good = (tag: string, sha = onMain) => ({ identity: AUTO, ref: "refs/heads/main", sha, statement: statement(tag, sha) });
  tagWith("v3.0.0", good("v3.0.0"));
  tagWith("v3.0.1", { ...good("v3.0.1"), identity: `https://github.com/${RELEASE_REPO}/.github/workflows/deploy.yml@refs/heads/main` });
  tagWith("v3.0.2", { ...good("v3.0.2"), identity: AUTO.replace("refs/heads/main", "refs/heads/feature"), ref: "refs/heads/feature" });
  tagWith("v3.0.3", good("v3.0.3", "2".repeat(40)));
  tagWith("v3.0.4", good("v3.0.0"));
  tagWith("v3.0.5", null);
  tagWith("v3.0.6", good("v3.0.6"), { blocks: 2 });
  git("checkout", "-q", "-b", "feature");
  git("commit", "-q", "--allow-empty", "-m", "not on main");
  tagWith("v3.0.7", good("v3.0.7", git("rev-parse", "HEAD")));
  git("checkout", "-q", "main");
  const bin = path.join(root, "bin");
  fs.mkdirSync(bin);
  fs.writeFileSync(path.join(bin, "cosign"), FAKE_COSIGN, { mode: 0o755 });

  const lines = verifyTagCommands([{ login: "maintainer", key: pub }])
    .map((l) => l.split(`https://github.com/${RELEASE_REPO}.git`).join(repo));
  const run = (version: string) => {
    const r = spawnSync("bash", ["-c", ["set -euo pipefail", ...lines, 'echo "COMMIT=$COMMIT"'].join("\n")], {
      env: { ...env, VERSION: version, PATH: `${bin}${path.delimiter}${process.env.PATH}` }, encoding: "utf8",
    });
    return { code: r.status, out: `${r.stdout}${r.stderr}` };
  };
  const ok = run("v3.0.0");
  assert.equal(ok.code, 0, ok.out);
  assert.match(ok.out, /released by auto-release\.yml on main/);
  assert.match(ok.out, new RegExp(`COMMIT=${onMain}`));
  for (const [v, why] of [
    ["v3.0.1", /not from auto-release\.yml on main/],   // another workflow
    ["v3.0.2", /not from auto-release\.yml on main/],   // another ref
    ["v3.0.3", /not from auto-release\.yml on main/],   // another commit
    ["v3.0.4", /not from auto-release\.yml on main/],   // another tag's statement
    ["v3.0.5", /has no SSH signature and no release attestation/],
    ["v3.0.6", /has no SSH signature and no release attestation/],
    ["v3.0.7", /is not on main/],
  ] as const) {
    const r = run(v);
    assert.notEqual(r.code, 0, `${v} must fail: ${r.out}`);
    assert.match(r.out, why, v);
  }
});
