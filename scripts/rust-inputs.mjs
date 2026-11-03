#!/usr/bin/env node
// Prints one sha256 for everything the Rust Lambdas (gateway, worker) are
// built from at a commit: every file under services/ (sources, Cargo.toml,
// Cargo.lock, rust-toolchain.toml), every file outside it that the code pulls
// in with include_str!/include_bytes!, and scripts/rust-build.sh (the build
// command). Two commits with the same digest build the same Lambdas, so the
// deploy reuses an earlier build and the release reuses the deploy's
// (.github/workflows/deploy.yml, .github/workflows/release.yml).
//
// Reads git objects only (not the working tree). An include whose path isn't
// a plain string literal, or a file it names that doesn't exist, makes the
// digest "unknown": callers then build.
//
//   node scripts/rust-inputs.mjs [commit]      # default HEAD
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import path from "node:path";

const commit = process.argv[2] || "HEAD";
const git = (args, input) =>
  execFileSync("git", args, { input, maxBuffer: 1 << 28, encoding: input === undefined ? "utf8" : "buffer" });

// mode type sha<TAB>path, for every file under services/ at the commit.
const tree = git(["ls-tree", "-r", "--full-tree", commit, "--", "services"])
  .split("\n")
  .filter(Boolean)
  .map((l) => {
    const [meta, p] = l.split("\t");
    const [mode, type, sha] = meta.split(" ");
    return { mode, type, sha, path: p };
  });
if (!tree.length) {
  console.log("unknown");
  process.exit(0);
}

// Read every .rs blob in one git process.
const rs = tree.filter((e) => e.type === "blob" && e.path.endsWith(".rs"));
const batch = git(["cat-file", "--batch"], Buffer.from(rs.map((e) => e.sha).join("\n") + "\n"));
const texts = [];
let at = 0;
for (let i = 0; i < rs.length; i++) {
  const nl = batch.indexOf(10, at);
  const size = Number(batch.subarray(at, nl).toString().split(" ")[2]);
  texts.push(batch.subarray(nl + 1, nl + 1 + size).toString("utf8"));
  at = nl + 1 + size + 1;
}

const INCLUDE = /include_(?:str|bytes)!\s*\(\s*(concat!\s*\()?([^)]*)\)/g;
const outside = new Set();
for (let i = 0; i < rs.length; i++) {
  for (const m of texts[i].matchAll(INCLUDE)) {
    const lit = m[1] ? null : /^\s*"([^"\\]+)"\s*,?\s*$/.exec(m[2]);
    if (!lit) {
      console.log("unknown");
      process.exit(0);
    }
    const p = path.posix.normalize(path.posix.join(path.posix.dirname(rs[i].path), lit[1]));
    if (p.startsWith("..")) {
      console.log("unknown");
      process.exit(0);
    }
    if (!p.startsWith("services/")) outside.add(p);
  }
}

const extra = [...outside, "scripts/rust-build.sh"].sort().map((p) => {
  const line = git(["ls-tree", "--full-tree", commit, "--", p]).trim();
  if (!line) return null;
  const [meta] = line.split("\t");
  const [mode, type, sha] = meta.split(" ");
  return { mode, type, sha, path: p };
});
if (extra.includes(null)) {
  console.log("unknown");
  process.exit(0);
}

const h = createHash("sha256");
h.update("coderhelm-rust-inputs v1\n");
for (const e of [...tree, ...extra].sort((a, b) => (a.path < b.path ? -1 : a.path > b.path ? 1 : 0))) {
  h.update(`${e.mode} ${e.type} ${e.sha}\t${e.path}\n`);
}
console.log(h.digest("hex"));
