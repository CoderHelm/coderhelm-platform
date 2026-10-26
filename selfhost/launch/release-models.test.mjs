import { test } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { PREFIXES, SOURCE, releaseModels } from "./release-models.mjs";

const source = JSON.parse(fs.readFileSync(SOURCE, "utf8"));
const RELEASE = fs.readFileSync(fileURLToPath(new URL("../../.github/workflows/release.yml", import.meta.url)), "utf8");

test("models.json lists the code's models with a Bedrock id for every prefix", () => {
  const m = releaseModels(source, "v1.2.3");
  assert.equal(m.version, "v1.2.3");
  assert.deepEqual(m.calls, source.calls);
  for (const model of m.calls) {
    assert.equal(m.anthropic[model], model);
    assert.deepEqual(Object.keys(m.bedrock[model]), PREFIXES);
    assert.equal(m.bedrock[model]["eu."], `eu.anthropic.${model}`);
    assert.equal(m.bedrock[model][""], `anthropic.${model}`);
  }
  // A model whose Bedrock id carries a date gets it.
  assert.equal(m.bedrock["claude-haiku-4-5"]["us."], "us.anthropic.claude-haiku-4-5-20251001-v1:0");
});

test("refuses a bad tag or an empty manifest", () => {
  assert.throws(() => releaseModels(source, "latest"), /not a release tag/);
  assert.throws(() => releaseModels({ schema: 1, calls: [] }, "v1.0.0"), /non-empty calls/);
});

test("the command prints the document", () => {
  const out = JSON.parse(execFileSync(process.execPath, [fileURLToPath(new URL("./release-models.mjs", import.meta.url)), "v0.9.0"], { encoding: "utf8" }));
  assert.equal(out.version, "v0.9.0");
  assert.deepEqual(out.calls, source.calls);
});

test("the release workflow publishes models.json, checksummed, from the code", () => {
  assert.match(RELEASE, /node selfhost\/launch\/release-models\.mjs "\$TAG" > dist\/models\.json/);
  const sums = RELEASE.split("\n").find((l) => l.includes("> SHA256SUMS"));
  assert.ok(sums && sums.includes("models.json"), "models.json is in SHA256SUMS");
});
