import { test } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import { fileURLToPath } from "node:url";
import { latestDoc, stampTemplate, templateUrl } from "./stamp-template.mjs";

const TEMPLATE = fs.readFileSync(fileURLToPath(new URL("../../infra/cfn/coderhelm-selfhost-launch.yaml", import.meta.url)), "utf8");

test("the real template is stamped with the release tag, and only Version changes", () => {
  const out = stampTemplate(TEMPLATE, "v0.7.0");
  assert.match(out, /\n {2}Version:\n(?: {4}.*\n|\s*\n)*? {4}Default: v0\.7\.0\n/);
  const diff = TEMPLATE.split("\n").filter((l, i) => l !== out.split("\n")[i]);
  assert.deepEqual(diff, ["    Default: latest"]);
});

test("refuses what isn't a tag, or a template it can't stamp", () => {
  assert.throws(() => stampTemplate(TEMPLATE, "latest"), /not a release tag/);
  assert.throws(() => stampTemplate("Parameters:\n  Stage:\n    Default: prod\n", "v1.0.0"), /no Version/);
  assert.throws(() => stampTemplate(stampTemplate(TEMPLATE, "v0.7.0"), "v0.7.1"), /no `Default: latest`/);
});

test("published by version in the public bucket", () => {
  assert.equal(templateUrl("v0.7.0"), "https://coderhelm-public.s3.amazonaws.com/launch/v0.7.0/coderhelm-launch.yaml");
  assert.deepEqual(latestDoc("v0.7.0", new Date(0)), {
    version: "v0.7.0",
    templateUrl: "https://coderhelm-public.s3.amazonaws.com/launch/v0.7.0/coderhelm-launch.yaml",
    publishedAt: "1970-01-01T00:00:00.000Z",
  });
});
