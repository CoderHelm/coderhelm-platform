// The Jira app is Jira-only: no Confluence scope, module or call. Confluence
// goes through the Atlassian connection in CoderHelm, so a release of this
// app never asks an admin for access to a second Atlassian product.

const test = require("node:test");
const assert = require("node:assert");
const fs = require("node:fs");
const path = require("node:path");

const root = path.join(__dirname, "..");
const manifest = fs.readFileSync(path.join(root, "manifest.yml"), "utf8");

test("the manifest asks for no Confluence scope", () => {
  assert.doesNotMatch(manifest, /confluence/i);
  const scopes = manifest.split("scopes:")[1].split("external:")[0];
  assert.deepStrictEqual(
    scopes.split("\n").map((l) => l.trim()).filter((l) => l.startsWith("- ")).map((l) => l.slice(2)),
    ["read:jira-work", "write:jira-work", "storage:app", "read:attachment:jira"],
  );
});

test("no source file calls Confluence", () => {
  for (const f of fs.readdirSync(path.join(root, "src"))) {
    if (!f.endsWith(".js")) continue;
    assert.doesNotMatch(fs.readFileSync(path.join(root, "src", f), "utf8"), /requestConfluence/, f);
  }
});
