const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const { appInfo, versionFields } = require("../src/version");

const APP = "ari:cloud:ecosystem::app/00000000-0000-0000-0000-000000000001";

test("the running version and app come from the Forge app context", () => {
  const ari = { toString: () => APP };
  assert.deepEqual(appInfo(() => ({ appVersion: "6.11.0", appAri: ari })), { appVersion: "6.11.0", appId: APP });
  assert.deepEqual(versionFields(() => ({ appVersion: "6.11.0", appAri: APP })), { app_version: "6.11.0", app_id: APP });
});

test("without a context the call still goes out, only without a version", () => {
  const thrown = () => {
    throw new Error("not in Forge");
  };
  assert.deepEqual(appInfo(thrown), { appVersion: "", appId: "" });
  assert.deepEqual(appInfo(() => undefined), { appVersion: "", appId: "" });
  assert.deepEqual(versionFields(thrown), { app_version: undefined, app_id: undefined });
  // Outside Forge, the real getAppContext throws: no crash either.
  assert.deepEqual(versionFields(), { app_version: undefined, app_id: undefined });
});

test("every event and every registration sent to CoderHelm carries the version", () => {
  const src = (f) => fs.readFileSync(path.join(__dirname, "..", "src", f), "utf8");
  const index = src("index.js");
  // Both payloads (comment events and issue events) sent to /webhooks/jira.
  assert.equal(index.split("${GATEWAY_URL}/webhooks/jira").length - 1, 2);
  assert.equal(index.split("...versionFields(),").length - 1, 2);
  const register = src("register.js");
  assert.ok(register.includes("/integrations/jira/forge-register"));
  assert.ok(register.includes("...versionFields(),"));
});
