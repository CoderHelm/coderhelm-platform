// A site whose app gained a web trigger after it registered (a minor
// version arrives with no lifecycle event) sends CoderHelm the new link by
// itself: on the first event after the version changes, and whenever
// CoderHelm asks through the project-list trigger.

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const {
  registerIfVersionChanged,
  registerAgainReply,
  triggerUrl,
  REGISTERED_VERSION_KEY,
} = require("../src/register");

const CONFIG = { teamId: "TEAM#1", forgeSecret: "secret-1" };

function memoryStore(initial = {}) {
  const m = new Map(Object.entries(initial));
  return { get: async (k) => m.get(k), set: async (k, v) => m.set(k, v), m };
}

test("a version change registers once; the same version again doesn't", async () => {
  const store = memoryStore({ [REGISTERED_VERSION_KEY]: "7.2.0" });
  let calls = 0;
  const register = async () => {
    calls += 1;
    return { urlsRegistered: true };
  };
  const appInfo = () => ({ appVersion: "7.3.0" });
  assert.equal(await registerIfVersionChanged(CONFIG, { store, register, appInfo }), true);
  assert.equal(store.m.get(REGISTERED_VERSION_KEY), "7.3.0");
  assert.equal(await registerIfVersionChanged(CONFIG, { store, register, appInfo }), false);
  assert.equal(calls, 1);
});

test("a site that never stored a version registers on its next event", async () => {
  const store = memoryStore();
  const register = async () => ({ urlsRegistered: true });
  const appInfo = () => ({ appVersion: "7.3.0" });
  assert.equal(await registerIfVersionChanged(CONFIG, { store, register, appInfo }), true);
});

test("a failed registration is tried again on the next event", async () => {
  const store = memoryStore();
  let ok = false;
  const register = async () => ({ urlsRegistered: ok, urlError: "Gateway returned 503" });
  const appInfo = () => ({ appVersion: "7.3.0" });
  assert.equal(await registerIfVersionChanged(CONFIG, { store, register, appInfo }), false);
  assert.equal(store.m.get(REGISTERED_VERSION_KEY), undefined);
  ok = true;
  assert.equal(await registerIfVersionChanged(CONFIG, { store, register, appInfo }), true);
});

test("an unconfigured site or an unknown version registers nothing", async () => {
  const store = memoryStore();
  const register = async () => assert.fail("must not register");
  assert.equal(await registerIfVersionChanged({ teamId: "" }, { store, register, appInfo: () => ({ appVersion: "7.3.0" }) }), false);
  assert.equal(await registerIfVersionChanged(CONFIG, { store, register, appInfo: () => ({ appVersion: "" }) }), false);
});

test("CoderHelm's request to register again is checked against the site's secret", async () => {
  let calls = 0;
  const register = async () => {
    calls += 1;
    return { urlsRegistered: true };
  };
  assert.equal(await registerAgainReply({}, CONFIG, register), null);
  const wrong = await registerAgainReply({ register_again: true, forge_secret: "nope-123" }, CONFIG, register);
  assert.equal(wrong.statusCode, 403);
  const none = await registerAgainReply({ register_again: true }, CONFIG, register);
  assert.equal(none.statusCode, 403);
  assert.equal(calls, 0);
  const ok = await registerAgainReply({ register_again: true, forge_secret: "secret-1" }, CONFIG, register);
  assert.equal(ok.statusCode, 200);
  assert.deepEqual(JSON.parse(ok.body), { registered: true });
  assert.equal(calls, 1);
});

test("a trigger this version lacks is sent empty, never failing the others", async () => {
  const getUrl = async (k) => {
    if (k === "get-issues-trigger") throw new Error("no such trigger");
    return `https://x.hello.atlassian-dev.net/x1/${k}`;
  };
  assert.equal(await triggerUrl("get-issues-trigger", getUrl), "");
  assert.match(await triggerUrl("add-comment-trigger", getUrl), /add-comment-trigger$/);
});

test("both event paths register again after a version change, and the project list answers the request", () => {
  const index = fs.readFileSync(path.join(__dirname, "..", "src", "index.js"), "utf8");
  assert.equal(index.split("await registerIfVersionChanged(config)").length - 1, 2);
  const lp = index.split("exports.listProjectsHandler")[1].split("exports.")[0];
  assert.match(lp, /registerAgainReply/);
  const manifest = fs.readFileSync(path.join(__dirname, "..", "manifest.yml"), "utf8");
  assert.match(manifest, /key: get-issues-trigger/);
});
