import { test } from "node:test";
import assert from "node:assert/strict";
import { leaveHybrid, registration, signRequest } from "../lib/hybrid.mjs";

test("the installer signs exactly as the services do (a fixed vector shared with sign.rs)", () => {
  assert.equal(signRequest("s3cret", 1800000000, "delete", "/hybrid/installs"), "v1=ae93880dea04941037494d54f0116ec78c7f934c8a4fceed86d64fa41d24a769");
});

test("only a real registration counts", () => {
  assert.deepEqual(registration({ hybrid_install_id: "ins_abcdefghijklmnopqrstuvwx", hybrid_install_secret: "s" }), { id: "ins_abcdefghijklmnopqrstuvwx", secret: "s" });
  assert.equal(registration({ hybrid_install_id: "nope", hybrid_install_secret: "s" }), null);
  assert.equal(registration({}), null);
  assert.equal(registration(null), null);
});

test("uninstall releases the address, signed, and never fails the uninstall", async () => {
  const logs = [];
  const calls = [];
  const ctx = { log: (m) => logs.push(m), runner: {}, fetchImpl: async (url, init) => { calls.push({ url, init }); return { ok: true, status: 200 }; } };
  const secret = { hybrid_install_id: "ins_abcdefghijklmnopqrstuvwx", hybrid_install_secret: "s3cret" };
  assert.equal(await leaveHybrid(ctx, secret), true);
  assert.equal(calls[0].url, "https://api.coderhelm.com/hybrid/installs");
  assert.equal(calls[0].init.method, "DELETE");
  assert.match(calls[0].init.headers["x-coderhelm-signature"], /^v1=[0-9a-f]{64}$/);
  const failing = { ...ctx, fetchImpl: async () => { throw new Error("offline"); } };
  assert.equal(await leaveHybrid(failing, secret), false);
  assert.equal(await leaveHybrid(ctx, {}), false, "not hybrid: nothing to do");
});
