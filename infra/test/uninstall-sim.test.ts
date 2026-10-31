// The guided uninstall's deletion rights, as AWS's IAM policy simulator sees
// them. test/fixtures/uninstall-sim.json comes from
// `python3 scripts/deploy-role-sim.py --uninstall` (read-only in AWS:
// SimulateCustomPolicy, and AWS's public service reference). CI has no AWS
// credentials, so these tests replay it: the simulator allowed every call on
// the install's pieces and refused every other one, the local evaluator
// agrees with it on each, the policies it saw are today's, and every
// condition key the window relies on is one AWS evaluates for that action.
//
// After changing infra/lib/uninstall-access.ts or uninstall-cases.ts, run
// the script again.

import { test } from "node:test";
import * as assert from "node:assert/strict";
import * as fs from "node:fs";
import * as path from "node:path";
import { PolicyDocument } from "../lib/access";
import { casePolicies, Mode } from "../lib/uninstall-cases";
import { decision } from "./iam-eval";

type Verdict = "allowed" | "explicitDeny" | "implicitDeny";
interface Row { why: string; action: string; resource: string; context?: Record<string, string>; allowed: Mode[]; keep: Verdict; everything: Verdict }
interface Fixture {
  account: string; userId: string; now: string; expires: string;
  policies: Record<Mode, { idle: PolicyDocument; window: PolicyDocument; boundary: PolicyDocument }>;
  conditionKeys: { statement: string; action: string; key: string; listed: boolean }[];
  rows: Row[];
  session: (Row & { anotherSession: Verdict; afterTheEnd: Verdict })[];
}

const F: Fixture = JSON.parse(fs.readFileSync(path.join(__dirname, "..", "..", "test", "fixtures", "uninstall-sim.json"), "utf8"));
const MODES: Mode[] = ["keep", "everything"];
const ctx = (r: Row, extra: Record<string, string> = {}) => ({ "aws:userid": F.userId, "aws:CurrentTime": F.now, ...(r.context ?? {}), ...extra });

test("the fixture is AWS's verdict on an example account only", () => {
  assert.equal(F.account, "111122223333");
  const accounts = new Set([...JSON.stringify(F).matchAll(/:(\d{12}):/g)].map((m) => m[1]));
  assert.deepEqual([...accounts], ["111122223333"]);
  assert.ok(F.rows.length >= 80, `${F.rows.length} calls`);
});

test("the policies the simulator saw are today's (run scripts/deploy-role-sim.py --uninstall after a change)", () => {
  for (const m of MODES) assert.deepEqual(F.policies[m], JSON.parse(JSON.stringify(casePolicies(m))), m);
});

test("AWS's simulator: every call on the install's pieces allowed, every other call refused, in both modes", () => {
  const wrong = F.rows.flatMap((r) => MODES.filter((m) => (r[m] === "allowed") !== r.allowed.includes(m)).map((m) => `${m}: ${r.why} (${r.action} ${r.resource}): ${r[m]}`));
  assert.deepEqual(wrong, []);
  // Something of each kind is refused, not only allowed.
  assert.ok(F.rows.filter((r) => !r.allowed.length).length >= 40);
});

test("the local evaluator gives the simulator's verdict on every call", () => {
  const wrong = F.rows.flatMap((r) => MODES.filter((m) => {
    const p = F.policies[m];
    return decision([p.idle, p.window], p.boundary, { action: r.action, resource: r.resource, context: ctx(r) }) !== r[m];
  }).map((m) => `${m}: ${r.why}`));
  assert.deepEqual(wrong, []);
});

test("another session of the build role, and the build after its window's end, may delete nothing", () => {
  for (const r of F.session) {
    const idle = r.action === "s3:PutObject" || r.action === "iam:DeleteRolePolicy";
    if (idle) continue;
    assert.equal(r.anotherSession, "explicitDeny", r.why);
    assert.equal(r.afterTheEnd, "explicitDeny", r.why);
  }
});

test("every condition key the window's allows use is one AWS evaluates for that action (service reference)", () => {
  assert.ok(F.conditionKeys.length > 10);
  assert.deepEqual(F.conditionKeys.filter((k) => !k.listed), []);
});
