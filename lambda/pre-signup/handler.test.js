// node --test lambda/pre-signup/handler.test.js
const test = require("node:test");
const assert = require("node:assert/strict");
const { decide, handler, run, normalizeEmail } = require("./handler.js");
const cases = require("./cases.json");

test("every shared join case decides as the gateway does", () => {
  assert.ok(cases.length >= 12);
  for (const c of cases) {
    const facts = { ...c, records: c.records || [] };
    assert.equal(decide(facts, new Date(c.now)), c.want, c.name);
  }
});

test("emails are matched the way the gateway stores them", () => {
  assert.equal(normalizeEmail(" Ann+tag@Example.COM "), "ann@example.com");
});

/** A fake DynamoDB: answers by command name and key. */
function fakeDb({ org = null, anyone = false, allow = {}, records = [] } = {}) {
  const seen = [];
  return {
    seen,
    async send(name, i) {
      seen.push(name);
      if (name === "GetItemCommand" && i.Key.pk.S === "ORG") return { Item: org || undefined };
      if (name === "GetItemCommand") return { Item: allow[i.Key.sk.S] };
      if (name === "ScanCommand") return { Count: anyone ? 1 : 0 };
      if (name === "QueryCommand") return { Items: records };
      throw new Error(`unexpected ${name}`);
    },
  };
}

const event = (email, triggerSource = "PreSignUp_SignUp") => ({
  triggerSource,
  request: { userAttributes: { email } },
  response: {},
});

test("a direct Cognito sign-up from a stranger is refused", async () => {
  const db = fakeDb({ org: { team_id: { S: "TEAM#org" }, domains: { L: [{ S: "example.com" }] } }, anyone: true });
  await assert.rejects(run(event("x@elsewhere.io"), db), /private to one organization/);
});

test("Google sign-ups are held to the same rules", async () => {
  const db = fakeDb({ org: { team_id: { S: "TEAM#org" }, domains: { L: [{ S: "example.com" }] } }, anyone: true });
  await assert.rejects(run(event("x@gmail.com", "PreSignUp_ExternalProvider"), db));
  const ok = await run(event("ann@example.com", "PreSignUp_ExternalProvider"), db);
  assert.equal(ok.triggerSource, "PreSignUp_ExternalProvider");
});

test("an invited person and the first admin get through", async () => {
  const invited = fakeDb({
    org: { team_id: { S: "TEAM#org" }, domains: { L: [] } },
    anyone: true,
    records: [{ pk: { S: "TEAM#org" }, sk: { S: "USER#invite_1" }, status: { S: "invited" }, invited_at: { S: new Date().toISOString() } }],
  });
  await run(event("bo@other.io"), invited);
  const first = fakeDb({ allow: { "EMAIL#admin@example.com": { pk: { S: "ALLOWLIST" } } } });
  await run(event("admin@example.com"), first);
  const asInvite = fakeDb({ allow: { "EMAIL#b@example.com": { kind: { S: "invite" } } } });
  await assert.rejects(run(event("b@example.com"), asInvite));
});

test("a table it can't read refuses (fails closed)", async () => {
  const broken = { async send() { throw new Error("throttled"); } };
  await assert.rejects(run(event("admin@example.com"), broken), /throttled/);
});

test("the entry point reads only the event: Lambda passes a callback third", () => {
  // Node's runtime calls handler(event, context, callback). A DynamoDB client
  // taken from the arguments would be that callback (db.send is not a function).
  assert.equal(handler.length, 1);
});

