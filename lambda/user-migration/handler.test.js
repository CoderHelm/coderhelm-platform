// node --test lambda/user-migration/handler.test.js
"use strict";
const test = require("node:test");
const assert = require("node:assert/strict");
const { handler, secretHash } = require("./handler.js");

const EMAIL = "pat@example.com";
const PASSWORD = "correct horse battery staple";

function fakeDeps({ secret = null, password = PASSWORD, email = EMAIL, challenge = false } = {}) {
  const calls = [];
  return {
    calls,
    clientId: "client123",
    clientSecret: async () => secret,
    cognito: {
      initiateAuth: async (p) => {
        calls.push(["initiateAuth", p]);
        if (p.AuthParameters.USERNAME !== email || p.AuthParameters.PASSWORD !== password) {
          const e = new Error("Incorrect username or password.");
          e.name = "NotAuthorizedException";
          throw e;
        }
        if (challenge) return { ChallengeName: "SOFTWARE_TOKEN_MFA", Session: "s" };
        return { AuthenticationResult: { AccessToken: "tok" } };
      },
      getUser: async (p) => {
        calls.push(["getUser", p]);
        return {
          UserAttributes: [
            { Name: "email", Value: email },
            { Name: "name", Value: "Pat" },
            { Name: "sub", Value: "old-sub" },
          ],
        };
      },
    },
  };
}

function event(over = {}) {
  return {
    triggerSource: "UserMigration_Authentication",
    userName: "Pat@Example.com",
    request: { password: PASSWORD },
    response: {},
    ...over,
  };
}

test("a right password makes a confirmed account with the same email, no welcome email", async () => {
  const deps = fakeDeps();
  const out = await handler(event(), {}, deps);
  assert.deepEqual(out.response.userAttributes, { email: EMAIL, email_verified: "true", name: "Pat" });
  assert.equal(out.response.finalUserStatus, "CONFIRMED");
  assert.equal(out.response.messageAction, "SUPPRESS");
  const [, p] = deps.calls[0];
  assert.equal(p.AuthFlow, "USER_PASSWORD_AUTH");
  assert.equal(p.ClientId, "client123");
  assert.equal(p.AuthParameters.USERNAME, EMAIL);
  assert.equal(p.AuthParameters.SECRET_HASH, undefined);
  // The old pool's own id never comes along.
  assert.equal(out.response.userAttributes.sub, undefined);
});

test("a client with a secret gets the SECRET_HASH Cognito expects", async () => {
  const deps = fakeDeps({ secret: "s3cr3t" });
  await handler(event(), {}, deps);
  const [, p] = deps.calls[0];
  assert.equal(p.AuthParameters.SECRET_HASH, secretHash(EMAIL, "client123", "s3cr3t"));
});

test("a wrong password or an unknown person is refused the same way", async () => {
  await assert.rejects(handler(event({ request: { password: "nope" } }), {}, fakeDeps()), /Incorrect username or password/);
  await assert.rejects(handler(event({ userName: "who@example.com" }), {}, fakeDeps()), /Incorrect username or password/);
});

test("a second factor on the other pool can't be finished here: refused", async () => {
  await assert.rejects(handler(event(), {}, fakeDeps({ challenge: true })), /Incorrect username or password/);
});

test("the other pool's account must be the same email", async () => {
  const deps = fakeDeps();
  deps.cognito.getUser = async () => ({ UserAttributes: [{ Name: "email", Value: "someone@else.com" }] });
  await assert.rejects(handler(event(), {}, deps), /Incorrect username or password/);
});

test("forgot password and missing input are refused without asking the other pool", async () => {
  const deps = fakeDeps();
  await assert.rejects(handler(event({ triggerSource: "UserMigration_ForgotPassword" }), {}, deps));
  await assert.rejects(handler(event({ request: {} }), {}, deps));
  assert.equal(deps.calls.length, 0);
});
