// Cognito UserMigration trigger (self-hosted installs with migrateUsersFrom).
//
// When someone signs in with an email this pool doesn't know yet, Cognito
// asks here. The password is checked against the other CoderHelm pool's app
// client (InitiateAuth with USER_PASSWORD_AUTH, then GetUser with the token
// it returns); on success the account is made here with the same password,
// already confirmed, and no welcome email. Nothing else comes from the other
// pool. A wrong password, an unknown person, a second factor on the other
// pool, or anything unexpected refuses: the person then signs up normally.
//
// The password is used for that one call and never logged or stored.
"use strict";

const crypto = require("node:crypto");

/** SECRET_HASH for an app client with a secret. */
function secretHash(username, clientId, clientSecret) {
  return crypto.createHmac("sha256", clientSecret).update(username + clientId).digest("base64");
}

/** The attributes the new account gets: email (verified) and name. */
function attributesFrom(userAttributes, email) {
  const get = (n) => (userAttributes || []).find((a) => a.Name === n)?.Value;
  const out = { email, email_verified: "true" };
  const name = get("name");
  if (name) out.name = name;
  return out;
}

/** What the trigger answers, or throws to refuse. `deps` are injected in tests. */
async function migrate(event, deps) {
  if (event.triggerSource !== "UserMigration_Authentication") {
    // Forgot password: the other pool can't be asked without its admin
    // rights. The person signs in once with their password first.
    throw new Error("Sign in with your password first.");
  }
  const email = String(event.userName || "").trim().toLowerCase();
  const password = event.request && event.request.password;
  if (!email || !password) throw new Error("Bad request");

  const params = {
    AuthFlow: "USER_PASSWORD_AUTH",
    ClientId: deps.clientId,
    AuthParameters: { USERNAME: email, PASSWORD: password },
  };
  const clientSecret = await deps.clientSecret();
  if (clientSecret) params.AuthParameters.SECRET_HASH = secretHash(email, deps.clientId, clientSecret);

  let auth;
  try {
    auth = await deps.cognito.initiateAuth(params);
  } catch (e) {
    // Wrong password, unknown user, unconfirmed: the same refusal for all.
    throw new Error("Incorrect username or password.");
  }
  const token = auth && auth.AuthenticationResult && auth.AuthenticationResult.AccessToken;
  if (!token) {
    // A challenge (second factor, new password): can't finish here.
    throw new Error("Incorrect username or password.");
  }
  const user = await deps.cognito.getUser({ AccessToken: token });
  const attrs = user.UserAttributes || [];
  const theirEmail = (attrs.find((a) => a.Name === "email")?.Value || "").toLowerCase();
  if (theirEmail !== email) throw new Error("Incorrect username or password.");

  event.response = event.response || {};
  event.response.userAttributes = attributesFrom(attrs, email);
  event.response.finalUserStatus = "CONFIRMED";
  event.response.messageAction = "SUPPRESS";
  return event;
}

let cached;
function realDeps() {
  if (cached) return cached;
  const { CognitoIdentityProviderClient, InitiateAuthCommand, GetUserCommand } = require("@aws-sdk/client-cognito-identity-provider");
  const client = new CognitoIdentityProviderClient({ region: process.env.FROM_REGION });
  let secret;
  cached = {
    clientId: process.env.FROM_CLIENT_ID,
    cognito: {
      initiateAuth: (p) => client.send(new InitiateAuthCommand(p)),
      getUser: (p) => client.send(new GetUserCommand(p)),
    },
    clientSecret: async () => {
      if (!process.env.SECRET_KEY) return null;
      if (secret === undefined) {
        const { SecretsManagerClient, GetSecretValueCommand } = require("@aws-sdk/client-secrets-manager");
        const sm = new SecretsManagerClient({});
        const out = await sm.send(new GetSecretValueCommand({ SecretId: process.env.SECRET_NAME }));
        secret = JSON.parse(out.SecretString || "{}")[process.env.SECRET_KEY] || null;
      }
      return secret;
    },
  };
  return cached;
}

exports.handler = async (event, _context, deps) => migrate(event, deps || realDeps());
exports.migrate = migrate;
exports.secretHash = secretHash;
exports.attributesFrom = attributesFrom;
