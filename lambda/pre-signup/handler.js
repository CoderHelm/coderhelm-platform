// Cognito pre-sign-up trigger for a self-hosted install: one organization,
// joined only by an admin's invite or an email at a domain the admins allow.
// The first admin from the install config starts it. Runs for every way an
// account is made in the user pool (SignUp with the app client, Google, and
// AdminCreateUser), so a direct Cognito SignUp call is refused too.
//
// The rules are the gateway's (services/gateway/src/routes/sign_in.rs,
// decide_join); both are tested against cases.json.

const SETTINGS = process.env.SETTINGS_TABLE_NAME;
const USERS = process.env.USERS_TABLE_NAME;
const INVITE_VALID_DAYS = 30;
const REFUSED =
  "This CoderHelm is private to one organization. Ask one of its admins to invite you.";

/** lowercase, and user+tag@x.io is user@x.io (as the gateway stores it). */
function normalizeEmail(email) {
  const e = String(email || "").trim().toLowerCase();
  const at = e.indexOf("@");
  if (at < 0) return e;
  return `${e.slice(0, at).split("+")[0]}${e.slice(at)}`;
}

function emailDomain(email) {
  const e = String(email || "").trim();
  const at = e.lastIndexOf("@");
  if (at <= 0) return null;
  const d = e.slice(at + 1).toLowerCase();
  return d && d.includes(".") ? d : null;
}

function inviteExpired(r, now) {
  const sent = Date.parse(r.invited_at || r.updated_at || "");
  if (Number.isNaN(sent)) return false;
  return sent + INVITE_VALID_DAYS * 86400 * 1000 <= now.getTime();
}

/**
 * The decision, from the facts: existing, invite, domain, first_admin,
 * listed or refused.
 */
function decide(f, now) {
  const isUser = (r) => String(r.sk || "").startsWith("USER#");
  const invited = (r) => r.status === "invited";
  const records = f.records || [];
  if (f.org) {
    const mine = records.filter((r) => isUser(r) && r.pk === f.org.team_id);
    if (mine.some((r) => !invited(r))) return "existing";
    if (mine.some((r) => invited(r) && !inviteExpired(r, now))) return "invite";
    const d = emailDomain(f.email);
    return d && (f.org.domains || []).includes(d) ? "domain" : "refused";
  }
  if (!f.anyone) return f.admin_listed ? "first_admin" : "refused";
  if (records.some((r) => isUser(r) && !invited(r))) return "existing";
  return f.listed ? "listed" : "refused";
}

const s = (v) => (v && typeof v.S === "string" ? v.S : undefined);

function orgFrom(item) {
  if (!item || !s(item.team_id)) return null;
  return {
    team_id: s(item.team_id),
    domains: ((item.domains && item.domains.L) || []).map((d) => String(s(d) || "").toLowerCase()),
  };
}

function recordFrom(item) {
  return { pk: s(item.pk), sk: s(item.sk), status: s(item.status), invited_at: s(item.invited_at), updated_at: s(item.updated_at) };
}

/** Everything the decision needs, read from the tables. */
async function gather(db, email) {
  const norm = normalizeEmail(email);
  const domain = emailDomain(norm);
  const get = (pk, sk, consistent) =>
    db.send("GetItemCommand", { TableName: SETTINGS, Key: { pk: { S: pk }, sk: { S: sk } }, ConsistentRead: !!consistent });
  const [org, anyone, exact, wild, recs] = await Promise.all([
    get("ORG", "META", true),
    db.send("ScanCommand", { TableName: USERS, Limit: 1, ProjectionExpression: "pk" }),
    norm ? get("ALLOWLIST", `EMAIL#${norm}`) : Promise.resolve({}),
    domain ? get("ALLOWLIST", `EMAIL#*@${domain}`) : Promise.resolve({}),
    norm
      ? db.send("QueryCommand", {
          TableName: USERS,
          IndexName: "gsi2",
          KeyConditionExpression: "gsi2pk = :pk",
          ExpressionAttributeValues: { ":pk": { S: `EMAIL#${norm}` } },
        })
      : Promise.resolve({ Items: [] }),
  ]);
  return {
    email: String(email || "").trim(),
    org: orgFrom(org.Item),
    anyone: (anyone.Count || 0) > 0,
    admin_listed: !!exact.Item && s(exact.Item.kind) !== "invite",
    listed: !!exact.Item || !!wild.Item,
    records: (recs.Items || []).map(recordFrom),
  };
}

let real;

/** DynamoDB, by command name (the Lambda runtime's SDK). */
function dynamo() {
  if (!real) {
    const sdk = require("@aws-sdk/client-dynamodb");
    const c = new sdk.DynamoDBClient({});
    real = { send: (name, input) => c.send(new sdk[name](input)) };
  }
  return real;
}

async function handler(event, _context, db = dynamo()) {
  const email = (event.request && event.request.userAttributes && event.request.userAttributes.email) || "";
  const facts = await gather(db, email);
  const outcome = decide(facts, new Date());
  console.log(JSON.stringify({ trigger: event.triggerSource, outcome }));
  if (outcome === "refused") throw new Error(REFUSED);
  return event;
}

module.exports = { handler, decide, gather, normalizeEmail, emailDomain };
