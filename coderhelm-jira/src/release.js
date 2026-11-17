// Jira issue lookups for CoderHelm (release notes and Chat). Release notes
// reach Confluence through the Atlassian connection in CoderHelm, not this app.
//
// The web trigger is called by the CoderHelm backend and require the
// shared secret the admin saved on the settings page (same secret the gateway
// verifies on forge-register).

const api = require("@forge/api");
const { storage, route } = api;
const { jqlFor, mineJql, issueOf, MAX_SEARCH, MAX_MINE } = require("./issues");

const json = (statusCode, obj) => ({
  statusCode,
  headers: { "Content-Type": ["application/json"] },
  body: JSON.stringify(obj),
});

async function authorized(body) {
  const config = await storage.get("coderhelm-config");
  return Boolean(config && config.forgeSecret && body.forge_secret === config.forgeSecret);
}

// ── Issue lookups (release notes and CoderHelm's Chat) ───────────────────────

/** One issue with its remote links; null when it can't be read. */
async function readIssue(key) {
  const res = await api
    .asApp()
    .requestJira(route`/rest/api/3/issue/${key}?fields=summary,issuetype,status,assignee,reporter,priority,duedate,updated,created,comment`);
  if (!res.ok) return null; // unknown key / no access → dropped
  const raw = await res.json();
  let links = [];
  try {
    const l = await api.asApp().requestJira(route`/rest/api/3/issue/${key}/remotelink`);
    if (l.ok) links = await l.json();
  } catch (_) {
    // the issue without its links
  }
  return issueOf(raw, links);
}

/** The issue keys a JQL search finds (up to `max`), or [] when it can't run. */
async function keysFor(jql, max) {
  const res = await api.asApp().requestJira(route`/rest/api/3/search/jql`, {
    method: "POST",
    headers: { Accept: "application/json", "Content-Type": "application/json" },
    body: JSON.stringify({ jql, maxResults: max, fields: ["summary"] }),
  });
  if (!res.ok) return { keys: [], ok: false };
  const found = await res.json();
  return { keys: ((found && found.issues) || []).map((i) => i.key).filter(Boolean).slice(0, max), ok: true };
}

/** Reads issues by key, a few at a time, skipping any that can't be read. */
async function readIssues(keys) {
  const issues = [];
  for (let i = 0; i < keys.length; i += 5) {
    const batch = await Promise.all(keys.slice(i, i + 5).map((k) => readIssue(k).catch(() => null)));
    issues.push(...batch.filter(Boolean));
  }
  return issues;
}

/**
 * The "what is waiting on me" read: the open issues assigned to the person,
 * and the open ones they watch or reported that changed in the last two
 * weeks (CoderHelm reads those for questions to them they haven't answered).
 */
async function mineHandler(mine) {
  // CoderHelm sends the person's account id (from the Jira events it has
  // seen); the app reads no user directory, so it needs no user scope.
  const accountId = String((mine && mine.account_id) || "");
  const jql = mineJql(accountId);
  if (!jql) {
    return json(200, { issues: [], mine: { found: false, why: "no Jira account id was sent" } });
  }
  const assigned = await keysFor(jql.assigned, MAX_MINE);
  // Watchers can be hidden from the app; then only assigned issues are read.
  const involved = await keysFor(jql.involved, MAX_MINE).catch(() => ({ keys: [], ok: false }));
  const keys = [...new Set([...assigned.keys, ...involved.keys])];
  return json(200, {
    issues: await readIssues(keys),
    mine: {
      found: true,
      account_id: accountId,
      assigned: assigned.keys,
      assigned_read: assigned.ok,
      involved_read: involved.ok,
    },
  });
}

/**
 * POST { forge_secret, keys: ["ABC-1", ...] } or { forge_secret, query: "words" }
 * or { forge_secret, mine: { account_id?, email? } }
 * → { issues: [{key, summary, type, status, assignee, priority, updated, created, comments, links}] }
 *
 * By keys: each issue the app can read (release notes, Chat). By query: the
 * issues whose text matches, newest change first (Chat's ticket lookup). By
 * mine: what is waiting on one person (Chat's "what needs me"), with a `mine`
 * object that says whose it is; an older app version leaves `mine` out.
 */
exports.getIssuesHandler = async (request) => {
  try {
    const body = JSON.parse(request.body || "{}");
    if (!(await authorized(body))) return json(403, { error: "forbidden" });
    if (body.mine && typeof body.mine === "object") return await mineHandler(body.mine);
    let keys = (Array.isArray(body.keys) ? body.keys : [])
      .filter((k) => /^[A-Z][A-Z0-9]+-\d+$/.test(k))
      .slice(0, 50);
    const jql = jqlFor(body.query);
    if (!keys.length && jql) {
      const res = await api.asApp().requestJira(route`/rest/api/3/search/jql`, {
        method: "POST",
        headers: { Accept: "application/json", "Content-Type": "application/json" },
        body: JSON.stringify({ jql, maxResults: MAX_SEARCH, fields: ["summary"] }),
      });
      if (res.ok) {
        const found = await res.json();
        keys = ((found && found.issues) || []).map((i) => i.key).filter(Boolean).slice(0, MAX_SEARCH);
      }
    }
    const issues = [];
    for (const key of keys) {
      try {
        const issue = await readIssue(key);
        if (issue) issues.push(issue);
      } catch (_) {
        // skip this key
      }
    }
    return json(200, { issues });
  } catch (e) {
    console.error("getIssues error:", e);
    return json(500, { error: String(e.message || e) });
  }
};

exports.authorized = authorized;

exports.json = json;
