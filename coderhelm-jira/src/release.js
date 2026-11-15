// Jira issue lookups for CoderHelm (release notes and Chat). Release notes
// reach Confluence through the Atlassian connection in CoderHelm, not this app.
//
// The web trigger is called by the CoderHelm backend and require the
// shared secret the admin saved on the settings page (same secret the gateway
// verifies on forge-register).

const api = require("@forge/api");
const { storage, route } = api;
const { jqlFor, issueOf, MAX_SEARCH } = require("./issues");

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
  const res = await api.asApp().requestJira(route`/rest/api/3/issue/${key}?fields=summary,issuetype,status,assignee,priority,updated,created,comment`);
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

/**
 * POST { forge_secret, keys: ["ABC-1", ...] } or { forge_secret, query: "words" }
 * → { issues: [{key, summary, type, status, assignee, priority, updated, created, comments, links}] }
 *
 * By keys: each issue the app can read (release notes, Chat). By query: the
 * issues whose text matches, newest change first (Chat's ticket lookup).
 */
exports.getIssuesHandler = async (request) => {
  try {
    const body = JSON.parse(request.body || "{}");
    if (!(await authorized(body))) return json(403, { error: "forbidden" });
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
