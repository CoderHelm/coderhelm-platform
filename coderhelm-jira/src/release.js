// Release notes: Confluence publishing + Jira lookups for CoderHelm.
//
// Both web triggers are called by the CoderHelm backend and require the
// shared secret the admin saved on the settings page (same secret the gateway
// verifies on forge-register).

const api = require("@forge/api");
const { storage, route } = api;

const json = (statusCode, obj) => ({
  statusCode,
  headers: { "Content-Type": ["application/json"] },
  body: JSON.stringify(obj),
});

async function authorized(body) {
  const config = await storage.get("coderhelm-config");
  return Boolean(config && config.forgeSecret && body.forge_secret === config.forgeSecret);
}

// ── Pure helpers (exported for tests) ────────────────────────────────────────

/** Anchor macro that marks one CoderHelm entry, so a re-publish replaces it. */
function anchorFor(entryKey) {
  const slug = `coderhelm-${String(entryKey).replace(/[^A-Za-z0-9._-]+/g, "-")}`;
  return `<ac:structured-macro ac:name="anchor"><ac:parameter ac:name="">${slug}</ac:parameter></ac:structured-macro>`;
}

/**
 * Put `entryHtml` (marked with its anchor) at the top of the year page — right
 * after the page's first <h1> when there is one — or, when the page already has
 * this entry, replace it in place. An entry runs from its anchor to the next
 * anchor or the next <h3> after its own heading.
 */
function upsertEntry(pageHtml, entryKey, entryHtml) {
  const anchor = anchorFor(entryKey);
  const block = anchor + entryHtml;
  const at = pageHtml.indexOf(anchor);
  if (at >= 0) {
    const afterAnchor = at + anchor.length;
    const ownHeading = pageHtml.indexOf("<h3", afterAnchor);
    const nextAnchor = pageHtml.indexOf('<ac:structured-macro ac:name="anchor">', afterAnchor);
    const nextHeading = ownHeading >= 0 ? pageHtml.indexOf("<h3", ownHeading + 3) : -1;
    const ends = [nextAnchor, nextHeading].filter((i) => i >= 0);
    const end = ends.length ? Math.min(...ends) : pageHtml.length;
    return pageHtml.slice(0, at) + block + pageHtml.slice(end);
  }
  const h1 = pageHtml.indexOf("</h1>");
  if (h1 >= 0) {
    const cut = h1 + "</h1>".length;
    return pageHtml.slice(0, cut) + block + pageHtml.slice(cut);
  }
  return block + pageHtml;
}

// ── Confluence calls ─────────────────────────────────────────────────────────

async function confluence(path, init) {
  const res = await api.asApp().requestConfluence(path, init);
  const text = await res.text();
  let data = null;
  try {
    data = text ? JSON.parse(text) : null;
  } catch (_) {
    data = text;
  }
  if (!res.ok) {
    throw new Error(`Confluence ${res.status}: ${typeof data === "string" ? data : JSON.stringify(data)}`.slice(0, 600));
  }
  return data;
}

async function childByTitle(parentId, title) {
  let cursor = "";
  for (let i = 0; i < 20; i++) {
    const page = await confluence(
      cursor
        ? route`/wiki/api/v2/pages/${parentId}/children?limit=250&cursor=${cursor}`
        : route`/wiki/api/v2/pages/${parentId}/children?limit=250`
    );
    const hit = (page.results || []).find((p) => p.title === title);
    if (hit) return hit;
    const next = page._links && page._links.next;
    if (!next) return null;
    const m = /cursor=([^&]+)/.exec(next);
    if (!m) return null;
    cursor = decodeURIComponent(m[1]);
  }
  return null;
}

async function findOrCreateChild(spaceId, parentId, title, seedHtml) {
  const existing = await childByTitle(parentId, title);
  if (existing) return existing.id;
  const created = await confluence(route`/wiki/api/v2/pages`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      spaceId,
      status: "current",
      title,
      parentId,
      body: { representation: "storage", value: seedHtml },
    }),
  });
  return created.id;
}

/**
 * POST { forge_secret, parentId, containerTitle, year, entryKey, entryHtml }
 * → { url }. Parent → container ("Changelog") → year page; entry upserted at top.
 */
exports.publishReleaseHandler = async (request) => {
  try {
    const body = JSON.parse(request.body || "{}");
    if (!(await authorized(body))) return json(403, { error: "forbidden" });
    const { parentId, containerTitle, year, entryKey, entryHtml } = body;
    if (!/^\d+$/.test(String(parentId || "")) || !year || !entryKey || !entryHtml) {
      return json(400, { error: "parentId, year, entryKey and entryHtml are required" });
    }
    const parent = await confluence(route`/wiki/api/v2/pages/${parentId}`);
    const spaceId = parent.spaceId;
    const container = containerTitle || "Changelog";
    const containerId = await findOrCreateChild(
      spaceId,
      parentId,
      container,
      `<h1>${container}</h1><p>Release notes published by CoderHelm, one page per year.</p>`
    );
    const yearTitle = String(year);
    const yearId = await findOrCreateChild(spaceId, containerId, yearTitle, `<h1>${yearTitle}</h1>`);

    // Read → upsert → write, retrying once on a version conflict.
    for (let attempt = 0; attempt < 2; attempt++) {
      const page = await confluence(route`/wiki/api/v2/pages/${yearId}?body-format=storage`);
      const current = (page.body && page.body.storage && page.body.storage.value) || "";
      const next = upsertEntry(current, entryKey, entryHtml);
      try {
        await confluence(route`/wiki/api/v2/pages/${yearId}`, {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            id: yearId,
            status: "current",
            title: page.title,
            body: { representation: "storage", value: next },
            version: { number: page.version.number + 1, message: `Release notes: ${entryKey}` },
          }),
        });
        const base = (page._links && page._links.base) || "";
        const webui = (page._links && page._links.webui) || "";
        return json(200, { url: base && webui ? base + webui : "", pageId: yearId });
      } catch (e) {
        if (attempt === 1 || !String(e.message).includes("409")) throw e;
      }
    }
    return json(500, { error: "unreachable" });
  } catch (e) {
    console.error("publishRelease error:", e);
    return json(502, { error: String(e.message || e) });
  }
};

/** POST { forge_secret, keys: ["CPM-1", ...] } → { issues: [{key, summary, type, status}] } */
exports.getIssuesHandler = async (request) => {
  try {
    const body = JSON.parse(request.body || "{}");
    if (!(await authorized(body))) return json(403, { error: "forbidden" });
    const keys = (Array.isArray(body.keys) ? body.keys : [])
      .filter((k) => /^[A-Z][A-Z0-9]+-\d+$/.test(k))
      .slice(0, 50);
    const issues = [];
    for (const key of keys) {
      try {
        const res = await api
          .asApp()
          .requestJira(route`/rest/api/3/issue/${key}?fields=summary,issuetype,status`);
        if (!res.ok) continue; // unknown key / no access → dropped
        const i = await res.json();
        issues.push({
          key: i.key,
          summary: (i.fields && i.fields.summary) || "",
          type: (i.fields && i.fields.issuetype && i.fields.issuetype.name) || "",
          status: (i.fields && i.fields.status && i.fields.status.name) || "",
        });
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

exports.upsertEntry = upsertEntry;
exports.anchorFor = anchorFor;
