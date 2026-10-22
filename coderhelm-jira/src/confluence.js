// Confluence reads (and one guarded write) for CoderHelm agents, through this
// app's own Confluence access on the site it is installed on.
//
// The CoderHelm backend calls this web trigger with the shared secret the
// admin saved on the settings page (the same check as the release-notes
// triggers). It mirrors exactly what CoderHelm's Confluence connection may do
// with an API token, and nothing more:
//
//   whoami    - who the app signs in as (the connection check)
//   page      - one page by id, with its storage body
//   space     - one space by id (so CoderHelm can check its key)
//   search    - a CQL search for pages (CoderHelm builds and checks the CQL)
//   children  - the pages directly under a page
//   append    - add markup to the END of one page, only when the page is
//               still at the version CoderHelm read; never replaces, creates,
//               moves or deletes a page
//
// Which pages may be appended to (the team's write switch and page list) is
// decided by CoderHelm before it calls; this trigger adds the version check
// and the "append only" rule on its side.

const api = require("@forge/api");
const { route } = api;
const { authorized, json } = require("./release");
const { parseRequest, appendUpdate } = require("./confluence-rules");

// ── Confluence calls ─────────────────────────────────────────────────────────

/** One Confluence call; returns { status, data } whatever the status. */
async function call(path, init) {
  const res = await api.asApp().requestConfluence(path, init);
  const text = await res.text();
  let data = null;
  try {
    data = text ? JSON.parse(text) : null;
  } catch (_) {
    data = { message: text.slice(0, 500) };
  }
  return { status: res.status, data };
}

/** Passes Confluence's answer back with its own status, so CoderHelm can explain it. */
const answer = ({ status, data }) => json(status, data === null ? {} : data);

/**
 * POST { forge_secret, op, ... } → Confluence's answer (same status and JSON
 * as Confluence's REST API gives for that call).
 */
exports.confluenceHandler = async (request) => {
  try {
    const body = JSON.parse(request.body || "{}");
    // 401, not 403: a 403 means "Confluence won't show the app this page".
    if (!(await authorized(body))) return json(401, { error: "forbidden" });
    const req = parseRequest(body);
    if (req.error) return json(400, { error: req.error });

    switch (req.op) {
      case "whoami":
        return answer(await call(route`/wiki/rest/api/user/current`));
      case "page":
        return answer(await call(route`/wiki/api/v2/pages/${req.id}?body-format=storage`));
      case "space":
        return answer(await call(route`/wiki/api/v2/spaces/${req.id}`));
      case "children":
        return answer(await call(route`/wiki/api/v2/pages/${req.id}/children?limit=50`));
      case "search":
        return answer(await call(route`/wiki/rest/api/search?limit=15&cql=${req.cql}`));
      case "append": {
        const read = await call(route`/wiki/api/v2/pages/${req.id}?body-format=storage`);
        if (read.status < 200 || read.status > 299) return answer(read);
        const update = appendUpdate(read.data, req.html, req.version);
        if (!update) return json(409, { error: "the page changed since it was read" });
        return answer(
          await call(route`/wiki/api/v2/pages/${req.id}`, {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(update),
          })
        );
      }
      default:
        return json(400, { error: "unknown op" });
    }
  } catch (e) {
    console.error("confluence trigger error:", e);
    return json(502, { error: String(e.message || e) });
  }
};
