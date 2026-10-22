// Pure rules for the Confluence web trigger (no Forge imports, so they can be
// tested with plain Node): which requests are accepted, and how an append
// becomes a page update.

/** The most markup one append may add (CoderHelm caps the text at 10k chars). */
const MAX_APPEND_MARKUP = 60000;
/** The longest CQL accepted (CoderHelm's own searches are far shorter). */
const MAX_CQL = 1000;

/** A Confluence id: digits only, at most 20 of them. */
function isId(v) {
  return /^\d{1,20}$/.test(String(v === undefined || v === null ? "" : v));
}

/**
 * Checks one request body and says which Confluence call it maps to, or why
 * it is refused. Returns { error } or { op, ... } with only checked values.
 */
function parseRequest(body) {
  const op = String(body.op || "");
  switch (op) {
    case "whoami":
      return { op };
    case "page":
    case "space":
    case "children":
      if (!isId(body.id)) return { error: "id must be a Confluence id (digits)" };
      return { op, id: String(body.id) };
    case "search": {
      const cql = typeof body.cql === "string" ? body.cql.trim() : "";
      // Only page searches: CoderHelm always starts its CQL this way.
      if (!cql || cql.length > MAX_CQL || !cql.startsWith("type = page")) {
        return { error: "cql must be a page search (type = page ...)" };
      }
      return { op, cql };
    }
    case "append": {
      if (!isId(body.id)) return { error: "id must be a Confluence id (digits)" };
      const html = typeof body.html === "string" ? body.html : "";
      if (!html.trim() || html.length > MAX_APPEND_MARKUP) {
        return { error: `html must be 1 to ${MAX_APPEND_MARKUP} characters` };
      }
      const version = Number(body.version);
      if (!Number.isInteger(version) || version < 1) {
        return { error: "version must be the page version that was read" };
      }
      return { op, id: String(body.id), html, version };
    }
    default:
      return { error: "unknown op" };
  }
}

/**
 * The page update for an append: the current body with `html` added at the
 * end, as the next version. Refuses (returns null) when the page moved past
 * the version CoderHelm read, so someone else's newer edit is never lost.
 */
function appendUpdate(page, html, readVersion) {
  const current = page && page.version && Number(page.version.number);
  if (!current || current !== readVersion) return null;
  const old = (page.body && page.body.storage && page.body.storage.value) || "";
  return {
    id: String(page.id),
    status: "current",
    title: page.title,
    body: { representation: "storage", value: old + html },
    version: { number: current + 1, message: "Added by Coderhelm" },
  };
}

module.exports = { isId, parseRequest, appendUpdate, MAX_APPEND_MARKUP, MAX_CQL };
