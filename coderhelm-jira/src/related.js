/**
 * The tickets related to the one CoderHelm works on: its issue links ("relates
 * to", "blocks", "is cloned by") and the tickets its description names. Their
 * acceptance criteria are part of the change, so the run reads them before it
 * plans and checks them before the pull request is called ready.
 *
 * A pull request was once closed by a person who expected a linked ticket's
 * pull request to cover it; that pull request had never read the linked
 * ticket, and the button it was about still pointed to the old address.
 */

/** The most related tickets sent with one ticket event. */
const MAX_RELATED = 6;

/** The most characters of one related ticket's description sent. */
const MAX_BODY_CHARS = 8000;

/** Ticket keys a text names (`ABC-123`), in order, without repeats. */
function keysInText(text, own) {
  const out = [];
  const re = /(^|[^A-Za-z0-9-])([A-Z][A-Z0-9]+-\d+)(?![A-Za-z0-9])/g;
  let m;
  while ((m = re.exec(text || "")) !== null) {
    const key = m[2];
    if (key !== own && !out.includes(key)) out.push(key);
  }
  return out;
}

/**
 * The related tickets' keys with how each relates, issue links first.
 * @param {string} own The ticket's own key.
 * @param {object} fields The ticket's fields (with `issuelinks` and `description`).
 * @returns {{key: string, relation: string}[]}
 */
function relatedKeys(own, fields) {
  const out = [];
  for (const l of (fields && fields.issuelinks) || []) {
    const other = l.outwardIssue || l.inwardIssue;
    if (!other || !other.key || other.key === own) continue;
    const type = l.type || {};
    const relation = (l.outwardIssue ? type.outward : type.inward) || type.name || "relates to";
    if (!out.some((r) => r.key === other.key)) out.push({ key: other.key, relation });
  }
  const text = typeof fields?.description === "string" ? fields.description : JSON.stringify(fields?.description || "");
  for (const key of keysInText(text, own)) {
    if (!out.some((r) => r.key === key)) out.push({ key, relation: "named in the ticket" });
  }
  return out.slice(0, MAX_RELATED);
}

/**
 * Each related ticket with its summary, status and description (the
 * gateway reads the description's words), read as the app.
 * @param {string} own The ticket's own key.
 * @param {object} fields The ticket's fields.
 * @param {(key: string) => Promise<object|null>} readIssue The issue's fields, or null.
 */
async function relatedTickets(own, fields, readIssue) {
  const out = [];
  for (const { key, relation } of relatedKeys(own, fields)) {
    let f = null;
    try {
      f = await readIssue(key);
    } catch (e) {
      f = null;
    }
    if (!f) continue;
    const body = typeof f.description === "string" ? f.description : JSON.stringify(f.description || "");
    out.push({
      key,
      relation,
      summary: f.summary || "",
      status: (f.status && f.status.name) || "",
      description: body.slice(0, MAX_BODY_CHARS),
    });
  }
  return out;
}

module.exports = { keysInText, relatedKeys, relatedTickets, MAX_RELATED };
