/**
 * The comments already on a ticket, as CoderHelm reads them when a run starts
 * on it: context people gave before the work began.
 *
 * Jira returns a ticket's comments in its `comment` field (oldest first). The
 * gateway keeps each one once, by its id, so sending the same comment with a
 * later event changes nothing. It also leaves out the app's own comments (by
 * the author's account type and name), so they are passed through as they are.
 */

/** The most comments sent with one ticket event: the newest ones. */
const MAX_COMMENTS = 20;

/**
 * The most characters of comment bodies sent with one ticket event. The
 * event also carries the ticket's images, and the whole request has to stay
 * well inside what the gateway accepts. When the newest comments are longer
 * than this in all, the oldest of them are left out.
 */
const MAX_TOTAL_CHARS = 200000;

/**
 * @param {object} fields The ticket's fields, as the issue REST API returns them.
 * @returns {{id: string, author: {displayName: string, accountType: string}, body: string}[]}
 */
function existingComments(fields) {
  const all = (fields && fields.comment && fields.comment.comments) || [];
  if (!Array.isArray(all)) return [];
  const newest = all.filter((c) => c && c.id && c.body).slice(-MAX_COMMENTS);
  const sent = newest.map((c) => ({
      id: String(c.id),
      author: {
        displayName: (c.author && c.author.displayName) || "",
        accountType: (c.author && c.author.accountType) || "",
      },
      // A comment body is a document (JSON); the gateway reads its words.
      body: typeof c.body === "string" ? c.body : JSON.stringify(c.body),
    }));
  // Keep the newest comments that fit; a body is never cut in the middle
  // (it is a JSON document, and half of one cannot be read).
  let total = 0;
  let from = sent.length;
  while (from > 0 && total + sent[from - 1].body.length <= MAX_TOTAL_CHARS) {
    total += sent[from - 1].body.length;
    from -= 1;
  }
  return sent.slice(from);
}

module.exports = { existingComments, MAX_COMMENTS, MAX_TOTAL_CHARS };
