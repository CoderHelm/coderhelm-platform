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
 * @param {object} fields The ticket's fields, as the issue REST API returns them.
 * @returns {{id: string, author: {displayName: string, accountType: string}, body: string}[]}
 */
function existingComments(fields) {
  const all = (fields && fields.comment && fields.comment.comments) || [];
  if (!Array.isArray(all)) return [];
  return all
    .filter((c) => c && c.id && c.body)
    .slice(-MAX_COMMENTS)
    .map((c) => ({
      id: String(c.id),
      author: {
        displayName: (c.author && c.author.displayName) || "",
        accountType: (c.author && c.author.accountType) || "",
      },
      // A comment body is a document (JSON); the gateway reads its words.
      body: typeof c.body === "string" ? c.body : JSON.stringify(c.body),
    }));
}

module.exports = { existingComments, MAX_COMMENTS };
