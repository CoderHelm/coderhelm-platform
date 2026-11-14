/**
 * Issues as CoderHelm reads them through the app's get-issues trigger: by key
 * (release notes, Chat) or by a text search (Chat's ticket lookup). Pure
 * helpers, so they are tested without Forge.
 */

/** The most issues one text search returns. */
const MAX_SEARCH = 8;
/** The newest comments sent back per issue, and the characters kept of each. */
const MAX_ISSUE_COMMENTS = 3;
const MAX_COMMENT_CHARS = 600;

/** The words of an Atlassian document (a comment body), in reading order. */
function adfText(node) {
  if (!node) return "";
  if (typeof node === "string") return node;
  if (node.type === "text") return node.text || "";
  const parts = (Array.isArray(node.content) ? node.content : []).map(adfText);
  const block = ["paragraph", "heading", "listItem", "codeBlock", "blockquote"].includes(node.type);
  return parts.join("") + (block ? "\n" : "");
}

/**
 * The JQL for a text search: the words as one phrase-free text match, newest
 * change first. Quotes and backslashes are taken out so the words can't end
 * the string and add their own clauses.
 */
function jqlFor(query) {
  const words = String(query || "")
    .replace(/["\\]/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 200);
  if (!words) return "";
  return `text ~ "${words}" ORDER BY updated DESC`;
}

/** One issue as CoderHelm reads it, from the issue REST API's shape. */
function issueOf(raw, links) {
  const f = (raw && raw.fields) || {};
  const comments = ((f.comment && f.comment.comments) || [])
    .slice(-MAX_ISSUE_COMMENTS)
    .map((c) => ({
      author: (c.author && c.author.displayName) || "",
      created: c.created || "",
      text: adfText(c.body).trim().slice(0, MAX_COMMENT_CHARS),
    }));
  return {
    key: raw.key,
    summary: f.summary || "",
    type: (f.issuetype && f.issuetype.name) || "",
    status: (f.status && f.status.name) || "",
    assignee: (f.assignee && f.assignee.displayName) || "",
    priority: (f.priority && f.priority.name) || "",
    updated: f.updated || "",
    created: f.created || "",
    comments,
    // Links on the issue to pull requests and branches (remote links).
    links: (links || [])
      .map((l) => ({ title: (l.object && l.object.title) || "", url: (l.object && l.object.url) || "" }))
      .filter((l) => /^https:\/\//.test(l.url))
      .slice(0, 10),
  };
}

module.exports = { adfText, jqlFor, issueOf, MAX_SEARCH, MAX_ISSUE_COMMENTS, MAX_COMMENT_CHARS };
