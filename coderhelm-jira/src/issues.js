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

/**
 * The account ids an Atlassian document mentions (`@Name` nodes), each once,
 * in reading order. A mention is a node of type "mention" whose `attrs.id`
 * is the person's account id.
 */
function adfMentions(node, out = []) {
  if (!node || typeof node !== "object") return out;
  if (node.type === "mention" && node.attrs && typeof node.attrs.id === "string" && node.attrs.id) {
    if (!out.includes(node.attrs.id)) out.push(node.attrs.id);
  }
  for (const child of Array.isArray(node.content) ? node.content : []) adfMentions(child, out);
  return out;
}

/**
 * The account id in a Jira user object, or "" when there is none. Jira user
 * objects carry `accountId`; anything else (an old payload) has no id.
 */
function accountIdOf(user) {
  return (user && typeof user.accountId === "string" && user.accountId) || "";
}

/**
 * The JQL for "what is waiting on this person": the open issues assigned to
 * them, most important and newest change first; and the open issues they
 * watch or reported that changed lately (where they may be asked something).
 * The account id is checked to be an account id, so it can't end the string.
 */
function mineJql(accountId) {
  const id = String(accountId || "");
  if (!/^[A-Za-z0-9:-]{8,128}$/.test(id)) return null;
  return {
    assigned: `assignee = "${id}" AND statusCategory != Done ORDER BY priority DESC, updated DESC`,
    involved: `(watcher = "${id}" OR reporter = "${id}") AND statusCategory != Done AND updated >= -14d ORDER BY updated DESC`,
  };
}

/** One issue as CoderHelm reads it, from the issue REST API's shape. */
function issueOf(raw, links) {
  const f = (raw && raw.fields) || {};
  const comments = ((f.comment && f.comment.comments) || [])
    .slice(-MAX_ISSUE_COMMENTS)
    .map((c) => ({
      author: (c.author && c.author.displayName) || "",
      // Who wrote it and who it mentions, as account ids, so CoderHelm can
      // tell a question to a person they haven't answered yet.
      author_id: accountIdOf(c.author),
      mentions: adfMentions(c.body),
      created: c.created || "",
      text: adfText(c.body).trim().slice(0, MAX_COMMENT_CHARS),
    }));
  return {
    key: raw.key,
    summary: f.summary || "",
    type: (f.issuetype && f.issuetype.name) || "",
    status: (f.status && f.status.name) || "",
    // "new", "indeterminate" or "done": where the status sits in the workflow.
    status_category: (f.status && f.status.statusCategory && f.status.statusCategory.key) || "",
    assignee: (f.assignee && f.assignee.displayName) || "",
    assignee_id: accountIdOf(f.assignee),
    reporter_id: accountIdOf(f.reporter),
    priority: (f.priority && f.priority.name) || "",
    due: f.duedate || "",
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

/** The most issues one "what is waiting on me" read returns, of each kind. */
const MAX_MINE = 15;

module.exports = {
  adfText,
  adfMentions,
  jqlFor,
  mineJql,
  issueOf,
  MAX_SEARCH,
  MAX_MINE,
  MAX_ISSUE_COMMENTS,
  MAX_COMMENT_CHARS,
};
