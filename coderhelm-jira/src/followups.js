/**
 * Follow-up tickets a CoderHelm run files from a ticket it worked on.
 *
 * A run that audits something often ends with tickets to file for the fixes.
 * CoderHelm files them through the create-ticket trigger with a `followup`
 * object, and the app then:
 *
 * 1. checks the app's own project permissions first, so a missing one is
 *    named exactly (`missingPermissions`), never "connect Jira";
 * 2. finds a ticket it already filed for the same draft (same project, same
 *    follow-up label, same summary), so a retried run never files twice
 *    (`dedupeJql`, `sameSummary`);
 * 3. picks the issue type (`pickIssueType`) and creates the ticket;
 * 4. links it to the source ticket with the site's "relates to" link type
 *    (`pickLinkType`).
 *
 * These are the pure parts, tested without Forge; index.js makes the calls.
 */

/** The Jira project permissions filing and linking need. */
const NEEDED_PERMISSIONS = ["BROWSE_PROJECTS", "CREATE_ISSUES", "LINK_ISSUES"];

/** Words for each permission, as Jira's project settings name them. */
const PERMISSION_NAMES = {
  BROWSE_PROJECTS: "Browse Projects",
  CREATE_ISSUES: "Create Issues",
  LINK_ISSUES: "Link Issues",
};

/** The most existing follow-ups one dedupe search reads. */
const MAX_DEDUPE = 50;

/** A Jira issue key such as ABC-12 (uppercase project, a number). */
function isIssueKey(key) {
  return typeof key === "string" && /^[A-Z][A-Z0-9_]{0,19}-[1-9][0-9]{0,9}$/.test(key);
}

/**
 * The label every follow-up of one source ticket carries, beside CoderHelm's
 * usual `coderhelm` and `coderhelm:<repo>` labels. Jira labels hold no
 * spaces, so the key is used as it is.
 */
function followupLabel(sourceKey) {
  return `coderhelm:followup:${sourceKey}`;
}

/** A summary as compared for "the same draft": case, spacing and trailing dots aside. */
function normalSummary(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .replace(/[.\s]+$/, "")
    .trim();
}

function sameSummary(a, b) {
  const x = normalSummary(a);
  return x.length > 0 && x === normalSummary(b);
}

/**
 * The JQL that finds the follow-ups already filed from `sourceKey` in
 * `projectKey`. Both are checked to be keys first, so neither can end the
 * string and add clauses. `null` when either isn't a key.
 */
function dedupeJql(projectKey, sourceKey) {
  if (!/^[A-Z][A-Z0-9_]{0,19}$/.test(String(projectKey || ""))) return null;
  if (!isIssueKey(sourceKey)) return null;
  return `project = "${projectKey}" AND labels = "${followupLabel(sourceKey)}" ORDER BY created DESC`;
}

/**
 * The permissions the app lacks, from Jira's `mypermissions` answer
 * (`{ permissions: { CREATE_ISSUES: { havePermission: true }, ... } }`).
 * A permission Jira didn't answer about counts as missing.
 */
function missingPermissions(answer, needed = NEEDED_PERMISSIONS) {
  const perms = (answer && answer.permissions) || {};
  return needed.filter((p) => !(perms[p] && perms[p].havePermission === true));
}

/** "Create Issues and Link Issues", for a message. */
function permissionWords(codes) {
  const names = codes.map((c) => PERMISSION_NAMES[c] || c);
  if (names.length <= 1) return names.join("");
  return `${names.slice(0, -1).join(", ")} and ${names[names.length - 1]}`;
}

/**
 * The issue type to file with, from the project's creatable types (the
 * `createmeta/{project}/issuetypes` answer's `issueTypes` or `values`).
 *
 * Jira keeps a project's default issue type in its issue type scheme, which
 * only an app with Jira admin access can read; this app has none. So the
 * type is: the one CoderHelm asked for, when the project has it; else Task,
 * the type CoderHelm has always filed; else the project's first standard
 * type (not a sub-task, not an epic level), in the order Jira lists them.
 *
 * Returns `{ id, name, from }` or `null` when the project has no type a
 * ticket can be filed with.
 */
function pickIssueType(types, requested) {
  const all = (Array.isArray(types) ? types : []).filter((t) => t && t.id && t.name);
  const standard = all.filter(
    (t) => t.subtask !== true && (t.hierarchyLevel === undefined || t.hierarchyLevel === 0)
  );
  const named = (name) =>
    standard.find((t) => String(t.name).toLowerCase() === String(name || "").toLowerCase());
  const asked = requested ? named(requested) : null;
  if (asked) return { id: asked.id, name: asked.name, from: "requested" };
  const task = named("Task");
  if (task) return { id: task.id, name: task.name, from: "task" };
  if (standard.length > 0) return { id: standard[0].id, name: standard[0].name, from: "first_standard" };
  return null;
}

/**
 * The link type for "relates to" on this site, from `/issueLinkType`'s
 * `issueLinkTypes`: the one CoderHelm asked for by name, else the one named
 * Relates (or whose outward words are "relates to"), else none (`null`):
 * a site without it gets no link rather than a wrong one.
 */
function pickLinkType(types, requested) {
  const all = (Array.isArray(types) ? types : []).filter((t) => t && t.name);
  const low = (s) => String(s || "").toLowerCase().trim();
  if (requested) {
    const asked = all.find((t) => low(t.name) === low(requested));
    if (asked) return asked.name;
  }
  const relates =
    all.find((t) => low(t.name) === "relates") || all.find((t) => low(t.outward) === "relates to");
  return relates ? relates.name : null;
}

/** Whether `issue` (search result with `fields.issuelinks`) is linked to `sourceKey`. */
function linkedTo(issue, sourceKey) {
  const links = (issue && issue.fields && issue.fields.issuelinks) || [];
  return links.some(
    (l) =>
      (l.inwardIssue && l.inwardIssue.key === sourceKey) ||
      (l.outwardIssue && l.outwardIssue.key === sourceKey)
  );
}

/**
 * A plain-text description as an Atlassian document: one paragraph per
 * paragraph of the text (blank lines), line breaks kept inside each.
 */
function descriptionAdf(text) {
  const paras = String(text || "")
    .split(/\n\s*\n/)
    .map((p) => p.trim())
    .filter(Boolean);
  return {
    type: "doc",
    version: 1,
    content: paras.map((p) => {
      const lines = p.split("\n");
      const content = [];
      lines.forEach((line, i) => {
        if (i > 0) content.push({ type: "hardBreak" });
        if (line) content.push({ type: "text", text: line });
      });
      return { type: "paragraph", content };
    }),
  };
}

module.exports = {
  NEEDED_PERMISSIONS,
  PERMISSION_NAMES,
  MAX_DEDUPE,
  isIssueKey,
  followupLabel,
  normalSummary,
  sameSummary,
  dedupeJql,
  missingPermissions,
  permissionWords,
  pickIssueType,
  pickLinkType,
  linkedTo,
  descriptionAdf,
};
