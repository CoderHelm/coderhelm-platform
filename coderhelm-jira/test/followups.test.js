const { test } = require("node:test");
const assert = require("node:assert/strict");
const f = require("../src/followups");

test("a follow-up search can't close its string and add clauses", () => {
  assert.equal(
    f.dedupeJql("ABC", "ABC-12"),
    'project = "ABC" AND labels = "coderhelm:followup:ABC-12" ORDER BY created DESC'
  );
  assert.equal(f.dedupeJql('ABC" OR project = X', "ABC-12"), null);
  assert.equal(f.dedupeJql("ABC", 'ABC-12" OR 1=1'), null);
  assert.equal(f.dedupeJql("abc", "ABC-12"), null);
});

test("the same draft is the same summary, case, spacing and the last dot aside", () => {
  assert.ok(f.sameSummary("Add alt validation to coach images.", "add  alt validation to Coach images"));
  assert.ok(!f.sameSummary("Add alt validation", "Add alt validation to coach images"));
  assert.ok(!f.sameSummary("", ""));
});

test("a missing permission is named, and one Jira didn't answer counts as missing", () => {
  const answer = {
    permissions: {
      BROWSE_PROJECTS: { havePermission: true },
      CREATE_ISSUES: { havePermission: false },
    },
  };
  assert.deepEqual(f.missingPermissions(answer), ["CREATE_ISSUES", "LINK_ISSUES"]);
  assert.equal(f.permissionWords(["CREATE_ISSUES", "LINK_ISSUES"]), "Create Issues and Link Issues");
  assert.deepEqual(
    f.missingPermissions({
      permissions: Object.fromEntries(f.NEEDED_PERMISSIONS.map((p) => [p, { havePermission: true }])),
    }),
    []
  );
});

test("the issue type: the one asked for, else Task, else the first standard type", () => {
  const types = [
    { id: "1", name: "Epic", hierarchyLevel: 1 },
    { id: "2", name: "Story", hierarchyLevel: 0 },
    { id: "3", name: "Task", hierarchyLevel: 0 },
    { id: "4", name: "Sub-task", subtask: true, hierarchyLevel: -1 },
  ];
  assert.deepEqual(f.pickIssueType(types, "story"), { id: "2", name: "Story", from: "requested" });
  assert.deepEqual(f.pickIssueType(types, "Sub-task"), { id: "3", name: "Task", from: "task" });
  assert.deepEqual(f.pickIssueType(types), { id: "3", name: "Task", from: "task" });
  assert.deepEqual(f.pickIssueType(types.filter((t) => t.name !== "Task")), {
    id: "2",
    name: "Story",
    from: "first_standard",
  });
  assert.equal(f.pickIssueType([{ id: "4", name: "Sub-task", subtask: true }]), null);
});

test("the link type is the site's relates-to type, or none", () => {
  const types = [
    { name: "Blocks", inward: "is blocked by", outward: "blocks" },
    { name: "Relates", inward: "relates to", outward: "relates to" },
  ];
  assert.equal(f.pickLinkType(types), "Relates");
  assert.equal(f.pickLinkType(types, "blocks"), "Blocks");
  assert.equal(f.pickLinkType([{ name: "Related", outward: "relates to" }]), "Related");
  assert.equal(f.pickLinkType([{ name: "Blocks", outward: "blocks" }]), null);
});

test("a ticket linked either way to the source is linked", () => {
  const issue = { fields: { issuelinks: [{ outwardIssue: { key: "ABC-12" } }] } };
  assert.ok(f.linkedTo(issue, "ABC-12"));
  assert.ok(!f.linkedTo(issue, "ABC-13"));
  assert.ok(!f.linkedTo({}, "ABC-12"));
});

test("a description keeps its paragraphs and line breaks", () => {
  const doc = f.descriptionAdf("First line\nsecond line\n\nNext paragraph");
  assert.equal(doc.content.length, 2);
  assert.deepEqual(doc.content[0].content.map((n) => n.type), ["text", "hardBreak", "text"]);
  assert.equal(doc.content[1].content[0].text, "Next paragraph");
});

test("the list path answers the follow-ups already filed, with their text, and drops non-keys", () => {
  const issues = [
    {
      key: "ABC-96",
      fields: {
        summary: "Fix alt on the unused components",
        status: { name: "To Do" },
        description: {
          type: "doc",
          version: 1,
          content: [
            { type: "paragraph", content: [{ type: "text", text: "In Hero.tsx" }, { type: "hardBreak" }, { type: "text", text: "and ContactCard." }] },
          ],
        },
      },
    },
    { key: "not a key", fields: { summary: "x" } },
  ];
  assert.deepEqual(f.listedIssues(issues), [
    { key: "ABC-96", summary: "Fix alt on the unused components", description: "In Hero.tsx\nand ContactCard.", status: "To Do" },
  ]);
  assert.deepEqual(f.listedIssues(undefined), []);
  assert.equal(f.adfText("plain", 3), "pla");
});

test("a list request files nothing: the handler takes it before the create path", () => {
  const src = require("node:fs").readFileSync(require.resolve("../src/index.js"), "utf8");
  const handler = src.slice(src.indexOf("exports.createTicketHandler"));
  assert.ok(handler.includes("listing ? await listFollowups(body) : await fileFollowup(body)"));
  const list = src.slice(src.indexOf("async function listFollowups"), src.indexOf("async function fileFollowup"));
  assert.ok(!list.includes('route`/rest/api/3/issue`'), "the list path never creates");
  assert.ok(list.includes("/rest/api/3/search/jql"));
});
