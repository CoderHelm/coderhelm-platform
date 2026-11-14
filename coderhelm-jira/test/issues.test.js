const { test } = require("node:test");
const assert = require("node:assert/strict");
const { adfText, jqlFor, issueOf } = require("../src/issues");

const doc = (text) => ({ type: "doc", version: 1, content: [{ type: "paragraph", content: [{ type: "text", text }] }] });

test("a text search can't close its string and add clauses", () => {
  assert.equal(jqlFor('Embed the portal" OR project = X'), 'text ~ "Embed the portal OR project = X" ORDER BY updated DESC');
  assert.equal(jqlFor("a\\b"), 'text ~ "a b" ORDER BY updated DESC');
  assert.equal(jqlFor("   "), "");
  assert.ok(jqlFor("x".repeat(500)).length < 260);
});

test("an issue carries its status, people, dates, newest comments and https links", () => {
  const raw = {
    key: "ABC-7",
    fields: {
      summary: "Embed the portal",
      issuetype: { name: "Story" },
      status: { name: "In Review" },
      assignee: { displayName: "Dana" },
      priority: { name: "High" },
      updated: "2026-11-12T10:00:00.000+0000",
      created: "2026-11-01T10:00:00.000+0000",
      comment: { comments: [1, 2, 3, 4].map((n) => ({ author: { displayName: `P${n}` }, created: `c${n}`, body: doc(`note ${n}`) })) },
    },
  };
  const links = [
    { object: { title: "PR #5", url: "https://github.com/acme/web/pull/5" } },
    { object: { title: "bad", url: "javascript:alert(1)" } },
  ];
  const i = issueOf(raw, links);
  assert.equal(i.status, "In Review");
  assert.equal(i.assignee, "Dana");
  assert.deepEqual(i.comments.map((c) => c.text), ["note 2", "note 3", "note 4"]);
  assert.deepEqual(i.links, [{ title: "PR #5", url: "https://github.com/acme/web/pull/5" }]);
  assert.equal(adfText(doc("hi")).trim(), "hi");
});
