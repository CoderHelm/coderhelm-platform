const { test } = require("node:test");
const assert = require("node:assert/strict");
const { adfText, adfMentions, jqlFor, mineJql, issueOf } = require("../src/issues");

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

test("mentions and account ids come with each comment, for questions a person hasn't answered", () => {
  const mention = (id, text) => ({ type: "mention", attrs: { id, text } });
  const body = {
    type: "doc",
    content: [{ type: "paragraph", content: [mention("acc-dana-0001", "@Dana"), { type: "text", text: " can you confirm?" }, mention("acc-dana-0001", "@Dana")] }],
  };
  const i = issueOf({
    key: "ABC-8",
    fields: {
      summary: "Pick the retry limit",
      status: { name: "To Do", statusCategory: { key: "new" } },
      assignee: { accountId: "acc-dana-0001", displayName: "Dana" },
      reporter: { accountId: "acc-sam-0002", displayName: "Sam" },
      duedate: "2026-11-20",
      comment: { comments: [{ author: { accountId: "acc-sam-0002", displayName: "Sam" }, created: "c1", body }] },
    },
  });
  assert.equal(i.status_category, "new");
  assert.equal(i.assignee_id, "acc-dana-0001");
  assert.equal(i.reporter_id, "acc-sam-0002");
  assert.equal(i.due, "2026-11-20");
  assert.equal(i.comments[0].author_id, "acc-sam-0002");
  assert.deepEqual(i.comments[0].mentions, ["acc-dana-0001"]);
  assert.deepEqual(adfMentions(null), []);
});

test("the waiting-on-me JQL takes only an account id, never more clauses", () => {
  const q = mineJql("557058:0a1b2c3d-1111-2222-3333-444455556666");
  assert.match(q.assigned, /^assignee = "557058:0a1b2c3d-1111-2222-3333-444455556666" AND statusCategory != Done/);
  assert.match(q.involved, /watcher = .* OR reporter = /);
  assert.equal(mineJql('x" OR project = SECRET'), null);
  assert.equal(mineJql(""), null);
});
