const { test } = require("node:test");
const assert = require("node:assert/strict");
const { keysInText, relatedKeys, relatedTickets, MAX_RELATED } = require("../src/related");

test("keys named in a text, without the ticket's own", () => {
  assert.deepEqual(keysInText("See ABC-2 and XY-3, not ABC-1 or abc-4. ABC-2 again.", "ABC-1"), ["ABC-2", "XY-3"]);
});

test("issue links come first, with how they relate, then keys in the description", () => {
  const fields = {
    issuelinks: [
      { type: { name: "Relates", inward: "relates to", outward: "relates to" }, outwardIssue: { key: "ABC-2" } },
      { type: { name: "Blocks", inward: "is blocked by", outward: "blocks" }, inwardIssue: { key: "ABC-3" } },
    ],
    description: { type: "doc", content: [{ type: "text", text: "Also covers ABC-4 and ABC-2." }] },
  };
  assert.deepEqual(relatedKeys("ABC-1", fields), [
    { key: "ABC-2", relation: "relates to" },
    { key: "ABC-3", relation: "is blocked by" },
    { key: "ABC-4", relation: "named in the ticket" },
  ]);
});

test("each related ticket is read with its summary and description; an unreadable one is left out", async () => {
  const fields = { issuelinks: [{ type: { outward: "relates to" }, outwardIssue: { key: "ABC-2" } }, { type: { outward: "relates to" }, outwardIssue: { key: "ABC-9" } }] };
  const read = async (key) => (key === "ABC-2" ? { summary: "Point the drawer button to the new page", status: { name: "Closed" }, description: "Acceptance criteria\n- The drawer Register button links to /new/register" } : null);
  const got = await relatedTickets("ABC-1", fields, read);
  assert.equal(got.length, 1);
  assert.equal(got[0].key, "ABC-2");
  assert.equal(got[0].status, "Closed");
  assert.match(got[0].description, /links to \/new\/register/);
});

test("at most a handful are sent", () => {
  const links = Array.from({ length: 10 }, (_, i) => ({ type: { outward: "relates to" }, outwardIssue: { key: `ABC-${i + 2}` } }));
  assert.equal(relatedKeys("ABC-1", { issuelinks: links }).length, MAX_RELATED);
});
