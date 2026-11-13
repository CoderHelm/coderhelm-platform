const { test } = require("node:test");
const assert = require("node:assert/strict");
const { existingComments, MAX_COMMENTS } = require("../src/comments");

const doc = (text) => ({ type: "doc", version: 1, content: [{ type: "paragraph", content: [{ type: "text", text }] }] });

test("sends a ticket's comments with their id, author and body", () => {
  const fields = {
    comment: {
      comments: [
        { id: "10001", author: { displayName: "Pat Doe", accountType: "atlassian" }, body: doc("Production only.") },
        { id: 10002, author: { displayName: "Coderhelm", accountType: "app" }, body: "Questions only a person can answer" },
      ],
    },
  };
  const got = existingComments(fields);
  assert.equal(got.length, 2);
  assert.deepEqual(got[0].author, { displayName: "Pat Doe", accountType: "atlassian" });
  assert.equal(got[0].id, "10001");
  assert.equal(JSON.parse(got[0].body).content[0].content[0].text, "Production only.");
  assert.equal(got[1].id, "10002");
  assert.equal(got[1].author.accountType, "app");
  assert.equal(got[1].body, "Questions only a person can answer");
});

test("a ticket without comments, or without the field, sends none", () => {
  assert.deepEqual(existingComments({}), []);
  assert.deepEqual(existingComments(undefined), []);
  assert.deepEqual(existingComments({ comment: {} }), []);
  assert.deepEqual(existingComments({ comment: { comments: "x" } }), []);
  assert.deepEqual(existingComments({ comment: { comments: [null, { id: "1" }, { body: "no id" }] } }), []);
});

test("only the newest comments are sent", () => {
  const comments = Array.from({ length: 50 }, (_, i) => ({ id: String(i), author: {}, body: "x" }));
  const got = existingComments({ comment: { comments } });
  assert.equal(got.length, MAX_COMMENTS);
  assert.equal(got[got.length - 1].id, "49");
  assert.deepEqual(got[0].author, { displayName: "", accountType: "" });
});
