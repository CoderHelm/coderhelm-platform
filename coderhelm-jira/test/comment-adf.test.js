const { test } = require("node:test");
const assert = require("node:assert/strict");
const { buildCommentAdf } = require("../src/comment-adf");

test("a reply opens with a mention of the person it answers", () => {
  const adf = buildCommentAdf(
    "Sam Lee, replying to your comment (\"Use the staging bucket.\").\n\nSwitched to the staging bucket.",
    "error",
    "Run failed",
    { accountId: "acc-1", author: "Sam Lee", commentId: "10001" },
  );
  const first = adf.content[1];
  assert.equal(first.content[0].type, "mention");
  assert.equal(first.content[0].attrs.id, "acc-1");
  assert.ok(first.content[1].text.startsWith(", replying to your comment"));
});

test("without an account id the name stays plain text", () => {
  const adf = buildCommentAdf("Sam Lee, replying to your comment.", undefined, undefined, { author: "Sam Lee" });
  assert.equal(adf.content[0].content[0].type, "text");
});

test("questions and status are plain text; only a failure gets one short red line", () => {
  for (const type of ["clarification", "no_changes", "success", "info", "covered", "status", "answer"]) {
    const adf = buildCommentAdf("Which page title should I use?", type, "Questions only a person can answer");
    assert.ok(adf.content.every((n) => n.type === "paragraph"), type);
    assert.ok(!JSON.stringify(adf).includes("Questions only a person can answer"), type);
  }
  const failed = buildCommentAdf("The sandbox could not start.\n\nRun it again.", "error", "Run failed");
  assert.equal(failed.content[0].type, "panel");
  assert.equal(failed.content[0].content.length, 1);
  assert.equal(failed.content[0].content[0].content[0].text, "Run failed");
  assert.equal(failed.content[1].type, "paragraph");
});
