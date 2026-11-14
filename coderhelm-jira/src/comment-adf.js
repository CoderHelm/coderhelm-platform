// The text CoderHelm posts on a ticket, as Jira's document format.

/**
 * A reply to a person's comment opens with their name: it becomes a mention
 * of them, so the comment reads as an answer to theirs (Jira's public API
 * has no threaded comment replies).
 */
function mentionFirst(paragraphs, replyTo) {
  if (!replyTo || !replyTo.accountId || !replyTo.author || paragraphs.length === 0) {
    return paragraphs;
  }
  const first = paragraphs[0].content[0].text;
  const lead = `${replyTo.author},`;
  if (!first.startsWith(lead)) {
    return paragraphs;
  }
  const head = {
    type: "paragraph",
    content: [
      { type: "mention", attrs: { id: replyTo.accountId, text: `@${replyTo.author}` } },
      { type: "text", text: first.slice(replyTo.author.length) },
    ],
  };
  return [head, ...paragraphs.slice(1)];
}

/**
 * CoderHelm writes like a colleague: plain paragraphs. Questions, status and
 * progress get no coloured panel. Only a failure that blocks the ticket gets
 * one short red line (its title), with the text plain below it.
 */
function buildCommentAdf(comment, commentType, title, replyTo) {
  const paragraphs = mentionFirst(
    comment.split("\n\n").filter(Boolean).map((para) => ({
      type: "paragraph",
      content: [{ type: "text", text: para.replace(/\n/g, " ") }],
    })),
    replyTo,
  );
  const body = paragraphs.length > 0 ? paragraphs : [{ type: "paragraph", content: [{ type: "text", text: comment }] }];
  if (commentType === "error" && title) {
    return {
      type: "doc",
      version: 1,
      content: [
        {
          type: "panel",
          attrs: { panelType: "error" },
          content: [{ type: "paragraph", content: [{ type: "text", text: title }] }],
        },
        ...body,
      ],
    };
  }
  return { type: "doc", version: 1, content: body };
}

module.exports = { buildCommentAdf, mentionFirst };
