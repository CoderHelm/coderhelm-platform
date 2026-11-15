// A reply to a person's comment, posted under that comment in Jira's thread.
//
// Jira Cloud shows comment threads (one level deep: a comment and the replies
// under it). Its public REST API does not document a way to create a reply:
// the comment schema has no parent field, and Atlassian said the threaded
// comments release made no API changes. Jira does, though, return a
// `parentId` on replies when comments are read, and accepts one when a
// comment is created. That field is undocumented, so this module treats it as
// something a site may refuse or ignore at any time:
//
//   1. The reply is created with `parentId` set to the root of the person's
//      thread (a reply's own parent, since a reply to a reply is refused).
//   2. The created comment is read back. Only when it comes back with that
//      `parentId` did Jira thread it.
//   3. When Jira refuses the field (a 4xx that names the parent), the reply is
//      posted again as an ordinary comment. When Jira takes the comment but
//      drops the field, the comment already posted stands.
//   4. Either failure is remembered for this site (the app's storage is per
//      installation, so per site) for 24 hours, so CoderHelm doesn't try the
//      field on every reply.
//
// The reply's text always opens with a mention of the person and quotes their
// comment, so it reads as an answer to theirs and notifies them even where it
// lands as a new comment.

/** Forge storage key for this site's last outcome with threaded replies. */
const STORAGE_KEY = "coderhelm-threaded-replies";

/** How long a refused or ignored `parentId` keeps this site on plain comments. */
const OFF_FOR_MS = 24 * 60 * 60 * 1000;

/**
 * The parent comment id a Jira comment carries, as a string ("" when it is not
 * a reply). Jira sends it as a number or a string; null, 0 and "" mean none.
 */
function parentIdOf(comment) {
  if (!comment || comment.parentId === undefined || comment.parentId === null) return "";
  const id = String(comment.parentId).trim();
  return id === "" || id === "0" || id === "-1" ? "" : id;
}

/**
 * Whether this site is currently on plain comments: a refused or ignored
 * `parentId` within the last 24 hours. A storage error reads as "not off".
 */
async function threadingOff(storage, now) {
  try {
    const kept = await storage.get(STORAGE_KEY);
    return Boolean(kept && kept.state === "off" && Number(kept.until) > now);
  } catch (e) {
    console.log(`Could not read the threaded replies setting: ${e.message}`);
    return false;
  }
}

/** Remembers for 24 hours that this site does not thread replies. */
async function rememberOff(storage, now, why) {
  console.log(`Threaded replies off on this site for 24 hours: ${why}`);
  try {
    await storage.set(STORAGE_KEY, { state: "off", until: now + OFF_FOR_MS, why });
  } catch (e) {
    console.log(`Could not keep the threaded replies setting: ${e.message}`);
  }
}

/** Whether Jira's refusal is about the parent field (not, say, a bad body). */
function refusedForParent(status, text) {
  return status >= 400 && status < 500 && /parent/i.test(text || "");
}

/**
 * The comment a reply goes under: the root of the person's thread. `hint` is
 * the parent CoderHelm already knows for the comment (from what it read); when
 * there is none, the comment is read to find out. A comment that can't be read
 * is taken as a root.
 */
async function threadRoot(jira, issueKey, commentId, hint) {
  if (hint) return String(hint);
  try {
    const res = await jira.getComment(issueKey, commentId);
    if (res.ok) {
      const parent = parentIdOf(await res.json());
      if (parent) return parent;
    }
  } catch (e) {
    console.log(`Could not read comment ${commentId} for its thread: ${e.message}`);
  }
  return String(commentId);
}

/** Jira's comment ids are numbers in a string; the field is sent as a number. */
function asIdValue(id) {
  return /^\d+$/.test(id) ? Number(id) : id;
}

/**
 * Posts `adfBody` as a reply to `replyTo.commentId` on `issueKey`.
 *
 * `jira` makes the three REST calls (so tests can stand in for Jira):
 *   createComment(issueKey, payload) and getComment(issueKey, id), each
 *   resolving to a fetch-like response ({ ok, status, json(), text() }).
 * `storage` is Forge's key-value storage (get/set).
 *
 * Resolves to { ok, status, id, threaded, error }: `threaded` is true only
 * when Jira read back the comment under the person's thread.
 */
async function postReply({ jira, storage, issueKey, adfBody, replyTo, now = Date.now() }) {
  const plain = async () => {
    const res = await jira.createComment(issueKey, { body: adfBody });
    if (!res.ok) {
      return { ok: false, status: res.status, error: await res.text(), threaded: false };
    }
    const created = await res.json();
    return { ok: true, status: res.status, id: created.id, threaded: false };
  };

  const target = replyTo && replyTo.commentId ? String(replyTo.commentId) : "";
  if (!target || (await threadingOff(storage, now))) {
    return plain();
  }

  const root = await threadRoot(jira, issueKey, target, parentIdOf(replyTo));
  const res = await jira.createComment(issueKey, { body: adfBody, parentId: asIdValue(root) });
  if (!res.ok) {
    const text = await res.text();
    if (refusedForParent(res.status, text)) {
      await rememberOff(storage, now, `Jira refused parentId (${res.status}): ${text.slice(0, 200)}`);
      return plain();
    }
    return { ok: false, status: res.status, error: text, threaded: false };
  }

  const created = await res.json();
  // Read back: only a comment Jira returns with this parent is in the thread.
  // When the read fails, the create's own answer is what there is.
  let readParent = parentIdOf(created);
  try {
    const back = await jira.getComment(issueKey, created.id);
    if (back.ok) readParent = parentIdOf(await back.json());
  } catch (e) {
    console.log(`Could not read back comment ${created.id}: ${e.message}`);
  }
  if (readParent !== root) {
    // The comment is posted (as a plain comment, with its mention and quote):
    // it stands, and this site posts plain comments for a while.
    await rememberOff(storage, now, `Jira ignored parentId ${root} (read back "${readParent}")`);
    return { ok: true, status: res.status, id: created.id, threaded: false };
  }
  return { ok: true, status: res.status, id: created.id, threaded: true };
}

module.exports = {
  postReply,
  parentIdOf,
  threadRoot,
  refusedForParent,
  STORAGE_KEY,
  OFF_FOR_MS,
};
