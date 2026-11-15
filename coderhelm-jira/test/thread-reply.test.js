const { test } = require("node:test");
const assert = require("node:assert/strict");
const { postReply, parentIdOf, refusedForParent, STORAGE_KEY, OFF_FOR_MS } = require("../src/thread-reply");
const { existingComments } = require("../src/comments");

const NOW = 1_800_000_000_000;
const adfBody = { type: "doc", version: 1, content: [] };

/** A fetch-like response. */
function response(status, body) {
  return {
    ok: status >= 200 && status < 300,
    status,
    json: async () => body,
    text: async () => (typeof body === "string" ? body : JSON.stringify(body)),
  };
}

/** Forge storage in memory. */
function memoryStorage(initial = {}) {
  const kept = { ...initial };
  return {
    kept,
    get: async (k) => kept[k],
    set: async (k, v) => {
      kept[k] = v;
    },
  };
}

/**
 * A stand-in for Jira's comment REST calls. `comments` are the ticket's
 * comments by id; `onCreate(payload)` answers a create and may add comments.
 */
function fakeJira(comments, onCreate) {
  const calls = [];
  return {
    calls,
    createComment: async (issueKey, payload) => {
      calls.push({ op: "create", issueKey, payload });
      return onCreate(payload, comments);
    },
    getComment: async (issueKey, id) => {
      calls.push({ op: "get", issueKey, id: String(id) });
      const c = comments[String(id)];
      return c ? response(200, c) : response(404, { errorMessages: ["Comment not found"] });
    },
  };
}

/** Jira that threads replies: the created comment keeps its parentId. */
const threading = (payload, comments) => {
  const id = String(20000 + Object.keys(comments).length);
  comments[id] = { id, ...(payload.parentId ? { parentId: payload.parentId } : {}) };
  return response(201, { id });
};

test("accepted: the reply goes under the person's comment and reads back with its parent", async () => {
  const jira = fakeJira({ 10001: { id: "10001" } }, threading);
  const storage = memoryStorage();
  const got = await postReply({
    jira,
    storage,
    issueKey: "ABC-1",
    adfBody,
    replyTo: { commentId: "10001", accountId: "acc-1", author: "Sam Lee" },
    now: NOW,
  });
  assert.equal(got.ok, true);
  assert.equal(got.threaded, true);
  const creates = jira.calls.filter((c) => c.op === "create");
  assert.equal(creates.length, 1);
  assert.equal(creates[0].payload.parentId, 10001);
  assert.deepEqual(creates[0].payload.body, adfBody);
  // Read back after the create.
  assert.ok(jira.calls.some((c) => c.op === "get" && c.id === got.id));
  assert.equal(storage.kept[STORAGE_KEY], undefined);
});

test("rejected: a 4xx about the parent posts a plain comment and turns threading off for 24 hours", async () => {
  const jira = fakeJira({ 10001: { id: "10001" } }, (payload) =>
    payload.parentId
      ? response(400, { errorMessages: ["Parent comment not found, and no child comments exist."] })
      : response(201, { id: "20001" }),
  );
  const storage = memoryStorage();
  const got = await postReply({
    jira,
    storage,
    issueKey: "ABC-1",
    adfBody,
    replyTo: { commentId: "10001" },
    now: NOW,
  });
  assert.equal(got.ok, true);
  assert.equal(got.threaded, false);
  assert.equal(got.id, "20001");
  const creates = jira.calls.filter((c) => c.op === "create");
  assert.equal(creates.length, 2);
  assert.equal(creates[1].payload.parentId, undefined);
  assert.equal(storage.kept[STORAGE_KEY].state, "off");
  assert.equal(storage.kept[STORAGE_KEY].until, NOW + OFF_FOR_MS);

  // The next reply within the 24 hours doesn't try the field.
  jira.calls.length = 0;
  await postReply({ jira, storage, issueKey: "ABC-1", adfBody, replyTo: { commentId: "10001" }, now: NOW + 1000 });
  assert.deepEqual(
    jira.calls.map((c) => [c.op, c.payload && c.payload.parentId]),
    [["create", undefined]],
  );

  // After them it tries again.
  jira.calls.length = 0;
  await postReply({ jira, storage, issueKey: "ABC-1", adfBody, replyTo: { commentId: "10001" }, now: NOW + OFF_FOR_MS + 1 });
  assert.equal(jira.calls.find((c) => c.op === "create").payload.parentId, 10001);
});

test("silently ignored: the comment stands as posted and threading turns off", async () => {
  // Jira takes the comment but drops parentId.
  const ignoring = (_payload, comments) => {
    comments["20001"] = { id: "20001" };
    return response(201, { id: "20001" });
  };
  const jira = fakeJira({ 10001: { id: "10001" } }, ignoring);
  const storage = memoryStorage();
  const got = await postReply({ jira, storage, issueKey: "ABC-1", adfBody, replyTo: { commentId: "10001" }, now: NOW });
  assert.equal(got.ok, true);
  assert.equal(got.threaded, false);
  assert.equal(got.id, "20001");
  // Posted once: no second comment.
  assert.equal(jira.calls.filter((c) => c.op === "create").length, 1);
  assert.equal(storage.kept[STORAGE_KEY].state, "off");
});

test("reply to a reply: the reply goes under the thread's root comment", async () => {
  const jira = fakeJira({ 10001: { id: "10001" }, 10005: { id: "10005", parentId: 10001 } }, threading);
  const got = await postReply({
    jira,
    storage: memoryStorage(),
    issueKey: "ABC-1",
    adfBody,
    replyTo: { commentId: "10005" },
    now: NOW,
  });
  assert.equal(got.threaded, true);
  assert.equal(jira.calls.find((c) => c.op === "create").payload.parentId, 10001);
});

test("a parent CoderHelm already knows is used without reading the comment first", async () => {
  const jira = fakeJira({ 10001: { id: "10001" } }, threading);
  await postReply({
    jira,
    storage: memoryStorage(),
    issueKey: "ABC-1",
    adfBody,
    replyTo: { commentId: "10005", parentId: "10001" },
    now: NOW,
  });
  assert.equal(jira.calls[0].op, "create");
  assert.equal(jira.calls[0].payload.parentId, 10001);
});

test("a refusal that isn't about the parent is an error, not a retry", async () => {
  const jira = fakeJira({ 10001: { id: "10001" } }, () => response(403, { errorMessages: ["No permission to add comments"] }));
  const storage = memoryStorage();
  const got = await postReply({ jira, storage, issueKey: "ABC-1", adfBody, replyTo: { commentId: "10001" }, now: NOW });
  assert.equal(got.ok, false);
  assert.equal(got.status, 403);
  assert.equal(jira.calls.filter((c) => c.op === "create").length, 1);
  assert.equal(storage.kept[STORAGE_KEY], undefined);
});

test("a comment that answers nobody is a plain comment", async () => {
  const jira = fakeJira({}, threading);
  const got = await postReply({ jira, storage: memoryStorage(), issueKey: "ABC-1", adfBody, replyTo: undefined, now: NOW });
  assert.equal(got.ok, true);
  assert.equal(got.threaded, false);
  assert.deepEqual(jira.calls.map((c) => c.op), ["create"]);
  assert.equal(jira.calls[0].payload.parentId, undefined);
});

test("reading parentId: numbers, strings and none", () => {
  assert.equal(parentIdOf({ parentId: 10001 }), "10001");
  assert.equal(parentIdOf({ parentId: "10001" }), "10001");
  assert.equal(parentIdOf({ parentId: null }), "");
  assert.equal(parentIdOf({ parentId: 0 }), "");
  assert.equal(parentIdOf({}), "");
  assert.equal(parentIdOf(undefined), "");
  assert.ok(refusedForParent(400, "Parent comment not found"));
  assert.ok(!refusedForParent(500, "parent"));
  assert.ok(!refusedForParent(400, "body is required"));
});

test("a ticket's comments carry the parent of a reply", () => {
  const got = existingComments({
    comment: {
      comments: [
        { id: "10001", author: { displayName: "Coderhelm", accountType: "app" }, body: "Which bucket?" },
        { id: "10002", parentId: 10001, author: { displayName: "Sam Lee", accountType: "atlassian" }, body: "Staging." },
      ],
    },
  });
  assert.equal(got[0].parentId, undefined);
  assert.equal(got[1].parentId, "10001");
});
