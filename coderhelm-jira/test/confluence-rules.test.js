// Run with `npm test` (plain Node; no Forge runtime needed).
const test = require("node:test");
const assert = require("node:assert/strict");
const { parseRequest, appendUpdate, MAX_APPEND_MARKUP } = require("../src/confluence-rules");

test("reads take only numeric ids", () => {
  assert.deepEqual(parseRequest({ op: "page", id: "42" }), { op: "page", id: "42" });
  assert.deepEqual(parseRequest({ op: "children", id: 42 }), { op: "children", id: "42" });
  assert.ok(parseRequest({ op: "page", id: "42/../1" }).error);
  assert.ok(parseRequest({ op: "space", id: "" }).error);
  assert.ok(parseRequest({ op: "page" }).error);
});

test("searches must be page searches", () => {
  const cql = 'type = page AND text ~ "deploy" ORDER BY lastmodified DESC';
  assert.deepEqual(parseRequest({ op: "search", cql }), { op: "search", cql });
  assert.ok(parseRequest({ op: "search", cql: "type = attachment" }).error);
  assert.ok(parseRequest({ op: "search", cql: "type = page " + "x".repeat(2000) }).error);
  assert.ok(parseRequest({ op: "search" }).error);
});

test("only the listed operations exist", () => {
  for (const op of ["create", "delete", "move", "update", "", undefined]) {
    assert.ok(parseRequest({ op, id: "42" }).error, `op ${op}`);
  }
});

test("an append needs markup and the version that was read", () => {
  assert.deepEqual(parseRequest({ op: "append", id: "42", html: "<p>x</p>", version: 3 }), {
    op: "append",
    id: "42",
    html: "<p>x</p>",
    version: 3,
  });
  assert.ok(parseRequest({ op: "append", id: "42", html: "", version: 3 }).error);
  assert.ok(parseRequest({ op: "append", id: "42", html: "<p>x</p>" }).error);
  assert.ok(parseRequest({ op: "append", id: "42", html: "<p>x</p>", version: 0 }).error);
  assert.ok(
    parseRequest({ op: "append", id: "42", html: "x".repeat(MAX_APPEND_MARKUP + 1), version: 3 }).error
  );
});

const PAGE = {
  id: "42",
  title: "Lessons",
  version: { number: 3 },
  body: { storage: { value: "<h1>Lessons</h1><p>one</p>" } },
};

test("an append keeps the page and adds to its end as the next version", () => {
  const u = appendUpdate(PAGE, "<p>two</p>", 3);
  assert.equal(u.body.value, "<h1>Lessons</h1><p>one</p><p>two</p>");
  assert.equal(u.version.number, 4);
  assert.equal(u.title, "Lessons");
  assert.equal(u.id, "42");
});

test("an append to a page that changed since it was read is refused", () => {
  assert.equal(appendUpdate(PAGE, "<p>two</p>", 2), null);
  assert.equal(appendUpdate({ ...PAGE, version: undefined }, "<p>two</p>", 3), null);
});
