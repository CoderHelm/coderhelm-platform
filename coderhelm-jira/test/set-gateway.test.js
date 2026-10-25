const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { setGateway } = require("../scripts/set-gateway");

const ROOT = path.join(__dirname, "..");

function copy() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "forge-"));
  fs.mkdirSync(path.join(dir, "src"));
  fs.copyFileSync(path.join(ROOT, "src", "gateway.js"), path.join(dir, "src", "gateway.js"));
  fs.copyFileSync(path.join(ROOT, "manifest.yml"), path.join(dir, "manifest.yml"));
  return dir;
}

test("points the app and its fetch permission at another URL", () => {
  const dir = copy();
  assert.equal(setGateway(dir, "https://helm.example.org/some/path"), "https://helm.example.org");
  assert.match(fs.readFileSync(path.join(dir, "src", "gateway.js"), "utf8"), /GATEWAY_URL = "https:\/\/helm\.example\.org"/);
  const mf = fs.readFileSync(path.join(dir, "manifest.yml"), "utf8");
  assert.match(mf, /- https:\/\/helm\.example\.org\n/);
  assert.doesNotMatch(mf, /api\.coderhelm\.com/);
});

test("refuses plain http", () => {
  assert.throws(() => setGateway(copy(), "http://helm.example.org"), /https/);
});

test("the app reads the URL from one place", () => {
  for (const f of ["index.js", "register.js"]) {
    const src = fs.readFileSync(path.join(ROOT, "src", f), "utf8");
    assert.doesNotMatch(src, /https:\/\/api\.coderhelm\.com/, f);
    assert.match(src, /require\("\.\/gateway"\)/, f);
  }
});
