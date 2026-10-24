#!/usr/bin/env node
// Points this Forge app at a CoderHelm API: rewrites src/gateway.js and the
// manifest's external fetch permission. For a self-hosted CoderHelm, run it
// with your URL, then `forge register` (a new app id, yours) and
// `forge deploy` / `forge install`.
//
//   node scripts/set-gateway.js https://coderhelm.example.com

const fs = require("node:fs");
const path = require("node:path");

function setGateway(root, url) {
  const u = new URL(url);
  if (u.protocol !== "https:") throw new Error("The URL must be https (Forge only calls https hosts).");
  const origin = u.origin;
  const gw = path.join(root, "src", "gateway.js");
  const before = fs.readFileSync(gw, "utf8");
  const after = before.replace(/const GATEWAY_URL = "[^"]*";/, `const GATEWAY_URL = ${JSON.stringify(origin)};`);
  if (after === before && !before.includes(JSON.stringify(origin))) throw new Error("src/gateway.js has no GATEWAY_URL line");
  fs.writeFileSync(gw, after);
  const mf = path.join(root, "manifest.yml");
  const m = fs.readFileSync(mf, "utf8");
  const m2 = m.replace(/(external:\n\s+fetch:\n\s+backend:\n\s+- )\S+/, `$1${origin}`);
  if (m2 === m && !m.includes(`- ${origin}`)) throw new Error("manifest.yml has no external fetch backend entry");
  fs.writeFileSync(mf, m2);
  return origin;
}

module.exports = { setGateway };

if (require.main === module) {
  const url = process.argv[2];
  if (!url) {
    console.error("Usage: node scripts/set-gateway.js https://your-coderhelm-url");
    process.exit(2);
  }
  const origin = setGateway(path.join(__dirname, ".."), url);
  console.log(`Pointed the app at ${origin}. Next: forge register, then forge deploy and forge install.`);
}
