#!/usr/bin/env node
// Fails unless selfhost/versions.json pins the dashboard to a full commit SHA.
// A branch or tag there would make a signed release bundle whatever that
// name points to when the release runs, not the commit reviewed here.
//
//   node scripts/check-dashboard-ref.mjs [path/to/versions.json]
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

export function checkDashboardRef(versions) {
  const ref = versions?.dashboardRef;
  if (typeof ref !== "string" || !/^[0-9a-f]{40}$/.test(ref)) {
    throw new Error(
      `selfhost/versions.json: dashboardRef must be a full 40-character commit SHA of CoderHelm/coderhelm, not ${JSON.stringify(ref)}. ` +
        "Run scripts/bump-dashboard.sh to pin one, and open a PR.",
    );
  }
  return ref;
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const file = process.argv[2] ?? path.join(path.dirname(fileURLToPath(import.meta.url)), "..", "selfhost", "versions.json");
  try {
    const ref = checkDashboardRef(JSON.parse(fs.readFileSync(file, "utf8")));
    console.log(`dashboardRef ${ref}: a commit SHA`);
  } catch (e) {
    console.error(`::error::${e.message}`);
    process.exit(1);
  }
}
