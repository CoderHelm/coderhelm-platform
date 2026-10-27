#!/usr/bin/env bash
# Pins the dashboard a release ships (selfhost/versions.json, dashboardRef)
# to a commit of CoderHelm/coderhelm, by its full SHA. The commit must be on
# that repository's main. Then open a PR with the change: the release
# workflow builds exactly that commit, so the bump is reviewed like code.
#
#   bash scripts/bump-dashboard.sh              # main's current commit
#   bash scripts/bump-dashboard.sh <sha>        # a given commit on main
#
# Needs git and node. Reads public data only (no token).
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
want="${1:-}"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

git -C "$tmp" init -q --bare
git -C "$tmp" fetch -q --no-tags --filter=tree:0 https://github.com/CoderHelm/coderhelm.git +refs/heads/main:refs/heads/main
if [ -z "$want" ]; then
  want=$(git -C "$tmp" rev-parse refs/heads/main)
fi
[[ "$want" =~ ^[0-9a-f]{40}$ ]] || { echo "Give a full 40-character commit SHA (got \"$want\")." >&2; exit 1; }
git -C "$tmp" merge-base --is-ancestor "$want" refs/heads/main 2>/dev/null \
  || { echo "$want is not a commit on CoderHelm/coderhelm main." >&2; exit 1; }

# shellcheck disable=SC2016 # JavaScript, not shell
WANT="$want" node -e '
  const fs = require("fs");
  const f = process.argv[1];
  const v = JSON.parse(fs.readFileSync(f, "utf8"));
  const before = v.dashboardRef;
  v.dashboardRef = process.env.WANT;
  fs.writeFileSync(f, JSON.stringify(v, null, 2) + "\n");
  console.log(`dashboardRef was ${before}, now ${v.dashboardRef}`);
' "$root/selfhost/versions.json"
node "$root/scripts/check-dashboard-ref.mjs"
echo "Next: commit selfhost/versions.json on a branch and open a PR (signed commit)."
echo "Compare: https://github.com/CoderHelm/coderhelm/commits/$want"
