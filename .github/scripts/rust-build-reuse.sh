#!/usr/bin/env bash
# Finds a Rust Lambda build (gateway.zip, worker.zip) that can be used instead
# of building again, checks it, and copies it into <out>. Prints hit=true or
# hit=false for $GITHUB_OUTPUT; anything that doesn't check out is a miss, and
# the caller builds (scripts/rust-build.sh). Never fails the job.
#
# The build is the "rust-build" artifact of a deploy run (deploy.yml): the two
# zips and rust-build.json, which names the Rust inputs digest
# (scripts/rust-inputs.mjs), the commit the run deployed, the run that
# compiled them and their sha256. A run is used only when it is deploy.yml
# itself, on main of this repository, started by its schedule or by hand, and
# finished successfully.
#
#   deploy   <out> <inputs>           the newest such run with the same inputs
#                                     digest (a deploy with no Rust change)
#   release  <out> <inputs> <commit>  the run that deployed exactly <commit>,
#                                     whose build is for <commit> and these inputs
#
# Needs GH_TOKEN with actions:read, gh, jq and sha256sum.
set -uo pipefail

mode=${1:-}
out=${2:-}
inputs=${3:-}
commit=${4:-}
repo=${GITHUB_REPOSITORY:?}

miss() {
  echo "Not reusing a build: $1" >&2
  echo "hit=false"
  exit 0
}

[ -n "$out" ] || miss "no output directory"
[[ "$inputs" =~ ^[0-9a-f]{64}$ ]] || miss "the Rust inputs digest is unknown"

if [ "$mode" = release ]; then
  [[ "$commit" =~ ^[0-9a-f]{40}$ ]] || miss "no commit"
  query="branch=main&status=success&head_sha=$commit&per_page=20"
elif [ "$mode" = deploy ]; then
  query="branch=main&status=success&per_page=30"
else
  miss "unknown mode $mode"
fi

runs=$(gh api "repos/$repo/actions/workflows/deploy.yml/runs?$query" --jq \
  ".workflow_runs[] | select(.path == \".github/workflows/deploy.yml\" and .head_branch == \"main\" and .head_repository.full_name == \"$repo\" and .conclusion == \"success\" and (.event == \"schedule\" or .event == \"workflow_dispatch\")) | [.id, .head_sha] | @tsv" 2>/dev/null) \
  || miss "could not list deploy runs"

work=$(mktemp -d)
while IFS=$'\t' read -r run sha; do
  [ -n "$run" ] || continue
  [ "$mode" = release ] && [ "$sha" != "$commit" ] && continue
  art=$(gh api "repos/$repo/actions/runs/$run/artifacts" --jq '[.artifacts[] | select(.name == "rust-build" and (.expired | not))] | length' 2>/dev/null || echo 0)
  [ "$art" = 1 ] || continue
  d="$work/$run"
  gh run download "$run" -R "$repo" -n rust-build -D "$d" >/dev/null 2>&1 || continue
  m="$d/rust-build.json"
  [ -f "$m" ] || continue
  [ "$(jq -r .inputs "$m")" = "$inputs" ] || { [ "$mode" = release ] && miss "run $run built other Rust inputs"; continue; }
  [ "$(jq -r .commit "$m")" = "$sha" ] || continue
  ok=1
  for f in gateway.zip worker.zip; do
    want=$(jq -r --arg f "$f" '.files[$f] // ""' "$m")
    [[ "$want" =~ ^[0-9a-f]{64}$ ]] || { ok=0; break; }
    [ -f "$d/$f" ] && [ "$(sha256sum "$d/$f" | cut -d' ' -f1)" = "$want" ] || { ok=0; break; }
  done
  [ "$ok" = 1 ] || continue
  mkdir -p "$out"
  cp "$d/gateway.zip" "$d/worker.zip" "$out/"
  jq -r '"built_at=\(.built_at)\nbuild_run=\(.build_run)"' "$m"
  echo "Reusing the build from deploy run $run (compiled in run $(jq -r .build_run "$m") at $(jq -r .built_at "$m"))" >&2
  echo "hit=true"
  exit 0
done <<< "$runs"

miss "no deploy run on main has a checked build for these inputs"
