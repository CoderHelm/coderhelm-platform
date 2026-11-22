#!/usr/bin/env bash
# Vendors a pinned release of Floorplan (https://github.com/haydenschmitty/floorplan,
# MIT) into vendor/floorplan: the extractor, builder and the job definitions the
# app map runs (services/worker/src/passes/app_map.rs drives them).
#
#   scripts/update-floorplan.sh floorplan--v0.11.0
#
# It clones the release tag, checks every file it copies against the release's
# own CHECKSUMS.txt, copies only those files, and records the tag in
# vendor/floorplan/VERSION. Vendored files are never edited: anything
# CoderHelm-specific lives in our adapter (services/worker/floorplan/,
# services/common/src/app_map.rs). After a bump, run the app map checks:
#   node --test services/worker/floorplan/
#   cargo test -p common app_map
set -euo pipefail

TAG="${1:?usage: scripts/update-floorplan.sh <release tag, like floorplan--v0.11.0>}"
case "$TAG" in floorplan--v[0-9]*.[0-9]*.[0-9]*) ;; *) echo "not a Floorplan release tag: $TAG" >&2; exit 1;; esac
REPO="https://github.com/haydenschmitty/floorplan.git"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DEST="$ROOT/vendor/floorplan"
# What the app map runs: the skill's scripts, its references and job file,
# and the assets build.mjs reads to write a map (the viewer template, the
# layout engine and fonts it inlines, and their licenses).
KEEP=(
  LICENSE
  CHECKSUMS.txt
  skills/floorplan/workflow.json
  skills/floorplan/references
  skills/floorplan/scripts
  skills/floorplan/assets
)

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
git -c advice.detachedHead=false clone --quiet --depth 1 --branch "$TAG" "$REPO" "$WORK/src"

sha() { if command -v sha256sum >/dev/null; then sha256sum "$1" | cut -d' ' -f1; else shasum -a 256 "$1" | cut -d' ' -f1; fi; }
# Every file we copy must be listed in CHECKSUMS.txt with a matching hash.
checked=0
while IFS= read -r f; do
  rel="${f#"$WORK/src/"}"
  [ "$rel" = CHECKSUMS.txt ] && continue
  [ "$rel" = LICENSE ] && continue
  want="$(awk -v p="$rel" '$2 == p { print $1 }' "$WORK/src/CHECKSUMS.txt")"
  if [ -z "$want" ]; then echo "not in CHECKSUMS.txt: $rel" >&2; exit 1; fi
  if [ "$(sha "$f")" != "$want" ]; then echo "checksum mismatch: $rel" >&2; exit 1; fi
  checked=$((checked + 1))
done < <(for k in "${KEEP[@]}"; do find "$WORK/src/$k" -type f; done)

rm -rf "$DEST"
mkdir -p "$DEST"
for k in "${KEEP[@]}"; do
  mkdir -p "$DEST/$(dirname "$k")"
  cp -R "$WORK/src/$k" "$DEST/$k"
done
echo "$TAG" > "$DEST/VERSION"
echo "Vendored Floorplan $TAG: $checked files checked against CHECKSUMS.txt"
