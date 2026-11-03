#!/usr/bin/env bash
# Publishes release files to the public bucket, each written once: a key that
# already holds a file must hold this same file (a re-run of the same tag), or
# the publish fails. Used by release.yml and auto-release.yml.
#
#   publish-once.sh LIST           # LIST: lines of "file<TAB>key<TAB>content-type"
#
# Every line but the last is uploaded in parallel; the last (the parent
# template, coderhelm.json) goes up only after all of them, so an install
# never finds a parent whose parts are missing. Needs PUBLIC_BUCKET and AWS
# credentials that may PutObject.
#
# One request per file: PutObject with If-None-Match: * writes only a new key.
# When the key exists (412 PreconditionFailed, or 409 while another write of
# it is in flight) the file there is downloaded and compared byte for byte.
set -euo pipefail

one() {
  local file="$1" key="$2" ct="$3" err
  err=$(mktemp)
  if aws s3api put-object --bucket "$PUBLIC_BUCKET" --key "$key" --body "$file" --if-none-match '*' \
    --content-type "$ct" --cache-control "public, max-age=31536000, immutable" >/dev/null 2>"$err"; then
    echo "put  $key"
    rm -f "$err"
    return 0
  fi
  if ! grep -qE 'PreconditionFailed|ConditionalRequestConflict' "$err"; then
    cat "$err" >&2
    echo "::error::$key could not be written"
    rm -f "$err"
    return 1
  fi
  rm -f "$err"
  local there
  there=$(mktemp)
  # A 409 means another write of this key was in flight: give it a moment.
  for _ in 1 2 3 4 5; do
    if curl --proto =https -fsSL -o "$there" "https://$PUBLIC_BUCKET.s3.amazonaws.com/$key"; then break; fi
    sleep 2
  done
  if cmp -s "$there" "$file"; then
    echo "same $key"
    rm -f "$there"
    return 0
  fi
  rm -f "$there"
  echo "::error::$key already holds another file"
  return 1
}

# One "file<TAB>key<TAB>content-type" line.
one_line() {
  local f k c
  IFS=$'\t' read -r f k c <<<"$1"
  one "$f" "$k" "$c"
}

if [ "${1:-}" = "--line" ]; then
  one_line "$2"
  exit
fi

list="${1:?usage: publish-once.sh LIST}"
: "${PUBLIC_BUCKET:?PUBLIC_BUCKET is not set}"
[ -s "$list" ] || { echo "::error::$list is empty"; exit 1; }
last=$(tail -n 1 "$list")
sed '$d' "$list" | tr '\n' '\0' | xargs -0 -n 1 -P 8 "$0" --line
one_line "$last"
