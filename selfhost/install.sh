#!/bin/sh
# Install CoderHelm in your own AWS account.
#
#   curl -fsSL https://github.com/CoderHelm/coderhelm-platform/releases/latest/download/install.sh | sh
#
# Checks for Node.js 20+ and the AWS CLI v2, downloads the installer for the
# latest release (or $CODERHELM_VERSION), verifies its checksum, and runs
# "coderhelm-selfhost init" then "deploy". Nothing is installed globally.
set -eu

REPO="CoderHelm/coderhelm-platform"
VERSION="${CODERHELM_VERSION:-latest}"

need() { command -v "$1" >/dev/null 2>&1 || { echo "Please install $2 first: $3" >&2; exit 1; }; }
need node "Node.js 20 or newer" "https://nodejs.org/"
need npx "npm (comes with Node.js)" "https://nodejs.org/"
need aws "the AWS CLI v2" "https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html"
need curl "curl" "your package manager"

major=$(node -p 'process.versions.node.split(".")[0]')
[ "$major" -ge 20 ] || { echo "Node.js 20 or newer is needed (found $(node --version))." >&2; exit 1; }

if [ "$VERSION" = "latest" ]; then
  base="https://github.com/$REPO/releases/latest/download"
else
  base="https://github.com/$REPO/releases/download/$VERSION"
fi

dir=$(mktemp -d)
trap 'rm -rf "$dir"' EXIT
curl -fsSL -o "$dir/coderhelm-selfhost.tgz" "$base/coderhelm-selfhost.tgz"
curl -fsSL -o "$dir/SHA256SUMS" "$base/SHA256SUMS"
want=$(grep ' coderhelm-selfhost.tgz$' "$dir/SHA256SUMS" | cut -d' ' -f1)
if command -v sha256sum >/dev/null 2>&1; then got=$(sha256sum "$dir/coderhelm-selfhost.tgz" | cut -d' ' -f1)
else got=$(shasum -a 256 "$dir/coderhelm-selfhost.tgz" | cut -d' ' -f1); fi
[ -n "$want" ] && [ "$want" = "$got" ] || { echo "The installer's checksum does not match. Not running it." >&2; exit 1; }

run() { npx --yes --package "$dir/coderhelm-selfhost.tgz" coderhelm-selfhost "$@" < /dev/tty; }
[ -f coderhelm.config.json ] || run init
run deploy
