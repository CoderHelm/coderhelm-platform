#!/bin/sh
# Install CoderHelm in your own AWS account.
#
#   curl -fsSL https://github.com/CoderHelm/coderhelm-platform/releases/latest/download/install.sh | sh
#
# Checks for Node.js 20+ and the AWS CLI v2, downloads the installer for the
# latest release (or $CODERHELM_VERSION), checks it against the release's
# signed SHA256SUMS, and runs "coderhelm-selfhost init" then "deploy".
# Nothing is installed globally.
#
# The signature must be cosign's keyless signature made by exactly
# CoderHelm/coderhelm-platform's release.yml on the release's own tag
# (cosign), or the installer's GitHub build attestation from that workflow
# and tag (gh). Without cosign or gh, or on any mismatch, nothing runs.
set -eu

REPO="CoderHelm/coderhelm-platform"
VERSION="${CODERHELM_VERSION:-latest}"
ISSUER="https://token.actions.githubusercontent.com"

need() { command -v "$1" >/dev/null 2>&1 || { echo "Please install $2 first: $3" >&2; exit 1; }; }
need node "Node.js 20 or newer" "https://nodejs.org/"
need npx "npm (comes with Node.js)" "https://nodejs.org/"
need aws "the AWS CLI v2" "https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html"
need curl "curl" "your package manager"

major=$(node -p 'process.versions.node.split(".")[0]')
[ "$major" -ge 20 ] || { echo "Node.js 20 or newer is needed (found $(node --version))." >&2; exit 1; }

get() { curl --proto =https --proto-redir =https --tlsv1.2 -fsSL "$@"; }

# The signature names one tag, so "latest" is turned into it first.
if [ "$VERSION" = "latest" ]; then
  VERSION=$(get "https://api.github.com/repos/$REPO/releases/latest" | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{const r=JSON.parse(s);if(r.draft||r.prerelease)process.exit(1);process.stdout.write(String(r.tag_name))})')
fi
echo "$VERSION" | grep -Eq '^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$' || { echo "\"$VERSION\" is not a CoderHelm release (vX.Y.Z)." >&2; exit 1; }
base="https://github.com/$REPO/releases/download/$VERSION"
identity="https://github.com/$REPO/.github/workflows/release.yml@refs/tags/$VERSION"

dir=$(mktemp -d)
trap 'rm -rf "$dir"' EXIT
get -o "$dir/coderhelm-selfhost.tgz" "$base/coderhelm-selfhost.tgz"
get -o "$dir/SHA256SUMS" "$base/SHA256SUMS"
get -o "$dir/SHA256SUMS.sigstore.json" "$base/SHA256SUMS.sigstore.json"

if command -v cosign >/dev/null 2>&1; then
  cosign verify-blob --bundle "$dir/SHA256SUMS.sigstore.json" \
    --certificate-identity "$identity" --certificate-oidc-issuer "$ISSUER" \
    --certificate-github-workflow-repository "$REPO" --certificate-github-workflow-ref "refs/tags/$VERSION" \
    "$dir/SHA256SUMS" >/dev/null 2>&1 || { echo "The release's signature does not verify. Not running it." >&2; exit 1; }
elif command -v gh >/dev/null 2>&1; then
  gh attestation verify "$dir/coderhelm-selfhost.tgz" --repo "$REPO" \
    --signer-workflow "$REPO/.github/workflows/release.yml" --source-ref "refs/tags/$VERSION" \
    --cert-oidc-issuer "$ISSUER" --deny-self-hosted-runners >/dev/null 2>&1 \
    || { echo "The installer has no valid build attestation from CoderHelm's release workflow. Not running it." >&2; exit 1; }
else
  echo "Can't check the release's signature: install cosign (https://docs.sigstore.dev/cosign/system_config/installation/) or the GitHub CLI (gh), then run this again." >&2
  exit 1
fi

want=$(grep ' coderhelm-selfhost.tgz$' "$dir/SHA256SUMS" | cut -d' ' -f1)
if command -v sha256sum >/dev/null 2>&1; then got=$(sha256sum "$dir/coderhelm-selfhost.tgz" | cut -d' ' -f1)
else got=$(shasum -a 256 "$dir/coderhelm-selfhost.tgz" | cut -d' ' -f1); fi
[ -n "$want" ] && [ "$want" = "$got" ] || { echo "The installer's checksum does not match. Not running it." >&2; exit 1; }
echo "Verified CoderHelm $VERSION (signed by $REPO's release workflow)."

run() { npx --yes --package "$dir/coderhelm-selfhost.tgz" coderhelm-selfhost "$@" < /dev/tty; }
[ -f coderhelm.config.json ] || run init
run deploy
