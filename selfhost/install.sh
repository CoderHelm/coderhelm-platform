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
# The release's tag must be an annotated tag on a commit on main, signed
# with a maintainer SSH key pinned below (selfhost/maintainers.json), or, for
# a release cut automatically after main's deploy, attested by exactly
# CoderHelm/coderhelm-platform's auto-release.yml on main for that tag and
# commit (a keyless signature in the tag message). The release's signature
# must be cosign's keyless signature made by exactly
# CoderHelm/coderhelm-platform's release.yml on the release's own tag, from
# that commit (cosign), or the installer's GitHub build attestation from that
# workflow, tag and commit (gh). Without git, ssh-keygen and cosign or gh, or
# on any mismatch, nothing runs.
set -eu

REPO="CoderHelm/coderhelm-platform"
VERSION="${CODERHELM_VERSION:-latest}"
ISSUER="https://token.actions.githubusercontent.com"
# The maintainer keys allowed to sign a release tag: selfhost/maintainers.json
# (installer/test/release-security.test.mjs checks they are the same).
SIGNERS='nambok namespaces="git" ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIH6UjRCKMcz6OyGQkv2PCBbzqzKx+ngQQmavwp8UDASV'

need() { command -v "$1" >/dev/null 2>&1 || { echo "Please install $2 first: $3" >&2; exit 1; }; }
need node "Node.js 20 or newer" "https://nodejs.org/"
need npx "npm (comes with Node.js)" "https://nodejs.org/"
need aws "the AWS CLI v2" "https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html"
need curl "curl" "your package manager"
need git "git 2.34 or newer" "https://git-scm.com/downloads"
need ssh-keygen "OpenSSH (ssh-keygen)" "your package manager"

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

# The tag: annotated, named $VERSION, on main, signed with a pinned maintainer key
# or attested by auto-release.yml on main. Git runs without your git configuration,
# so nothing local can make it pass.
refuse() { echo "$1. Not running it." >&2; exit 1; }
src="$dir/src"
g() { GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1 GIT_TERMINAL_PROMPT=0 git -C "$src" "$@"; }
mkdir -p "$src" && g init -q --bare
printf '%s\n' "$SIGNERS" > "$dir/allowed_signers"
g fetch -q --no-tags --filter=tree:0 "https://github.com/$REPO.git" "+refs/tags/$VERSION:refs/tags/$VERSION" "+refs/heads/main:refs/heads/main" \
  || refuse "Could not fetch the tag $VERSION from GitHub"
[ "$(g cat-file -t "refs/tags/$VERSION")" = tag ] || refuse "$VERSION is not an annotated tag"
g cat-file tag "refs/tags/$VERSION" > "$dir/tag"
[ "$(sed -n '/^$/q;s/^tag //p' "$dir/tag")" = "$VERSION" ] || refuse "The tag object under $VERSION is named for another version"
COMMIT=$(g rev-parse --verify "refs/tags/$VERSION^{commit}")
g merge-base --is-ancestor "$COMMIT" refs/heads/main || refuse "$VERSION ($COMMIT) is not on main"
if grep -qxF -e '-----BEGIN SSH SIGNATURE-----' "$dir/tag"; then
  # Cut by hand: a maintainer's SSH signature, and only that, decides.
  [ "$(grep -cxF -e '-----BEGIN SSH SIGNATURE-----' "$dir/tag")" = 1 ] || refuse "$VERSION has more than one SSH signature"
  g -c gpg.format=ssh -c gpg.ssh.allowedSignersFile="$dir/allowed_signers" -c gpg.ssh.program=ssh-keygen verify-tag "refs/tags/$VERSION" >/dev/null 2>&1 \
    || refuse "$VERSION is not signed by a CoderHelm maintainer key"
  vouched="tag signed by a maintainer"
else
  # Cut after main's deploy: auto-release.yml on main signed this statement of the tag and commit.
  BEGIN='-----BEGIN CODERHELM RELEASE ATTESTATION-----'
  END='-----END CODERHELM RELEASE ATTESTATION-----'
  { [ "$(grep -cxF -e "$BEGIN" "$dir/tag")" = 1 ] && [ "$(grep -cxF -e "$END" "$dir/tag")" = 1 ]; } \
    || refuse "$VERSION has no SSH signature and no release attestation"
  sed -n "/^$BEGIN\$/,/^$END\$/p" "$dir/tag" | sed '1d;$d' \
    | node -e 'let s="";process.stdin.on("data",(d)=>(s+=d)).on("end",()=>process.stdout.write(Buffer.from(s.replace(/\s+/g,""),"base64")))' > "$dir/tag.sigstore.json"
  statement="$dir/$VERSION.release-tag"
  printf 'coderhelm-release-tag v1\nrepository %s\ntag %s\ncommit %s\n' "$REPO" "$VERSION" "$COMMIT" > "$statement"
  if command -v cosign >/dev/null 2>&1; then
    cosign verify-blob --bundle "$dir/tag.sigstore.json" \
      --certificate-identity "https://github.com/$REPO/.github/workflows/auto-release.yml@refs/heads/main" --certificate-oidc-issuer "$ISSUER" \
      --certificate-github-workflow-repository "$REPO" --certificate-github-workflow-ref refs/heads/main \
      --certificate-github-workflow-sha "$COMMIT" \
      "$statement" >/dev/null 2>&1 || refuse "$VERSION's release attestation is not from auto-release.yml on main for this tag and commit"
  elif command -v gh >/dev/null 2>&1; then
    gh attestation verify "$statement" --repo "$REPO" \
      --signer-workflow "$REPO/.github/workflows/auto-release.yml" --source-ref refs/heads/main --source-digest "$COMMIT" \
      --cert-oidc-issuer "$ISSUER" --deny-self-hosted-runners >/dev/null 2>&1 \
      || refuse "$VERSION has no valid attestation from auto-release.yml on main for this tag and commit"
  else
    echo "Can't check the release's tag: install cosign (https://docs.sigstore.dev/cosign/system_config/installation/) or the GitHub CLI (gh), then run this again." >&2
    exit 1
  fi
  vouched="released by $REPO's auto-release workflow on main"
fi

if command -v cosign >/dev/null 2>&1; then
  cosign verify-blob --bundle "$dir/SHA256SUMS.sigstore.json" \
    --certificate-identity "$identity" --certificate-oidc-issuer "$ISSUER" \
    --certificate-github-workflow-repository "$REPO" --certificate-github-workflow-ref "refs/tags/$VERSION" \
    --certificate-github-workflow-sha "$COMMIT" \
    "$dir/SHA256SUMS" >/dev/null 2>&1 || { echo "The release's signature does not verify. Not running it." >&2; exit 1; }
elif command -v gh >/dev/null 2>&1; then
  gh attestation verify "$dir/coderhelm-selfhost.tgz" --repo "$REPO" \
    --signer-workflow "$REPO/.github/workflows/release.yml" --source-ref "refs/tags/$VERSION" --source-digest "$COMMIT" \
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
echo "Verified CoderHelm $VERSION ($vouched, commit $COMMIT on main, built and signed by $REPO's release workflow)."

run() { npx --yes --package "$dir/coderhelm-selfhost.tgz" coderhelm-selfhost "$@" < /dev/tty; }
[ -f coderhelm.config.json ] || run init
run deploy
