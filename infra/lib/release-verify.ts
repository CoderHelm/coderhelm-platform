/**
 * How a CodeBuild project (the update build, infra/lib/updates-stack.ts, and
 * the Launch Stack's installer, infra/cfn/coderhelm-selfhost-launch.yaml)
 * checks a CoderHelm release before running anything from it.
 *
 * - cosign is downloaded from its GitHub release and must match a SHA-256
 *   pinned here (not only the checksums file published next to it).
 * - $VERSION must be a release tag (vX.Y.Z), so it can't reach a path or a
 *   command as anything else.
 * - Downloads are HTTPS only, redirects included (curl --proto =https).
 * - SHA256SUMS must carry a keyless signature whose certificate names
 *   exactly this repository's release.yml on refs/tags/$VERSION, issued by
 *   GitHub Actions' OIDC issuer; cosign also requires its Rekor (transparency
 *   log) entry, since no --insecure-ignore-tlog is passed.
 *
 * The Launch Stack template is plain YAML, so its buildspec repeats these
 * lines; infra/test/launch-stack.test.ts checks they stay identical.
 */

export const RELEASE_REPO = "CoderHelm/coderhelm-platform";
export const RELEASE_WORKFLOW = `${RELEASE_REPO}/.github/workflows/release.yml`;
export const SIGNER_ISSUER = "https://token.actions.githubusercontent.com";

/** The cosign release the builds install, and its linux-amd64 binary's SHA-256 (cosign_checksums.txt of that release). */
export const COSIGN_VERSION = "v2.4.1";
export const COSIGN_LINUX_AMD64_SHA256 = "8b24b946dd5809c6bd93de08033bcf6bc0ed7d336b7785787c080f574b89249b";

/** curl that only speaks HTTPS, also after a redirect. */
export const CURL = "curl --proto =https --proto-redir =https --tlsv1.2 -fsSL";

/** Installs the pinned cosign. */
export function installCosignCommands(): string[] {
  return [
    `${CURL} -o /tmp/cosign https://github.com/sigstore/cosign/releases/download/${COSIGN_VERSION}/cosign-linux-amd64`,
    `echo "${COSIGN_LINUX_AMD64_SHA256}  /tmp/cosign" | sha256sum -c -`,
    "install -m 0755 /tmp/cosign /usr/local/bin/cosign",
  ];
}

/** Refuses a $VERSION that isn't a release tag. */
export const CHECK_VERSION = `[[ "$VERSION" =~ ^v[0-9]+\\.[0-9]+\\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]] || { echo "VERSION=$VERSION is not a release tag"; exit 1; }`;

/** Where release files come from. */
export const RELEASE_BASE = `B="https://github.com/${RELEASE_REPO}/releases/download/$VERSION"`;

/** Checks SHA256SUMS against its signature: exactly release.yml on this tag, GitHub's issuer, in Rekor. */
export const VERIFY_SUMS =
  "cosign verify-blob --bundle SHA256SUMS.sigstore.json" +
  ` --certificate-identity "https://github.com/${RELEASE_WORKFLOW}@refs/tags/$VERSION"` +
  ` --certificate-oidc-issuer ${SIGNER_ISSUER}` +
  ` --certificate-github-workflow-repository ${RELEASE_REPO}` +
  ` --certificate-github-workflow-ref "refs/tags/$VERSION"` +
  " SHA256SUMS";
