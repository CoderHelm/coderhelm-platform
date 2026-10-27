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
 * - The tag $VERSION, fetched from this repository with git, must be an
 *   annotated tag named exactly $VERSION, signed with an SSH key pinned in
 *   selfhost/maintainers.json (written into the build at synth time, so a
 *   release is checked with the keys of the release already installed, never
 *   with keys it brings itself), on a commit that is on main. That commit is
 *   $COMMIT.
 * - SHA256SUMS must carry a keyless signature whose certificate names
 *   exactly this repository's release.yml on refs/tags/$VERSION, run on
 *   $COMMIT, issued by GitHub Actions' OIDC issuer; cosign also requires its
 *   Rekor (transparency log) entry, since no --insecure-ignore-tlog is passed.
 *   So the workflow that signed is the release.yml of the commit a maintainer
 *   signed: a release.yml changed and pushed under a new tag can't produce a
 *   release an install accepts.
 *
 * The Launch Stack template is plain YAML, so its buildspec repeats these
 * lines; infra/test/launch-stack.test.ts checks they stay identical.
 */

import * as fs from "fs";
import { repoPath } from "./paths";

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

export interface MaintainerKey {
  login: string;
  key: string;
}

const KEY_RE = /^(ssh-ed25519|ecdsa-sha2-nistp256|ecdsa-sha2-nistp384|ecdsa-sha2-nistp521|sk-ssh-ed25519@openssh\.com|sk-ecdsa-sha2-nistp256@openssh\.com|ssh-rsa) [A-Za-z0-9+/]+={0,3}$/;
const LOGIN_RE = /^[A-Za-z0-9-]{1,39}$/;

/** The SSH keys allowed to sign a release tag (selfhost/maintainers.json). Throws on anything else. */
export function maintainerKeys(file = repoPath("selfhost", "maintainers.json")): MaintainerKey[] {
  const m = JSON.parse(fs.readFileSync(file, "utf8"));
  if (!m || !Array.isArray(m.keys) || m.keys.length === 0) throw new Error(`${file} lists no maintainer keys`);
  return m.keys.map((k: MaintainerKey) => {
    if (!LOGIN_RE.test(String(k.login))) throw new Error(`${file}: ${JSON.stringify(k.login)} is not a GitHub login`);
    if (!KEY_RE.test(String(k.key))) throw new Error(`${file}: the key for ${k.login} is not an SSH public key (type and base64, no comment)`);
    return { login: k.login, key: k.key };
  });
}

/** Where the build keeps the tag's git objects and the allowed signers file. */
const TAG_DIR = "/tmp/ch-tag";
/** git with no global or system configuration. */
const G = `env GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1 GIT_TERMINAL_PROMPT=0 git -C ${TAG_DIR}`;

/**
 * Checks the tag $VERSION and sets $COMMIT to the commit it points to:
 * annotated, named $VERSION, SSH-signed by a pinned maintainer key, on main.
 * Each line exits the build with the reason when its check fails.
 */
export function verifyTagCommands(keys: MaintainerKey[] = maintainerKeys()): string[] {
  const lines = keys.map((k) => `'${k.login} namespaces="git" ${k.key}'`).join(" ");
  return [
    `rm -rf ${TAG_DIR} && mkdir -p ${TAG_DIR} && printf '%s\\n' ${lines} > ${TAG_DIR}.signers`,
    `${G} init -q --bare`,
    `${G} fetch -q --no-tags --filter=tree:0 https://github.com/${RELEASE_REPO}.git "+refs/tags/$VERSION:refs/tags/$VERSION" "+refs/heads/main:refs/heads/main"`,
    `[ "$(${G} cat-file -t "refs/tags/$VERSION")" = tag ] || { echo "$VERSION is not an annotated, signed tag"; exit 1; }`,
    `[ "$(${G} cat-file tag "refs/tags/$VERSION" | sed -n '/^$/q;s/^tag //p')" = "$VERSION" ] || { echo "the tag object under $VERSION is named for another version"; exit 1; }`,
    `[ "$(${G} cat-file tag "refs/tags/$VERSION" | sed -n '/^$/q;s/^type //p')" = commit ] || { echo "$VERSION does not point to a commit"; exit 1; }`,
    `[ "$(${G} cat-file tag "refs/tags/$VERSION" | grep -cxF -e '-----BEGIN SSH SIGNATURE-----')" = 1 ] || { echo "$VERSION has no SSH signature"; exit 1; }`,
    `${G} -c gpg.format=ssh -c gpg.ssh.allowedSignersFile=${TAG_DIR}.signers -c gpg.ssh.program=ssh-keygen verify-tag "refs/tags/$VERSION" || { echo "$VERSION is not signed by a CoderHelm maintainer key (selfhost/maintainers.json)"; exit 1; }`,
    `COMMIT=$(${G} rev-parse --verify "refs/tags/$VERSION^{commit}")`,
    `[[ "$COMMIT" =~ ^[0-9a-f]{40}$ ]] && ${G} merge-base --is-ancestor "$COMMIT" refs/heads/main || { echo "$VERSION ($COMMIT) is not on main"; exit 1; }`,
  ];
}

/** Checks SHA256SUMS against its signature: exactly release.yml on this tag and on $COMMIT, GitHub's issuer, in Rekor. */
export const VERIFY_SUMS =
  "cosign verify-blob --bundle SHA256SUMS.sigstore.json" +
  ` --certificate-identity "https://github.com/${RELEASE_WORKFLOW}@refs/tags/$VERSION"` +
  ` --certificate-oidc-issuer ${SIGNER_ISSUER}` +
  ` --certificate-github-workflow-repository ${RELEASE_REPO}` +
  ` --certificate-github-workflow-ref "refs/tags/$VERSION"` +
  ` --certificate-github-workflow-sha "$COMMIT"` +
  " SHA256SUMS";
