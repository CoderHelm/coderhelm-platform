---
title: How updates are kept safe
description: How CoderHelm proves a release is really CoderHelm's, what installs and updates check, what GitHub and AWS settings add, and how to verify a release yourself.
order: 9.5
---

Nothing gets onto your install just because it looks like a CoderHelm release. Before an install or update uses anything from a release, it checks two things:

1. **A maintainer signed the release's tag.** The tag's commit is on `main`.
2. **CoderHelm's release workflow built and signed the release from that same commit.**

If any check fails, nothing is installed.

This page says what each protection is and what enforces it: your install's own code, AWS, or settings on GitHub.

## What installs and updates check

The installer, the update build (`coderhelm-<stage>-update`), the release check build that runs before an update (`coderhelm-<stage>-release-check`) and the Launch Stack's installer make the same checks. They run before anything from the release is used:

1. **The tag.** `vX.Y.Z` is fetched from `https://github.com/CoderHelm/coderhelm-platform.git` with git. Several things must hold:
   - It is an annotated tag. A lightweight tag is refused.
   - The tag object's own name is exactly `vX.Y.Z`. An old signed tag can't be moved under a new name.
   - It is signed with one of the SSH keys in [`selfhost/maintainers.json`](#rotating-a-maintainer-key).
   - It points to a commit that is on `main`.

   Git runs without your own git configuration. A local key list, GPG keyring or signing program can't make a signature pass.
2. **The signature.** `SHA256SUMS` has a keyless Sigstore signature, `SHA256SUMS.sigstore.json`. Its certificate must name exactly
   `https://github.com/CoderHelm/coderhelm-platform/.github/workflows/release.yml@refs/tags/<the version>`.
   It must also:
   - be issued by `https://token.actions.githubusercontent.com`;
   - name the tag's commit as the commit the workflow ran on (Fulcio's source repository digest, build signer digest and workflow SHA);
   - be in the Rekor transparency log.

   So the workflow that signed is the `release.yml` of the commit a maintainer signed. Several kinds of signature are refused:
   - one from another repository, another workflow, a branch or another tag;
   - one made from another commit, such as a `release.yml` changed and pushed under a new tag;
   - a bundle with no Rekor entry;
   - a missing signature.
3. **The files.** Every file must match its line in `SHA256SUMS`. That covers the Lambdas, the dashboard, the source, the installer, `install.sh`, `launch-stack.yaml` and `launch-bootstrap.mjs`. A file changed after signing is refused before anything is unpacked.
4. **Where files come from.** Files come only from `https://github.com/CoderHelm/coderhelm-platform/releases/download/<version>/` over HTTPS, including after GitHub's redirects. Any other host, plain HTTP, or a version that isn't `vX.Y.Z` is refused.
5. **No going back.** An update only goes to a newer release than the one running. A rollback only goes to a release this install has run before. An admin starts it from **Settings, then System**, or with `coderhelm-selfhost rollback`. An older signed release that this install never ran can't be pushed onto it.
6. **A fresh release list.** The updater reads the release list only from `api.github.com` for this repository. If the list stops showing the newest version it saw before, **Settings, then System** marks the list as stale. It does the same when the list hasn't been read for over 3 days. A stale copy can hold an install back, but it can never make it go back. The list only decides what to offer. What is trusted is decided by the checks above, in the build.

**Whose keys.** An update build checks a new release against the maintainer keys of the release your install already runs. They are written into the build when that release was deployed. A release can't bring its own keys to vouch for itself. The installer checks with the keys in its own package, and the Launch Stack template with the keys written into it.

The installer verifies the signature with `cosign` when it is installed, or else with the GitHub CLI. With the GitHub CLI it runs `gh attestation verify`, checking the same repository, workflow, tag and commit. It also needs `git` 2.34 or newer and `ssh-keygen` for the tag. If a tool is missing, it stops and asks you to install it. No flag skips any check. To run code that isn't a signed release, deploy from a checkout you reviewed (`--source`).

## Who can publish a release

A release is a `vX.Y.Z` tag on `CoderHelm/coderhelm-platform`, built by `.github/workflows/release.yml`. The protections come from three places.

### In every install's code (always on)

The checks above. Someone who could push a tag and change `release.yml` still can't make a release that installs and updates accept without a maintainer's signing key. The signature would name a commit that no maintainer signed a tag for.

### In AWS (the hosted account's IAM)

The release workflow uploads the Launch Stack and AWS role templates to the public bucket `coderhelm-public`. It uses two roles (`infra/lib/worker-stack.ts`). Each trusts only a GitHub token whose claims match all of these:

- `sub` is `repo:CoderHelm/coderhelm-platform:environment:release`;
- `job_workflow_ref` matches `CoderHelm/coderhelm-platform/.github/workflows/release.yml@refs/tags/v*`;
- `ref` matches `refs/tags/v*`. AWS has no `ref_type` key for GitHub tokens, and only a tag's ref matches this.

Another workflow that names the `release` environment gets neither role. What each role may do:

- **`release-publisher`** may only create objects under `launch/v*/`. Its policy requires the `If-None-Match` header (S3 conditional writes), so it can't overwrite a template already there.
- **`release-latest`** may only write `launch/latest.json`, and only as a conditional write: a create, or a replace of the exact copy it read. The workflow uses it only after it has downloaded the published release and checked it as an install would.

### In GitHub's settings (applied with `scripts/release-protection.sh`)

These settings make a bad tag hard to create in the first place. They are not stored in the code. An organization owner applies them by running `scripts/release-protection.sh`. Until then they are not in force. `bash scripts/release-protection.sh --check` prints what is applied and what isn't (read only).

- **`main`, on both `CoderHelm/coderhelm-platform` and `CoderHelm/coderhelm`.** Changes go in only through a pull request with an approving review and passing checks. Commits must be signed, and force pushes and deletion are blocked. Admins may merge their own pull request after the checks pass, but never push to `main` directly. `--no-admin-merge-bypass` removes that too.
- **`v*` tags.** Tags must be signed, and nobody can update, move or delete one. There is no admin bypass unless you pass `--allow-admin-tag-bypass`. Only admins can create a `v*` tag.
- **The `release` environment.** A listed reviewer approves every publish. Only `v*` tags may deploy to it, and admins can't skip the approval. So a pushed tag alone publishes nothing.
- **Immutable releases.** A published release's files can't be replaced.
- **Actions.** Actions must be pinned by commit SHA, the default token is read-only, and workflows from outside contributors wait for approval.

### In the release workflow

These run inside `release.yml`. They catch mistakes early, but by themselves they would not stop a changed copy of the file. The install checks do that.

- The tag must be signed with a key in `main`'s `selfhost/maintainers.json`, be on `main`, and be the commit the run is on.
- The build job runs with a read-only token, no secrets and no cloud credentials. Only the publish job, in the `release` environment, can write the release or assume the AWS roles.
- A release that already exists is never given other files, and a template already in S3 is never overwritten.
- Every GitHub Action is pinned by commit SHA, and CI fails on any action that isn't. No workflow runs on `pull_request_target`.

## The dashboard a release ships

`selfhost/versions.json` pins the dashboard to a full commit SHA of `CoderHelm/coderhelm` (`dashboardRef`). The release builds exactly that commit and checks that the checkout is it. CI and the release refuse a branch or tag name there (`scripts/check-dashboard-ref.mjs`). So a release never ships whatever happens to be on the dashboard's `main` when it runs.

To move it forward, run `bash scripts/bump-dashboard.sh` for `main`'s current commit, or pass a SHA. It checks that the commit is on the dashboard's `main` and writes it. Then open a pull request with the change. The bump is reviewed like any other change, and is often part of the same pull request as the API change the dashboard needs.

## Rotating a maintainer key

`selfhost/maintainers.json` lists the SSH public keys allowed to sign a release tag. A maintainer's current key is on GitHub: `gh api users/<login>/ssh_signing_keys`. Installs check a new release with the keys of the release they already run, so a change reaches them one release later.

1. **Add the new key.** Put it in `selfhost/maintainers.json` next to the old one, in a reviewed pull request. Keep the old key there.
2. **Release with the old key.** Tag and publish the next release with the old key. Installs that update to it now know the new key.
3. **Remove the old key.** Sign later releases with the new key. Remove the old key in a later pull request. Installs still on a release from before step 2 have to update through the step 2 release first. Its release notes should say so.

If a key is lost or exposed, remove it in a release signed with another listed key. If no other key is listed, installs can't accept a new release on their own. Each admin then has to update once from a checkout they reviewed (`--source`), or with a new Launch Stack.

## The Launch Stack

CloudFormation can't check a signature on a template. So the template is published at one fixed CoderHelm address per release:

```text
https://coderhelm-public.s3.amazonaws.com/launch/<version>/coderhelm-launch.yaml
```

It is the same file, byte for byte, as `launch-stack.yaml` in that release's signed `SHA256SUMS`. `launch/latest.json` names the newest one and its SHA-256, and it only ever moves forward. The template's installer runs nothing from the release until it has:

1. installed a cosign pinned by its SHA-256;
2. checked the tag with the maintainer keys written into the template;
3. checked `SHA256SUMS` against its signature, with the exact identity and the tag's commit;
4. checked `coderhelm-selfhost.tgz` and `launch-bootstrap.mjs` against `SHA256SUMS`.

The installer then checks the rest of the release the same way. You can check the template yourself before you launch it (below).

## Check a release yourself

Use git 2.34 or newer, and [cosign](https://docs.sigstore.dev/cosign/system_config/installation/) 2.x or the [GitHub CLI](https://cli.github.com/).

```sh
V=v0.7.0
R=CoderHelm/coderhelm-platform

# 1. The tag is signed by a maintainer key (selfhost/maintainers.json) and is on main.
git clone --quiet --filter=tree:0 --no-checkout "https://github.com/$R.git" ch && cd ch
git show origin/main:selfhost/maintainers.json \
  | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{for(const k of JSON.parse(s).keys)console.log(`${k.login} namespaces="git" ${k.key}`)})' > ../allowed_signers
git -c gpg.ssh.allowedSignersFile=../allowed_signers verify-tag "$V"
COMMIT=$(git rev-parse "$V^{commit}")
git merge-base --is-ancestor "$COMMIT" origin/main && echo "on main: $COMMIT"
cd ..

for f in SHA256SUMS SHA256SUMS.sigstore.json coderhelm-selfhost.tgz launch-stack.yaml; do
  curl --proto =https -fsSLO "https://github.com/$R/releases/download/$V/$f"
done

# 2. The checksums were signed by CoderHelm's release workflow, on this tag, from that commit.
cosign verify-blob --bundle SHA256SUMS.sigstore.json \
  --certificate-identity "https://github.com/$R/.github/workflows/release.yml@refs/tags/$V" \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --certificate-github-workflow-repository "$R" \
  --certificate-github-workflow-ref "refs/tags/$V" \
  --certificate-github-workflow-sha "$COMMIT" \
  SHA256SUMS

# 3. The files match the checksums (on macOS: shasum -a 256 -c).
sha256sum -c --ignore-missing SHA256SUMS

# Or, instead of step 2, with the GitHub CLI: each file was built by that workflow, on that tag and commit.
gh attestation verify coderhelm-selfhost.tgz --repo "$R" \
  --signer-workflow "$R/.github/workflows/release.yml" \
  --source-ref "refs/tags/$V" --source-digest "$COMMIT" \
  --cert-oidc-issuer https://token.actions.githubusercontent.com \
  --deny-self-hosted-runners
```

To check the Launch Stack template before you launch it, compare it with the signed one:

```sh
curl --proto =https -fsSL "https://coderhelm-public.s3.amazonaws.com/launch/$V/coderhelm-launch.yaml" | sha256sum
grep ' launch-stack.yaml$' SHA256SUMS
```

The two hashes must be the same.

To check `install.sh` before running it, download it with the files above and add `install.sh` to the `for` line. Run step 3, then `sh install.sh`. The script checks the tag and the installer's signature itself before running it. Its maintainer keys are written into it.

## When a check fails

Nothing is installed or changed:

- **Update build:** the history in **Settings, then System** shows the update as failed, with the step that stopped it. Admins get an email. Automatic updates skip that release until an admin tries it again by hand.
- **Terminal:** the installer names the check that failed and exits without deploying. For example: *"The release's tag doesn't check out: v0.8.0 is not signed by a CoderHelm maintainer key"*, or *"The release's signature is not CoderHelm's release workflow for v0.8.0"*.
- **Launch Stack:** the install guide shows *"The release files did not match their signed checksums, so nothing was installed."* The stack ends in a failed state, and you can delete it.

A failure on a release that should be good means something between you and GitHub changed the files. Please report it privately (see [Security model](./security.md#reporting-a-problem)).
