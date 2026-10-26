---
title: How updates are kept safe
description: How CoderHelm proves a release is really CoderHelm's, what installs and updates check, what happens when a check fails, and how to verify a release yourself.
order: 9.5
---

Nothing gets onto your install just because it looks like a CoderHelm release. Every install and update checks a release's signature. The signature must have been made by CoderHelm's own release workflow, on that release's own tag. If any check fails, nothing is installed.

## Who can publish a release

A release is a `vX.Y.Z` tag on `CoderHelm/coderhelm-platform`, built by `.github/workflows/release.yml`.

- **The tag.** It must be an annotated tag, signed by a maintainer, on a commit that is already on `main`. Code reaches `main` only through a reviewed pull request with signed commits and passing checks. Only repository admins can create `v*` tags. Nobody can move or delete one.
- **The build.** It runs with a read-only token, no secrets and no cloud credentials.
- **Publishing.** A separate job signs and publishes the release. It runs in the `release` environment, so a maintainer has to approve it. A pushed tag alone publishes nothing. Only that job can write the GitHub release or upload the Launch Stack template to S3. The AWS role it uses trusts only that environment.
- **No replacing.** A published release's files are never replaced. Releases are immutable on GitHub, and a template already in S3 is never overwritten.
- **Third-party code.** Every GitHub Action is pinned by commit SHA, and CI fails on any action that isn't. No workflow runs on `pull_request_target`. A pull request from outside gets a read-only token and no secrets.

## What installs and updates check

The installer, the update build (`coderhelm-<stage>-update`) and the Launch Stack's installer make the same checks before they use anything from a release:

1. **The signature.** `SHA256SUMS` has a keyless Sigstore signature, `SHA256SUMS.sigstore.json`. Its certificate must name exactly
   `https://github.com/CoderHelm/coderhelm-platform/.github/workflows/release.yml@refs/tags/<the version>`.
   It must also be issued by `https://token.actions.githubusercontent.com` and be in the Rekor transparency log. Several kinds of signature are refused:
   - one from another repository, another workflow or a branch;
   - one made for a different tag, so an old release's signature can't stand in for a new one;
   - a bundle with no Rekor entry;
   - a missing signature.
2. **The files.** Every file must match its line in `SHA256SUMS`. That covers the Lambdas, the dashboard, the source, the installer, `install.sh`, `launch-stack.yaml` and `launch-bootstrap.mjs`. A file changed after signing is refused before anything is unpacked.
3. **Where files come from.** Files come only from `https://github.com/CoderHelm/coderhelm-platform/releases/download/<version>/` over HTTPS, including after GitHub's redirects. Any other host, plain HTTP, or a version that isn't `vX.Y.Z` is refused.
4. **No going back.** An update only goes to a newer release than the one running. A rollback only goes to a release this install has run before. An admin starts it from **Settings, then System**, or with `coderhelm-selfhost rollback`. An older signed release that this install never ran can't be pushed onto it.
5. **A fresh release list.** The updater reads the release list only from `api.github.com` for this repository. If the list stops showing the newest version it saw before, **Settings, then System** marks the list as stale. It does the same when the list hasn't been read for over 3 days. A stale copy can hold an install back, but it can never make it go back.

The installer verifies with `cosign` when it is installed, or else with the GitHub CLI (`gh attestation verify`, checking the same repository, workflow and tag). If neither tool is installed, it stops and asks you to install one. No flag skips the check. To run code that isn't a signed release, deploy from a checkout you reviewed (`--source`).

## The Launch Stack

CloudFormation can't check a signature on a template. So the template is published at one fixed CoderHelm address per release:

```text
https://coderhelm-public.s3.amazonaws.com/launch/<version>/coderhelm-launch.yaml
```

It is the same file, byte for byte, as `launch-stack.yaml` in that release's signed `SHA256SUMS`. `launch/latest.json` names the newest one and its SHA-256, and it only ever moves forward. The template's installer runs nothing from the release until it has done three things:

1. installed a cosign pinned by its SHA-256;
2. checked `SHA256SUMS` against its signature, with the exact identity above;
3. checked `coderhelm-selfhost.tgz` and `launch-bootstrap.mjs` against `SHA256SUMS`.

The installer then checks the rest of the release the same way. You can check the template yourself before you launch it (below).

## Check a release yourself

Use [cosign](https://docs.sigstore.dev/cosign/system_config/installation/) 2.x or the [GitHub CLI](https://cli.github.com/).

```sh
V=v0.7.0
R=CoderHelm/coderhelm-platform
for f in SHA256SUMS SHA256SUMS.sigstore.json coderhelm-selfhost.tgz launch-stack.yaml; do
  curl --proto =https -fsSLO "https://github.com/$R/releases/download/$V/$f"
done

# 1. The checksums were signed by CoderHelm's release workflow, on this tag.
cosign verify-blob --bundle SHA256SUMS.sigstore.json \
  --certificate-identity "https://github.com/$R/.github/workflows/release.yml@refs/tags/$V" \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --certificate-github-workflow-repository "$R" \
  --certificate-github-workflow-ref "refs/tags/$V" \
  SHA256SUMS

# 2. The files match the checksums (on macOS: shasum -a 256 -c).
sha256sum -c --ignore-missing SHA256SUMS

# Or, with the GitHub CLI: each file was built by that workflow, on that tag.
gh attestation verify coderhelm-selfhost.tgz --repo "$R" \
  --signer-workflow "$R/.github/workflows/release.yml" \
  --source-ref "refs/tags/$V" \
  --cert-oidc-issuer https://token.actions.githubusercontent.com \
  --deny-self-hosted-runners
```

To check the Launch Stack template before you launch it, compare it with the signed one:

```sh
curl --proto =https -fsSL "https://coderhelm-public.s3.amazonaws.com/launch/$V/coderhelm-launch.yaml" | sha256sum
grep ' launch-stack.yaml$' SHA256SUMS
```

The two hashes must be the same.

To check `install.sh` before running it, download it with the files above and add `install.sh` to the `for` line. Run step 2, then `sh install.sh`. The script itself checks the installer's signature before running it.

## When a check fails

Nothing is installed or changed:

- **Update build:** the history in **Settings, then System** shows the update as failed, with the step that stopped it. Admins get an email. Automatic updates skip that release until an admin tries it again by hand.
- **Terminal:** the installer names the check that failed, for example *"The release's signature is not CoderHelm's release workflow for v0.8.0"*, and exits without deploying.
- **Launch Stack:** the install guide shows *"The release files did not match their signed checksums, so nothing was installed."* The stack ends in a failed state, and you can delete it.

A failure on a release that should be good means something between you and GitHub changed the files. Please report it privately (see [Security model](./security.md#reporting-a-problem)).
