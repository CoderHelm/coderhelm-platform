---
title: How updates are kept safe
description: How CoderHelm proves a release is really CoderHelm's, what installs and updates check, what GitHub and AWS settings add, and how to verify a release yourself.
order: 9.5
---

Nothing gets onto your install just because it looks like a CoderHelm release. Before an install or update uses anything from a release, it checks two things:

1. **The release's tag is on `main` and vouched for.** Either a maintainer signed it, or CoderHelm's automatic release workflow on `main` attested it for exactly that tag and commit.
2. **CoderHelm's release workflow built and signed the release from that same commit.**

If any check fails, nothing is installed.

This page says what each protection is and what enforces it: your install's own code, AWS, or settings on GitHub.

## What installs and updates check

The release check build that runs before every update (`coderhelm-<stage>-release-check`), the command line installer, and the update build of an install from before native releases (`coderhelm-<stage>-update`) make the same checks. They run before anything from the release is used. For Launch Stack, you check the template and the install checks everything else against it: see [The Launch Stack](#the-launch-stack).

1. **The tag.** `vX.Y.Z` is fetched from `https://github.com/CoderHelm/coderhelm-platform.git` with git. Several things must hold:
   - It is an annotated tag. A lightweight tag is refused.
   - The tag object's own name is exactly `vX.Y.Z`. An old signed tag can't be moved under a new name.
   - It points to a commit that is on `main`.
   - It is vouched for in one of two ways:
     - **A release cut by hand:** it is signed with one of the SSH keys in [`selfhost/maintainers.json`](#rotating-a-maintainer-key).
     - **An automatic release:** it has no SSH signature, and its message carries a *release attestation*. That is a keyless Sigstore signature of the four lines `coderhelm-release-tag v1`, `repository CoderHelm/coderhelm-platform`, `tag vX.Y.Z` and `commit <the tag's commit>`. Its certificate must name exactly `https://github.com/CoderHelm/coderhelm-platform/.github/workflows/auto-release.yml@refs/heads/main`, be issued by `https://token.actions.githubusercontent.com`, name the tag's commit as the one the workflow ran on, and be in the Rekor transparency log. With only the GitHub CLI, `gh attestation verify` checks GitHub's attestation of the same four lines, from the same workflow, ref and commit.

     A tag with an SSH signature is only ever checked as a maintainer's. An attestation from another workflow, a branch other than `main`, a tag ref, another commit or another tag is refused.

   Git runs without your own git configuration. A local key list, GPG keyring or signing program can't make a signature pass.
2. **The signature.** `SHA256SUMS` has a keyless Sigstore signature, `SHA256SUMS.sigstore.json`. Its certificate must name exactly
   `https://github.com/CoderHelm/coderhelm-platform/.github/workflows/release.yml@refs/tags/<the version>`.
   It must also:
   - be issued by `https://token.actions.githubusercontent.com`;
   - name the tag's commit as the commit the workflow ran on (Fulcio's source repository digest, build signer digest and workflow SHA);
   - be in the Rekor transparency log.

   So the workflow that signed is the `release.yml` of the tag's commit on `main`. Several kinds of signature are refused:
   - one from another repository, another workflow, a branch or another tag;
   - one made from another commit, such as a `release.yml` changed and pushed under a new tag;
   - a bundle with no Rekor entry;
   - a missing signature.
3. **The files.** Every file must match its line in `SHA256SUMS`. That covers the Lambdas, the dashboard, the source, the installer, `install.sh`, the Launch Stack template `coderhelm.json`, and `coderhelm-native.tar.gz` (the nested templates and code bundles that `coderhelm.json` pins). A file changed after signing is refused before anything is unpacked.
4. **Where files come from.** Files come only from `https://github.com/CoderHelm/coderhelm-platform/releases/download/<version>/` over HTTPS, including after GitHub's redirects. Any other host, plain HTTP, or a version that isn't `vX.Y.Z` is refused.
5. **No going back.** An update only goes to a newer release than the one running. A rollback only goes to a release this install has run before. An admin starts it from **Settings, then System**, or with `coderhelm-selfhost rollback`. An older signed release that this install never ran can't be pushed onto it.
6. **A fresh release list.** The updater reads the release list only from `api.github.com` for this repository. If the list stops showing the newest version it saw before, **Settings, then System** marks the list as stale. It does the same when the list hasn't been read for over 3 days. A stale copy can hold an install back, but it can never make it go back. The list only decides what to offer. What is trusted is decided by the checks above, in the build.

**Whose keys.** An update build checks a new release against the maintainer keys of the release your install already runs. They are written into the build when that release was deployed. A release can't bring its own keys to vouch for itself. The installer checks with the keys in its own package.

The installer verifies the signature with `cosign` when it is installed, or else with the GitHub CLI. With the GitHub CLI it runs `gh attestation verify`, checking the same repository, workflow, tag and commit. It also needs `git` 2.34 or newer and `ssh-keygen` for the tag. An automatic release's tag is checked with the same `cosign` or GitHub CLI. If a tool is missing, it stops and asks you to install it. No flag skips any check. To run code that isn't a signed release, deploy from a checkout you reviewed (`--source`).

## Who can publish a release

A release is a `vX.Y.Z` tag on `CoderHelm/coderhelm-platform`, built by `.github/workflows/release.yml`. A tag comes one of two ways.

**Automatic (the usual way).** When the hosted deploy of `main` succeeds, `deploy.yml` starts `.github/workflows/auto-release.yml` on `main`. It:

1. checks it runs on `refs/heads/main`, on the commit that was just deployed, and that the deploy of that commit succeeded;
2. plans the release (`.github/scripts/auto_release.py`). It skips when nothing that ships in a self-host release changed since the last release (`services/`, `lambda/`, `infra/`, `installer/`, `selfhost/`, the dashboard pin check and `release.yml`). The version is the last release plus a patch, or plus a minor when a merged pull request is labelled `enhancement` or `feature` or titled `feat:`. It is never below the version in `installer/package.json`, which is how a reviewed pull request asks for a minor or major release. The notes list the merged pull requests;
3. signs the four lines above with Sigstore keyless signing. No key is stored anywhere: GitHub's OIDC token names the workflow file, the ref and the commit, and the certificate carries exactly those. GitHub also records an attestation of the same lines;
4. creates the annotated tag with that signature in its message. GitHub refuses to create a tag that exists, so a tag is never re-cut or moved;
5. starts `release.yml` on the tag. That builds and signs the release exactly as for a tag cut by hand, with no approval step;
6. downloads the published release and checks it as an install does. Only then does it upload the Launch Stack templates and move `launch/latest.json`. It then writes the release notes on the GitHub release, where installs read them.

**By hand.** A maintainer signs a tag with their SSH key and pushes it. `release.yml` runs, and a maintainer approves the `release` environment before it publishes.

### What an attacker would need

For installs and updates to run their code:

- **Automatic path:** their code on `main`. `main` takes changes only through a pull request with an approving review and passing checks, and signed commits. A reviewer must approve the malicious change. The attestation names the commit that `auto-release.yml` ran on, and that commit's `auto-release.yml` and `release.yml` are the ones that signed. So a changed copy of either file on a branch, a fork, a tag or another workflow gets a certificate that installs refuse. Taking over GitHub Actions' OIDC issuer or Sigstore's certificate authority (Fulcio) would also do it. Rekor would then show the forged certificate publicly.
- **Path by hand:** a maintainer's SSH signing key, and their code on `main`.

For the install guide to link to their template (`launch/latest.json`, the templates under `launch/<version>/`):

- **Automatic path:** a run of `auto-release.yml` from `main` in the `release-auto` environment. The AWS roles that write those objects trust only that file, on `refs/heads/main`, in that environment, and the environment admits only `main`. So again: a reviewed, merged pull request.
- **Path by hand:** a maintainer's approval of the `release` environment.

Someone with write access to the repository, but no review, could at most create a `v*` tag from a workflow on a branch, and run `release.yml` on it. That makes a GitHub release with a signature naming that tag. No install accepts it: the tag isn't on `main`, or has neither a maintainer signature nor an attestation from `auto-release.yml` on `main`. It gets no approval-free access to the AWS roles. `auto-release.yml` ignores tags that are not on `main`. Anyone with write access can already create a GitHub release by hand, so this adds nothing.

The protections come from three places.

### In every install's code (always on)

The checks above. Someone who could push a tag and change `release.yml` still can't make a release that installs and updates accept. They would need a maintainer's signing key, or a tag attested by `auto-release.yml` running on `main`. Either way the signature names a commit on `main` whose workflows signed.

### In AWS (the hosted account's IAM)

The release workflows upload the Launch Stack and AWS role templates to the public bucket `coderhelm-public`. They use two roles (`infra/lib/worker-stack.ts`). Each trusts a GitHub token only when its claims match one of these two sets.

A release cut by hand (`release.yml`):

- `sub` is `repo:CoderHelm/coderhelm-platform:environment:release`;
- `job_workflow_ref` matches `CoderHelm/coderhelm-platform/.github/workflows/release.yml@refs/tags/v*`;
- `ref` matches `refs/tags/v*`. AWS has no `ref_type` key for GitHub tokens, and only a tag's ref matches this.

An automatic release (`auto-release.yml`):

- `sub` is `repo:CoderHelm/coderhelm-platform:environment:release-auto`;
- `job_workflow_ref` is exactly `CoderHelm/coderhelm-platform/.github/workflows/auto-release.yml@refs/heads/main`;
- `ref` is exactly `refs/heads/main`.

Another workflow that names either environment gets neither role. What each role may do:

- **`release-publisher`** may only create objects under `launch/v*/`. Its policy requires the `If-None-Match` header (S3 conditional writes), so it can't overwrite a template already there.
- **`release-latest`** may only write `launch/latest.json`, and only as a conditional write: a create, or a replace of the exact copy it read. Both workflows use it only after they have downloaded the published release and checked it as an install would.

### In GitHub's settings (applied with `scripts/release-protection.sh`)

These settings make a bad tag hard to create in the first place. They are not stored in the code. An organization owner applies them by running `scripts/release-protection.sh`. Until then they are not in force. `bash scripts/release-protection.sh --check` prints what is applied and what isn't (read only).

- **`main`, on both `CoderHelm/coderhelm-platform` and `CoderHelm/coderhelm`.** Changes go in only through a pull request with an approving review and passing checks. Commits must be signed, and force pushes and deletion are blocked. Admins may merge their own pull request after the checks pass, but never push to `main` directly. `--no-admin-merge-bypass` removes that too.
- **`v*` tags.** Their commits must be signed, and nobody can update, move or delete a tag. There is no admin bypass unless you pass `--allow-admin-tag-bypass`. Who can create a `v*` tag is not limited. `auto-release.yml` creates its tag with the workflow token, and GitHub refuses GitHub Actions as an exception to an organization repository's tag creation rule. So the checks above are what stop a tag made any other way: an install accepts only a tag signed by a maintainer key or attested by `auto-release.yml` on `main`.
- **The `release` environment.** A listed reviewer approves every publish of a tag cut by hand. Only `v*` tags may deploy to it, and admins can't skip the approval. So a pushed tag alone publishes nothing.
- **The `release-auto` environment.** No reviewer, and only the `main` branch may deploy to it. `auto-release.yml` publishes the templates and `latest.json` from it.
- **Immutable releases.** A published release's files can't be replaced or added to. A re-run of the release workflow on a published release checks its `SHA256SUMS` is this build's and uploads nothing.
- **Actions.** Actions must be pinned by commit SHA, the default token is read-only, and workflows from outside contributors wait for approval.

### In the release workflow

These run inside `release.yml`. They catch mistakes early, but by themselves they would not stop a changed copy of the file. The install checks do that.

- The tag must be on `main`, be the commit the run is on, and be signed with a key in `main`'s `selfhost/maintainers.json` or attested by `auto-release.yml` on `main` for that tag and commit.
- The build job runs with a read-only token, no secrets and no cloud credentials. Only the publish job can write the release. For a tag cut by hand it runs in the `release` environment and assumes the AWS roles. For an automatic tag it does neither, and `auto-release.yml` publishes the templates from `main`.
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

CloudFormation can't check a signature on a template. So the release's parent template is published at one fixed CoderHelm address per release, written once and never replaced:

```text
https://coderhelm-public.s3.amazonaws.com/launch/<version>/coderhelm.json
```

It is the same file, byte for byte, as `coderhelm.json` in that release's signed `SHA256SUMS`; the release workflow checks that before `launch/latest.json` moves. `launch/latest.json` names the newest stable release, its `templateUrl` and the `templateSha256` of its `coderhelm.json`, and it only ever moves forward.

`coderhelm.json` pins everything else. It lists every nested template (`stacks/<Part>.json`) and every code bundle (`assets/<hash>.zip`) with its sha256, and the install helper's code is written inside it. When the stack is created or updated, the install's copy step downloads each of those files from `launch/<version>/`, checks it against the sha256 in `coderhelm.json`, and refuses any file that doesn't match before any part of CoderHelm is created or changed. The nested stacks and functions read only the install's own checked copies. So once `coderhelm.json` checks out, so does everything the stack runs.

## Verify a release before launching

Check `coderhelm.json` before you launch it, or before you execute an update's change set yourself. Use [cosign](https://docs.sigstore.dev/cosign/system_config/installation/) 2.x.

```sh
V=v0.9.0
R=CoderHelm/coderhelm-platform
COMMIT=$(git ls-remote "https://github.com/$R.git" "refs/tags/$V^{}" | cut -f1)

for f in SHA256SUMS SHA256SUMS.sigstore.json; do
  curl --proto =https -fsSLO "https://github.com/$R/releases/download/$V/$f"
done
curl --proto =https -fsSLO "https://coderhelm-public.s3.amazonaws.com/launch/$V/coderhelm.json"

# 1. The checksums were signed by CoderHelm's release workflow, on this tag, from its commit.
cosign verify-blob --bundle SHA256SUMS.sigstore.json \
  --certificate-identity "https://github.com/$R/.github/workflows/release.yml@refs/tags/$V" \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --certificate-github-workflow-repository "$R" \
  --certificate-github-workflow-ref "refs/tags/$V" \
  --certificate-github-workflow-sha "$COMMIT" \
  SHA256SUMS

# 2. The template is the signed one (on macOS: shasum -a 256 -c -).
grep ' coderhelm.json$' SHA256SUMS | sha256sum -c -

# 3. For the newest release: the same sha256 as latest.json names.
curl --proto =https -fsSL https://coderhelm-public.s3.amazonaws.com/launch/latest.json
sha256sum coderhelm.json
```

Step 2 must print `coderhelm.json: OK`, and for the newest release the sha256 in step 3 must equal `templateSha256`. To check that the tag itself is on `main` and vouched for, run step 1 of [Check a release yourself](#check-a-release-yourself) too.

You don't need to check the other files: `coderhelm.json` pins every one of them by sha256, and the install refuses any that doesn't match.

## What an update may do in your account

Checking the release decides *what* runs. What it may then do in your account is limited separately.

On an install made with Launch Stack, an update is a stack update to the new release's `coderhelm.json`, prepared as a change set:

- **The updater can propose, not choose the code.** It may create a change set only on the install's own stack, and only with a template from `https://coderhelm-public.s3.amazonaws.com/launch/v*.*.*/coderhelm.json`. It proposes one only after the release check verified the release and reported the template's sha256, and only when the template it downloads matches it.
- **Auto-update off (the default):** there is no deploy role. You review the change set in the CloudFormation console and execute it with your own permissions.
- **Auto-update on:** the stack adds a CloudFormation service role, `coderhelm-<name>-deploy-stack`, and the updater executes the change set through it in your maintenance window. The updater may pass that role only to CloudFormation. The role can change only CoderHelm's resources.
- **Always:** every CoderHelm role, including the auto-update role, carries the install's permissions boundary. It refuses IAM users and keys, IAM outside `coderhelm-<name>-*`, roles without the boundary, Organizations, account settings, CloudTrail, GuardDuty, and assuming the CDK bootstrap's or your roles.

So even a release that passed every check above can't create an admin role or reach outside CoderHelm's resources. [What an install can do in your account](./security.md#what-an-install-can-do-in-your-account) lists exactly what the updater, the auto-update role and the install helper can and can't do, what that still leaves open, and the read-only check you can run.

An install from before native releases updates with a build. If an admin hardened it, the build holds no deploy rights between updates and gets a *deploy window* from the updater only while it runs, bound to its session and ending after 100 minutes; otherwise it updates through the CDK bootstrap roles. [Migrate to one stack](./migrate.md) moves it to the model above.

## Check a release yourself

Use git 2.34 or newer, and [cosign](https://docs.sigstore.dev/cosign/system_config/installation/) 2.x or the [GitHub CLI](https://cli.github.com/).

```sh
V=v0.7.0
R=CoderHelm/coderhelm-platform

# 1. The tag is on main, and signed by a maintainer key (selfhost/maintainers.json)
#    or attested by auto-release.yml on main.
git clone --quiet --filter=tree:0 --no-checkout "https://github.com/$R.git" ch && cd ch
COMMIT=$(git rev-parse "$V^{commit}")
git merge-base --is-ancestor "$COMMIT" origin/main && echo "on main: $COMMIT"
if git cat-file tag "$V" | grep -qxF -e '-----BEGIN SSH SIGNATURE-----'; then
  # Cut by hand.
  git show origin/main:selfhost/maintainers.json \
    | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{for(const k of JSON.parse(s).keys)console.log(`${k.login} namespaces="git" ${k.key}`)})' > ../allowed_signers
  git -c gpg.ssh.allowedSignersFile=../allowed_signers verify-tag "$V"
else
  # Automatic: the release attestation in the tag message.
  git cat-file tag "$V" | sed -n '/^-----BEGIN CODERHELM RELEASE ATTESTATION-----$/,/^-----END CODERHELM RELEASE ATTESTATION-----$/p' \
    | sed '1d;$d' | base64 -d > ../tag.sigstore.json
  printf 'coderhelm-release-tag v1\nrepository %s\ntag %s\ncommit %s\n' "$R" "$V" "$COMMIT" > ../statement
  cosign verify-blob --bundle ../tag.sigstore.json \
    --certificate-identity "https://github.com/$R/.github/workflows/auto-release.yml@refs/heads/main" \
    --certificate-oidc-issuer https://token.actions.githubusercontent.com \
    --certificate-github-workflow-repository "$R" \
    --certificate-github-workflow-ref refs/heads/main \
    --certificate-github-workflow-sha "$COMMIT" \
    ../statement
fi
cd ..

for f in SHA256SUMS SHA256SUMS.sigstore.json coderhelm-selfhost.tgz coderhelm.json; do
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

To check the Launch Stack template against the copy in S3, see [Verify a release before launching](#verify-a-release-before-launching).

To check `install.sh` before running it, download it with the files above and add `install.sh` to the `for` line. Run step 3, then `sh install.sh`. The script checks the tag and the installer's signature itself before running it. Its maintainer keys are written into it.

## When a check fails

Nothing is installed or changed:

- **Update:** the history in **Settings, then System** shows the update as failed, with the step that stopped it. No change set is proposed. Admins get an email. Automatic updates skip that release until an admin tries it again by hand.
- **Terminal:** the installer names the check that failed and exits without deploying. For example: *"The release's tag doesn't check out: v0.8.0 is not signed by a CoderHelm maintainer key"*, *"v0.8.0's release attestation is not from CoderHelm/coderhelm-platform/.github/workflows/auto-release.yml on main"*, or *"The release's signature is not CoderHelm's release workflow for v0.8.0"*.
- **Launch Stack:** a file that doesn't match the sha256 in `coderhelm.json` stops the copy step, which names the file and both sha256s in the stack's events. A first launch rolls back and deletes what it created; an update rolls back to the release that was running. The install guide shows CloudFormation's reason.

A failure on a release that should be good means something between you and GitHub changed the files. Please report it privately (see [Security model](./security.md#reporting-a-problem)).
