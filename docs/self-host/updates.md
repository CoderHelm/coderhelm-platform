---
title: Updates
description: See which version you run, turn on automatic updates with a maintenance window, update with one click, and roll back.
order: 9
---

## Which version you run

- **Dashboard:** your account menu shows the version. **Settings, then System** (install admins only) shows the version, its commit and release date, the latest release with its notes, and the update history.
- **Terminal:**
  ```sh
  npx -y github:CoderHelm/coderhelm-platform version
  ```
  prints the installer's version, the installed release and the latest one.
- **API:** `GET /api/system/version` (install admins) returns the same as JSON.

Each deploy stamps the release version, commit, release date and a short fingerprint of your config into the services and the dashboard, so two installs, or one before and after a config change, can be told apart.

## Versions

The first self-host release is **v0.7.0**. Older `v0.2.x` to `v0.6.0` tags in the repository are from before self-hosting and have no releases; every self-host release sorts above them, so the updater's version comparison always moves forward. Updates only ever go to a published GitHub release, never to a bare tag.

## How updates are found

The updater (a Lambda in your account) checks GitHub for releases every hour, keeping the list for an hour. When a newer release appears it emails every install admin once, and posts to your Slack or Teams webhook if you set `updates.webhookUrl`. The notice links to **Settings, then System**, where the release notes are.

## Automatic updates

Automatic updates are off by default. With them off, CoderHelm still prepares each new release for you to review (see [What an update does](#what-an-update-does)).

On an install made with Launch Stack, set the stack parameter **AutoUpdate** to `on`, with **UpdateDay**, **UpdateHour** and **UpdateTimezone** for the maintenance window. You can set them at launch, or later: select the stack `coderhelm-<name>`, choose **Update**, **Replace existing template** with your release's template (`https://coderhelm-public.s3.amazonaws.com/launch/<Version>/coderhelm.json`, the **Version** from the stack's **Outputs**), and change only those parameters. Don't choose **Use current template**: on an install from v0.8.4 or v0.8.5 it keeps the old values ([why](./uninstall.md#change-it-before-you-delete)). **AutoUpdate** `on` adds the auto-update role, `coderhelm-<name>-deploy-stack`, which is what lets the updater apply a release without you ([what it may do](./security.md#updates-and-the-optional-auto-update-role)). Without it, an update waits for your review even when the setting in **Settings, then System** is on.

On an install from before native releases, turn them on in **Settings, then System**, or:

```sh
npx -y github:CoderHelm/coderhelm-platform auto-update on --window "Sun 03:00" --timezone Europe/Berlin --channel stable --majors ask
```

- **Auto-update** *default off*\
  On, off.
- **Window** *default Sun 03:00*\
  A day (or `daily`) and an hour.
- **Time zone** *default UTC*\
  Any IANA name.
- **Channel** *default stable*\
  `stable`, or `preview` for release candidates too.
- **Majors** *default ask*\
  `ask`: a new major version waits for an admin; `auto`: it installs too.

The settings in **Settings, then System** take effect at once (no deploy). The launch parameters, or the same values in `coderhelm.config.json` under `updates`, are the starting point of a new install.

An automatic update starts only when all of these hold: auto-update is on, the hour is the window's hour in your time zone, the release is on your channel, it isn't a major release while majors are `ask`, no update is running, that version hasn't already failed (an admin can still start it by hand), and every model the release calls answers a test call through your provider.

## The model check

Each release lists the models it calls (`models.json`, made from the code and covered by the signed `SHA256SUMS`). The updater reads it only after a read-only build has checked the release's signature and the file's checksum; until then the update waits. Before an update, automatic or **Update now**, the updater asks the worker to make one test call to each, through your current provider (Bedrock or Anthropic keys). If one is refused, the update waits: **Settings, System** and **Setup** say **Update to vX needs your action** with the model, the fix and the AWS console link, and admins get one email. The check runs again daily and the update applies by itself once it passes (with auto-update on). **Check again** reruns it now. CoderHelm never accepts model terms or submits forms for you; see [Model providers](./model-providers.md#bedrock-model-access).

An install on Anthropic API keys (the launch parameter **ModelProvider** `anthropic`) never needs Bedrock: the worker makes the test calls with a team's key, never through Bedrock. Each team adds its own key, so until one exists there is nothing to check with: the check notes it, and nothing holds the update. Once a team adds its key, a model refused with that key holds the update like any other.

`update` from your machine runs the same check after the release's signature passes, through your install's worker, before anything is deployed. A refused model stops it with the fix and the console link. `--skip-model-check` goes on only when the refused model is one the release doesn't call; a model it calls can't be skipped. A rollback skips the check (you go back to models you already used).

## Update readiness

Before you update, **Settings, then System** shows **Update readiness**: the same checks an update makes, run now, each with its fix.

- **Blocks the update:** a check that fails (the update's health check would stop it), a model the release calls that was refused, or an update already running. **Update now** stays off until it is fixed.
- **Read first:** a major release whose notes ask you to do something, or a release whose last try failed or rolled back (with the reason).

Each item says what is wrong and how to fix it: a command to copy, a link, or a page in CoderHelm. **Check again** reruns them. The same list is `GET /api/system/preflight`; see [Check your install](./check-your-install.md).

From the terminal, `update` runs a preflight first and stops before anything changes when:

- the config has a problem (each one named, with the setting to fix);
- there is less than about 1.5 GB free where releases are kept (`~/.coderhelm/releases`, or `CODERHELM_HOME`);
- the install isn't healthy (each failed check says how to fix it; `doctor` shows every check);
- it crosses a release marked **NEEDS ACTION** in the notes (do what it says, then run again with `--yes`).

## Update now

**Settings, then System, then Update now** starts the same update as the automatic one, and records who started it. With auto-update off, it prepares the change set for you to review.

## What an update does

On an install made with Launch Stack, an update is a stack update of `coderhelm-<name>` to the new release's `coderhelm.json`, prepared as a CloudFormation change set. Creating a change set changes nothing.

1. **Checks the release.** A read-only build in your account checks the release's tag: it must be on a commit on `main`, and be signed with a maintainer key pinned in the release your install runs or attested by CoderHelm's automatic release workflow on `main` for that tag and commit. It then downloads the release's `SHA256SUMS` and checks its signature with cosign. It must be signed, keyless, by exactly `CoderHelm/coderhelm-platform`'s `release.yml`, on that release's own tag and from the tag's commit, and be in the Rekor transparency log. It reports the sha256 of `coderhelm.json` from the signed `SHA256SUMS`. A release that fails is refused. See [How updates are kept safe](./updates-security.md).
2. **Checks the models** the release calls ([the model check](#the-model-check)).
3. **Prepares the change set.** The updater downloads `coderhelm.json` from `https://coderhelm-public.s3.amazonaws.com/launch/<version>/`, where release files are written once and never replaced, and proposes it only if its sha256 matches the signed one. First the install helper copies the release's part templates into the install's own release bucket, each checked against the sha256 that verified `coderhelm.json` lists, so CloudFormation can read them while it prepares the parts' own change sets. Every parameter keeps its current value. After CloudFormation reads the template, the updater compares what it read with the verified one, and runs nothing if they differ.
4. **Waits for you, or applies it.**
   - **Auto-update off (the default):** **Settings, then System, then Update** shows the change set's summary (how many resources are added, changed and removed, which ones are replaced, and which parts it touches) and a link to it in the CloudFormation console. Admins get an email with the same link. Review it there and choose **Execute**; it runs with your own permissions.
   - **Auto-update on:** the updater executes it in your maintenance window, through the auto-update role.
5. **Follows it.** Every 5 minutes the updater reads the change set and the stack until the update ends. When the stack update starts, the install's own copy step checks every file of the release against the sha256 that `coderhelm.json` lists before any part changes. If a part fails, CloudFormation rolls the whole stack back to the previous release by itself.
6. **Reports.** The history in **Settings, then System** shows the version, when, who (or "auto") and the result: succeeded, failed, or rolled back, with CloudFormation's own reason for the first resource that failed. Admins get an email either way.

A change set that waits too long can go stale (the stack changed since it was made); the history then says so, and **Update now** prepares a new one.

### Installs from before native releases

An install from before native releases updates with a CodeBuild job in your account, `coderhelm-<stage>-update`. It runs the same release checks, checks health first, reads the migration notes (a release that needs action stops an automatic update), deploys the release's CDK app, checks health again and deploys the previous release again if the new one is unhealthy. On an install an admin hardened, the job's role holds no deploy rights until the updater gives this one build a deploy window, and the build deploys only through the install's own deploy role ([what that means](./updates-security.md#what-an-update-may-do-in-your-account)). Otherwise the job's role assumes the CDK bootstrap roles, as it always has. To move such an install to one stack, see [Migrate to one stack](./migrate.md).

## When an update fails

The history entry says what happened and what to do next:

- **Rolled back:** the update didn't finish, so CloudFormation (or, on an install from before, the update job) put the previous release back. Your data is untouched. Fix what the reason and **Settings, then Setup** (or `doctor`) list, then update again. Automatic updates skip that release until an admin updates to it by hand.
- **Failed:** the update stopped before or during the deploy. Check **Setup** for what fails and how to fix it. If the install isn't working, roll back.

On an install made with Launch Stack, the stack's **Events** tab in the CloudFormation console shows every step and the first error.

The terminal (installs from before native releases) prints the same guidance:

```text
The update to v0.8.0 deployed, but the install wasn't healthy afterwards, so it went back to v0.7.0. Your data is untouched.
What to do:
  1. See what fails and how to fix it: npx -y github:CoderHelm/coderhelm-platform doctor
  2. If a stack failed, its Events tab in the CloudFormation console shows the first error.
  3. Then try again: npx -y github:CoderHelm/coderhelm-platform update --to v0.8.0
  Auto-update skips this release until an admin updates to it by hand.
```

Rolling back from a hardened install to a release from before hardening needs an admin ([why](./security.md#rolling-back-past-hardening)).

## Roll back

**Settings, then System, then Roll back** (or `coderhelm-selfhost rollback`) deploys the release before the last successful update. To pick a version: `coderhelm-selfhost rollback --to v0.7.0`. Only a release this install ran before can be picked: an older release it never ran is refused, even though it is signed.

Data written by a newer release stays. Releases only add fields to stored items, so an older release keeps reading them. CloudFormation also rolls a stack back by itself if a deploy fails partway.

## From the terminal

For installs from before native releases (an install made with Launch Stack updates as a stack update, above):

```sh
npx -y github:CoderHelm/coderhelm-platform update                # latest release
npx -y github:CoderHelm/coderhelm-platform update --to v0.8.0
npx -y github:CoderHelm/coderhelm-platform update --auto-rollback
```

The terminal update runs the same steps on your machine. It shows `cdk diff` and asks before deploying unless you pass `--yes`. It needs `cosign` or the GitHub CLI (`gh`) to check the signature, and refuses the release without one of them. (`--require-signature` is still accepted; it is always on.)

## Another machine

For installs from before native releases: the installer keeps your config and the installed version in SSM in your account (`/coderhelm-<stage>/installer/config` and `/state`, no secrets in either). To manage an install from a new machine:

```sh
npx -y github:CoderHelm/coderhelm-platform pull --stage prod --region us-east-1
```

## Release files

Each release (a `vX.Y.Z` tag) has, on its GitHub release page:

| File | What |
|---|---|
| `gateway.zip`, `worker.zip` | The Rust Lambdas, built for ARM64 |
| `mcp-proxy.zip` | The MCP proxy Lambda |
| `dashboard.tar.gz` | The dashboard (it reads its API location at runtime) |
| `source.tar.gz` | The repository at the tag, with `selfhost/release.json` (the version stamp) |
| `coderhelm-selfhost.tgz` | The installer |
| `install.sh` | The install script |
| `coderhelm.json` | The Launch Stack template: the parent stack, with the sha256 of every other file below |
| `coderhelm-native.tar.gz` | The nested stack templates (`stacks/<Part>.json`) and the code bundles (`assets/<hash>.zip`) that `coderhelm.json` pins |
| `models.json` | The models the release calls |
| `coderhelm-log-reader.yaml`, `coderhelm-log-reader-update.yaml` | The log reader role template, and its update |
| `SHA256SUMS`, `SHA256SUMS.sigstore.json` | Checksums, and their keyless signature |

The Launch Stack files are also in S3, written once by version: `https://coderhelm-public.s3.amazonaws.com/launch/<version>/coderhelm.json`, with `stacks/<Part>.json` and `assets/<hash>.zip` next to it. `https://coderhelm-public.s3.amazonaws.com/launch/latest.json` names the newest stable release, its `templateUrl` and the `templateSha256` of its `coderhelm.json`.

Every artifact also has a GitHub build provenance attestation. To check a release yourself, see [Verify a release before launching](./updates-security.md#verify-a-release-before-launching) and [How updates are kept safe](./updates-security.md#check-a-release-yourself).
