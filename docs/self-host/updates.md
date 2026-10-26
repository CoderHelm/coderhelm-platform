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

Turn them on in **Settings, then System**, at install (the install guide's **More options**, or the Launch Stack's `AutoUpdate`, `UpdateWindow` and `UpdateTimezone`), or:

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

The settings take effect at once (no deploy). The same values in `coderhelm.config.json` under `updates` are the starting point of a new install.

An update starts only when all of these hold: auto-update is on, the hour is the window's hour in your time zone, the release is on your channel, it isn't a major release while majors are `ask`, no update is running, and that version hasn't already failed (an admin can still start it by hand).

## Update readiness

Before you update, **Settings, then System** shows **Update readiness**: the same checks an update makes, run now, each with its fix.

- **Blocks the update:** a check that fails (the update's health check would stop it), or an update already running. **Update now** stays off until it is fixed.
- **Read first:** a major release whose notes ask you to do something, or a release whose last try failed or rolled back (with the reason).

Each item says what is wrong and how to fix it: a command to copy, a link, or a page in CoderHelm. **Check again** reruns them. The same list is `GET /api/system/preflight`; see [Check your install](./check-your-install.md).

From the terminal, `update` runs a preflight first and stops before anything changes when:

- the config has a problem (each one named, with the setting to fix);
- there is less than about 1.5 GB free where releases are kept (`~/.coderhelm/releases`, or `CODERHELM_HOME`);
- the install isn't healthy (each failed check says how to fix it; `doctor` shows every check);
- it crosses a release marked **NEEDS ACTION** in the notes (do what it says, then run again with `--yes`).

## Update now

**Settings, then System, then Update now** (or `coderhelm-selfhost update`) starts the same update as the automatic one, and records who started it.

## What an update does

The update runs as a CodeBuild job in your account, `coderhelm-<stage>-update`:

1. **Checks the release.** It downloads the release's `SHA256SUMS` and checks its signature with cosign: it must be signed, keyless, by exactly `CoderHelm/coderhelm-platform`'s `release.yml`, on that release's own tag, and be in the Rekor transparency log. Every file is then checked against `SHA256SUMS`. A release that fails either check is refused, and so is an older release than the one you run unless it is a rollback. See [How updates are kept safe](./updates-security.md).
2. **Checks health first.** The stacks, functions, dead-letter queue, keys and URLs. An unhealthy install is not updated.
3. **Reads the migration notes.** Every release between yours and the target is listed. One that needs action stops an automatic update; an admin does what it says, then updates by hand.
4. **Deploys.** It runs the release's CDK app through the CDK roles in your account.
5. **Checks health again.** If the new release is unhealthy, the previous release is deployed again automatically.
6. **Reports.** The history in **Settings, then System** shows the version, when, who (or "auto") and the result: succeeded, failed, or rolled back with the reason. Admins get an email either way.

## When an update fails

The history entry says what happened and what to do next:

- **Rolled back:** the new release deployed but wasn't healthy, so the previous one was deployed again. Your data is untouched. Fix what **Settings, then Setup** (or `doctor`) lists, then update again. Automatic updates skip that release until an admin updates to it by hand.
- **Failed:** the update stopped before or during the deploy. Check **Setup** for what fails and how to fix it. If the install isn't working, roll back.

The terminal prints the same guidance:

```text
The update to v0.8.0 deployed, but the install wasn't healthy afterwards, so it went back to v0.7.0. Your data is untouched.
What to do:
  1. See what fails and how to fix it: npx -y github:CoderHelm/coderhelm-platform doctor
  2. If a stack failed, its Events tab in the CloudFormation console shows the first error.
  3. Then try again: npx -y github:CoderHelm/coderhelm-platform update --to v0.8.0
  Auto-update skips this release until an admin updates to it by hand.
```

The job's role can only assume the CDK bootstrap roles (which do the deploying), read this install's stacks, functions, queues, secret and installer state, and report to the updater.

## Roll back

**Settings, then System, then Roll back** (or `coderhelm-selfhost rollback`) deploys the release before the last successful update. To pick a version: `coderhelm-selfhost rollback --to v0.7.0`. Only a release this install ran before can be picked: an older release it never ran is refused, even though it is signed.

Data written by a newer release stays. Releases only add fields to stored items, so an older release keeps reading them. CloudFormation also rolls a stack back by itself if a deploy fails partway.

## From the terminal

```sh
npx -y github:CoderHelm/coderhelm-platform update                # latest release
npx -y github:CoderHelm/coderhelm-platform update --to v0.8.0
npx -y github:CoderHelm/coderhelm-platform update --auto-rollback
```

The terminal update runs the same steps on your machine. It shows `cdk diff` and asks before deploying unless you pass `--yes`. It needs `cosign` or the GitHub CLI (`gh`) to check the signature, and refuses the release without one of them. (`--require-signature` is still accepted; it is always on.)

## Another machine

The installer keeps your config and the installed version in SSM in your account (`/coderhelm-<stage>/installer/config` and `/state`, no secrets in either). To manage an install from a new machine:

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
| `install.sh`, `launch-stack.yaml` | The install script and the Launch Stack template |
| `SHA256SUMS`, `SHA256SUMS.sigstore.json` | Checksums, and their keyless signature |

Every artifact also has a GitHub build provenance attestation. To check a release yourself, see [How updates are kept safe](./updates-security.md#check-a-release-yourself).
