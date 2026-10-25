---
title: Updates
description: See which version you run, turn on automatic updates with a maintenance window, update with one click, and roll back.
order: 9
---

## Which version you run

- **Dashboard:** your account menu shows the version. **Settings → System** (install admins only) shows the version, its commit and release date, the latest release with its notes, and the update history.
- **Terminal:**
  ```sh
  npx -y github:CoderHelm/coderhelm-platform version
  ```
  prints the installer's version, the installed release and the latest one.
- **API:** `GET /api/system/version` (install admins) returns the same as JSON.

Each deploy stamps the release version, commit, release date and a short fingerprint of your config into the services and the dashboard, so two installs, or one before and after a config change, can be told apart.

## How updates are found

The updater (a Lambda in your account) checks GitHub for releases every hour, keeping the list for an hour. When a newer release appears it emails every install admin once, and posts to your Slack or Teams webhook if you set `updates.webhookUrl`. The notice links to **Settings → System**, where the release notes are.

## Automatic updates

Turn them on in **Settings → System**, or:

```sh
npx -y github:CoderHelm/coderhelm-platform auto-update on --window "Sun 03:00" --timezone Europe/Berlin --channel stable --majors ask
```

| Setting | Values | Default |
|---|---|---|
| Auto-update | on, off | off |
| Window | a day (or `daily`) and an hour | Sun 03:00 |
| Time zone | any IANA name | UTC |
| Channel | `stable`, or `preview` for release candidates too | stable |
| Majors | `ask`: a new major version waits for an admin; `auto`: it installs too | ask |

The settings take effect at once (no deploy). The same values in `coderhelm.config.json` under `updates` are the starting point of a new install.

An update starts only when all of these hold: auto-update is on, the hour is the window's hour in your time zone, the release is on your channel, it isn't a major release while majors are `ask`, no update is running, and that version hasn't already failed (an admin can still start it by hand).

## Update now

**Settings → System → Update now** (or `coderhelm-selfhost update`) starts the same update as the automatic one, and records who started it.

## What an update does

The update runs as a CodeBuild job in your account, `coderhelm-<stage>-update`:

1. **Checks the release.** It downloads the release's `SHA256SUMS` and checks its signature with cosign: it must be signed, keyless, by `CoderHelm/coderhelm-platform`'s release workflow on a version tag. Every file is then checked against `SHA256SUMS`. A release that fails either check is refused.
2. **Checks health first.** The stacks, functions, dead-letter queue, keys and URLs. An unhealthy install is not updated.
3. **Reads the migration notes.** Every release between yours and the target is listed. One that needs action stops an automatic update; an admin does what it says, then updates by hand.
4. **Deploys.** It runs the release's CDK app through the CDK roles in your account.
5. **Checks health again.** If the new release is unhealthy, the previous release is deployed again automatically.
6. **Reports.** The history in **Settings → System** shows the version, when, who (or "auto") and the result: succeeded, failed, or rolled back with the reason. Admins get an email either way.

The job's role can only assume the CDK bootstrap roles (which do the deploying), read this install's stacks, functions, queues, secret and installer state, and report to the updater.

## Roll back

**Settings → System → Roll back** (or `coderhelm-selfhost rollback`) deploys the release before the last successful update. To pick a version: `coderhelm-selfhost rollback --to v1.3.2`.

Data written by a newer release stays. Releases only add fields to stored items, so an older release keeps reading them. CloudFormation also rolls a stack back by itself if a deploy fails partway.

## From the terminal

```sh
npx -y github:CoderHelm/coderhelm-platform update                # latest release
npx -y github:CoderHelm/coderhelm-platform update --to v1.4.0
npx -y github:CoderHelm/coderhelm-platform update --auto-rollback --require-signature
```

The terminal update runs the same steps on your machine. It shows `cdk diff` and asks before deploying unless you pass `--yes`. Signatures are checked when `cosign` or the GitHub CLI is installed; `--require-signature` refuses a release that can't be checked.

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

Every artifact also has a GitHub build provenance attestation (`gh attestation verify <file> --repo CoderHelm/coderhelm-platform`).
