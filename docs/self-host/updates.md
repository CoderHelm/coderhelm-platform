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

The updater (a Lambda in your account) checks GitHub for releases every hour. Opening **Settings, then System** reads the list again when it is more than two minutes old, and **Check for updates** in the Version section reads it right away; at most one read every 30 seconds per install, each one conditional (an unchanged list costs no GitHub API quota). Pressing **Update** reads the list first too, so it always installs the newest release at that moment. When `launch/latest.json` names a release that GitHub's list doesn't show yet, no older release is offered meanwhile.

When a newer release appears it emails every install admin once, and posts to your Slack or Teams webhook if you set `updates.webhookUrl`. The notice links to **Settings, then System**, where the release notes are.

## Automatic updates

Automatic updates are off by default. With them off, nothing installs by itself: an admin presses **Update** in **Settings, then System** and the update applies (see [What an update does](#what-an-update-does)).

Turn them on in **Settings, then System, then Automatic updates**, with the maintenance window. On an install made with Launch Stack, the stack parameters **AutoUpdate**, **UpdateDay**, **UpdateHour** and **UpdateTimezone** are only the starting values of those settings: change them in **Settings, then System** afterwards, never in the stack. Every install made with Launch Stack has the deploy role, `coderhelm-<name>-deploy-stack`, that applies an update, whether you press **Update** or the maintenance window starts it ([what it may do](./security.md#updates-and-the-deploy-role)).

On an install from before native releases, turn them on in **Settings, then System** too, or:

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

Each item says what is wrong and how to fix it: a link or a page in CoderHelm, or a command to copy for an install from before native releases. **Check again** reruns them. The same list is `GET /api/system/preflight`; see [Check your install](./check-your-install.md).

From the terminal, `update` runs a preflight first and stops before anything changes when:

- the config has a problem (each one named, with the setting to fix);
- there is less than about 1.5 GB free where releases are kept (`~/.coderhelm/releases`, or `CODERHELM_HOME`);
- the install isn't healthy (each failed check says how to fix it; `doctor` shows every check);
- it crosses a release marked **NEEDS ACTION** in the notes (do what it says, then run again with `--yes`).

## Update now

**Settings, then System, then Update to vX** starts the same update as the automatic one, and records who started it. On an install made with Launch Stack it applies, whether auto-update is on or off: the page shows it preparing, then running, then the result in the update history (10 to 20 minutes). You never need the AWS console or a terminal for it.

Some releases need you to say yes first. When the prepared update would replace or remove data (a table, bucket, key, user pool or secret) or change permissions (an IAM role or policy, a function permission, a resource policy, or a new principal), the page lists exactly those changes and asks you to confirm them. It applies only once you confirm that list; if the list changes before you do, you confirm again.

## What an update does

On an install made with Launch Stack, an update is a stack update of `coderhelm-<name>` to the new release's `coderhelm.json`, prepared as a CloudFormation change set. Creating a change set changes nothing.

1. **Checks the release.** A read-only build in your account checks the release's tag: it must be on a commit on `main`, and be signed with a maintainer key pinned in the release your install runs or attested by CoderHelm's automatic release workflow on `main` for that tag and commit. It then downloads the release's `SHA256SUMS` and checks its signature with cosign. It must be signed, keyless, by exactly `CoderHelm/coderhelm-platform`'s `release.yml`, on that release's own tag and from the tag's commit, and be in the Rekor transparency log. It reports the sha256 of `coderhelm.json` from the signed `SHA256SUMS`. A release that fails is refused. See [How updates are kept safe](./updates-security.md).
2. **Checks the models** the release calls ([the model check](#the-model-check)).
3. **Prepares the change set.** The updater downloads `coderhelm.json` from `https://coderhelm-public.s3.amazonaws.com/launch/<version>/`, where release files are written once and never replaced, and proposes it only if its sha256 matches the signed one. First the install helper copies the release's part templates into the install's own release bucket, each checked against the sha256 that verified `coderhelm.json` lists, so CloudFormation can read them while it prepares the parts' own change sets. Every parameter keeps its current value. After CloudFormation reads the template, the updater compares what it read with the verified one, and runs nothing if they differ.
4. **Applies it, through the deploy role.** **Settings, then System** shows the change set's summary while it runs: how many resources are added, changed and removed, which ones are replaced, which parts it touches, and a link to it in the CloudFormation console if you want to look.
   - **You pressed Update:** it applies as soon as it is prepared. When it would replace or remove data or change permissions, it waits for you to confirm exactly those changes on the page, then applies.
   - **The maintenance window started it (auto-update on):** it applies by itself, except when it would replace or remove data or change permissions: then it waits, admins get an email, and you confirm it on the page the same way. If auto-update was turned off before it ran, it waits for you to press **Apply**.
   - **It changes the deploy role or a permissions boundary:** the deploy role can't change those (that is what keeps it from giving itself more), so CoderHelm can't apply it for you. The page says so and links to the change set: someone with administrator rights in the AWS account reviews it and chooses **Execute**. Releases do this rarely, and their notes say so.
5. **Follows it.** Every 5 minutes the updater reads the change set and the stack until the update ends. When the stack update starts, the install's own copy step checks every file of the release against the sha256 that `coderhelm.json` lists before any part changes. If a part fails, CloudFormation rolls the whole stack back to the previous release by itself.
6. **Reports.** The history in **Settings, then System** shows the version, when, who (or "auto") and the result: succeeded, failed, or rolled back. A try that didn't succeed says in plain words what happened, where your install is now and what to do next; CloudFormation's own text (for the first resource that failed) is under **Technical details**. Admins get an email either way.

A change set that waits too long can go stale (the stack changed since it was made); the history then says so, and **Update** prepares a new one. Pressing **Update** while an older prepared update waits replaces it (nothing was changed by the older one).

### The first update onto a release with the deploy role on every install

Up to v0.8.26 the deploy role existed only with **AutoUpdate** `on`. An install launched with it off on one of those releases gets the role from its next update, and that one update can't apply through a role it doesn't have yet: the page says so and links to the change set, and someone with administrator rights in the AWS account executes it once in the CloudFormation console. Every update after that applies from the page. An install that had **AutoUpdate** `on` from the start already has the role and needs nothing. If you once turned the stack parameter **AutoUpdate** on and then off, the role it made was kept and that one update fails with "already exists": delete the kept role and its boundary first (`coderhelm-selfhost leftovers --delete` lists and removes them), then press **Update** again.

### Installs from before native releases

An install from before native releases updates with a CodeBuild job in your account, `coderhelm-<stage>-update`. It runs the same release checks, checks health first, reads the migration notes (a release that needs action stops an automatic update), deploys the release's CDK app, checks health again and deploys the previous release again if the new one is unhealthy. On an install an admin hardened, the job's role holds no deploy rights until the updater gives this one build a deploy window, and the build deploys only through the install's own deploy role ([what that means](./updates-security.md#what-an-update-may-do-in-your-account)). Otherwise the job's role assumes the CDK bootstrap roles, as it always has. To move such an install to one stack, see [Migrate to one stack](./migrate.md).

## When an update fails

The history entry says what happened and what to do next:

- **Rolled back:** the update didn't finish, so CloudFormation (or, on an install from before, the update job) put the previous release back. Your data is untouched. Fix what the reason and **Settings, then Setup** (or `doctor`) list, then update again. Automatic updates skip that release until an admin updates to it by hand.
- **Failed:** the update stopped before or during the deploy. Check **Setup** for what fails and how to fix it. If the install isn't working, roll back.
- **Couldn't start:** AWS refused to prepare the change, so nothing changed and your install stays on the release it runs.

A try that didn't succeed never holds up the next release: automatic updates skip only that one version, and **Update** works for a newer release (or the same one again) as soon as it is out.

### The update couldn't start: the update role isn't allowed to use an AWS template feature

Up to v0.8.31, three part templates used an AWS template feature (the `AWS::LanguageExtensions` transform) that the install's deploy role isn't allowed to use. Since every update from the page goes through that role, an **Update** to one of those releases failed before anything changed, for example: "The update couldn't start: CoderHelm's update role isn't allowed to use an AWS template feature this release needs. Nothing changed: your install stayed on v0.8.29." The releases after v0.8.31 use no transform, and CoderHelm's tests fail a release whenever a template uses one the deploy role can't. Press **Update** again once a newer release is out: it applies from the page as usual, with no step in the AWS console. Your data and the **KeepDataOnDelete** choice are unchanged.

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
