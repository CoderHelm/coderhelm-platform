---
title: Security model
description: What runs where, who can reach what, and how secrets and credentials are kept.
order: 11
---

## Trust boundaries

- **Everything runs in your AWS account.** CoderHelm the company has no access to it: no cross-account role, no support backdoor, no license check calling home.
- **Your GitHub App.** Created by you, installed by you, with the permissions listed in [The GitHub App](./github-app.md#what-the-app-may-do). Webhooks are verified with its secret before anything runs.
- **Model calls** go from your functions to the Anthropic API with a team's key, or to Bedrock in your account with no key at all.
- **Telemetry is off by default.** When you turn it on, only the `coderhelm-selfhost` command line sends a ping on install, upgrade, health check and uninstall: a random install id, the release, the event, the region, the layout and the provider. No account id, domain, email, repository or IP address is accepted by the receiver. See `installer/lib/commands.mjs` (`telemetryPayload`) and `services/gateway/src/routes/telemetry.rs`.

## Network paths

| From | To | How |
|---|---|---|
| People | CloudFront (your hostname) | HTTPS only, TLS 1.2+, HSTS |
| CloudFront | HTTP API, streaming function URL, dashboard bucket | HTTPS; the bucket is private and only CloudFront can read it |
| GitHub, Jira | `/webhooks/*` | Signed payloads, checked before use |
| Functions | GitHub, Anthropic, Bedrock, the services teams connect | HTTPS from Lambda (no VPC) |

## Identity and sessions

- Accounts live in a Cognito user pool in your account, with optional TOTP and passkeys. Sign-up is limited to `admins`, `signup.emails` and `signup.domains`.
- Sessions are `__Host-` cookies (HttpOnly, Secure, SameSite=Strict) signed with the deployment's own key, on your one hostname.
- Install admins (`admins`) must have an email CoderHelm verified itself; a GitHub profile email never counts.

## Secrets and encryption

- GitHub App keys and the session signing key are in one Secrets Manager secret. Only the gateway, streaming and worker roles can read it (and the install helper, which stores the setup key and reads the hybrid registration on delete). CloudFormation creates it with a generated session signing key; the GitHub App keys are written when you create the app, and never printed.
- Team credentials (MCP servers, data sources, chat webhooks) are sealed with a KMS key and bound to their team.
- Tables, the data bucket and queues are encrypted (customer managed KMS keys for tables and the bucket).

## Least privilege

- Each function has its own role with only the tables, queues, keys and actions it uses.
- The build sandbox runs a repository's own build scripts, which are untrusted: its CodeBuild role can only read and write one bucket of short-lived source tarballs and write its logs. No GitHub token, no tables, no KMS.
- Roles in teams' AWS accounts are assumed only by name (`CoderHelmLogReader`, `CoderhelmReadOnly*`) and with an external id. When the data is in the install's own account, see the exception in [The permissions boundary](#the-permissions-boundary).
- An install made with Launch Stack holds no broad standing rights in your account: see [What an install can do in your account](#what-an-install-can-do-in-your-account).

## What an install can do in your account

This section is about an install made with Launch Stack: one CloudFormation stack, `coderhelm-<name>`, made of nested stacks. [Installs from before](#installs-from-before-native-releases) work differently.

Nothing CoderHelm creates holds AdministratorAccess. There is no build job that installs or updates CoderHelm, and no deploy role unless you turn auto-update on. CloudFormation runs the stack with the permissions of the person who launches, updates or deletes it (or a CloudFormation service role that person picks in the console).

### What Launch Stack creates

The stack you launch (the parent, from the release's `coderhelm.json`) creates:

- the permissions boundary, `coderhelm-<name>-boundary-stack`;
- a private bucket for this release's files, `coderhelm-<name>-release-<account>` (public access blocked, encrypted, tagged for the install). Its bucket policy lets only the install helper write or delete files in it;
- the install's secret, `coderhelm/<name>/secrets`, with a session signing key CloudFormation generates. The GitHub App keys are added later, when you create the app;
- the install helper: one function, `coderhelm-<name>-install-helper`, with its role and its log group. Its code is written inline in `coderhelm.json`, so the signature that covers the template covers it too;
- the parts of CoderHelm (database, storage, API, worker, web and the rest) as nested stacks, each read from the release bucket above;
- a few custom resources the install helper answers (below);
- only with **AutoUpdate** set to `on`: the auto-update role, `coderhelm-<name>-deploy-stack`, and its own copy of the boundary, `coderhelm-<name>-deploy-boundary-stack`.

An install's name is lowercase letters and digits only, with no hyphen, so `coderhelm-<name>-*` never also names another install (`prod` and `prod-eu` would overlap).

Every function's log group is a resource of its stack, so deleting the stack removes it.

If the first launch fails, CloudFormation rolls the stack back and deletes what it created. The stack's events name what AWS refused, and so does the install guide when it follows the launch.

### The custom resources

| Resource | What it does |
|---|---|
| ReleaseAssets | Downloads every nested template and code file of this release from `https://coderhelm-public.s3.amazonaws.com/launch/<version>/`, checks each against the sha256 that `coderhelm.json` lists for it, and writes it to the release bucket with that SHA-256 as the upload's checksum. S3 checks it and keeps its own, so a file already there is reused only when S3's checksum says it is that file. A file that doesn't match is refused and the stack fails. On an update it keeps the files of the new and the previous release (a rollback needs them) and deletes the rest. When the stack is deleted it empties the release bucket. |
| SetupKey | Stores the install guide's one-time setup key in the secret, only while no GitHub App is stored there. |
| Allowlist | Puts the first admin, the sign-up domain and the two-factor rule on the sign-in allowlist. It adds entries and never removes them. |
| Domain | With **Hostname** set: asks the updater to request the certificate (see [Your own domain](./domains.md)). |
| Status | With the install guide's progress id: writes a small public progress file (the phase, and your URL when done). On a failed first launch it writes CloudFormation's own reason. |
| HybridLeave | When the stack is deleted: releases the CoderHelm address and links of a [hybrid](./hybrid.md) install. |
| Sweep | When the stack is deleted with **KeepDataOnDelete** `false`: empties the data buckets and deletes the tables. See [Uninstall](./uninstall.md). |
| Final | When the stack is deleted, after every part and after HybridLeave and SetupKey: deletes the certificates the updater requested for this install and their validation records, and every log group the parts declare, by its exact name (Lambda makes one again when a function's last logs arrive after CloudFormation deleted it). With **KeepDataOnDelete** `false`, it also deletes the install's other log groups that are still there, only those tagged `coderhelm:install = coderhelm-<name>`, and the install's secret, without a recovery window. |

Delete work happens only while the root stack itself is being deleted, or while its first launch rolls back. An update that removes or replaces one of these resources never empties or deletes anything. Every step and every AWS error goes to the helper's log group, and the reason goes back to CloudFormation, which shows it in the stack's events.

### The permissions boundary

`coderhelm-<name>-boundary-stack` is the most any CoderHelm role can ever do, whatever its own policy says. Every role CoderHelm creates carries it: the functions, the updater and the install helper. The auto-update role carries an identical copy, `coderhelm-<name>-deploy-boundary-stack`: that role is kept when the stack is deleted, and IAM refuses to delete a policy a role still carries, so its copy is kept with it while the boundary every other role carries goes with the stack. Its text is written once, in `infra/lib/access.ts`. It allows only the services CoderHelm uses: Lambda, DynamoDB, S3, SQS, SNS, EventBridge, CloudWatch and its logs, Secrets Manager, KMS, SES, Cognito, CloudFront, API Gateway, CodeBuild, Systems Manager parameters, ACM, Route 53, CloudFormation, and calls to Bedrock models. Even within those, it refuses:

- creating IAM users, access keys, sign-in profiles, groups or identity providers;
- anything in Organizations, account settings, CloudTrail or GuardDuty;
- any IAM action on a role or policy whose name doesn't start with `coderhelm-<name>-`;
- creating or changing a role without this boundary on it, and removing a boundary from any role;
- changing, invoking or creating a function, changing a stack, writing or deleting in a bucket, reading or changing a secret, or changing a parameter, unless it is the install's own: functions and layers named `coderhelm-<name>-*`, stacks named `coderhelm-<name>...`, buckets named `coderhelm-<name>-*` (and the progress bucket `coderhelm-setup-*`), secrets under `coderhelm/<name>/`, parameters under `/coderhelm-<name>/`. So a role CoderHelm creates can't rewrite another function in the account or update a stack that runs with another role;
- running commands or sessions on instances (Systems Manager Run Command, Session Manager, Automation, associations);
- changing, deleting or retagging the boundary itself or the auto-update role's copy, or changing the auto-update role;
- changing a stack named `coderhelm-<name>-access` (where an install hardened from the command line keeps its boundary);
- assuming any other role in this account, such as the CDK bootstrap roles, the auto-update role, CoderHelm's own roles or yours. Roles in other accounts can still be assumed by name and external id, which is how log reading works.

One exception covers an install in the same AWS account as the data it reads. There, the AWS connection uses the log reader role (`CoderHelmLogReader`, from CoderHelm's log reader template) and AWS data sources use the read-only role (`CoderhelmReadOnly`, from the data-source setup script). The boundary lets CoderHelm take on exactly these two roles in its own account, and only:

- by their exact names: a role named differently (`CoderhelmReadOnly-2`, another case, a path) stays refused;
- with `sts:AssumeRole`: no other STS action (session tags, source identity, web identity or SAML) on them;
- by the gateway's and the worker's roles, which carry the tag `coderhelm:role` (`gateway` or `worker`). Any other role, CoderHelm's included, is refused. No CoderHelm role but the auto-update role can set or remove that tag; a stack update you run with your own permissions can too;
- with the team's external id, which each role's trust policy requires. In the install's own account, anyone who can read the role can also read that id, so what limits these roles is their own permissions: read-only, as CoderHelm's template and setup script create them. `check-access.mjs` fails if either role holds more.

Two limits remain. The log alerts sweep (the log analyzer) is not among these roles, so in a same-account install it can't read logs through `CoderHelmLogReader`. And the log reader can read every log group in the account, CoderHelm's own included.

### The install helper

The helper's role carries the boundary and can only do this (`helperPolicy()` in `infra/lib/native/parent.ts`):

- write its own log group, `/aws/lambda/coderhelm-<name>-install-helper`;
- read, write, list and delete objects in the release bucket;
- list and delete objects in buckets named `coderhelm-<name>-*` (to empty the data buckets on delete);
- write the install guide's progress file and its read policy, in this install's own progress bucket, `coderhelm-setup-<progress id>`;
- read and write the install's secret, `coderhelm/<name>/secrets`, and delete it (only on a stack delete with **KeepDataOnDelete** `false`);
- write items to, describe, update and delete the tables named `coderhelm-<name>` and `coderhelm-<name>-*` (the allowlist at launch; the tables on delete), and use the KMS keys tagged `coderhelm:install = coderhelm-<name>` to encrypt and decrypt for them;
- invoke `coderhelm-<name>-updater` (the domain);
- describe the root stack and read its events;
- list log groups, and read the tags of and delete those named `/aws/lambda/coderhelm-<name>-*` or `/aws/codebuild/coderhelm-<name>-*`. The code deletes the exact names the parts declare, and otherwise only those tagged `coderhelm:install = coderhelm-<name>`;
- list and describe certificates in ACM in us-east-1, and delete only those tagged `coderhelm = coderhelm-<name>` (the ones the updater requested): IAM checks the tag;
- list Route 53 zones, and delete only CNAME records whose names start with `_` (certificate validation records, removed with the certificate). It can't create or change any record.

Besides CloudFormation, only the updater calls it, for one thing: before a change set, copy a release's part templates into the release bucket (`stage`). It downloads that release's `coderhelm.json` itself, checks it against the sha256 from the signed `SHA256SUMS` that the updater passes, and copies only the part templates it lists, each checked like ReleaseAssets does.

It answers only CloudFormation's own response address for a custom resource, and it keeps account numbers, ARNs and email addresses out of the public progress file.

It downloads release files over HTTPS from `coderhelm-public.s3.amazonaws.com/launch/v...` only, and refuses any other address. It can't create or change roles, policies, functions or stacks.

### Updates and the optional auto-update role

An update is a stack update of `coderhelm-<name>` to the next release's `coderhelm.json`, prepared as a change set that includes each part's own change set. Creating a change set changes nothing. The updater creates it only after its release check verified the release's signature and reported the sha256 of `coderhelm.json` from the signed `SHA256SUMS`, and only when the template it downloads matches that sha256. A domain move is made the same way, from a verified template. Once CloudFormation has made the change set, the updater reads back the template CloudFormation itself fetched and compares it with the verified one; if they differ, it deletes the change set and nothing runs. It never proposes an older release as an update. The parent template has no transform (such as `AWS::LanguageExtensions`): CloudFormation keeps a template with a transform already resolved, so what it fetched could never be compared with what was verified, and an update with the current template would keep every old parameter value. The updater refuses a template with a transform. See [Updates](./updates.md).

The updater's role may (`nativeUpdates()` in `infra/lib/updates-stack.ts`):

- create a change set on this stack only, only with a template URL like `https://coderhelm-public.s3.amazonaws.com/launch/v*.*.*/coderhelm.json` (release files there are written once and never replaced), and with a service role only if it is `coderhelm-<name>-deploy-stack`;
- describe, list, execute and delete change sets on this stack and its parts, read the template a change set was made from, and describe the stack and its events;
- pass `coderhelm-<name>-deploy-stack`, and only to CloudFormation;
- invoke the install helper, to stage a release's part templates before a change set, and read the staged files in the release bucket (CloudFormation reads the parts' templates with the caller's access while it prepares their change sets).

It can't choose the code: only a published release's template. It does choose the parameter values it passes, which it keeps as they are except for the domain parameters when it moves your domain.

**AutoUpdate off (the default).** There is no deploy role. The change set waits for you: Settings, System, Update shows its summary and a link to it in the CloudFormation console, where you review it and choose **Execute** with your own permissions. The updater never executes it.

**AutoUpdate on.** The stack creates `coderhelm-<name>-deploy-stack`, a CloudFormation service role, and the updater executes the change set through it in your maintenance window. It never does so when the change set (or a part's) would replace or remove a table, bucket, key, user pool or secret: then it waits for you, and says which.

- Only CloudFormation can assume it, only for this account (`aws:SourceAccount`) and only for this stack and its parts (`aws:SourceArn`).
- It carries the boundary (its own copy, kept with it).
- Its policy is the deploy policy from `infra/lib/access.ts`, plus every CloudFormation action on the parent stack and its parts, reading the release bucket, reading and updating (never deleting) the install's secret, and the install guide's progress bucket. `infra/test/fixtures/native-sim.json` holds AWS's IAM policy simulator verdict on every call CloudFormation makes through it. The deploy policy lets it:
  - create and change roles named `coderhelm-<name>-*`, and only with the boundary on them. It can pass only those roles;
  - create and change functions, tables, buckets, queues, topics, log groups, rules, schedules, alarms, build projects, parameters, email settings and key aliases whose names start with `coderhelm-<name>`. Every one of CoderHelm's resources is named that way, and a test checks it;
  - create KMS keys, user pools, CloudFront distributions and certificates. AWS names these by id, not by name, so it can change them only while they carry the tag `coderhelm:install = coderhelm-<name>`. It can never delete or disable a KMS key;
  - create CloudFront functions and SES email templates by any name, because AWS checks those two calls against no resource at all. Every later change to a function is limited to names that start with `coderhelm-<name>`.
- It can't create IAM users or keys, give a role more than the boundary allows, use the CDK bootstrap, or touch CloudTrail, GuardDuty, Organizations or your other resources by name.

So whoever controls the updater can move the install only to a template CoderHelm published, and can use the role only through CloudFormation.

**A known limit.** CloudFormation keeps using a stack's service role for later operations once a stack was updated through it, and a role can't delete itself. So the auto-update role is kept when you delete the stack (and when you turn auto-update off again). `coderhelm-selfhost leftovers` lists it after the stack is gone, and `--delete` removes it. Its own copy of the boundary, `coderhelm-<name>-deploy-boundary-stack`, stays with it (IAM refuses to delete a policy a role still carries), and the leftover check lists both, only once this region's stack is gone and only when the kept role trusts that stack. Because later operations run through the role, they are limited to what it may do: it may not schedule a KMS key for deletion, change either boundary, or change its own policy or boundary. So a release that changes the boundary or that role's policy waits for an admin, and so does the first update of an install with auto-update on to a release that gives the role its own copy (v0.8.6), and a delete with **KeepDataOnDelete** `false` needs `aws cloudformation delete-stack --role-arn <a role of yours>`. Anyone who may update this stack in CloudFormation can use the role this way too, without needing to pass it: keep CloudFormation write access on `coderhelm-<name>` to the people who administer the account. With auto-update off (the default) none of this applies and nothing is left.

Who can still act within the boundary:

- someone who can change CoderHelm's functions or its IAM roles. That already takes administrator rights in your account.
- a release that passed every check in [How updates are kept safe](./updates-security.md). A malicious release could change any CoderHelm resource. It could also change the few things the deploy policy can't scope by name or tag in this region: CloudFront origin access controls and response header policies, API Gateway HTTP APIs, and SES email identities. It could add CoderHelm's tag to one of your KMS keys, user pools, distributions or certificates and then change it. It could not create an admin role or an IAM user, touch IAM outside `coderhelm-<name>-*`, or switch off CloudTrail or GuardDuty. With auto-update off, such a release runs only when you execute its change set.

### How a release is verified

You check one file, `coderhelm.json`, against the release's signed `SHA256SUMS` (the commands are in [Verify a release before launching](./updates-security.md#verify-a-release-before-launching)). That file pins the sha256 of every other file of the release: each nested template and each code bundle. The ReleaseAssets resource checks every file against those sha256s before it writes it to your release bucket, and the nested stacks and functions read only from that bucket. So the signature covers everything the install runs, and rollbacks read your own copies, not CoderHelm's.

### Uninstalling

Uninstalling is **Delete stack** in CloudFormation, with the permissions of the person who deletes it (or the auto-update role, if the stack was ever updated through it). **KeepDataOnDelete** decides about the data. See [Uninstall](./uninstall.md).

### Check it yourself

From a machine with read access to the account (IAM, CloudFormation, CodeBuild, SSM):

```sh
AWS_PROFILE=<profile> node selfhost/check-access.mjs --stage <name> --region <region>
```

It only reads, and uses IAM's policy simulator, which starts nothing. For an install made with Launch Stack it prints a PASS/FAIL table covering these checks:

- the install is one CloudFormation stack, with auto-update on or off;
- every CoderHelm role carries the boundary;
- no role has AdministratorAccess, and no policy allows `*:*`;
- with auto-update off, there is no deploy role (one left from when auto-update was on fails, and `leftovers` lists it); with it on, its trust names only CloudFormation, and the simulator refuses it IAM users, roles without the boundary, the boundary itself, other roles, Organizations, CloudTrail and key deletion;
- no CoderHelm stack runs with any role but the auto-update role, and none uses the CDK bootstrap roles;
- there is no installer or update build;
- the boundary refuses every role in the account except CoderHelm's two read roles, and those only with `sts:AssumeRole` and only by the gateway's and the worker's roles;
- only the gateway's and the worker's roles carry `coderhelm:role`, and the policy simulator lets those two roles assume the two read roles and no other role in the account;
- when `CoderHelmLogReader` or `CoderhelmReadOnly` exists in the account, its trust needs an external id and names only this account or the install's roles. It also has no managed policy, and nothing that allows `*`, assumes or passes roles, or writes IAM.

To check the policies themselves, run `python3 scripts/deploy-role-sim.py --native` from `infra/` (after `npx tsc`, with credentials for any AWS account). It takes every call CloudFormation makes for the native parts, and a list of what the install helper may and may not do, and checks each with IAM's policy simulator against the auto-update role's and the helper's policies under the boundary. It only reads, and writes the verdicts to `infra/test/fixtures/native-sim.json`.

### When a release changes the boundary

The boundary is a resource of the parent stack, so a release that changes it changes it in the stack update, and the change set shows it. Review it like any other change.

### Installs from before native releases

An install made before native releases (with the command line, or the earlier Launch Stack and its install build) is about eleven stacks named `coderhelm-<name>-<part>`. It still deploys the way it always has. Move it to one stack with [`coderhelm-selfhost migrate`](./migrate.md), which keeps every table, bucket, user pool, key and secret.

Until then, `access harden` still moves such an install onto its own boundary and deploy role (in the stack `coderhelm-<name>-access`), so its updates hold deploy rights only while they run. An admin runs it once, with administrator credentials, from a machine with the install's config (`coderhelm-selfhost pull`):

```sh
npx -y github:CoderHelm/coderhelm-platform access harden
```

Run it again if it stops part way. Afterwards `selfhost/check-access.mjs` should pass.

### Rolling back past hardening

A hardened install can roll back only to releases that know the boundary. A release from before hardening would remove the boundary from CoderHelm's roles, which the boundary forbids. **Roll back** refuses such a release, and an automatic rollback after an unhealthy update stops instead of trying. Going back that far needs an admin: deploy the older release with administrator credentials, knowing it puts back the old way of deploying.

## Supply chain

Releases are built by GitHub Actions from a tag on `main` and signed keylessly (Sigstore) with build provenance attestations. The updater's release check, and the command line, check two things. The release's tag must be on `main`, and signed with a CoderHelm maintainer key pinned in the code or attested by CoderHelm's automatic release workflow on `main` for that exact tag and commit. The signature must be made by exactly CoderHelm's release workflow, on that tag and from the commit the tag points to. Anything else is refused, including older releases you never ran. For Launch Stack you check `coderhelm.json` yourself, and the install checks every other file against it ([How a release is verified](#how-a-release-is-verified)). [How updates are kept safe](./updates-security.md) has the details and the commands. You can also deploy from source (`--source`) after reviewing the code.

## Hybrid threat model

This part covers a [hybrid](./hybrid.md) install: one that uses CoderHelm's address, GitHub App or integration apps. A fully private install talks to none of what follows.

### What is worth protecting

- **Your code and repositories.** CoderHelm's GitHub App can read and change code. Its installation tokens are the most valuable thing the hybrid side hands out.
- **Your install's identity**: its id, its registration secret and its relay key. Whoever holds them can act as your install toward CoderHelm.
- **Your provider sign-ins** (Notion, Confluence, Google Drive): what your agents read as knowledge.
- **Your events**: GitHub and Jira payloads, which can contain code, titles and comments.
- **Your address**, `yourname.coderhelm.com`, and CoderHelm's own hosts in the same zone (`app`, `api`, `auth`, `docs`).
- **Sessions on app.coderhelm.com and api.coderhelm.com** of people who also use the hosted service.

### Who might attack

1. Someone who runs their own hybrid install (and so controls its code, its CloudFront and its address).
2. Anyone on the internet, with no account.
3. Someone who controls one `{name}.coderhelm.com` address and the pages it serves.
4. Someone who can see one install's network traffic.

### Attacks and what stops them

| Attack | What stops it |
|---|---|
| Register an install, or act as another one | Anyone may register (10 an hour per address, counted across all of CoderHelm's servers), but a registration does nothing until CoderHelm's challenge is answered from the install's own CloudFront address with that install's secret. Every later call is signed with the install's secret (HMAC-SHA256 over the time, method, path and body, compared in constant time, at most 5 minutes old). Ids have one fixed shape, so no id can be built to collide with another install's keys. |
| Work out an install's keys | Each install's secret and relay key are HMAC-SHA256 of CoderHelm's master key with a fixed label (`install:` or `relay:`) and the install id. The labels differ, so no install's secret can equal another install's relay key, and a key is useless without the master key. |
| Keep working after CoderHelm turns an install off | Every signed call, token request, sign-in and relayed event checks that the install is active. Revoking also removes its name and its GitHub links, and its CloudFront address can't register again. |
| Send a forged GitHub event | The hosted side checks GitHub's own signature (`X-Hub-Signature-256`) before it looks at anything else, and forwards only to the install that installation is linked to. |
| Send a forged or replayed relayed event to an install | The install checks the relay signature (time, delivery id, body, and from `v2` the source and event headers), refuses anything older than 5 minutes, and takes each delivery id once in its own database. If it can't record the delivery id, it refuses the event and the relay tries again. |
| Forge a Jira event through the relay | The install checks the Forge app's own secret, which CoderHelm forwards but doesn't decide. |
| Get events or tokens for someone else's GitHub organization | A link needs a GitHub sign-in that sees that installation of CoderHelm's app (another app's installation never counts), and a press of **Link** on a CoderHelm page that names the install's address and can't be shown inside another page. One installation links to one install, never to one a hosted team already uses. Tokens reach only the repositories the person who linked it can push to, so read access to one repository can't become write access to all. |
| Mint a wider token | The broker asks GitHub for the token with an explicit list of repositories and permissions, never more than the link allows and never more than the app's own permissions. GitHub refuses anything outside that list. Tokens last at most an hour. |
| Take over someone's Notion, Confluence or Google Drive sign-in | The sign-in is finished in a holding place of its own and becomes the install's only when the install redeems the one-time code with its secret and the PKCE verifier (S256), at the return address it registered. Starting sign-ins is limited per address. |
| Send people somewhere else after linking or signing in | Return addresses are compared host by host against the install's own CloudFront address and name (and the docs install guide); anything else goes to the install's settings page. Sign-in returns must be the install's own callback page, over HTTPS, with no port or user name. |
| Make CoderHelm's servers call something inside AWS | CoderHelm calls an install only at the CloudFront address it proved at registration, which never changes afterwards, and, for email links, at the install's own domain: that name must resolve only to public addresses (no private, loopback, link-local, metadata or unique-local ones) and the connection goes to the address checked. Those calls never follow a redirect and read at most 16 KB of the answer. CloudFront's own names can't be pointed at other addresses. |
| Claim a reserved or misleading name | Names are lowercase letters, digits and single hyphens (so no punycode, other alphabets or trailing dots), 3 to 40 characters. CoderHelm's own hosts and a blocklist are refused, and so is any name that reads as CoderHelm's own: `coderhelm` anywhere, digits standing in for letters included, or a part such as `app`, `api`, `login`, `billing` or `support` (`app2`, `acme-login`). |
| Take over another install's name, or point a name elsewhere | A claim is a write that fails when the name exists. The address record must point at the install's own verified CloudFront, proved again just before it is written; the certificate record must have the exact shape ACM uses and point at `acm-validations.aws`. |
| Make CoderHelm write any other DNS record | The code writes only two CNAMEs per name (the name and `_<token>.<name>`) plus the setup page's alias while the name is new, and never overwrites a record it didn't write. Its AWS permissions are a second fence: CNAME, A and AAAA only, in that one zone, never the apex, the wildcard or a reserved name, never anything under CoderHelm's own hosts, never mail or certificate-authority records (`_domainkey`, `_dmarc`, `_mta-sts`, `_acme-challenge`) and never anything three labels deep. |
| Use a customer address against CoderHelm's own sites | Every address under coderhelm.com is the same site as app.coderhelm.com and api.coderhelm.com to a browser, so nothing relies on that. Cookies are `__Host-` (no Domain attribute), so a customer page can neither read nor set them. CORS answers only exact origins, never `*.coderhelm.com`. A change sent with cookies from any other page on the same site is refused. Sign-in messages between windows check the exact origin. |
| Show other content on the "no CoderHelm here yet" page | The wildcard site has one origin, CoderHelm's own fallback page, which shows one of two fixed pages and nothing from the request but whether the name is being set up. |
| Read through the public name check | It answers only whether a name is free (claimed names are public in DNS anyway), carries no cookies, and is limited to 30 checks a minute per address. The install guide's setup routes need the install's one-time setup key. |
| Read other teams' deliveries | Deliveries are read by team, by admins only. An install reads only its own relay and broker records, with its own signature. Stored headers have every secret-looking value removed, and the relay keeps no payload in its records. |
| Read traffic between an install and CoderHelm | Everything is HTTPS. The keys never travel after registration: requests carry only signatures, which are bound to the time, the path and the body. |

### What CoderHelm can and cannot see

CoderHelm **can** see: each relayed event's payload while it forwards it (and holds it for up to 24 hours only while retrying), the Forge app's secret as it passes, delivery metadata for 14 days, the installation tokens it mints as it hands them out, your provider sign-ins (sealed with a key only its services can use), your name, your CloudFront address and your two DNS records, and failed-delivery metadata you chose to share.

CoderHelm **cannot** see: your code at rest, your runs, plans, reviews, memory, knowledge or secrets, your model calls, your AWS account, or your people's sign-ins. It can't sign in to your install.

### Known limits

- A link made before the **Link** confirmation and the push-permission limit existed gets no tokens (`broker_relink_needed`). Link it again to bring it under both.
- Someone who controls CoderHelm's hosted side could mint tokens for linked installations, read relayed events and act as the relay. Choose your own GitHub App and apps (fully private) if that is outside what you accept.
- Install requests carry no one-time number, so someone who could break HTTPS could repeat one within its 5 minutes. They would already see the same traffic, tokens included.

## Reporting a problem

Report security issues privately to the support address in the CoderHelm repository rather than in a public issue.
