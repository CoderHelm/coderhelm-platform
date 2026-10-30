---
title: Security model
description: What runs where, who can reach what, and how secrets and credentials are kept.
order: 11
---

## Trust boundaries

- **Everything runs in your AWS account.** CoderHelm the company has no access to it: no cross-account role, no support backdoor, no license check calling home.
- **Your GitHub App.** Created by you, installed by you, with the permissions listed in [The GitHub App](./github-app.md#what-the-app-may-do). Webhooks are verified with its secret before anything runs.
- **Model calls** go from your functions to the Anthropic API with a team's key, or to Bedrock in your account with no key at all.
- **Telemetry is off by default.** When you turn it on, only the installer sends a ping on install, upgrade, health check and uninstall: a random install id, the release, the event, the region, the layout and the provider. No account id, domain, email, repository or IP address is accepted by the receiver. See `installer/lib/commands.mjs` (`telemetryPayload`) and `services/gateway/src/routes/telemetry.rs`.

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

- GitHub App keys and the session signing key are in one Secrets Manager secret. Only the gateway, streaming and worker roles can read it. The installer writes it from a temporary 0600 file and never prints it.
- Team credentials (MCP servers, data sources, chat webhooks) are sealed with a KMS key and bound to their team.
- Tables, the data bucket and queues are encrypted (customer managed KMS keys for tables and the bucket).

## Least privilege

- Each function has its own role with only the tables, queues, keys and actions it uses.
- The build sandbox runs a repository's own build scripts, which are untrusted: its CodeBuild role can only read and write one bucket of short-lived source tarballs and write its logs. No GitHub token, no tables, no KMS.
- Roles in teams' AWS accounts are assumed only by name (`CoderHelmLogReader`, `CoderhelmReadOnly*`) and with an external id. When the data is in the install's own account, see the exception in [The permissions boundary](#the-permissions-boundary).
- Installs made with the Launch Stack, and installs an admin hardened, hold no broad standing rights in your account: see [What the installer can do in your account](#what-the-installer-can-do-in-your-account).

## What the installer can do in your account

This section is about installs made with the Launch Stack, and installs an admin hardened with `coderhelm-selfhost access harden` (both are called *hardened* here). [Installs from before](#installs-from-before-hardening) work differently.

Nothing CoderHelm creates holds AdministratorAccess. Three pieces decide what CoderHelm can do. They are written once, in `infra/lib/access.ts`, and the Launch Stack's template is tested to be equal to them.

### The permissions boundary

`coderhelm-<name>-boundary` (with the Launch Stack, `coderhelm-<name>-boundary-<id>`) is the most any CoderHelm role can ever do, whatever its own policy says. Every role CoderHelm creates carries it: the functions, the builds, the updater, the installer and the deploy role. It allows only the services CoderHelm uses: Lambda, DynamoDB, S3, SQS, SNS, EventBridge, CloudWatch and its logs, Secrets Manager, KMS, SES, Cognito, CloudFront, API Gateway, CodeBuild, Systems Manager parameters, ACM, Route 53, CloudFormation, and calls to Bedrock models. Even within those, it refuses:

- creating IAM users, access keys, sign-in profiles, groups or identity providers;
- anything in Organizations, account settings, CloudTrail or GuardDuty;
- any IAM action on a role or policy whose name doesn't start with `coderhelm-<name>-`;
- creating or changing a role without this boundary on it, and removing a boundary from any role;
- changing, deleting or retagging the boundary itself, or changing the deploy role;
- changing the stack that holds the boundary and the deploy role (`coderhelm-<name>-access`);
- assuming any other role in this account, such as the CDK bootstrap roles, the deploy role, CoderHelm's own roles or yours. Roles in other accounts can still be assumed by name and external id, which is how log reading works.

One exception covers an install in the same AWS account as the data it reads. There, the AWS connection uses the log reader role (`CoderHelmLogReader`, from CoderHelm's log reader template) and AWS data sources use the read-only role (`CoderhelmReadOnly`, from the data-source setup script). The boundary lets CoderHelm take on exactly these two roles in its own account, and only:

- by their exact names: a role named differently (`CoderhelmReadOnly-2`, another case, a path) stays refused;
- with `sts:AssumeRole`: no other STS action (session tags, source identity, web identity or SAML) on them;
- by the gateway's and the worker's roles, which carry the tag `coderhelm:role` (`gateway` or `worker`). Any other role, CoderHelm's included, is refused. Only a CloudFormation deploy through the deploy role (or an admin) can set or remove that tag;
- with the team's external id, which each role's trust policy requires. In the install's own account, anyone who can read the role can also read that id, so what limits these roles is their own permissions: read-only, as CoderHelm's template and setup script create them. `check-access.mjs` fails if either role holds more.

Two limits remain. The log alerts sweep (the log analyzer) is not among these roles, so in a same-account install it can't read logs through `CoderHelmLogReader`. And the log reader can read every log group in the account, CoderHelm's own included.

### The deploy role

`coderhelm-<name>-deploy` is the role CloudFormation uses for CoderHelm's stacks. Only CloudFormation can assume it. It can:

- create and change roles named `coderhelm-<name>-*`, and only with the boundary on them. It can pass only those roles.
- create and change functions, tables, buckets, queues, topics, log groups, rules, schedules, alarms, build projects, parameters, email settings and key aliases whose names start with `coderhelm-<name>`. Every one of CoderHelm's resources is named that way, and a test checks it.
- create KMS keys, user pools, CloudFront distributions and certificates. AWS names these by id, not by name, so the deploy role can change them only while they carry the tag `coderhelm:install = coderhelm-<name>`. It can never delete or disable a KMS key.
- create CloudFront functions and SES email templates by any name, because AWS checks those two calls against no resource at all. Every later change to a function is limited to names that start with `coderhelm-<name>`.

It can't create IAM users or keys, give a role more than the boundary allows, use the CDK bootstrap, or touch CloudTrail, GuardDuty, Organizations or your other resources by name.

### The deploy window

The builds that install and update CoderHelm have **no deploy rights between runs**. While idle, the installer's role can only write its log and progress file, ask for a window and close its own. The update build's role can only ask the updater for a window and report back. Starting the project, or changing it, gains nothing.

When a build runs, it asks for its window once the release's signature and checksums check out:

1. The updater (for updates), or the Launch Stack's window function (for the install), reads the build from CodeBuild. The build must be running and use exactly this release's buildspec (compared by SHA-256), build image, role and settings, and the updater must have started it. A build someone started by hand, or a project someone changed, is refused, with the reason in the build's log.
2. It adds the policy `deploy-window` to the build's role. That policy is bound to that one build's session (`aws:userid`) and ends 100 minutes later (the build stops at 90). It lets the build change CoderHelm's stacks only through the deploy role, pass only the deploy role, upload to the install's assets bucket, and read and write the install's own secret and settings.
3. When the build ends, it removes the window, and the updater removes it too. The hourly check removes any window left while no update runs. If the Launch Stack is deleted or rolls back, it removes a window a failed install left behind.

Only one build of each project runs at a time.

Who can still act within the boundary:

- someone who can change CoderHelm's functions (the updater or the window function) or its IAM roles. That already takes administrator rights in your account.
- a release that passed every check in [How updates are kept safe](./updates-security.md). A malicious release could change any CoderHelm resource. It could also change the few things the deploy role can't scope by name or tag in this region: CloudFront origin access controls and response header policies, API Gateway HTTP APIs, and SES email identities. It could add CoderHelm's tag to one of your KMS keys, user pools, distributions or certificates and then change it, though never delete or disable a key. It could not create an admin role or an IAM user, touch IAM outside `coderhelm-<name>-*`, or switch off CloudTrail or GuardDuty.

### What the Launch Stack creates

The Launch Stack creates:

- the boundary, the deploy role and an assets bucket (`coderhelm-<name>-assets-<id>-<account>`);
- the installer (a CodeBuild project with its role);
- a starter function, a window function, and two parameters holding the window's policy.

The boundary, the deploy role and the assets bucket stay when you delete the stack, or when it rolls back after a failed install. CoderHelm keeps running and updating without the Launch Stack. Each launch names its pieces with its own short id, so launching again after a failure never collides with what the first launch left. If an install fails, the stack's events and the install guide name what AWS refused (for example `AWS refused iam:CreateRole on role/coderhelm-prod-...`). The stack then rolls back cleanly.

There is no CDK bootstrap: deploys use the install's own assets bucket and the deploy role.

### Check it yourself

From a machine with read access to the account (IAM, CloudFormation, CodeBuild, SSM):

```sh
AWS_PROFILE=<profile> node selfhost/check-access.mjs --stage <name> --region <region>
```

It only reads. It uses IAM's policy simulator for the idle build roles and starts nothing. It prints a PASS/FAIL table covering these checks:

- every CoderHelm role carries the boundary;
- no role has AdministratorAccess, and no policy allows `*:*`;
- every CoderHelm stack deploys through the deploy role, and none uses the CDK bootstrap roles;
- the deploy role's trust names only CloudFormation;
- the idle build roles have no deploy rights and no open window (a window held by a build that is running right now passes, and the row names the build);
- the deploy role is refused IAM users, roles without the boundary, the boundary itself and key deletion;
- the boundary refuses every role in the account except CoderHelm's two read roles, and those only with `sts:AssumeRole` and only by the gateway's and the worker's roles. The row names the exception when it is there, and says so when the boundary is from before it. Anything broader fails;
- only the gateway's and the worker's roles carry `coderhelm:role`, only the deploy role may set that tag, and the policy simulator lets those two roles assume the two read roles and no other role in the account;
- when `CoderHelmLogReader` or `CoderhelmReadOnly` exists in the account, its trust needs an external id and names only this account or the install's roles. It also has no managed policy, and nothing that allows `*`, assumes or passes roles, or writes IAM. The simulator also refuses it the install's secret and the deploy role.

### When a release changes the boundary

The boundary and the deploy role belong to an admin. An update never changes them, so a release that changes them says so in its notes. To pick up the change:

- **Launch Stack installs.** Update the Launch Stack (the CloudFormation stack you launched, usually `coderhelm-<name>`) with the new release's template. In the CloudFormation console choose **Update stack**, then **Replace existing template**, with `https://coderhelm-public.s3.amazonaws.com/launch/<version>/coderhelm-launch.yaml`, and keep every parameter as it is. The update changes the boundary, the deploy role and the installer's pieces in place. It doesn't run the installer and doesn't touch CoderHelm's other stacks. Then update CoderHelm to the same release as usual.
- **Installs hardened from the CLI.** Run `access harden` again with administrator credentials, on the new release. It redeploys `coderhelm-<name>-access` first.

Afterwards `selfhost/check-access.mjs` should pass.

### Installs from before hardening

An install made before this release, with the CLI or an older Launch Stack, still deploys the way it always has. Its updates assume the CDK bootstrap roles, whose CloudFormation role has AdministratorAccess by default, and an older Launch Stack's installer role has AdministratorAccess. Updating to this release changes neither. To move such an install onto its own boundary and deploy role, an admin runs this once, with administrator credentials, from a machine with the install's config (`coderhelm-selfhost pull`):

```sh
npx -y github:CoderHelm/coderhelm-platform access harden
```

The command works in this order:

1. It deploys `coderhelm-<name>-access`, which holds the boundary, the deploy role and the assets bucket.
2. It deploys every stack once with your credentials. This step puts the names, the boundary and the tags in place.
3. It deploys every stack again through the deploy role.
4. It offers to take AdministratorAccess off the old Launch Stack's installer role.

Run it again if it stops part way. Afterwards `selfhost/check-access.mjs` should pass. The CDK bootstrap stays in your account (other CDK apps may use it); CoderHelm no longer can.

### Rolling back past hardening

A hardened install can roll back only to releases that know the boundary. A release from before hardening would remove the boundary from CoderHelm's roles, which the boundary forbids. **Roll back** refuses such a release, and an automatic rollback after an unhealthy update stops instead of trying. Going back that far needs an admin: deploy the older release with administrator credentials, knowing it puts back the old way of deploying.

## Supply chain

Releases are built by GitHub Actions from a tag on `main` and signed keylessly (Sigstore) with build provenance attestations. The installer, the update build and the Launch Stack check two things. The release's tag must be on `main`, and signed with a CoderHelm maintainer key pinned in the code or attested by CoderHelm's automatic release workflow on `main` for that exact tag and commit. The signature must be made by exactly CoderHelm's release workflow, on that tag and from the commit the tag points to. Anything else is refused, including older releases you never ran. [How updates are kept safe](./updates-security.md) has the details and the commands to check a release yourself. You can also deploy from source (`--source`) after reviewing the code.

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
