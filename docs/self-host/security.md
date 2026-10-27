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
- Roles in teams' AWS accounts are assumed only by name (`CoderHelmLogReader`, `CoderhelmReadOnly*`) and with an external id.
- The installer and the Launch Stack's CodeBuild role need administrator rights to bootstrap CDK and create roles. Run later deploys with a narrower role if your policies require it.

## Supply chain

Releases are built by GitHub Actions from a tag on `main` and signed keylessly (Sigstore) with build provenance attestations. The installer, the update build and the Launch Stack check two things. The release's tag must be signed with a CoderHelm maintainer key pinned in the code. The signature must be made by exactly CoderHelm's release workflow, on that tag and from the commit the tag points to. Anything else is refused, including older releases you never ran. [How updates are kept safe](./updates-security.md) has the details and the commands to check a release yourself. You can also deploy from source (`--source`) after reviewing the code.

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
| Register an install, or act as another one | Anyone may register, but a registration does nothing until CoderHelm's challenge is answered from the install's own CloudFront address with that install's secret. Every later call is signed with the install's secret (HMAC-SHA256 over the time, method, path and body, compared in constant time, at most 5 minutes old). Ids have one fixed shape, so no id can be built to collide with another install's keys. |
| Work out an install's keys | Each install's secret and relay key are HMAC-SHA256 of CoderHelm's master key with a fixed label (`install:` or `relay:`) and the install id. The labels differ, so no install's secret can equal another install's relay key, and a key is useless without the master key. |
| Keep working after CoderHelm turns an install off | Every signed call, token request, sign-in and relayed event checks that the install is active. Revoking also removes its name and its GitHub links, and its CloudFront address can't register again. |
| Send a forged GitHub event | The hosted side checks GitHub's own signature (`X-Hub-Signature-256`) before it looks at anything else, and forwards only to the install that installation is linked to. |
| Send a forged or replayed relayed event to an install | The install checks the relay signature (time, delivery id, body, and from `v2` the source and event headers), refuses anything older than 5 minutes, and takes each delivery id once in its own database. If it can't record the delivery id, it refuses the event and the relay tries again. |
| Forge a Jira event through the relay | The install checks the Forge app's own secret, which CoderHelm forwards but doesn't decide. |
| Get events or tokens for someone else's GitHub organization | A link needs a GitHub sign-in that sees that installation of CoderHelm's app (another app's installation never counts), and a press of **Link** on a CoderHelm page that names the install's address and can't be shown inside another page. One installation links to one install, never to one a hosted team already uses. Tokens reach only the repositories the person who linked it can push to, so read access to one repository can't become write access to all. |
| Mint a wider token | The broker asks GitHub for the token with an explicit list of repositories and permissions, never more than the link allows and never more than the app's own permissions. GitHub refuses anything outside that list. Tokens last at most an hour. |
| Take over someone's Notion, Confluence or Google Drive sign-in | The sign-in is finished in a holding place of its own and becomes the install's only when the install redeems the one-time code with its secret and the PKCE verifier (S256), at the return address it registered. Starting sign-ins is limited per address. |
| Send people somewhere else after linking or signing in | Return addresses are compared host by host against the install's own CloudFront address and name (and the docs install guide); anything else goes to the install's settings page. Sign-in returns must be the install's own callback page, over HTTPS, with no port or user name. |
| Make CoderHelm's servers call something inside AWS | CoderHelm calls an install only at the CloudFront address it proved at registration, which never changes afterwards. Those calls never follow a redirect and read at most 16 KB of the answer. CloudFront's own names can't be pointed at other addresses. |
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

- A link made before the **Link** confirmation and the push-permission limit existed keeps the repositories it had. Link it again to bring it under both.
- Someone who controls CoderHelm's hosted side could mint tokens for linked installations, read relayed events and act as the relay. Choose your own GitHub App and apps (fully private) if that is outside what you accept.
- Install requests carry no one-time number, so someone who could break HTTPS could repeat one within its 5 minutes. They would already see the same traffic, tokens included.

## Reporting a problem

Report security issues privately to the support address in the CoderHelm repository rather than in a public issue.
