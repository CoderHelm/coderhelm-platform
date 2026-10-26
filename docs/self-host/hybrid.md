---
title: Hybrid
description: Run CoderHelm in your AWS account and use CoderHelm's address, GitHub App and integration apps where that's easier. What goes through CoderHelm's servers, exactly.
order: 1.7
---

A **hybrid** install runs entirely in your AWS account, like any self-hosted CoderHelm, and borrows up to three things from CoderHelm's hosted side so you don't have to set them up yourself:

| Part | Instead of | What you get |
|---|---|---|
| **A CoderHelm address**, `yourname.coderhelm.com` | buying a domain and adding DNS records | a real address with HTTPS in a few minutes, set up end to end |
| **CoderHelm's GitHub App**, through the event relay | creating and keeping your own GitHub App | install one app on your organization; events and tokens arrive at your install |
| **CoderHelm's integration apps** (Jira, Notion, Confluence, Google Drive) | registering an app with each provider | sign in with one click, as on coderhelm.com |

Each part is a separate choice, at install time (the [install guide](/self-host/install/)) or later in **Settings, System, Hybrid**. **Fully private** (your own domain or the AWS address, your own GitHub App and apps) stays the default and sends nothing to CoderHelm.

## Trust model

Your code, runs, plans, reviews, memory, knowledge and secrets stay in your AWS account in every setup. What CoderHelm's servers see depends only on the parts you turn on:

| Part | What passes through CoderHelm | What CoderHelm keeps |
|---|---|---|
| CoderHelm address | nothing: visitors go straight to your CloudFront | the name, your CloudFront address and the two DNS records (the certificate itself is in your account) |
| GitHub App relay | each event's payload, in transit: CoderHelm checks GitHub's signature and forwards it re-signed | the payload only while a forward is retried (at most 24 hours, then deleted); delivery records (ids, event types, status, timings) for 14 days |
| Token broker | the short-lived installation token it mints for your install (scoped to the repositories you chose, never more than the app's permissions) | no tokens; a record of each mint for 14 days |
| Jira app relay | each issue event, in transit, unchanged | as for GitHub. The Forge app's secret is checked by your install, so CoderHelm can't make an event up |
| Notion, Confluence, Google Drive | the sign-in, once | the provider sign-in, encrypted with a key only CoderHelm's services can use, so it can hand your install access tokens. Disconnecting revokes it. Notion's tokens don't expire by themselves: a copy your install holds stays valid until Notion's own integration page removes it |
| Sharing with support (opt-in) | metadata of failed deliveries, and payload previews only if you also choose that | for 14 days, so support can see what failed |

CoderHelm never stores your code, never sees your model calls or your AWS credentials, and can't sign in to your install.

How the pieces are protected:

- **Registration.** Your install registers once and gets an id, a secret and a relay key. Both keys are derived from a key on CoderHelm's side, so CoderHelm stores no copy of them; your install keeps them in its own Secrets Manager secret.
- **Proof of ownership.** Before a name or an app is linked, CoderHelm asks your install's CloudFront address to answer a challenge with its secret. Names only ever point at a verified install's own CloudFront distribution.
- **Signed both ways.** Every call from your install is signed (`X-CoderHelm-Signature`), and every relayed event is signed with your relay key (`X-CoderHelm-Relay-Signature: t=,d=,v1=,v2=`; `v2` also covers the source and event headers). Your install refuses anything older than 5 minutes and any delivery id it has already taken, and refuses an event it couldn't record as taken.
- **Linking proves both sides.** Linking CoderHelm's GitHub App needs your install (its setup key in the guide, or an install admin in Settings), a GitHub sign-in that shows the installation is yours, and a press of **Link** on a CoderHelm page that names your install's address. A GitHub redirect or a link someone sends you can't link anything by itself.
- **Scoped tokens.** The broker mints tokens only for installations linked to your install, narrowed to the repositories you chose and, within those, to the repositories the person who linked it can push to on GitHub. GitHub itself enforces both: they are part of the token request.
- **Sign-ins are yours only once your install takes them.** A Notion, Confluence or Google Drive sign-in is held apart until your install redeems it with its secret and its PKCE verifier, so nobody else can put their account in place of yours.
- **Revocable.** CoderHelm can turn an install's access off (its name and records go, its GitHub links are removed, the relay and broker refuse it, and the same CloudFront can't register again), and every action is in an audit log.
- **Same-site safety.** Addresses under coderhelm.com are the same site as app.coderhelm.com and api.coderhelm.com to a browser, so CoderHelm doesn't rely on that: its cookies are host-only (`__Host-`, no Domain attribute), its CORS lists exact origins, and the API refuses any change sent with cookies from another page on the same site. An install's pages can't read or change coderhelm.com sessions.

The full threat model, with each attack and what stops it, is in [Security model](./security.md#hybrid-threat-model).

## A CoderHelm address

Pick a name in the guide or in **Settings, System, Domain**. The page checks it as you type: lowercase letters, digits and hyphens, 3 to 40 characters, not reserved (CoderHelm's own hosts such as `app`, `api`, `docs` and `auth`, and names such as `admin`, `support` or `status`), not reading as CoderHelm's own (no `coderhelm` in it, even spelled with digits, and no part such as `app`, `login`, `billing` or `support`, as in `app2` or `acme-login`), and not taken.

Then everything happens by itself:

1. CoderHelm reserves the name and points it at a "being set up" page.
2. Your install requests a certificate for the name in your AWS account (ACM). CoderHelm writes the certificate's validation record into the coderhelm.com zone.
3. AWS issues the certificate, usually within 30 minutes. Your install moves to the name (about 10 minutes).
4. CoderHelm points the name at your CloudFront. Done: the guide and Settings show each step's check as it happens.

The provisioning API can only create records under your name (the name itself and `_<token>.<name>`), never overwrite a record it didn't write, and never touch a reserved name; its AWS permissions say the same. An address nobody has claimed shows CoderHelm's "There's no CoderHelm here yet" page (404). A claimed address that is still being set up shows "This CoderHelm is still being set up". Once live, the address goes straight to your install, so if your install is down, the page is your CloudFront's.

A name can change five times a day. Uninstalling with `npx github:CoderHelm/coderhelm-platform uninstall` releases it; so does **Leave hybrid** in Settings.

## CoderHelm's GitHub App

1. In the guide (or **Settings, System, Hybrid**), choose **Use CoderHelm's GitHub App**.
2. Press **Install on GitHub**, pick the organization and repositories, and sign in to GitHub when it asks. That sign-in proves the installation is yours.
3. CoderHelm shows which install the organization is about to be linked to. Check the address and press **Link**.
4. You come back linked. Tokens reach only the repositories you can push to on GitHub; someone with more access can link it again to widen that. From then on GitHub sends events to CoderHelm, which forwards each one to `https://<your CloudFront>/webhooks/relay`, and your install asks the broker for tokens when it works on a repository.

A forward that fails is retried with growing waits for about 24 hours. Both sides show it on their **Deliveries** page with each attempt, the answer and the next retry. Choose your own GitHub App again any time; signing in with GitHub on your install needs your own app.

## Integrations

| Integration | Through CoderHelm's app | Notes |
|---|---|---|
| GitHub | yes | relay and token broker |
| Jira (Forge app) | yes | install CoderHelm's Jira app and enter your install id as the team id |
| Notion, Confluence, Google Drive | yes | one-click sign-in through CoderHelm; tokens brokered |
| Slack and Teams notifications | not needed | your install posts to the incoming webhook you paste |
| Figma, Linear, Sentry, Sanity and other sign-in apps | not needed | your install registers itself with each provider |
| Slack and Teams commands | no | there is no shared Slack or Teams app: each team uses its own app's signing secret, as today |
| Sign in with GitHub | no | it would hand your people's identities to CoderHelm; use email, or Google with your own domain |

## Turn hybrid off

**Leave hybrid** in Settings releases the name, unlinks CoderHelm's apps and revokes the sign-ins. To make sure an install never talks to CoderHelm, set `"hybridUrl": null` in `coderhelm.config.json` and deploy.

## For CoderHelm's operators

The hosted side runs in the hosted deployment's API stack when `hybrid.zone` is set in its config (the parent zone is configurable, so moving customer addresses to a separate domain later is a config change). It needs `hybrid_master_key` (32 or more random characters) in the hosted secret, the callback URL `https://api.coderhelm.com/auth/github/callback/hybrid` on CoderHelm's GitHub App, and the GitHub App's setup page (the dashboard's Settings, GitHub) to forward `state=hyb_…` returns. Platform admins see every install and the audit log at `GET /api/admin/hybrid`, and revoke one with `POST /api/admin/hybrid/:install_id/revoke`.
