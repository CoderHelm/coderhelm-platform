---
title: Hybrid
description: Run CoderHelm in your AWS account and use CoderHelm's address, GitHub App and integration apps where that's easier. What goes through CoderHelm's servers, exactly.
order: 1.7
---

A **hybrid** install runs entirely in your AWS account, like any self-hosted CoderHelm, and borrows up to four things from CoderHelm's hosted side so you don't have to set them up yourself:

| Part | Instead of | What you get |
|---|---|---|
| **A CoderHelm address**, `yourname.coderhelm.com` | buying a domain and adding DNS records | a real address with HTTPS in a few minutes, set up end to end |
| **CoderHelm's GitHub App**, through the event relay | creating and keeping your own GitHub App | install one app on your organization; events and tokens arrive at your install |
| **CoderHelm's Jira app**, through the event relay | setting up your own Jira app | link your Jira site once; its events arrive at your install, without the app's secret |
| **CoderHelm's integration apps** (Notion, Confluence, Google Drive) | registering an app with each provider | sign in with one click, as on coderhelm.com |

Each part is a separate choice, at install time (the [install guide](/self-host/install/)) or later in **Settings, System, Hybrid**. **Fully private** (your own domain or the AWS address, your own GitHub App and apps) stays the default and sends nothing to CoderHelm.

## Trust model

Your code, runs, plans, reviews, memory, knowledge and secrets stay in your AWS account in every setup. What CoderHelm's servers see depends only on the parts you turn on:

| Part | What passes through CoderHelm | What CoderHelm keeps |
|---|---|---|
| CoderHelm address | nothing: visitors go straight to your CloudFront | the name, your CloudFront address and the two DNS records (the certificate itself is in your account) |
| GitHub App relay | each event's payload, in transit: CoderHelm checks GitHub's signature and forwards it re-signed | the payload only while a forward is retried (at most 24 hours, then deleted); delivery records (ids, event types, status, timings) for 14 days |
| Token broker | the short-lived installation token it mints for your install (scoped to the repositories you chose, never more than the app's permissions) | no tokens; a record of each mint for 14 days |
| Jira app relay | each issue event, in transit, unchanged | as for GitHub. The Forge app's secret is checked by your install, so CoderHelm can't make an event up |
| Jira site link | each event of the linked site (the Jira app's and a plain Jira webhook's), in transit, without the app's secret; your install's calls to the site's Jira app (comments, tickets, release notes), in transit, through CoderHelm's Jira proxy | as for GitHub; the link itself (which team's site, who linked it, when). The site's Jira app secret and links stay with CoderHelm |
| Notion, Confluence, Google Drive | the sign-in, once | the provider sign-in, encrypted with a key only CoderHelm's services can use, so it can hand your install access tokens. Disconnecting revokes it. Notion's tokens don't expire by themselves: a copy your install holds stays valid until Notion's own integration page removes it |
| Email through CoderHelm (opt-in, [Email](./email.md#send-through-coderhelm)) | each email's recipient address, template id and parameters (names, PR titles and run links in the weekly summary), in transit; CoderHelm writes the email from its own templates | delivery metadata only: template id, a shortened address and the status, for 3 days. Salted hashes of the addresses your install listed, and your install admins' addresses sealed with a key only CoderHelm's services can use, to tell them if email pauses |
| Sharing with support (opt-in) | metadata of failed deliveries, and payload previews only if you also choose that | for 14 days, so support can see what failed |

CoderHelm never stores your code, never sees your model calls or your AWS credentials, and can't sign in to your install.

The install guide page itself (docs.coderhelm.com) counts anonymous page views with Google Analytics: the page's address without anything after `?` or `#`, the step reached by name, and the host of a link you open. Nothing you type, including the name you check, and nothing about your install is sent to Google. Your install sends nothing to Google Analytics.

How the pieces are protected:

- **Registration.** Your install registers once and gets an id, a secret and a relay key. Both keys are derived from a key on CoderHelm's side, so CoderHelm stores no copy of them; your install keeps them in its own Secrets Manager secret.
- **Proof of ownership.** Before a name or an app is linked, CoderHelm asks your install's CloudFront address to answer a challenge with its secret. Names only ever point at a verified install's own CloudFront distribution.
- **Signed both ways.** Every call from your install is signed (`X-CoderHelm-Signature`), and every relayed event is signed with your relay key (`X-CoderHelm-Relay-Signature: t=,d=,v1=,v2=`; `v2` also covers the source and event headers, and an event without it is refused). Your install refuses anything older than 5 minutes and any delivery id it has already taken, and refuses an event it couldn't record as taken.
- **Linking proves both sides.** Linking CoderHelm's GitHub App needs your install (its setup key in the guide, or an install admin in Settings), a GitHub sign-in that shows the installation is yours, and a press of **Link** on a CoderHelm page that names your install's address. A GitHub redirect or a link someone sends you can't link anything by itself.
- **Scoped tokens.** The broker mints tokens only for installations linked to your install, narrowed to the repositories you chose and, within those, to the repositories the person who linked it can push to on GitHub. GitHub itself enforces both: they are part of the token request.
- **Sign-ins are yours only once your install takes them.** A Notion, Confluence or Google Drive sign-in is held apart until your install redeems it with its secret and its PKCE verifier, so nobody else can put their account in place of yours.
- **Revocable.** CoderHelm can turn an install's access off (its name and records go, its GitHub links are removed, the relay and broker refuse it, and the same CloudFront can't register again), and every action is in an audit log.
- **Email only as itself.** See [Email threat model](#email-threat-model) below.
- **Same-site safety.** Addresses under coderhelm.com are the same site as app.coderhelm.com and api.coderhelm.com to a browser, so CoderHelm doesn't rely on that: its cookies are host-only (`__Host-`, no Domain attribute), its CORS lists exact origins, and the API refuses any change sent with cookies from another page on the same site. An install's pages can't read or change coderhelm.com sessions.

The full threat model, with each attack and what stops it, is in [Security model](./security.md#hybrid-threat-model).

## How an install knows a message is from CoderHelm

Everything CoderHelm's servers send a hybrid install carries CoderHelm's own signature: every relayed GitHub and Jira event, and every answer to the install's own calls (GitHub tokens, the CoderHelm address and its records, email, links, registration).

- **CoderHelm signs with one Ed25519 key kept in AWS KMS.** The private key never leaves KMS, and only the two CoderHelm services that send to installs may use it.
- **Your install checks with CoderHelm's public keys, which come inside the release it runs.** The release is signed, so the trust comes from that signature, not from anything sent over the network. Settings, System, Hybrid lists the key ids and fingerprints your release trusts, to compare with the ones on each GitHub release.
- **The signature covers** the install the message is for, the time, the delivery or call id, where it came from, its type and a hash of its content. A message for another install, an older copy, an altered body or an answer to another call is refused.
- **Once a release pins CoderHelm's keys, anything without CoderHelm's signature is refused**, and the Deliveries page shows "Refused: not signed by CoderHelm".

**Changing keys.** A release may trust two keys at once. CoderHelm adds its next key in a release before it starts signing with it, and drops the old key in a later release.

**Switching on.** The first releases with this work as before (the relay key is the check) while CoderHelm signs everything alongside it. The release that pins CoderHelm's first key turns the check on: from then on both the relay key's signature and CoderHelm's are required.

## A CoderHelm address

Pick a name in the guide or in **Settings, System, Domain**. The page checks it as you type: lowercase letters, digits and hyphens, 3 to 40 characters, not reserved (CoderHelm's own hosts such as `app`, `api`, `docs` and `auth`, and names such as `admin`, `support` or `status`), not reading as CoderHelm's own (no `coderhelm` in it, even spelled with digits, and no part such as `app`, `login`, `billing` or `support`, as in `app2` or `acme-login`), and not taken.

Then everything happens by itself:

1. CoderHelm reserves the name and points it at a "being set up" page.
2. Your install requests a certificate for the name in your AWS account (ACM). CoderHelm writes the certificate's validation record into the coderhelm.com zone.
3. AWS issues the certificate, usually within 30 minutes. Your install moves to the name (about 10 minutes).
4. CoderHelm points the name at your CloudFront. Done: the guide and Settings show each step's check as it happens.

The provisioning API can only create records under your name (the name itself and `_<token>.<name>`), never overwrite a record it didn't write, and never touch a reserved name; its AWS permissions say the same. An address nobody has claimed shows CoderHelm's "There's no CoderHelm here yet" page (404). A claimed address that is still being set up shows "This CoderHelm is still being set up". Once live, the address goes straight to your install, so if your install is down, the page is your CloudFront's.

### How a CoderHelm address stays yours

- **Only your verified install gets one.** CoderHelm points a name at a CloudFront address only after that address answers an ownership challenge signed with your install's registration secret, and it checks again before every change.
- **The certificate lives only in your AWS account.** CoderHelm writes the validation record for it; the private key never leaves your ACM. Every certificate for coderhelm.com and its names comes from Amazon.
- **Records go when you leave.** Uninstalling or **Leave hybrid** releases the name and removes every record CoderHelm wrote for it.
- **CoderHelm checks every name every 15 minutes.** It compares the records under your name with the ones it wrote, then asks your install to prove itself twice with a fresh challenge: once at its CloudFront address and once at the name. When the CloudFront address answers as anyone else (or as nobody) three times in a row, or doesn't answer at all for a whole day, or a record under the name wasn't written by CoderHelm, the name goes back to the "still being set up" page. The name stays reserved for your install: nobody else can claim it. **Settings, System, Domain** shows what happened; Try again there sets it up again. A network hiccup on its own never counts.
- **Records CoderHelm didn't write are removed or reported.** A leftover record under a name nobody holds, in a shape CoderHelm writes, is removed; anything else is reported to CoderHelm's operators and left alone.
- **Only Amazon may issue certificates.** For the coderhelm.com zone, CoderHelm's recommended CAA records allow only Amazon (`amazon.com`, `amazontrust.com`, `awstrust.com`, `amazonaws.com`), so no other certificate authority will issue one for a name, even if someone could answer its checks.

A name can change five times a day. [Uninstalling](./uninstall.md#hybrid-installs), from the guided uninstall or `npx github:CoderHelm/coderhelm-platform uninstall`, releases it first, unlinks the install's GitHub installations and Jira site so their events go back to CoderHelm hosted, and turns its registration off; so does **Leave hybrid** in Settings.

## CoderHelm's GitHub App

1. In the guide (or **Settings, System, Hybrid**), choose **Use CoderHelm's GitHub App**.
2. Press **Install on GitHub**, pick the organization and repositories, and sign in to GitHub when it asks. That sign-in proves the installation is yours.
3. CoderHelm shows which install the organization is about to be linked to. Check the address and press **Link**.
4. You come back linked. Tokens reach only the repositories you can push to on GitHub; someone with more access can link it again to widen that. From then on GitHub sends events to CoderHelm, which forwards each one to `https://<your CloudFront>/webhooks/relay`, and your install asks the broker for tokens when it works on a repository.

A forward that fails is retried with growing waits for about 24 hours. Both sides show it on their **Deliveries** page with each attempt, the answer and the next retry. Choose your own GitHub App again any time; signing in with GitHub on your install needs your own app.

## Jira through CoderHelm

A team that uses hosted CoderHelm with CoderHelm's Jira app can move that Jira site to its hybrid install without touching Jira: CoderHelm keeps receiving the site's events and forwards them to the install, the same way it forwards GitHub's.

**In the install guide**, **Use CoderHelm's Jira app** (under What CoderHelm sets up for you) is on by default in the hybrid setup and can be turned off. With it on, the guide turns on **Link through CoderHelm** in your install as soon as it registers (with its setup key), and after Claude answers it shows a **Jira** step: sign up as the admin if you haven't, type your Jira site's address, and **Open Settings, Integrations, Jira** opens your install there with the site filled in (`/settings/integrations/jira/?link=coderhelm&site=<site>#link`). Steps 1 to 3 below follow; the guide checks `hybrid_jira` every few seconds until it's `ok`. **Link later in Settings** skips the step.

1. On your install, open **Settings, Integrations, Jira**, turn on **Link through CoderHelm**, type your Jira site's address (`example.atlassian.net`) and press **Link**. Your install asks CoderHelm (signed, install admins only) for a one-hour link for that site, and shows a **confirmation code** and **Open CoderHelm to confirm**.
2. A CoderHelm page opens (app.coderhelm.com, **Settings, Integrations, Jira**). Sign in as an **admin of the hosted team that runs CoderHelm's Jira app on that site**. The page names your install's address, when it registered with CoderHelm, and the site, and links nothing until you type the confirmation code your install shows and press **Link**: a link someone else got hold of can't be confirmed without it. It refuses when the team's site isn't the one your install asked for, when the team doesn't run the Jira app on a site (a plain webhook alone isn't proof), when the site is already linked to another install, or when your install already has another team's site. Support views and API keys can't link.
3. You come back to your install linked.

From then on CoderHelm checks each event of that site first, exactly as it does for a hosted team (the Jira app's secret, or a plain webhook's signature), then forwards it to `https://<your CloudFront>/webhooks/relay` with source `jira`, **without the app's secret**, and stops: **CoderHelm never handles an event it relays**, so nothing runs twice. Your install refuses the forward unless **Link through CoderHelm** is on there. Forwards are signed, retried for about 24 hours, held only while retrying and recorded exactly like GitHub's, and they show on **Deliveries** beside GitHub's.

**Your install never gets the site's Jira app secret or its Jira app links.** Its Jira settings point at CoderHelm's Jira proxy instead: one link per action (comment, create a ticket, publish release notes, list projects, read issues, Confluence), each with a token for your install, that action and that link (linking again makes new tokens). CoderHelm calls only the site's own Jira app links on Atlassian's Forge host, never follows a redirect, and only while the link stands and your install is active. Your install's own Jira app settings, if it had any, are kept aside and come back when you unlink.

**Unlink** from either side: **Unlink** (or turning **Link through CoderHelm** off) on your install, or **Unlink** on the hosted team's Settings, Integrations, Jira. Leaving hybrid and CoderHelm revoking the install unlink too. Both records go together; CoderHelm handles the site's events itself again at once, the proxy refuses your install's calls, and retries still queued for your install stop. If your install is turned off while linked, CoderHelm handles the site's events itself rather than dropping them.

The **Jira app relay** above is the other way round: a Jira site whose own Jira app is set up with your install's id, without a hosted team.

The Setup checks show the link as `hybrid_jira` (in Settings, Setup and the install guide's `/api/setup/status`): `no_link` (todo) until it is linked, `syncing` (wait) while your install fetches the proxy settings, `ok` once linked. Linking needs an install admin, so `/api/setup/status` keeps reporting this one check (its code only) after someone has signed up, while Link through CoderHelm is on.

## Integrations

| Integration | Through CoderHelm's app | Notes |
|---|---|---|
| GitHub | yes | relay and token broker |
| Jira (Forge app) | yes | install CoderHelm's Jira app and enter your install id as the team id, or link a hosted team's site ([Jira through CoderHelm](#jira-through-coderhelm)) |
| Notion, Confluence, Google Drive | yes | one-click sign-in through CoderHelm; tokens brokered |
| Slack and Teams notifications | not needed | your install posts to the incoming webhook you paste |
| Figma, Linear, Sentry, Sanity and other sign-in apps | not needed | your install registers itself with each provider |
| Slack and Teams commands | no | there is no shared Slack or Teams app: each team uses its own app's signing secret, as today |
| Sign in with GitHub | no | it would hand your people's identities to CoderHelm; use email and password |
| Sign in with Google | no | a self-hosted install has no Google sign-in, through CoderHelm or its own ([Sign-in and people](./sign-in.md#no-google-sign-in)) |

## Turn hybrid off

**Leave hybrid** in Settings releases the name, unlinks CoderHelm's apps and a linked Jira site, and revokes the sign-ins. To make sure an install never talks to CoderHelm, set `"hybridUrl": null` in `coderhelm.config.json` and deploy.

## For CoderHelm's operators

The hosted side runs in the hosted deployment's API stack when `hybrid.zone` is set in its config (the parent zone is configurable, so moving customer addresses to a separate domain later is a config change). It needs `hybrid_master_key` (32 or more random characters) in the hosted secret, the callback URL `https://api.coderhelm.com/auth/github/callback/hybrid` on CoderHelm's GitHub App, and the GitHub App's setup page (the dashboard's Settings, GitHub) to forward `state=hyb_…` returns. A hosted team links its Jira site with `GET` and `POST /api/hybrid/jira-link/:state` (team admins, from the dashboard's Settings, Integrations, Jira, which opens with `?hybrid_link=hyj_…`); sees and removes the link with `GET` and `DELETE /api/hybrid/jira-link`; installs call `POST /hybrid/jira/link-start`, `DELETE /hybrid/jira/link` and `POST /hybrid/jira/config` (signed), and the Jira proxy at `POST /hybrid/jira/trigger/<install>/<action>/<token>`. Platform admins see every install and the audit log (`jira_link_start`, `link_jira`, `unlink_jira`, `jira_link_refused`, `jira_link_declined`) at `GET /api/admin/hybrid`, and revoke one with `POST /api/admin/hybrid/:install_id/revoke`.


## Email threat model

What CoderHelm's mail service refuses, and how. Each line has a test (`services/common/src/mail`).

| Threat | What stops it |
|---|---|
| Another install, or someone without the install's secret, sends as your install | Every request is signed with the install's own secret (derived per install on CoderHelm's side), checked within 5 minutes, with a request id that is refused the second time. Only a registered, verified, not revoked install can send; revoking stops it at the next request. |
| An email made to look like it comes from CoderHelm support or security | No subject, HTML, sender or Reply-To can be sent: only a template id and parameters. Subjects are fixed and never name CoderHelm ("You're invited to your team's workspace"). Parameters are plain text, escaped, length-limited, and refused when they read as CoderHelm itself ("CoderHelm support", "CoderHelm security"). The sender name is built only from the install's verified address, and a name that reads as CoderHelm staff (`support`, `billing`, `c0derhe1m` and other look-alikes) falls back to the CloudFront address. Every email's footer names the install. |
| A phishing link | Link parameters must be `https://` links to the install's own verified address (its CloudFront address, its CoderHelm name, or its own domain while CoderHelm's HTTPS proof on it is under a week old). An own domain that reads as CoderHelm (`c0derhelm.io`, `coderhelm-security.com`, or a word like `support` or `login` in the domain name itself) is never used; a first label of your own choosing is fine (`coderhelm.example.com`, `app.example.com`). CoderHelm checks the domain only at public addresses (never private, loopback, link-local or metadata ones) and connects to the address it checked. Links, addresses and look-alike domains in text parameters are removed before the email is written. |
| Mail to people outside the install | Only listed addresses: the install's admins, people and pending invites. Only an install admin can list an invite, at most 50 new addresses a day. The first listing (the admin turning email on, with the install's people) happens once per install, ever: removing admins never opens it again. CoderHelm's own addresses are never a recipient. |
| Spam or a burst | 30 emails a minute and 500 a day per install, 100 a day for its first 14 days after it proves its address. Bounced and complaining addresses get nothing more; too many bounces or complaints pause the install's email and its admins are told. |
| Spoofing CoderHelm's sending domain | Mail comes from its own subdomain, `notify.coderhelm.com`, with Easy DKIM, SPF for a custom MAIL FROM and DMARC `p=reject`, so it can't harm the main domain's reputation and can't be forged from it. |
| Secrets in mail | Sign-up codes never leave the install. Parameters marked secret (signed approval and unsubscribe links) are never logged or stored; nothing is kept but delivery metadata. |
