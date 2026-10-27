---
title: Email
description: How CoderHelm sends email from your AWS account with Amazon SES, and how to leave the SES sandbox.
order: 7
---

A self-hosted install sends the emails it needs, and nothing else until you turn it on. It sends them through Amazon SES in your account, from `email.from`, or, if you choose, through CoderHelm's mail service (see [Send through CoderHelm](#send-through-coderhelm)). It never emails a CoderHelm address.

## Which emails it sends

**Settings, Emails** (install admins) lists them all.

| Email | Kind | Why |
|---|---|---|
| Sign-up codes and password resets | Essential | Nobody can verify their address or get back in without them. Amazon Cognito makes, sends and checks them inside your install. |
| Team invites | Essential | The only way an invited person learns how to join. |
| Plugin approval requests | Essential | A named approver must approve a plugin draft before it publishes. |
| Knowledge source sign-in alerts | Essential | Tells admins a knowledge source stopped syncing. |
| Token limit warnings | Essential | Tells the team before work stops at the monthly limit. Each person can turn these off. |
| Support ticket updates | Essential | Replies and status changes on tickets someone filed. |
| Webhook endpoint turned off | Essential | Tells a team's admins when CoderHelm stopped sending to an endpoint that failed for days. |
| Two-factor lockout alerts | Essential | Tells admins and owners when someone's two-factor sign-in was paused after too many wrong codes. |
| Update notices | Essential | New releases, finished or failed updates, address changes, to install admins. |
| Test email | Essential | Only when an admin presses **Send test email** in Setup. |
| Weekly summary | Optional, **off** | The Monday email with each team's week. |
| Welcome email | Optional, **off** | A short hello after someone signs up. |

Optional emails stay off until an install admin turns them on in **Settings, Emails**; no redeploy is needed. `integrations.weeklySummary: true` in the config starts the summary on instead. Each person can still turn emails off for themselves in **Settings, Notifications** or with an email's unsubscribe link. The hosted CoderHelm service is unchanged: its weekly summary goes to everyone who hasn't unsubscribed.

## The identity

The deploy creates an SES identity for `email.identity` (by default the domain of `email.from`):

- **A domain in the same Route 53 zone as your CoderHelm domain:** its DKIM records are created for you, and it verifies in a few minutes.
- **Any other domain:** SES shows three DKIM CNAME records in the console (**SES, Identities, your domain**). Add them at your DNS provider.
- **One address** (`"identity": "coderhelm@example.com"`): SES emails it a confirmation link. Good for trying CoderHelm out.
- **null:** you manage the identity yourself.

The install guide's **Optional setup** list and **Settings, then Setup** check both: the identity (`email_identity`) and the sandbox (`email_sandbox`), each with the console link pre-filled for your region.

## Leaving the SES sandbox

New AWS accounts are in the SES sandbox: email only reaches verified addresses, at most 200 a day. Request production access in the SES console (**Account dashboard, Request production access**), saying CoderHelm sends transactional email to your own team. The doctor (`coderhelm-selfhost doctor`) and **Settings, then Setup** tell you which mode you are in and print the DKIM records to add.

## Sign-up and password emails

Sign-up codes and password resets come from Amazon Cognito. Until `email.cognitoUsesSes` is true, Cognito sends them from its own address (`no-reply@verificationemail.com`), limited to 50 a day. Once your SES identity shows **Verified**, set:

```json
"email": { "from": "coderhelm@example.com", "cognitoUsesSes": true }
```

and deploy again. Turning it on before the identity is verified makes the deploy fail, because Cognito checks the identity.

## Templates

The email templates are created by the email stack with your URLs filled in. `email.logoUrl` and `email.privacyUrl` change the header logo and the footer link.

## Links in emails and messages

Every link CoderHelm sends from your install points at your install: emails (run results, invites, approvals, the weekly summary and its unsubscribe link), Slack and Teams cards, Jira comments and GitHub comments all use your own address. The header logo loads from your install (`<your address>/favicon.svg`) unless you set `email.logoUrl`, so opening an email never calls CoderHelm's servers. Support and "reply to unsubscribe" go to `email.supportInbox` (by default `email.from`), never to CoderHelm support. The only coderhelm.com links left are the public docs on docs.coderhelm.com.

If the install doesn't know its own address (no `APP_URL` and `API_URL`, and the CloudFront address not yet readable from SSM), links stay relative rather than pointing anywhere else, the services log an error at start, and **Settings, then Setup** fails **Web address answers** until a deploy sets it.

## Send through CoderHelm

If you'd rather not set up SES (verifying a sender and leaving the sandbox), an install registered for [hybrid](./hybrid.md) can send its email through CoderHelm's mail service instead. Choose **Through CoderHelm** in **Settings, Emails**, or in the install guide's **Optional setup**, or set `"email": { "transport": "coderhelm" }`. It is off unless you pick it, and it applies to every email the install sends (invites, approvals, alerts, update notices, and the optional ones you turned on). Sign-up codes stay with Amazon Cognito inside your install: CoderHelm never sees them.

If CoderHelm's mail service can't be reached, the email fails and shows in **Deliveries** and **Settings, Setup**. Nothing is sent another way instead.

**What CoderHelm sees:** each recipient's address, the template id and its parameters (names, PR titles and run links in the weekly summary). It keeps only delivery metadata after sending: the template id, a shortened address (`d***@example.com`) and whether it was delivered, for 3 days. It never keeps the email itself.

**How it works:**

- Your install sends a template id and its parameters, signed with its registration secret, never a subject or HTML. CoderHelm writes the email from its own templates.
- Mail comes from `notifications@notify.coderhelm.com` with your install's address as the name (`acme.coderhelm.com via CoderHelm`), and no Reply-To. The footer names your install and its admin.
- Every link in the email goes to your install's own verified address: its CloudFront address, its CoderHelm name, or its own domain once CoderHelm has reached your install over HTTPS on that name (DNS and certificate working). Your install asks for that by itself the first time it needs it, and the proof counts for a week at a time.
- It only sends to your install's people and pending invites, listed by an install admin, at most 50 new addresses a day, 30 emails a minute and 500 a day.
- An address that bounces or marks an email as spam gets nothing more. Too many bounces or complaints pause email for your install, and your install admins are told.

See [Threat model](./hybrid.md#email-threat-model) for what each of these protects against.
