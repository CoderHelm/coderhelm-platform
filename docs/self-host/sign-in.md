---
title: Sign-in and people
description: Who may sign up, email and password with passkeys and two-factor codes, Google sign-in, GitHub accounts, install admins and team roles.
order: 6.5
---

Accounts live in a Cognito user pool in your AWS account. Nobody can sign up unless the config lets them.

## Who may sign up

| Setting | Who | Where |
|---|---|---|
| `admins` | The install's admins: they may always sign up, and see the admin pages (teams, failed events, support, Settings, then Setup and System) | The Launch Stack's **Your email**, or the config |
| `signup.domains` | Anyone with an email at these domains | The Launch Stack's **Who else may sign up** (one domain), or the config |
| `signup.emails` | These addresses | The config |
| Invites | Anyone a team admin invites | **Settings, People, Invite** (no config) |

Sign-up is closed to everyone else. Change `signup` in the config and deploy, or invite people from the dashboard. The install guide's **Optional setup** list links to the invite page.

## Email and password, passkeys and two-factor codes

Always on. People confirm their email with a code, then can add a passkey or an authenticator app code in their own settings. Sign-up and password emails come from Amazon Cognito until `email.cognitoUsesSes` is on (see [Email](./email.md#sign-up-and-password-emails)); Cognito's own address is limited to 50 emails a day.

## Sign in with Google

Optional, and it needs your own domain (Google requires a fixed sign-in address; see [Domains](./domains.md)).

1. In Google Cloud, **APIs and services, Credentials, Create credentials, OAuth client ID**, type **Web application**.
2. Add the authorized redirect URI `https://<sign-in domain>/oauth2/idpresponse` (the deploy's `SignInDomain` output shows the domain).
3. Add `google_client_id` and `google_client_secret` to the install's secret (`coderhelm/<stage>/secrets` in Secrets Manager, **Retrieve secret value, Edit**).
4. Set `"integrations": { "googleSignIn": true }` in the config and deploy.

Settings, then Setup checks it (`google_sign_in`): off, on with its keys, or on without them (the button fails until the keys are there). The install guide shows it in **Optional setup**.

## GitHub accounts

People connect their GitHub account in their own settings, so pull requests and reviews are credited to them. It uses the GitHub App's own OAuth client, which the GitHub App step stores; nothing to set up.

## Single sign-on (SAML, OIDC)

Not available yet: CoderHelm's sign-in page offers email and password and Google. The Cognito user pool is in your account, so an identity provider added there by hand is not shown on the sign-in page and isn't supported.

## Install admins and team roles

- **Install admins** (`admins`) see the whole install: every team, the failed events queue, support tickets, and the Setup and System pages. They must have an email CoderHelm verified itself.
- **Team roles** (viewer, member, admin, owner, and custom roles) are set per team in **Settings, People** and **Settings, Roles**.
- **Support access:** a team admin can let the install admins see the team's workspace read-only for a fixed time, with every read in the team's audit log (**Settings, Support access**).
