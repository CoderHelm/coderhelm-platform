---
title: Sign-in and people
description: One organization per install, who may join (invites and allowed email domains), required two-factor sign-in, Google sign-in, GitHub accounts, install admins and team roles.
order: 6.5
---

Accounts live in a Cognito user pool in your AWS account. A self-hosted install is for **one organization**: one team, owned by the first admin. Nobody can make another team, and nobody can sign up on their own.

## Who may join

People join in one of two ways:

- **An invite.** An admin invites them by email in **Settings, Team**. The invite works for 30 days; resend it from **Pending invites**.
- **An allowed email domain.** Admins can let anyone with an email at a domain join, such as `example.com`, in **Settings, Sign-in**. They join as members. Public email services (Gmail, Outlook and the like) can't be allowed: that would let anyone in.

Everyone else is refused, on every way in: email sign-up, Google, GitHub, and the Cognito API itself. A pre-sign-up trigger in the user pool checks the same rules as CoderHelm, so even a direct Cognito `SignUp` call with the app's client id is refused.

The first admin is the address you gave the install (the Launch Stack's **Your email**, or `admins` in the config). Their first sign-in makes the organization; until then, only they can sign up.

### At install time

The install guide's **Let people from your company join** starts as **Only people I invite**. Pick **Anyone with an email at** and type your company's domain to allow it from the start. The config's `signup.domains` and `signup.emails` do the same: on first boot the domains become the allowed domains and the addresses become invites. After that, admins change both in **Settings** (the config is not read again).

| Setting | First boot | After |
|---|---|---|
| `admins` | The first admin (the first one to sign in makes the organization); the others are invited as admins | Install admins (see below) |
| `signup.domains` | Allowed email domains | **Settings, Sign-in** |
| `signup.emails` | Invited as members | **Settings, Team** |
| `signup.requireTwoFactor` | Two-factor sign-in required | **Settings, Sign-in** |

An install from before this change keeps working: its oldest team becomes the organization at the next sign-in, its config list is carried over as above, and people on other teams need an invite.

## Two-factor sign-in

Anyone can turn on two-factor sign-in for themselves in **Settings, Security**. Admins can require it for everyone: **Settings, Sign-in, Require two-factor sign-in**. Turn it on for yourself first; the switch asks for that, so you can't lock yourself out.

With it on:

- **Everyone** must set up an authenticator app (any TOTP app: 1Password, Google Authenticator, Authy and so on) before they can use CoderHelm. At their next sign-in they see a setup screen with a code to scan, and nothing else works until they enter a code from the app.
- **People who sign in with a password** use Cognito's own two-factor codes: Cognito asks for the code at every sign-in.
- **People who sign in with Google or GitHub** don't pass through Cognito's two-factor step, so CoderHelm asks for its own authenticator-app code right after Google or GitHub. Their secret is sealed with the install's KMS key. No sign-in path skips the code.
- **Sessions from before the switch** are checked at their next request: someone already signed in without a code is sent to the setup screen, or asked to sign in again with their code.
- **API keys** are not people and keep working. **Settings, Sign-in** lists them, so you can revoke any you don't want.

**Settings, Sign-in** shows who has set it up. If someone loses their phone, an admin can reset their two-factor sign-in after checking it's really them (a video call, say). The admin writes how they checked; the reset and the reason go in the log under **Settings, Roles**. The person sets it up again at their next sign-in. Only an owner can reset an admin's or another owner's. Nobody can reset their own.

### Wrong codes

Every place CoderHelm checks a code counts wrong ones against the person, by email, across every way in: the code after Google or GitHub, the code after a password, and turning the app code off.

- **Ten wrong codes in 15 minutes** pause that person's sign-in with a code for 15 minutes. Each pause after that is twice as long, up to a day. After a quiet day the count starts over. A right code clears the wrong ones so far.
- **One sign-in through Google or GitHub** allows five wrong codes; after that the person starts again from the sign-in screen.
- **The person** sees a plain sentence saying how long the pause lasts.
- **Admins** get an email, and a line in the log under **Settings, Roles**. If they lost their phone, check it's them and reset; if it wasn't them, someone may know their password or have their Google or GitHub account.

Cognito also limits wrong passwords on its own.

### Sessions that end

A person's existing sessions end, everywhere, when:

- an admin resets their two-factor sign-in;
- an admin removes them from the team;
- their role changes;
- they turn their own two-factor sign-in off (the session they did it from keeps working).

Every request checks the person's record, so the next request from an ended session goes back to the sign-in screen.

Why an authenticator app and not passkeys: Cognito's passkeys need its paid Essentials tier, a fixed sign-in domain (passkeys break when the CloudFront address changes to your own domain) and Cognito's hosted sign-in pages. Authenticator apps work on every install, including one on its free CloudFront address.

## Email and password

Always on. People confirm their email with a code. Sign-up and password emails come from Amazon Cognito until `email.cognitoUsesSes` is on (see [Email](./email.md#sign-up-and-password-emails)); Cognito's own address is limited to 50 emails a day.

## Sign in with Google

Optional, and it needs your own domain (Google requires a fixed sign-in address; see [Domains](./domains.md)). It follows the same rules: only invited people and allowed domains.

1. In Google Cloud, **APIs and services, Credentials, Create credentials, OAuth client ID**, type **Web application**.
2. Add the authorized redirect URI `https://<sign-in domain>/oauth2/idpresponse` (the deploy's `SignInDomain` output shows the domain).
3. Add `google_client_id` and `google_client_secret` to the install's secret (`coderhelm/<stage>/secrets` in Secrets Manager, **Retrieve secret value, Edit**).
4. Set `"integrations": { "googleSignIn": true }` in the config and deploy.

Settings, then Setup checks it (`google_sign_in`): off, on with its keys, or on without them (the button fails until the keys are there). The install guide shows it in **Optional setup**.

## GitHub accounts

People connect their GitHub account in their own settings, so pull requests and reviews are credited to them. Signing in with GitHub follows the same rules as everything else: it lands on the organization only for someone already in it, invited, or at an allowed domain (by the email on their GitHub account). It uses the GitHub App's own OAuth client, which the GitHub App step stores; nothing to set up.

## Single sign-on (SAML, OIDC)

Not available yet: CoderHelm's sign-in page offers email and password, Google and GitHub. The Cognito user pool is in your account, so an identity provider added there by hand is not shown on the sign-in page and isn't supported.

## Install admins and team roles

- **Install admins** (`admins`) see the whole install: the failed events queue, support tickets, and the Setup and System pages. They must have an email CoderHelm verified itself.
- **Team roles** (viewer, member, admin, owner, and custom roles) are set in **Settings, Team** and **Settings, Roles**. Sign-in rules stay with built-in admins and owners; a custom role can't hold them.
- **Support access:** a team admin can let the install admins see the team's workspace read-only for a fixed time, with every read in the team's audit log (**Settings, Support access**).
