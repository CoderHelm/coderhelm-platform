---
title: Sign-in and people
description: One organization per install, how people join (an admin's invite), required two-factor sign-in, GitHub accounts, install admins and team roles.
order: 6.5
---

Accounts live in a Cognito user pool in your AWS account. A self-hosted install is for **one organization**: one team, owned by the first admin. Nobody can make another team, and nobody can sign up on their own.

## Who may join

Admins add people. An admin invites someone by email in **Settings, Team**; the invite email has a link that opens the account form, where they set their password. The invite works for 30 days; resend it from **Pending invites**.

The sign-in page has no open sign-up: it shows **Sign in** only, and says new people come in through an invite. The account form opens only from an invite email's link (`/?invite=1`) or, for the first admin, the installer's link (`/?signup=1`).

An install whose setup listed email domains (`signup.domains`, or **Anyone with an email at** in the install guide) also lets people at those domains join; they appear in **Settings, Sign-in**, where an admin can remove them. New domains can't be added there.

Everyone else is refused, on every way in: email sign-up, GitHub, and the Cognito API itself. A pre-sign-up trigger in the user pool checks the same rules as CoderHelm, so even a direct Cognito `SignUp` call with the app's client id is refused.

The first admin is the address you gave the install (the Launch Stack's **Your email**, or `admins` in the config). Their first sign-in makes the organization; until then, only they can sign up.

### People moved from another CoderHelm

A team moved from CoderHelm's hosted service arrives with its people already on it. Someone who hasn't signed in on the install yet shows **Hasn't set up an account here yet** in **Settings, Team**. An admin opens the row's menu and picks **Send set-up email**: it's the invite email, with the link to set up their account (at most once an hour and five times a week per person). The row then shows when it went. Once they've signed in, it's gone.

### At install time

The install guide's **Let people from your company join** starts as **Only people I invite**. Pick **Anyone with an email at** and type your company's domain to allow it from the start. The config's `signup.domains` and `signup.emails` do the same: on first boot the domains become the allowed domains and the addresses become invites. After that, admins invite people in **Settings, Team** and can remove listed domains in **Settings, Sign-in** (the config is not read again).

| Setting | First boot | After |
|---|---|---|
| `admins` | The first admin (the first one to sign in makes the organization); the others are invited as admins | Install admins (see below) |
| `signup.domains` | Allowed email domains | Shown in **Settings, Sign-in**, where they can be removed |
| `signup.emails` | Invited as members | **Settings, Team** |
| `signup.requireTwoFactor` | Two-factor sign-in required | **Settings, Sign-in** |

An install from before this change keeps working: its oldest team becomes the organization at the next sign-in, its config list is carried over as above, and people on other teams need an invite.

## Two-factor sign-in

Anyone can turn on two-factor sign-in for themselves in **Settings, Security**. Admins can require it for everyone: **Settings, Sign-in, Require two-factor sign-in**. Turn it on for yourself first; the switch asks for that, so you can't lock yourself out.

When you turn it on, everyone who has no second factor yet gets an email saying so, with a link to set it up (Deliveries shows each one). **Settings, Sign-in** shows how many people haven't set it up, before and after.

With it on:

- **Everyone** must set up an authenticator app (any TOTP app: 1Password, Google Authenticator, Authy and so on) before they can use CoderHelm. At their next sign-in they see a setup screen with a code to scan, and nothing else works until they enter a code from the app.
- **People who sign in with a password** use Cognito's own two-factor codes: Cognito asks for the code at every sign-in.
- **People who sign in with GitHub** don't pass through Cognito's two-factor step, so CoderHelm asks for its own authenticator-app code right after GitHub. Their secret is sealed with the install's KMS key. No sign-in path skips the code.
- **Sessions from before the switch** are checked at their next request: someone already signed in without a code is sent to the setup screen, or asked to sign in again with their code.
- **API keys** are not people and keep working: a key never signs in, so the rule doesn't hold it back. **Settings, Sign-in** and **Settings, API keys** mark each live key made before the rule was turned on, with whether the person who made it has a second factor now (or has left the team). Such a key may have been made from a session without a second factor: check them and revoke any you don't recognize in **Settings, API keys**. Keys made after the rule is on come from sessions that passed it.

### One second factor per person

Email and password and GitHub lead to the same person in CoderHelm (by email), so each person has **one** second factor, and it is asked for however they sign in, whether the rule is on or not:

- **An authenticator app set up in CoderHelm** (after GitHub): its code is asked for after GitHub and a password alike.
- **Cognito's code on the password account**: only Cognito can check it, so it is asked for at the password sign-in. Signing in with GitHub sends the person back to sign in with their email and password.
- **Neither kind can be set up while the person has one**, from any session, including one held at the setup screen. If someone lost their phone, an admin resets it (below).
- **Replacing the app** (a new phone): **Settings, Security, Replace authenticator app**, also while two-factor sign-in is required. The person enters a current code from the app they have now (and their password, when the code is on the password account), scans a new code, and enters a code from the new app. Only then does the new app replace the old one, in one step; the old app stops working right away, their other sessions end, the change goes in the log under **Settings, Roles**, and they get an email saying their authenticator app was changed. The new code is always CoderHelm's own authenticator-app code, so a code that was on the password account moves to it. Without a current code from the old app there's no way to replace it yourself: an admin resets it.
- **Turning it off** takes a current code, and isn't possible while it's required.

**Settings, Sign-in** shows who has set it up. If someone loses their phone, an admin can reset their two-factor sign-in after checking it's really them (a video call, say). The admin writes how they checked; the reset and the reason go in the log under **Settings, Roles**. The person sets it up again at their next sign-in, and gets an email saying their two-factor sign-in was reset, so a reset they didn't ask for doesn't go unnoticed. Only an owner can reset an admin's or another owner's. Nobody can reset their own.

### Wrong codes

Every place CoderHelm checks a code counts wrong ones against the person, by email, across every way in: the code after GitHub, the code after a password, and turning the app code off.

- **Ten wrong codes in 15 minutes** pause that person's sign-in with a code for 15 minutes. Each pause after that is twice as long, up to a day. After a quiet day the count starts over. A right code clears the wrong ones so far.
- **One sign-in through GitHub** allows five wrong codes; after that the person starts again from the sign-in screen.
- **The person** sees a plain sentence saying how long the pause lasts.
- **Admins** get an email, and a line in the log under **Settings, Roles**. If they lost their phone, check it's them and reset; if it wasn't them, someone may know their password or have their GitHub account.

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

## No Google sign-in

A self-hosted install signs people in with email and password, and two-factor where it's set. It has no "Sign in with Google": the sign-in page has no Google button, and the deploy creates no Google identity provider (`integrations.googleSignIn` is ignored).

An install that had Google sign-in on loses it at its next update, which removes the Google provider. People who signed in with Google sign in again with **Forgot password** on the sign-in page, with the same email: they set a password and keep their account, teams and settings.

## GitHub accounts

People connect their GitHub account in their own settings, so pull requests and reviews are credited to them. Signing in with GitHub follows the same rules as everything else: it lands on the organization only for someone already in it, invited, or at an allowed domain (by the email on their GitHub account). It uses the GitHub App's own OAuth client, which the GitHub App step stores; nothing to set up. An install on CoderHelm's GitHub App (hybrid) has no client of its own: once it is registered with CoderHelm, GitHub sign-in goes through CoderHelm's app, and the install still checks the account with GitHub itself and applies these same rules and two-factor ([Sign in with GitHub through CoderHelm](./hybrid.md#sign-in-with-github-through-coderhelm)). The sign-in page offers **Continue with GitHub** only when one of the two works (it asks the API, `GET /auth/methods`). The `github_sign_in` check in **Settings, Setup** says which applies.

## Single sign-on (SAML, OIDC)

Not available yet: CoderHelm's sign-in page offers email and password, and GitHub. The Cognito user pool is in your account, so an identity provider added there by hand is not shown on the sign-in page and isn't supported.

## Install admins and team roles

- **Install admins** (`admins`) see the whole install: the failed events queue, support tickets, and the Setup and System pages. They must have an email CoderHelm verified itself.
- **Team roles** (viewer, member, admin, owner, and custom roles) are set in **Settings, Team** and **Settings, Roles**. Sign-in rules stay with built-in admins and owners; a custom role can't hold them.
- **Support access:** a team admin can let the install admins see the team's workspace read-only for a fixed time, with every read in the team's audit log (**Settings, Support access**).
