---
title: Sign in with Microsoft and Okta
description: Set up Sign in with Microsoft (an Entra app registration) and Sign in with Okta (an Okta OIDC Web app), what each checks, and which email CoderHelm trusts.
order: 6.6
---

CoderHelm signs people in with Microsoft (work, school and personal accounts) and with an organization's own Okta. Both use OpenID Connect with PKCE, run by CoderHelm's API itself, the way GitHub sign-in is. Neither replaces the rules in [Sign-in and people](./sign-in.md). Microsoft follows them like Google and GitHub: on a self-hosted install a person still needs an invite or an allowed email domain, and their member record is found by email. Okta never finds anyone by email: it signs in only a member who linked their Okta account themselves (below). The person's second factor is asked for after both.

The sign-in page shows **Continue with Microsoft** and **Continue with Okta** only when they work (it asks the API, `GET /auth/methods`).

## What CoderHelm checks

Every Microsoft and Okta sign-in:

- starts with a one-time state in a `__Host-` cookie (10 minutes, one use) and a PKCE S256 code challenge, with a nonce;
- exchanges the code with the PKCE verifier and the client secret;
- checks the ID token's RS256 signature against the provider's published keys, its issuer, its audience (the client ID), the nonce and its expiry;
- takes the email only from a claim the provider vouches for (below);
- reads the provider's discovery document and keys from exactly the address asked for (no redirects), through the same guarded connection MCP sign-ins use: https only, never a private, loopback, link-local or metadata address;
- then goes through the same join rules (Microsoft) or the member's own link (Okta), and the two-factor step, as every other way in.

When something fails, the person lands on the sign-in screen with one plain sentence. The address carries only a short code (`signin_error=no_verified_email`, for example); the reason is in the API's log.

## Which email is trusted

- **Microsoft (work, school and personal accounts):** the `email` claim, only when the same token says `xms_edov: true`. Microsoft's optional claims reference says `email` "isn't guaranteed to be correct, and is mutable over time", and describes `xms_edov` as "Boolean value indicating whether the user's email domain owner has been verified" ([optional claims reference](https://learn.microsoft.com/en-us/entra/identity-platform/optional-claims-reference)). `preferred_username` is never used. `verified_primary_email` is not used either: the same page says only that it is "Sourced from the user's PrimaryAuthoritativeEmail", not that it is at a verified domain. The rule is the same for personal accounts: Microsoft's docs don't say a personal account's `email` is verified, and its `xms_edov` notes name Microsoft accounts among those whose email can count as domain verified. A personal account whose token has no `xms_edov: true` can't sign in with Microsoft; it signs in another way. Without a verified email, sign-in stops: "Microsoft didn't share a verified email address for this account".
- **Okta:** the `email` claim, only with `email_verified: true`. It finds the Okta and, on hosted CoderHelm, is checked against the proven domains. It never picks the member: the link does.

## Sign in with Microsoft

One app registration in Microsoft Entra serves every Microsoft account. Create it once per deployment.

### 1. Register the app

1. In the [Microsoft Entra admin center](https://entra.microsoft.com), open **Identity, Applications, App registrations** and choose **New registration**.
2. **Name:** CoderHelm (people see it on Microsoft's consent page).
3. **Supported account types:** *Accounts in any organizational directory (Any Microsoft Entra ID tenant, Multitenant) and personal Microsoft accounts (e.g. Skype, Xbox)*.
4. **Redirect URI:** platform **Web**, address `<API URL>/auth/microsoft/callback`:
   - hosted CoderHelm: `https://api.coderhelm.com/auth/microsoft/callback`;
   - a self-hosted install: its own address, for example `https://coderhelm.example.com/auth/microsoft/callback` (the dashboard and API share one address; with no domain it is the `https://<id>.cloudfront.net` address).
5. Choose **Register**. Copy the **Application (client) ID** from the Overview.

### 2. Add the email claims

1. Open **Token configuration** and choose **Add optional claim**, token type **ID**.
2. Tick `email` and `xms_edov`, and choose **Add**. When asked, tick **Turn on the Microsoft Graph email permission**.

Without both, nobody can sign in with Microsoft: CoderHelm needs `xms_edov` beside the email.

### 3. Make a client secret

1. Open **Certificates & secrets**, **Client secrets**, **New client secret**. Pick an expiry (24 months at most) and note the date: sign-in with Microsoft stops when it expires.
2. Copy the secret's **Value** now (it is shown once). Not the Secret ID.

API permissions: the default `User.Read` with `openid`, `email` and `profile` is enough. Nothing needs an admin's consent, although an organization's admin may require it for every app.

### 4. Give CoderHelm the client ID and secret

They go in the deployment's secret in AWS Secrets Manager, next to the other keys (the same JSON that holds `jwt_secret` and `github_app_id`):

```json
{ "microsoft_client_id": "<Application (client) ID>", "microsoft_client_secret": "<client secret Value>" }
```

- **Hosted CoderHelm:** `coderhelm/prod/secrets` (the gateway reads `SECRETS_NAME`).
- **A self-hosted install:** `coderhelm/<stage>/secrets`, or `<prefix>/secrets` for a custom prefix.

Add the two keys to the JSON that is there (keep every other key), then redeploy or wait for the gateway to start a new instance. The sign-in page then shows **Continue with Microsoft**. The secret is never shown in the dashboard and never logged. To turn Microsoft sign-in off, remove the two keys.

When the secret is near its expiry, make a new one in **Certificates & secrets**, replace `microsoft_client_secret`, then delete the old one in Entra.

## Sign in with Okta

Okta belongs to each organization, so there is no CoderHelm-wide Okta app. On hosted CoderHelm each team's admins set up their own; on a self-hosted install the organization's admins set up one for the install.

### 1. Create the Okta app

1. In the Okta Admin Console, open **Applications, Applications** and choose **Create App Integration**.
2. **Sign-in method:** OIDC - OpenID Connect. **Application type:** Web Application. Choose **Next**.
3. **App integration name:** CoderHelm.
4. **Grant type:** Authorization Code (the default). Leave Refresh Token off.
5. **Sign-in redirect URIs:** the address CoderHelm shows in **Settings, Sign-in rules, Okta sign-in** (with a **Copy** button). It is `<API URL>/auth/okta/callback`: `https://api.coderhelm.com/auth/okta/callback` on hosted CoderHelm, or `https://<your install's address>/auth/okta/callback`.
6. **Sign-out redirect URIs:** optional; the dashboard's address is fine.
7. **Assignments:** the people or groups who may sign in to CoderHelm.
8. Choose **Save**. On the app's **General** tab, keep **Client authentication** on **Client secret** (CoderHelm sends it with HTTP Basic, Okta's default), and copy the **Client ID** and the **Client secret**. **Require PKCE as additional verification** may be on: CoderHelm always sends PKCE.
9. Note your Okta domain (for example `acme.okta.com`). If you use a custom authorization server, the issuer is on **Security, API, Authorization Servers** (for example `https://acme.okta.com/oauth2/default`).

People's emails must be verified in Okta (Okta's `email_verified`), which is usual for people an admin added.

### 2. Set it up in CoderHelm

In **Settings, Sign-in rules, Okta sign-in** (admins prepare and test it; only the owner can save it or turn it on, on any install, since an Okta setup decides who can sign in as whom. The save names the exact draft the owner looked at, so a draft changed in between is refused):

1. **Okta domain or issuer:** `acme.okta.com`, or the custom authorization server's issuer. It must be https, and CoderHelm reads its `/.well-known/openid-configuration`; every address in it must be on the same host.
2. **Client ID** and **Client secret** from the Okta app. The secret is encrypted with the deployment's AWS KMS key before it is saved, and never shown again (leave the box empty to keep the saved one).
3. **Email domains:** the work email domains that sign in with this Okta, for example `example.com`. Required on hosted CoderHelm. Optional on a self-hosted install: empty means the organization's allowed email domains, or everyone when there are none.
4. Choose **Test sign-in**. Your Okta opens and asks you to sign in again. CoderHelm runs every check a real sign-in makes, then brings you back to the page with the outcome. Nobody else is affected by the test. The test link works only in your own browser, signed in as you, on this team.
5. **Hosted CoderHelm: prove each email domain.** The section shows a TXT record for each domain: the name `_coderhelm-okta.<domain>` and a value `coderhelm-okta=<code>` made for your team and that domain. Add it in your domain's DNS, wait for it to publish, then choose **Verify domains**. CoderHelm reads the record itself and keeps when it was verified. A domain can be proven by one team only. Okta sign-in can't be turned on until every listed domain is verified, and a sign-in counts only for an email at a verified domain. Keep the record: CoderHelm reads it again whenever Okta is turned on, and at sign-in once the last check is a day old. A record that is gone (no such name, or no matching record) releases the domain; a DNS read that gets no clear answer keeps it and tries again later. A CNAME in the answer is followed. A domain also goes when the draft that listed it is discarded or replaced, unless the live setup still lists it. Domains also stop being yours when you remove them from the setup and save, choose **Release** beside one, or remove Okta sign-in.
6. When the test passed (and, on hosted CoderHelm, the domains are verified), choose **Save and turn on**. Only settings whose test passed can be saved; change anything and test again. Changing the Okta domain or the Client ID needs the client secret entered again: a saved secret is never sent to a different Okta.

Turn it off, or remove it, from the same section. People who signed in with Okta keep their accounts and sign in another way. Removing Okta, or saving a different Okta domain or issuer, drops every member's Okta link: each links again.

### 3. Each member links their Okta account

Okta signs in only a member who linked their own Okta account. In **Settings, Security, Okta**, a member chooses **Link Okta** from a session they started another way (password, Google, GitHub or Microsoft). Okta asks them to sign in, and CoderHelm keeps Okta's issuer and the account's subject (its fixed id) on their member record. It never matches anyone by email, so whoever runs an Okta can't sign in as a member who didn't link that account. **Unlink** in the same place removes it. One Okta account links to one member of a team.

A session signed in with Okta ends at its next request when the member unlinks, when the Okta domain or issuer changes, or when Okta is turned off or removed. The member's sessions from other ways in are not affected. An Okta session is never a CoderHelm platform or install admin, on any install.

New people join the usual way first (an invite, or an allowed email domain on a self-hosted install), then link Okta. While Okta sign-in is required (below), they accept their invite with Okta instead.

### 4. How people sign in

**Continue with Okta** asks for the work email and sends the person to the Okta whose setup covers it, with the email filled in. After Okta, CoderHelm signs in the member who linked exactly that Okta account. Someone who hasn't linked it sees: "sign in another way first, then link Okta in Settings, Security".

- **Hosted CoderHelm:** the Okta of the team that proved the email's domain in DNS, when its Okta sign-in is on. Which Okta it is depends only on the domain, never on whether the person is on a team.
- **What an Okta session can do (hosted):** everything on that team, and nothing beyond it. It can't switch to the person's other teams, see them in the team list, change their password, change or turn off their second factor, link GitHub or Okta, end their other sessions, or act as a CoderHelm platform admin. Those need a sign-in with a password, Google, GitHub or Microsoft. Someone on no other team can still set up a second factor from an Okta session, so a team that requires two-factor sign-in never leaves them stuck; someone on other teams is told to sign in another way to set it up.
- **A self-hosted install:** the organization's Okta. There is one organization, so an Okta session there isn't held to a team. It still can't act as an install admin: an install admin signs in another way for Settings, Setup and Settings, System.

### 5. Require Okta sign-in (optional)

In **Settings, Sign-in rules**, beside **Require two-factor sign-in**, the owner can turn on **Require Okta sign-in**. On a self-hosted install it is the organization's rule, saved on the organization record; on hosted CoderHelm it is each team's own.

**Turning it on.** Only the owner can turn it on or off, and every change is in the team's audit log. It turns on only when:

1. Okta sign-in is set up and turned on.
2. On hosted CoderHelm, every email domain of the setup is proven in DNS. CoderHelm reads each TXT record again at that moment. A self-hosted install proves no domains: only its owner can set up the organization's Okta.
3. The owner's own session came from this Okta. Sign out, choose **Continue with Okta**, then turn it on. That proves Okta sign-in works before everyone depends on it.

The page lists the members who haven't linked Okta yet, and turning the rule on asks you to confirm that list.

**While it is on:**

- Members sign in only with Okta. Password, Google, GitHub and Microsoft sign-ins are refused with a message that names the team (on a self-hosted install, the organization) and links to Okta sign-in. Nothing is made instead: no personal team, no accepted invite, no join by email domain.
- Sessions from any other way in end at their next request, including ones made before the rule. Switching into the team from another team's session is refused. Apps connected through CoderHelm's MCP server work only when they were connected from an Okta session.
- On hosted CoderHelm only this team is held back. A person on other teams signs in to those as before, and their sessions there go on.
- While the rule is on, Okta can't be turned off or removed, moved to another Okta domain or issuer, or lose an email domain, and members can't unlink Okta. Turn the rule off first. Each of those writes checks the rule in the same database transaction, and turning the rule on checks that the Okta setup (and each proven domain) is unchanged, so the two can never both land.
- When the organization's Okta lists no email domains of its own, it signs in the organization's allowed domains: while the rule is on, none of those can be removed.
- On hosted CoderHelm, a TXT record that disappears while the rule is on doesn't release its domain: the domain is kept and shown as failing, and the owners get an email. Turning the rule on needs every record to answer clearly at that moment.
- The short-lived tokens the plan chat and the brain stream use carry how their session signed in, and are checked against the rule when used.
- A CoderHelm support view of a team that requires Okta needs the viewing admin's session to have passed a second factor (an Okta session is never a platform admin, so a second factor is the bar).
- API keys and CI tokens are not sessions. They keep working as they are.

**Break-glass.** If your Okta is down or misconfigured, the owner can still sign in with their password and their second factor: on the sign-in page, choose the small owner link under Okta (an owner of several such teams gets the team whose sign-in page they came from). A second factor is required, with no exception; an owner without one can't use it, so set one up before you turn the rule on. Each such sign-in is written to the team's audit log before its session starts, and listed on the Sign-in rules page. Nobody else has this way in, and a session from it can't connect apps.

**New members.** An admin invites them as usual. While the rule is on, every invite waits to be accepted (even for someone who already has a CoderHelm account). The person accepts it by signing in with Okta: when Okta's verified email is exactly the invited email (compared without case, nothing else: `ann+work@example.com` and `ann@example.com` are different addresses here) (and, on hosted CoderHelm, its domain is one the team proved), CoderHelm makes their member record and links their Okta account in one step. It never links or takes over an existing member record, and an Okta account linked to one member can't accept another invite. Why this is safe: an admin named the exact email; Okta's email counts only when Okta marks it verified, from the team's own Okta; on hosted CoderHelm the email's domain is one the team proved it controls in DNS, and the session is held to that team like every Okta session.

**Members who haven't linked Okta.** Their Okta sign-in works only through a link they made before the rule. Otherwise they are told to ask an admin. An admin can remove them and invite them again (they accept with Okta), or turn the rule off while they link. CoderHelm never matches an existing member by email.

**Turning it off** brings every other way in back at once.

## On CoderHelm's hosted deployment

What the deployment's owner sets up once:

| What | Where it goes |
|---|---|
| Entra app registration: multitenant and personal accounts, Web redirect URI `https://api.coderhelm.com/auth/microsoft/callback`, optional ID token claims `email` and `xms_edov` | Microsoft Entra admin center |
| `microsoft_client_id` (Application (client) ID) | `coderhelm/prod/secrets` |
| `microsoft_client_secret` (client secret Value) | `coderhelm/prod/secrets` |
| Okta | Nothing: each team's admins set up their own in Settings |

A staging deployment uses its own API address in the redirect URI (add it to the same registration, or make a second one) and its own `coderhelm/<stage>/secrets`.
