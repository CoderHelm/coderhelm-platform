---
title: Sign in with Microsoft and Okta
description: Set up Sign in with Microsoft (an Entra app registration) and Sign in with Okta (an Okta OIDC Web app), what each checks, and which email CoderHelm trusts.
order: 6.6
---

CoderHelm signs people in with Microsoft (work, school and personal accounts) and with an organization's own Okta. Both use OpenID Connect with PKCE, run by CoderHelm's API itself, the way GitHub sign-in is. Neither replaces the rules in [Sign-in and people](./sign-in.md): on a self-hosted install a person still needs an invite or an allowed email domain, the person's member record is found by email, and their second factor is asked for after Microsoft or Okta, the same way it is after Google and GitHub.

The sign-in page shows **Continue with Microsoft** and **Continue with Okta** only when they work (it asks the API, `GET /auth/methods`).

## What CoderHelm checks

Every Microsoft and Okta sign-in:

- starts with a one-time state in a `__Host-` cookie (10 minutes, one use) and a PKCE S256 code challenge, with a nonce;
- exchanges the code with the PKCE verifier and the client secret;
- checks the ID token's RS256 signature against the provider's published keys, its issuer, its audience (the client ID), the nonce and its expiry;
- takes the email only from a claim the provider vouches for (below);
- then goes through the same join rules and two-factor step as every other way in.

When something fails, the person lands on the sign-in screen with one plain sentence. The address carries only a short code (`signin_error=no_verified_email`, for example); the reason is in the API's log.

## Which email is trusted

- **Microsoft (work, school and personal accounts):** the `email` claim, only when the same token says `xms_edov: true`. Microsoft's optional claims reference says `email` "isn't guaranteed to be correct, and is mutable over time", and describes `xms_edov` as "Boolean value indicating whether the user's email domain owner has been verified" ([optional claims reference](https://learn.microsoft.com/en-us/entra/identity-platform/optional-claims-reference)). `preferred_username` is never used. `verified_primary_email` is not used either: the same page says only that it is "Sourced from the user's PrimaryAuthoritativeEmail", not that it is at a verified domain. Without a verified email, sign-in stops: "Microsoft didn't share a verified email address for this account".
- **Okta:** the `email` claim, only with `email_verified: true`.

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

In **Settings, Sign-in rules, Okta sign-in** (admins and owners):

1. **Okta domain or issuer:** `acme.okta.com`, or the custom authorization server's issuer. It must be https, and CoderHelm reads its `/.well-known/openid-configuration`; every address in it must be on the same host.
2. **Client ID** and **Client secret** from the Okta app. The secret is encrypted with the deployment's AWS KMS key before it is saved, and never shown again (leave the box empty to keep the saved one).
3. **Email domains:** the work email domains that sign in with this Okta, for example `example.com`. Required on hosted CoderHelm. Optional on a self-hosted install: empty means the organization's allowed email domains, or everyone when there are none.
4. Choose **Test sign-in**. Your Okta opens and asks you to sign in again. CoderHelm runs every check a real sign-in makes, then brings you back to the page with the outcome. Nobody else is affected by the test. The test link works only in your own browser, signed in as you, on this team.
5. **Hosted CoderHelm: prove each email domain.** The section shows a TXT record for each domain: the name `_coderhelm-okta.<domain>` and a value `coderhelm-okta=<code>` made for your team and that domain. Add it in your domain's DNS, wait for it to publish, then choose **Verify domains**. CoderHelm reads the record itself and keeps when it was verified. A domain can be proven by one team only. Okta sign-in can't be turned on until every listed domain is verified, and a sign-in counts only for an email at a verified domain. Keep the record: removing Okta sign-in releases the domains.
6. When the test passed (and, on hosted CoderHelm, the domains are verified), choose **Save and turn on**. Only settings whose test passed can be saved; change anything and test again. Changing the Okta domain or the Client ID needs the client secret entered again: a saved secret is never sent to a different Okta.

Turn it off, or remove it, from the same section. People who signed in with Okta keep their accounts and sign in another way.

### 3. How people sign in

**Continue with Okta** asks for the work email and sends the person to the Okta whose setup covers it, with the email filled in:

- **Hosted CoderHelm:** the Okta of the team that proved the email's domain in DNS, when its Okta sign-in is on. Which Okta it is depends only on the domain, never on whether the person is on a team. After Okta, CoderHelm signs in only someone who is already an active member of that team (they joined it some other way, with an invite and a password, Google, GitHub or Microsoft). Okta never accepts an invite, never makes an account or a team, and its email must be at one of that team's verified domains. Someone else sees "you aren't on the team that uses this Okta".
- **What an Okta session can do (hosted):** everything on that team, and nothing beyond it. It can't switch to the person's other teams, see them in the team list, change their password or second factor, link a GitHub account, end their other sessions, or act as a CoderHelm platform admin. Those need a sign-in with a password, Google, GitHub or Microsoft. On any install, an Okta session is never a platform or install admin.
- **A self-hosted install:** the organization's Okta, and then the same rules as every way in: already in the organization, invited, or at an allowed email domain.

## On CoderHelm's hosted deployment

What the deployment's owner sets up once:

| What | Where it goes |
|---|---|
| Entra app registration: multitenant and personal accounts, Web redirect URI `https://api.coderhelm.com/auth/microsoft/callback`, optional ID token claims `email` and `xms_edov` | Microsoft Entra admin center |
| `microsoft_client_id` (Application (client) ID) | `coderhelm/prod/secrets` |
| `microsoft_client_secret` (client secret Value) | `coderhelm/prod/secrets` |
| Okta | Nothing: each team's admins set up their own in Settings |

A staging deployment uses its own API address in the redirect URI (add it to the same registration, or make a second one) and its own `coderhelm/<stage>/secrets`.
