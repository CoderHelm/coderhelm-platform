---
title: Email
description: How CoderHelm sends email from your AWS account with Amazon SES, and how to leave the SES sandbox.
order: 7
---

CoderHelm sends run results, invites, token limit warnings, the weekly summary and account emails through Amazon SES in your account, from `email.from`.

## The identity

The deploy creates an SES identity for `email.identity` (by default the domain of `email.from`):

- **A domain in the same Route 53 zone as your CoderHelm domain:** its DKIM records are created for you, and it verifies in a few minutes.
- **Any other domain:** SES shows three DKIM CNAME records in the console (**SES, Identities, your domain**). Add them at your DNS provider.
- **One address** (`"identity": "coderhelm@example.com"`): SES emails it a confirmation link. Good for trying CoderHelm out.
- **null:** you manage the identity yourself.

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
