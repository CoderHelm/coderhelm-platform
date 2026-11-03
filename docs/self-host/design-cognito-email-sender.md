# Design: sign-up and password emails through the install's own email

Status: proposed, not built. This note decides how a self-hosted install
sends Amazon Cognito's sign-up codes and password resets through the same
transport as every other email it sends (CoderHelm's relay or its own SES),
on both kinds of install.

## Today

- Cognito sends these emails itself. A config-file install can switch the
  pool to its own SES (`email.cognitoUsesSes`) once the SES identity is
  verified. A launch-page install (one stack) has no such switch: it always
  uses Cognito's own sender, about 50 emails a day.
- The custom-message trigger (`lambda/custom-message`) only rewrites the
  subject and body from SES templates. Cognito still does the sending, so it
  changes nothing about who sends or the daily limit.
- An install that sends through CoderHelm's relay has no SES identity of its
  own, so `cognitoUsesSes` can't help it at all.

## Proposal: Cognito's CustomEmailSender trigger

Cognito hands each message to a Lambda instead of sending it. The Lambda
sends it through the install's transport, the same path as invites and run
results (`services/common/src/mail`).

**Which messages.** Every Cognito email: sign-up code (`CustomEmailSender_SignUp`),
resend code, forgot password, admin-created user, attribute verification, and
the account-takeover and authentication messages if those features are ever on.
The trigger receives them all, so each one needs a template or an explicit
refusal that sends nothing (logged, counted in Deliveries).

**The KMS key.** Cognito encrypts the code with a KMS key the pool names
(`KMSKeyID` on the trigger). The install creates one key for it: a symmetric
key whose policy lets the Cognito service encrypt only for this pool
(`aws:SourceArn` the pool's ARN), and lets only the sender Lambda's role
decrypt. Rotation is on, and the key follows KeepDataOnDelete like the other
keys (7-day wait). On a launch-page install the key and its policy live in
the parent's templates and carry the parent's boundary.

**Decrypting the code.** Cognito encrypts with the AWS Encryption SDK, not
plain KMS `Decrypt`, so the Lambda uses the Encryption SDK with a keyring
naming exactly this key. The code lives only in memory: never logged, never
in an error, never in Deliveries. It goes into the message body and nowhere
else.

**Self-host link rules.** The messages follow the rules every self-host email
follows: links and the logo point at the install, the sender is the
organization's name (else the install's domain, else "CoderHelm"), and replies
go to the install's admin or support address. No coderhelm.com link except
public docs. The existing golden-file test that renders every email as a
self-hosted install covers these messages too.

**The custom-message trigger.** Once CustomEmailSender is on, Cognito no
longer calls the custom-message trigger for email (it still would for SMS,
which CoderHelm doesn't use). The templates move into
`services/common/src/mail/templates.rs` with the other emails, and
`lambda/custom-message` and its SES templates are removed in the same change,
so there is one source for the wording.

**Both kinds of install.** A launch-page install gets a launch parameter
(say `SignInEmails`: `cognito` or `install`, default `cognito`), shown in
Settings, System. Changing it is an ordinary update. A config-file install
gets the same choice in the config, and `cognitoUsesSes` stays for installs
that want Cognito to send through their SES without the trigger. The setup
check then names the right switch for each kind, instead of the text it has
today.

## Failure modes

- **Relay or SES down.** Nobody can verify an address or reset a password
  while it's down, and Cognito doesn't retry. The trigger can't fall back to
  Cognito's own sender at runtime: with CustomEmailSender on, Cognito never
  sends. So the only fallback is switching the parameter back to `cognito`
  (an update). The setup check warns when the transport's recent sends are
  failing, and the switch says plainly that sign-in codes depend on it.
- **Trigger errors.** An error or timeout in the trigger fails the user's
  request (sign-up or reset), and the user sees an error, which is better than
  a silent loss. Every refusal and error is counted in Deliveries with its
  reason, never with the code.
- **Key misconfiguration.** If Cognito can't encrypt with the key, every
  sign-up fails. The pool and key change in the same stack update, and the
  test plan below catches it before release.
- **Relay limits.** Sign-in codes share the relay's per-install limits with
  other mail. They get their own small reserved budget, so a burst of run
  emails can't block someone signing in.

## Test plan: run the real triggers before any release

The pre-sign-up check shipped broken (`db.send is not a function`, fixed in
#412): unit tests called the handler with a fake client, but Lambda calls it
with `(event, context, callback)`. Unit tests alone don't cover how AWS
invokes a trigger. So before a release that touches any Cognito trigger:

1. Deploy the release to a throwaway test stack (launch page, test account).
2. Against that stack's pool, run each flow for real: sign up with a new
   address (pre-sign-up, then the sign-up code), resend the code, forgot
   password, and a sign-in carried over through a migration trigger if one is
   attached.
3. Check each email arrives through the chosen transport, with the right
   sender, links that point at the install, and a working code, and check
   that Deliveries counted it without the code.
4. Switch the parameter to `install` and back to `cognito` as an update, and
   repeat step 2 each time.
5. Delete the stack and confirm nothing is left.

This runs as a release gate in CI against the test account, so a trigger that
fails under AWS's real calling convention never reaches an install.
