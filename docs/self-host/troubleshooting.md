---
title: Troubleshooting
description: The problems people hit when installing and running CoderHelm, and what fixes them.
order: 12
---

Start with:

```sh
npx -y github:CoderHelm/coderhelm-platform health
```

Every check names what to do when it fails. Function logs are in CloudWatch under `/aws/lambda/coderhelm-<stage>-gateway`, `-worker`, `-streaming` and `-mcp-proxy`.

## Installing

**"Invalid CoderHelm config"** at synth. The message lists every problem with the setting's name; fix them in `coderhelm.config.json`. See the [Config reference](./config-reference.md).

**"Your AWS credentials are for account X, but the config deploys to Y."** Switch profile (`export AWS_PROFILE=...`) or sign in again (`aws sso login`).

**"This stack uses assets, so the toolkit stack must be deployed"** or a bootstrap error. Run `npx cdk bootstrap aws://<account>/<region>` with administrator credentials; with a domain outside us-east-1, bootstrap us-east-1 too. The installer does both.

**A bucket "already exists".** S3 bucket names are global. Keep the default `globalNameSuffix` (your account id) or set your own.

**The deploy fails on the user pool with an SES error.** `email.cognitoUsesSes` is true but the SES identity isn't verified yet. Set it to false, deploy, verify the identity, then turn it on. See [Email](./email.md).

**Certificate validation never finishes.** With `hostedZone`, the zone must be the public zone your domain's registrar points to. With `certificateArn`, the certificate must be in us-east-1 and issued.

**The first deploy seems stuck on the web stack.** Creating a CloudFront distribution takes 5 to 15 minutes.

## Signing in

**"Not on the allowlist" or sign-up refused.** Only `admins`, `signup.emails` and `signup.domains` may create accounts. Add the address or domain and deploy; or, as an admin, invite the person from the dashboard.

**No sign-up email.** Cognito's own sender allows 50 emails a day and SES's sandbox only reaches verified addresses. Check spam, then see [Email](./email.md).

**Signed out right after signing in.** You opened the API's own URL or an old hostname. Use the one URL `deploy` printed; the session cookie belongs to it.

## GitHub

**Nothing happens when I assign an issue.**
1. The app is installed on that repository (GitHub → the app → **Install App**).
2. The app's webhook URL is `<your URL>/webhooks/github` and recent deliveries show `200` (GitHub → the app → **Advanced**).
3. `health` shows the GitHub App keys as stored. If you stored them by hand, redeploy so the functions read them: `coderhelm-selfhost github-app` does this for you.

**Webhook deliveries show 401.** The app's webhook secret doesn't match the stored one. Run `github-app --force` to create a fresh app, or fix the secret in Secrets Manager and redeploy.

**The manifest page says the name is taken.** GitHub App names are unique across GitHub. Change the name on GitHub's page, or pass `--name`.

## Runs

**Runs fail with "Anthropic API key is not configured".** With the `anthropic` provider each team adds its key in **Settings → Model Provider**. Or switch to [Bedrock](./model-providers.md#amazon-bedrock).

**Bedrock "AccessDeniedException" or "model identifier is invalid".** Enable the model in Bedrock **Model access** for your region, check `bedrockInferencePrefix` matches your region, or map the model to an exact id with `bedrockModelIds`.

**Failed messages pile up.** The admin queue page (`/admin/queue/`) lists dead-letter messages with their error, and can resend or delete them. `health` shows the count.

## Links point to localhost

Links in comments and emails come from the public URL. Without a domain it is read from SSM (`/coderhelm-<stage>/public-url`) when a function starts; if a function started before the first deploy finished, it retries every 30 seconds. Check the parameter exists, then send a request (any page) to refresh it.
