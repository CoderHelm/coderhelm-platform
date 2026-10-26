---
title: Troubleshooting
description: The problems people hit when installing and running CoderHelm, and what fixes them.
order: 13
---

Start with the doctor. It checks everything (your AWS sign-in and permissions, the region, CDK, DNS, the certificate, the stacks, the GitHub App, email and a real model call) and says, for each problem, what is wrong, why it matters and how to fix it:

```sh
npx -y github:CoderHelm/coderhelm-platform doctor
```

In the dashboard, **Settings, then Setup** runs the server-side checks live. Every check and its fix is listed in [Check your install](./check-your-install.md). Errors print a short message; the full details (with the failed command's output) are saved under `.coderhelm/logs/`, and `--verbose` prints them. Function logs are in CloudWatch under `/aws/lambda/coderhelm-<stage>-gateway`, `-worker`, `-streaming` and `-mcp-proxy`.

## The guided install stops at a check

Each step checks what you did before it moves on. When a check fails, fix it and press **Enter** to check again; nothing restarts. Type **skip** to go on anyway (the doctor at the end lists it again) or **back** to change an answer. Without a terminal, the install stops with the same text: fix it and run the same command again (it picks up where it stopped), or pass `--skip-checks <id>`.

| Check | Usual fix |
|---|---|
| **Signed in to AWS** | `aws sso login --profile <name>` (expired), or `aws configure sso` (no profile yet) |
| **The right AWS account** | `export AWS_PROFILE=<profile for that account>` |
| **AWS permissions for the install** | The missing actions are listed. Use a role with AdministratorAccess, or ask your AWS admin to allow them. If it says the permissions couldn't be checked, your role lacks `iam:SimulatePrincipalPolicy`: carry on if you have AdministratorAccess |
| **Region has every service** | Pick a region the check doesn't flag (us-east-1, us-west-2, eu-west-1 and eu-central-1 have everything) |
| **CDK set up** | `npx -y cdk bootstrap aws://<account>/<region>` (the installer offers to run it; with a domain outside us-east-1, us-east-1 too). If the `CDKToolkit` stack is in a failed state, delete it first |
| **Route 53 zone** | Create the zone (`aws route53 create-hosted-zone --name <zone> --caller-reference <anything>`), or leave the hostname empty |
| **Points at Route 53** | Set the NS records at your registrar to the four the check prints; it also shows what public DNS answers now. Changes can take up to an hour |
| **TLS certificate issued** | Add the validation records the check prints, or request a certificate in us-east-1 for the exact hostname |
| **Claude models enabled in Bedrock** | Open the Model access link the check prints and enable the models it names |
| **Anthropic API key works** | 401: copy the key again. 403 or 404: the key's workspace can't use the model. "No credit": add credit in the Anthropic console |
| **Email sender verified** | Add the three DKIM records the check prints at your DNS provider |
| **Email can reach anyone** | A warning: request SES production access with the link the check prints |
| **Slack or Teams webhook** | Create a new incoming webhook; the old one was removed |

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

**Nothing happens when I assign an issue.** Run the doctor or open **Settings, then Setup**: the GitHub checks cover each cause.
1. **GitHub App installed:** the app is installed on that repository (on GitHub: the app, then **Install App**).
2. **GitHub webhook reaches this install:** the webhook URL is `<your URL>/webhooks/github` and the latest delivery shows `200` (on GitHub: the app, then **Advanced**).
3. **GitHub App permissions and events:** every permission and event is granted. A missing one, such as **Workflows: write**, is named with the page to change it; after saving, accept the new permissions on each installation.
4. **GitHub accepts the app's keys:** if you stored them by hand, `coderhelm-selfhost github-app` stores them and restarts the services.

**Webhook deliveries show 401.** The app's webhook secret doesn't match the stored one. Run `github-app --force` to create a fresh app, or fix the secret in Secrets Manager and redeploy.

**The manifest page says the name is taken.** GitHub App names are unique across GitHub. Change the name on GitHub's page, or pass `--name`.

## Runs

**Runs fail with "Anthropic API key is not configured".** With the `anthropic` provider each team adds its key in **Settings, AI models**. **Settings, then Setup** tests it with a real one-token call. Or switch to [Bedrock](./model-providers.md#amazon-bedrock).

**Bedrock "AccessDeniedException" or "model identifier is invalid".** Enable the model in Bedrock **Model access** for your region, check `bedrockInferencePrefix` matches your region, or map the model to an exact id with `bedrockModelIds`.

**Failed messages pile up.** The admin queue page (`/admin/queue/`) lists dead-letter messages with their error, and can resend or delete them. The doctor and **Setup** show the count.

## Updates

**Update now is greyed out.** **Update readiness** on **Settings, then System** lists what blocks it, each with its fix. Press **Check again** after fixing it.

**An update rolled back.** The history says why. Your data is untouched. Run the doctor, fix what fails, then update again: automatic updates skip that release until an admin updates to it by hand. See [Updates](./updates.md#when-an-update-fails).

**`update` says there isn't enough space.** Releases are kept in `~/.coderhelm/releases`. Delete old ones (keep the one you run), or set `CODERHELM_HOME` to a bigger disk.

## Links point to localhost

Links in comments and emails come from the public URL. Without a domain it is read from SSM (`/coderhelm-<stage>/public-url`) when a function starts; if a function started before the first deploy finished, it retries every 30 seconds. Check the parameter exists, then send a request (any page) to refresh it.
