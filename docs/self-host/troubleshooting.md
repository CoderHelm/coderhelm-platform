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

- **Signed in to AWS**\
  `aws sso login --profile <name>` (expired), or `aws configure sso` (no profile yet).
- **The right AWS account**\
  `export AWS_PROFILE=<profile for that account>`.
- **AWS permissions for the install**\
  The missing actions are listed. Use a role with AdministratorAccess, or ask your AWS admin to allow them. If it says the permissions couldn't be checked, your role lacks `iam:SimulatePrincipalPolicy`: carry on if you have AdministratorAccess.
- **Region has every service**\
  Pick a region the check doesn't flag (us-east-1, us-west-2, eu-west-1 and eu-central-1 have everything).
- **CDK set up**\
  `npx -y cdk bootstrap aws://<account>/<region>` (the installer offers to run it; with a domain outside us-east-1, us-east-1 too). If the `CDKToolkit` stack is in a failed state, delete it first.
- **TLS certificate issued**\
  Add the validation records the check prints, or request a certificate in us-east-1 for the exact hostname.
- **Claude models enabled in Bedrock**\
  Open the Model access link the check prints and enable the models it names.
- **Anthropic API key works**\
  401: copy the key again. 403 or 404: the key's workspace can't use the model. "No credit": add credit in the Anthropic console.
- **Email sender verified**\
  Add the three DKIM records the check prints at your DNS provider.
- **Email can reach anyone**\
  A warning: request SES production access with the link the check prints.
- **Slack or Teams webhook**\
  Create a new incoming webhook; the old one was removed.

## Installing

**"Invalid CoderHelm config"** at synth. The message lists every problem with the setting's name; fix them in `coderhelm.config.json`. See the [Config reference](./config-reference.md).

**"Your AWS credentials are for account X, but the config deploys to Y."** Switch profile (`export AWS_PROFILE=...`) or sign in again (`aws sso login`).

**"This stack uses assets, so the toolkit stack must be deployed"** or a bootstrap error. Run `npx cdk bootstrap aws://<account>/<region>` with administrator credentials; with a domain outside us-east-1, bootstrap us-east-1 too. The installer does both.

**A bucket "already exists".** S3 bucket names are global. Keep the default `globalNameSuffix` (your account id) or set your own.

**The deploy fails on the user pool with an SES error.** `email.cognitoUsesSes` is true but the SES identity isn't verified yet. Set it to false, deploy, verify the identity, then turn it on. See [Email](./email.md).

**Certificate validation never finishes.** The validation record must be in the zone public DNS uses for your domain (the one your registrar points to), and the certificate must be in us-east-1.

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

**The update says it can't install yet.** **Install health** on **Settings, then System** lists the check that stops it (marked **Stops updates**), with its fix. Press **Re-run checks** after fixing it. **Update to vX needs your action** is the model check instead: fix the model access, then **Re-run the model check**.

**The update waits: "needs your OK", "ready to apply" or "needs an AWS admin".** It is prepared and waits for a person. **Review and apply** lists the data or permission changes to confirm; **Apply vX** applies it; **needs an AWS admin** means it changes the deploy role or a permissions boundary, so someone with administrator rights executes it in the AWS console. Admins get one email when it starts waiting, and one reminder after 7 days. See [The page, state by state](./updates.md#the-page-state-by-state).

**"This update was prepared without the deploy role".** The install runs v0.8.28 to v0.8.38, whose apply check was wrong. Execute that one change set in the CloudFormation console; from v0.8.39 on, updates apply from the page. See [Installs on v0.8.28 to v0.8.38](./updates.md#installs-on-v0828-to-v0838).

**"Checking vX didn't finish".** The release check stopped without an answer, or had none for 30 minutes. Nothing changed. Press **Check again**; if it stalls again, the reason it shows names the step.

**No email about a new release.** That is expected: a new release and a finished update show only in **Settings, then System**. Emails go out when a person must act or something went wrong, plus a weekly **What's new** digest. See [Emails about updates](./updates.md#emails-about-updates).

**An update rolled back.** The history says why. Your data is untouched. Run the doctor, fix what fails, then update again: automatic updates skip that release until an admin updates to it by hand. See [Updates](./updates.md#when-an-update-fails).

**`update` says there isn't enough space.** Releases are kept in `~/.coderhelm/releases`. Delete old ones (keep the one you run), or set `CODERHELM_HOME` to a bigger disk.

## Links point to localhost

Links in comments and emails come from the public URL. Without a domain it is read from SSM (`/coderhelm-<stage>/public-url`) when a function starts; if a function started before the first deploy finished, it retries every 30 seconds. Check the parameter exists, then send a request (any page) to refresh it.
