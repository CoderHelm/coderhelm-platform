---
title: Quick start
description: The guided install, screen by screen. About 30 minutes, most of it waiting for CloudFront.
order: 1
---

**No terminal?** The [install guide](https://docs.coderhelm.com/self-host/install/) installs CoderHelm from your browser: one click opens AWS with everything filled in, then it creates your GitHub App, checks model access and confirms every step against your own install. You only need to be signed in to AWS and GitHub.

The rest of this page is the terminal install. You need an AWS account you can administer, Node.js 20 or newer and the AWS CLI v2. The guided install checks both and tells you how to fix anything missing. See [Requirements](./requirements.md).

```sh
mkdir coderhelm && cd coderhelm
npx -y github:CoderHelm/coderhelm-platform
```

The guided install runs one step at a time. Each step explains what it asks and suggests an answer (press Enter to keep it), then **checks what you did before it moves on**. Type `back` at any question to return to the previous step.

## When a check fails

Every check prints `PASS`, `WARN` or `FAIL`. A failure says what is wrong, why it matters and how to fix it, with a command to copy or a link, and waits:

```text
  FAIL  AWS permissions for the install
        What's wrong: Your AWS identity isn't allowed 2 things the install does: iam:PassRole, ecr:CreateRepository.
        Why it matters: The deploy stops halfway at the first of them.
        How to fix: Ask your AWS admin to allow them, or sign in with a role that has AdministratorAccess, then check again.
          https://console.aws.amazon.com/iam/home#/roles
Fix it, then press Enter to check again ("skip" to go on anyway, "back" to change an answer):
  Checking again...
  PASS  AWS permissions for the install  (18 actions allowed for Developer)
```

Fix it and press **Enter**: it checks again without starting over. Warnings (`WARN`) don't stop the install; fix them when you can. Every check and its fix is in [Check your install](./check-your-install.md). Your answers are saved after each step: if you stop, running the same command again picks up where you left off (`--restart` starts over).

## Step 1: your machine and AWS sign-in

```
CoderHelm setup. Type "back" at any question to return to the previous step.

Step 1 of 8 [██░░░░░░░░░░░░░░] Your machine and AWS sign-in
CoderHelm deploys with the AWS CLI and Node.js from this machine. Checking them.
You're not signed in to AWS. Profiles on this machine: dev, prod-admin
AWS profile to use [dev]: prod-admin
Sign in now with `aws sso login --profile prod-admin`? (yes/no) [yes]:
  Signed in to account 123456789012 as arn:aws:sts::123456789012:assumed-role/Admin/you
```

Node.js, the AWS CLI and tar are checked first. If you aren't signed in, it lists your AWS profiles, and for an SSO profile it runs `aws sso login` for you. If the sign-in still fails (an expired session, the wrong profile), it shows the command to run and waits.

## Step 2: where to install

```
Step 2 of 8 [████░░░░░░░░░░░░] Where to install
AWS account [123456789012]:
AWS region [eu-west-1]:
A short name for this install [prod]:
```

The account must be the one you're signed in to. The short name prefixes every resource (`coderhelm-prod-...`), so two installs can share an account.

Then it checks:

- **The region has every service** the install uses.
- **Your permissions:** it simulates the actions the deploy needs with `iam:SimulatePrincipalPolicy` and lists any you're missing.
- **CDK is set up** in the account and region (`cdk bootstrap`). If not, the deploy runs it, or you can run the command it shows now.

## Step 3: web address

```
Step 3 of 8 [██████░░░░░░░░░░] Web address
CoderHelm needs one web address. Without a domain it uses CloudFront's own (https://dxxxx.cloudfront.net), ...
  Route 53 zones in this account: example.com
Hostname (empty for the CloudFront address) [coderhelm.example.com]:
  example.com is in Route 53: the certificate and DNS records are created for you.
```

Leave it empty to use CloudFront's address. With a Route 53 zone it checks the zone exists and that **your domain's name servers point at it**: when they don't, it prints the four NS records to set at your registrar and what public DNS answers now. A hostname outside Route 53 asks for an ACM certificate in us-east-1 and checks it covers the hostname and is issued (or prints the validation records to add); see [Domains](./domains.md).

## Step 4: Claude access

```
Step 4 of 8 [████████░░░░░░░░] Claude access
Claude through anthropic or bedrock [anthropic]: bedrock
Bedrock inference profile prefix [eu.]:
  Checking Bedrock in this region...
  PASS  Region eu-west-1 has every service
  FAIL  Claude models enabled in Bedrock
        What's wrong: Not enabled in eu-west-1: Claude Opus 5.5 (anthropic.claude-opus-5-5), Claude Sonnet 5.5 (anthropic.claude-sonnet-5-5).
        Why it matters: Every run calls these models; none can start until Bedrock allows them.
        How to fix: Open Model access, choose Modify model access, tick these models, submit, then check again (it usually takes a minute).
          https://console.aws.amazon.com/bedrock/home?region=eu-west-1#/modelaccess
Fix it, then press Enter to check again ("skip" to go on anyway, "back" to change an answer):
```

With `anthropic`, you can paste a key to check it with a real one-token call to Anthropic (it costs a fraction of a cent). The key is not saved: teams add their own in **Settings, AI models**. With `bedrock`, it checks that each model is enabled in your region and names any that aren't. See [Model providers](./model-providers.md).

## Step 5: you and email

```
Step 5 of 8 [██████████░░░░░░] You and email
Your email: you@example.com
Emails come from [coderhelm@example.com]:
  example.com is your Route 53 zone: the deploy adds its DKIM records and SES verifies it in minutes.
  This account is in the SES sandbox: email only reaches verified addresses, 200 a day. ...
This install is for one organization. Let people from your company join: an email domain such as example.com (empty: only people you invite) []:
```

You become the first admin. Nobody else can sign up unless you invite them or allow their email domain (change it later in **Settings, Sign-in**; see [Sign-in and people](./sign-in.md)). It checks the sender's SES identity (verified, with DKIM; when not, it prints the three DNS records to add) and whether the account is still in the SES sandbox (a warning, with the link to request production access). See [Email](./email.md).

## Step 6: optional extras

```
Step 6 of 8 [████████████░░░░] Optional extras
  Jira: connect from Settings, Integrations, Jira after sign-up ...
A Slack or Teams incoming webhook for update notices (empty: email only):
```

A Slack or Teams webhook is checked to still exist. See [Integrations](./integrations.md).

## Step 7: updates

```
Step 7 of 8 [██████████████░░] Updates
Install updates automatically? (yes/no) [no]: yes
Maintenance window ("Sun 03:00", "daily 02:00") [Sun 03:00]:
Time zone of the window [Europe/Berlin]:
Major releases: ask (wait for an admin) or auto [ask]:
```

You can change these any time in **Settings, then System**. See [Updates](./updates.md).

## Step 8: summary and cost

```
Step 8 of 8 [████████████████] Summary and cost
Here is what will be installed:
  Account / region   123456789012 / eu-west-1 (install "prod")
  Address            https://coderhelm.example.com
  Claude             Amazon Bedrock (eu.)
  ...
Estimated AWS cost per month for a small team (USD):
  Lambda, API Gateway, SQS, DynamoDB, EventBridge    2 to 15
  ...
  Total                                              about 11 to 62
  Not included: Claude on Bedrock: per token, on this AWS bill.
Save this and deploy now? ("back" to change something) (yes/no) [yes]:
```

It saves `coderhelm.config.json` (every setting is in the [Config reference](./config-reference.md)) and deploys.

## Deploy, GitHub App, first sign-up

The deploy shows each resource as CloudFormation creates it; the first one takes 20 to 30 minutes. Then:

1. Your browser opens a page that creates your GitHub App. Press **Create GitHub App** on github.com. The keys go into Secrets Manager. See [The GitHub App](./github-app.md).
2. It runs every check against the running install (the same as the `doctor` command): DNS and the certificate, the stacks and functions, the GitHub App's keys, permissions and webhook deliveries, email, and a real model call. Anything that fails says how to fix it, and you press **Enter** to check again.
3. Only when nothing fails does it say so:

```
Your install is ready.

  Dashboard: https://coderhelm.example.com

Next steps
  1. Sign up as the first admin: https://coderhelm.example.com/?signup=1
  2. Turn CoderHelm on for a repository in Settings, Repositories
  3. Assign an issue to CoderHelm, or add the "coderhelm" label
  4. Settings, then Setup shows these checks live: https://coderhelm.example.com/settings/setup/

Check again any time: npx -y github:CoderHelm/coderhelm-platform doctor
```

## The setup page

After you sign up, **Settings, then Setup** shows two lists to install admins:

- **Install checks:** the install checks itself (the web address, the GitHub App's keys, permissions, installations and webhook deliveries, the email sender and DKIM, the SES sandbox, a real model call, failed events, the updater, and Slack, Teams and Jira when you use them). Each problem says what is wrong, why it matters and how to fix it, with a command to copy or a link. **Check again** reruns them.
- **Getting started:** repositories turned on, a test email, your first run, and the optional extras.

## Without a terminal (CI)

Every question has a flag, and `--answers <file>` reads them from JSON. A bad answer, or a check that fails, stops with the reason and the fix (`--skip-checks <id>` goes past a check):

```sh
npx -y github:CoderHelm/coderhelm-platform init --yes \
  --region eu-west-1 --admin you@example.com --provider bedrock --bedrock-prefix eu. \
  --auto-update on --window "Sun 03:00" --timezone Europe/Berlin --majors ask --deploy yes
```

## Launch Stack

The [install guide](https://docs.coderhelm.com/self-host/install/) builds this link for you and follows the install. To use the template directly:

- Template URL, by release: `https://coderhelm-public.s3.amazonaws.com/launch/<version>/coderhelm.json`. Use the newest release: `https://coderhelm-public.s3.amazonaws.com/launch/latest.json` names it, its template URL (`templateUrl`) and the template's sha256 (`templateSha256`). Releases are cut automatically after each update of CoderHelm, so pick the version from that file rather than writing one down.
- Quick-create link (replace both regions with yours, and `<version>` with the one in `latest.json`):
  `https://us-east-1.console.aws.amazon.com/cloudformation/home?region=us-east-1#/stacks/create/review?stackName=coderhelm-prod&templateURL=https://coderhelm-public.s3.amazonaws.com/launch/<version>/coderhelm.json&param_AdminEmail=you@example.com`

  Name the stack `coderhelm-<name>`, with the same name as the `Stage` parameter (default `prod`).

Only your email is required. The optional parameters: `Stage`, `ModelProvider`, `KeepDataOnDelete` (default `true`: deleting the stack keeps the data, see [Uninstall](./uninstall.md)), `Hostname` (your own domain: CoderHelm requests the certificate, see [Domains](./domains.md)), `EmailTransport` and `EmailFrom`, `SignupDomain` (empty: only people you invite) and `RequireTwoFactor`, `AutoUpdate` with `UpdateDay`, `UpdateHour` and `UpdateTimezone`, and `BedrockInferencePrefix`. [Everything you can set up](./options.md) lists each with its config setting. Before you launch, you can [verify the template](./updates-security.md#verify-a-release-before-launching), and read [What an install can do in your account](./security.md#what-an-install-can-do-in-your-account): what the stack creates, the permissions boundary on every role, and what an update may do.

Tick the boxes that let the stack create IAM resources with custom names and expand nested stacks, and press **Create stack**. CloudFormation runs with your permissions. The stack copies the release's files into a private bucket in your account, checking each against the sha256 the template lists, then creates every part of CoderHelm as a nested stack (about 20 minutes). It finishes with your URL in the stack's **Outputs** (`CoderHelm`, and `SignUpUrl` for the first admin). If it fails, the stack's **Events** tab names the first error, and CloudFormation rolls it back and deletes what it created.

Without the guide's setup key, create the GitHub App from your computer: `coderhelm-selfhost pull --stage <name> --region <region>`, then `coderhelm-selfhost github-app`.

## Check it

```sh
npx -y github:CoderHelm/coderhelm-platform doctor
```

See [Check your install](./check-your-install.md).
