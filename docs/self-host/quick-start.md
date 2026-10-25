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

The guided install runs one step at a time. Each step explains what it asks and suggests an answer (press Enter to keep it). Type `back` at any question to return to the previous step. Your answers are saved after each step: if you stop, running the same command again picks up where you left off (`--restart` starts over).

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

Node.js and the AWS CLI are checked first. If you aren't signed in, it lists your AWS profiles, and for an SSO profile it runs `aws sso login` for you.

## Step 2: where to install

```
Step 2 of 8 [████░░░░░░░░░░░░] Where to install
AWS account [123456789012]:
AWS region [eu-west-1]:
A short name for this install [prod]:
```

The account must be the one you're signed in to. The short name prefixes every resource (`coderhelm-prod-...`), so two installs can share an account.

## Step 3: web address

```
Step 3 of 8 [██████░░░░░░░░░░] Web address
CoderHelm needs one web address. Without a domain it uses CloudFront's own (https://dxxxx.cloudfront.net), ...
  Route 53 zones in this account: example.com
Hostname (empty for the CloudFront address) [coderhelm.example.com]:
  example.com is in Route 53: the certificate and DNS records are created for you.
```

Leave it empty to use CloudFront's address. A hostname outside Route 53 asks for an ACM certificate in us-east-1; see [Domains](./domains.md).

## Step 4: Claude access

```
Step 4 of 8 [████████░░░░░░░░] Claude access
Claude through anthropic or bedrock [anthropic]: bedrock
Bedrock inference profile prefix [eu.]:
  ok   anthropic.claude-opus-5-5
  TODO anthropic.claude-sonnet-5-5 is not enabled in this region
  Enable model access here before your first run: https://console.aws.amazon.com/bedrock/home?region=eu-west-1#/modelaccess
```

With `anthropic`, you can paste a key to check it with one free call to Anthropic. The key is not saved: teams add their own in **Settings → AI models**. With `bedrock`, it checks that each model is enabled in your region. See [Model providers](./model-providers.md).

## Step 5: you and email

```
Step 5 of 8 [██████████░░░░░░] You and email
Your email: you@example.com
Emails come from [coderhelm@example.com]:
  example.com is your Route 53 zone: the deploy adds its DKIM records and SES verifies it in minutes.
  This account is in the SES sandbox: email only reaches verified addresses, 200 a day. ...
Let anyone with this email domain sign up (empty: only you) [example.com]:
```

You become the first admin. It checks the sender's SES identity and whether the account is still in the SES sandbox. See [Email](./email.md).

## Step 6: optional extras

```
Step 6 of 8 [████████████░░░░] Optional extras
  Jira: connect from Settings → Jira after sign-up ...
A Slack or Teams incoming webhook for update notices (empty: email only):
Turn on Sign in with Google (needs a Google OAuth client)? (yes/no) [no]:
```

Google's client id and secret go straight into Secrets Manager, never into a file. See [Integrations](./integrations.md).

## Step 7: updates

```
Step 7 of 8 [██████████████░░] Updates
Install updates automatically? (yes/no) [no]: yes
Maintenance window ("Sun 03:00", "daily 02:00") [Sun 03:00]:
Time zone of the window [Europe/Berlin]:
Major releases: ask (wait for an admin) or auto [ask]:
```

You can change these any time in **Settings → System**. See [Updates](./updates.md).

## Step 8: summary and cost

```
Step 8 of 8 [████████████████] Summary and cost
Here is what will be installed:
  Account / region   123456789012 / eu-west-1 (install "prod")
  Address            https://coderhelm.example.com
  Claude             Amazon Bedrock (eu.)
  ...
Estimated AWS cost per month for a small team (USD):
  Lambda, API Gateway, SQS, DynamoDB, EventBridge    2–15
  ...
  Total                                              about 11–62
  Not included: Claude on Bedrock: per token, on this AWS bill.
Save this and deploy now? ("back" to change something) (yes/no) [yes]:
```

It saves `coderhelm.config.json` (every setting is in the [Config reference](./config-reference.md)) and deploys.

## Deploy, GitHub App, first sign-up

The deploy shows each resource as CloudFormation creates it; the first one takes 20 to 30 minutes. Then:

1. Your browser opens a page that creates your GitHub App. Press **Create GitHub App** on github.com. The keys go into Secrets Manager. See [The GitHub App](./github-app.md).
2. The sign-up page opens. Sign up with the admin email.
3. The terminal prints a checklist:

```
CoderHelm v1.0.0 is running at https://coderhelm.example.com

Checklist
  [x] CoderHelm deployed
  [x] GitHub App created
  [ ] Sign up as the first admin (you@example.com): https://coderhelm.example.com/?signup=1
  [ ] Install the GitHub App on your repositories
  [x] Models: Amazon Bedrock (no key needed)
  [ ] Send a test email from the setup page (new AWS accounts start in the SES sandbox)
  [x] Auto-update on

The setup page checks each of these live: https://coderhelm.example.com/settings/setup/
```

## The setup page

After you sign up, the dashboard shows the setup checklist to the first admin, at **Settings → Setup**. Each item checks itself live and links to where you fix it:

- GitHub App connected
- repositories enabled
- model provider working
- email working (it sends you a test)
- Jira or Slack connected (optional)
- auto-update on
- first run tried

## Without a terminal (CI)

Every question has a flag, and `--answers <file>` reads them from JSON. A bad answer stops with the reason:

```sh
npx -y github:CoderHelm/coderhelm-platform init --yes \
  --region eu-west-1 --admin you@example.com --provider bedrock --bedrock-prefix eu. \
  --auto-update on --window "Sun 03:00" --timezone Europe/Berlin --majors ask --deploy yes
```

## Launch Stack

The [install guide](https://docs.coderhelm.com/self-host/install/) builds this link for you and follows the install. To use the template directly:

- Template URL, by release: `https://coderhelm-public.s3.amazonaws.com/launch/v0.7.0/coderhelm-launch.yaml` (the newest stable release is named in `https://coderhelm-public.s3.amazonaws.com/launch/latest.json`)
- Quick-create link (replace both regions with yours):
  `https://us-east-1.console.aws.amazon.com/cloudformation/home?region=us-east-1#/stacks/create/review?stackName=coderhelm-prod&templateURL=https://coderhelm-public.s3.amazonaws.com/launch/v0.7.0/coderhelm-launch.yaml&param_AdminEmail=you@example.com`

Only your email is required. Tick the box that lets the stack create IAM roles and press **Create stack**. A CodeBuild project in your account downloads the release, checks its signature and checksums, deploys every CoderHelm stack (about 20 minutes) and finishes with your URL in the stack's **Outputs**. Its log stays in CloudWatch under `/aws/codebuild/<stack name>-installer`, also if the stack is rolled back.

Without the guide's setup key, create the GitHub App from your computer with the two commands in the **GitHubAppWithoutTheGuide** output (`pull`, then `github-app`).

## Check it

```sh
npx -y github:CoderHelm/coderhelm-platform health
```
