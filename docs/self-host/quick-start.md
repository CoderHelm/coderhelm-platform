---
title: Quick start
description: Install CoderHelm in your AWS account in about 30 minutes, most of it waiting for CloudFront.
order: 1
---

You need an AWS account you can administer, Node.js 20 or newer and the AWS CLI v2 signed in to that account. See [Requirements](./requirements.md) for the full list.

## 1. Write the config

```sh
mkdir coderhelm && cd coderhelm
npx -y github:CoderHelm/coderhelm-platform init
```

`init` checks your tools and AWS credentials, then asks:

- the AWS account and region (it suggests the ones your credentials use)
- a short name for the install (`prod` by default)
- your own hostname, or none to use a CloudFront URL
- the model provider: `anthropic` (each team adds its own key) or `bedrock` (Claude in your AWS account)
- your email (you become the first admin) and the address emails come from
- an email domain whose people may sign up
- whether to send anonymous install pings (off unless you say yes)

It writes `coderhelm.config.json`. Every setting is in the [Config reference](./config-reference.md); you can edit the file and deploy again at any time.

## 2. Deploy

```sh
npx -y github:CoderHelm/coderhelm-platform deploy
```

`deploy` downloads the latest release, checks its checksums and build provenance, creates the deployment's secret, bootstraps CDK, and deploys every stack. The first deploy takes 20 to 30 minutes; CloudFront is most of it.

When the stacks are up it opens a page in your browser to create your GitHub App. Press **Create GitHub App** on github.com; the installer stores the app's keys in your Secrets Manager and restarts the services. See [The GitHub App](./github-app.md).

At the end it prints:

```
CoderHelm is running at https://d1234abcd.cloudfront.net
Sign up as the first admin (you@example.com): https://d1234abcd.cloudfront.net/?signup=1
Install the app on your repositories: https://github.com/apps/<your-app>/installations/new
```

## 3. Sign up and connect

1. Open the sign-up link and create your account with the admin email from your config.
2. Install your GitHub App on the repositories CoderHelm should work on.
3. With the `anthropic` provider, add your team's Anthropic API key in **Settings → Model Provider**. With `bedrock`, there is nothing to add.
4. Assign an issue to CoderHelm, or add the `coderhelm` label, and watch the run on the dashboard.

## 4. Check it

```sh
npx -y github:CoderHelm/coderhelm-platform health
```

It checks the stacks, the functions, the dead-letter queue, the GitHub App keys, email sending and both URLs, and says what to do about anything that fails.

## Launch Stack

To install from the AWS console instead, create a stack from the template the release publishes:

- Template URL: `https://coderhelm-public.s3.amazonaws.com/cfn/coderhelm-selfhost-launch.yaml`
- One-click link (us-east-1; change the region in the console):
  `https://console.aws.amazon.com/cloudformation/home?region=us-east-1#/stacks/quickcreate?stackName=CoderHelmInstaller&templateURL=https://coderhelm-public.s3.amazonaws.com/cfn/coderhelm-selfhost-launch.yaml`

The stack asks the same questions as `init`, then starts a CodeBuild project that runs the installer in your account. It finishes when CoderHelm is running and shows the URL. Because creating a GitHub App needs a browser, the stack's **NextStep** output gives the two commands to run from your computer afterwards (`pull`, then `github-app`).

The template is also attached to every release as `launch-stack.yaml`.

## Pin a release

Commands use the latest release unless you name one:

```sh
npx -y github:CoderHelm/coderhelm-platform#v1.2.0 deploy --version v1.2.0
```

The installed version is recorded in `.coderhelm/state.json` next to your config, and in SSM in your account so any machine can [pull](./upgrades.md#another-machine) it.
