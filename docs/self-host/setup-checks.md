---
title: Setup checks and the install guide
description: How a self-hosted install reports its own setup, and how the browser install guide uses it. The contract for GET /api/setup/status and the GitHub App manifest landing.
order: 13
---

The [install guide](https://docs.coderhelm.com/self-host/install/) installs CoderHelm from your browser. It reads three things, all in your own account: the installer's progress file, your install's public `GET /api/setup/status`, and your install's GitHub App manifest landing. Nothing about your install passes through CoderHelm's servers. The hosted CoderHelm has neither route. The page counts anonymous page views with Google Analytics (origin and path only); nothing you type and nothing about your install is sent.

## How the install guide works

1. **Launch.** The guide makes a CloudFormation quick-create link with your region, install name and email filled in, plus two random values made in your browser:
   - a **progress ID** (16 characters), which names a small bucket, `coderhelm-setup-<id>`, in your account;
   - a **setup key** (40 characters), kept in your browser's storage and in the stack's parameters.

   Quick-create links ignore hidden (NoEcho) parameters, so both values appear in the stack's Parameters tab. The setup key works only once, only on your install, and only until a GitHub App exists.
2. **Progress.** The installer (CodeBuild, `selfhost/launch/bootstrap.mjs`) writes `status.json` to that bucket: the phase, a plain message, the step count, the part being deployed with its resources done so far (`part`: a label such as `API`, `done` and `total`, refreshed about every 15 seconds while it changes) and, at the end, your CoderHelm URL. With your own domain it also writes `domain`: the hostname, `mode` (`route53` when the deploy set it all up, `external` otherwise), `state`, and the DNS `records` to add (names and values, which are public in DNS anyway). Account numbers, ARNs and email addresses are removed before writing. The guide reads `https://coderhelm-setup-<id>.s3.<region>.amazonaws.com/status.json`. If your account blocks public bucket policies (S3 Block Public Access at account level), the file stays private and the guide asks you to paste the URL from the stack's Outputs instead. The install itself is unaffected.
3. **Checks.** Once the URL is known, the guide polls `GET <url>/api/setup/status` (below) and shows each check as waiting, passing or failing, with a fix.
4. **GitHub App.** The guide posts a GitHub App manifest to github.com with `redirect_url` set to `<url>/api/setup/github/callback` and `state` set to the setup key. GitHub sends the browser to your install with a one-time code. Your install exchanges the code for the app's keys, stores them in its own secret, restarts its services to read them and sends you back to the guide. The keys never leave your account.

## GET /api/setup/status

The guide's checks come from the install's public `GET /api/setup/status`, the same checks the CLI's `doctor` and the dashboard's setup checklist use. Its shape, codes, setup window and rate limit are documented in [Check your install](./check-your-install.md). The guide uses:

- `checks[]` (`id`, `label`, `status`, `code`, `required`) for `api`, `github_app`, `github_installed`, `models`, `email`, `domain`, `google_sign_in`, `updates` and `admin`, each code mapped to one sentence and a fix; an unknown check shows by its label.
- `domain`: when you chose your own domain, the guide shows a Domain step between AWS and GitHub with the records from the progress file, checks until the code is `ok`, then follows the install to the domain (so the GitHub App gets the domain's URLs). You can skip it and add the domain later in Settings, then System.
- `github_manifest_ready`: one click when true; the terminal command (`pull`, then `github-app`) when false.
- `setup_open: false` (someone has signed up): the install is shown as ready.

The answer carries no ids or URLs, so the guide makes its own links: the Bedrock console for the region it launched in, the models a new install calls (the release's defaults with the region's inference profile prefix), the SES console for that region, and `https://github.com/apps/<slug>/installations/new` from the slug the manifest landing hands back.

## GET /api/setup/github/callback

GitHub's manifest flow lands here with `code` and `state`. The install:

1. refuses when a GitHub App is already stored (`exists`), or when `state` is not its setup key (`bad_state`);
2. exchanges the code at `POST https://api.github.com/app-manifests/{code}/conversions` (`github_refused` when GitHub says no; codes last one hour and work once);
3. stores `github_app_id`, `github_private_key`, `github_webhook_secret`, `github_client_id`, `github_client_secret` and `github_app_slug` in its secret and deletes the setup key (`store_failed` when it cannot);
4. asks the updater to restart the gateway, streaming and worker functions (`refresh_secrets`) so they read the new keys;
5. redirects to `SETUP_RETURN_URL` (default `https://docs.coderhelm.com/self-host/install/`) with `?github=<outcome>&app=<slug>`, where the outcome is `created`, `created_restart_needed` or one of the errors above.

## Where it lives

- Manifest landing: `services/gateway/src/routes/setup.rs`
- Setup checks: `services/gateway/src/routes/setup_checks.rs`
- Launch Stack template: `infra/cfn/coderhelm-selfhost-launch.yaml`, pinned to each release by `selfhost/launch/stamp-template.mjs` and published to `s3://coderhelm-public/launch/<tag>/coderhelm-launch.yaml` by the release workflow
- Installer run by the stack: `selfhost/launch/bootstrap.mjs`
- Install guide: `docs/app/self-host/install/` in the CoderHelm/coderhelm repository
