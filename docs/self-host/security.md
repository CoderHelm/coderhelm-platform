---
title: Security model
description: What runs where, who can reach what, and how secrets and credentials are kept.
order: 11
---

## Trust boundaries

- **Everything runs in your AWS account.** CoderHelm the company has no access to it: no cross-account role, no support backdoor, no license check calling home.
- **Your GitHub App.** Created by you, installed by you, with the permissions listed in [The GitHub App](./github-app.md#what-the-app-may-do). Webhooks are verified with its secret before anything runs.
- **Model calls** go from your functions to the Anthropic API with a team's key, or to Bedrock in your account with no key at all.
- **Telemetry is off by default.** When you turn it on, only the installer sends a ping on install, upgrade, health check and uninstall: a random install id, the release, the event, the region, the layout and the provider. No account id, domain, email, repository or IP address is accepted by the receiver. See `installer/lib/commands.mjs` (`telemetryPayload`) and `services/gateway/src/routes/telemetry.rs`.

## Network paths

| From | To | How |
|---|---|---|
| People | CloudFront (your hostname) | HTTPS only, TLS 1.2+, HSTS |
| CloudFront | HTTP API, streaming function URL, dashboard bucket | HTTPS; the bucket is private and only CloudFront can read it |
| GitHub, Jira | `/webhooks/*` | Signed payloads, checked before use |
| Functions | GitHub, Anthropic, Bedrock, the services teams connect | HTTPS from Lambda (no VPC) |

## Identity and sessions

- Accounts live in a Cognito user pool in your account, with optional TOTP and passkeys. Sign-up is limited to `admins`, `signup.emails` and `signup.domains`.
- Sessions are `__Host-` cookies (HttpOnly, Secure, SameSite=Strict) signed with the deployment's own key, on your one hostname.
- Install admins (`admins`) must have an email CoderHelm verified itself; a GitHub profile email never counts.

## Secrets and encryption

- GitHub App keys and the session signing key are in one Secrets Manager secret. Only the gateway, streaming and worker roles can read it. The installer writes it from a temporary 0600 file and never prints it.
- Team credentials (MCP servers, data sources, chat webhooks) are sealed with a KMS key and bound to their team.
- Tables, the data bucket and queues are encrypted (customer managed KMS keys for tables and the bucket).

## Least privilege

- Each function has its own role with only the tables, queues, keys and actions it uses.
- The build sandbox runs a repository's own build scripts, which are untrusted: its CodeBuild role can only read and write one bucket of short-lived source tarballs and write its logs. No GitHub token, no tables, no KMS.
- Roles in teams' AWS accounts are assumed only by name (`CoderHelmLogReader`, `CoderhelmReadOnly*`) and with an external id.
- The installer and the Launch Stack's CodeBuild role need administrator rights to bootstrap CDK and create roles. Run later deploys with a narrower role if your policies require it.

## Supply chain

Releases are built by GitHub Actions from a tag, with checksums and build provenance attestations (Sigstore, through GitHub). The installer checks both. You can also deploy from source (`--source`) after reviewing the code.

## Reporting a problem

Report security issues privately to the support address in the CoderHelm repository rather than in a public issue.
