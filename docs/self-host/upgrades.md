---
title: Upgrades and rollbacks
description: Move to a new release with a migration check, see what changes first, and go back if you need to.
order: 9
---

## Upgrade

```sh
npx -y github:CoderHelm/coderhelm-platform upgrade              # the latest release
npx -y github:CoderHelm/coderhelm-platform upgrade --to v1.4.0  # a given one
```

`upgrade`:

1. downloads the target release and checks its checksums and build provenance;
2. reads `selfhost/MIGRATIONS.json` from it and prints the note of every release between yours and the target. If one of them is marked as needing action, it stops and says what to do; run it again with `--yes` once done;
3. runs `cdk diff` so you see every change before it happens, and asks to continue;
4. deploys, and records the new version (and the old one, for rollback).

Releases are plain redeploys unless their note says otherwise: tables and buckets keep their data, and items only ever gain fields, so an older release can still read data a newer one wrote.

## Roll back

```sh
npx -y github:CoderHelm/coderhelm-platform rollback              # the version before the last upgrade
npx -y github:CoderHelm/coderhelm-platform rollback --to v1.3.2
```

Rollback deploys the older release the same way. CloudFormation also rolls a stack back by itself if a deploy fails halfway.

## Another machine

The installer keeps your config and the installed version in SSM in your account (`/coderhelm-<stage>/installer/config` and `/state`; no secrets in either). To manage an install from a new machine:

```sh
npx -y github:CoderHelm/coderhelm-platform pull --stage prod --region us-east-1
npx -y github:CoderHelm/coderhelm-platform upgrade
```

## Installed with Launch Stack

Either use the commands above after `pull`, or open the `coderhelm-<stage>-installer` project in CodeBuild and **Start build with overrides**, setting `VERSION` to the release you want.

## Release artifacts

Each release (a `vX.Y.Z` tag) has, on its GitHub release page:

| File | What |
|---|---|
| `gateway.zip`, `worker.zip` | The Rust Lambdas, built for ARM64 |
| `mcp-proxy.zip` | The MCP proxy Lambda |
| `dashboard.tar.gz` | The dashboard (it reads its API location at runtime) |
| `source.tar.gz` | The repository at the tag: the CDK app the installer deploys |
| `coderhelm-selfhost.tgz` | The installer |
| `install.sh`, `launch-stack.yaml` | The install script and the Launch Stack template |
| `SHA256SUMS` | Checksums of all of the above |

Every artifact has a build provenance attestation: proof it was built by this repository's release workflow from that tag. The installer checks it with the GitHub CLI when it is installed (`gh attestation verify <file> --repo CoderHelm/coderhelm-platform`), and checks the checksums always.
