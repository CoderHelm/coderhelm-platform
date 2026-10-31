---
title: Migrate to one stack
description: Move an install from before native releases to one CloudFormation stack, keeping every table, bucket, user pool, key and secret.
order: 9.7
---

# Migrate to one stack

Since native releases, an install is one CloudFormation stack, `coderhelm-<name>`, made of nested stacks. You update it with a stack update and remove it with **Delete stack** ([Security](./security.md#what-an-install-can-do-in-your-account)).

## Who needs this

Before it runs anything, `migrate` checks the release's signature (cosign on its `SHA256SUMS`, as an update does) and that `coderhelm.json` matches its signed checksum, and after CloudFormation makes the change set it checks that the template CloudFormation fetched is that same file.

An install name with a hyphen (`prod-eu`) can't move: one-stack installs don't allow hyphens, so that `coderhelm-prod-*` never also names another install. Use the fallback below.

You need this page if your install is from before native releases: it was made with the command line or the earlier Launch Stack, and CloudFormation shows about eleven top-level stacks named `coderhelm-<name>-database`, `coderhelm-<name>-api` and so on. An install made with the current Launch Stack is already one stack.

`coderhelm-selfhost migrate` moves such an install to one stack without moving its data: the tables, buckets, user pool, keys and secret stay exactly where they are, under the same names.

## Before you start

- Run it from a machine with the install's `coderhelm.config.json` (on a new machine, `coderhelm-selfhost pull --stage <name> --region <region>` writes it) and administrator credentials for the account. It uses the AWS CLI.
- Pick a quiet time. The update takes about 20 minutes, and queued work waits a few minutes while its event sources are made again.
- If you installed with the earlier Launch Stack, its stack is usually named `coderhelm-<name>`, the name the new stack needs. Delete that Launch Stack first (`migrate` stops and says so if it finds it). Its permissions boundary, deploy role and assets bucket are kept when it is deleted, and CoderHelm keeps running without it. Keep those three until the migration has finished.

## Run it

```sh
npx -y github:CoderHelm/coderhelm-platform migrate --yes
```

It moves to the latest release; `--version vX.Y.Z` picks another. It works in this order:

1. **Saves the parts' templates.** It finds the install's part stacks (`coderhelm-<name>-*`, never another install whose name starts with this one's, never the access stack). It copies each one's current template to a new private bucket, `coderhelm-<name>-migrate-<account>`, because CloudFormation reads nested templates from S3.
2. **Nests the parts.** It creates the stack `coderhelm-<name>` with an import change set that takes in every part stack as it is, with a deletion policy of Retain. Nothing inside the parts changes, and no table, bucket, user pool or key changes owner one by one.
3. **Adds the secret.** A second import adds `coderhelm/<name>/secrets` to the new stack, also with Retain.
4. **Prepares the update and checks it.** It creates the change set `coderhelm-migrate-update` to the release's `coderhelm.json`, with the nested change sets of every part, and with the parameters read from your config (admin, email, model provider, domain, sign-up, updates; **KeepDataOnDelete** `true`). Then it reads every one of those change sets. This is the safety gate: if any table, bucket, user pool, KMS key or secret would be removed or replaced (or might be replaced), it deletes the change set and stops.
5. **Clears the way.** Just before the update, it deletes what the update would otherwise collide with, and prints each one:
   - event source mappings the update replaces. Lambda refuses a second mapping for the same queue and function, so the old one goes first; messages wait in their queues for the few minutes in between;
   - log groups that Lambda made by itself for the install's functions and that the new stack now declares (so **Delete stack** removes them later). CloudFormation can't create a log group that already exists. Their old log lines go with them.
6. **Runs the update** and waits for it to finish.

Without `--yes`, it stops after step 4, once the checked change set is ready, and prints what step 5 would delete. Steps 1 to 3 have run by then: the parts are nested and the install keeps running as before. To finish, review the change set `coderhelm-migrate-update` on the stack `coderhelm-<name>` in the CloudFormation console, then run `migrate --yes`: it sees the nested stack, carries on from step 4 (making the change set again and checking it again), deletes what it listed, and runs the update.

## Clean up afterwards

When it finishes, `coderhelm-<name>` is one stack. List what is left of the old layout:

```sh
npx -y github:CoderHelm/coderhelm-platform leftovers --stage <name> --region <region>
```

The pieces to remove are the earlier Launch Stack's permissions boundary (`coderhelm-<name>-boundary-<id>`), deploy role (`coderhelm-<name>-deploy-<id>`) and assets bucket (`coderhelm-<name>-assets-<id>-<account>`), if you had them, and the migration bucket `coderhelm-<name>-migrate-<account>`. `leftovers --delete` refuses while the stack exists, so remove these in the IAM and S3 consoles (empty a bucket before deleting it, and delete the role before the boundary). Everything else the listing names belongs to the new stack.

Then check the install the usual way ([Check your install](./check-your-install.md)), and `selfhost/check-access.mjs` ([Security](./security.md#check-it-yourself)).

## If the check stops

The command names each change that stopped it, as `part.resource (type): action, replacement`. Nothing was changed except the nesting: the install keeps running as before, inside `coderhelm-<name>`, and the change set was deleted.

- Read what the change is. A replacement usually means a property AWS can't change in place differs between your install and the release, for example a table or bucket name built from a custom prefix, or a setting changed by hand.
- If you can bring your install in line (update it to the latest release the old way first, or undo a hand change), do that, then run `migrate` again.
- Otherwise use the fallback below. Don't execute a change set that would replace data.

Running `migrate` again carries on from the check: it makes the change set again and checks it again. To start over instead, delete the stack `coderhelm-<name>`: the parts and the secret are kept, because they were imported with Retain.

If the update itself fails, CloudFormation rolls it back to the nested parts as they were, and the stack's **Events** tab names the first error.

## The fallback: a fresh install

If the install can't be migrated:

1. Install CoderHelm again under a new name with Launch Stack (for example `coderhelm-prod2`).
2. Move your data across with the copy steps in [Backups](./backups.md#restore). People reset their passwords in the new user pool.
3. Move your GitHub App to it, or create a new one.
4. When the new install works, remove the old one with `coderhelm-selfhost uninstall --stage <old name> --region <region>` ([Uninstall](./uninstall.md#installs-from-before-native-releases)).
