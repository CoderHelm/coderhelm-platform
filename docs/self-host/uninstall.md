---
title: Uninstall
description: Remove CoderHelm from your AWS account, keeping its data or deleting everything, from the browser or a terminal, and check that nothing is left.
order: 12
---

# Uninstall

Two ways, with the same result:

- **From the browser:** the [guided uninstall](https://docs.coderhelm.com/self-host/uninstall/). Admins open it from **Settings, System, Uninstall**, with the install filled in.
- **From a terminal:** `npx -y github:CoderHelm/coderhelm-platform uninstall`, with credentials for the account.

Both show what will go before anything is deleted, ask you to type the install's name, then end with a check of what is left.

## Keep the data, or delete everything

| | Remove CoderHelm, keep my data | Delete everything |
| --- | --- | --- |
| CoderHelm's stacks: functions, queues, API, CloudFront, dashboard, schedules, alarms, the build sandbox | deleted | deleted |
| The Launch Stack that installed it: its installer, window and starter functions and roles | deleted | deleted |
| The permissions boundary and the deploy role (`coderhelm-<name>-boundary-<id>`, `coderhelm-<name>-deploy-<id>`) | deleted | deleted |
| The assets bucket and the install guide's progress bucket (`coderhelm-setup-<id>`) | emptied and deleted | emptied and deleted |
| Log groups (`/aws/lambda/coderhelm-<name>-*`, `/aws/codebuild/coderhelm-<name>-*`) and parameters (`/coderhelm-<name>/...`) | deleted | deleted |
| DynamoDB tables and their on-demand backups | kept | deleted |
| The data bucket (`coderhelm-<name>-data-<account>`), every version too | kept | emptied and deleted |
| The Cognito user pool (everyone's sign-in) | kept | deleted |
| The secret `coderhelm/<name>/secrets` | kept | deleted at once, no recovery window |
| The KMS keys | kept | scheduled for deletion in 7 days, AWS's shortest wait |

Delete everything can't be undone. The one thing you can stop is the KMS keys: until their deletion date, **Cancel key deletion** in the KMS console keeps them, though the data they encrypted is already gone.

Not removed, because CoderHelm didn't create them or they aren't only CoderHelm's:

- A certificate the updater requested for your own domain. It is in us-east-1 whatever the install's region, so it is listed (tagged `coderhelm = coderhelm-<name>`) for you to delete in ACM once no install of that name uses it.
- DNS records you added at another DNS provider for your own domain.
- The read-only roles your teams created for CoderHelm (`CoderHelmLogReader`, `CoderhelmReadOnly`): delete their stacks in each account.
- Your GitHub App: uninstall it from GitHub (Settings, Developer settings, GitHub Apps).
- AWS Bedrock model access and SES settings for the account.

## Hybrid installs

An install registered with CoderHelm leaves first, before anything is deleted:

- its `<name>.coderhelm.com` address is released;
- its GitHub installations and its Jira site are unlinked, so their events go back to CoderHelm hosted;
- its registration is turned off, so CoderHelm refuses its broker key and its sign-ins from then on.

If CoderHelm forwards a team's GitHub or Jira events to this install today, the guided uninstall and Settings, System, Uninstall say so before you confirm. Those repositories and that Jira site are then served by CoderHelm hosted again, as if they had never moved.

## The guided uninstall

The guide opens the CloudFormation console on a quick-create page for the uninstall stack (`coderhelm--uninstall-<name>-<id>`) with everything filled in, including your choice and the name you typed to confirm (`ConfirmStage`; the stack refuses to start unless it equals `Stage`). Tick the IAM box and press **Create stack**. Launching the template by hand, you type the name in both fields. It takes 10 to 40 minutes; the guide follows it through a small progress file and ends with the list of what went, what was kept and anything left.

When it ends, the stack rolls itself back. That is how the uninstaller removes itself: CloudFormation deletes its pieces with your credentials, because an IAM role can't delete itself. CloudFormation then lists the stack as `ROLLBACK_COMPLETE` with the reason "Done: CoderHelm is uninstalled". It holds nothing and costs nothing; delete it from the list if you like. After a finished uninstall the uninstaller's log group and progress bucket go too. After a failure they stay, so the reason stays readable; launch the uninstaller again and it picks up where it stopped.

Keep the console's default **Roll back all stack resources** on failure. With **Preserve successfully provisioned resources**, the uninstaller's own pieces stay after it ends; they can't delete anything more (the window function refuses once the stack has ended), and you delete the stack yourself.

### What the uninstaller may touch

The uninstall stack works like the [Launch Stack](./security.md#what-the-installer-can-do-in-your-account):

- Its pieces are named `coderhelm--uninstall-<name>-*`. An install's name starts with a letter, so no install's names (`coderhelm-<name>-*`), and nothing its deploy role may change, ever reach them.
- Its build role can delete nothing at rest. It can write its log and progress file, ask the window function for its window, and remove its window.
- The window function checks the build is this stack's own running build, started by its starter, with exactly this template's buildspec, image, variables and their values, no debug session and no other fleet, asking as that build's own session, while this stack is still being created. It checks the window's statements against a sha256 pinned in the function. Only then does it write the build role's inline policy `uninstall-window`, bound to that build's session and an end time 100 minutes on. The mode (keep or everything) and the install's name come from the stack's own parameters, never from the build.
- Something of the install must be in this region (a Launch Stack, a resource tagged for it or its secret), or the window is refused and nothing is deleted.
- A permissions boundary on all three of the uninstaller's roles caps them: no IAM users or keys, no Organizations, account, CloudTrail or GuardDuty changes, no IAM writes outside `coderhelm-<name>-*`, no role assumed, no new role or policy, and none of the uninstaller's own pieces changed by the uninstaller.

The window allows deleting only:

- stacks tagged `coderhelm:install = coderhelm-<name>`, through the install's own deploy role;
- the Launch Stack(s) named `coderhelm-<name>` or `coderhelm-<name>-<n>` with `Stage=<name>` and no service role, found in this region, and their functions, project, parameters and roles, by the Launch Stack's id (their custom resource only when CloudFormation invokes it);
- buckets by exact name: `coderhelm-<name>-assets-<id>-<account>`, the progress buckets of this install's Launch Stacks, and `-dashboard-<account>`, `-sandbox-<account>` and, when deleting everything, `-data-<account>` only when they are in this region (S3 names carry no region);
- a role, and its policies, only when it carries one of the boundaries of this install's Launch Stacks in this region, or ends in one of their ids (IAM is global, so a tag alone never counts); the boundaries by exact name;
- tables, KMS keys and user pools only while they carry the install's tag; KMS keys by scheduling their deletion (7 days); aliases, backups, log groups and parameters named for the install;
- the secret `coderhelm/<name>/...`.

An install whose name starts with this one's (`coderhelm-prod-eu` next to `coderhelm-prod`) is found by its tag, its secret, its Launch Stack and its permissions boundary, and every name pattern above is denied for it. The uninstaller's own log groups, which match the install's log names, are denied too. Another region's install of the same name is never touched: regional resources are named by region, and IAM pieces are found by the Launch Stack ids in this region.

`infra/scripts/deploy-role-sim.py --uninstall` checks this against AWS's IAM policy simulator: about a hundred calls, each allowed or refused as it should be in both modes, and the same calls refused by another session or after the window's end (`infra/test/fixtures/uninstall-sim.json`, replayed by `infra/test/uninstall-sim.test.ts`).

A window has to fit IAM's 10,240 characters for a role's inline policies. An install with several Launch Stack attempts, other installs whose names start with its own and a long name can need more; the window function then refuses before anything is deleted and says to use the terminal.

## From a terminal

```bash
# Remove CoderHelm, keep the data
npx -y github:CoderHelm/coderhelm-platform uninstall

# Delete everything (KMS keys after 7 days)
npx -y github:CoderHelm/coderhelm-platform uninstall --everything

# Without the install's coderhelm.config.json
npx -y github:CoderHelm/coderhelm-platform uninstall --stage prod --region us-east-1

# Only list what is left (read-only)
npx -y github:CoderHelm/coderhelm-platform uninstall --verify --stage prod --region us-east-1
```

It uses your own credentials, so it can also remove an install from before hardened installs. It stops before deleting anything unless something of the install (a Launch Stack, a resource tagged for it or its secret) is in that region. It finds the install's pieces the same way as the guided uninstall and touches only those: never another install whose name starts with this one's, never a resource of the install's name tagged for another install. `--dry-run` shows the list and deletes nothing; `--yes` skips the prompt. `--everything` is also `--delete-data`.

## Check that nothing is left

The guided uninstall ends with this check, and `uninstall --verify` runs it any time. It lists anything named `coderhelm-<name>-*`, `coderhelm/<name>/` or `/coderhelm-<name>/`, or tagged `coderhelm:install = coderhelm-<name>`, still in the region: stacks, tables, backups, buckets, keys and aliases, user pools, secrets, log groups, parameters, certificates, roles and boundaries. KMS keys waiting for deletion are listed with their date. Each thing it leaves alone, such as a table of the name tagged for another install, is listed with why.
