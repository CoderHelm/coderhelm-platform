---
title: Backups
description: What CoderHelm keeps, how it is protected, and how to back it up and restore it.
order: 10
---

## What holds data

| Where | What | Protection |
|---|---|---|
| DynamoDB tables `coderhelm-<stage>-*` | Teams, runs, settings, plans, usage, memories' metadata | Point-in-time recovery (35 days) on every table; deletion protection and retention while `retainData` is true |
| S3 `coderhelm-<stage>-data-<account>` | Run artifacts, memory snapshots, images from tickets | Versioning (old versions kept 30 days); retained |
| Secrets Manager `coderhelm/<stage>/secrets` | GitHub App keys, session signing key | Kept when stacks are deleted |
| KMS keys | Encrypt the tables, the bucket and sealed credentials | Retained, yearly rotation |
| Cognito user pool | Accounts and passwords | Retained while `retainData` is true |

Everything else (functions, queues, the dashboard bucket, logs) is rebuilt by a deploy.

## On-demand backups

```sh
npx -y github:CoderHelm/coderhelm-platform backup
```

takes an on-demand backup of every table of the install. They are kept until you delete them. For scheduled, cross-region or cross-account copies, add the tables to an AWS Backup plan (tag-based selection works: every resource carries the stack's tags).

## Restore

- **A table to a point in time:** DynamoDB console → the table → **Backups → Restore to point in time**, into a new table name. Then swap: delete the old table (turn off deletion protection first) and restore again under the original name, or copy the items back.
- **A bucket object:** S3 console → **Show versions** → restore the version you want.
- **The whole install in a new account:** restore the tables and copy the bucket into the new account with the original names, set `account` in the config, and deploy. The secret and the GitHub App move with `github-app --app-id ...` (see [The GitHub App](./github-app.md#github-enterprise-server)), or create a new app.

Cognito user pools can't be backed up with passwords; people reset their passwords after a move to a new pool.
