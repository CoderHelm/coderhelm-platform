---
title: Uninstall
description: Remove CoderHelm from your AWS account by deleting its stack, keep its data or delete it too, and check that nothing is left.
order: 12
---

# Uninstall

An install made with Launch Stack is one CloudFormation stack, `coderhelm-<name>`. To remove it:

1. Open the [CloudFormation console](https://console.aws.amazon.com/cloudformation/home) in the install's region.
2. Select the stack `coderhelm-<name>` (the top one, not a nested stack).
3. Choose **Delete**, then confirm.

It takes about 10 to 20 minutes. The stack's **Events** tab shows each step, and the reason for anything that fails. Before you start, choose what happens to the data (below).

An install from before native releases is several stacks instead: see [Installs from before native releases](#installs-from-before-native-releases).

## Keep the data, or delete it too

The data is kept unless you choose otherwise. The simplest way to delete it too:

1. In CoderHelm, open **Settings**, **System**, **Uninstall** (install admins only).
2. Turn on **Delete everything when CoderHelm is uninstalled**, and type the install's name to confirm. It takes effect at once; nothing is deleted yet. Who changed it and when is recorded.
3. Delete the stack.

When the stack is deleted, it reads the switch and deletes the data too. Turn the switch off again to go back to keeping the data. If **Settings**, **System** has no such switch, the install is older than it: update, or use the stack parameter instead (below).

The stack parameter **KeepDataOnDelete** does the same from AWS. It is `true` unless you changed it, and you can see it in the stack's **Parameters** or **Outputs** tab. The data is deleted when either says so: the switch on, or **KeepDataOnDelete** `false`.

| | Kept (the default) | Deleted (the switch on, or `false`) |
| --- | --- | --- |
| CoderHelm's functions, roles, queues, API, CloudFront, dashboard, schedules, alarms, the permissions boundary, the install helper | deleted | deleted |
| The release bucket (`coderhelm-<name>-release-<account>`) and the install guide's progress bucket | emptied and deleted | emptied and deleted |
| Log groups the stack declares (every function's) | deleted | deleted |
| Certificates the updater requested for your own domain, and their validation records in Route 53 | deleted | deleted |
| DynamoDB tables | kept | deleted |
| The data buckets, every version too | kept | emptied and deleted |
| The Cognito user pool (everyone's sign-in) | kept | deleted |
| The secret `coderhelm/<name>/secrets` | kept | deleted at once, no recovery window |
| The KMS keys | kept | scheduled for deletion in 7 days, AWS's shortest wait |
| Log groups Lambda made by itself under `/aws/lambda/coderhelm-<name>-` or `/aws/codebuild/coderhelm-<name>-` | kept | deleted |
| A log group the stack declares that Lambda made again after the delete (a function's last logs arriving late) | deleted | deleted |

When the data is kept, deletion protection stays on for the tables and the user pool, and what is kept no longer belongs to any stack. `coderhelm-selfhost leftovers` lists it (below).

Deleting the data can't be undone. The one thing you can stop is the KMS keys: until their deletion date, **Cancel key deletion** in the KMS console keeps them, though the data they encrypted is already gone.

### Or change KeepDataOnDelete before you delete

Without the switch, set **KeepDataOnDelete** to `false` first. Give the stack its release's template again; don't choose **Use current template**, so the new value is certain to apply.

1. Select the stack `coderhelm-<name>` and open **Outputs**. Note **Version**, for example `v0.8.6`.
2. Choose **Update**, then **Replace existing template**, **Amazon S3 URL**: `https://coderhelm-public.s3.amazonaws.com/launch/<Version>/coderhelm.json`, with the version from step 1. It is the release you run, so nothing else changes.
3. Choose **Next**. Set **KeepDataOnDelete** to `false`. Leave every other parameter as it is.
4. Go through the remaining pages, acknowledge the IAM and nested stack boxes, and choose **Submit**.
5. Wait for `UPDATE_COMPLETE`. Then check **Outputs** again: **KeepDataOnDelete** must say `false`. If it still says `true`, the update didn't take; do it again with the template URL.
6. Delete the stack.

From a terminal:

```bash
STACK=coderhelm-<name> REGION=<region>
V=$(aws cloudformation describe-stacks --region $REGION --stack-name $STACK --query "Stacks[0].Outputs[?OutputKey=='Version'].OutputValue" --output text)
aws cloudformation update-stack --region $REGION --stack-name $STACK \
  --template-url "https://coderhelm-public.s3.amazonaws.com/launch/$V/coderhelm.json" \
  --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND \
  --parameters ParameterKey=KeepDataOnDelete,ParameterValue=false \
    $(for k in AdminEmail Stage ModelProvider BedrockInferencePrefix EmailTransport EmailFrom Hostname SignupDomain RequireTwoFactor AutoUpdate UpdateDay UpdateHour UpdateTimezone SetupToken StatusId DomainCertificateArn DomainZoneId; do echo "ParameterKey=$k,UsePreviousValue=true"; done)
aws cloudformation wait stack-update-complete --region $REGION --stack-name $STACK
aws cloudformation describe-stacks --region $REGION --stack-name $STACK --query "Stacks[0].Outputs[?OutputKey=='KeepDataOnDelete'].OutputValue" --output text
```

The last command must print `false`. The update changes only the deletion settings: deletion protection comes off the tables and the user pool, and their deletion policy becomes Delete. Nothing is deleted until you delete the stack. Set it back to `true` the same way.

### What happens when the data is deleted

When you delete the stack, two of its resources do the work CloudFormation can't do alone:

- **Sweep** runs before the parts are deleted. It empties every data bucket (every object version and delete marker), so CloudFormation can delete them. It deletes each table itself: it turns deletion protection off if it is still on, retries what AWS reports as temporary (a table in use, throttling, a replica still settling) with growing waits, and waits until each table is gone. CloudFormation then finds nothing left to delete. If a table still won't go before the helper's time runs out, Sweep fails with the table's name and AWS's reason. Delete the stack again: it picks up where it stopped.
- **Final** runs after every part is deleted. It deletes the certificates the updater requested (tagged `coderhelm = coderhelm-<name>`, in us-east-1) with their validation records, the log groups Lambda made by itself under the install's name, and the install's secret, without a recovery window (after **HybridLeave**, which reads it).

With **KeepDataOnDelete** `false`, CloudFormation itself deletes the data buckets and the user pool and schedules the KMS keys for deletion after 7 days. With the switch on and **KeepDataOnDelete** `true`, the stack keeps those, so the helper deletes them itself: Sweep deletes the data buckets (after emptying them) and the user pool (its deletion protection off first), and Final schedules the install's KMS keys for deletion after 7 days and removes their aliases. Final also removes the switch.

These steps run only when the whole stack is being deleted, never during an update.

## Hybrid installs

An install registered with CoderHelm leaves when you delete the stack. The **HybridLeave** resource releases its `<name>.coderhelm.com` address and its links, so the GitHub installations and Jira site it served go back to CoderHelm hosted. If CoderHelm can't be reached, the delete goes on anyway, and CoderHelm releases the address once the install stops answering.

## Check that nothing is left

After the stack is gone, run the read-only check from a terminal with credentials for the account:

```bash
npx -y github:CoderHelm/coderhelm-platform leftovers --stage <name> --region <region>
```

It lists anything still in the region named `coderhelm-<name>-*`, `coderhelm/<name>/` or `/coderhelm-<name>/`, or tagged `coderhelm:install = coderhelm-<name>`: tables, backups, buckets, KMS keys and aliases, user pools (two pools of the same name are both listed), secrets, log groups, parameters, certificates, roles and boundaries. KMS keys waiting for deletion are listed with their date. Things it leaves alone, such as a table of the install's name tagged for another install, are listed with why. It exits with 1 when anything is left.

Expect to see:

- what **KeepDataOnDelete** `true` kept: the tables, data buckets, keys, user pool and secret;
- with `false`, the tables' on-demand backups, which outlive their tables;
- the auto-update role `coderhelm-<name>-deploy-stack`, if auto-update was ever on. CloudFormation keeps using a stack's role once a stack was updated through it, and a role can't delete itself, so the stack keeps it on delete (see [Security](./security.md#updates-and-the-optional-auto-update-role));
- pieces an earlier, failed install left behind.

To remove them:

```bash
npx -y github:CoderHelm/coderhelm-platform leftovers --stage <name> --region <region> --delete
```

It shows the list and asks you to type the install's name. If the stack `coderhelm-<name>` is still there (never deleted, or a delete that stopped partway), it deletes that stack first, as one: it empties the install's buckets (every object version), then deletes the stack, which takes its parts with it in order; it never deletes a part on its own. If the stack's delete fails, it prints the part and AWS's reason and stops; run it again to pick up there. Then it removes each piece left with your own credentials, printing each result: it empties a bucket before deleting it, waits for each table to go, and schedules KMS keys for deletion (7 days). It never touches another install whose name starts with this one's (`coderhelm-prod-eu` next to `coderhelm-prod`).

While the stack still runs, the read-only `leftovers` says the install is still installed and lists nothing: nothing of a running install is a leftover.

## If the delete fails

The stack's **Events** tab names the resource and AWS's reason. Fix what it says, then choose **Delete** again; CloudFormation picks up where it stopped. If a resource still can't be deleted, the console offers to keep it and delete the rest. Keep it, then remove it with `leftovers --delete`.

If the stack was ever updated through the auto-update role, CloudFormation deletes it through that role too, and some deletes are outside what the role may do: scheduling a KMS key for deletion, and deleting the permissions boundary (no CoderHelm role may delete it). The simplest is to delete with a role of your own: `aws cloudformation delete-stack --region <region> --stack-name coderhelm-<name> --role-arn <a role of yours>`. Or use the same path: delete again, keep what failed, then `leftovers --delete`.

## Not removed

Not removed, because CoderHelm didn't create them or they aren't only CoderHelm's:

- DNS records you added at another DNS provider for your own domain.
- The read-only roles your teams created for CoderHelm (`CoderHelmLogReader`, `CoderhelmReadOnly`): delete their stacks in each account.
- Your GitHub App: uninstall it from GitHub (Settings, Developer settings, GitHub Apps).
- AWS Bedrock model access and SES settings for the account.

## Installs from before native releases

An install made before native releases (with the command line, or the earlier Launch Stack) is about eleven stacks named `coderhelm-<name>-<part>`, not one. Two choices:

- Move it to one stack with [`coderhelm-selfhost migrate`](./migrate.md), then delete that stack as above.
- Or remove it from a terminal with your own credentials:

```bash
# Remove CoderHelm, keep the data
npx -y github:CoderHelm/coderhelm-platform uninstall --stage <name> --region <region>

# Delete everything (KMS keys after 7 days)
npx -y github:CoderHelm/coderhelm-platform uninstall --everything --stage <name> --region <region>
```

It shows what will go, asks you to type the install's name, and ends with the same check as `leftovers`. `--dry-run` shows the list and deletes nothing; `--yes` skips the prompt. Without `--stage` and `--region` it reads them from the install's `coderhelm.config.json`. On a native install it deletes the stack as one first, like `leftovers --delete`. Without `--everything` it keeps the data, and refuses a stack whose **KeepDataOnDelete** is `false` (deleting it would delete the data): use `--everything` to confirm.
