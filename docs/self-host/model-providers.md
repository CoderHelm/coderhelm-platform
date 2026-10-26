---
title: Model providers
description: Call Claude through the Anthropic API with each team's key, or through Amazon Bedrock in your own AWS account.
order: 5
---

CoderHelm uses Claude for every run, review, plan chat and log analysis. You pick how it is reached when you install (`models.provider` in the config, or the install guide's **Models** step), and you can change it later in **Settings, System, Models** without a redeploy. See [Switch providers](#switch-providers).

## Anthropic API (default)

```json
"models": { "provider": "anthropic" }
```

Each team adds its own Anthropic API key in **Settings, AI models**, and picks its models there. Calls go from your Lambda functions straight to `api.anthropic.com`; the key is stored on the team's settings item in your DynamoDB table (encrypted with your KMS key) and is never sent anywhere else.

## Amazon Bedrock

```json
"models": {
  "provider": "bedrock",
  "bedrockInferencePrefix": "us."
}
```

The gateway, streaming, worker and log analyzer functions call Bedrock with their own IAM roles. No API key exists anywhere; usage and cost land on your AWS bill, and data stays in AWS.

**Before you deploy (or switch):** see [Bedrock model access](#bedrock-model-access) below, then pick the inference profile prefix for your region: `us.` for US regions, `eu.` for Europe, `apac.` for Asia Pacific, `global.` for the global profiles, or `""` to call a model in your region directly.

**How model names map to Bedrock ids.** Teams and the config use Anthropic's model names (`claude-opus-5-5`). CoderHelm turns them into Bedrock ids:

- an entry in `models.bedrockModelIds` wins, for example an application inference profile ARN:
  ```json
  "bedrockModelIds": {
    "claude-opus-5-5": "arn:aws:bedrock:us-east-1:123456789012:application-inference-profile/abc123"
  }
  ```
- a name that already is a Bedrock id (contains `anthropic.`) is used as it is;
- otherwise it becomes `<prefix>anthropic.<name>`, so `claude-sonnet-5-5` with `us.` is `us.anthropic.claude-sonnet-5-5`. Models whose Bedrock id carries a date (Claude Haiku 4.5) are mapped to the dated id for you.

**What the roles may call:** `bedrock:InvokeModel` (which also covers the Converse API) and `bedrock:InvokeModelWithResponseStream` on Anthropic foundation models and your account's inference profiles, nothing else. Every self-hosted install gets this, even one that starts on Anthropic keys, so you can switch to Bedrock later without a redeploy. The roles never get `aws-marketplace:Subscribe` or the calls that accept a model agreement or submit the use case form.

With Bedrock, the **Settings, AI models** page says the install uses Bedrock and no key is needed; teams can still choose which models they use.

## Bedrock model access

Before CoderHelm can call a Claude model on Bedrock, someone in your company does three things in the AWS console, once per account. CoderHelm never does them for you: it only links to the page and checks the result.

1. **Model access.** In the Bedrock console of your region, open **Model access** and turn on the Claude models CoderHelm calls (by default Claude Opus 5.5 and Claude Sonnet 5.5).
2. **Use case form.** The first time an account asks for an Anthropic model, Bedrock asks for a short description of how you will use it (**Submit use case details** on the same page). A person fills it in; it applies to every Anthropic model in the account.
3. **Anthropic's terms (AWS Marketplace).** Each Anthropic model is offered through AWS Marketplace. Accepting its terms subscribes your account; this needs a user allowed to subscribe in Marketplace and a valid payment method on the account. It can take a few minutes to finish.

If a step is missing, every check (Setup, a switch, an update) names the model, says which step in one sentence, and links to the page. Then use **Check again**.

## Switch providers

Install admins can switch at any time in **Settings, System, Models**:

- **Claude on Amazon Bedrock**, with the inference profile prefix (default `us.`) and, under **Advanced**, an exact Bedrock id for any model (an application inference profile ARN, for example).
- **Anthropic API keys**. Each team keeps using its own key from **Settings, AI models**, as before. There is no install-wide key: the key already lives with each team, every service reads it there, and each team's usage stays on its own Anthropic account.

Before anything is saved, CoderHelm makes one test call (one token) to every model the running release calls, through the provider you picked: Bedrock with your settings, or the Anthropic API with your team's key. If any model is refused, nothing changes and you see which model, the fix in one sentence and a link, with **Check again**. A switch to Anthropic keys needs your own team to have a working key first.

A saved switch:

- is written to your team's audit log first (who, when, from what to what); if the log can't be written, nothing changes;
- reaches every part of CoderHelm within 30 seconds, with no redeploy;
- never stops a run. Model calls already under way finish where they started, and later calls use the new provider. A run that started on Bedrock without a team key keeps using Bedrock until it ends.

The deploy config's `models` settings are only the first-boot default. Once you switch in Settings, that choice wins until you switch again; a redeploy doesn't undo it.

## Updates and new models

A new release can call a model your install hasn't used before. Each release publishes `models.json` with the models it calls and their Bedrock ids for every prefix. It is made from the code, so it can't miss one. It is listed in the release's signed `SHA256SUMS`, and your install only reads it through a small read-only build (`<prefix>-release-check`) that checks the signature first, exactly like an update does; a file that doesn't match is never used.

Before the updater applies a release, automatically or with **Update now**, it makes one test call to each of those models (plus the ones your config names) through your current provider:

- **All answer:** the update goes ahead as usual.
- **One is refused** (model access off, use case form missing, Marketplace terms not accepted or still processing, IAM, or the model isn't offered in your region): the update doesn't start. **Settings, System** and **Setup** show **Update to vX needs your action** with the model, the fix and the console link. Install admins get one email (and a post to `updates.webhookUrl` if set).
- The check runs again every day. Once it passes, the update applies by itself in your maintenance window if auto-update is on; otherwise **Update now** works again.

**Update now** has no override for a model the release calls. **Skip this check** appears only when the failing model isn't one the release calls.

On the Anthropic API there are no terms to accept; the check uses the key of the team that last switched to keys, and is skipped when there is no such key. Models a team picks in **Settings, AI models** are checked by **Setup**, not before an update.

## Which to choose

| | Anthropic API | Bedrock |
|---|---|---|
| Keys | One per team | None |
| Billing | Each team's Anthropic account | Your AWS bill |
| Data path | Your AWS, then Anthropic | Stays in AWS |
| New models | Available on release day | When Bedrock adds them in your region |
| Setup | Nothing at deploy | Enable model access first |
| Change later | Settings, System, Models | Settings, System, Models |

## Embeddings

Agent memory uses Amazon Titan Text Embeddings V2 on Bedrock in your account with either provider. Enable it in **Model access** too; without it, memory search is off and runs still work.

## Usage records

Every model call is counted per team in your analytics table, with either provider. The daily reconciliation against Anthropic's usage report only runs when you add an Anthropic admin key secret (`coderhelm/<stage>/anthropic-admin-keys`); self-hosted installs normally leave it out.
