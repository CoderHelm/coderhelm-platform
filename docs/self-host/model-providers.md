---
title: Model providers
description: Call Claude through the Anthropic API with each team's key, or through Amazon Bedrock in your own AWS account.
order: 5
---

CoderHelm uses Claude for every run, review, plan chat and log analysis. You pick how it is reached with `models.provider` in the config.

## Anthropic API (default)

```json
"models": { "provider": "anthropic" }
```

Each team adds its own Anthropic API key in **Settings → Model Provider**, and picks its models there. Calls go from your Lambda functions straight to `api.anthropic.com`; the key is stored on the team's settings item in your DynamoDB table (encrypted with your KMS key) and is never sent anywhere else.

## Amazon Bedrock

```json
"models": {
  "provider": "bedrock",
  "bedrockInferencePrefix": "us."
}
```

The gateway, streaming, worker and log analyzer functions call Bedrock with their own IAM roles. No API key exists anywhere; usage and cost land on your AWS bill, and data stays in AWS.

**Before you deploy:**

1. In the Bedrock console of your region, open **Model access** and enable the Claude models you will use (by default Claude Opus 5.5 and Claude Sonnet 5.5). Some models ask you to accept terms first.
2. Pick the inference profile prefix for your region: `us.` for US regions, `eu.` for Europe, `apac.` for Asia Pacific, `global.` for the global profiles, or `""` to call a model in your region directly.

**How model names map to Bedrock ids.** Teams and the config use Anthropic's model names (`claude-opus-5-5`). CoderHelm turns them into Bedrock ids:

- an entry in `models.bedrockModelIds` wins, for example an application inference profile ARN:
  ```json
  "bedrockModelIds": {
    "claude-opus-5-5": "arn:aws:bedrock:us-east-1:123456789012:application-inference-profile/abc123"
  }
  ```
- a name that already is a Bedrock id (contains `anthropic.`) is used as it is;
- otherwise it becomes `<prefix>anthropic.<name>`, so `claude-sonnet-5-5` with `us.` is `us.anthropic.claude-sonnet-5-5`. Models whose Bedrock id carries a date (Claude Haiku 4.5) are mapped to the dated id for you.

**What the roles may call:** `bedrock:InvokeModel` and `bedrock:InvokeModelWithResponseStream` on Anthropic foundation models and your account's inference profiles, nothing else.

With Bedrock, the **Settings → Model Provider** page says the install uses Bedrock and no key is needed; teams can still choose which models they use.

## Which to choose

| | Anthropic API | Bedrock |
|---|---|---|
| Keys | One per team | None |
| Billing | Each team's Anthropic account | Your AWS bill |
| Data path | Your AWS → Anthropic | Stays in AWS |
| New models | Available on release day | When Bedrock adds them in your region |
| Setup | Nothing at deploy | Enable model access first |

## Embeddings

Agent memory uses Amazon Titan Text Embeddings V2 on Bedrock in your account with either provider. Enable it in **Model access** too; without it, memory search is off and runs still work.

## Usage records

Every model call is counted per team in your analytics table, with either provider. The daily reconciliation against Anthropic's usage report only runs when you add an Anthropic admin key secret (`coderhelm/<stage>/anthropic-admin-keys`); self-hosted installs normally leave it out.
