---
title: Bedrock knowledge bases
description: Let Amazon Bedrock use your install's knowledge: search it live through AgentCore Gateway, or keep chosen knowledge bases copied into an S3 folder a Bedrock knowledge base reads.
order: 8.5
---

A self-hosted install can give Amazon Bedrock its teams' knowledge two ways. Both are set up on **Knowledge**, **Use in Bedrock** by people who manage knowledge bases. The guide [Connect Coderhelm knowledge to Amazon Bedrock](https://docs.coderhelm.com/bedrock/) covers both in full; this page is what differs on your own install.

## Search live with AgentCore Gateway

Nothing to deploy. Your MCP server is `https://<your Coderhelm address>/mcp`, and its OAuth discovery address is `https://<your Coderhelm address>/.well-known/oauth-authorization-server`.

1. Make a knowledge key on **Use in Bedrock**. It reads only the knowledge bases you tick, and only while you can still read them in Chat.
2. Save it in AgentCore Identity as an API key (sent as `Authorization: Bearer <key>`) or as OAuth client credentials (client ID `chkbc_...`, the key as the secret).
3. Add an MCP server target with your address. The gateway must reach your install over HTTPS: an install behind a private network needs the gateway to reach it, for example through a VPC egress configuration for the gateway.

## Copy into a Bedrock knowledge base through an S3 folder

The install copies each chosen knowledge base's documents into an S3 folder in its own account: `<folder><base id>/<file id>.txt`, with Bedrock's metadata file beside each one (`<file id>.txt.metadata.json`: `title`, `link`, `source`, `team`, `base`, `base_id`, `file_id`, `updated_at`). Changed documents are written again and deleted ones removed, every 15 minutes and right after you save.

**Your permission limit doesn't change.** The folder is in a bucket named with your install's prefix, the only buckets the install may write. The install never writes to a knowledge base and never starts an ingestion job itself: the stack below syncs the data source on a schedule with its own role. So you don't need to update the access stack, and nothing in the install's own stacks changes.

**Everyone with Bedrock access to the knowledge base reads everything copied into it.** It has no per-person access, so only the knowledge bases you tick are copied, and the page asks you to confirm this first.

### Set it up with one button

On **Knowledge**, **Use in Bedrock**, **Copy into a Bedrock knowledge base**: tick the knowledge bases, confirm the warning, and choose **Set up in AWS**. The AWS console opens the export stack with every parameter filled in. An AWS admin signed in to the install's account ticks the IAM acknowledgement and chooses **Create stack**. The page follows it: **Waiting for the stack**, **Creating the bucket**, **Knowledge base ready**, **First sync started**. Nothing is copied by hand.

How the page learns what the stack made: the stack reports each step to the install itself, at `https://<your Coderhelm address>/api/knowledge/bedrock-setup/report`, with a one-time token the button made. The install's own role gains no permission for it.

- The token is a `NoEcho` parameter. The install keeps only its SHA-256.
- It works for one setup of one team, for two hours. It stops working once the setup is ready or failed, when a newer setup replaces it, or when you choose **Cancel** on the page.
- The install accepts a report only from a stack in its own account, and only from the first stack that used the token.
- Before copying starts, the install checks what the stack made: the bucket must be `<prefix>-kb-export-<account>`, and the knowledge base must be in the install's account and region.
- The stack's first step checks the account and the token before it makes anything. If the install can't be reached or refuses, the stack stops and says why, and so does the page.

The stack must reach your install over HTTPS. If your install is on a private network, its address must be reachable from a Lambda function in the same account, or use **Enter it yourself** below.

### What the stack makes

| Parameter | What it is |
| --- | --- |
| `InstallPrefix` | Your install's resource prefix, such as `coderhelm-prod` |
| `Prefix` | The folder in the bucket. Default `coderhelm/` |
| `CreateKnowledgeBase` | `yes` (the default): a new knowledge base. `no`: only the bucket |
| `KnowledgeBaseId` | Optional: add the data source to a knowledge base you already have instead. It must be in the same region |
| `ExistingDataSourceId` | Optional, with `KnowledgeBaseId`: an S3 data source it already has that reads this folder |
| `KnowledgeBaseRoleName` | Optional, with `KnowledgeBaseId`: that knowledge base's service role, given read access to the folder |
| `InstallAccount`, `CoderHelmUrl`, `SetupToken` | Filled in by the button. Empty when you deploy it yourself: then it reports nothing |

- **The bucket** `<InstallPrefix>-kb-export-<account>`. It is SSE-S3 encrypted, blocks all public access and allows TLS only. It is versioned, removes old versions after 7 days, and is kept if the stack is deleted. Its policy lets only the install's own worker and gateway roles put, get, delete and list objects in the folder. Those roles' names start with the prefix and carry the `coderhelm:role` tag, which only the install's deploy role sets.
- **A knowledge base** named `<InstallPrefix>-coderhelm` (unless `CreateKnowledgeBase` is `no` or you named one). It stores its vectors in Amazon S3 Vectors: a vector bucket `<InstallPrefix>-kb-vectors-<account>` with one 1,024 dimension cosine index. It embeds with Amazon Titan Text Embeddings V2. Its own service role may read the folder, call that one model, and write and query that one index.
- **An S3 data source** reading the folder. Its deletion policy removes a document's vectors along with the document.
- **A schedule** (EventBridge Scheduler, with its own role) that starts the data source's sync every 15 minutes (`bedrock:StartIngestionJob` on that knowledge base only).
- **The reporter**: a small Lambda function. Its role may read only this stack's status and events.

S3 Vectors is the cheapest vector store Bedrock knowledge bases use: you pay for storage and queries, nothing while it sits idle. It is in most commercial regions. Where it isn't, the stack fails with that reason and the page says so. Then choose **Use an existing knowledge base**.

**What it costs**, at US East (N. Virginia) list prices when written:
- Embedding: $0.02 per million tokens.
- Vector storage: $0.06 per GB a month.
- Queries: $2.50 per million.
- Plus the S3 copies themselves.

A team with a few thousand documents usually pays well under $1 a month. AWS's pricing pages for [S3](https://aws.amazon.com/s3/pricing/) and [Bedrock](https://aws.amazon.com/bedrock/pricing/) have the current numbers.

### Use an existing knowledge base

Choose **Use an existing knowledge base**. The page lists the knowledge bases in the install's account and region, read with the install's own role (`bedrock:ListKnowledgeBases`). Pick one and choose **Set up in AWS**: the same stack opens, adding the S3 data source, the read policy and the schedule to that knowledge base. The knowledge base must be a vector knowledge base, and its service role's name is filled in when the install can read it. If the list can't be read, the page says why.

### Enter it yourself

Under **Enter it yourself** the five fields stay: deploy `coderhelm-bedrock-export.yaml` yourself (the **export stack** link downloads it), then paste its `BucketName`, `Prefix`, `KnowledgeBaseArn` and `DataSourceId` outputs.

```bash
aws cloudformation deploy \
  --stack-name coderhelm-prod-bedrock-export \
  --template-file coderhelm-bedrock-export.yaml \
  --capabilities CAPABILITY_IAM \
  --parameter-overrides InstallPrefix=coderhelm-prod
```

To stop, choose **Turn copying off**. The install removes its documents from the folder, and the next scheduled sync removes them from the knowledge base. After that you can delete the stack. The bucket is kept: empty it and delete it if you no longer want it.

## A knowledge base in another account

To copy into a knowledge base in another AWS account, use **A custom data source** instead. Connect that account on **Settings**, **Integrations**, **AWS**, add a data source of type Custom to the knowledge base, and run the role update the page links to. It adds the knowledge base to the `CoderHelmLogReader` role's `SyncKnowledgeBases`, which allows writing documents to that knowledge base only. A custom data source in the install's own account isn't offered, because the install's own role never writes to a knowledge base.

## What it costs

S3 storage and requests for the copies, plus Bedrock's own charges: embedding while documents are indexed, the vector store, and retrieval. AgentCore Gateway charges per call. The [guide](https://docs.coderhelm.com/bedrock/#costs) lists them with AWS's pricing pages.
