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

**Your permission limit doesn't change.** The folder is in a bucket named with your install's prefix, the only buckets the install may write. The install never writes to a knowledge base and never starts an ingestion job itself: the stack below syncs the data source on a schedule with its own role. So no access stack update is needed, and nothing in the install's own stacks changes.

**Everyone with Bedrock access to the knowledge base reads everything copied into it.** It has no per-person access, so only the knowledge bases you tick are copied, and the page asks you to confirm this first.

### 1. Deploy the export stack

An AWS admin deploys `coderhelm-bedrock-export.yaml` in the install's account and region. Download it from **Use in Bedrock** (the **export stack** link), or take it from the release at `infra/cfn/coderhelm-bedrock-export.yaml`.

```bash
aws cloudformation deploy \
  --stack-name coderhelm-bedrock-export \
  --template-file coderhelm-bedrock-export.yaml \
  --capabilities CAPABILITY_IAM \
  --parameter-overrides InstallPrefix=coderhelm-prod \
    KnowledgeBaseId=ABCDE12345 \
    KnowledgeBaseRoleName=AmazonBedrockExecutionRoleForKnowledgeBase_abc
```

| Parameter | What to put |
| --- | --- |
| `InstallPrefix` | Your install's resource prefix, such as `coderhelm-prod` (**Settings**, **System** shows it) |
| `Prefix` | The folder in the bucket. Default `coderhelm/` |
| `KnowledgeBaseId` | Optional: the knowledge base to add an S3 data source to. It must be in the same region |
| `ExistingDataSourceId` | Optional, with `KnowledgeBaseId`: use an S3 data source the knowledge base already has instead of making one. It must read this bucket's folder |
| `KnowledgeBaseRoleName` | Optional, with `KnowledgeBaseId`: the knowledge base's service role, given read access to the folder |

What it makes:

- **The bucket** `<InstallPrefix>-kb-export-<account>`: SSE-S3 encrypted, every public access blocked, TLS only, versioned with old versions removed after 7 days, kept if the stack is deleted. Its policy lets only the install's own worker and gateway roles (their names start with the prefix, and they carry the `coderhelm:role` tag only the install's deploy role sets) put, get, delete and list objects in the folder.
- **With `KnowledgeBaseId`**: an `AWS::Bedrock::DataSource` of type S3 reading the folder (its deletion policy removes a document's vectors with it), unless you named an existing one; a read policy on the knowledge base's service role (`s3:ListBucket` on the bucket and `s3:GetObject` on the folder, this account only); and an EventBridge Scheduler schedule with its own role that starts the data source's ingestion job every 15 minutes (`bedrock:StartIngestionJob` on that knowledge base only). A sync with nothing new finishes quickly, and one that finds a sync already running waits for the next.

Without `KnowledgeBaseId`, point an existing knowledge base's S3 data source at the bucket and folder yourself, give its role read access, and sync it after changes (the console's **Sync**, or `aws bedrock-agent start-ingestion-job`).

### 2. Turn copying on

On **Knowledge**, **Use in Bedrock**, **Copy into a Bedrock knowledge base**: choose **An S3 folder**, paste the stack's `BucketName` and `Prefix` (and `KnowledgeBaseArn` and `DataSourceId`, so the page checks them), tick the knowledge bases, confirm the warning, and choose **Start copying**.

To stop, choose **Turn copying off**: the install removes its documents from the folder, and the next scheduled sync removes them from the knowledge base. Then you can delete the stack; the bucket is kept, empty it and delete it if you no longer want it.

## A knowledge base in another account

To copy into a knowledge base in another AWS account, use **A custom data source** instead. Connect that account on **Settings**, **Integrations**, **AWS**, add a data source of type Custom to the knowledge base, and run the role update the page links to. It adds the knowledge base to the `CoderHelmLogReader` role's `SyncKnowledgeBases`, which allows writing documents to that knowledge base only. A custom data source in the install's own account isn't offered, because the install's own role never writes to a knowledge base.

## What it costs

S3 storage and requests for the copies, plus Bedrock's own charges: embedding while documents are indexed, the vector store, and retrieval. AgentCore Gateway charges per call. The [guide](https://docs.coderhelm.com/bedrock/#costs) lists them with AWS's pricing pages.
