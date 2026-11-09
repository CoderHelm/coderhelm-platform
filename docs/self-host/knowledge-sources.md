---
title: Knowledge sources
description: Sanity connects by signing in on every install with nothing to set up; the OAuth apps and webhook settings here give Notion, Confluence and Google Drive one-click sign-in and instant updates. Bedrock knowledge bases and MCP search tools connect in place; S3 buckets, Bedrock knowledge bases and export archives import.
order: 14
---

Teams add connected apps to a knowledge base from the **Knowledge** page. Sign-in comes first wherever the app allows it, and every source works on a fresh install without any settings here:

| Source | Works without setup | One-click sign-in needs | Instant updates need |
|---|---|---|---|
| Sanity | yes: people sign in with their own Sanity account | nothing (Sanity's remote MCP server registers CoderHelm by itself) | an API token instead of a sign-in (see below) |
| Notion | yes, with a token from a Notion internal connection, pasted in the add flow or on the Integrations page | a Notion public connection (OAuth) | a Notion webhook subscription |
| Confluence | yes, with an Atlassian API token, email and site, pasted in the add flow or on the Integrations page | an Atlassian OAuth 2.0 (3LO) app | nothing more (synced every hour) |
| Google Drive | no | a Google OAuth client | nothing more (synced every hour) |
| GitHub | yes, through your GitHub App | not used | nothing: pushes arrive through the GitHub App's webhook |
| Website | yes | not used | not used (synced every hour) |

The **Sign-in apps** section of the platform admin page (`/admin`) shows which of the three apps your install has, the redirect URI to register and the key names below. It never shows a client secret.

All the settings below are optional keys in your deployment's secret, `coderhelm/<stage>/secrets` (the same JSON that holds `github_app_id`). Add them, then redeploy or wait for the gateway to start a new instance. Leave a key out to keep that source on its saved token.

## Sanity: signing in, with nothing to set up

Sanity sources connect by signing in with a Sanity account, through Sanity's official remote MCP server at `https://mcp.sanity.io`, the same sign-in **Connected apps** uses for agents. The server supports dynamic client registration, so CoderHelm registers itself the first time someone signs in: there is no OAuth app to create and no key to add, on any install.

- **Whose sign-in.** A source records the member who connected it and syncs with their sign-in, and its page says so ("Synced with Ada's Sanity sign-in"). Someone already signed in to Sanity on Connected apps is reused without signing in again. Otherwise the sign-in runs in a small window and hands back to the add flow.
- **What it reads.** Only the server's read tools: `list_projects`, `list_datasets`, `query_documents` (GROQ, published perspective, drafts and release versions left out) and `whoami` (for "Connected as"). Nothing is created, changed or published.
- **Where the token goes.** The sign-in's token is for the MCP server only. CoderHelm sends it to `mcp.sanity.io` and nowhere else (not to `api.sanity.io`). It is sealed with your KMS key for one team, one connection and one person, and renewed before it expires.
- **Staying up to date.** A sign-in brings no webhook, so these sources are checked every 15 minutes, and each check asks only for documents whose `_updatedAt` is after the newest one seen (a document's `_rev` decides whether it is read again). Every 6 hours, and whenever the selection changes, a full listing also finds deleted documents.
- **When the sign-in stops working.** If the sign-in expires or is revoked, or the member leaves the team, the source stops with one sentence and a **Sign in again** button. Any admin can press it: the source then syncs with their sign-in. The team's owner and admins are told once, in the team's chat channels and by email (when `SES_FROM_ADDRESS` is set).
- **Egress.** The worker and gateway reach `mcp.sanity.io` over HTTPS. If you restrict outbound traffic, allow it.

### Sanity with an API token (instant updates)

The token path stays as the second choice in the add flow ("Use an API token instead"). An admin saves a Sanity API token (Viewer is enough) and the project id on the Integrations page. With a token, CoderHelm registers a webhook when the token may manage webhooks (otherwise the dashboard shows the address and secret to add by hand), so changes arrive the moment they are published, with a check every 6 hours as a backstop.

### Notion and Confluence sign-ins

Notion (`mcp.notion.com`) and Atlassian (`mcp.atlassian.com`) also run remote MCP servers with dynamic registration, but their tools can't keep a knowledge base in sync yet: Notion's can't list a page's children or give an edit time without reading each whole page, and cuts long pages short; Atlassian's doesn't publish its tools' arguments or answers. Their sources keep using the OAuth apps below, or the keys saved on the Integrations page. Agents can still use both servers through Connected apps.

## Without a sign-in app: tokens in the add flow

When an app isn't set up, the add flow says so and offers the token path in the same dialog, so nobody is sent elsewhere:

- **Notion.** A short how-to (create an internal connection at <https://www.notion.so/profile/integrations>, tick **Read content** only, share pages with **•••**, **Connections**, **+ Add connection**), then a field for the token. CoderHelm checks the token with one Notion search before saving it, and saves it exactly as the Integrations page does (the `notion` entry, sealed with your KMS key). A token Notion refuses is never saved; one that sees no pages yet is saved, and the flow says how to share pages.
- **Confluence.** The Confluence address, the Atlassian account email and an API token from <https://id.atlassian.com/manage-profile/security/api-tokens>. They go through the Integrations page's own save, which signs in once and records the result; a refused token isn't counted as connected.
- **Google Drive.** There is no token path: Drive connects only through Google sign-in. The dialog says so in one sentence and asks for whoever runs CoderHelm to set it up; a platform admin also gets a link to these steps.

Only admins and owners save tokens (the `integrations` capability). Anyone else who can add knowledge sees "Ask an admin to connect Notion". The request is `POST /api/knowledge/connectors/{kind}/token` with `{ "token" }` (Notion) or `{ "token", "site", "email" }` (Confluence).

## The sign-in address

Every provider sends people back to the dashboard's sign-in page, the same one remote MCP servers use:

```
https://<your dashboard URL>/settings/integrations/mcp-callback/
```

Register exactly this address (with the trailing slash) as the redirect URI in each app below. On the hosted service it is `https://app.coderhelm.com/settings/integrations/mcp-callback/`, and the secret is `coderhelm/prod/secrets` in Secrets Manager (the gateway reads `SECRETS_NAME`, which the deploy sets to `coderhelm/<stage>/secrets`, or `<prefix>/secrets` for a custom prefix).

## Notion

1. Open <https://www.notion.so/profile/integrations> (Notion's developer page, now called connections) and create a **Public** connection (older workspaces: a new integration of type **Public**). Notion asks for a company name, website, privacy policy and terms URLs.
2. Capabilities: **Read content** only. No insert, update or comment capabilities, and no user information, are needed.
3. Redirect URI: the sign-in address above. CoderHelm asks for no scopes (Notion has none); it sends `owner=user`, so each person picks pages on Notion's own consent screen.
4. Copy the **OAuth client ID** and **OAuth client secret** from the Configuration tab.
5. Add to the secret:

```json
{ "notion_client_id": "<OAuth client ID>", "notion_client_secret": "<OAuth client secret>" }
```

People pick which pages to share on Notion's own consent screen; CoderHelm sees only those pages.

### Instant updates from Notion (optional)

1. In the same integration, open **Webhooks** and add the subscription URL `https://<your API URL>/webhooks/knowledge/notion`.
2. Subscribe to page events (created, content updated, moved, deleted, undeleted) and database events.
3. Notion sends a one-time verification request to that address. CoderHelm writes its `verification_token` to the gateway's log (the CloudWatch log group of the `<prefix>-gateway` function, search for "Notion webhook subscription check"). Paste it in Notion's **Verify** step, and add it to the secret:

```json
{ "notion_webhook_token": "<verification token>" }
```

Events are checked with `X-Notion-Signature` (HMAC-SHA256 with that token). An event only starts a sync; what changed is always read back with the team's own connection. Without it, Notion sources sync every hour.

## Confluence (Atlassian)

1. Open <https://developer.atlassian.com/console/myapps/> and create an **OAuth 2.0 integration**.
2. Under **Permissions**, add the Confluence API with these scopes: `read:page:confluence`, `read:space:confluence`, `read:hierarchical-content:confluence`, `read:confluence-content.summary`, `search:confluence`.
3. Under **Authorization**, set the callback URL to the sign-in address above.
4. Under **Distribution**, make it sharable if teams outside your Atlassian organization will connect.
5. Add to the secret:

```json
{ "atlassian_client_id": "<client ID>", "atlassian_client_secret": "<secret>" }
```

CoderHelm asks for `offline_access` too, so the sign-in renews itself.

## Google Drive

Google Drive connects only through Google sign-in, with an OAuth client of its own. CoderHelm reads `google_drive_client_id` and `google_drive_client_secret` and nothing else (a self-hosted install has no Google sign-in client to borrow; a `google_client_id` left in an old secret is never used for Drive). Without the two Drive keys, the add flow says "Google Drive sign-in isn't set up on this Coderhelm yet. Ask whoever runs Coderhelm to set up Google Drive sign-in." and the **Sign-in apps** section shows Google Drive as not set up.

### Which scope, and why

CoderHelm asks for `https://www.googleapis.com/auth/drive.readonly`. A knowledge source keeps a folder in sync: it lists the folder, reads every file in it and picks up files added later. Google's three candidate scopes compare like this:

| Scope | Google's class | What CoderHelm could do with it |
|---|---|---|
| `drive.readonly` | restricted | Browse Drive in the add flow, pick folders and files, read them and keep folders in sync, including files added later. |
| `drive.file` with the Google Picker | non-sensitive | Read only the files a person picks in Google's picker. Picking a folder doesn't grant its contents, so a folder can't be synced and new files never appear until someone picks them. |
| `drive.metadata.readonly` | restricted | Names, folders and dates only, never file contents: nothing to index. Restricted anyway, so it saves no review. |

`drive.readonly` is a restricted scope. An **Internal** app (only people in your own Google Workspace) needs no verification. An **External** app used by anyone outside your Workspace must pass Google's verification for restricted scopes, which includes an annual third-party security assessment (CASA). Until then it stays in **Testing**: only the test users you list (up to 100) can sign in, they see an "unverified app" warning, and their sign-ins expire after 7 days.

- **Self-hosted:** make the app **Internal** in your Workspace. No verification, no test users, and sign-ins that don't expire.
- **A shared, public install (like the hosted service):** `drive.readonly` with Google's verification. Until it's verified, list the people who need Drive as test users.

### Register the app

1. In [Google Cloud](https://console.cloud.google.com/), pick or create a project, open **APIs and services**, **Library**, and enable the **Google Drive API**.
2. Open **Google Auth Platform** (formerly **OAuth consent screen**):
   - **Branding**: app name, support email, and your dashboard's domain under **Authorized domains**.
   - **Audience**: **Internal** for a self-hosted install in your own Workspace. **External** otherwise; while it is in **Testing**, add each person who will connect Drive under **Test users**.
   - **Data access**: **Add or remove scopes**, then add `https://www.googleapis.com/auth/drive.readonly` (paste it under **Manually add scopes** if it isn't listed).
3. **Clients**, **Create client**: type **Web application**, any name. Under **Authorized redirect URIs**, add exactly the sign-in address above, with the trailing slash. **Authorized JavaScript origins** can stay empty.
4. Copy the client ID and client secret into the secret:

```json
{ "google_drive_client_id": "<client ID>", "google_drive_client_secret": "<client secret>" }
```

5. Redeploy, or wait for a new gateway instance. **Sign-in apps** then shows Google Drive as set up, from `google_drive_client_id`.

On a hybrid install that chose CoderHelm's Google app, Drive is offered only while the hosted service has its own Drive app; otherwise the choice is unavailable and the install falls back to its own keys.

## Bedrock knowledge bases, connected in place

A knowledge base in Amazon Bedrock can be connected in place instead of copied. CoderHelm searches it live in your AWS account each time an agent or the brain chat needs it, and nothing is copied out: no documents, no passages, no vectors. Bedrock turns the question into a search itself, so it doesn't matter which embedding model, chunking or vector store the knowledge base uses. Both kinds of Bedrock knowledge base work, the classic ones whose vector store you run and the managed ones.

To connect one, open **Knowledge**, **New knowledge base**, **Bedrock knowledge base**. On a self-hosted install the dialog lists the knowledge bases in the install's own account and region; pick one, or paste the ARN of any knowledge base (`arn:aws:bedrock:<region>:<account>:knowledge-base/<id>`). Before anything is saved, CoderHelm runs one test search. When it fails, the dialog says why in one sentence and what to do. The base's page then shows **Connected in place: searched live in your AWS account, nothing copied** instead of a list of files, and it takes no uploads or synced apps.

How it is searched:

- **Hybrid search.** CoderHelm asks for Bedrock's hybrid search (vectors and keywords) where the vector store supports it, and plain vector search where it doesn't (the test search finds out). No reranking.
- **Merged with your other bases.** Bedrock's ranked list joins the same rank fusion as CoderHelm's own lists, so answers can cite a connected base and an uploaded file side by side. At most three passages come from one document.
- **Citations.** A citation names the document and links to it: the original page for a web, Confluence or SharePoint source, and the S3 console for an S3 object (which opens only for people who can sign in to that account).
- **The region is the ARN's.** A knowledge base in another region than the install is searched in its own region.
- **When it doesn't answer.** The other bases still answer, and the agent or chat is told that this one couldn't be searched, and why.

### Who searches it

A knowledge base in the install's own account is searched with the install's own gateway and worker roles. Their read-only policy comes from the same update template as the rest of the install's own-account reads, and for the install it names every knowledge base in its account (`bedrock:Retrieve` and `bedrock:GetDocumentContent` on `arn:aws:bedrock:*:<account>:knowledge-base/*`, and `bedrock:ListKnowledgeBases` for the list). Admins still choose which ones a team connects. Each version update brings this policy along.

A knowledge base in another account is searched with that account's `CoderHelmLogReader` role, the one the account was connected with on **Settings**, **Integrations**, **AWS**. Connect that account first. The role may search only the knowledge bases named in its stack's `KnowledgeBases` parameter (their ARNs separated by commas, or `all` for every knowledge base in that account). When the test search finds the role can't, the dialog says "The role in your account does not allow Retrieve on this knowledge base" and offers a one-click update stack in that account with the parameter filled in. That stack carries everything the role's other update stacks do, so you can delete the older `CoderHelmLogReaderUpdate` stacks once it is in place. If AWS says the role's policies are too large, delete them first.

### The permissions boundary: one admin step

Searching needs `bedrock:Retrieve`, and the install's permissions boundary allows it only from this release on. Listing and reading a cited document were already allowed (`bedrock:List*`, `bedrock:Get*`). An update never changes the boundary: the parent stack keeps the boundary installs have, and the new one reaches an install only through the access stack an AWS admin applies (**Settings**, **System**, **Version**, **Permissions**, one link to the AWS console). See [Updates](./updates.md).

Until an admin applies it, connecting a knowledge base in the install's own account says so plainly: "This install's permission limit doesn't allow searching Bedrock knowledge bases yet: the release that searches them needs a permission change an AWS admin hasn't applied." Nothing else waits on it, and knowledge bases in other accounts work without it.

### Private networks

An install whose functions have no internet route needs interface VPC endpoints for `bedrock-agent-runtime` (search) and `bedrock-agent` (the list), in each region a connected knowledge base lives in. Without them the dialog says CoderHelm couldn't reach Bedrock.

## Connecting a search tool through MCP

Any search tool your team already runs behind an MCP server can be connected in place too: Azure AI Search or Foundry IQ, Pinecone Assistant, Glean, Atlassian Rovo, Dropbox Dash, a Bedrock knowledge base behind an AgentCore gateway, or your own retrieval server. Add the server on **Settings**, **Connected apps** first, then open **Knowledge**, **New knowledge base**, **Search tool (MCP)** and pick it.

- **The tools.** CoderHelm calls the server's search tool (`search`, `knowledge_base_retrieve`, `retrieve`, `get_context` and similar names are found by themselves, or pick one) and, for results without text, its fetch tool (`fetch` or similar). Nothing else is called, and a tool the server marks as destructive is never picked.
- **Check on save.** One test search runs before anything is saved. When it fails, the dialog says why in one sentence (not signed in, no search tool, an answer it couldn't read, the server's own error, unreachable) and what to do.
- **Who it searches as.** A server the team shares is searched with the team's connection. A server that signs each person in is searched with the asking person's own sign-in, so they only get what the tool lets them see; agent runs search it as the person who started them, and runs nobody started skip it.
- **Nothing new in AWS.** It uses the Connected apps sign-in and tokens (sealed with your KMS key) and needs no permission change. If you restrict outbound traffic, allow the server's address.

## Importing an existing knowledge base

### From Amazon S3

A bucket of documents (or a folder in one) becomes a source of a knowledge base: **Add source**, **Amazon S3**, pick the AWS account, type the bucket name, and pick folders or files. CoderHelm reads supported files (PDF, Word, Markdown, text, CSV, Excel, HTML), embeds them with Titan and keeps them in sync like any other source: an object whose ETag didn't change isn't read again, a changed one re-embeds only its changed passages, and a deleted one is removed. A `<file>.metadata.json` sidecar next to a document (the Bedrock data source format) gives it its title, original link and date.

The bucket is always read with that account's `CoderHelmLogReader` role (connect the account on **Settings**, **Integrations**, **AWS**, including the install's own account), never with the install's own role. The role may read only the buckets named in its stack's `ImportBuckets` parameter: `s3:ListBucket`, `s3:GetBucketLocation` and `s3:GetObject` on them, and `kms:Decrypt` only through S3 for a bucket with its own KMS key. When the role can't read a bucket, the dialog offers the one-click update stack with the bucket named. The install's permissions boundary already allows S3 and KMS, so there is no admin step on the install itself.

### From a Bedrock knowledge base

**Import from a Bedrock knowledge base** reads the knowledge base's settings (`bedrock:GetKnowledgeBase`, `bedrock:ListDataSources`, `bedrock:GetDataSource`) and turns each S3 data source into an S3 source on its bucket and prefixes. Bedrock can't export its passages, so CoderHelm reads the original documents and indexes them itself; nothing in Bedrock changes. A Confluence or web data source points you at CoderHelm's own connector instead; SharePoint and Salesforce data sources can't be imported, so connect the knowledge base in place to search them. On a self-hosted install, a knowledge base in its own account is read with the install's own role (the boundary's `bedrock:Get*` and `bedrock:List*`, which arrive with the version update); one in another account with that account's role, whose `ReadKnowledgeBases` statement now includes these three actions.

### From an export archive

**Upload an archive** takes a `.zip` (up to 200 MB) of a Notion export (Markdown and CSV), a Confluence HTML export or a zipped docs folder, and unpacks it on the server. Every supported entry becomes a page of the base, titled without the export's ids. Uploading a newer export to the same archive keeps unchanged pages, re-reads changed ones and removes pages it no longer has. The archive is kept in the team's folder of the data bucket and goes with its source. Before the upload the dialog warns that every page becomes readable by everyone who can read the base, and that a Confluence Full Export includes restricted pages.

### Limits

An import keeps up to 2,500 documents per source (other sources keep 1,000). CoderHelm ranks its own bases in memory, so a bigger corpus is better connected in place; the page says so when an import is cut short. Each search reads up to 16 indexes at once and keeps recently used indexes in memory (up to 160 MB per process).

## Who sees what

Every knowledge base has a lowest read role, and can also be limited to named people and roles (**Only these people or roles**). A person only gets passages from bases they may read, in the brain chat and on the Knowledge page.

Agents search the bases attached to them. A base limited to named people is searched only in runs one of them started; a search tool that signs each person in is searched as the person who started the run. Every other attached base is searched by every run, whatever the starter's rights in the systems the content came from, and the page says so when a base is attached. Connect and attach only content that everyone with that access may read.

## What runs

- **Schedule.** An EventBridge rule (`<prefix>-knowledge-sync`) drops `knowledge_sync_sweep` on the ticket queue every 15 minutes. The worker queues a sync for each source that is due: every 15 minutes for a Sanity sign-in source, every hour for other sources, or every 6 hours as a backstop for sources that get instant updates.
- **Webhooks.** `POST /webhooks/knowledge/{token}` on the API (one token per Sanity source, `notion` for the Notion subscription). Sanity requests are signed with `sanity-webhook-signature` and refused when older than five minutes.
- **Storage.** Synced pages are stored like uploaded files: a record per page in the settings table and its search index in the data bucket, both under the team. Sign-ins are sealed with your KMS key for one team and one connection, renewed before they expire, and revoked when a team disconnects.
- **Cost.** Indexing uses Titan embeddings and is recorded like every other model call. Only pages whose text changed are indexed again, and within a page only the passages that changed.
