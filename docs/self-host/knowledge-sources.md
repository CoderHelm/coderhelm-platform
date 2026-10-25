---
title: Knowledge sources
description: The OAuth apps and webhook settings that give Notion, Confluence and Google Drive one-click sign-in and instant updates on your install.
order: 14
---

Teams add connected apps to a knowledge base from the **Knowledge** page. Every source works on a fresh install without any settings here:

| Source | Works without setup | One-click sign-in needs | Instant updates need |
|---|---|---|---|
| Notion | yes, with a Notion integration token saved on the Integrations page | a Notion public integration | a Notion webhook subscription |
| Confluence | yes, with the Confluence email and API token saved on the Integrations page | an Atlassian OAuth 2.0 (3LO) app | nothing more (synced every hour) |
| Google Drive | no | a Google OAuth client | nothing more (synced every hour) |
| Sanity | yes, with the Sanity token and project id saved on the Integrations page | not used | nothing: CoderHelm registers the webhook when the token may manage webhooks, otherwise the dashboard shows the address and secret to add |
| GitHub | yes, through your GitHub App | not used | nothing: pushes arrive through the GitHub App's webhook |
| Website | yes | not used | not used (synced every hour) |

All the settings below are optional keys in your deployment's secret, `coderhelm/<stage>/secrets` (the same JSON that holds `github_app_id`). Add them, then redeploy or wait for the gateway to start a new instance. Leave a key out to keep that source on its saved token.

## The sign-in address

Every provider sends people back to the dashboard's sign-in page, the same one remote MCP servers use:

```
https://<your dashboard URL>/settings/integrations/mcp-callback/
```

Register exactly this address (with the trailing slash) as the redirect URI in each app below.

## Notion

1. Open <https://www.notion.so/profile/integrations> and create a **Public** integration.
2. Capabilities: **Read content** only. No insert, update or comment capabilities are needed.
3. Redirect URI: the sign-in address above.
4. Add to the secret:

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

1. In Google Cloud, enable the **Google Drive API** for a project.
2. Create an **OAuth client ID** of type **Web application** with the sign-in address above as an authorized redirect URI.
3. On the OAuth consent screen, add the scope `https://www.googleapis.com/auth/drive.readonly`. Google reviews apps that request it before people outside your organization can use them; an **Internal** consent screen needs no review.
4. Add to the secret:

```json
{ "google_drive_client_id": "<client ID>", "google_drive_client_secret": "<client secret>" }
```

If you already set up [Google sign-in](./integrations.md#google-sign-in), you can reuse that client instead: add the redirect URI and the Drive scope to it and leave these two keys out. CoderHelm falls back to `google_client_id` and `google_client_secret`.

## What runs

- **Schedule.** An EventBridge rule (`<prefix>-knowledge-sync`) drops `knowledge_sync_sweep` on the ticket queue every 15 minutes. The worker queues a sync for each source that is due: every hour, or every 6 hours as a backstop for sources that get instant updates.
- **Webhooks.** `POST /webhooks/knowledge/{token}` on the API (one token per Sanity source, `notion` for the Notion subscription). Sanity requests are signed with `sanity-webhook-signature` and refused when older than five minutes.
- **Storage.** Synced pages are stored like uploaded files: a record per page in the settings table and its search index in the data bucket, both under the team. Sign-ins are sealed with your KMS key for one team and one connection, renewed before they expire, and revoked when a team disconnects.
- **Cost.** Indexing uses Titan embeddings and is recorded like every other model call. Only pages whose text changed are indexed again, and within a page only the passages that changed.
