---
title: Knowledge sources
description: Sanity connects by signing in on every install with nothing to set up; the OAuth apps and webhook settings here give Notion, Confluence and Google Drive one-click sign-in and instant updates.
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
- **Google Drive.** There is no token path: Drive connects only through Google sign-in, and the dialog says so.

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

1. In Google Cloud, enable the **Google Drive API** for a project.
2. Create an **OAuth client ID** of type **Web application** with the sign-in address above as an authorized redirect URI.
3. On the OAuth consent screen, add the scope `https://www.googleapis.com/auth/drive.readonly`. Google reviews apps that request it before people outside your organization can use them; an **Internal** consent screen needs no review.
4. Add to the secret:

```json
{ "google_drive_client_id": "<client ID>", "google_drive_client_secret": "<client secret>" }
```

If you already set up [Google sign-in](./integrations.md#google-sign-in), you can reuse that client instead: add the redirect URI and the Drive scope to it and leave these two keys out. CoderHelm falls back to `google_client_id` and `google_client_secret`.

## What runs

- **Schedule.** An EventBridge rule (`<prefix>-knowledge-sync`) drops `knowledge_sync_sweep` on the ticket queue every 15 minutes. The worker queues a sync for each source that is due: every 15 minutes for a Sanity sign-in source, every hour for other sources, or every 6 hours as a backstop for sources that get instant updates.
- **Webhooks.** `POST /webhooks/knowledge/{token}` on the API (one token per Sanity source, `notion` for the Notion subscription). Sanity requests are signed with `sanity-webhook-signature` and refused when older than five minutes.
- **Storage.** Synced pages are stored like uploaded files: a record per page in the settings table and its search index in the data bucket, both under the team. Sign-ins are sealed with your KMS key for one team and one connection, renewed before they expire, and revoked when a team disconnects.
- **Cost.** Indexing uses Titan embeddings and is recorded like every other model call. Only pages whose text changed are indexed again, and within a page only the passages that changed.
