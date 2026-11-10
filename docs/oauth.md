# OAuth sign-ins: every flow, its redirect, its scopes

This page lists every place CoderHelm sends a browser to another service to sign in, and what each provider's app must have registered. It covers the hosted service (`app.coderhelm.com` and `api.coderhelm.com`), a fully private self-hosted install (`<URL>` below is the install's own address, for example `https://coderhelm.example.com`), and a hybrid install, which uses CoderHelm's apps through the signed relay ([hybrid](./self-host/hybrid.md)).

## The one callback page

Every app, MCP server and knowledge-source sign-in comes back to one dashboard page:

| Deployment | Redirect URI (register exactly this, trailing slash included) |
|---|---|
| Hosted | `https://app.coderhelm.com/settings/integrations/mcp-callback/` |
| Hybrid install, CoderHelm's app | the hosted address above (the provider never sees the install's address) |
| Private self-host, the install's own app | `<URL>/settings/integrations/mcp-callback/` |

Several kinds of sign-in share that page, because a provider app has one redirect (Atlassian allows only one callback URL per app). The page tells them apart like this:

- **The `state` carries the kind.** It is `<kind>.<random>`, where kind is `mcp` (an MCP server), `app` (a catalog server signed in with an OAuth app, such as Figma), `ks` (a knowledge source) or `hyb` (the hosted side signing a hybrid install in).
- **The kind is only a hint for the words.** Everything the answer may do comes from the sign-in CoderHelm stored when it started, found by the hash of the whole `state`. That record holds the kind, the team, the person, the PKCE verifier, the issuer, the dashboard page to return to, and a 10-minute expiry. Changing the kind changes the hash, so a tampered `state` matches nothing.
- **It works once.** Finishing takes the record (a refusal too), and only the person and team that started it can finish it.
- **The page always asks the API.** It posts everything the provider sent (`code`, `state`, `error`, `error_description`, `iss`) to `POST /api/mcp-servers/oauth/callback`. The answer says which kind it was and where to go back to:
  - a knowledge source returns to the Knowledge page that started it;
  - an MCP server or app returns to its row on Settings, Integrations;
  - a return path is only ever a path on the dashboard.
- **A refusal is shown as the provider said it.** The page shows the provider's own error code and description, with a way back to the page the sign-in started from. `access_denied` with no description says both things it can mean: Cancel was chosen, or the provider won't let this account use the app.
- **Old states still finish.** A `state` from before kinds were added (a bare random value) works: its kind is worked out from the server it signs in to.
- **A sign-in that can't be matched says to start again,** and never claims access was refused.
- **Failures are logged.** Every failed callback is written to the gateway log ("OAuth sign-in didn't finish") with the kind, the provider's code and the reason. Tokens and codes are never logged.

PKCE (S256) is sent on every one of these sign-ins. Tokens are sealed with KMS for one team, one server and one owner. They are renewed before they expire, a rotated refresh token is never lost, a refused renewal marks the connection "needs reconnect", and disconnecting revokes where the provider supports it.

### Hybrid installs through CoderHelm's apps

1. The install sends the browser to `https://api.coderhelm.com/hybrid/oauth/<provider>/authorize` with its install id as client id, its own callback as `redirect_uri` (checked against its verified addresses), its `state` and a PKCE challenge.
2. The hosted side signs in with CoderHelm's app, using the hosted redirect above.
3. The hosted callback page passes the answer to `POST /hybrid/oauth/callback`. The hosted side finishes the provider sign-in in a holding partition, then sends the browser to the install's callback page:
   - with a one-time code, which becomes the install's only when the install redeems it at `/hybrid/oauth/token` with its registration secret and PKCE verifier;
   - or, when it failed, with `error` and `error_description`: the provider's own when the provider refused, or `server_error` with the reason when CoderHelm's side couldn't finish (an earlier version always sent `access_denied` here).

## Knowledge sources

| Source | Authorize | Scopes | Secret keys | Notes |
|---|---|---|---|---|
| Notion | `https://api.notion.com/v1/oauth/authorize`, `owner=user` | none (Notion has none) | `notion_client_id`, `notion_client_secret` | **Public** connection, Read content only. The token request uses Basic auth and a JSON body. Cancel answers `access_denied`. |
| Confluence (Atlassian 3LO) | `https://auth.atlassian.com/authorize`, `audience=api.atlassian.com`, `prompt=consent` | granular `read:page:confluence`, `read:space:confluence`, `read:hierarchical-content:confluence`; classic `search:confluence`; plus `offline_access` | `atlassian_client_id`, `atlassian_client_secret` | One callback URL per app. Under Distribution, share the app with anyone outside the owner's account, or they get `access_denied`. Refresh tokens rotate and expire after 90 days unused. |
| Google Drive | `https://accounts.google.com/o/oauth2/v2/auth`, `access_type=offline`, `prompt=consent` | `https://www.googleapis.com/auth/drive.readonly` | `google_drive_client_id`, `google_drive_client_secret` (never the Google sign-in client) | A restricted scope: an External app needs Google's verification, and in Testing its sign-ins expire after 7 days. |
| Sanity | the person's own sign-in to Sanity's MCP server | registered dynamically | none | Nothing to set up. |
| GitHub, web pages, S3, archives | no OAuth | | | Through the GitHub App or a token. |

Setup steps for each are in [knowledge sources](./self-host/knowledge-sources.md).

## MCP servers

Servers are added by address. CoderHelm discovers their authorization server (RFC 9728, RFC 8414) and registers itself in this order:

1. **Client ID Metadata Document.** The hosted service uses `https://app.coderhelm.com/oauth/mcp-client.json`; an install with an https address uses `<API URL>/integrations/mcp-client.json`.
2. **Dynamic registration,** when the server offers it.
3. **A client id an admin pastes,** for servers that need an app registered by hand, at the redirect above.

Every authorize and token request names the server as `resource` (RFC 8707). `iss` (RFC 9207) is checked when the server advertises it.

| Server | Registration it accepts | Notes |
|---|---|---|
| Linear `mcp.linear.app` | metadata document or dynamic | |
| Sentry `mcp.sentry.dev` | metadata document or dynamic | |
| Vercel `mcp.vercel.com` | metadata document, or clients Vercel approved | A self-host install needs its metadata document (an https address). |
| Stripe `mcp.stripe.com` | dynamic | An agent API key works too. |
| Atlassian Rovo `mcp.atlassian.com` | dynamic | An Atlassian organization admin may need to allow the redirect domain under Atlassian Administration, Rovo, Rovo MCP server ("Your organization admin must authorize access from this redirect URL"). |
| Figma `mcp.figma.com` | only clients Figma approved | Use a Figma OAuth app instead: the team's own, or CoderHelm's through hybrid. Scopes `current_user:read` and `file_content:read`; redirect as above. |

## Integrations

- **GitHub App (hosted, CoderHelm's app):**
  - Callback URLs: `https://api.coderhelm.com/auth/github/callback` (sign-in and Connect GitHub) and `https://api.coderhelm.com/auth/github/callback/hybrid` (hybrid installation links and hybrid sign-ins).
  - Setup URL: `https://app.coderhelm.com/settings/integrations/github/`, with "Redirect on update" on.
  - Account permission **Email addresses: read**, so a person whose GitHub email is private still matches their invite. Without it, matching falls back to the public profile email. Changing the permission is an app setting, made in GitHub, and each installation's owner accepts it.
- **GitHub App (self-host, the install's own):** the install guide sets `<URL>/api/setup/github/callback` (manifest), the callback `<URL>/auth/github/callback`, the setup URL `<URL>/settings/github/` and the webhook `<URL>/webhooks/github`. See [the GitHub App](./self-host/github-app.md).
- **What the GitHub pages show on return:**
  - Changing repository access on GitHub returns `setup_action=update` with no state; the page reloads the list and says the access was updated.
  - Asking an organization owner to approve the app returns `setup_action=request`; the page says the request was sent.
  - Connect GitHub to link returns `?github_link=` with what linking did: `linked`, `not_admin`, `owned_elsewhere` or `none_visible`.
  - Cancelling GitHub's sign-in during a hybrid link goes back to the install with `hybrid_github=declined`. The token used to check a hybrid link is revoked as soon as the check is done.
- **Jira (Forge app):** no OAuth. The app registers its web triggers with `POST <API URL>/integrations/jira/forge-register`. Every trigger must be a Forge web trigger (https on `*.atlassian-dev.net`, no port or credentials), or the registration is refused.
- **Slack and Microsoft Teams:** no OAuth. Slack uses a slash command with the app's signing secret, and Teams an incoming webhook or an outgoing webhook token ([integrations](./self-host/integrations.md)).

## Signing in to CoderHelm

- **Email and password, and two-factor:** Cognito, with no redirect.
- **Google (hosted only):**
  - Cognito's hosted UI calls back to `https://api.coderhelm.com/auth/google/callback`.
  - The Google client must have `https://<Cognito domain>/oauth2/idpresponse` registered. A prefix domain lives in the user pool's region.
  - The state is a `__Host-` cookie that lasts 10 minutes and works once.
- **GitHub:** the GitHub App's own client, as above.
- **SSO (OIDC and SAML):** not available yet ([sign-in](./self-host/sign-in.md)).
- **CoderHelm's own MCP server:**
  - CoderHelm is the authorization server: metadata at `<API URL>/.well-known/oauth-authorization-server`, consent at `<URL>/authorize/`.
  - Clients register themselves and must use PKCE S256.
  - Nothing has to be registered anywhere.

## When a sign-in fails

| What the page shows | What it means | Fix |
|---|---|---|
| "<Provider> didn't grant access (access_denied)", followed by the provider's own words | The provider refused | Read its words. For Atlassian: share the app (Distribution), or check that the account can reach the site. |
| access_denied, with no other words | Cancel, or the account may not use the app | Start again, or ask an admin of the provider. |
| invalid_scope | The app lacks a scope CoderHelm asks for | Add the scopes listed above to the app. |
| unauthorized_client or invalid_request | The client id or redirect URI isn't registered | Register the redirect exactly, trailing slash included. |
| server_error, "The sign-in server refused (invalid_grant)" | The code exchange failed (an expired or used code, a wrong secret) | Start again; check the client secret. |
| "That sign-in was already used or has expired" | The page was reloaded, or 10 minutes passed | Start again from the page you started on. |

Sources: [Atlassian 3LO](https://developer.atlassian.com/cloud/confluence/oauth-2-3lo-apps/), [Confluence scopes](https://developer.atlassian.com/cloud/confluence/scopes-for-oauth-2-3LO-and-forge-apps/), [Notion authorization](https://developers.notion.com/docs/authorization), [Google web server flow](https://developers.google.com/identity/protocols/oauth2/web-server), [MCP authorization](https://modelcontextprotocol.io/specification/2025-11-25/basic/authorization), [RFC 6749 section 4.1.2.1](https://www.rfc-editor.org/rfc/rfc6749#section-4.1.2.1).
