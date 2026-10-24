# Connector references

How each connection the marketing plugin uses is set up. Installing the
plugin grants nothing by itself: an admin connects each service, and the
plugin only gets the tools its manifest allows.

| Connection | Where it is set up | What the plugin may do |
|---|---|---|
| CMS (Sanity) | Connections page, Sanity card, a token with draft access | Read content (`get_*`, `list_*`, `query_*`) and create drafts (`create_draft`) |
| LinkedIn | The plugin's `linkedin` setting: a LinkedIn connection, or a key an admin pastes | `publish_post`, only after a person approves the exact version |
| Email | The plugin's `email` setting: the email service's API key | `send_email`, only after approval |
| Analytics (PostHog) | Connections page, PostHog card, a read-only key | Read insights (`query_*`, `get_*`), used for the weekly roundup and to read results back |
| Images | The plugin's `images` setting: the image service's API key | `generate_image` (makes an image for a draft; publishes nothing) |

Change the URLs and body in `plugin.yaml` to use other services. A change
that adds a tool, a scope, a trigger, a connection or an output waits for
an admin to approve it before the plugin uses it.
