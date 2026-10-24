// Tells the CoderHelm gateway where this installation's web triggers are.
//
// Web trigger URLs belong to one installation, so CoderHelm only learns them
// when the app sends them. That happens when an admin saves the settings page
// and again, on its own, whenever the app is installed or upgraded to a new
// major version (a version that adds a web trigger is one). So a site that
// accepts an update gets the new abilities without anyone re-saving settings.

const api = require("@forge/api");
const { storage, webTrigger, route } = api;

const { GATEWAY_URL } = require("./gateway");

/** The site's address (e.g. https://yourcompany.atlassian.net), or "". */
async function siteUrl() {
  try {
    const res = await api.asApp().requestJira(route`/rest/api/3/serverInfo`);
    if (res.ok) {
      const info = await res.json();
      return info.baseUrl || "";
    }
  } catch (e) {
    console.log("Failed to fetch site URL:", e);
  }
  return "";
}

/**
 * Whether this site has Confluence the app can read. Only then does the
 * Confluence trigger get registered, so CoderHelm shows Confluence as
 * connected only on sites that have it.
 */
async function hasConfluence() {
  try {
    const res = await api.asApp().requestConfluence(route`/wiki/api/v2/spaces?limit=1`);
    return res.ok;
  } catch (e) {
    console.log("Confluence isn't reachable on this site:", e);
    return false;
  }
}

/**
 * Sends every trigger URL to the gateway for `config` ({ teamId, forgeSecret }).
 * Returns { urlsRegistered, urlError? }.
 */
async function registerWithGateway(config) {
  try {
    const body = {
      team_id: config.teamId,
      forge_secret: config.forgeSecret,
      list_projects_url: await webTrigger.getUrl("list-projects-trigger"),
      create_ticket_url: await webTrigger.getUrl("create-ticket-trigger"),
      add_comment_url: await webTrigger.getUrl("add-comment-trigger"),
      publish_release_url: await webTrigger.getUrl("publish-release-trigger"),
      get_issues_url: await webTrigger.getUrl("get-issues-trigger"),
      confluence_trigger_url: (await hasConfluence()) ? await webTrigger.getUrl("confluence-trigger") : "",
      site_url: await siteUrl(),
    };
    const res = await api.fetch(`${GATEWAY_URL}/integrations/jira/forge-register`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!res.ok) return { urlsRegistered: false, urlError: `Gateway returned ${res.status}` };
    return { urlsRegistered: true };
  } catch (e) {
    return { urlsRegistered: false, urlError: String(e) };
  }
}

/**
 * Lifecycle trigger (app installed, or upgraded to a new major version):
 * registers the trigger URLs again when the site is already set up, so new
 * triggers reach CoderHelm without an admin re-saving the settings page.
 */
exports.lifecycleHandler = async (event) => {
  const config = await storage.get("coderhelm-config");
  if (!config || !config.teamId || !config.forgeSecret) {
    console.log(`Lifecycle ${event && event.eventType}: not configured yet, nothing to register`);
    return;
  }
  const r = await registerWithGateway(config);
  console.log(
    `Lifecycle ${event && event.eventType}: trigger URLs ${r.urlsRegistered ? "registered" : `not registered (${r.urlError})`}`
  );
};

exports.registerWithGateway = registerWithGateway;
