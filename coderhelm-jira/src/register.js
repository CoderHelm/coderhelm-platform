// Tells the CoderHelm gateway where this installation's web triggers are.
//
// Web trigger URLs belong to one installation, so CoderHelm only learns them
// when the app sends them. That happens when an admin saves the settings page,
// when the app is installed or upgraded to a new major version, on the first
// event after the running version changes (Forge rolls a minor version out
// to every site without any lifecycle event, and a minor version can add a
// web trigger), and whenever CoderHelm finds a link missing and asks for
// them again through the project-list trigger. So a site gets new abilities
// without anyone re-saving settings.

/** The Forge SDK, loaded when first used, so this file (and its tests) load without it. */
function forge() {
  return require("@forge/api");
}

const { GATEWAY_URL } = require("./gateway");
const { versionFields, appInfo } = require("./version");

/** Where the version that last registered is kept. */
const REGISTERED_VERSION_KEY = "coderhelm-registered-version";

/** One trigger's URL, or "" when this version of the app doesn't have it. */
async function triggerUrl(key, getUrl = (k) => forge().webTrigger.getUrl(k)) {
  try {
    return (await getUrl(key)) || "";
  } catch (e) {
    console.log(`No web trigger ${key} in this version: ${e}`);
    return "";
  }
}

/** The site's address (e.g. https://yourcompany.atlassian.net), or "". */
async function siteUrl() {
  try {
    const res = await forge().asApp().requestJira(forge().route`/rest/api/3/serverInfo`);
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
 * Sends every trigger URL to the gateway for `config` ({ teamId, forgeSecret }).
 * Returns { urlsRegistered, urlError? }.
 */
async function registerWithGateway(config) {
  try {
    const body = {
      team_id: config.teamId,
      forge_secret: config.forgeSecret,
      list_projects_url: await triggerUrl("list-projects-trigger"),
      create_ticket_url: await triggerUrl("create-ticket-trigger"),
      add_comment_url: await triggerUrl("add-comment-trigger"),
      // The app is Jira-only: Confluence (pages for agents, release notes)
      // goes through the Atlassian connection in CoderHelm. Empty addresses
      // tell CoderHelm this site's app no longer does either.
      publish_release_url: "",
      get_issues_url: await triggerUrl("get-issues-trigger"),
      confluence_trigger_url: "",
      site_url: await siteUrl(),
      // The version that registers: after an accepted update, CoderHelm
      // knows the site caught up at once.
      ...versionFields(),
    };
    const res = await forge().fetch(`${GATEWAY_URL}/integrations/jira/forge-register`, {
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
  const config = await forge().storage.get("coderhelm-config");
  if (!config || !config.teamId || !config.forgeSecret) {
    console.log(`Lifecycle ${event && event.eventType}: not configured yet, nothing to register`);
    return;
  }
  const r = await registerWithGateway(config);
  console.log(
    `Lifecycle ${event && event.eventType}: trigger URLs ${r.urlsRegistered ? "registered" : `not registered (${r.urlError})`}`
  );
};

/**
 * Registers again when the running version isn't the one that last
 * registered (`store` and `register` are stand-ins in tests). Returns true
 * when it registered.
 */
async function registerIfVersionChanged(config, deps = {}) {
  const store = deps.store || forge().storage;
  const register = deps.register || registerWithGateway;
  const version = (deps.appInfo || appInfo)().appVersion;
  if (!version || !config || !config.teamId || !config.forgeSecret) return false;
  if ((await store.get(REGISTERED_VERSION_KEY)) === version) return false;
  const r = await register(config);
  if (r.urlsRegistered) await store.set(REGISTERED_VERSION_KEY, version);
  console.log(`Version ${version}: trigger URLs ${r.urlsRegistered ? "registered" : `not registered (${r.urlError})`}`);
  return r.urlsRegistered;
}

/** Same length and same bytes, without leaking how much matched. */
function sameSecret(a, b) {
  const crypto = require("node:crypto");
  const x = Buffer.from(String(a || ""));
  const y = Buffer.from(String(b || ""));
  return x.length > 0 && x.length === y.length && crypto.timingSafeEqual(x, y);
}

/**
 * A request to the project-list trigger that asks the app to register all of
 * its links again (`{ register_again: true, forge_secret }`): CoderHelm found
 * one missing. Answers the trigger's reply, or null for any other request.
 */
async function registerAgainReply(body, config, register = registerWithGateway) {
  if (!body || body.register_again !== true) return null;
  if (!config || !config.teamId || !sameSecret(body.forge_secret, config.forgeSecret)) {
    return { statusCode: 403, body: JSON.stringify({ error: "Wrong secret." }) };
  }
  const r = await register(config);
  return {
    statusCode: r.urlsRegistered ? 200 : 502,
    headers: { "Content-Type": ["application/json"] },
    body: JSON.stringify({ registered: r.urlsRegistered, error: r.urlError }),
  };
}

exports.registerWithGateway = registerWithGateway;
exports.registerIfVersionChanged = registerIfVersionChanged;
exports.registerAgainReply = registerAgainReply;
exports.triggerUrl = triggerUrl;
exports.REGISTERED_VERSION_KEY = REGISTERED_VERSION_KEY;
