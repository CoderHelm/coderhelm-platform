// Which version of this app is running, so CoderHelm can tell when a site
// runs an older one.
//
// Forge numbers every deploy itself (6.10.0, 6.11.0, ...) and tells the
// running function which version it is through the app context. A site
// whose admin hasn't accepted a major update keeps running an older
// version; CoderHelm compares what each site reports with the version the
// release workflow last deployed, and shows the team what their site lacks.
//
// Every call the app makes to CoderHelm (each Jira event and each trigger
// registration) carries these two fields.

const api = require("@forge/api");

/**
 * { appVersion, appId } of the running app, from `getAppContext` (or a
 * stand-in for tests). Empty strings when the context isn't there, such as
 * outside Forge: the call to CoderHelm still goes out, only without them.
 */
function appInfo(getAppContext = api.getAppContext) {
  try {
    const ctx = (typeof getAppContext === "function" && getAppContext()) || {};
    return {
      appVersion: typeof ctx.appVersion === "string" ? ctx.appVersion : "",
      appId: typeof ctx.appAri === "string" ? ctx.appAri : ctx.appAri ? String(ctx.appAri) : "",
    };
  } catch (e) {
    return { appVersion: "", appId: "" };
  }
}

/** The fields an event's `coderhelm` object and a registration body carry. */
function versionFields(getAppContext) {
  const { appVersion, appId } = appInfo(getAppContext);
  return { app_version: appVersion || undefined, app_id: appId || undefined };
}

module.exports = { appInfo, versionFields };
