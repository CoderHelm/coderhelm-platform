const { storage, fetch: forgeFetch } = require("@forge/api");
const { webTrigger } = require("@forge/api");
const api = require("@forge/api");
const { route } = require("@forge/api");
const Resolver = require("@forge/resolver").default;

const GATEWAY_URL = "https://api.coderhelm.com";

const resolver = new Resolver();

resolver.define("getConfig", async () => {
  const config = await storage.get("coderhelm-config");
  return config || { installationId: "" };
});

resolver.define("saveConfig", async ({ payload }) => {
  const { installationId } = payload;
  if (!installationId) {
    return { success: false, error: "Installation ID is required" };
  }
  await storage.set("coderhelm-config", { installationId });

  // Auto-register web trigger URLs with the Coderhelm gateway
  try {
    const listProjectsUrl = await webTrigger.getUrl("list-projects-trigger");
    const createTicketUrl = await webTrigger.getUrl("create-ticket-trigger");
    const addCommentUrl = await webTrigger.getUrl("add-comment-trigger");
    const tid = `TENANT#${installationId}`;

    // Get the Jira site URL (e.g. https://mysite.atlassian.net)
    let siteUrl = "";
    try {
      const siteRes = await api.asApp().requestJira(route`/rest/api/3/serverInfo`);
      if (siteRes.ok) {
        const siteData = await siteRes.json();
        siteUrl = siteData.baseUrl || "";
      }
    } catch (e) {
      console.log("Failed to fetch site URL:", e);
    }

    const res = await forgeFetch(`${GATEWAY_URL}/integrations/jira/forge-register`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        tenant_id: tid,
        installation_id: installationId,
        list_projects_url: listProjectsUrl,
        create_ticket_url: createTicketUrl,
        add_comment_url: addCommentUrl,
        site_url: siteUrl,
      }),
    });
    if (!res.ok) {
      return { success: true, urlsRegistered: false, urlError: `Gateway returned ${res.status}` };
    }
    return { success: true, urlsRegistered: true };
  } catch (e) {
    return { success: true, urlsRegistered: false, urlError: String(e) };
  }
});

resolver.define("getWebTriggerUrls", async () => {
  const listProjectsUrl = await webTrigger.getUrl("list-projects-trigger");
  const createTicketUrl = await webTrigger.getUrl("create-ticket-trigger");
  const addCommentUrl = await webTrigger.getUrl("add-comment-trigger");
  return { listProjectsUrl, createTicketUrl, addCommentUrl };
});

exports.resolver = resolver.getDefinitions();
