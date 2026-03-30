const { storage } = require("@forge/api");
const { webTrigger } = require("@forge/api");
const Resolver = require("@forge/resolver").default;

const resolver = new Resolver();

resolver.define("getConfig", async () => {
  const config = await storage.get("coderhelm-config");
  return config || { installationId: "", tenantId: "" };
});

resolver.define("saveConfig", async ({ payload }) => {
  const { installationId, tenantId } = payload;
  if (!installationId) {
    return { success: false, error: "Installation ID is required" };
  }
  await storage.set("coderhelm-config", { installationId, tenantId: tenantId || "" });
  return { success: true };
});

resolver.define("getWebTriggerUrls", async () => {
  const listProjectsUrl = await webTrigger.getUrl("list-projects-trigger");
  const createTicketUrl = await webTrigger.getUrl("create-ticket-trigger");
  return { listProjectsUrl, createTicketUrl };
});

exports.resolver = resolver.getDefinitions();
