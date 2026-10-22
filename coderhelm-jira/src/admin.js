const { storage } = require("@forge/api");
const { webTrigger } = require("@forge/api");
const Resolver = require("@forge/resolver").default;
const { registerWithGateway } = require("./register");

const resolver = new Resolver();

resolver.define("getConfig", async () => {
  const config = await storage.get("coderhelm-config");
  return config || { teamId: "", forgeSecret: "" };
});

resolver.define("saveConfig", async ({ payload }) => {
  const { teamId, forgeSecret } = payload;
  if (!teamId) {
    return { success: false, error: "Team ID is required" };
  }
  if (!forgeSecret) {
    return { success: false, error: "Secret is required" };
  }
  await storage.set("coderhelm-config", { teamId, forgeSecret });

  // Auto-register web trigger URLs with the Coderhelm gateway
  const r = await registerWithGateway({ teamId, forgeSecret });
  return { success: true, ...r };
});

resolver.define("getWebTriggerUrls", async () => {
  const listProjectsUrl = await webTrigger.getUrl("list-projects-trigger");
  const createTicketUrl = await webTrigger.getUrl("create-ticket-trigger");
  const addCommentUrl = await webTrigger.getUrl("add-comment-trigger");
  return { listProjectsUrl, createTicketUrl, addCommentUrl };
});

exports.resolver = resolver.getDefinitions();
