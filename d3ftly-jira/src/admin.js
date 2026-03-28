const { storage } = require("@forge/api");
const Resolver = require("@forge/resolver").default;

const resolver = new Resolver();

resolver.define("getConfig", async () => {
  const config = await storage.get("d3ftly-config");
  return config || { installationId: "", tenantId: "", defaultRepo: "" };
});

resolver.define("saveConfig", async ({ payload }) => {
  const { installationId, tenantId, defaultRepo } = payload;
  if (!installationId) {
    return { success: false, error: "Installation ID is required" };
  }
  await storage.set("d3ftly-config", {
    installationId,
    tenantId: tenantId || "",
    defaultRepo: defaultRepo || "",
  });
  return { success: true };
});

exports.resolver = resolver.getDefinitions();
