const { fetch } = require("@forge/api");
const { storage } = require("@forge/api");

exports.handler = async (event, context) => {
  const issue = event.issue || {};
  const fields = issue.fields || {};

  // Only proceed if the issue is assigned to someone
  const assignee = fields.assignee;
  if (!assignee) {
    console.log(`Skipping ${issue.key} — no assignee`);
    return;
  }

  // Load config from Forge storage (set via admin page)
  const config = await storage.get("d3ftly-config");
  if (!config || !config.installationId) {
    console.log(`Skipping ${issue.key} — d3ftly not configured. Go to Apps > d3ftly Settings.`);
    return;
  }

  // Check for d3ftly label: d3ftly:owner/repo (explicit) or d3ftly (auto-resolve)
  const labels = fields.labels || [];
  const repoLabel = labels.find((l) => l.startsWith("d3ftly:"));
  const bareLabel = labels.some((l) => l === "d3ftly");

  let repoOwner, repoName;

  if (repoLabel) {
    [repoOwner, repoName] = repoLabel.replace("d3ftly:", "").split("/");
    if (!repoOwner || !repoName) {
      console.log(`Skipping ${issue.key} — invalid label: ${repoLabel}`);
      return;
    }
  } else if (!bareLabel) {
    console.log(`Skipping ${issue.key} — no d3ftly label`);
    return;
  }

  const payload = {
    webhookEvent: event.eventType || "jira:issue_assigned",
    issue: {
      key: issue.key,
      id: issue.id,
      fields: {
        summary: fields.summary,
        description: fields.description,
        labels: fields.labels,
        assignee: fields.assignee,
        project: fields.project,
      },
    },
    user: { displayName: assignee.displayName || "jira" },
    d3ftly: {
      repo_owner: repoOwner || undefined,
      repo_name: repoName || undefined,
      installation_id: parseInt(config.installationId, 10),
      tenant_id: config.tenantId || undefined,
    },
  };

  const response = await fetch("https://api.d3ftly.com/webhooks/jira", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

  console.log(`Forwarded ${issue.key} → ${repoOwner}/${repoName} (${response.status})`);
};
