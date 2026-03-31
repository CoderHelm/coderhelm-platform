const api = require("@forge/api");
const { fetch } = api;
const { storage } = api;
const { route } = api;

exports.handler = async (event, context) => {
  const issue = event.issue || {};

  // Load config from Forge storage (set via admin page)
  const config = await storage.get("coderhelm-config");
  if (!config || !config.installationId) {
    console.log(`Skipping ${issue.key} — coderhelm not configured. Go to Apps > coderhelm Settings.`);
    return;
  }

  // Forge event payloads only include issue id/key — fetch full issue via REST API
  let fields = {};
  if (issue.id) {
    try {
      const res = await api.asApp().requestJira(route`/rest/api/3/issue/${issue.id}?fields=summary,description,labels,assignee,project`);
      if (res.ok) {
        const full = await res.json();
        fields = full.fields || {};
      } else {
        console.log(`Failed to fetch issue ${issue.key}: ${res.status}`);
      }
    } catch (e) {
      console.log(`Error fetching issue ${issue.key}: ${e.message}`);
    }
  }

  // Extract repo from explicit label if present (coderhelm:owner/repo)
  const labels = (fields.labels || []).map((l) => (typeof l === "string" ? l : l.name || ""));
  const repoLabel = labels.find((l) => l.startsWith("coderhelm:"));
  let repoOwner, repoName;
  if (repoLabel) {
    [repoOwner, repoName] = repoLabel.replace("coderhelm:", "").split("/");
  }

  // Forward everything to the gateway — it handles label/assignee/project filtering
  const payload = {
    webhookEvent: event.eventType || "jira:issue_assigned",
    issue: {
      key: issue.key,
      id: issue.id,
      fields: {
        summary: fields.summary,
        description: fields.description,
        labels: labels,
        assignee: fields.assignee,
        project: fields.project,
      },
    },
    user: { displayName: (fields.assignee && fields.assignee.displayName) || fields.reporter?.displayName || "jira" },
    coderhelm: {
      repo_owner: repoOwner || undefined,
      repo_name: repoName || undefined,
      installation_id: parseInt(config.installationId, 10),
      tenant_id: config.tenantId || undefined,
    },
  };

  const response = await fetch("https://api.coderhelm.com/webhooks/jira", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

  console.log(`Forwarded ${issue.key} → gateway (${response.status})`);
};

// ── Web trigger: list Jira projects ──────────────────────────────────────────
exports.listProjectsHandler = async (request) => {
  try {
    const res = await api.asApp().requestJira(route`/rest/api/3/project?expand=lead`);
    if (!res.ok) {
      return { statusCode: res.status, body: JSON.stringify({ error: "Failed to fetch projects" }) };
    }
    const projects = await res.json();
    const simplified = projects.map((p) => ({
      key: p.key,
      name: p.name,
      lead: p.lead?.displayName || null,
      style: p.style || "classic",
    }));
    return {
      statusCode: 200,
      headers: { "Content-Type": ["application/json"] },
      body: JSON.stringify({ projects: simplified }),
    };
  } catch (e) {
    console.error("listProjects error:", e);
    return { statusCode: 500, body: JSON.stringify({ error: e.message }) };
  }
};

// ── Web trigger: create a Jira ticket ────────────────────────────────────────
exports.createTicketHandler = async (request) => {
  try {
    const body = JSON.parse(request.body || "{}");
    const { projectKey, summary, description, labels } = body;

    if (!projectKey || !summary) {
      return { statusCode: 400, body: JSON.stringify({ error: "projectKey and summary required" }) };
    }

    const issueBody = {
      fields: {
        project: { key: projectKey },
        summary,
        issuetype: { name: "Task" },
      },
    };

    if (description) {
      issueBody.fields.description = {
        type: "doc",
        version: 1,
        content: [{ type: "paragraph", content: [{ type: "text", text: description }] }],
      };
    }

    if (labels && labels.length > 0) {
      issueBody.fields.labels = labels;
    }

    const res = await api.asApp().requestJira(route`/rest/api/3/issue`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(issueBody),
    });

    if (!res.ok) {
      const err = await res.text();
      console.error("createTicket failed:", res.status, err);
      return { statusCode: res.status, body: JSON.stringify({ error: "Failed to create ticket", details: err }) };
    }

    const created = await res.json();
    return {
      statusCode: 201,
      headers: { "Content-Type": ["application/json"] },
      body: JSON.stringify({ key: created.key, id: created.id, self: created.self }),
    };
  } catch (e) {
    console.error("createTicket error:", e);
    return { statusCode: 500, body: JSON.stringify({ error: e.message }) };
  }
};
