const api = require("@forge/api");
const { fetch } = api;
const { storage } = api;
const { route } = api;

exports.handler = async (event, context) => {
  const issue = event.issue || {};
  const eventType = event.eventType || "unknown";

  // Load config from Forge storage (set via admin page)
  const config = await storage.get("coderhelm-config");
  if (!config || !config.teamId) {
    console.log(`Skipping ${issue.key} — coderhelm not configured. Go to Apps > coderhelm Settings.`);
    return;
  }

  // Handle comment events — Forge payloads don't include comment data,
  // so we fetch the latest comment via REST API
  if (eventType.includes("comment")) {
    let comment;
    try {
      const res = await api
        .asApp()
        .requestJira(route`/rest/api/3/issue/${issue.id}/comment?orderBy=-created&maxResults=1`);
      if (res.ok) {
        const data = await res.json();
        comment = (data.comments || [])[0];
      } else {
        console.log(`Failed to fetch comments for ${issue.key}: ${res.status}`);
        return;
      }
    } catch (e) {
      console.log(`Error fetching comments for ${issue.key}: ${e.message}`);
      return;
    }

    if (!comment) {
      console.log(`No comments found on ${issue.key}`);
      return;
    }

    const authorName = (comment.author && (comment.author.displayName || comment.author.accountId)) || "";

    // Skip comments by the app itself to avoid loops
    if (authorName.toLowerCase().includes("coderhelm")) {
      console.log(`Skipping own comment on ${issue.key}`);
      return;
    }

    const commentBody = typeof comment.body === "string" ? comment.body : JSON.stringify(comment.body || "");

    // Fetch full issue fields so the worker gets the current description
    let fields = {};
    if (issue.id) {
      try {
        const res = await api.asApp().requestJira(route`/rest/api/3/issue/${issue.id}?fields=summary,description,labels,assignee,project,attachment`);
        if (res.ok) {
          const full = await res.json();
          fields = full.fields || {};
        }
      } catch (e) {
        console.log(`Error fetching issue fields for comment on ${issue.key}: ${e.message}`);
      }
    }

    const labels = (fields.labels || []).map((l) => (typeof l === "string" ? l : l.name || ""));

    // Extract repo from label if present (coderhelm:owner/repo)
    const repoLabel = labels.find((l) => l.startsWith("coderhelm:"));
    let commentRepoOwner, commentRepoName;
    if (repoLabel) {
      [commentRepoOwner, commentRepoName] = repoLabel.replace("coderhelm:", "").split("/");
    }

    const payload = {
      webhookEvent: "jira:comment_created",
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
      comment: {
        id: comment.id,
        body: commentBody,
        author: { displayName: authorName },
      },
      coderhelm: {
        repo_owner: commentRepoOwner || undefined,
        repo_name: commentRepoName || undefined,
        team_id: config.teamId,
        forge_secret: config.forgeSecret,
      },
    };

    const response = await fetch("https://api.coderhelm.com/webhooks/jira", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    console.log(`Forwarded comment on ${issue.key} → gateway (${response.status})`);
    return;
  }

  // Forge event payloads only include issue id/key — fetch full issue via REST API
  let fields = {};
  if (issue.id) {
    try {
      const res = await api.asApp().requestJira(route`/rest/api/3/issue/${issue.id}?fields=summary,description,labels,assignee,project,attachment`);
      if (res.ok) {
        const full = await res.json();
        fields = full.fields || {};
        console.log(`Issue ${issue.key} fields keys: ${Object.keys(fields).join(", ")}`);
        console.log(`Issue ${issue.key} attachment field: ${JSON.stringify(fields.attachment?.length ?? "missing")}`);
      } else {
        console.log(`Failed to fetch issue ${issue.key}: ${res.status} ${await res.text()}`);
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

  // Download image attachments from the ticket
  const imageAttachments = await fetchImageAttachments(fields);
  console.log(`Image attachments for ${issue.key}: found=${imageAttachments.length}, raw_attachments=${(fields.attachment || []).length}`);
  if (imageAttachments.length > 0) {
    console.log(`Downloaded ${imageAttachments.length} image attachment(s) for ${issue.key}`);
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
    image_attachments: imageAttachments,
    user: { displayName: (fields.assignee && fields.assignee.displayName) || fields.reporter?.displayName || "jira" },
    coderhelm: {
      repo_owner: repoOwner || undefined,
      repo_name: repoName || undefined,
      team_id: config.teamId,
      forge_secret: config.forgeSecret,
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

// ── Web trigger: add a comment to a Jira ticket ──────────────────────────────
exports.addCommentHandler = async (request) => {
  try {
    const body = JSON.parse(request.body || "{}");
    const { issueKey, comment, commentType, title } = body;

    if (!issueKey || !comment) {
      return { statusCode: 400, body: JSON.stringify({ error: "issueKey and comment required" }) };
    }

    const adfBody = buildCommentAdf(comment, commentType, title);

    const res = await api.asApp().requestJira(route`/rest/api/3/issue/${issueKey}/comment`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ body: adfBody }),
    });

    if (!res.ok) {
      const err = await res.text();
      console.error("addComment failed:", res.status, err);
      return { statusCode: res.status, body: JSON.stringify({ error: "Failed to add comment", details: err }) };
    }

    const created = await res.json();
    return {
      statusCode: 201,
      headers: { "Content-Type": ["application/json"] },
      body: JSON.stringify({ id: created.id }),
    };
  } catch (e) {
    console.error("addComment error:", e);
    return { statusCode: 500, body: JSON.stringify({ error: e.message }) };
  }
};

/**
 * Download image attachments from Jira and return as base64-encoded objects.
 * Caps at 5 images, 5MB each to keep payload reasonable.
 */
async function fetchImageAttachments(fields) {
  const attachments = fields.attachment || [];
  const imageTypes = ["image/png", "image/jpeg", "image/gif", "image/webp"];
  const maxImages = 5;
  const maxSizeBytes = 5 * 1024 * 1024;

  const imageAttachments = attachments.filter(
    (a) => imageTypes.includes(a.mimeType) && (a.size || 0) <= maxSizeBytes
  );

  const results = [];
  for (const att of imageAttachments.slice(0, maxImages)) {
    try {
      const res = await api.asApp().requestJira(route`/rest/api/3/attachment/content/${att.id}`, {
        headers: { Accept: att.mimeType },
      });
      if (!res.ok) {
        console.log(`Failed to download attachment ${att.filename}: ${res.status}`);
        continue;
      }
      const buffer = await res.arrayBuffer();
      const base64 = Buffer.from(buffer).toString("base64");
      results.push({
        filename: att.filename,
        media_type: att.mimeType,
        data_base64: base64,
        size: buffer.byteLength,
      });
    } catch (e) {
      console.log(`Error downloading attachment ${att.filename}: ${e.message}`);
    }
  }
  return results;
}

/**
 * Build ADF (Atlassian Document Format) for a comment.
 * Uses native Jira panel nodes for professional styling.
 */
function buildCommentAdf(comment, commentType, title) {
  // Map comment type to Jira ADF panel type
  const panelTypeMap = {
    clarification: "warning",  // yellow
    no_changes: "info",        // blue
    error: "error",            // red
    success: "success",        // green
  };

  const panelType = panelTypeMap[commentType];

  // Split comment into paragraphs
  const paragraphs = comment.split("\n\n").filter(Boolean).map((para) => ({
    type: "paragraph",
    content: [{ type: "text", text: para.replace(/\n/g, " ") }],
  }));

  // If we have a known panel type, wrap in a panel with bold title
  if (panelType && title) {
    return {
      type: "doc",
      version: 1,
      content: [
        {
          type: "panel",
          attrs: { panelType },
          content: [
            {
              type: "paragraph",
              content: [{ type: "text", text: title, marks: [{ type: "strong" }] }],
            },
            ...paragraphs,
          ],
        },
      ],
    };
  }

  // Fallback: plain paragraphs
  return {
    type: "doc",
    version: 1,
    content: paragraphs.length > 0 ? paragraphs : [{ type: "paragraph", content: [{ type: "text", text: comment }] }],
  };
}

// ── Web trigger: get Jira site URL ───────────────────────────────────────────
exports.getSiteUrlHandler = async () => {
  try {
    const res = await api.asApp().requestJira(route`/rest/api/3/serverInfo`);
    if (!res.ok) {
      return { statusCode: res.status, body: JSON.stringify({ error: "Failed to get server info" }) };
    }
    const info = await res.json();
    return {
      statusCode: 200,
      headers: { "Content-Type": ["application/json"] },
      body: JSON.stringify({ baseUrl: info.baseUrl }),
    };
  } catch (e) {
    console.error("getSiteUrl error:", e);
    return { statusCode: 500, body: JSON.stringify({ error: e.message }) };
  }
};
