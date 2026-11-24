const api = require("@forge/api");
const { buildCommentAdf } = require("./comment-adf");
const { fetch } = api;
const { storage } = api;
const { route } = api;
const { GATEWAY_URL } = require("./gateway");
const { existingComments } = require("./comments");
const { postReply, parentIdOf } = require("./thread-reply");
const { versionFields } = require("./version");
const { relatedTickets } = require("./related");
const { registerIfVersionChanged, registerAgainReply } = require("./register");
const followups = require("./followups");

/** Jira's comment calls as the app, for a threaded reply (see thread-reply.js). */
const jiraComments = {
  createComment: (issueKey, payload) =>
    api.asApp().requestJira(route`/rest/api/3/issue/${issueKey}/comment`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    }),
  getComment: (issueKey, id) => api.asApp().requestJira(route`/rest/api/3/issue/${issueKey}/comment/${id}`),
};

// The event's comment keys are logged once per running instance: enough to
// see whether Jira's comment events carry a parent id.
let loggedCommentKeys = false;

/** A related ticket's summary, status and description, read as the app. */
async function readRelatedIssue(key) {
  const res = await api.asApp().requestJira(route`/rest/api/3/issue/${key}?fields=summary,description,status`);
  if (!res.ok) return null;
  const full = await res.json();
  return full.fields || null;
}

/** The parts of a Jira user CoderHelm keeps: account id, name, email. */
function jiraUser(u) {
  return { accountId: u.accountId, displayName: u.displayName, emailAddress: u.emailAddress };
}

/**
 * Who asked for the work: the person whose change sent the event (Forge gives
 * their account id), named from the ticket when they are its reporter,
 * creator or assignee. Without an actor, the ticket's reporter.
 */
function requester(actorId, fields) {
  const people = [fields.reporter, fields.creator, fields.assignee].filter(Boolean);
  const named = actorId && people.find((u) => u.accountId === actorId);
  if (named) return jiraUser(named);
  if (actorId) return { accountId: actorId };
  return fields.reporter ? jiraUser(fields.reporter) : {};
}

exports.handler = async (event, context) => {
  const issue = event.issue || {};
  const eventType = event.eventType || "unknown";

  // Load config from Forge storage (set via admin page)
  const config = await storage.get("coderhelm-config");
  if (!config || !config.teamId) {
    console.log(`Skipping ${issue.key} — coderhelm not configured. Go to Apps > coderhelm Settings.`);
    return;
  }

  // Handle comment events
  if (eventType.includes("comment")) {
    // Skip self-generated events (comments posted by this app)
    if (event.selfGenerated) {
      console.log(`Skipping self-generated comment on ${issue.key}`);
      return;
    }

    // Forge comment events include event.comment with author/body metadata.
    // Use it directly; fall back to REST API fetch if missing.
    let comment = event.comment || null;
    if (!loggedCommentKeys) {
      loggedCommentKeys = true;
      console.log(`Comment event on ${issue.key}: event.comment keys=${comment ? Object.keys(comment).join(",") : "null"}, atlassianId=${event.atlassianId || "none"}`);
    }

    if (!comment || !comment.body) {
      // Fallback: fetch the specific comment by ID, or latest if no ID
      const commentId = comment && comment.id;
      try {
        let res;
        if (commentId) {
          res = await api.asApp().requestJira(route`/rest/api/3/issue/${issue.id}/comment/${commentId}`);
          if (res.ok) {
            comment = await res.json();
          }
        }
        if (!commentId || !res || !res.ok) {
          res = await api.asApp().requestJira(route`/rest/api/3/issue/${issue.id}/comment?orderBy=-created&maxResults=1`);
          if (res.ok) {
            const data = await res.json();
            comment = (data.comments || [])[0];
          } else {
            console.log(`Failed to fetch comments for ${issue.key}: ${res.status}`);
            return;
          }
        }
      } catch (e) {
        console.log(`Error fetching comments for ${issue.key}: ${e.message}`);
        return;
      }
    }

    if (!comment) {
      console.log(`No comments found on ${issue.key}`);
      return;
    }

    const author = comment.author || {};
    const authorName = author.displayName || author.accountId || "";

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
        const res = await api.asApp().requestJira(route`/rest/api/3/issue/${issue.id}?fields=summary,description,labels,assignee,reporter,creator,project,attachment,issuetype`);
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

    // Download image attachments from the issue (same as issue-update path)
    const imageAttachments = await fetchImageAttachments(fields);

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
          reporter: fields.reporter,
          creator: fields.creator,
          project: fields.project,
          issuetype: fields.issuetype ? { name: fields.issuetype.name } : undefined,
        },
      },
      comment: {
        id: comment.id,
        body: commentBody,
        // The comment it replies to, when it is a reply in a Jira thread
        // (an undocumented field; absent on sites that don't send it).
        parentId: parentIdOf(comment) || undefined,
        // accountType tells a person ("atlassian") from an app ("app"): the
        // gateway never reads an app's comment as a person's.
        author: { ...jiraUser(author), displayName: authorName, accountType: author.accountType },
      },
      image_attachments: imageAttachments,
      coderhelm: {
        repo_owner: commentRepoOwner || undefined,
        repo_name: commentRepoName || undefined,
        team_id: config.teamId,
        forge_secret: config.forgeSecret,
        // Which version of this app sent it (see version.js).
        ...versionFields(),
      },
    };

    const response = await fetch(`${GATEWAY_URL}/webhooks/jira`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    console.log(`Forwarded comment on ${issue.key} → gateway (${response.status})`);
    await registerIfVersionChanged(config);
    return;
  }

  // Forge event payloads only include issue id/key — fetch full issue via REST API
  let fields = {};
  if (issue.id) {
    try {
      const res = await api.asApp().requestJira(route`/rest/api/3/issue/${issue.id}?fields=summary,description,labels,assignee,reporter,creator,project,attachment,issuetype,comment,issuelinks`);
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
        reporter: fields.reporter,
        creator: fields.creator,
        project: fields.project,
        issuetype: fields.issuetype ? { name: fields.issuetype.name } : undefined,
      },
    },
    image_attachments: imageAttachments,
    // What people already wrote on the ticket: the run that starts reads it
    // before it plans (the gateway keeps each comment once, by its id).
    comments: existingComments(fields),
    // The tickets linked to this one and the ones its text names: their
    // acceptance criteria are part of the change (see related.js).
    related: await relatedTickets(issue.key, fields, readRelatedIssue),
    user: requester(event.atlassianId, fields),
    coderhelm: {
      repo_owner: repoOwner || undefined,
      repo_name: repoName || undefined,
      team_id: config.teamId,
      forge_secret: config.forgeSecret,
      // Which version of this app sent it (see version.js).
      ...versionFields(),
    },
  };

  const response = await fetch(`${GATEWAY_URL}/webhooks/jira`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

  console.log(`Forwarded ${issue.key} → gateway (${response.status})`);
  // A new version (a minor one arrives with no lifecycle event) may have
  // new web triggers: CoderHelm learns them now.
  await registerIfVersionChanged(config);
};

// ── Web trigger: list Jira projects ──────────────────────────────────────────
exports.listProjectsHandler = async (request) => {
  try {
    // CoderHelm asking for every trigger link again (one was missing).
    let asked = null;
    try {
      asked = JSON.parse((request && request.body) || "{}");
    } catch (e) {
      asked = null;
    }
    if (asked && asked.register_again === true) {
      return await registerAgainReply(asked, await storage.get("coderhelm-config"));
    }
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

// ── Follow-up tickets (see followups.js) ─────────────────────────────────────

/** A JSON answer for a web trigger. */
const answer = (statusCode, obj) => ({
  statusCode,
  headers: { "Content-Type": ["application/json"] },
  body: JSON.stringify(obj),
});

/** Jira's error text from a failed call, one line, at most 300 characters. */
async function jiraError(res) {
  const text = await res.text().catch(() => "");
  try {
    const v = JSON.parse(text);
    const parts = [...(v.errorMessages || []), ...Object.values(v.errors || {})];
    if (parts.length > 0) return parts.join("; ").slice(0, 300);
  } catch (_) {
    // not JSON: the text itself
  }
  return text.replace(/\s+/g, " ").trim().slice(0, 300);
}

/** Links `newKey` to `sourceKey` with the site's "relates to" type. */
async function linkToSource(newKey, sourceKey, requestedType) {
  const types = await api.asApp().requestJira(route`/rest/api/3/issueLinkType`);
  if (!types.ok) return { ok: false, error: `link types unreadable (${types.status}): ${await jiraError(types)}` };
  const name = followups.pickLinkType((await types.json()).issueLinkTypes, requestedType);
  if (!name) return { ok: false, error: "this site has no \"relates to\" link type" };
  const res = await api.asApp().requestJira(route`/rest/api/3/issueLink`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ type: { name }, inwardIssue: { key: newKey }, outwardIssue: { key: sourceKey } }),
  });
  if (!res.ok) return { ok: false, type: name, error: `${res.status}: ${await jiraError(res)}` };
  return { ok: true, type: name };
}

/**
 * The follow-ups already filed from `followup.source` in `projectKey`
 * (read only): key, summary, description text and status.
 */
async function listFollowups(body) {
  const { projectKey, followup } = body;
  const jql = followups.dedupeJql(projectKey, followup && followup.source);
  if (!jql) {
    return answer(400, { error: "projectKey and followup.source must be keys" });
  }
  const found = await api.asApp().requestJira(route`/rest/api/3/search/jql`, {
    method: "POST",
    headers: { Accept: "application/json", "Content-Type": "application/json" },
    body: JSON.stringify({
      jql,
      maxResults: followups.MAX_DEDUPE,
      fields: ["summary", "description", "status"],
    }),
  });
  if (!found.ok) {
    return answer(found.status, { error: "search_failed", details: await jiraError(found), project: projectKey });
  }
  return answer(200, { followup: 1, issues: followups.listedIssues((await found.json()).issues) });
}

/**
 * Files one follow-up ticket of `followup.source` in `projectKey`, once:
 * checks the app's permissions, returns the ticket already filed for the
 * same draft, else creates it with the picked issue type and links it.
 */
async function fileFollowup(body) {
  const { projectKey, summary, description, labels, issueType, followup } = body;
  const source = followup && followup.source;
  if (!followups.isIssueKey(source)) {
    return answer(400, { error: "followup.source must be an issue key" });
  }
  // 1. The app's own permissions in the project, so a gap is named exactly.
  const perms = await api
    .asApp()
    .requestJira(
      route`/rest/api/3/mypermissions?projectKey=${projectKey}&permissions=${followups.NEEDED_PERMISSIONS.join(",")}`
    );
  if (!perms.ok) {
    return answer(perms.status, {
      error: "permissions_unreadable",
      details: await jiraError(perms),
      project: projectKey,
    });
  }
  const missing = followups.missingPermissions(await perms.json());
  if (missing.includes("BROWSE_PROJECTS") || missing.includes("CREATE_ISSUES")) {
    return answer(403, {
      error: "missing_permission",
      missing,
      words: followups.permissionWords(missing),
      project: projectKey,
    });
  }
  const label = followups.followupLabel(source);
  // 2. The ticket already filed for this draft.
  const jql = followups.dedupeJql(projectKey, source);
  if (jql) {
    const found = await api.asApp().requestJira(route`/rest/api/3/search/jql`, {
      method: "POST",
      headers: { Accept: "application/json", "Content-Type": "application/json" },
      body: JSON.stringify({ jql, maxResults: followups.MAX_DEDUPE, fields: ["summary", "issuelinks"] }),
    });
    if (found.ok) {
      const same = ((await found.json()).issues || []).find((i) =>
        followups.sameSummary(i.fields && i.fields.summary, summary)
      );
      if (same) {
        let linked = { ok: true, existing: true };
        if (!followups.linkedTo(same, source)) {
          linked = missing.includes("LINK_ISSUES")
            ? { ok: false, error: "missing_permission", missing: ["LINK_ISSUES"] }
            : await linkToSource(same.key, source, followup.linkType);
        }
        return answer(200, { key: same.key, id: same.id, existing: true, linked, followup: 1 });
      }
    }
  }
  // 3. The issue type, from the project's creatable types.
  const meta = await api.asApp().requestJira(route`/rest/api/3/issue/createmeta/${projectKey}/issuetypes`);
  if (!meta.ok) {
    return answer(meta.status, { error: "issue_types_unreadable", details: await jiraError(meta), project: projectKey });
  }
  const metaBody = await meta.json();
  const type = followups.pickIssueType(metaBody.issueTypes || metaBody.values, issueType);
  if (!type) {
    return answer(400, { error: "no_issue_type", project: projectKey });
  }
  const fields = {
    project: { key: projectKey },
    summary: String(summary).slice(0, 254),
    issuetype: { id: type.id },
    labels: [...new Set([...(Array.isArray(labels) ? labels : []), label])],
  };
  if (description) fields.description = followups.descriptionAdf(description);
  const res = await api.asApp().requestJira(route`/rest/api/3/issue`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ fields }),
  });
  if (!res.ok) {
    return answer(res.status, { error: "create_failed", details: await jiraError(res), project: projectKey });
  }
  const created = await res.json();
  // 4. The link to the source ticket.
  const linked = missing.includes("LINK_ISSUES")
    ? { ok: false, error: "missing_permission", missing: ["LINK_ISSUES"] }
    : await linkToSource(created.key, source, followup.linkType);
  return answer(201, {
    key: created.key,
    id: created.id,
    existing: false,
    issue_type: type.name,
    issue_type_from: type.from,
    linked,
    followup: 1,
  });
}

// ── Web trigger: create a Jira ticket ────────────────────────────────────────
exports.createTicketHandler = async (request) => {
  try {
    const body = JSON.parse(request.body || "{}");
    const { projectKey, summary, description, labels } = body;

    // A follow-up of a ticket a run worked on: checked, deduplicated, typed
    // and linked (fileFollowup). Only with the shared secret.
    if (body.followup) {
      // The list of follow-ups already filed: read only, no summary.
      const listing = body.followup.list === true;
      if (!projectKey || (!summary && !listing)) {
        return answer(400, { error: "projectKey and summary required" });
      }
      const config = await storage.get("coderhelm-config");
      if (!config || !config.forgeSecret || body.forge_secret !== config.forgeSecret) {
        return answer(401, { error: "unauthorized" });
      }
      return listing ? await listFollowups(body) : await fileFollowup(body);
    }

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
    const { issueKey, comment, commentType, title, commentId, replyTo } = body;

    if (!issueKey || !comment) {
      return { statusCode: 400, body: JSON.stringify({ error: "issueKey and comment required" }) };
    }

    const adfBody = buildCommentAdf(comment, commentType, title, replyTo);

    // CoderHelm keeps one live comment per subject: it edits its own
    // comment in place instead of adding another.
    if (commentId) {
      const res = await api.asApp().requestJira(route`/rest/api/3/issue/${issueKey}/comment/${commentId}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ body: adfBody }),
      });
      if (!res.ok) {
        const err = await res.text();
        console.error("addComment failed:", res.status, err);
        return { statusCode: res.status, body: JSON.stringify({ error: "Failed to add comment", details: err }) };
      }
      const edited = await res.json();
      return {
        statusCode: 200,
        headers: { "Content-Type": ["application/json"] },
        body: JSON.stringify({ id: edited.id, edited: true }),
      };
    }

    // A new comment. An answer to a person's comment goes under it in Jira's
    // thread where the site takes it; otherwise it is an ordinary comment
    // (its text mentions and quotes the person either way).
    const posted = await postReply({ jira: jiraComments, storage, issueKey, adfBody, replyTo });
    if (!posted.ok) {
      console.error("addComment failed:", posted.status, posted.error);
      return {
        statusCode: posted.status,
        body: JSON.stringify({ error: "Failed to add comment", details: posted.error }),
      };
    }
    return {
      statusCode: 201,
      headers: { "Content-Type": ["application/json"] },
      body: JSON.stringify({ id: posted.id, edited: false, threaded: posted.threaded }),
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
      console.log(`Downloading attachment ${att.filename} (id=${att.id}, mime=${att.mimeType}, size=${att.size})`);
      const res = await api.asApp().requestJira(route`/rest/api/3/attachment/content/${att.id}?redirect=false`);
      if (!res.ok) {
        const body = await res.text().catch(() => "");
        console.log(`Failed to download attachment ${att.filename}: ${res.status} ${body.substring(0, 200)}`);
        continue;
      }
      const buffer = await res.arrayBuffer();
      console.log(`Downloaded ${att.filename}: ${buffer.byteLength} bytes`);
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
