// Creating the deployment's own GitHub App with GitHub's manifest flow: the
// installer describes the app (permissions, events, URLs), the person clicks
// "Create GitHub App" once on github.com, GitHub sends back a one-time code,
// and the installer trades it for the app's id, private key, webhook secret
// and OAuth client. Those go straight into the deployment's secret; nothing
// is printed or written elsewhere.
//
// https://docs.github.com/en/apps/sharing-github-apps/registering-a-github-app-from-a-manifest
import http from "node:http";
import crypto from "node:crypto";
import { spawn } from "node:child_process";

/** What the app may do: exactly what CoderHelm uses. */
export const PERMISSIONS = {
  // Read code, push branches with its changes.
  contents: "write",
  // Open and update pull requests, post reviews.
  pull_requests: "write",
  // Read issues it is assigned, comment progress.
  issues: "write",
  // Read check results to fix failing CI.
  checks: "read",
  // Read workflow runs and their logs (CI fixes).
  actions: "read",
  // Read commit statuses (CI that reports statuses, not checks).
  statuses: "read",
  // Change workflow files when a task asks for it.
  workflows: "write",
  // Repository list and basic details.
  metadata: "read",
  // The email of the person who connects their GitHub account.
  email_addresses: "read",
};

/** The webhook events the gateway handles (services/gateway/src/routes/github_webhook.rs). */
export const EVENTS = [
  "issues",
  "issue_comment",
  "pull_request",
  "pull_request_review",
  "pull_request_review_comment",
  "pull_request_review_thread",
  "push",
  "check_run",
  "check_suite",
  "workflow_run",
  "repository",
];

/** The manifest GitHub registers the app from. */
export function buildManifest({ publicUrl, name, redirectUrl }) {
  const url = publicUrl.replace(/\/+$/, "");
  return {
    name,
    url,
    hook_attributes: { url: `${url}/webhooks/github`, active: true },
    redirect_url: redirectUrl,
    callback_urls: [`${url}/auth/github/callback`],
    setup_url: `${url}/settings/github/`,
    setup_on_update: true,
    description: "CoderHelm in your own AWS account: turns issues into pull requests and reviews them.",
    public: false,
    default_permissions: PERMISSIONS,
    default_events: EVENTS,
  };
}

/** Where the manifest form posts: a personal account or an organization. */
export function newAppUrl(org, state) {
  const base = org
    ? `https://github.com/organizations/${encodeURIComponent(org)}/settings/apps/new`
    : "https://github.com/settings/apps/new";
  return `${base}?state=${encodeURIComponent(state)}`;
}

/** The page that posts the manifest to GitHub when opened (one click for the person). */
export function formPage(manifest, action) {
  const esc = (s) => s.replace(/&/g, "&amp;").replace(/"/g, "&quot;").replace(/</g, "&lt;");
  return `<!doctype html><html><head><meta charset="utf-8"><title>Create your CoderHelm GitHub App</title></head>
<body style="font-family:system-ui,sans-serif;max-width:640px;margin:48px auto;padding:0 16px">
<h1>Create your CoderHelm GitHub App</h1>
<p>GitHub will show the app's name, permissions and events. Change the name if you like, then press <b>Create GitHub App</b>.</p>
<form id="f" method="post" action="${esc(action)}">
<input type="hidden" name="manifest" value="${esc(JSON.stringify(manifest))}">
<button type="submit" style="font-size:16px;padding:10px 18px">Continue to GitHub</button>
</form>
<script>document.getElementById("f").submit()</script>
</body></html>`;
}

/** Trades the one-time code for the app's credentials. */
export async function convertCode(code, fetchImpl = fetch) {
  if (!/^[A-Za-z0-9_-]{8,128}$/.test(code)) throw new Error("GitHub returned an unexpected code");
  const r = await fetchImpl(`https://api.github.com/app-manifests/${code}/conversions`, {
    method: "POST",
    headers: { accept: "application/vnd.github+json", "user-agent": "coderhelm-selfhost" },
  });
  if (!r.ok) throw new Error(`GitHub refused the app conversion (${r.status}). The code is valid for one hour and one use; run github-app again.`);
  const j = await r.json();
  for (const k of ["id", "pem", "webhook_secret", "client_id", "client_secret"]) {
    if (!j[k]) throw new Error(`GitHub's answer has no ${k}`);
  }
  return j;
}

/** The secret keys the services read, from GitHub's answer. */
export function secretKeysFromApp(app) {
  return {
    github_app_id: String(app.id),
    github_private_key: app.pem,
    github_webhook_secret: app.webhook_secret,
    github_client_id: app.client_id,
    github_client_secret: app.client_secret,
    github_app_slug: app.slug ?? "",
  };
}

/**
 * Runs the flow: a local page on 127.0.0.1 posts the manifest to GitHub;
 * GitHub redirects back with a code; the code becomes credentials.
 * Resolves with GitHub's answer (credentials included: the caller stores
 * them and must not print them).
 */
export async function runManifestFlow({ publicUrl, name, org, open = openBrowser, log = console.log, fetchImpl = fetch, timeoutMs = 15 * 60_000 }) {
  const state = crypto.randomBytes(16).toString("hex");
  return new Promise((resolve, reject) => {
    const server = http.createServer(async (req, res) => {
      const u = new URL(req.url, "http://127.0.0.1");
      if (u.pathname === "/") {
        const manifest = buildManifest({ publicUrl, name, redirectUrl: `http://127.0.0.1:${server.address().port}/callback` });
        res.writeHead(200, { "content-type": "text/html; charset=utf-8" });
        res.end(formPage(manifest, newAppUrl(org, state)));
        return;
      }
      if (u.pathname === "/callback") {
        if (u.searchParams.get("state") !== state) {
          res.writeHead(400).end("This link is not from this installer run.");
          return;
        }
        try {
          const app = await convertCode(u.searchParams.get("code") ?? "", fetchImpl);
          res.writeHead(200, { "content-type": "text/html; charset=utf-8" });
          res.end(`<!doctype html><meta charset="utf-8"><body style="font-family:system-ui,sans-serif;margin:48px">
<h1>GitHub App created</h1><p>You can close this tab and go back to the terminal.</p></body>`);
          finish(null, app);
        } catch (e) {
          res.writeHead(500).end(String(e.message));
          finish(e);
        }
        return;
      }
      res.writeHead(404).end();
    });
    const timer = setTimeout(() => finish(new Error("Timed out waiting for GitHub (15 minutes). Run github-app again.")), timeoutMs);
    function finish(err, value) {
      clearTimeout(timer);
      server.close();
      err ? reject(err) : resolve(value);
    }
    server.listen(0, "127.0.0.1", () => {
      const local = `http://127.0.0.1:${server.address().port}/`;
      log(`\nOpen this page to create your GitHub App (it opens by itself if it can):\n  ${local}\n`);
      open(local);
    });
  });
}

export function openBrowser(url) {
  const cmd = process.platform === "darwin" ? "open" : process.platform === "win32" ? "cmd" : "xdg-open";
  const args = process.platform === "win32" ? ["/c", "start", "", url] : [url];
  try {
    spawn(cmd, args, { stdio: "ignore", detached: true }).unref();
  } catch {
    // The URL is printed; the person can open it.
  }
}
