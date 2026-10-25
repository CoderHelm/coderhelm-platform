#!/usr/bin/env node
// The Launch Stack's installer (infra/cfn/coderhelm-selfhost-launch.yaml).
//
// The stack's CodeBuild project downloads this file from the release it
// installs, checks it against the release's signed SHA256SUMS, and runs it.
// It installs CoderHelm in the account the build runs in:
//
//   1. writes the install's config from the stack's parameters (or picks up
//      an existing install of the same name, so a second launch upgrades),
//   2. runs `coderhelm-selfhost deploy` for the release,
//   3. stores the one-time setup key, so the install's own setup endpoint can
//      take the GitHub App's keys from the browser (docs/self-host/setup-checks.md),
//   4. keeps a small progress file up to date in the stack's status bucket,
//      which the install guide in the browser reads. The file holds the
//      phase, a plain message and the install's URL: no names, emails, keys
//      or account numbers.
//
// No Node, AWS CLI or Rust on the person's machine: CodeBuild has them.
//
//   node bootstrap.mjs            (in CodeBuild; reads the environment below)
//   node --test bootstrap.test.mjs
//
// Environment (set by the Launch Stack template):
//   VERSION          release tag, e.g. v0.7.0
//   STAGE            install name (resources start with coderhelm-<stage>)
//   ADMIN_EMAIL      the first admin
//   EMAIL_FROM       sender address; empty means ADMIN_EMAIL
//   HOSTNAME_ARG, HOSTED_ZONE    optional custom domain
//   MODEL_PROVIDER   bedrock | anthropic
//   BEDROCK_PREFIX   auto | us. | eu. | apac. | global. | "" (in-region ids)
//   SIGNUP_DOMAIN    optional email domain anyone may sign up from
//   TELEMETRY        on | off
//   STATUS_BUCKET    the stack's status bucket (empty: no progress file)
//   SETUP_TOKEN      the one-time setup key from the install guide (may be empty)
//   AWS_REGION       set by CodeBuild

import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

export const STATUS_SCHEMA = 1;
export const STATUS_KEY = "status.json";

/** The phases the guide shows, in order. */
export const PHASES = ["starting", "preparing", "deploying", "finishing", "done", "failed"];

const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

/** The Bedrock cross-region inference prefix for a region ("auto"). */
export function bedrockPrefixFor(region) {
  if (/^us-gov-/.test(region)) return "us-gov.";
  if (/^(us|ca)-/.test(region)) return "us.";
  if (/^eu-/.test(region)) return "eu.";
  if (/^ap-/.test(region)) return "apac.";
  return "global.";
}

/** The SES identity to create: none of the person's DNS unless they gave a domain they own in Route 53. */
export function emailIdentity({ from, hostedZone }) {
  // A domain whose Route 53 zone is in this account: its DKIM records are created for you.
  if (hostedZone && from.split("@")[1] === hostedZone) return hostedZone;
  // Otherwise the address itself: AWS emails it a link to click, no DNS records.
  return from;
}

/**
 * The install's config (coderhelm.config.json) from the stack's parameters.
 * Throws with a plain sentence when a parameter can't work.
 */
export function configFromEnv(env, { account }) {
  const region = env.AWS_REGION || env.AWS_DEFAULT_REGION;
  const stage = env.STAGE || "prod";
  const admin = (env.ADMIN_EMAIL || "").trim().toLowerCase();
  const from = (env.EMAIL_FROM || "").trim().toLowerCase() || admin;
  const problems = [];
  if (!/^\d{12}$/.test(String(account))) problems.push("the AWS account number could not be read");
  if (!region) problems.push("the AWS region is not set");
  if (!/^[a-z][a-z0-9-]{0,19}$/.test(stage)) problems.push("the install name must be lowercase letters, digits and hyphens");
  if (!EMAIL_RE.test(admin)) problems.push("the admin email is not an email address");
  if (!EMAIL_RE.test(from)) problems.push("the sender address is not an email address");
  const hostname = (env.HOSTNAME_ARG || "").trim().toLowerCase();
  const hostedZone = (env.HOSTED_ZONE || "").trim().toLowerCase().replace(/\.$/, "");
  if (hostname && !hostedZone) problems.push("a custom hostname needs its Route 53 hosted zone");
  if (hostname && hostedZone && !(hostname === hostedZone || hostname.endsWith(`.${hostedZone}`))) {
    problems.push(`${hostname} is not inside the hosted zone ${hostedZone}`);
  }
  const provider = env.MODEL_PROVIDER === "anthropic" ? "anthropic" : "bedrock";
  if (problems.length) throw new Error(`The stack's settings need a change: ${problems.join("; ")}.`);

  const models = { provider };
  if (provider === "bedrock") {
    const p = env.BEDROCK_PREFIX ?? "auto";
    models.bedrockInferencePrefix = p === "auto" ? bedrockPrefixFor(region) : p;
  }
  const cfg = {
    account: String(account),
    region,
    stage,
    domain: hostname ? { hostname, hostedZone } : null,
    models,
    email: { from, identity: emailIdentity({ from, hostedZone }) },
    admins: [admin],
    telemetry: { enabled: env.TELEMETRY === "on" },
  };
  const signup = (env.SIGNUP_DOMAIN || "").trim().toLowerCase();
  if (signup) cfg.signup = { domains: [signup] };
  return cfg;
}

/** Account numbers and ARNs out of anything that goes in the public progress file. */
export function redact(text) {
  return String(text ?? "")
    .replace(/arn:aws[a-z-]*:[^\s"'`]+/g, "<arn>")
    .replace(/\b\d{12}\b/g, "<account>")
    .replace(/[^\s@"'<>]+@[^\s@"'<>]+\.[a-z]{2,}/gi, "<email>")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 300);
}

/** The progress file the install guide reads. */
export function statusDoc(fields, now = new Date()) {
  const doc = {
    schema: STATUS_SCHEMA,
    phase: fields.phase,
    message: redact(fields.message ?? ""),
    step: fields.step ?? null,
    steps: fields.steps ?? null,
    url: fields.url ?? null,
    version: fields.version ?? null,
    stage: fields.stage ?? null,
    region: fields.region ?? null,
    logUrl: fields.logUrl ?? null,
    fix: fields.fix ?? null,
    updatedAt: now.toISOString(),
  };
  if (!PHASES.includes(doc.phase)) throw new Error(`unknown phase ${doc.phase}`);
  return doc;
}

/**
 * Follows `cdk deploy --progress events` output: which part is deploying,
 * how many are done, and the CloudFormation errors worth showing.
 */
export function createProgress() {
  const p = { step: 0, steps: null, current: null, done: [], errors: [] };
  p.feed = (line) => {
    const clean = line.replace(/\x1b\[[0-9;]*m/g, "");
    let m = clean.match(/^\s*(\S+): deploying\.\.\. \[(\d+)\/(\d+)\]/);
    if (m) {
      p.current = m[1];
      p.step = Number(m[2]);
      p.steps = Number(m[3]);
      return "step";
    }
    m = clean.match(/^\s*✅\s+(\S+)/);
    if (m) {
      if (!p.done.includes(m[1])) p.done.push(m[1]);
      return "done";
    }
    if (/(CREATE|UPDATE|DELETE)_FAILED/.test(clean) && !/Resource creation cancelled/.test(clean)) {
      p.errors.push(clean.trim());
      return "error";
    }
    if (/^\s*❌/.test(clean)) {
      p.errors.push(clean.trim());
      return "error";
    }
    return null;
  };
  return p;
}

/** A plain sentence (and a fix) for the errors people actually hit. */
export function explainFailure(lines) {
  const text = lines.join("\n");
  const rules = [
    [/already exists/i, "Something with the same name is already in this account, probably from an earlier install with this install name.", "Launch again with a different install name, or delete the old CoderHelm stacks in CloudFormation first."],
    [/not authorized to perform|AccessDenied/i, "The installer was refused a permission.", "Sign in to AWS as an administrator (or ask one) and launch again."],
    [/LimitExceeded|quota|Maximum number of/i, "This AWS account hit a service limit.", "Request a higher limit in Service Quotas for the service named in the log, then launch again."],
    [/hosted zone|HostedZone|NoSuchHostedZone/i, "The hosted zone for your domain was not found in Route 53.", "Leave the domain empty to use the free CloudFront address, or enter a hosted zone this account owns."],
    [/CDKToolkit|bootstrap/i, "Preparing the account for deployment (CDK bootstrap) failed.", "Open the log, fix the CDKToolkit stack in CloudFormation (delete it if it is in a failed state), then launch again."],
    [/checksum|sha256sum|verify-blob|signature/i, "The release files did not match their signed checksums, so nothing was installed.", "Launch again. If it happens twice, tell us: the download may have been tampered with."],
    [/Could not resolve host|ENOTFOUND|timed out/i, "The installer could not download the release.", "Launch again in a few minutes."],
  ];
  for (const [re, message, fix] of rules) if (re.test(text)) return { message, fix };
  const last = lines.filter((l) => l.trim()).slice(-1)[0];
  return { message: last ? `The install stopped: ${last}` : "The install stopped.", fix: "Open the log for details, then launch again." };
}

/** The installer's log group: CodeBuild makes it, so it stays when the stack is deleted or rolled back. */
export function logGroup(env) {
  const project = String(env.CODEBUILD_BUILD_ID || "").split(":")[0] || `coderhelm-${env.STAGE || "prod"}-installer`;
  return `/aws/codebuild/${project}`;
}

/** The CloudWatch Logs page for a log group. */
export function logUrl(region, group) {
  return `https://${region}.console.aws.amazon.com/cloudwatch/home?region=${region}#logsV2:log-groups/log-group/${group.replace(/\//g, "$252F")}`;
}

/** The deployment's secret name (infra/lib/config.ts secretPath, default prefix). */
export function secretName(stage) {
  return `coderhelm/${stage}/secrets`;
}

/** The secret with the setup key added, or null when nothing should change. */
export function withSetupToken(secret, token) {
  if (!token || !/^[A-Za-z0-9]{32,64}$/.test(token)) return null;
  if (secret.github_app_id && secret.github_private_key) return null; // the GitHub App exists: the key is never needed
  if (secret.setup_token === token) return null;
  return { ...secret, setup_token: token };
}

// ── Running ────────────────────────────────────────────────────────────────

/** Runs a command; resolves with {code, out}. Each output line goes to onLine too. */
export function sh(cmd, args, { cwd, env, onLine, input } = {}) {
  return new Promise((resolve) => {
    const child = spawn(cmd, args, { cwd, env: env ?? process.env, stdio: ["pipe", "pipe", "pipe"] });
    let out = "";
    let buf = "";
    const take = (d) => {
      const s = d.toString();
      out += s;
      process.stdout.write(s);
      buf += s;
      const lines = buf.split("\n");
      buf = lines.pop();
      for (const l of lines) onLine?.(l);
    };
    child.stdout.on("data", take);
    child.stderr.on("data", take);
    child.on("error", (e) => resolve({ code: 127, out: String(e.message) }));
    child.on("close", (code) => {
      if (buf) onLine?.(buf);
      resolve({ code: code ?? 1, out });
    });
    if (input !== undefined) child.stdin.end(input);
    else child.stdin.end();
  });
}

function writeTemp(value) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "coderhelm-"));
  const file = path.join(dir, "v.json");
  fs.writeFileSync(file, typeof value === "string" ? value : JSON.stringify(value), { mode: 0o600 });
  return { file, done: () => fs.rmSync(dir, { recursive: true, force: true }) };
}

/** The real side effects; the tests pass fakes. */
export function realDeps(env = process.env) {
  const region = env.AWS_REGION;
  return {
    async account() {
      const r = await sh("aws", ["sts", "get-caller-identity", "--query", "Account", "--output", "text"]);
      return r.out.trim();
    },
    async writeStatus(doc) {
      if (!env.STATUS_BUCKET) return;
      const t = writeTemp(doc);
      try {
        await sh("aws", ["s3", "cp", t.file, `s3://${env.STATUS_BUCKET}/${STATUS_KEY}`, "--region", region,
          "--content-type", "application/json", "--cache-control", "no-store", "--only-show-errors"]);
      } finally {
        t.done();
      }
    },
    async installCli(tgz) {
      return sh("npm", ["install", "-g", "--no-audit", "--no-fund", tgz]);
    },
    async pull(stage) {
      return sh("coderhelm-selfhost", ["pull", "--stage", stage, "--region", region]);
    },
    async writeConfig(cfg) {
      fs.writeFileSync("coderhelm.config.json", JSON.stringify(cfg, null, 2) + "\n");
    },
    async deploy(version, onLine) {
      return sh("coderhelm-selfhost", ["deploy", "--yes", "--version", version, "--skip-github-app"], { onLine });
    },
    readState() {
      try {
        return JSON.parse(fs.readFileSync(path.join(".coderhelm", "state.json"), "utf8"));
      } catch {
        return {};
      }
    },
    async readSecret(name) {
      const r = await sh("aws", ["secretsmanager", "get-secret-value", "--secret-id", name, "--region", region, "--query", "SecretString", "--output", "text"]);
      if (r.code !== 0) return null;
      try {
        return JSON.parse(r.out);
      } catch {
        return null;
      }
    },
    async putSecret(name, value) {
      const t = writeTemp(value);
      try {
        const r = await sh("aws", ["secretsmanager", "put-secret-value", "--secret-id", name, "--region", region, "--secret-string", `file://${t.file}`, "--output", "text", "--query", "VersionId"]);
        return r.code === 0;
      } finally {
        t.done();
      }
    },
  };
}

/**
 * The whole install. Resolves with the final status document; never throws
 * (a failure is a "failed" status the guide shows with its fix).
 */
export async function run({ env = process.env, deps = realDeps(env), tgz = "coderhelm-selfhost.tgz", log = console.log } = {}) {
  const region = env.AWS_REGION;
  const stage = env.STAGE || "prod";
  const version = env.VERSION;
  const base = { version, stage, region, logUrl: logUrl(region, logGroup(env)) };
  const status = async (fields) => {
    const doc = statusDoc({ ...base, ...fields });
    try {
      await deps.writeStatus(doc);
    } catch (e) {
      log(`(could not update the progress file: ${e.message})`);
    }
    return doc;
  };
  const fail = (message, fix, extra = {}) => status({ phase: "failed", message, fix, ...extra });

  await status({ phase: "preparing", message: `Downloading CoderHelm ${version}` });
  const inst = await deps.installCli(tgz);
  if (inst.code !== 0) return fail("The installer could not be set up.", "Open the log, then launch again.");

  // A second launch with the same install name upgrades it.
  const pulled = await deps.pull(stage);
  if (pulled.code !== 0) {
    let cfg;
    try {
      cfg = configFromEnv(env, { account: await deps.account() });
    } catch (e) {
      return fail(e.message, "Launch again with corrected settings.");
    }
    await deps.writeConfig(cfg);
  } else {
    log(`Found an existing install named ${stage}: upgrading it to ${version}.`);
  }

  await status({ phase: "deploying", message: "Creating your CoderHelm (about 20 minutes)", step: 0, steps: null });
  const progress = createProgress();
  let last = 0;
  const lines = [];
  const deployed = await deps.deploy(version, (line) => {
    lines.push(line);
    if (lines.length > 400) lines.shift();
    const kind = progress.feed(line);
    // One progress write per new part, at most every 10 seconds.
    if (kind === "step" && Date.now() - last > 10_000) {
      last = Date.now();
      void status({ phase: "deploying", message: `Creating part ${progress.step} of ${progress.steps}`, step: progress.step, steps: progress.steps });
    }
  });
  if (deployed.code !== 0) {
    const { message, fix } = explainFailure(progress.errors.length ? progress.errors : lines.slice(-20));
    return fail(message, fix, { step: progress.step || null, steps: progress.steps });
  }

  await status({ phase: "finishing", message: "Getting the setup page ready", step: progress.steps, steps: progress.steps });
  const url = deps.readState().publicUrl ?? null;

  // The one-time setup key: lets the guide hand the GitHub App's keys
  // straight to this install (GET /api/setup/github/callback).
  if (env.SETUP_TOKEN) {
    const name = secretName(stage);
    const secret = await deps.readSecret(name);
    const next = secret ? withSetupToken(secret, env.SETUP_TOKEN) : null;
    if (next && !(await deps.putSecret(name, next))) log("Could not store the setup key; create the GitHub App with the CLI instead.");
  }

  if (!url) return fail("CoderHelm deployed, but its address could not be read.", "Open the CloudFormation stack named coderhelm-" + stage + "-web and copy its PublicUrl output.");
  return status({ phase: "done", message: "CoderHelm is running", url, step: progress.steps, steps: progress.steps });
}

if (process.argv[1] && fileURLToPath(import.meta.url) === path.resolve(process.argv[1])) {
  const doc = await run();
  fs.writeFileSync("/tmp/coderhelm-final.json", JSON.stringify(doc));
  process.exit(doc.phase === "done" ? 0 : 1);
}
