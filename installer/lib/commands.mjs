// The installer's commands. Each takes a context: the parsed flags, a
// Runner (dry runs print instead of running), a prompter and a logger.
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { checkPrereqs } from "./prereqs.mjs";
import { checkTag, fetchRelease, latestVersion, releasePaths, releasesDir, TAG_RE } from "./release.mjs";
import { openBrowser } from "./github-app.mjs";
import { ensureSecret, hasGithubApp, mergeSecret, readSecret } from "./secrets.mjs";
import { runManifestFlow, secretKeysFromApp } from "./github-app.mjs";
import { leaveHybrid } from "./hybrid.mjs";
import { DEFAULT_TELEMETRY_ENDPOINT, derived, readJson, semverCompare, writeJson } from "./util.mjs";

export const STATE_DIR = ".coderhelm";

// ── Config and state ────────────────────────────────────────────────────

export function configPath(flags) {
  return path.resolve(flags.config ?? "coderhelm.config.json");
}

export function statePath(flags) {
  return path.join(path.dirname(configPath(flags)), STATE_DIR, "state.json");
}

export function loadState(flags) {
  return readJson(statePath(flags), {});
}

export function saveState(flags, state, runner) {
  if (runner.dryRun) return;
  writeJson(statePath(flags), state);
}

export function loadConfig(flags) {
  const file = configPath(flags);
  if (!fs.existsSync(file)) {
    throw new Error(`No config at ${file}. Run "coderhelm-selfhost init" first (or pass --config).`);
  }
  return readJson(file);
}

/** The config init writes from answers (validated fully by the CDK app at deploy). */
export function buildConfig(a) {
  const cfg = {
    account: a.account,
    region: a.region,
    stage: a.stage,
    domain: a.hostname
      ? { hostname: a.hostname, ...(a.zone ? { hostedZone: a.zone } : {}), ...(a.certificateArn ? { certificateArn: a.certificateArn } : {}) }
      : null,
    models: { provider: a.provider, ...(a.provider === "bedrock" && a.bedrockPrefix !== undefined ? { bedrockInferencePrefix: a.bedrockPrefix } : {}) },
    email: { from: a.emailFrom },
    admins: [a.admin],
    signup: a.signupDomain ? { domains: [a.signupDomain] } : {},
    integrations: { googleSignIn: false, sandbox: true, logAnalyzer: true, weeklySummary: true },
    telemetry: { enabled: !!a.telemetry },
  };
  return cfg;
}

/** Quick checks before the CDK app's full validation, for early, clear errors. */
export function quickCheck(cfg) {
  const p = [];
  if (!/^\d{12}$/.test(cfg.account ?? "")) p.push("account must be a 12-digit AWS account id");
  if (!/^[a-z]{2}(-gov)?-[a-z]+-\d$/.test(cfg.region ?? "")) p.push("region must be an AWS region such as us-east-1");
  if (!/^[a-z][a-z0-9-]{0,19}$/.test(cfg.stage ?? "")) p.push("stage: lowercase letters, digits and hyphens");
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(cfg.email?.from ?? "")) p.push("email.from must be an email address");
  if (!Array.isArray(cfg.admins) || !cfg.admins.length) p.push("admins: add at least one email address");
  if (cfg.layout === "split") p.push('layout "split" is for the hosted deployment; the installer deploys "single-origin"');
  return p;
}

// ── deploy ───────────────────────────────────────────────────────────────

/** Where the CDK app and the Lambda code come from: a release, or this checkout. */
export async function resolveSource(ctx, version) {
  if (ctx.resolveSource) return ctx.resolveSource(ctx, version);
  const { flags, runner, log } = ctx;
  if (flags.source) {
    const root = path.resolve(flags.source);
    log(`Using the CoderHelm source at ${root} (Lambda code must be built there).`);
    return { version: "source", infra: path.join(root, "infra"), env: {}, dashboard: flags.dashboard ? path.resolve(flags.dashboard) : null };
  }
  const tag = version ?? (runner.dryRun ? "v0.0.0-dry-run" : await latestVersion(ctx.fetchImpl));
  log(`Release ${tag}`);
  // Downloads, then checks the checksums and the signature before unpacking.
  const paths = await fetchRelease(tag, { runner, fetchImpl: ctx.fetchImpl, log });
  return {
    version: tag,
    infra: paths.infra,
    dashboard: paths.dashboard,
    env: { GATEWAY_ZIP: paths.gatewayZip, WORKER_ZIP: paths.workerZip, MCP_PROXY_DIR: paths.mcpProxyZip },
  };
}

export async function cdk(ctx, src, args, extraEnv = {}) {
  const { runner, flags } = ctx;
  const env = {
    ...src.env,
    ...(src.dashboard ? { CODERHELM_DASHBOARD_DIR: src.dashboard } : {}),
    CODERHELM_CONFIG: configPath(flags),
    ...extraEnv,
  };
  return runner.run("npx", ["--yes", "cdk", ...args], { cwd: src.infra, env });
}

export async function deploy(ctx) {
  const { flags, runner, log } = ctx;
  const cfg = loadConfig(flags);
  const bad = quickCheck(cfg);
  if (bad.length) throw new Error(`Fix the config first:\n  - ${bad.join("\n  - ")}`);
  const d = derived(cfg);
  const state = loadState(flags);

  log("Checking prerequisites");
  const { problems } = await checkPrereqs(runner, { account: cfg.account });
  if (problems.length) throw new Error(`Before installing:\n  - ${problems.join("\n  - ")}`);

  const src = await resolveSource(ctx, flags.version ?? state.version);

  log("Preparing the secret");
  await ensureSecret(runner, d.secretName, cfg.region, log);

  log("Installing the CDK app's packages");
  if (runner.dryRun || !fs.existsSync(path.join(src.infra, "node_modules"))) {
    await runner.run("npm", ["ci", "--no-audit", "--no-fund"], { cwd: src.infra });
  }

  if (!flags.skipBootstrap) {
    log("Bootstrapping CDK (once per account and region)");
    await cdk(ctx, src, ["bootstrap", `aws://${cfg.account}/${cfg.region}`]);
    if (cfg.domain?.hostname && cfg.region !== "us-east-1" && !cfg.domain.certificateArn) {
      await cdk(ctx, src, ["bootstrap", `aws://${cfg.account}/us-east-1`]);
    }
  }

  const outputsFile = path.join(path.dirname(statePath(flags)), "outputs.json");
  log("Deploying (the first time takes about 15 minutes, mostly CloudFront)");
  // Live progress: every resource as CloudFormation creates it.
  await cdk(ctx, src, ["deploy", "--all", "--require-approval", "never", "--progress", "events", "--outputs-file", outputsFile],
    src.version && src.version !== "source" ? { CODERHELM_VERSION: src.version } : {});

  const outputs = runner.dryRun ? {} : readJson(outputsFile, {});
  const known = findOutput(outputs, "PublicUrl") ?? (cfg.domain?.hostname ? `https://${cfg.domain.hostname}` : state.publicUrl);
  const publicUrl = known ?? "https://<your CloudFront URL>";
  const next = { ...state, installId: state.installId ?? crypto.randomUUID(), version: src.version, deployedAt: new Date().toISOString(), ...(known ? { publicUrl: known } : {}) };
  if (state.version && state.version !== src.version) next.previousVersion = state.version;
  next.versions = ranVersions(next);
  saveState(flags, next, runner);
  await saveRemote(ctx, cfg, next);

  // The GitHub App: once, after the URL is known.
  const secret = await readSecret(runner, d.secretName, cfg.region);
  if (!hasGithubApp(secret) && !flags.skipGithubApp) {
    if (ctx.interactive || flags.githubApp) {
      await githubApp({ ...ctx, flags: { ...flags, url: publicUrl } }, { src });
    } else {
      log('\nNo GitHub App yet. Run "coderhelm-selfhost github-app" from a machine with a browser.');
    }
  }

  await telemetryPing(ctx, cfg, next, state.version ? "upgrade" : "install");

  if (!flags.quiet) {
    log(`\nCoderHelm ${src.version} is deployed at ${publicUrl}`);
    if (!flags.quietReady) log(checklist(cfg, publicUrl, await readSecret(runner, d.secretName, cfg.region)));
    if (ctx.interactive && !runner.dryRun && !publicUrl.includes("<")) (ctx.open ?? openBrowser)(`${publicUrl}/?signup=1`);
  }
  return { publicUrl, version: src.version };
}

/** What is done and what is left, after a deploy. */
export function checklist(cfg, url, secret) {
  const tick = (ok) => (ok ? "[x]" : "[ ]");
  const lines = [
    "",
    "Checklist",
    `  ${tick(true)} CoderHelm deployed`,
    `  ${tick(hasGithubApp(secret))} GitHub App created${hasGithubApp(secret) ? "" : " (coderhelm-selfhost github-app)"}`,
    `  [ ] Sign up as the first admin (${cfg.admins?.[0] ?? "an admin in your config"}): ${url}/?signup=1`,
    "  [ ] Install the GitHub App on your repositories",
    cfg.models?.provider === "bedrock"
      ? "  [x] Models: Amazon Bedrock (no key needed)"
      : "  [ ] Add your team's Anthropic key in Settings, AI models",
    "  [ ] Send a test email from the setup page (new AWS accounts start in the SES sandbox)",
    `  ${tick(cfg.updates?.auto === true)} Auto-update ${cfg.updates?.auto ? "on" : "off (Settings, System)"}`,
    "",
    `The setup page checks each of these live: ${url}/settings/setup/`,
    `Check everything from here: ${CHECK_CMD}`,
  ];
  return lines.join("\n");
}

export function findOutput(outputs, key) {
  for (const stack of Object.values(outputs ?? {})) {
    if (stack && typeof stack === "object" && stack[key]) return stack[key];
  }
  return undefined;
}

// ── config kept in the account (pull) ───────────────────────────────────

/** SSM names holding the install's config and state (no secrets in either). */
export function remoteNames(cfg) {
  const { prefix } = derived(cfg);
  return { config: `/${prefix}/installer/config`, state: `/${prefix}/installer/state` };
}

/**
 * Keeps the config and state in the account too, so any machine (or the
 * Launch Stack's build) can upgrade later: "coderhelm-selfhost pull".
 */
export async function saveRemote(ctx, cfg, state) {
  const n = remoteNames(cfg);
  for (const [name, value] of [[n.config, cfg], [n.state, { ...state, publicUrl: state.publicUrl }]]) {
    await ctx.runner.run("aws", [
      "ssm", "put-parameter", "--name", name, "--type", "String", "--overwrite",
      "--region", cfg.region, "--value", JSON.stringify(value),
    ], { capture: true, allowFail: true });
  }
}

/** Writes coderhelm.config.json and the state from the account. */
export async function pull(ctx) {
  const { flags, runner, log } = ctx;
  if (!flags.stage || !flags.region) throw new Error("pull needs --stage and --region (and AWS credentials for the account).");
  const n = remoteNames({ stage: flags.stage, resourcePrefix: flags.resourcePrefix, account: "0" });
  const get = (name) =>
    runner.json("aws", ["ssm", "get-parameter", "--name", name, "--region", flags.region, "--query", "Parameter.Value", "--output", "json"], {
      allowFail: true,
      dryRunAnswer: "null",
    });
  const cfgText = await get(n.config);
  if (typeof cfgText !== "string") throw new Error(`No install named ${flags.stage} in ${flags.region} (no ${n.config}).`);
  const stateText = await get(n.state);
  if (runner.dryRun) return log(`  [dry run] write ${configPath(flags)} and ${statePath(flags)}`);
  writeJson(configPath(flags), JSON.parse(cfgText));
  if (typeof stateText === "string") writeJson(statePath(flags), JSON.parse(stateText));
  log(`Wrote ${configPath(flags)} from the account.`);
}

// ── github-app ───────────────────────────────────────────────────────────

export async function githubApp(ctx, { src } = {}) {
  const { flags, runner, log } = ctx;
  const cfg = loadConfig(flags);
  const d = derived(cfg);
  const state = loadState(flags);
  const publicUrl = flags.url ?? state.publicUrl;
  if (!publicUrl || publicUrl.includes("<")) throw new Error('Deploy first (the app needs your URL), or pass --url https://...');
  const existing = await readSecret(runner, d.secretName, cfg.region);
  if (hasGithubApp(existing) && !flags.force) {
    log("A GitHub App is already stored. Pass --force to create a new one.");
    return null;
  }
  const name = flags.name ?? `CoderHelm ${cfg.stage} ${cfg.account.slice(-4)}`;
  // An app made by hand (where the manifest flow isn't available): its
  // values from flags, the key from a file, the secrets from the environment.
  if (flags.appId) {
    const keys = manualAppKeys(flags, process.env);
    if (runner.dryRun) {
      log(`  [dry run] store GitHub App ${keys.github_app_id} in ${d.secretName}`);
      return null;
    }
    const version = await mergeSecret(runner, d.secretName, cfg.region, keys);
    log(`Stored GitHub App ${keys.github_app_id} in ${d.secretName}.`);
    const source = src ?? (await resolveSource(ctx, state.version));
    await cdk(ctx, source, ["deploy", "--all", "--require-approval", "never"], { CODERHELM_SECRETS_REVISION: version });
    return { id: keys.github_app_id };
  }
  if (runner.dryRun) {
    log(`  [dry run] create GitHub App "${name}" for ${publicUrl}${flags.org ? ` in ${flags.org}` : ""} and store its keys in ${d.secretName}`);
    return null;
  }
  const app = await (ctx.manifestFlow ?? runManifestFlow)({ publicUrl, name, org: flags.org, log, fetchImpl: ctx.fetchImpl });
  const version = await mergeSecret(runner, d.secretName, cfg.region, secretKeysFromApp(app));
  log(`Stored the GitHub App's keys in ${d.secretName}.`);

  // The services read the secret at start: restart them on the new version.
  const source = src ?? (await resolveSource(ctx, state.version));
  log("Restarting the services so they read the new keys");
  await cdk(ctx, source, ["deploy", "--all", "--require-approval", "never"], { CODERHELM_SECRETS_REVISION: version });

  log(`\nInstall the app on your repositories: ${app.html_url}/installations/new`);
  return { id: app.id, slug: app.slug, installUrl: `${app.html_url}/installations/new` };
}

/**
 * The secret keys for an app created by hand: --app-id, --client-id and
 * --private-key-file from flags; the webhook and client secrets from
 * GITHUB_WEBHOOK_SECRET and GITHUB_CLIENT_SECRET (never argv).
 */
export function manualAppKeys(flags, env) {
  const missing = [];
  if (!/^\d+$/.test(String(flags.appId))) missing.push("--app-id (the app's number)");
  if (!flags.clientId) missing.push("--client-id");
  if (!flags.privateKeyFile || !fs.existsSync(flags.privateKeyFile)) missing.push("--private-key-file (the .pem GitHub gave you)");
  if (!env.GITHUB_WEBHOOK_SECRET) missing.push("GITHUB_WEBHOOK_SECRET in the environment");
  if (!env.GITHUB_CLIENT_SECRET) missing.push("GITHUB_CLIENT_SECRET in the environment");
  if (missing.length) throw new Error(`For an app made by hand, give:\n  - ${missing.join("\n  - ")}`);
  const pem = fs.readFileSync(flags.privateKeyFile, "utf8");
  if (!pem.includes("PRIVATE KEY")) throw new Error(`${flags.privateKeyFile} is not a private key`);
  return {
    github_app_id: String(flags.appId),
    github_private_key: pem,
    github_webhook_secret: env.GITHUB_WEBHOOK_SECRET,
    github_client_id: String(flags.clientId),
    github_client_secret: env.GITHUB_CLIENT_SECRET,
  };
}

// ── upgrade / rollback ───────────────────────────────────────────────────

/**
 * Notes for every release after `from` up to `to` (MIGRATIONS.json in the
 * target release). A note with `breaking: true` stops the upgrade unless
 * --yes is given, after printing what to do.
 */
export function migrationNotes(migrations, from, to) {
  return (migrations ?? [])
    .filter((m) => (!from || semverCompare(m.version, from) > 0) && semverCompare(m.version, to) <= 0)
    .sort((a, b) => semverCompare(a.version, b.version));
}

/**
 * Moves the install to a release, safely:
 *   1. health check first (an unhealthy install isn't updated, unless --force);
 *   2. the release's checksums and signature (always: a release that can't
 *      be verified is refused);
 *   3. the migration notes it crosses (action-needed ones stop it without --yes);
 *   4. deploy;
 *   5. health check after; when it fails and --auto-rollback is set, the
 *      previous release is deployed again.
 * --result-file gets {status, from, to, reason} (the update build reports it).
 */
export async function update(ctx) {
  const { flags, runner, log } = ctx;
  const state = loadState(flags);
  const result = { status: "failed", from: state.version ?? null, to: null, reason: "" };
  const done = (status, reason) => {
    Object.assign(result, { status, reason });
    if (flags.resultFile && !runner.dryRun) writeJson(path.resolve(flags.resultFile), result);
    return result;
  };
  try {
    const target = flags.to ?? (runner.dryRun ? "v0.0.0-dry-run" : await latestVersion(ctx.fetchImpl));
    result.to = target;
    if (state.version && state.version === target && !flags.force) {
      log(`Already on ${target}.`);
      return done("succeeded", `already on ${target}`);
    }
    if (state.version && semverCompare(target, state.version) < 0 && !flags.rollback) {
      throw new Error(`${target} is older than ${state.version}. Use "coderhelm-selfhost rollback" to go back.`);
    }
    // Going back only to a release this install ran before: an older signed
    // release with a since-fixed problem can't be pushed onto it.
    if (flags.rollback) checkRollbackTarget(state, target);
    log(`${flags.rollback ? "Rolling back" : "Updating"} ${state.version ?? "(unknown)"} to ${target}`);

    // Preflight: what would stop the update, each with its fix, before
    // anything is downloaded or changed.
    log("\nBefore the update");
    const pre = preflightProblems(loadConfig(flags), { freeBytes: ctx.freeBytes });
    for (const p of pre) log(`  FAIL  ${p.what}\n        How to fix: ${p.fix}`);
    if (pre.length && !flags.force) throw plainError(`The update can't start: ${pre.length} problem${pre.length === 1 ? "" : "s"} above. Fix ${pre.length === 1 ? "it" : "them"}, then run the update again.`);
    if (!flags.skipHealth) {
      log("Health check before the update");
      const before = await health({ ...ctx, quiet: true });
      if (before !== 0 && !flags.force) {
        throw plainError(`The install isn't healthy before the update (see above), so it wasn't touched. Fix what failed (each FAIL says how), check with "${CHECK_CMD}", then run the update again. --force updates anyway.`);
      }
    }

    const src = await resolveSource(ctx, target);
    const migrationsFile = path.join(path.dirname(src.infra), "selfhost", "MIGRATIONS.json");
    const notes = migrationNotes(runner.dryRun ? [] : readJson(migrationsFile, { releases: [] }).releases, state.version, target);
    if (notes.length) log("Release notes this update crosses:");
    for (const n of notes) {
      log(`  ${n.version}${n.breaking ? "  NEEDS ACTION" : ""}: ${n.notes}`);
      if (n.breaking && n.action) log(`        What to do: ${n.action}`);
    }
    if (notes.some((n) => n.breaking) && !flags.yes) {
      throw plainError("This update crosses a release that needs action (marked above). Do what it says, then run the update again with --yes.");
    }
    if (ctx.interactive && !flags.yes) {
      log("Changes this update makes (cdk diff):");
      await cdk(ctx, src, ["diff", "--all"]).catch(() => null);
      if (!(await ctx.ask.confirm("Deploy these changes?", true))) return done("failed", "cancelled");
    }

    const deployFlags = { ...flags, version: target, skipGithubApp: true, skipBootstrap: true, quiet: true };
    await deploy({ ...ctx, flags: deployFlags, resolveSource: async () => src });

    if (!flags.skipHealth) {
      log("Health check after the update");
      const after = await health({ ...ctx, quiet: true });
      if (after !== 0) {
        if (flags.autoRollback && result.from && result.from !== target) {
          log(`The new release is unhealthy: rolling back to ${result.from}.`);
          const back = await resolveSource(ctx, result.from);
          await deploy({ ...ctx, flags: { ...deployFlags, version: result.from }, resolveSource: async () => back });
          log(afterFailure(target, result.from, true));
          return done("rolled_back", `health check failed after deploying ${target}; back on ${result.from}`);
        }
        log(afterFailure(target, result.from, false));
        return done("failed", `health check failed after deploying ${target}`);
      }
    }
    log(`\nNow on ${target}.`);
    return done("succeeded", `now on ${target}`);
  } catch (e) {
    done("failed", e.message);
    throw e;
  }
}

const CHECK_CMD = "npx -y github:CoderHelm/coderhelm-platform doctor";

function plainError(message) {
  return Object.assign(new Error(message), { plain: true });
}

/** Room for a release (about 400 MB unpacked, plus the CDK's build output). */
export const MIN_FREE_BYTES = 1.5 * 1024 ** 3;

/** Config and disk problems that would stop an update, each with its fix. */
export function preflightProblems(cfg, { freeBytes } = {}) {
  const out = quickCheck(cfg).map((p) => ({ what: `Config: ${p}.`, fix: `Edit ${"coderhelm.config.json"} (docs/self-host/config-reference.md).` }));
  let free = freeBytes;
  if (free === undefined) {
    try {
      const dir = fs.existsSync(releasesDir()) ? releasesDir() : path.dirname(releasesDir());
      const st = fs.statfsSync(fs.existsSync(dir) ? dir : process.cwd());
      free = st.bavail * st.bsize;
    } catch {
      free = Infinity;
    }
  }
  if (free < MIN_FREE_BYTES) {
    out.push({
      what: `Only ${(free / 1024 ** 3).toFixed(1)} GB free where releases are kept (${releasesDir()}); an update needs about 1.5 GB.`,
      fix: `Delete old releases there (keep the one you run), or set CODERHELM_HOME to a folder on a bigger disk.`,
    });
  }
  return out;
}

/** What went wrong after a deploy, and what to do, in plain words. */
export function afterFailure(target, from, rolledBack) {
  return [
    "",
    rolledBack
      ? `The update to ${target} deployed, but the install wasn't healthy afterwards, so it went back to ${from}. Your data is untouched.`
      : `The update to ${target} deployed, but the install isn't healthy now.`,
    "What to do:",
    `  1. See what fails and how to fix it: ${CHECK_CMD}`,
    "  2. If a stack failed, its Events tab in the CloudFormation console shows the first error.",
    rolledBack
      ? `  3. Then try again: npx -y github:CoderHelm/coderhelm-platform update --to ${target}`
      : `  3. Go back to the version before: npx -y github:CoderHelm/coderhelm-platform rollback${from ? ` --to ${from}` : ""}`,
    "  Auto-update skips this release until an admin updates to it by hand.",
  ].join("\n");
}

/** How to fix each health check that fails. */
const HEALTH_FIX = {
  Stacks: "Open the failed stack's Events tab in the CloudFormation console for the first error, fix it, then deploy again.",
  "Dead-letter queue": "Open the failed events page in the dashboard (/admin/queue/) to retry or clear them.",
  "GitHub App keys": "Create the app: npx -y github:CoderHelm/coderhelm-platform github-app",
  "Email sending": "Request SES production access in the SES console (Account dashboard). Not needed for the update itself.",
  Dashboard: "Deploy again; the dashboard files are uploaded with it.",
  API: "Check the gateway function's logs in CloudWatch, then deploy again.",
};

/** Every release this install has run (state.versions, plus the current and previous one). */
export function ranVersions(state) {
  const all = [...(state.versions ?? []), state.previousVersion, state.version];
  return [...new Set(all.filter((v) => typeof v === "string" && TAG_RE.test(v)))].sort(semverCompare).slice(-50);
}

/** A rollback goes only to an older release this install ran before. */
export function checkRollbackTarget(state, target) {
  checkTag(target);
  const ran = ranVersions(state).filter((v) => v !== state.version);
  if (!ran.includes(target)) {
    throw plainError(`${target} is not a release this install ran before (${ran.join(", ") || "none recorded"}), so it can't be rolled back to. Update forward instead.`);
  }
}

/** The old name for update. */
export const upgrade = update;

export async function rollback(ctx) {
  const { flags } = ctx;
  const state = loadState(flags);
  const to = flags.to ?? state.previousVersion;
  if (!to) throw new Error("No earlier version is recorded. Pass --to vX.Y.Z.");
  ctx.log(`Rolling back to ${to}. Data written by the newer version stays; releases only add fields, so the older version reads it.`);
  return update({ ...ctx, flags: { ...flags, to, rollback: true, force: true } });
}

// ── health ───────────────────────────────────────────────────────────────

export async function health(ctx) {
  const { flags, runner, log } = ctx;
  const cfg = loadConfig(flags);
  const d = derived(cfg);
  const state = loadState(flags);
  const r = (args, answer) => runner.json("aws", [...args, "--region", cfg.region, "--output", "json"], { allowFail: true, dryRunAnswer: answer });
  const results = [];
  const check = (name, ok, detail = "") => results.push({ name, ok, detail });

  const stacks = await r(["cloudformation", "list-stacks", "--stack-status-filter",
    "CREATE_COMPLETE", "UPDATE_COMPLETE", "UPDATE_ROLLBACK_COMPLETE", "UPDATE_IN_PROGRESS", "UPDATE_ROLLBACK_FAILED", "CREATE_FAILED", "ROLLBACK_COMPLETE"],
    JSON.stringify({ StackSummaries: [] }));
  const ours = (stacks?.StackSummaries ?? []).filter((s) => s.StackName.startsWith(`${d.prefix}-`));
  const bad = ours.filter((s) => !/_COMPLETE$/.test(s.StackStatus) || /ROLLBACK/.test(s.StackStatus));
  check("Stacks", runner.dryRun || (ours.length >= 7 && bad.length === 0), `${ours.length} found${bad.length ? `; not healthy: ${bad.map((s) => `${s.StackName} ${s.StackStatus}`).join(", ")}` : ""}`);

  for (const fn of ["gateway", "worker", "streaming"]) {
    const c = await r(["lambda", "get-function-configuration", "--function-name", `${d.prefix}-${fn}`], JSON.stringify({ State: "Active", LastUpdateStatus: "Successful" }));
    check(`Function ${fn}`, c?.State === "Active" && c?.LastUpdateStatus !== "Failed", c ? `${c.State}/${c.LastUpdateStatus}` : "not found");
  }

  const q = await r(["sqs", "get-queue-url", "--queue-name", `${d.prefix}-dlq`], JSON.stringify({ QueueUrl: "dry-run" }));
  if (q?.QueueUrl) {
    const a = await r(["sqs", "get-queue-attributes", "--queue-url", q.QueueUrl, "--attribute-names", "ApproximateNumberOfMessages"], JSON.stringify({ Attributes: { ApproximateNumberOfMessages: "0" } }));
    const n = Number(a?.Attributes?.ApproximateNumberOfMessages ?? 0);
    check("Dead-letter queue", n === 0, n ? `${n} failed messages (see the admin queue page)` : "empty");
  }

  const secret = await readSecret(runner, d.secretName, cfg.region);
  check("GitHub App keys", runner.dryRun || hasGithubApp(secret), hasGithubApp(secret) ? "stored" : 'missing: run "coderhelm-selfhost github-app"');

  const ses = await r(["sesv2", "get-account"], JSON.stringify({ ProductionAccessEnabled: true }));
  check("Email sending", runner.dryRun || !!ses?.ProductionAccessEnabled, ses?.ProductionAccessEnabled ? "production access" : "SES sandbox: email only reaches verified addresses (see docs/self-host/email.md)");

  const url = state.publicUrl;
  if (url && !url.includes("<") && !runner.dryRun) {
    const page = await (ctx.fetchImpl ?? fetch)(`${url}/`).catch(() => null);
    check("Dashboard", page?.status === 200, page ? `HTTP ${page.status}` : "unreachable");
    const api = await (ctx.fetchImpl ?? fetch)(`${url}/api/me`).catch(() => null);
    check("API", api?.status === 401 || api?.status === 200, api ? `HTTP ${api.status} (401 = up, not signed in)` : "unreachable");
  }

  for (const x of results) {
    log(`${x.ok ? "ok  " : "FAIL"}  ${x.name.padEnd(18)} ${x.detail}`);
    const fix = HEALTH_FIX[x.name] ?? (x.name.startsWith("Function") ? "Deploy again; it replaces the function: npx -y github:CoderHelm/coderhelm-platform deploy" : null);
    if (!x.ok && fix) log(`      How to fix: ${fix}`);
  }
  const version = state.version ? ` (on ${state.version})` : "";
  if (!ctx.quiet) log(`Install ${d.prefix}${version}`);
  const failed = results.filter((x) => !x.ok).length;
  log(failed ? `\n${failed} check(s) failed. For every check with its fix: ${CHECK_CMD}` : "\nAll checks passed.");
  await telemetryPing(ctx, cfg, state, "health");
  return failed ? 1 : 0;
}

// ── backup ───────────────────────────────────────────────────────────────

export async function backup(ctx) {
  const { flags, runner, log } = ctx;
  const cfg = loadConfig(flags);
  const d = derived(cfg);
  const listed = await runner.json("aws", ["dynamodb", "list-tables", "--region", cfg.region, "--output", "json"], {
    dryRunAnswer: JSON.stringify({ TableNames: [`${d.prefix}-runs`, `${d.prefix}-settings`] }),
  });
  const tables = (listed?.TableNames ?? []).filter((t) => t === d.prefix || t.startsWith(`${d.prefix}-`));
  if (!tables.length) throw new Error(`No tables start with ${d.prefix}.`);
  const stamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
  for (const t of tables) {
    await runner.run("aws", ["dynamodb", "create-backup", "--table-name", t, "--backup-name", `${t}-${stamp}`, "--region", cfg.region], { capture: true });
    log(`  backed up ${t}`);
  }
  log(`\n${tables.length} on-demand table backups made (kept until you delete them).`);
  log("Point-in-time recovery is on for every table (35 days), and the data bucket keeps old versions for 30 days.");
  return tables;
}

// ── uninstall ────────────────────────────────────────────────────────────

export async function uninstall(ctx) {
  const { flags, runner, log } = ctx;
  const cfg = loadConfig(flags);
  const d = derived(cfg);
  if (!flags.yes) {
    if (!ctx.interactive) throw new Error("Pass --yes to uninstall without a prompt.");
    const typed = await ctx.ask.ask(`Type the install name (${cfg.stage}) to delete CoderHelm from ${cfg.account}`, "");
    if (typed !== cfg.stage) throw new Error("Not uninstalled.");
  }
  const state = loadState(flags);
  await telemetryPing(ctx, cfg, state, "uninstall");
  // A hybrid install gives back its CoderHelm address and links first.
  const secret = await readSecret(runner, d.secretName, cfg.region);
  await leaveHybrid(ctx, secret, cfg.hybridUrl || undefined);
  const src = await resolveSource(ctx, state.version);
  await cdk(ctx, src, ["destroy", "--all", "--force"]);
  log("\nThe stacks are deleted.");
  if (cfg.retainData !== false) {
    log(`Kept on purpose (retainData): the ${d.prefix}-* DynamoDB tables, the ${d.prefix}-data${d.suffix} bucket, their KMS keys, the Cognito user pool and the secret ${d.secretName}.`);
    log("Delete them in the AWS console when you are sure, or set retainData to false before uninstalling.");
  }
}

// ── telemetry ────────────────────────────────────────────────────────────

export function telemetryPayload(cfg, state, event, version) {
  return {
    install_id: state.installId,
    version: String(version ?? state.version ?? "unknown").replace(/[^A-Za-z0-9._+-]/g, "").slice(0, 40) || "unknown",
    event,
    layout: "single-origin",
    provider: cfg.models?.provider === "bedrock" ? "bedrock" : "anthropic",
    region: cfg.region,
  };
}

/** One ping, only when telemetry is on in the config; never fails a command. */
export async function telemetryPing(ctx, cfg, state, event) {
  if (cfg.telemetry?.enabled !== true || !state?.installId) return false;
  const body = telemetryPayload(cfg, state, event);
  const endpoint = cfg.telemetry.endpoint ?? DEFAULT_TELEMETRY_ENDPOINT;
  if (ctx.runner.dryRun) {
    ctx.log(`  [dry run] telemetry ping to ${endpoint}: ${JSON.stringify(body)}`);
    return true;
  }
  try {
    await (ctx.fetchImpl ?? fetch)(endpoint, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(3000),
    });
  } catch {
    // Telemetry never gets in the way.
  }
  return true;
}

export async function telemetry(ctx) {
  const { flags, runner, log } = ctx;
  const cfg = loadConfig(flags);
  const want = ctx.flags._[1];
  if (!want || want === "status") {
    log(`Telemetry is ${cfg.telemetry?.enabled === true ? "on" : "off"}.`);
    return cfg.telemetry?.enabled === true;
  }
  if (!["on", "off"].includes(want)) throw new Error('Use "telemetry on", "telemetry off" or "telemetry status".');
  cfg.telemetry = { ...(cfg.telemetry ?? {}), enabled: want === "on" };
  if (runner.dryRun) log(`  [dry run] set telemetry.enabled = ${want === "on"} in ${configPath(flags)}`);
  else writeJson(configPath(flags), cfg);
  log(`Telemetry is ${want}.`);
  return want === "on";
}

export { releasePaths };

// ── version / auto-update ────────────────────────────────────────────────

export async function version(ctx) {
  const { flags, log } = ctx;
  const pkg = JSON.parse(fs.readFileSync(new URL("../package.json", import.meta.url), "utf8"));
  log(`Installer   ${pkg.version}`);
  const state = fs.existsSync(configPath(flags)) ? loadState(flags) : {};
  log(`Installed   ${state.version ?? "(not deployed from this folder)"}${state.deployedAt ? ` since ${state.deployedAt.slice(0, 10)}` : ""}`);
  const latest = ctx.runner.dryRun ? null : await latestVersion(ctx.fetchImpl).catch(() => null);
  log(`Latest      ${latest ?? "(could not check)"}`);
  if (latest && state.version && semverCompare(latest, state.version) > 0) log(`Update with: coderhelm-selfhost update --to ${latest}`);
  return { installer: pkg.version, installed: state.version ?? null, latest };
}

/** `"Sun 03:00"` becomes { day, hour }; null when it doesn't read. */
export function parseWindow(w) {
  const m = String(w ?? "").trim().toLowerCase().match(/^(mon|tue|wed|thu|fri|sat|sun|daily)[a-z]*\s+(\d{1,2}):00$/);
  if (!m || Number(m[2]) > 23) return null;
  return { day: m[1], hour: Number(m[2]) };
}

/**
 * auto-update on|off [--window "Sun 03:00"] [--timezone Europe/Berlin]
 * [--channel stable|preview] [--majors ask|auto]: saved in the config (the
 * starting point of a deploy) and applied to the running updater at once.
 */
export async function autoUpdate(ctx) {
  const { flags, runner, log } = ctx;
  const cfg = loadConfig(flags);
  const want = flags._[1];
  if (!["on", "off", "status"].includes(want ?? "status")) throw new Error('Use "auto-update on", "auto-update off" or "auto-update status".');
  const u = { ...(cfg.updates ?? {}) };
  if (!want || want === "status") {
    log(`Auto-update is ${u.auto ? "on" : "off"}: window ${u.window ?? "Sun 03:00"} ${u.timezone ?? "UTC"}, channel ${u.channel ?? "stable"}, majors ${u.majors ?? "ask"}.`);
    return u;
  }
  u.auto = want === "on";
  if (flags.window !== undefined) {
    if (!parseWindow(flags.window)) throw new Error('--window: a day and hour such as "Sun 03:00" or "daily 02:00"');
    u.window = flags.window;
  }
  if (flags.channel !== undefined) {
    if (!["stable", "preview"].includes(flags.channel)) throw new Error("--channel: stable or preview");
    u.channel = flags.channel;
  }
  if (flags.majors !== undefined) {
    if (!["ask", "auto"].includes(flags.majors)) throw new Error("--majors: ask or auto");
    u.majors = flags.majors;
  }
  if (flags.timezone !== undefined) u.timezone = flags.timezone;
  cfg.updates = u;
  if (runner.dryRun) log(`  [dry run] save updates ${JSON.stringify(u)} in ${configPath(flags)}`);
  else writeJson(configPath(flags), cfg);

  // The running install, right away (no redeploy needed).
  const w = parseWindow(u.window ?? "Sun 03:00");
  const settings = { auto: u.auto, channel: u.channel ?? "stable", day: w.day, hour: w.hour, timezone: u.timezone ?? "UTC", majors: u.majors ?? "ask" };
  const payload = JSON.stringify({ action: "settings", settings, actor: "coderhelm-selfhost" });
  const out = await runner.run("aws", [
    "lambda", "invoke", "--function-name", `${derived(cfg).prefix}-updater`, "--region", cfg.region,
    "--cli-binary-format", "raw-in-base64-out", "--payload", payload, "/dev/stdout",
  ], { capture: true, allowFail: true });
  log(out.code === 0 ? `Auto-update is ${want} on the running install.` : "Saved in the config; it applies on the next deploy (the updater could not be reached).");
  return settings;
}
