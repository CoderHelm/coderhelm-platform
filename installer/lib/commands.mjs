// The installer's commands. Each takes a context: the parsed flags, a
// Runner (dry runs print instead of running), a prompter and a logger.
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import os from "node:os";
import { checkPrereqs } from "./prereqs.mjs";
import { checkTag, fetchRelease, latestVersion, releasePaths, releasesDir, TAG_RE, verifiedTemplateSha } from "./release.mjs";
import { openBrowser } from "./github-app.mjs";
import { ensureSecret, hasGithubApp, mergeSecret, readSecret } from "./secrets.mjs";
import { runManifestFlow, secretKeysFromApp } from "./github-app.mjs";
import * as un from "./uninstall.mjs";
import * as mig from "./migrate.mjs";
import { accessOf, DEFAULT_TELEMETRY_ENDPOINT, derived, readJson, semverCompare, writeJson } from "./util.mjs";

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
    integrations: { sandbox: true, logAnalyzer: true, weeklySummary: true },
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
    // The release's model list, read after its signature check (or null).
    models: paths.models ?? null,
    infra: paths.infra,
    dashboard: paths.dashboard,
    env: { GATEWAY_ZIP: paths.gatewayZip, WORKER_ZIP: paths.workerZip, MCP_PROXY_DIR: paths.mcpProxyZip },
  };
}

/**
 * Runs the CDK CLI on the release's app. A hardened install (config
 * `access`) deploys and destroys through its own deploy role and never
 * touches its access stack; anything else runs exactly as it always has.
 * `opts.adminCredentials`: `access harden` deploys once with the admin's
 * own credentials (no role), and its access stack only when asked.
 */
export async function cdk(ctx, src, args, extraEnv = {}, opts = {}) {
  const { runner, flags } = ctx;
  const access = accessOf(safeConfig(flags));
  let cmd = args;
  if (access && ["deploy", "destroy"].includes(args[0]) && !opts.adminCredentials) cmd = [...args, "--role-arn", access.deployRoleArn];
  const env = {
    ...src.env,
    ...(src.dashboard ? { CODERHELM_DASHBOARD_DIR: src.dashboard } : {}),
    CODERHELM_CONFIG: configPath(flags),
    ...(access ? { CODERHELM_ACCESS_STACK: opts.accessStack ? "include" : "skip" } : {}),
    ...extraEnv,
  };
  return runner.run("npx", ["--yes", "cdk", ...cmd], { cwd: src.infra, env });
}

function safeConfig(flags) {
  try {
    return loadConfig(flags);
  } catch {
    return null;
  }
}

/** A release that knows hardened installs (its CDK app has infra/lib/access.ts). */
export function releaseSupportsAccess(src, runner) {
  if (runner?.dryRun || src.version === "source") return true;
  return fs.existsSync(path.join(src.infra, "lib", "access.ts"));
}

const BEFORE_HARDENING = (v) =>
  `${v} is from before hardened installs: it can't deploy into this install's permissions boundary, so going back to it needs an admin. See docs/self-host/security.md, "Rolling back past hardening".`;

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

  // A hardened install never bootstraps: its own bucket and deploy role.
  if (!flags.skipBootstrap && !accessOf(cfg)) {
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
    if (accessOf(loadConfig(flags)) && !releaseSupportsAccess(src, runner)) throw plainError(BEFORE_HARDENING(target));
    // Every model the release calls must answer through the install's
    // provider before anything is deployed (the same check as the updater's).
    if (!flags.rollback) {
      const note = await updateModelCheck(ctx, loadConfig(flags), src, target);
      if (note) result.note = note;
    }
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
          if (accessOf(loadConfig(flags)) && !releaseSupportsAccess(back, runner)) {
            log(BEFORE_HARDENING(result.from));
            return done("failed", `health check failed after deploying ${target}; ${BEFORE_HARDENING(result.from)}`);
          }
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

/**
 * What a release's model check found: blocking failures are models the
 * release calls (or the config names), which nothing can skip; the rest
 * may be skipped with --skip-model-check, as on the dashboard.
 */
export function modelCheckVerdict(results, required, skip = false) {
  const failing = (results ?? []).filter((r) => r.status === "fail");
  const blocking = failing.filter((r) => required.has(r.model));
  const other = failing.filter((r) => !required.has(r.model));
  if (blocking.length) return { ok: false, blocking, other, skippable: false };
  if (other.length && !skip) return { ok: false, blocking, other, skippable: true };
  return { ok: true, blocking, other, skipped: skip ? other.map((r) => r.model) : [] };
}

/** The models to test: the release's, plus the config's own (each with its Bedrock ids). */
export function modelTargets(manifest, cfg) {
  const names = [...(manifest.calls ?? [])];
  for (const m of [cfg.models?.primary ?? "claude-opus-5-5", cfg.models?.light ?? "claude-sonnet-5-5"]) {
    if (!names.includes(m)) names.push(m);
  }
  return names.map((model) => ({ model, bedrock_ids: manifest.bedrock?.[model] ?? {} }));
}

/**
 * The model check before an update from this machine: the worker of the
 * running install makes one test call per model through the install's
 * provider (Settings, System, Models), with its own role or a team's key.
 * A refused model stops the update with its fix and the console link.
 * Nothing here accepts terms or subscribes: a person does that in the
 * AWS console. Returns a note when a check was skipped.
 */
export async function updateModelCheck(ctx, cfg, src, target) {
  const { flags, runner, log } = ctx;
  log("\nThe models this release calls");
  if (src.models === undefined || src.version === "source") {
    log("  skipped: deploying from a checkout, not a signed release");
    return null;
  }
  if (runner.dryRun) {
    log("  [dry run] test each model the release calls through the install's provider");
    return null;
  }
  if (!src.models) {
    log(`  ${target} lists no models to check`);
    return null;
  }
  const d = derived(cfg);
  const stored = await runner.json("aws", [
    "dynamodb", "get-item", "--table-name", `${d.prefix}-settings`, "--region", cfg.region,
    "--key", JSON.stringify({ pk: { S: "SYSTEM" }, sk: { S: "MODELS" } }), "--output", "json",
  ], { capture: true, allowFail: true });
  let probeTeam = null;
  try {
    probeTeam = JSON.parse(stored?.Item?.data?.S ?? "{}").probe_team ?? null;
  } catch {
    probeTeam = null;
  }
  const targets = modelTargets(src.models, cfg);
  const required = new Set(targets.map((t) => t.model));
  const out = path.join(fs.mkdtempSync(path.join(os.tmpdir(), "ch-models-")), "out.json");
  const r = await runner.run("aws", [
    "lambda", "invoke", "--function-name", `${d.prefix}-worker`, "--region", cfg.region,
    "--cli-binary-format", "raw-in-base64-out",
    "--payload", JSON.stringify({ model_check: { targets, probe_team: probeTeam } }), out,
  ], { capture: true, allowFail: true });
  let answer = null;
  try {
    answer = r.code === 0 ? JSON.parse(fs.readFileSync(out, "utf8")) : null;
  } catch {
    answer = null;
  }
  if (!answer || !Array.isArray(answer.results)) {
    throw plainError(`The model check couldn't run (the install's worker, ${d.prefix}-worker, didn't answer), so nothing was changed. Check your AWS credentials for ${cfg.region} and that the install is running, then run the update again.`);
  }
  for (const a of answer.results) {
    if (a.status === "pass") log(`  PASS  ${a.model}`);
    else if (a.status === "skip") log(`  SKIP  ${a.model}: ${a.detail}`);
    else log(`  FAIL  ${a.model}: ${a.detail}\n        How to fix: ${a.fix ?? "see Settings, System, Models"}${a.url ? `\n        ${a.url}` : ""}`);
  }
  const v = modelCheckVerdict(answer.results, required, !!flags.skipModelCheck);
  if (v.blocking.length) {
    throw plainError(`Update to ${target} needs your action: ${v.blocking.map((b) => b.model).join(" and ")} ${v.blocking.length === 1 ? "was" : "were"} refused (fix above). The release calls ${v.blocking.length === 1 ? "it" : "them"}, so this can't be skipped; nothing was changed. Agreeing to model terms and the Bedrock use case form are done by a person in the AWS console. Then run the update again.`);
  }
  if (v.skippable) {
    throw plainError(`Update to ${target} needs your action: ${v.other.map((b) => b.model).join(" and ")} ${v.other.length === 1 ? "was" : "were"} refused (fix above). This release doesn't call ${v.other.length === 1 ? "it" : "them"}, so --skip-model-check lets the update go ahead; nothing was changed.`);
  }
  if (v.skipped?.length) {
    log(`  skipped for ${v.skipped.join(", ")} (--skip-model-check; the release doesn't call ${v.skipped.length === 1 ? "it" : "them"})`);
    return `model check skipped for ${v.skipped.join(", ")}`;
  }
  return null;
}

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

// ── migrate ──────────────────────────────────────────────────────────────

/**
 * `migrate`: an install from before native releases becomes one stack,
 * coderhelm-<stage> (installer/lib/migrate.mjs, docs/self-host/migrate.md).
 * Without --yes it stops once the checked change set is ready.
 */
export async function migrate(ctx) {
  const { flags, log, runner } = ctx;
  const cfg = loadConfig(flags);
  const version = flags.version ?? (await latestVersion(ctx.fetchImpl));
  checkTag(version);
  const templateUrl = `https://coderhelm-public.s3.amazonaws.com/launch/${version}/coderhelm.json`;
  const putObject = async (bucket, key, text, region) => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "ch-migrate-"));
    const file = path.join(dir, "o.json");
    fs.writeFileSync(file, text);
    try {
      await runner.run("aws", ["s3api", "put-object", "--bucket", bucket, "--key", key, "--body", file, "--region", region], { capture: true });
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  };
  log(`Checking ${version}'s signature`);
  const templateSha = await (ctx.verifiedTemplateSha ?? verifiedTemplateSha)(version, { runner, fetchImpl: ctx.fetchImpl, log });
  const r = await mig.migrate({ ...ctx, putObject }, { stage: cfg.stage, region: cfg.region, account: cfg.account, templateUrl, templateSha, cfg: cfg, yes: !!flags.yes });
  if (r.ready) log(`\nThe update is prepared and checked (change set ${r.ready}). Review it in CloudFormation, then run migrate again with --yes: it carries on from here.`);
  return 0;
}

// ── leftovers ────────────────────────────────────────────────────────────

/**
 * `leftovers`: after Delete stack, anything still named or tagged for the
 * install in this region (read-only): what KeepDataOnDelete kept, what an
 * earlier failed install left (two user pools of the same name are both
 * listed), the auto-update role. `--delete` removes them, one by one, after
 * the typed name, with every reason printed; never while the install's
 * stack exists.
 */
export async function leftovers(ctx) {
  const flags = ctx.flags;
  if (!flags.delete) return uninstall({ ...ctx, flags: { ...flags, verify: true } });
  return uninstall({ ...ctx, flags: { ...flags, everything: true, verify: false } });
}

// ── uninstall ────────────────────────────────────────────────────────────

/**
 * `uninstall`: removes CoderHelm and keeps its data (tables, data bucket,
 * KMS keys, user pool, secret). `--everything` (or `--delete-data`) deletes
 * those too; KMS keys after AWS's 7-day minimum. `--verify` only lists
 * what is left. `--guided` is the uninstall stack's build
 * (installer/lib/uninstall.mjs, docs/self-host/uninstall.md).
 */
export async function uninstall(ctx) {
  const { flags, log } = ctx;
  if (flags.guided) throw new Error("The guided uninstall is now Delete stack in CloudFormation (docs.coderhelm.com/self-host/uninstall/).");
  let cfg = null;
  try {
    cfg = loadConfig(flags);
  } catch {
    cfg = null;
  }
  const stage = typeof flags.stage === "string" ? flags.stage : cfg?.stage;
  const region = typeof flags.region === "string" ? flags.region : cfg?.region;
  if (!stage || !region) throw new Error("Pass --stage <install name> and --region, or run this where the install's coderhelm.config.json is.");
  if (cfg && cfg.stage === stage && derived(cfg).prefix !== `coderhelm-${stage}`) {
    throw new Error(`This install names its resources ${derived(cfg).prefix}-*, not coderhelm-${stage}-*: remove its stacks in CloudFormation.`);
  }
  const mode = flags.everything || flags.deleteData ? "everything" : "keep";
  ctx.aws ??= un.awsClient(ctx, region);
  const account = cfg?.account ?? (await ctx.aws.read("sts", "get-caller-identity", []))?.Account;
  const scope = un.scopeOf({ stage, region, account });
  const inv = await un.discover(ctx, scope, { mode, installStatus: typeof flags.installStatus === "string" ? flags.installStatus : "", fetchImpl: ctx.fetchImpl });
  // A native install is one stack: Delete stack removes it; this never takes it apart by name.
  const parent = inv.stacks.find((x) => x.name === scope.prefix);
  if (parent && !flags.verify) {
    throw new Error(`${scope.prefix} is a CloudFormation stack: delete it in CloudFormation (Delete stack). "leftovers" then lists, and with --delete removes, anything it left.`);
  }

  if (flags.verify) {
    const { left, scheduled } = un.leftovers(inv);
    log(`\nCoderHelm ${stage} in ${region} (${account}): ${left.length ? `${left.length} piece${left.length === 1 ? "" : "s"} left` : "nothing left"}.`);
    for (const l of left) log(`  ${l.kind.padEnd(22)} ${l.name}`);
    for (const k of scheduled) log(`  KMS key ${k.name}: scheduled for deletion${k.date ? ` on ${k.date}` : ""} (AWS's 7-day minimum)`);
    for (const x of un.plan(inv).notRemoved) log(`  not removed: ${x.kind} ${x.name}, ${x.why}`);
    for (const x of inv.skipped) log(`  not this install's: ${x.kind} ${x.name} (${x.why})`);
    return left.length ? 1 : 0;
  }

  const p = un.plan(inv);
  if (!inv.anchored || (!p.remove.length && !p.keep.length)) {
    log(`Nothing named or tagged for the install ${stage} is in ${region} (${account}): no Launch Stack, no resource tagged for it and no secret.`);
    for (const x of inv.skipped) log(`  not this install's: ${x.kind} ${x.name} (${x.why})`);
    return 0;
  }
  log(`\nThis removes CoderHelm ${stage} from ${account} in ${region}:`);
  logByKind(log, p.remove);
  if (p.keep.length) {
    log("\nKept (your data; uninstall --everything deletes it too):");
    logByKind(log, p.keep);
  } else if (mode === "everything") {
    log(`\nNothing can be recovered afterwards. KMS keys are scheduled for deletion in ${un.KEY_DELETION_DAYS} days (AWS's minimum); until then they can be cancelled.`);
  }
  for (const x of p.notRemoved) log(`\nNot removed: ${x.kind} ${x.name}, ${x.why}.`);
  if (inv.scope.siblings.length) log(`\nNot touched: the other installs ${inv.scope.siblings.join(", ")} (their names start with this one's).`);
  if (ctx.runner.dryRun) {
    log("\nDry run: nothing was deleted.");
    return 0;
  }
  if (!flags.yes) {
    if (!ctx.interactive) throw new Error("Pass --yes to uninstall without a prompt.");
    const typed = await ctx.ask.ask(`\nType the install name (${stage}) to ${mode === "everything" ? "delete CoderHelm and all its data" : "remove CoderHelm"}`, "");
    if (typed !== stage) throw new Error("Not uninstalled: the name didn't match.");
  }
  if (cfg) await telemetryPing(ctx, cfg, loadState(flags), "uninstall");
  let last = "";
  const doc = await un.runUninstall(ctx, {
    stage, region, account, mode,
    installStatus: typeof flags.installStatus === "string" ? flags.installStatus : "",
    hostedUrl: cfg?.hybridUrl || undefined,
    write: async (d) => {
      if (d.message && d.message !== last) log(`  ${d.message}`);
      last = d.message;
    },
  });
  log("");
  for (const k of doc.scheduled) log(`KMS key ${k.name}: scheduled for deletion${k.date ? ` on ${k.date}` : ""}.`);
  if (doc.kept.length) log(`Kept on purpose: ${doc.kept.length} piece${doc.kept.length === 1 ? "" : "s"} of data (tables, data bucket, KMS keys, user pool, secret).`);
  if (doc.hybrid?.registered) log(doc.hybrid.released ? "Released the CoderHelm address; GitHub and Jira events go back to CoderHelm." : "CoderHelm couldn't be reached to release the address; it is released when the install stops answering.");
  if (doc.phase !== "done") {
    const e = new Error(`${doc.message}\n${[...doc.problems, ...doc.left].map((x) => `  ${x.kind} ${x.name}${x.error ? `: ${x.error}` : ""}`).join("\n")}\n${doc.fix ?? ""}`);
    e.plain = true;
    throw e;
  }
  log(doc.message);
  return 0;
}

function logByKind(log, items) {
  const by = new Map();
  for (const x of items) by.set(x.kind, [...(by.get(x.kind) ?? []), x.name]);
  for (const [kind, names] of by) log(`  ${String(names.length).padStart(3)} ${kind}${names.length === 1 ? "" : "s"}: ${names.slice(0, 6).join(", ")}${names.length > 6 ? ", ..." : ""}`);
}

// ── access (hardened installs) ───────────────────────────────────────────

/**
 * `access status`: how this install deploys.
 * `access harden`: moves an install that deploys through the CDK bootstrap
 * roles onto its own permissions boundary and deploy role (docs/self-host/
 * security.md). Run once, by an admin, from a machine with admin
 * credentials for the account, on a release that has it:
 *   1. deploys `<prefix>-access` (boundary, deploy role, assets bucket);
 *   2. deploys every stack once with the admin's credentials: names, the
 *      boundary on every role, the install's tag, the update build's
 *      window;
 *   3. deploys every stack again through the deploy role, which from then
 *      on is the only role CloudFormation uses for them;
 *   4. takes AdministratorAccess off the Launch Stack installer role this
 *      install was made with, if there is one (asks first without --yes).
 * Updates from then on get deploy rights only while they run.
 */
export async function access(ctx) {
  const { flags, runner, log } = ctx;
  const sub = flags._[1] ?? "status";
  const cfg = loadConfig(flags);
  const a = accessOf(cfg);
  if (sub === "status") {
    if (!a) log("This install deploys through the CDK bootstrap roles (not hardened). An admin can run: coderhelm-selfhost access harden");
    else log(`Hardened (${a.owner === "launch" ? "made by the Launch Stack" : "hardened from the CLI"}).\n  Boundary:    ${a.boundaryArn}\n  Deploy role: ${a.deployRoleArn}\n  Assets:      s3://${a.assetsBucket}`);
    return a;
  }
  if (sub !== "harden") throw new Error('Use "access status" or "access harden".');
  if (a?.owner === "launch") {
    log("Hardened by the Launch Stack: nothing to do.");
    return a;
  }
  // Hardened from the CLI already: running it again finishes or repeats the steps.
  const state = loadState(flags);
  const src = await resolveSource(ctx, flags.version ?? state.version);
  if (!releaseSupportsAccess(src, runner)) throw plainError(`${src.version} has no hardened installs: update to a newer release first.`);
  if (!flags.yes) {
    if (!ctx.interactive) throw new Error("Pass --yes to harden without a prompt.");
    if (!(await ctx.ask.confirm(`Harden ${cfg.stage} in ${cfg.account}? Updates will deploy only through the install's own deploy role.`, true))) return null;
  }
  const next = { ...cfg, access: { mode: "hardened", owner: "app" } };
  if (!runner.dryRun) writeJson(configPath(flags), next);
  const hardened = accessOf(next);
  if (runner.dryRun || !fs.existsSync(path.join(src.infra, "node_modules"))) {
    await runner.run("npm", ["ci", "--no-audit", "--no-fund"], { cwd: src.infra });
  }
  try {
    log(`1/3 The boundary, deploy role and assets bucket (${hardened.accessStackName})`);
    await cdk(ctx, src, ["deploy", hardened.accessStackName, "--exclusively", "--require-approval", "never"], {}, { adminCredentials: true, accessStack: true });
  } catch (e) {
    if (!runner.dryRun && !a) writeJson(configPath(flags), cfg);
    throw plainError(`Hardening stopped before changing CoderHelm's stacks: ${String(e.message ?? e).split("\n")[0]}\nThe install keeps deploying as before. Fix what failed and run "access harden" again.`);
  }
  // From here the stacks change: the account's copy of the config says
  // hardened too, so an update never deploys them the old way again.
  await saveRemote(ctx, next, { ...state });
  try {
    const v = src.version && src.version !== "source" ? { CODERHELM_VERSION: src.version } : {};
    log("2/3 Every stack, once with your credentials (names, boundary, tags)");
    await cdk(ctx, src, ["deploy", "--all", "--require-approval", "never", "--progress", "events"], v, { adminCredentials: true });
    log("3/3 Every stack through the deploy role, which CloudFormation uses from now on");
    await cdk(ctx, src, ["deploy", "--all", "--require-approval", "never", "--force"], v);
  } catch (e) {
    throw plainError(`Hardening stopped part way: ${String(e.message ?? e).split("\n")[0]}\nRun "access harden" again (with admin credentials) to finish; updates wait for it.`);
  }
  await retireLaunchAdmin(ctx, next);
  log("\nHardened. Check it with: selfhost/check-access.sh (docs/self-host/security.md).");
  return hardened;
}

/** The Launch Stack installer role an older template gave AdministratorAccess: found, and (asked) detached. */
export async function retireLaunchAdmin(ctx, cfg) {
  const { runner, log, flags } = ctx;
  const admin = "arn:aws:iam::aws:policy/AdministratorAccess";
  const names = (await runner.json("aws", ["codebuild", "list-projects", "--region", cfg.region, "--output", "json"], { allowFail: true, dryRunAnswer: '{"projects":[]}' }))?.projects ?? [];
  const candidates = names.filter((n) => n.endsWith("-installer"));
  if (!candidates.length) return null;
  const got = await runner.json("aws", ["codebuild", "batch-get-projects", "--names", ...candidates, "--region", cfg.region, "--output", "json"], { allowFail: true });
  for (const p of got?.projects ?? []) {
    const env = Object.fromEntries((p.environment?.environmentVariables ?? []).map((v) => [v.name, v.value]));
    if (env.STAGE !== cfg.stage || !String(p.source?.buildspec ?? "").includes("launch-bootstrap.mjs")) continue;
    const role = String(p.serviceRole ?? "").split("/").pop();
    const attached = await runner.json("aws", ["iam", "list-attached-role-policies", "--role-name", role, "--output", "json"], { allowFail: true });
    if (!(attached?.AttachedPolicies ?? []).some((x) => x.PolicyArn === admin)) continue;
    log(`The Launch Stack's installer role ${role} (project ${p.name}) still has AdministratorAccess.`);
    if (!flags.yes && !(ctx.interactive && (await ctx.ask.confirm("Take it off? (the old installer can't run again; updates don't use it)", true)))) {
      log(`Left as it is. To take it off later: aws iam detach-role-policy --role-name ${role} --policy-arn ${admin}`);
      return role;
    }
    await runner.run("aws", ["iam", "detach-role-policy", "--role-name", role, "--policy-arn", admin], { capture: true });
    log(`Took AdministratorAccess off ${role}.`);
    return role;
  }
  return null;
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
