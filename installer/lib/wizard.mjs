// The guided install: `coderhelm-selfhost init` (and the bare
// `npx github:CoderHelm/coderhelm-platform`). One step at a time, each with a
// plain explanation, a suggested answer, a check of the answer, a progress
// bar, and "back" to return to the previous step. Answers are saved after
// every step (.coderhelm/wizard.json, no secrets), so running it again picks
// up where it stopped. Without a terminal (CI), every answer comes from flags
// or --answers <file>, and a bad one stops with the reason.
//
// Every step checks what it was told before moving on (lib/checks.mjs):
// sign-in and permissions, the region, the domain's DNS, the certificate,
// Bedrock model access or the Anthropic key (a real one-token call), email.
// A failed check says what is wrong, why it matters and how to fix it, and
// waits: press Enter to check again, "skip" to go on, "back" to change an
// answer (lib/gate.mjs). After the deploy, the doctor runs every check and
// only then says the install is ready.
import fs from "node:fs";
import path from "node:path";
import { checkPrereqs } from "./prereqs.mjs";
import * as C from "./checks.mjs";
import { BACK, gate } from "./gate.mjs";
import { runDoctor, readyMessage } from "./doctor.mjs";
import { ensureSecret, mergeSecret } from "./secrets.mjs";
import { configPath, deploy, statePath } from "./commands.mjs";
import { readJson, writeJson } from "./util.mjs";

export { BACK };

export const STEPS = [
  { id: "prereqs", title: "Your machine and AWS sign-in" },
  { id: "account", title: "Where to install" },
  { id: "domain", title: "Web address" },
  { id: "model", title: "Claude access" },
  { id: "email", title: "You and email" },
  { id: "integrations", title: "Optional extras" },
  { id: "updates", title: "Updates" },
  { id: "summary", title: "Summary and cost" },
];

const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const HOST_RE = /^(?=.{1,253}$)([a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}$/;

export function wizardFile(flags) {
  return path.join(path.dirname(statePath(flags)), "wizard.json");
}

/** `Step 3 of 8 [███░░░░░] Web address` */
export function progress(i, total, title) {
  const width = 16;
  const filled = Math.round(((i + 1) / total) * width);
  return `\nStep ${i + 1} of ${total} [${"█".repeat(filled)}${"░".repeat(width - filled)}] ${title}`;
}

/** Asks until the answer passes `check` (or "back"); without a terminal, a bad answer stops. */
async function askValid(ctx, question, def, check = () => null) {
  for (let tries = 0; tries < 6; tries++) {
    const raw = await ctx.ask.ask(question, def);
    const a = typeof raw === "string" ? raw.trim() : raw;
    if (ctx.interactive && (a === "back" || a === "<")) return BACK;
    const err = check(a);
    if (!err) return a;
    if (!ctx.interactive) throw new Error(`${question}: ${err}`);
    ctx.log(`  ${err}`);
  }
  throw new Error(`${question}: no valid answer`);
}

const pick = (flags, answers, key) => (flags[key] !== undefined ? flags[key] : answers[key]);

/** A given answer (flag, file, saved) is checked like a typed one; asks when there is none. */
async function answer(ctx, provided, question, def, check = () => null) {
  if (provided !== undefined && provided !== true) {
    const v = typeof provided === "string" ? provided.trim() : provided;
    const err = check(v);
    if (!err) return v;
    if (!ctx.interactive) throw new Error(`${question}: ${err}`);
    ctx.log(`  ${err}`);
  }
  return askValid(ctx, question, def, check);
}

async function yesNo(ctx, question, def) {
  const a = await askValid(ctx, `${question} (yes/no)`, def ? "yes" : "no", (v) => (/^(y|yes|n|no)$/i.test(v) ? null : "Answer yes or no."));
  return a === BACK ? BACK : /^y/i.test(a);
}

// ── Steps ─────────────────────────────────────────────────────────────────

async function stepPrereqs(ctx, answers) {
  const { runner, log, flags } = ctx;
  log("CoderHelm deploys with the AWS CLI and Node.js from this machine. Checking them.");
  const tools = await gate(ctx, async () => [C.checkNode(ctx.nodeVersion), await C.checkAwsCli(ctx), await C.checkTar(ctx)], { heading: "Checking this machine" });
  if (tools === BACK) return BACK;
  let { identity } = await checkPrereqs(runner, {});
  if (!identity) {
    const profiles = ((await runner.run("aws", ["configure", "list-profiles"], { capture: true, allowFail: true })).stdout || "")
      .split("\n").map((s) => s.trim()).filter(Boolean);
    log(profiles.length
      ? `You're not signed in to AWS. Profiles on this machine: ${profiles.join(", ")}`
      : "You're not signed in to AWS and have no profiles. Run `aws configure sso` (or `aws configure`), then run this again.");
    if (!profiles.length) throw new Error("No AWS credentials.");
    const profile = flags.profile ?? (await askValid(ctx, "AWS profile to use", profiles[0], (p) => (profiles.includes(p) ? null : `One of: ${profiles.join(", ")}`)));
    if (profile === BACK) return BACK;
    process.env.AWS_PROFILE = profile;
    if (runner.env) runner.env.AWS_PROFILE = profile;
    const sso = (await runner.run("aws", ["configure", "get", "sso_session", "--profile", profile], { capture: true, allowFail: true })).stdout?.trim()
      || (await runner.run("aws", ["configure", "get", "sso_start_url", "--profile", profile], { capture: true, allowFail: true })).stdout?.trim();
    if (sso && ctx.interactive && (await yesNo(ctx, `Sign in now with \`aws sso login --profile ${profile}\`?`, true)) === true) {
      await runner.run("aws", ["sso", "login", "--profile", profile]);
    }
    ({ identity } = await checkPrereqs(runner, {}));
    if (!identity) {
      // Wait here until the sign-in works (what's wrong and the command to fix it).
      const signed = await gate(ctx, async () => {
        const r = await C.checkSignIn(ctx, { profile });
        identity = r.identity;
        return [r.result];
      }, { heading: "Checking the AWS sign-in" });
      if (signed === BACK) return BACK;
      if (!identity) throw new Error(`Still no AWS credentials for profile ${profile}. Sign in and run this again.`);
    }
    answers.profile = profile;
  }
  log(`  Signed in to account ${identity.Account} as ${identity.Arn}`);
  return { ...answers, detectedAccount: identity.Account, detectedArn: identity.Arn };
}

async function stepAccount(ctx, answers) {
  const { flags, runner, log } = ctx;
  log("CoderHelm installs into one AWS account and region. Every resource is named coderhelm-<name>-..., so two installs can share an account.");
  const configured = (await runner.run("aws", ["configure", "get", "region"], { capture: true, allowFail: true })).stdout?.trim();
  const account = await answer(ctx, pick(flags, answers, "account"), "AWS account", answers.detectedAccount ?? "", (v) => (/^\d{12}$/.test(v) ? null : "A 12-digit account id."));
  if (account === BACK) return BACK;
  if (answers.detectedAccount && account !== answers.detectedAccount) {
    const msg = `You're signed in to ${answers.detectedAccount}, not ${account}. Switch profiles first.`;
    if (!ctx.interactive) throw new Error(msg);
    log(`  ${msg}`);
    return stepAccount(ctx, answers);
  }
  const region = await answer(ctx, pick(flags, answers, "region"), "AWS region", process.env.AWS_REGION || configured || "us-east-1", (v) => (/^[a-z]{2}(-gov)?-[a-z]+-\d$/.test(v) ? null : "A region such as us-east-1."));
  if (region === BACK) return BACK;
  const stage = await answer(ctx, pick(flags, answers, "stage"), "A short name for this install", "prod", (v) => (/^[a-z][a-z0-9-]{0,19}$/.test(v) ? null : "Lowercase letters, digits and hyphens, up to 20."));
  if (stage === BACK) return BACK;
  const identity = { Account: answers.detectedAccount ?? account, Arn: answers.detectedArn ?? "" };
  const checked = await gate(ctx, async () => [
    await C.checkRegion(ctx, region),
    identity.Arn ? await C.checkPermissions(ctx, identity) : null,
    await C.checkBootstrap(ctx, account, region, { missing: C.WARN }),
  ], { heading: `Checking ${account} / ${region}` });
  if (checked === BACK) return BACK;
  return { ...answers, account, region, stage };
}

async function stepDomain(ctx, answers) {
  const { flags, runner, log } = ctx;
  log("CoderHelm needs one web address. Without a domain it uses CloudFront's own (https://dxxxx.cloudfront.net), and everything works except Google sign-in.");
  const zones = ((await runner.json("aws", ["route53", "list-hosted-zones", "--output", "json"], { allowFail: true, dryRunAnswer: '{"HostedZones":[]}' }))?.HostedZones ?? [])
    .filter((z) => !z.Config?.PrivateZone)
    .map((z) => z.Name.replace(/\.$/, ""));
  if (zones.length) log(`  Route 53 zones in this account: ${zones.join(", ")}`);
  const hostname = await answer(ctx, pick(flags, answers, "domain"), "Hostname (empty for the CloudFront address)", zones.length && ctx.interactive ? `coderhelm.${zones[0]}` : "",
    (v) => (!v || HOST_RE.test(v) ? null : "A lowercase hostname such as coderhelm.example.com, or empty."));
  if (hostname === BACK) return BACK;
  if (!hostname) return { ...answers, domain: "", hostedZone: undefined, certificateArn: undefined };
  const zone = zones.find((z) => hostname === z || hostname.endsWith(`.${z}`));
  if (zone) {
    log(`  ${zone} is in Route 53: the certificate and DNS records are created for you.`);
    const checked = await gate(ctx, async () => {
      const z = await C.checkHostedZone(ctx, zone);
      return [z.result, await C.checkDelegation(ctx, zone, z.zoneId), ...(await usEast1Bootstrap(ctx, answers, false))];
    }, { heading: `Checking ${zone}'s DNS` });
    if (checked === BACK) return BACK;
    return { ...answers, domain: hostname, hostedZone: zone, certificateArn: undefined };
  }
  log("  That hostname isn't in a Route 53 zone here. Create an ACM certificate for it in us-east-1 (CloudFront only uses us-east-1), and you'll add one CNAME after the deploy.");
  const certificateArn = await answer(ctx, pick(flags, answers, "certificateArn"), "ACM certificate ARN (us-east-1)", "",
    (v) => (/^arn:aws[a-z-]*:acm:us-east-1:\d{12}:certificate\/[0-9a-f-]+$/.test(v) ? null : "An ACM certificate ARN in us-east-1."));
  if (certificateArn === BACK) return BACK;
  const checked = await gate(ctx, async () => [await C.checkCertificate(ctx, certificateArn, hostname)], { heading: "Checking the certificate" });
  if (checked === BACK) return BACK;
  return { ...answers, domain: hostname, hostedZone: undefined, certificateArn };
}

/** CloudFront's certificate is made in us-east-1: CDK must be set up there too. */
async function usEast1Bootstrap(ctx, answers, hasCert) {
  if (hasCert || answers.region === "us-east-1") return [];
  return [await C.checkBootstrap(ctx, answers.account, "us-east-1", { missing: C.WARN })];
}

export function bedrockPrefixFor(region) {
  if (region.startsWith("us-")) return "us.";
  if (region.startsWith("eu-")) return "eu.";
  if (region.startsWith("ap-")) return "apac.";
  return "";
}

async function stepModel(ctx, answers) {
  const { flags, runner, log } = ctx;
  log("CoderHelm uses Claude. Either each team adds its own Anthropic API key, or the install calls Claude on Amazon Bedrock in this AWS account (no keys, billed by AWS). You can change this later in Settings, System.");
  const provider = await answer(ctx, pick(flags, answers, "provider"), "Claude through anthropic or bedrock", "anthropic", (v) => (["anthropic", "bedrock"].includes(v) ? null : "anthropic or bedrock"));
  if (provider === BACK) return BACK;
  if (provider === "anthropic") {
    const key = flags.anthropicKey ?? process.env.ANTHROPIC_API_KEY ?? (ctx.interactive ? await askValid(ctx, "An Anthropic API key to check now (empty to skip; it is not saved)", "") : "");
    if (key === BACK) return BACK;
    if (key) {
      const r = await C.checkAnthropicKey(ctx, key);
      log(C.formatResults([r]));
      if (r.status === C.FAIL) {
        const msg = "Anthropic refused that key.";
        if (!ctx.interactive) throw new Error(`${msg}\n${C.formatResults([r])}`);
        log(`  ${msg} Try again, or leave it empty.`);
        return stepModel(ctx, answers);
      }
      log(r.status === C.PASS ? "  The key works. Add it in Settings, AI models after you sign up (keys belong to teams, not the install)." : "  You can add the key later in Settings, AI models.");
    }
    return { ...answers, provider, bedrockPrefix: undefined };
  }
  const bedrockPrefix = await answer(ctx, pick(flags, answers, "bedrockPrefix"), "Bedrock inference profile prefix", bedrockPrefixFor(answers.region), (v) => (/^([a-z]{2,6}\.)?$/.test(v) ? null : 'Such as "us.", "eu.", "apac." or empty.'));
  if (bedrockPrefix === BACK) return BACK;
  const checked = await gate(ctx, async () => [await C.checkRegion(ctx, answers.region, { bedrock: true }), await C.checkBedrock(ctx, answers.region)], { heading: "Checking Bedrock in this region" });
  if (checked === BACK) return BACK;
  return { ...answers, provider, bedrockPrefix };
}

/** true: key works; false: refused; null: couldn't check (a real one-token call). */
export async function checkAnthropicKey(ctx, key) {
  const r = await C.checkAnthropicKey(ctx, key);
  return r.status === C.PASS ? true : r.status === C.FAIL ? false : null;
}

/** Whether each model is usable in the region (Bedrock model access). */
export async function checkBedrockAccess(runner, region, models) {
  const out = {};
  for (const m of models) {
    const a = await runner.json("aws", ["bedrock", "get-foundation-model-availability", "--model-id", m, "--region", region, "--output", "json"], {
      allowFail: true,
      dryRunAnswer: JSON.stringify({ authorizationStatus: "AUTHORIZED", agreementAvailability: { status: "AVAILABLE" } }),
    });
    out[m] = !!a && a.authorizationStatus === "AUTHORIZED" && (a.agreementAvailability?.status ?? "AVAILABLE") === "AVAILABLE";
  }
  return out;
}

async function stepEmail(ctx, answers) {
  const { flags, runner, log } = ctx;
  log("You become the first admin. CoderHelm emails run results, invites and update notices through Amazon SES in this account.");
  const admin = await answer(ctx, pick(flags, answers, "admin"), "Your email", "", (v) => (EMAIL_RE.test(v) ? null : "An email address."));
  if (admin === BACK) return BACK;
  const defDomain = answers.hostedZone ?? admin.split("@")[1];
  const emailFrom = await answer(ctx, pick(flags, answers, "emailFrom"), "Emails come from", `coderhelm@${defDomain}`, (v) => (EMAIL_RE.test(v) ? null : "An email address."));
  if (emailFrom === BACK) return BACK;
  let sandbox;
  const checked = await gate(ctx, async () => {
    const results = [await C.checkSesIdentity(ctx, emailFrom, answers.region, { zone: answers.hostedZone }), await C.checkSesSandbox(ctx, answers.region)];
    sandbox = results[1].status !== C.PASS;
    return results;
  }, { heading: "Checking Amazon SES" });
  if (checked === BACK) return BACK;
  const acct = runner.dryRun ? null : { ProductionAccessEnabled: !sandbox };
  const signupDomain = await answer(ctx, pick(flags, answers, "signupDomain"), "Let anyone with this email domain sign up (empty: only you)", admin.split("@")[1], (v) => (!v || HOST_RE.test(v) ? null : "A domain such as example.com, or empty."));
  if (signupDomain === BACK) return BACK;
  return { ...answers, admin, emailFrom, signupDomain, sesSandbox: acct ? !acct.ProductionAccessEnabled : undefined };
}

async function stepIntegrations(ctx, answers) {
  const { flags, log, runner } = ctx;
  log("Optional. Each can be added later too.");
  log("  Jira: connect from Settings, Integrations, Jira after sign-up (a webhook, or your own copy of the Forge app).");
  const webhook = await answer(ctx, pick(flags, answers, "updatesWebhook"), "A Slack or Teams incoming webhook for update notices (empty: email only)", "", (v) => (!v || /^https:\/\/\S+$/.test(v) ? null : "An https URL, or empty."));
  if (webhook === BACK) return BACK;
  if (webhook) {
    const checked = await gate(ctx, async () => [await C.checkWebhook(ctx, webhook)], { heading: "Checking the webhook" });
    if (checked === BACK) return BACK;
  }
  let google = flags.google !== undefined ? flags.google === true || flags.google === "yes" : answers.google ?? false;
  if (flags.google === undefined && answers.domain) {
    const g = await yesNo(ctx, "Turn on Sign in with Google (needs a Google OAuth client)?", google);
    if (g === BACK) return BACK;
    google = g;
  } else if (!answers.domain) {
    google = false;
  }
  if (google) {
    const id = flags.googleClientId ?? (await askValid(ctx, "Google OAuth client id", "", (v) => (v.endsWith(".apps.googleusercontent.com") ? null : "It ends in .apps.googleusercontent.com.")));
    if (id === BACK) return BACK;
    const secret = flags.googleClientSecret ?? process.env.GOOGLE_CLIENT_SECRET ?? (await askValid(ctx, "Google OAuth client secret", "", (v) => (v ? null : "Required for Google sign-in.")));
    if (secret === BACK) return BACK;
    // Straight into the install's secret: never in the config or the wizard file.
    const name = `coderhelm/${answers.stage}/secrets`;
    await ensureSecret(runner, name, answers.region, log);
    await mergeSecret(runner, name, answers.region, { google_client_id: id, google_client_secret: secret });
    log("  Stored in Secrets Manager.");
  }
  return { ...answers, updatesWebhook: webhook || undefined, google };
}

async function stepUpdates(ctx, answers) {
  const { flags, log } = ctx;
  log("CoderHelm checks for new releases every hour and emails admins. With auto-update on, it also installs them in a maintenance window: health check, signature check, deploy, health check, and an automatic rollback if the new release is unhealthy.");
  let auto = flags.autoUpdate !== undefined ? flags.autoUpdate === true || flags.autoUpdate === "on" || flags.autoUpdate === "yes" : answers.auto;
  if (auto === undefined) {
    const a = await yesNo(ctx, "Install updates automatically?", false);
    if (a === BACK) return BACK;
    auto = a;
  }
  let window = pick(flags, answers, "window");
  let timezone = pick(flags, answers, "timezone");
  let majors = pick(flags, answers, "majors");
  if (auto) {
    window ??= await askValid(ctx, 'Maintenance window ("Sun 03:00", "daily 02:00")', "Sun 03:00", (v) => (/^(mon|tue|wed|thu|fri|sat|sun|daily)[a-z]*\s+\d{1,2}:00$/i.test(v) && Number(v.split(/\s+/)[1].split(":")[0]) <= 23 ? null : 'A day and hour such as "Sun 03:00".'));
    if (window === BACK) return BACK;
    timezone ??= await askValid(ctx, "Time zone of the window", Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC", (v) => (/^(UTC|[A-Za-z_]+(\/[A-Za-z0-9_+-]+)+)$/.test(v) ? null : "An IANA time zone such as Europe/Berlin."));
    if (timezone === BACK) return BACK;
    majors ??= await askValid(ctx, "Major releases: ask (wait for an admin) or auto", "ask", (v) => (["ask", "auto"].includes(v) ? null : "ask or auto"));
    if (majors === BACK) return BACK;
  }
  return { ...answers, auto: !!auto, window, timezone, majors };
}

/** A rough monthly AWS bill for a small team, without model usage. */
export function estimateMonthlyCost(answers) {
  const lines = [
    ["Lambda, API Gateway, SQS, DynamoDB, EventBridge", 2, 15],
    ["CloudFront and the dashboard bucket", 1, 5],
    ["KMS keys (4)", 4, 4],
    ["Secrets Manager, SSM", 0.4, 1],
    ["CloudWatch Logs", 1, 10],
    ["Build sandbox (CodeBuild, by the minute)", 3, 25],
    ["Updates (hourly check, a few builds)", 0, 1],
    ["Email (SES)", 0, 1],
  ];
  const low = lines.reduce((a, l) => a + l[1], 0);
  const high = lines.reduce((a, l) => a + l[2], 0);
  const model = answers.provider === "bedrock" ? "Claude on Bedrock: per token, on this AWS bill" : "Claude: per token, on each team's Anthropic bill";
  return { lines, low: Math.round(low), high: Math.round(high), model };
}

async function stepSummary(ctx, answers) {
  const { log, flags } = ctx;
  const cfg = configFromAnswers(answers);
  log("Here is what will be installed:");
  log(`  Account / region   ${cfg.account} / ${cfg.region} (install "${cfg.stage}")`);
  log(`  Address            ${cfg.domain ? `https://${cfg.domain.hostname}` : "CloudFront's own address"}`);
  log(`  Claude             ${cfg.models.provider === "bedrock" ? `Amazon Bedrock (${cfg.models.bedrockInferencePrefix || "in-region"})` : "Anthropic API, each team's key"}`);
  log(`  Admin / sender     ${cfg.admins[0]} / ${cfg.email.from}`);
  log(`  Sign-up            ${cfg.signup.domains?.length ? `anyone at ${cfg.signup.domains[0]}` : "only you (invite others later)"}`);
  log(`  Google sign-in     ${cfg.integrations.googleSignIn ? "on" : "off"}`);
  log(`  Auto-update        ${cfg.updates.auto ? `on, ${cfg.updates.window} ${cfg.updates.timezone}, majors: ${cfg.updates.majors}` : "off (you're emailed about new releases)"}`);
  const c = estimateMonthlyCost(answers);
  log("\nEstimated AWS cost per month for a small team (USD):");
  for (const [item, lo, hi] of c.lines) log(`  ${item.padEnd(50)} ${lo === hi ? `${lo}` : `${lo} to ${hi}`}`);
  log(`  ${"Total".padEnd(50)} about ${c.low} to ${c.high}`);
  log(`  Not included: ${c.model}.`);
  if (flags.deploy !== undefined) return { ...answers, deploy: flags.deploy !== "no" && flags.deploy !== false };
  if (!ctx.interactive) return { ...answers, deploy: false };
  const go = await yesNo(ctx, "Save this and deploy now? (\"back\" to change something)", true);
  if (go === BACK) return BACK;
  return { ...answers, deploy: go };
}

const RUN = {
  prereqs: stepPrereqs,
  account: stepAccount,
  domain: stepDomain,
  model: stepModel,
  email: stepEmail,
  integrations: stepIntegrations,
  updates: stepUpdates,
  summary: stepSummary,
};

/** The config file from the wizard's answers. */
export function configFromAnswers(a) {
  const updates = { auto: !!a.auto };
  if (a.auto) Object.assign(updates, { window: a.window ?? "Sun 03:00", timezone: a.timezone ?? "UTC", majors: a.majors ?? "ask" });
  if (a.updatesWebhook) updates.webhookUrl = a.updatesWebhook;
  return {
    account: a.account,
    region: a.region,
    stage: a.stage,
    domain: a.domain
      ? { hostname: a.domain, ...(a.hostedZone ? { hostedZone: a.hostedZone } : {}), ...(a.certificateArn ? { certificateArn: a.certificateArn } : {}) }
      : null,
    models: { provider: a.provider, ...(a.provider === "bedrock" && a.bedrockPrefix !== undefined ? { bedrockInferencePrefix: a.bedrockPrefix } : {}) },
    email: { from: a.emailFrom },
    admins: [a.admin],
    signup: a.signupDomain ? { domains: [a.signupDomain] } : {},
    integrations: { googleSignIn: !!a.google, sandbox: true, logAnalyzer: true, weeklySummary: true },
    updates,
    telemetry: { enabled: a.telemetry === true },
  };
}

/** Runs the wizard; returns the config it saved. */
export async function wizard(ctx) {
  const { flags, runner, log } = ctx;
  const file = configPath(flags);
  const wfile = wizardFile(flags);
  const finished = fs.existsSync(wfile) && STEPS.every((s) => readJson(wfile, { done: [] }).done?.includes(s.id));
  if (fs.existsSync(file) && !flags.force && (!fs.existsSync(wfile) || finished)) {
    throw new Error(`${file} already exists. Run "coderhelm-selfhost deploy", edit the file, or pass --force to start over.`);
  }
  let w = flags.restart ? { answers: {}, done: [] } : readJson(wfile, { answers: {}, done: [] });
  if (flags.answers) w.answers = { ...readJson(path.resolve(flags.answers)), ...w.answers };
  if (flags.telemetry !== undefined) w.answers.telemetry = flags.telemetry === true || flags.telemetry === "on";
  const save = () => {
    if (!runner.dryRun) writeJson(wfile, w);
  };
  let i = STEPS.findIndex((s) => !w.done.includes(s.id));
  if (i < 0) i = STEPS.length;
  if (i > 0 && i < STEPS.length) log(`Picking up at step ${i + 1}, "${STEPS[i].title}" (--restart starts over). Type "back" at any question to go back.`);
  else if (i === 0) log('CoderHelm setup. Type "back" at any question to return to the previous step.');
  // The first step always runs (it signs you in for this session).
  if (i > 0 && !w.done.includes("deployed")) {
    const pre = await stepPrereqs(ctx, w.answers);
    if (pre !== BACK) w.answers = pre;
  }
  // The answers as they were before each step, so "back" asks that step again.
  const before = {};
  while (i < STEPS.length) {
    const step = STEPS[i];
    log(progress(i, STEPS.length, step.title));
    before[i] ??= { ...w.answers };
    const out = await RUN[step.id](ctx, { ...w.answers });
    if (out === BACK) {
      delete before[i];
      i = Math.max(0, i - 1);
      w.answers = { ...(before[i] ?? w.answers) };
      w.done = w.done.filter((d) => d !== STEPS[i].id);
      continue;
    }
    w.answers = out;
    w.done = [...new Set([...w.done, step.id])];
    save();
    i++;
  }
  const cfg = configFromAnswers(w.answers);
  if (runner.dryRun) log(`  [dry run] write ${file}`);
  else writeJson(file, cfg);
  log(`\nSaved ${file}.`);
  if (w.answers.deploy && !w.done.includes("deployed")) {
    await deploy({ ...ctx, flags: { ...flags, githubApp: true, quietReady: true } });
    w.done = [...w.done, "deployed"];
    save();
    // Every check against the running install; "ready" only when all pass.
    log("\nChecking the install from end to end (the same checks as the doctor command)");
    for (;;) {
      const report = await runDoctor(ctx);
      if (report.ok) {
        log(readyMessage(report));
        break;
      }
      if (!ctx.interactive) {
        log(`\nNot ready yet: fix what failed above, then run: ${C.CLI} doctor`);
        break;
      }
      const a = String(await ctx.ask.ask('Fix what failed above, then press Enter to check again ("done" to stop here)', "")).trim().toLowerCase();
      if (a === "done" || a === "skip") {
        log(`\nRun "${C.CLI} doctor" any time to check again.`);
        break;
      }
    }
  } else if (!w.answers.deploy) {
    log('Deploy when you are ready: coderhelm-selfhost deploy');
  }
  return cfg;
}
