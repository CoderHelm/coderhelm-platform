// `coderhelm-selfhost doctor`: every check against an existing install,
// grouped, each with pass, warn, fail or skip and, when it isn't a pass,
// what is wrong, why it matters and how to fix it. Exit code 0 when nothing
// fails. The guided install runs it after the deploy and says "Your install
// is ready" only when everything passes. `--json` prints the results for
// scripts instead.
import fs from "node:fs";
import path from "node:path";
import * as C from "./checks.mjs";
import { configPath, loadConfig, loadState, statePath } from "./commands.mjs";
import { derived, writeJson } from "./util.mjs";

/** The groups, in order, each a list of results (nulls dropped). */
export async function collect(ctx, cfg, state) {
  const groups = [];
  const add = (title, results) => groups.push({ title, results: results.flat().filter(Boolean) });

  const { result: signIn, identity } = await C.checkSignIn(ctx, { profile: process.env.AWS_PROFILE });
  add("This machine and AWS", [
    C.checkNode(ctx.nodeVersion),
    await C.checkAwsCli(ctx),
    signIn,
    C.checkAccountMatch(identity, cfg.account),
    identity ? await C.checkPermissions(ctx, identity) : null,
  ]);
  if (!identity) return groups; // Nothing else can be read without AWS.

  const bedrock = cfg.models?.provider === "bedrock";
  const needsUsEast1 = cfg.domain?.hostname && cfg.region !== "us-east-1" && !cfg.domain.certificateArn;
  add("Region and CDK", [
    await C.checkRegion(ctx, cfg.region, { bedrock }),
    await C.checkBootstrap(ctx, cfg.account, cfg.region),
    needsUsEast1 ? await C.checkBootstrap(ctx, cfg.account, "us-east-1") : null,
  ]);

  const publicUrl = state.publicUrl ?? (cfg.domain?.hostname ? `https://${cfg.domain.hostname}` : null);
  const dns = [];
  if (cfg.domain?.hostedZone) {
    const z = await C.checkHostedZone(ctx, cfg.domain.hostedZone);
    dns.push(z.result, await C.checkDelegation(ctx, cfg.domain.hostedZone, z.zoneId));
  }
  if (cfg.domain?.certificateArn) dns.push(await C.checkCertificate(ctx, cfg.domain.certificateArn, cfg.domain.hostname));
  dns.push(...(await C.checkUrls(ctx, publicUrl, cfg)));
  add("Web address", dns);

  add("Install health", [
    await C.checkStacks(ctx, cfg),
    ...(await C.checkFunctions(ctx, cfg)),
    await C.checkDlq(ctx, cfg),
  ]);

  const { result: secretCheck, secret } = await C.checkSecretStored(ctx, cfg);
  const hybrid = ctx.runner.dryRun ? null : await C.readHybrid(ctx, cfg);
  add("GitHub", [secretCheck, ...(ctx.runner.dryRun ? [] : await C.checkGithubApp(ctx, secret, publicUrl, hybrid))]);

  add("Email", [
    await C.checkSesIdentity(ctx, cfg.email?.from ?? "", cfg.region, { zone: cfg.domain?.hostedZone }),
    await C.checkSesSandbox(ctx, cfg.region),
  ]);

  const key = ctx.flags?.anthropicKey ?? process.env.ANTHROPIC_API_KEY;
  add("Claude", bedrock
    ? [await C.checkBedrock(ctx, cfg.region), await C.checkBedrockCall(ctx, cfg.region, cfg.models?.bedrockInferencePrefix ?? "us.")]
    : [key
        ? await C.checkAnthropicKey(ctx, key)
        : C.problem(C.SKIP, "anthropic_key", "Anthropic API key works", {
            what: "Keys belong to teams, so the doctor has none to test.",
            why: "Settings, then Setup in the dashboard tests your team's key with a real call.",
            fix: { text: "Or test one here:", command: `ANTHROPIC_API_KEY=sk-ant-... ${C.CLI} doctor` },
          })]);

  if (cfg.updates?.webhookUrl) add("Notices", [await C.checkWebhook(ctx, cfg.updates.webhookUrl)]);
  return groups;
}

/** Runs every check and prints them; returns { ok, groups, tally, url }. */
export async function runDoctor(ctx) {
  const { flags, log } = ctx;
  const cfg = loadConfig(flags);
  const state = loadState(flags);
  const d = derived(cfg);
  const groups = await collect(ctx, cfg, state);
  const all = groups.flatMap((g) => g.results);
  const t = C.tally(all);
  const url = state.publicUrl ?? (cfg.domain?.hostname ? `https://${cfg.domain.hostname}` : null);
  const report = { ok: t.ok, tally: t, groups, url, install: d.prefix, version: state.version ?? null, checkedAt: new Date().toISOString() };
  if (flags.json) {
    log(JSON.stringify(report, null, 2));
  } else {
    log(`CoderHelm doctor: install "${cfg.stage}" in ${cfg.account} / ${cfg.region}${state.version ? ` (${state.version})` : ""}`);
    for (const g of groups) {
      if (!g.results.length) continue;
      log(`\n${g.title}`);
      log(C.formatResults(g.results));
    }
    log(`\n${t.pass} passed, ${t.warn} warning${t.warn === 1 ? "" : "s"}, ${t.fail} failed${t.skip ? `, ${t.skip} skipped` : ""}.`);
  }
  // The last result, for "check again" and for support.
  if (!ctx.runner.dryRun && fs.existsSync(path.dirname(statePath(flags)))) {
    writeJson(path.join(path.dirname(statePath(flags)), "doctor.json"), report);
  }
  return report;
}

/** What to say when everything passes: the URL and the next steps. */
export function readyMessage(report) {
  const url = report.url ?? "your install's address";
  return [
    "",
    "Your install is ready.",
    "",
    `  Dashboard: ${url}`,
    "",
    "Next steps",
    `  1. Sign up as the first admin: ${url}/?signup=1`,
    "  2. Turn CoderHelm on for a repository in Settings, Repositories",
    '  3. Assign an issue to CoderHelm, or add the "coderhelm" label',
    `  4. Settings, then Setup shows these checks live: ${url}/settings/setup/`,
    "",
    `Check again any time: ${C.CLI} doctor`,
  ].join("\n");
}

/** The doctor command: exit 0 when nothing fails. */
export async function doctor(ctx) {
  if (!fs.existsSync(configPath(ctx.flags))) {
    throw Object.assign(new Error(`No install config here (${configPath(ctx.flags)}). Run the doctor in the folder you installed from, pass --config, or fetch the config from the account:\n  ${C.CLI} pull --stage <name> --region <region>`), { plain: true });
  }
  const report = await runDoctor(ctx);
  if (!ctx.flags.json) {
    if (report.ok) ctx.log(report.tally.warn ? `\nNothing fails. Fix the warnings when you can.${readyMessage(report)}` : readyMessage(report));
    else ctx.log(`\nFix what failed above, then check again: ${C.CLI} doctor`);
  }
  return report.ok ? 0 : 1;
}
