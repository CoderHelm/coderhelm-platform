// coderhelm-selfhost: install and run CoderHelm in your own AWS account.
import * as cmd from "./commands.mjs";
import { wizard } from "./wizard.mjs";
import { doctor } from "./doctor.mjs";
import { domain } from "./domain.mjs";
import fs from "node:fs";
import path from "node:path";
import { Runner } from "./runner.mjs";
import { parseArgs, prompter } from "./util.mjs";

export const HELP = `coderhelm-selfhost: run CoderHelm in your own AWS account

Usage: coderhelm-selfhost <command> [options]

Commands
  init          The guided install: checks, a few questions with explanations,
                a cost estimate, then deploy, GitHub App and first sign-up.
                Every step checks what you did and shows how to fix what
                isn't right. Resumable; "back" returns to the previous step;
                flags or --answers <file> answer it without a terminal
  doctor        Check everything about an install: AWS sign-in and
                permissions, region, CDK, DNS and certificate, stacks,
                functions, queues, the GitHub App (keys, permissions, webhook
                deliveries), email (SES identity, DKIM, sandbox) and a real
                model call. Each problem comes with its fix. --json for scripts
  deploy        Deploy (or redeploy) the release your config names, then
                create your GitHub App if there isn't one yet
  github-app    Create the GitHub App (one click on github.com) and store its keys
  update        Move to the latest release (or --to vX.Y.Z): health check,
                signature check, migration check, deploy, health check
                (--auto-rollback goes back if the new release is unhealthy)
  rollback      Go back to the previous release (or --to vX.Y.Z)
  auto-update   on | off | status [--window "Sun 03:00"] [--timezone UTC]
                [--channel stable|preview] [--majors ask|auto]
  health        The quick health check the updater runs (doctor checks more)
  backup        Take on-demand backups of every table
  uninstall     Remove CoderHelm from the account: its stacks, Launch Stack,
                permissions boundary, deploy role, buckets, logs and
                parameters. Keeps the data (tables, data bucket, KMS keys,
                user pool, secret) unless --everything. Shows the list and
                asks for the install name first. --verify lists what is left
                (read-only). The guided uninstall does the same from the
                browser: docs.coderhelm.com/self-host/uninstall/
  telemetry     on | off | status: anonymous install and upgrade pings (off by default)
  domain apply  Put your own domain on the install (--hostname, --certificate-arn
                in us-east-1, and --zone/--zone-id for a Route 53 zone), or
                --action remove for the AWS address. Settings, then System
                does all of it with checks and the DNS records to add
  access        status | harden: how this install deploys. "harden" (an
                admin, once) moves it onto its own permissions boundary and
                deploy role, so updates get deploy rights only while they run
                (docs/self-host/security.md)
  pull          Write the config and state from the account (--stage, --region),
                to manage an install from another machine
  version       The installer's, the installed and the latest version

Options
  --config <file>     Config file (default ./coderhelm.config.json)
  --version <tag>     Release to deploy (default: the one installed, else the latest)
  --source <dir>      Deploy from a CoderHelm checkout instead of a release
  --dry-run           Print every command and change, run nothing
  --yes               Don't ask (upgrade across action-needed releases, uninstall)
  --everything        uninstall: delete the data too (also --delete-data);
                      KMS keys after AWS's 7-day minimum
  --verify            uninstall: only list what is left of the install
  --stage, --region   uninstall: the install, without a config file
  --org <name>        github-app: create the app in this GitHub organization
  --app-id, --client-id, --private-key-file
                      github-app: store an app you made by hand (GitHub
                      Enterprise Server); secrets come from GITHUB_WEBHOOK_SECRET
                      and GITHUB_CLIENT_SECRET
  --skip-github-app   deploy: don't create the GitHub App now
  --require-signature (always on: a release whose signature can't be checked is refused)
  --auto-rollback     update: deploy the previous release again if health fails
  --result-file <f>   update: write {status, from, to, reason} to this file
  --skip-model-check  update: go on when only a model the release doesn't call
                      was refused (a model it calls can never be skipped)
  --skip-checks <ids> init: go on past these failed checks (comma-separated
                      ids, or "all"); without a terminal a failed check stops
  --verbose           Show full error details (they are always saved to a
                      log file under .coderhelm/logs)

Docs: https://github.com/CoderHelm/coderhelm-platform/tree/main/docs/self-host
`;

export async function main(argv, deps = {}) {
  const flags = parseArgs(argv);
  // Bare `npx github:CoderHelm/coderhelm-platform`: the guided install, unless
  // this folder already has an install.
  const command = flags._[0] ?? (flags.version === true ? "version" : fs.existsSync(cmd.configPath(flags)) || flags.help ? "help" : "init");
  if (typeof flags.version !== "string") delete flags.version;
  const transcript = [];
  const baseLog = deps.log ?? console.log;
  const log = (m) => {
    transcript.push(String(m));
    baseLog(m);
  };
  const interactive = deps.interactive ?? (process.stdin.isTTY && !flags.yes && !flags.dryRun);
  const runner = deps.runner ?? new Runner({ dryRun: !!flags.dryRun, log });
  const ask = deps.ask ?? prompter({ interactive });
  const ctx = {
    flags, runner, ask, log, interactive,
    fetchImpl: deps.fetchImpl ?? fetch,
    dns: deps.dns,
    nodeVersion: deps.nodeVersion,
    freeBytes: deps.freeBytes,
    manifestFlow: deps.manifestFlow,
    resolveSource: deps.resolveSource,
    open: deps.open,
    aws: deps.aws,
    sleep: deps.sleep,
  };
  try {
    switch (command) {
      case "init":
      case "setup":
        await wizard(ctx);
        return 0;
      case "deploy":
      case "install":
        await cmd.deploy(ctx);
        return 0;
      case "github-app":
        await cmd.githubApp(ctx);
        return 0;
      case "pull":
        await cmd.pull(ctx);
        return 0;
      case "access":
        await cmd.access(ctx);
        return 0;
      case "domain":
        return (await domain(ctx)).status === "succeeded" ? 0 : 1;
      case "update":
      case "upgrade": {
        const r = await cmd.update(ctx);
        return r && r.status !== "succeeded" ? 1 : 0;
      }
      case "auto-update":
        await cmd.autoUpdate(ctx);
        return 0;
      case "rollback":
        await cmd.rollback(ctx);
        return 0;
      case "health":
        return await cmd.health(ctx);
      case "doctor":
      case "check":
        return await doctor(ctx);
      case "backup":
        await cmd.backup(ctx);
        return 0;
      case "uninstall":
        return (await cmd.uninstall(ctx)) ?? 0;
      case "telemetry":
        await cmd.telemetry(ctx);
        return 0;
      case "version":
      case "--version":
        await cmd.version(ctx);
        return 0;
      case "help":
      case "--help":
      case "-h":
        log(HELP);
        return 0;
      default:
        log(`Unknown command "${command}".\n`);
        log(HELP);
        return 2;
    }
  } catch (err) {
    // A plain message by default; the details go to a log file (and to the
    // terminal with --verbose). Tests pass deps.log and get the error back.
    if (deps.log && !deps.handleErrors) throw err;
    const file = saveLog(flags, command, transcript, err);
    const msg = String(err?.message ?? err);
    baseLog(`\n${err?.plain || flags.verbose ? msg : firstLines(msg)}`);
    if (flags.verbose && err?.stack) baseLog(err.stack);
    if (file) baseLog(`\nFull details: ${file}${flags.verbose ? "" : " (or run again with --verbose)"}`);
    return 1;
  } finally {
    ask.close?.();
  }
}

/** The first lines of a long error (a failed command's whole stderr goes to the log). */
export function firstLines(msg, n = 6) {
  const lines = msg.split("\n");
  return lines.length <= n ? msg : `${lines.slice(0, n).join("\n")}\n  ...`;
}

/** Writes what this run printed, and the error with its stack, to .coderhelm/logs. */
export function saveLog(flags, command, transcript, err) {
  try {
    const dir = path.join(path.dirname(cmd.configPath(flags)), cmd.STATE_DIR, "logs");
    fs.mkdirSync(dir, { recursive: true });
    const file = path.join(dir, `${command}-${new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19)}.log`);
    fs.writeFileSync(file, `${transcript.join("\n")}\n\n--- error ---\n${err?.stack ?? err}\n`);
    return file;
  } catch {
    return null;
  }
}
