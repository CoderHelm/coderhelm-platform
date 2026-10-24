// coderhelm-selfhost: install and run CoderHelm in your own AWS account.
import * as cmd from "./commands.mjs";
import { Runner } from "./runner.mjs";
import { parseArgs, prompter } from "./util.mjs";

export const HELP = `coderhelm-selfhost: run CoderHelm in your own AWS account

Usage: coderhelm-selfhost <command> [options]

Commands
  init          Ask a few questions and write coderhelm.config.json
  deploy        Deploy (or redeploy) the release your config names, then
                create your GitHub App if there isn't one yet
  github-app    Create the GitHub App (one click on github.com) and store its keys
  upgrade       Move to the latest release (or --to vX.Y.Z), after the migration check
  rollback      Go back to the previous release (or --to vX.Y.Z)
  health        Check the stacks, functions, queues, keys, email and URLs
  backup        Take on-demand backups of every table
  uninstall     Delete the stacks (tables, bucket and secret are kept)
  telemetry     on | off | status: anonymous install and upgrade pings (off by default)
  pull          Write the config and state from the account (--stage, --region),
                to manage an install from another machine
  version       Print the installer version

Options
  --config <file>     Config file (default ./coderhelm.config.json)
  --version <tag>     Release to deploy (default: the one installed, else the latest)
  --source <dir>      Deploy from a CoderHelm checkout instead of a release
  --dry-run           Print every command and change, run nothing
  --yes               Don't ask (upgrade across action-needed releases, uninstall)
  --org <name>        github-app: create the app in this GitHub organization
  --app-id, --client-id, --private-key-file
                      github-app: store an app you made by hand (GitHub
                      Enterprise Server); secrets come from GITHUB_WEBHOOK_SECRET
                      and GITHUB_CLIENT_SECRET
  --skip-github-app   deploy: don't create the GitHub App now

Docs: https://github.com/CoderHelm/coderhelm-platform/tree/main/docs/self-host
`;

export async function main(argv, deps = {}) {
  const flags = parseArgs(argv);
  const command = flags._[0] ?? (flags.version === true ? "version" : "help");
  if (typeof flags.version !== "string") delete flags.version;
  const log = deps.log ?? console.log;
  const interactive = deps.interactive ?? (process.stdin.isTTY && !flags.yes && !flags.dryRun);
  const runner = deps.runner ?? new Runner({ dryRun: !!flags.dryRun, log });
  const ask = deps.ask ?? prompter({ interactive });
  const ctx = { flags, runner, ask, log, interactive, fetchImpl: deps.fetchImpl ?? fetch, manifestFlow: deps.manifestFlow };
  try {
    switch (command) {
      case "init":
        await cmd.init(ctx);
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
      case "upgrade":
        await cmd.upgrade(ctx);
        return 0;
      case "rollback":
        await cmd.rollback(ctx);
        return 0;
      case "health":
        return await cmd.health(ctx);
      case "backup":
        await cmd.backup(ctx);
        return 0;
      case "uninstall":
        await cmd.uninstall(ctx);
        return 0;
      case "telemetry":
        await cmd.telemetry(ctx);
        return 0;
      case "version":
      case "--version":
        log((await import("../package.json", { with: { type: "json" } })).default.version);
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
  } finally {
    ask.close?.();
  }
}
