// Runs the commands the installer needs (aws, npx cdk, npm, git, tar), or,
// with --dry-run, prints them and runs nothing. Tests swap in a fake that
// records commands and answers reads.
import { spawn } from "node:child_process";

export class Runner {
  constructor({ dryRun = false, log = console.log, env = process.env } = {}) {
    this.dryRun = dryRun;
    this.log = log;
    this.env = env;
    /** Every command asked for, in order (also in dry runs). */
    this.commands = [];
  }

  /**
   * Run a command. `capture` returns its stdout; otherwise output streams to
   * the terminal. `mutates: false` marks a read that a dry run may still
   * answer from `dryRunAnswer`.
   */
  async run(cmd, args, { cwd, env, capture = false, allowFail = false, input, dryRunAnswer = "" } = {}) {
    const shown = [cmd, ...args.map(redactArg)].join(" ");
    this.commands.push({ cmd, args, cwd, env: env ? Object.keys(env) : [] });
    if (this.dryRun) {
      this.log(`  [dry run] ${cwd ? `(in ${cwd}) ` : ""}${shown}`);
      return { code: 0, stdout: dryRunAnswer, stderr: "" };
    }
    return new Promise((resolve, reject) => {
      const child = spawn(cmd, args, {
        cwd,
        env: { ...this.env, ...(env || {}) },
        stdio: [input === undefined ? "inherit" : "pipe", capture ? "pipe" : "inherit", capture ? "pipe" : "inherit"],
      });
      let stdout = "";
      let stderr = "";
      if (capture) {
        child.stdout.on("data", (d) => (stdout += d));
        child.stderr.on("data", (d) => (stderr += d));
      }
      if (input !== undefined) {
        child.stdin.end(input);
      }
      child.on("error", (e) => reject(new Error(`${cmd} could not start: ${e.message}`)));
      child.on("close", (code) => {
        if (code !== 0 && !allowFail) {
          reject(new Error(`${shown} failed (exit ${code})${stderr ? `:\n${stderr.trim()}` : ""}`));
        } else {
          resolve({ code, stdout, stderr });
        }
      });
    });
  }

  /** Run and parse JSON stdout. */
  async json(cmd, args, opts = {}) {
    const { stdout, code } = await this.run(cmd, args, { ...opts, capture: true });
    if (code !== 0) return null;
    try {
      return JSON.parse(stdout || "null");
    } catch {
      return null;
    }
  }
}

/** Never show secret material in printed commands. */
export function redactArg(a) {
  if (/^--secret-string/.test(a)) return "--secret-string <hidden>";
  if (/^file:\/\/.*secret/i.test(a)) return "file://<hidden>";
  return /\s/.test(a) ? JSON.stringify(a) : a;
}
