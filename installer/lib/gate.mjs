// A step's checks, shown, and held until they pass: the guided install
// never moves on from a step whose checks fail. Each failed check says
// what is wrong, why it matters and how to fix it; the person fixes it and
// presses Enter to check again (no restart), types "skip" to go on anyway,
// or "back" to change an earlier answer. A check that offers an action
// (such as running `cdk bootstrap`) is asked about first.
//
// Without a terminal, a failed check stops the install with the same text,
// unless --skip-checks names it (or is "all").
import { FAIL, WARN, formatResults, tally } from "./checks.mjs";

export const BACK = Symbol("back");

export class CheckFailed extends Error {
  constructor(results) {
    const failed = results.filter((r) => r.status === FAIL);
    super(`${failed.length} check${failed.length === 1 ? "" : "s"} failed:\n${formatResults(failed)}\n\nFix ${failed.length === 1 ? "it" : "them"} and run the same command again (it picks up where it stopped).`);
    this.results = results;
    this.plain = true;
  }
}

function skipped(flags, id) {
  const s = flags.skipChecks;
  if (!s) return false;
  if (s === true || s === "all") return true;
  return String(s).split(",").map((x) => x.trim()).includes(id);
}

/**
 * Runs `run()` (an array of results, nulls ignored), prints them, and loops
 * until nothing fails. Returns the results, or BACK.
 */
export async function gate(ctx, run, { heading = "Checking" } = {}) {
  const { log, flags = {} } = ctx;
  for (let round = 0; ; round++) {
    if (round === 0) log(`  ${heading}...`);
    const results = (await run()).filter(Boolean);
    if (results.length) log(formatResults(results));
    const failed = results.filter((r) => r.status === FAIL && !skipped(flags, r.id));
    if (!failed.length) {
      const t = tally(results);
      if (t.warn) log(`  ${t.warn} warning${t.warn === 1 ? "" : "s"} above: you can carry on and fix ${t.warn === 1 ? "it" : "them"} later.`);
      return results;
    }
    if (!ctx.interactive) throw new CheckFailed(results);

    // Offer the actions failed checks come with, one at a time.
    let acted = false;
    for (const f of failed.filter((r) => r.action)) {
      const a = await ctx.ask.ask(`${f.action.label}? (yes/no)`, "yes");
      if (a === "back") return BACK;
      if (/^y/i.test(a)) {
        const [cmd, args] = f.action.command;
        try {
          await ctx.runner.run(cmd, args);
          acted = true;
        } catch (e) {
          log(`  That didn't work: ${String(e.message).split("\n")[0]}`);
        }
      }
    }
    if (acted) continue;
    const a = String(await ctx.ask.ask(`Fix ${failed.length === 1 ? "it" : "them"}, then press Enter to check again ("skip" to go on anyway, "back" to change an answer)`, "")).trim().toLowerCase();
    if (a === "back" || a === "<") return BACK;
    if (a === "skip") {
      log(`  Skipped. The final check will list ${failed.length === 1 ? "it" : "them"} again.`);
      return results;
    }
    log("  Checking again...");
  }
}

/** True when any result is at least a warning (for summaries). */
export function hasWarnings(results) {
  return results.some((r) => r?.status === WARN);
}
