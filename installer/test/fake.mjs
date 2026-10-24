// A Runner stand-in: records every command and answers reads from a table.
export class FakeRunner {
  constructor(answers = {}, { dryRun = false } = {}) {
    this.dryRun = dryRun;
    this.answers = answers;
    this.commands = [];
  }
  key(cmd, args) {
    return [cmd, ...args].join(" ");
  }
  async run(cmd, args, opts = {}) {
    this.commands.push({ cmd, args, cwd: opts.cwd, env: opts.env ?? {} });
    const k = this.key(cmd, args);
    for (const [pattern, out] of Object.entries(this.answers)) {
      if (k.includes(pattern)) {
        const v = typeof out === "function" ? out(args, opts) : out;
        if (v && typeof v === "object" && "code" in v) return { stdout: "", stderr: "", ...v };
        return { code: 0, stdout: typeof v === "string" ? v : JSON.stringify(v), stderr: "" };
      }
    }
    return { code: 0, stdout: this.dryRun ? opts.dryRunAnswer ?? "" : "", stderr: "" };
  }
  async json(cmd, args, opts = {}) {
    const { stdout, code } = await this.run(cmd, args, opts);
    if (code !== 0) return null;
    try {
      return JSON.parse(stdout || "null");
    } catch {
      return null;
    }
  }
  lines() {
    return this.commands.map((c) => [c.cmd, ...c.args].join(" "));
  }
}

export function quietAsk(answers = {}) {
  return {
    async ask(q, def = "") {
      for (const [k, v] of Object.entries(answers)) if (q.includes(k)) return v;
      return def;
    },
    async confirm(q, def = false) {
      for (const [k, v] of Object.entries(answers)) if (q.includes(k)) return v;
      return def;
    },
    close() {},
  };
}
