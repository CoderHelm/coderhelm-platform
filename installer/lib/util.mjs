import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import readline from "node:readline/promises";

export const REPO = "CoderHelm/coderhelm-platform";
export const DEFAULT_TELEMETRY_ENDPOINT = "https://api.coderhelm.com/api/telemetry/ping";

export function readJson(file, fallback = undefined) {
  if (!fs.existsSync(file)) {
    if (fallback !== undefined) return fallback;
    throw new Error(`${file} does not exist`);
  }
  try {
    return JSON.parse(fs.readFileSync(file, "utf8"));
  } catch (e) {
    throw new Error(`${file} is not valid JSON: ${e.message}`);
  }
}

export function writeJson(file, value, mode) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, JSON.stringify(value, null, 2) + "\n", mode ? { mode } : undefined);
}

export function randomHex(bytes = 32) {
  return crypto.randomBytes(bytes).toString("hex");
}

export function sha256File(file) {
  return crypto.createHash("sha256").update(fs.readFileSync(file)).digest("hex");
}

/** The prefix, secret name and parameter the CDK app derives (infra/lib/config.ts). */
export function derived(cfg) {
  const prefix = cfg.resourcePrefix ?? `coderhelm-${cfg.stage}`;
  const secretBase = prefix === `coderhelm-${cfg.stage}` ? `coderhelm/${cfg.stage}` : prefix;
  return {
    prefix,
    secretName: `${secretBase}/secrets`,
    publicUrlParameter: `/${prefix}/public-url`,
    suffix: cfg.globalNameSuffix ?? `-${cfg.account}`,
  };
}

/**
 * A hardened install's access pieces (infra/lib/access.ts, accessNames):
 * null for an install that deploys through the CDK bootstrap as before.
 */
export function accessOf(cfg) {
  const a = cfg?.access;
  if (!a || a.mode !== "hardened") return null;
  const { prefix, suffix } = derived(cfg);
  const partition = /^us-gov-/.test(cfg.region) ? "aws-us-gov" : /^cn-/.test(cfg.region) ? "aws-cn" : "aws";
  const tail = a.owner === "launch" && a.id ? `-${a.id}` : "";
  return {
    owner: a.owner,
    deployRoleArn: `arn:${partition}:iam::${cfg.account}:role/${prefix}-deploy${tail}`,
    boundaryArn: `arn:${partition}:iam::${cfg.account}:policy/${prefix}-boundary${tail}`,
    assetsBucket: `${prefix}-assets${tail}${suffix}`,
    accessStackName: `${prefix}-access`,
  };
}

/** A tiny prompt helper that falls back to defaults when not interactive. */
export function prompter({ interactive, input = process.stdin, output = process.stdout }) {
  let rl;
  return {
    async ask(question, def = "") {
      if (!interactive) return def;
      rl ??= readline.createInterface({ input, output });
      const a = (await rl.question(`${question}${def ? ` [${def}]` : ""}: `)).trim();
      return a || def;
    },
    async confirm(question, def = false) {
      if (!interactive) return def;
      rl ??= readline.createInterface({ input, output });
      const a = (await rl.question(`${question} ${def ? "[Y/n]" : "[y/N]"}: `)).trim().toLowerCase();
      return a ? a.startsWith("y") : def;
    },
    close() {
      rl?.close();
    },
  };
}

export function parseArgs(argv) {
  const out = { _: [] };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a.startsWith("--")) {
      const [k, v] = a.slice(2).split("=", 2);
      const key = k.replace(/-([a-z])/g, (_, c) => c.toUpperCase());
      if (v !== undefined) out[key] = v;
      else if (argv[i + 1] !== undefined && !argv[i + 1].startsWith("--")) out[key] = argv[++i];
      else out[key] = true;
    } else out._.push(a);
  }
  return out;
}

export function semverCompare(a, b) {
  const p = (v) => String(v).replace(/^v/, "").split(/[.-]/).map((x) => (/^\d+$/.test(x) ? Number(x) : x));
  const [x, y] = [p(a), p(b)];
  for (let i = 0; i < Math.max(x.length, y.length); i++) {
    const [m, n] = [x[i] ?? 0, y[i] ?? 0];
    if (m === n) continue;
    if (typeof m === "number" && typeof n === "number") return m < n ? -1 : 1;
    return String(m) < String(n) ? -1 : 1;
  }
  return 0;
}
