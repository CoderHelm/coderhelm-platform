/**
 * Which release a deploy is, stamped into the services and the dashboard so
 * an install can say what it runs.
 *
 * The release workflow writes `selfhost/release.json` into each release's
 * source ({version, commit, releasedAt}); a checkout without it is a
 * development build. CODERHELM_VERSION / _COMMIT / _RELEASED_AT override it.
 * The config fingerprint is a short hash of the config (without the
 * dashboard path), so two installs, or one before and after a config change,
 * can be told apart without showing the config.
 */
import * as crypto from "crypto";
import * as fs from "fs";
import { ResolvedConfig } from "./config";
import { repoPath } from "./paths";

export interface ReleaseStamp {
  version: string | null;
  commit: string | null;
  releasedAt: string | null;
  configFingerprint: string;
}

function sorted(v: unknown): unknown {
  if (Array.isArray(v)) return v.map(sorted);
  if (v && typeof v === "object") {
    return Object.fromEntries(Object.keys(v as object).sort().map((k) => [k, sorted((v as Record<string, unknown>)[k])]));
  }
  return v;
}

export function configFingerprint(cfg: ResolvedConfig): string {
  const { dashboard, ...rest } = cfg.raw;
  return crypto.createHash("sha256").update(JSON.stringify(sorted(rest))).digest("hex").slice(0, 12);
}

export function releaseStamp(cfg: ResolvedConfig, env: NodeJS.ProcessEnv = process.env, file = repoPath("selfhost", "release.json")): ReleaseStamp {
  let rel: { version?: string; commit?: string; releasedAt?: string } = {};
  if (fs.existsSync(file)) {
    try {
      rel = JSON.parse(fs.readFileSync(file, "utf8"));
    } catch {
      rel = {};
    }
  }
  return {
    version: env.CODERHELM_VERSION || rel.version || null,
    commit: env.CODERHELM_COMMIT || rel.commit || null,
    releasedAt: env.CODERHELM_RELEASED_AT || rel.releasedAt || null,
    configFingerprint: configFingerprint(cfg),
  };
}

/** The stamp as the services' environment variables (only what is known). */
export function stampEnvironment(s: ReleaseStamp): Record<string, string> {
  const env: Record<string, string> = { CODERHELM_CONFIG_FINGERPRINT: s.configFingerprint };
  if (s.version) env.CODERHELM_VERSION = s.version;
  if (s.commit) env.CODERHELM_COMMIT = s.commit;
  if (s.releasedAt) env.CODERHELM_RELEASED_AT = s.releasedAt;
  return env;
}
