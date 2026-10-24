// The deployment's secret (Secrets Manager): GitHub App keys, the session
// signing key, optional Google sign-in keys. Values never pass through argv
// or the terminal: they go in a temporary 0600 file that is removed after.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomHex } from "./util.mjs";

/** Keys the services expect; GitHub ones stay empty until the GitHub App exists. */
export function initialSecret() {
  return {
    github_app_id: "",
    github_private_key: "",
    github_webhook_secret: "",
    github_client_id: "",
    github_client_secret: "",
    jwt_secret: randomHex(32),
  };
}

export async function readSecret(runner, name, region) {
  const out = await runner.json(
    "aws",
    ["secretsmanager", "get-secret-value", "--secret-id", name, "--region", region, "--query", "SecretString", "--output", "json"],
    { allowFail: true, dryRunAnswer: "null" },
  );
  if (typeof out !== "string") return null;
  try {
    return JSON.parse(out);
  } catch {
    return null;
  }
}

/** Creates the secret when it doesn't exist; returns its current value. */
export async function ensureSecret(runner, name, region, log = console.log) {
  const existing = await readSecret(runner, name, region);
  if (existing) return existing;
  const value = initialSecret();
  log(`  creating secret ${name}`);
  await withSecretFile(value, (file) =>
    runner.run("aws", [
      "secretsmanager", "create-secret", "--name", name, "--region", region,
      "--description", "CoderHelm: GitHub App keys and the session signing key",
      "--secret-string", `file://${file}`,
    ], { capture: true }),
  );
  return value;
}

/** Merges keys into the secret; returns the new version id. */
export async function mergeSecret(runner, name, region, keys) {
  const current = (await readSecret(runner, name, region)) ?? initialSecret();
  const next = { ...current, ...keys };
  const out = await withSecretFile(next, (file) =>
    runner.json("aws", [
      "secretsmanager", "put-secret-value", "--secret-id", name, "--region", region,
      "--secret-string", `file://${file}`, "--output", "json",
    ], { dryRunAnswer: JSON.stringify({ VersionId: "dry-run-version" }) }),
  );
  return out?.VersionId ?? "unknown";
}

/** True when the secret holds a GitHub App. */
export function hasGithubApp(secret) {
  return !!(secret && secret.github_app_id && secret.github_private_key);
}

async function withSecretFile(value, fn) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "coderhelm-secret-"));
  const file = path.join(dir, "secret.json");
  fs.writeFileSync(file, JSON.stringify(value), { mode: 0o600 });
  try {
    return await fn(file);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
}
