// The release's model manifest, `models.json`: the models a release calls,
// read from the code (services/common/release_models.json, which the
// services build in), with the Bedrock id of each model for every inference
// profile prefix. The release workflow publishes it next to the other
// assets (and in SHA256SUMS); the updater reads it before it applies a
// release and checks each model through the install's provider.
//
//   node selfhost/launch/release-models.mjs v1.2.3 > dist/models.json
import fs from "node:fs";
import { fileURLToPath } from "node:url";

export const SOURCE = fileURLToPath(new URL("../../services/common/release_models.json", import.meta.url));

/** The inference profile prefixes a release lists ids for ("" is in-region). */
export const PREFIXES = ["us.", "eu.", "apac.", "global.", ""];

/** The Bedrock id for a model name (services/common/src/model_provider.rs, map_model_id). */
export function bedrockId(model, prefix, names) {
  return `${prefix}anthropic.${names[model] ?? model}`;
}

/** models.json for a release, from the code's manifest. */
export function releaseModels(source, version) {
  if (!/^v\d+\.\d+\.\d+(-[0-9A-Za-z.-]+)?$/.test(version)) throw new Error(`${version} is not a release tag`);
  if (source.schema !== 1 || !Array.isArray(source.calls) || source.calls.length === 0) {
    throw new Error("release_models.json: schema 1 with a non-empty calls list");
  }
  const names = source.bedrock_names ?? {};
  const models = [...new Set([...source.calls, ...(source.selectable ?? [])])];
  return {
    schema: 1,
    version,
    calls: source.calls,
    selectable: source.selectable ?? [],
    anthropic: Object.fromEntries(models.map((m) => [m, m])),
    bedrock: Object.fromEntries(models.map((m) => [m, Object.fromEntries(PREFIXES.map((p) => [p, bedrockId(m, p, names)]))])),
  };
}

if (process.argv[1] && fileURLToPath(import.meta.url) === fs.realpathSync(process.argv[1])) {
  const doc = releaseModels(JSON.parse(fs.readFileSync(SOURCE, "utf8")), process.argv[2] ?? "");
  process.stdout.write(`${JSON.stringify(doc, null, 2)}\n`);
}
