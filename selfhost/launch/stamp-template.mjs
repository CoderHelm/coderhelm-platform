#!/usr/bin/env node
// Pins a Launch Stack template to one release: the Version parameter's
// default becomes the release's tag, so the template a release publishes
// always installs that same release (release.yml runs this).
//
//   node stamp-template.mjs <template.yaml> <tag> > out.yaml
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

export const TAG_RE = /^v\d+\.\d+\.\d+(-[0-9A-Za-z.]+)?$/;

/** The template with `Version`'s `Default: latest` replaced by the tag. */
export function stampTemplate(text, tag) {
  if (!TAG_RE.test(tag)) throw new Error(`not a release tag: ${tag}`);
  const lines = text.split("\n");
  const start = lines.findIndex((l) => /^ {2}Version:\s*$/.test(l));
  if (start < 0) throw new Error("the template has no Version parameter");
  for (let i = start + 1; i < lines.length && /^ {4}|^\s*$/.test(lines[i]); i++) {
    if (/^ {4}Default:\s*latest\s*$/.test(lines[i])) {
      lines[i] = `    Default: ${tag}`;
      return lines.join("\n");
    }
  }
  throw new Error("the Version parameter has no `Default: latest` to stamp");
}

/** Where a release's template is published (the hosted deployment's public bucket). */
export function templateUrl(tag, bucket = "coderhelm-public") {
  if (!TAG_RE.test(tag)) throw new Error(`not a release tag: ${tag}`);
  return `https://${bucket}.s3.amazonaws.com/launch/${tag}/coderhelm-launch.yaml`;
}

/** latest.json: what the install guide reads to pick the newest release. */
export function latestDoc(tag, now = new Date()) {
  return { version: tag, templateUrl: templateUrl(tag), publishedAt: now.toISOString() };
}

if (process.argv[1] && fileURLToPath(import.meta.url) === path.resolve(process.argv[1])) {
  const [file, tag] = process.argv.slice(2);
  process.stdout.write(stampTemplate(fs.readFileSync(file, "utf8"), tag));
}
