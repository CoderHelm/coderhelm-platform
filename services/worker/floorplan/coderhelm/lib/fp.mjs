// What CoderHelm's detectors use from the vendored Floorplan release, imported (never edited)
// from wherever the release sits: fp/skills/floorplan in the sandbox, vendor/floorplan in tests.
import path from 'node:path';
import { pathToFileURL } from 'node:url';

export const fp = { buildScreens: null, parseYaml: null };

export async function loadFloorplan(skill) {
  const lib = (f) => import(pathToFileURL(path.join(skill, 'scripts', 'lib', f)).href);
  fp.buildScreens = (await lib('screens.mjs')).buildScreens;
  fp.parseYaml = (await lib('serverless.mjs')).parseYaml;
  return fp;
}

/** YAML through Floorplan's reader, with CloudFormation's short tags made into their long form
 *  ("!Ref Bucket" becomes {Ref: "Bucket"}, "!GetAtt Fn.Arn" becomes {"Fn::GetAtt": ["Fn", "Arn"]}). */
export function yaml(text) {
  let doc;
  try { doc = fp.parseYaml(keepBlocks(text)); } catch { return null; }
  const tag = (v) => {
    if (typeof v === 'string') {
      v = v.replace(/\u0001/g, '\n').replace(/\u0002/g, '"');
      const m = /^!(\w+)\s+([\s\S]*)$/.exec(v);
      if (!m) return v;
      const val = m[2].trim().replace(/^["']|["']$/g, '');
      if (m[1] === 'Ref' || m[1] === 'Condition') return { [m[1]]: val };
      if (m[1] === 'GetAtt') return { 'Fn::GetAtt': val.split('.') };
      return { ['Fn::' + m[1]]: val };
    }
    if (Array.isArray(v)) return v.map(tag);
    if (v && typeof v === 'object') return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, tag(x)]));
    return v;
  };
  return tag(doc);
}

/** Floorplan's reader drops block text (`run: |`); deploy steps live there, so each block is
 *  folded into one quoted line first (newlines and quotes stand in as control characters). */
function keepBlocks(text) {
  const lines = text.replace(/\r/g, '').split('\n');
  const out = [];
  for (let i = 0; i < lines.length; i++) {
    const m = /^(\s*(?:-\s+)?[^\s#][^:#]*:\s*)[|>][-+\d]*\s*$/.exec(lines[i]);
    if (!m) { out.push(lines[i]); continue; }
    const own = lines[i].length - lines[i].trimStart().length;
    const body = [];
    let j = i + 1;
    for (; j < lines.length; j++) {
      const l = lines[j];
      if (l.trim() && l.length - l.trimStart().length <= own) break;
      body.push(l);
    }
    while (body.length && !body[body.length - 1].trim()) body.pop();
    const pad = Math.min(...body.filter((l) => l.trim()).map((l) => l.length - l.trimStart().length), 1e9);
    const joined = body.map((l) => l.slice(Math.min(pad, l.length))).join('\u0001').replace(/"/g, '\u0002');
    out.push(`${m[1]}"${joined}"`);
    for (let k = i + 1; k < j; k++) out.push('');
    i = j - 1;
  }
  return out.join('\n');
}
