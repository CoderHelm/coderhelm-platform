// Shared helpers for CoderHelm's detectors. No dependencies; regex + bracket matching, the same
// family of techniques Floorplan's own screens.mjs uses (no AST), so they run in the sandbox
// next to it. Nothing here runs repository code.
import fs from 'node:fs';
import path from 'node:path';

export const SKIP_DIR = /^(node_modules|\.git|\.next|dist|build|Pods|DerivedData|\.dart_tool|\.gradle|vendor|__tests__|__mocks__|e2e|test|tests|androidTest|testFixtures|screenshots?)$/;

/** Test, fixture, mock and story files: never evidence for a link, a screen or a service. */
export const TEST_PATH = /(^|\/)(__tests__|__mocks__|tests?|spec|e2e|cypress|playwright|fixtures?|mocks?|stories)(\/|$)|\.(test|spec|e2e|stories|mock)\.[a-z]+$|(^|\/)test_[^/]+\.py$|_test\.(py|go)$/i;
export const isTest = (p) => TEST_PATH.test(p);

// Each detector reads files only until its deadline; past it, read() answers null and the
// detector finishes with what it has (the map notes which detector ran out of time).
let deadline = Infinity;
let ranOut = false;
export function startClock(ms) { deadline = Date.now() + ms; ranOut = false; }
export function stopClock() { deadline = Infinity; const r = ranOut; ranOut = false; return r; }
export const outOfTime = () => { if (Date.now() > deadline) { ranOut = true; return true; } return false; };

/** Lists files under root (relative paths), bounded. */
export function listFiles(root, { exts, max = 60000, skip = SKIP_DIR } = {}) {
  const out = [];
  const walk = (d) => {
    if (outOfTime()) return;
    let ents; try { ents = fs.readdirSync(path.join(root, d), { withFileTypes: true }); } catch { return; }
    for (const e of ents) {
      if (out.length >= max) return;
      if (e.name.startsWith('.') && e.name !== '.') continue;
      const rel = d ? `${d}/${e.name}` : e.name;
      if (e.isDirectory()) { if (!skip.test(e.name)) walk(rel); }
      else if (!exts || exts.test(e.name)) out.push(rel);
    }
  };
  walk('');
  return out;
}

const MAX_BYTES = 1024 * 1024;
export function read(root, rel) {
  if (outOfTime()) return null;
  try { const p = path.join(root, rel); if (fs.statSync(p).size > MAX_BYTES) return null; return fs.readFileSync(p, 'utf8'); } catch { return null; }
}

/** Directories never walked by the link detectors (build output, dependencies, tool caches). */
export const SKIP_LINK_DIR = /^(\.git|node_modules|dist|build|\.next|\.terraform|cdk\.out|\.aws-sam|vendor|__pycache__|\.venv|venv|coverage|\.serverless|Pods|\.expo|\.turbo|\.terragrunt-cache|floorplan)$/;

/** Every file under root (relative paths) whose name or extension is in `exts` (a Set), bounded. */
export function walk(root, exts, { max = 60000 } = {}) {
  const out = [];
  const go = (d) => {
    if (outOfTime()) return;
    let ents; try { ents = fs.readdirSync(path.join(root, d), { withFileTypes: true }); } catch { return; }
    for (const e of ents) {
      if (out.length >= max) return;
      const rel = d ? `${d}/${e.name}` : e.name;
      if (e.isDirectory()) { if (!SKIP_LINK_DIR.test(e.name) && !(e.name.startsWith('.') && e.name !== '.github')) go(rel); continue; }
      if (!e.isFile()) continue;
      if (exts && !exts.has(path.extname(e.name)) && !exts.has(e.name)) continue;
      out.push(rel);
    }
  };
  go('');
  return out;
}

/** 1-based line of offset i, for one-off lookups. */
export const lineOf = (s, i) => { let n = 1; for (let k = 0; k < i && k < s.length; k++) if (s.charCodeAt(k) === 10) n++; return n; };

/** Evidence in the map's contract: how is "code" (one deterministic reference) or "inferred". */
export const ev = (file, line, how = 'code', confidence = 0.9) => ({ file, line: line || 1, how: how === 'exact' ? 'code' : how, confidence: Math.round(confidence * 100) / 100 });

/** Blanks comments (keeps offsets and newlines) for C-family languages. Strings are kept. */
export function stripComments(s) {
  let out = '', i = 0, q = null;
  while (i < s.length) {
    const c = s[i], n = s[i + 1];
    if (q) { out += c; if (c === '\\') { out += n ?? ''; i += 2; continue; } if (c === q) q = null; i++; continue; }
    if (c === '"' || c === "'" || c === '`') { q = c; out += c; i++; continue; }
    if (c === '/' && n === '/') { const j = s.indexOf('\n', i); const e = j < 0 ? s.length : j; out += ' '.repeat(e - i); i = e; continue; }
    if (c === '/' && n === '*') { const j = s.indexOf('*/', i + 2); const e = j < 0 ? s.length : j + 2; out += s.slice(i, e).replace(/[^\n]/g, ' '); i = e; continue; }
    out += c; i++;
  }
  return out;
}

/** Index of the bracket closing the one at s[i] (skips strings), or -1. Bounded. */
export function closeOf(s, i, limit = 400000) {
  const open = s[i], close = { '{': '}', '(': ')', '[': ']', '<': '>' }[open];
  let depth = 0, q = null;
  for (let k = i, stop = Math.min(s.length, i + limit); k < stop; k++) {
    const c = s[k];
    if (q) { if (c === '\\') k++; else if (c === q) q = null; continue; }
    if (c === '"' || c === "'" || c === '`') q = c;
    else if (c === open) depth++;
    else if (c === close && --depth === 0) return k;
  }
  return -1;
}

/** End of a JSX opening tag starting at i ('<'), skipping {...} and strings. */
export function tagEnd(s, i, limit = 20000) {
  let depth = 0, q = null;
  for (let k = i + 1, stop = Math.min(s.length, i + limit); k < stop; k++) {
    const c = s[k];
    if (q) { if (c === '\\') k++; else if (c === q) q = null; continue; }
    if (c === '"' || c === "'" || c === '`') q = c;
    else if (c === '{') depth++;
    else if (c === '}') depth--;
    else if (c === '>' && depth === 0) return k;
  }
  return -1;
}

/** Top-level `key: value` entries of an object literal body (between the braces). */
export function objectEntries(s, open) {
  const close = closeOf(s, open);
  if (close < 0) return [];
  const out = [];
  let k = open + 1;
  const valueEnd = (from) => {
    let e = from, depth = 0, q = null;
    for (; e < close; e++) {
      const c = s[e];
      if (q) { if (c === '\\') e++; else if (c === q) q = null; continue; }
      if (c === '"' || c === "'" || c === '`') q = c;
      else if (c === '{' || c === '(' || c === '[') depth++;
      else if (c === '}' || c === ')' || c === ']') depth--;
      else if (c === ',' && depth <= 0) break;
    }
    return e;
  };
  while (k < close) {
    const ws = /^\s*/.exec(s.slice(k, k + 200))[0].length;
    const at = k + ws;
    if (at >= close) break;
    if (s.startsWith('...', at)) { const e = valueEnd(at + 3); out.push({ key: null, spread: s.slice(at + 3, e).trim(), start: at, vOffset: at + 3, value: s.slice(at + 3, e).trim() }); k = e + 1; continue; }
    const m = /^(?:(["'])((?:(?!\1).)*)\1|([A-Za-z_$][\w$]*)|\[([^\]]*)\])\s*(:)?/s.exec(s.slice(at, Math.min(close, at + 400)));
    if (!m) { k = valueEnd(at) + 1; continue; }
    const key = m[2] ?? m[3] ?? m[4];
    if (!m[5]) {
      // shorthand `Name,` or method `name() {}`
      const e = valueEnd(at);
      out.push({ key, shorthand: true, start: at, vOffset: at, value: s.slice(at, e).trim() === key ? key : s.slice(at, e).trim() });
      k = e + 1; continue;
    }
    const vStart = at + m[0].length;
    const e = valueEnd(vStart);
    const raw = s.slice(vStart, e);
    out.push({ key, shorthand: false, start: at, vOffset: vStart + (raw.length - raw.trimStart().length), value: raw.trim() });
    k = e + 1;
  }
  return out;
}

export function lineAt(s, i) { let n = 1; for (let k = 0; k < i && k < s.length; k++) if (s.charCodeAt(k) === 10) n++; return n; }
/** Faster line lookup for many queries on one text. */
export function lineIndex(s) { const starts = [0]; for (let k = 0; k < s.length; k++) if (s.charCodeAt(k) === 10) starts.push(k + 1); return (i) => { let lo = 0, hi = starts.length - 1; while (lo < hi) { const mid = (lo + hi + 1) >> 1; if (starts[mid] <= i) lo = mid; else hi = mid - 1; } return lo + 1; }; }

/** Auth vocabulary used to label a gate from a condition, hook or group name. */
export const SIGNED_OUT = /(signed|logged)[_-]?out|unauth|not[_-]?(signed|logged|auth)|guest|anonym|public|outside|onboard|welcome|login|signin|sign[_-]?in|signup|register|auth(?!or)\b|\(auth\)/i;
export const SIGNED_IN = /(signed|logged)[_-]?in|person|member|credential|jwt|\buid\b|is[_-]?auth|authed|authenticated|session(?!s)|current[_-]?(user|account)|has[_-]?(token|account|session)|token|user(?!name)|account|inside|protected|private|member|\(app\)|dashboard/i;

/** Gate from a condition expression ("isSignedIn", "!user", "state.userToken == null"). */
export function gateFromCondition(cond, branch = true) {
  if (!cond) return null;
  const c = cond.replace(/\s+/g, ' ');
  let neg = /^!(?!=)|\b(===?|!==?)\s*(null|undefined|false)\b|\bnull\s*(===?|!==?)/.test(c) ? (/!==?\s*(null|undefined)|(null|undefined)\s*!==?/.test(c) ? false : /===?\s*false|^!/.test(c) || /===?\s*(null|undefined)/.test(c)) : false;
  if (!branch) neg = !neg;
  if (/(signed|logged|sign|log)[_-]?out|unauth|guest|anonym|outside/i.test(c)) return neg ? 'signed-in' : 'signed-out';
  if (SIGNED_IN.test(c) || /\b(auth|login)/i.test(c)) return neg ? 'signed-out' : 'signed-in';
  return null;
}

/** Module resolver over a file list: relative specs, tsconfig/jsconfig `paths` aliases, and a
 *  suffix fallback for `@/`, `~/`, `#/`. Returns a repo-relative file or null. */
export function makeResolver(root, files) {
  const P = path.posix;
  const EXT = /\.(tsx?|jsx?|mjs|vue|astro|svelte)$/;
  const stems = new Map();
  for (const f of files) { const st = f.replace(EXT, ''); stems.set(st, f); if (st.endsWith('/index')) stems.set(st.slice(0, -6), f); }
  const aliases = []; // [prefix, [targetDirs], configDir]
  for (const cfg of files.length ? listFiles(root, { exts: /^(tsconfig|jsconfig)(\.\w+)?\.json$/, max: 200 }) : []) {
    let j; try { j = JSON.parse((read(root, cfg) || '').replace(/\/\*[\s\S]*?\*\/|(^|[^:"])\/\/[^\n]*/g, '$1').replace(/,(\s*[}\]])/g, '$1')); } catch { continue; }
    const base = P.normalize(P.join(P.dirname(cfg), j?.compilerOptions?.baseUrl || '.'));
    for (const [k, v] of Object.entries(j?.compilerOptions?.paths || {})) aliases.push([k.replace(/\*$/, ''), (v || []).map((t) => P.normalize(P.join(base, t.replace(/\*$/, '')))), P.dirname(cfg)]);
  }
  aliases.sort((a, b) => b[0].length - a[0].length);
  const tryStem = (st) => stems.get(P.normalize(st).replace(/^\.\//, '').replace(/\/$/, '')) || null;
  return (from, spec) => {
    if (spec.startsWith('.')) return tryStem(P.join(P.dirname(from), spec));
    for (const [pre, targets, dir] of aliases) {
      if (!spec.startsWith(pre) || (dir !== '.' && !from.startsWith(dir + '/'))) continue;
      for (const t of targets) { const hit = tryStem(P.join(t, spec.slice(pre.length))); if (hit) return hit; }
    }
    if (/^[@~#]\//.test(spec)) { const tail = spec.slice(2); for (const [st, f] of stems) if (st === tail || st.endsWith('/' + tail)) return f; }
    return null;
  };
}
