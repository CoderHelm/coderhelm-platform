// Journeys (flows) for CoderHelm's app map, in the run sandbox after
// Floorplan's build and CoderHelm's detectors. Deterministic, no model:
//
// - `candidates` reads the built map (screens, requests matched to
//   endpoints, outside services, navigation) and the code, and lists every
//   step a journey could take, each citing file:line and a token that is on
//   that line. Five things the map alone misses are closed here, as code:
//   async callbacks (webhook endpoints, events and their listeners, outgoing
//   webhooks), steps inside one page (a form that switches between step
//   components), React context (a hook whose work happens in its provider),
//   the Next route-handler hop (a route that calls the backend), and
//   services reached through dependency injection or a dynamic import.
// - `revalidate` re-checks every stored journey against the code of this
//   build: a step whose token moved is re-lined, a step whose token is gone
//   is dropped, and a journey whose entry is gone, that lost 30% or more of
//   its steps, or has fewer than 3 left is dropped.
// - `prepare` does both for the driver and says which entries the model
//   must (re)write, with the candidates it may choose from. A rebuild where
//   nothing a journey stands on changed asks for nothing.
//
// The model (in the worker, passes/app_map_flows.rs) only names journeys
// and picks and orders candidate ids; it never writes a citation.
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';

export const STORE = 'floorplan/coderhelm/flows.json';
export const SCHEMA = 'coderhelm/flows@1';
/** Steps in one journey, at most. */
export const MAX_STEPS = 15;
/** The candidate list the model reads, at most (characters). */
export const MAX_LIST = 90_000;
/** The candidate list of one journey family in one app, at most. */
export const MAX_GROUP_LIST = 40_000;

/** Words that make a screen the start of a journey. */
export const WORDS = {
  'sign-in': /(log[-_ ]?in(?!g)|sign[-_ ]?in|authenticat)/i,
  'sign-up': /(sign[-_ ]?up|register|create[-_ ]?account|request[-_ ]?membership|join|onboard)/i,
  'password-reset': /(forgot|reset[-_ ]?password|recover|password[-_ ]?update|new[-_ ]?password|set[-_ ]?password)/i,
  booking: /(book(ing)?(?!mark)|reserv|class[-_ ]?detail|schedul|appointment)/i,
  checkout: /(checkout|purchase|payment|basket|cart|buy|subscri|billing|clipcard|credits?\b)/i,
};

const TEST = /(\.test\.|\.spec\.|__tests__|\/tests?\/|\/e2e\/|\.stories\.|\/mocks?\/|\/__mocks__\/|\/fixtures?\/|playwright|cypress|\.d\.ts$)/;
const NOISE_DIR = /\/(ui|icons?|styles?|theme|themes|i18n|locales?|assets|lang|translations)\//i;
const CODE = /\.(m?[jt]sx?|cjs)$/;

export const isTest = (p) => TEST.test(p);
const sha = (s) => crypto.createHash('sha256').update(s).digest('hex').slice(0, 16);
const esc = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

// ── Reading the code ──────────────────────────────────────────────────────

/** A cached line reader over the map's root. */
export function reader(root) {
  const cache = new Map();
  const text = (p) => {
    if (!cache.has(p)) {
      let t = null;
      try {
        const full = path.join(root, p);
        if (!path.resolve(full).startsWith(path.resolve(root) + path.sep)) t = null;
        else t = fs.readFileSync(full, 'utf8');
      } catch { t = null; }
      cache.set(p, t);
    }
    return cache.get(p);
  };
  const split = new Map();
  const lines = (p) => {
    if (!split.has(p)) split.set(p, text(p)?.split('\n') ?? null);
    return split.get(p);
  };
  // Per-file results of the scans below, each file read once.
  const memo = new Map();
  const once = (kind, p, f) => {
    const k = kind + '\0' + p;
    if (!memo.has(k)) memo.set(k, f());
    return memo.get(k);
  };
  return { text, lines, once };
}

/** The line (1-based) holding `token` nearest to `line`: within 3 lines,
 * else anywhere in the file (it moved), else null (it's gone). */
export function findToken(lines, line, token) {
  if (!lines || !token) return null;
  for (let d = 0; d <= 3; d++) {
    for (const l of d ? [line - d, line + d] : [line]) {
      if (l >= 1 && l <= lines.length && lines[l - 1].includes(token)) return l;
    }
  }
  let best = null;
  lines.forEach((t, i) => {
    if (t.includes(token) && (best == null || Math.abs(i + 1 - line) < Math.abs(best - line))) best = i + 1;
  });
  return best;
}

/** A token on `line` to cite: the first hint found on it, else its longest
 * quoted string or identifier. */
export function pickToken(lines, line, hints = []) {
  const t = lines?.[line - 1];
  if (t == null) return null;
  for (const h of hints) if (h && h.length >= 3 && t.includes(h)) return h;
  const quoted = [...t.matchAll(/(['"`])([^'"`\n]{3,80})\1/g)].map((m) => m[2]);
  const words = [...t.matchAll(/[A-Za-z_$@][\w$.@-]{3,60}/g)].map((m) => m[0]);
  const all = [...quoted, ...words].filter((w) => !/^(const|return|export|import|function|async|await|default|from)$/.test(w));
  all.sort((a, b) => b.length - a.length);
  return all[0] || null;
}

/** Evidence on a line, or null when nothing on it can be cited. */
function cite(r, file, line, hints) {
  const lines = r.lines(file);
  if (!lines) return null;
  let at = Math.max(1, Math.min(line || 1, lines.length));
  for (const h of hints) {
    const l = h && findToken(lines, at, h);
    if (l && Math.abs(l - at) <= 3) { at = l; break; }
  }
  const token = pickToken(lines, at, hints);
  return token ? { file, line: at, token } : null;
}

// ── The map, indexed ──────────────────────────────────────────────────────

const strip = (ref) => (typeof ref === 'string' && ref.startsWith('f:') ? ref.slice(2) : null);

function index(graph) {
  const files = new Set((graph.files || []).map((f) => f.p));
  const syms = new Map((graph.files || []).map((f) => [f.p, (f.s || []).map(([name, kind, line]) => ({ name, kind, line })).sort((a, b) => a.line - b.line)]));
  const imports = new Map();
  const ext = new Map();
  const externals = new Map((graph.nodes || []).filter((n) => n.kind === 'external').map((n) => [n.id, n]));
  for (const l of graph.links || []) {
    const a = strip(l.a);
    if (!a || isTest(a)) continue;
    const b = strip(l.b);
    if (l.k === 'import' && b) {
      if (!imports.has(a)) imports.set(a, []);
      imports.get(a).push({ to: b, names: l.names || null });
    }
    if (typeof l.b === 'string' && l.b.startsWith('n:') && externals.has(l.b.slice(2))) {
      if (!ext.has(a)) ext.set(a, []);
      ext.get(a).push({ node: l.b.slice(2), link: l });
    }
  }
  return { files, syms, imports, ext, externals };
}

/** Line ranges of a file's top-level symbols, by name. */
function ranges(ix, r, file) {
  const s = ix.syms.get(file) || [];
  const n = r.lines(file)?.length || 0;
  const out = new Map();
  s.forEach((x, i) => {
    const end = i + 1 < s.length ? s[i + 1].line - 1 : n;
    out.set(x.name, [x.line, Math.max(x.line, end)]);
  });
  return out;
}

function slice(r, file, names, ix) {
  const t = r.text(file) || '';
  if (!names) return t;
  return r.once('slice:' + [...names].sort().join(','), file, () => sliceOf(r, file, names, ix, t));
}

function sliceOf(r, file, names, ix, t) {
  const lines = r.lines(file) || t.split('\n');
  const rg = ranges(ix, r, file);
  const parts = [];
  for (const n of names) {
    const x = rg.get(n);
    if (x) parts.push(lines.slice(x[0] - 1, x[1]).join('\n'));
  }
  return parts.length ? parts.join('\n') : t;
}

// ── Gap closer: dependency injection and dynamic imports ──────────────────

function classIndex(ix, r) {
  const out = new Map();
  for (const [p, s] of ix.syms) {
    if (isTest(p)) continue;
    for (const x of s) if (x.kind === 'class' && !out.has(x.name)) out.set(x.name, p);
  }
  // Floorplan lists exported symbols; a class it missed is found by name.
  return { get: (name) => out.get(name) || null, all: out };
}

/** Resolve an import specifier to a file on the map. */
function resolveSpec(ix, from, spec) {
  const tries = [];
  if (spec.startsWith('.')) tries.push(path.posix.normalize(path.posix.join(path.posix.dirname(from), spec)));
  else {
    // An alias (`@/x`, `~/x`, `@scope/pkg/x`, `#/x`): match by the tail.
    const tail = spec.replace(/^(@[\w-]+\/[\w-]+\/|[@~#]\/|src\/)/, '');
    tries.push(tail);
  }
  const exts = ['', '.ts', '.tsx', '.js', '.jsx', '.mjs', '/index.ts', '/index.tsx', '/index.js'];
  for (const t of tries) {
    for (const e of exts) if (ix.files.has(t + e)) return t + e;
    if (!spec.startsWith('.')) {
      for (const e of exts) {
        const want = '/' + t + e;
        for (const f of ix.files) if (f.endsWith(want)) return f;
      }
    }
  }
  return null;
}

/** Imports the map has no link for: constructor injection (Nest and
 * similar) and `import()` / `require()` with a literal. */
export function hiddenImports(ix, r, classes, file) {
  return r.once('hidden', file, () => hiddenImportsOf(ix, r, classes, file));
}

function hiddenImportsOf(ix, r, classes, file) {
  const t = r.text(file);
  if (!t) return [];
  const out = [];
  for (const m of t.matchAll(/(?:private|public|protected|readonly)\s+(?:readonly\s+)?(\w+)\s*:\s*([A-Z]\w+)/g)) {
    const f = classes.get(m[2]);
    if (f && f !== file) out.push({ to: f, names: null, how: 'di', prop: m[1] });
  }
  for (const m of t.matchAll(/(?:\bimport|\brequire)\(\s*['"]([^'"]+)['"]\s*\)/g)) {
    const f = resolveSpec(ix, file, m[1]);
    if (f && f !== file) out.push({ to: f, names: null, how: 'dynamic' });
  }
  return out;
}

// ── Gap closer: React context ─────────────────────────────────────────────

/** Whether `name` in `file` is a hook that reads a context: then the work
 * it hands back happens in the provider, so the whole module counts. */
function readsContext(ix, r, file, name) {
  if (!/^use[A-Z]/.test(name)) return false;
  const body = slice(r, file, [name], ix);
  return /\buse(Context)?\(\s*\w*Context\b|\buseContext\(/.test(body);
}

// ── Closures ──────────────────────────────────────────────────────────────

/** Lines that are part of an import statement. */
function importLines(lines) {
  const out = new Set();
  let open = false;
  lines.forEach((t, i) => {
    if (open || /^\s*import\b/.test(t) || /^\s*(const|let|var)\s+[\w{},\s]+=\s*require\(/.test(t)) {
      out.add(i);
      open = /^\s*import\b/.test(t) || open ? !/\bfrom\s*['"]|^\s*import\s*['"]|\)\s*;?\s*$/.test(t) : false;
    }
  });
  return out;
}

/** Where `file` first runs what it imports from `to` (1-based; 0 when
 * unknown): the first line outside the imports that names it. A hook that
 * hands back functions (`const { mutate: pay } = usePay()`) runs when
 * those are called, so its line is the first call of what it hands back. */
export function useLine(ix, r, file, to, names) {
  return r.once('use:' + to + ':' + (names || []).join(','), file, () => {
    const lines = r.lines(file) || [];
    const skip = importLines(lines);
    const ids = new Set((names || []).filter((n) => n !== 'default' && n !== '*'));
    const def = (ix.syms.get(to) || []).find((x) => x.kind === 'default');
    if (def && def.name !== 'default' && !(names || []).some((n) => n.startsWith('this.'))) ids.add(def.name);
    const base = path.posix.basename(to).replace(/\.\w+$/, '');
    if (!(names || []).some((n) => n.startsWith('this.'))) ids.add(base === 'index' ? path.posix.basename(path.posix.dirname(to)) : base);
    const named = [...ids].filter((n) => /^(this\.)?[A-Za-z_$][\w$]*$/.test(n));
    if (!named.length) return 0;
    // A call, a render or a `this.x.` use runs it; a type or a mention
    // doesn't, so it counts only when nothing runs it.
    const alt = named.map(esc).join('|');
    const runs = new RegExp('(?:\\b(?:' + alt + ')\\s*(?:<[^>()]*>)?\\(|<(?:' + alt + ')\\b|\\b(?:' + alt + ')\\.\\w+\\s*\\()');
    const re = new RegExp('\\b(' + alt + ')\\b');
    let at = lines.findIndex((t, i) => !skip.has(i) && runs.test(t));
    if (at < 0) at = lines.findIndex((t, i) => !skip.has(i) && re.test(t));
    if (at < 0) return 0;
    const m = /^\s*(?:const|let|var)\s+(\{[^}]*\}|\[[^\]]*\]|[\w$]+)\s*=\s*(?:await\s+)?(use[A-Z]\w*)\s*\(/.exec(lines[at]);
    if (m) {
      const bound = m[1].replace(/[{}[\]]/g, '').split(',').map((x) => x.split(':').pop().trim().replace(/\s*=.*$/, '')).filter((x) => /^[A-Za-z_$][\w$]*$/.test(x));
      if (bound.length) {
        const call = new RegExp('\\b(' + bound.map(esc).join('|') + ')(\\.(mutate|mutateAsync|trigger|refetch|execute))?\\s*\\(');
        const later = lines.findIndex((t, i) => i > at && call.test(t));
        if (later >= 0) return later + 1;
      }
    }
    return at + 1;
  });
}

/** The names a file runs what it imports by: the imported names, and what
 * a hook among them hands back (`const { login } = useFlow()`). */
function runNames(lines, names) {
  const out = new Set((names || []).filter((n) => /^[A-Za-z_$][\w$]*$/.test(n) && n !== 'default'));
  for (const t of lines) {
    const m = /^\s*(?:const|let|var)\s+(\{[^}]*\}|\[[^\]]*\]|[\w$]+)\s*=\s*(?:await\s+)?(use[A-Z]\w*)\s*\(/.exec(t);
    if (!m || !out.has(m[2])) continue;
    for (const x of m[1].replace(/[{}[\]]/g, '').split(',')) {
      const b = x.split(':').pop().trim().replace(/\s*=.*$/, '');
      if (/^[A-Za-z_$][\w$]*$/.test(b)) out.add(b);
    }
  }
  return [...out];
}

/** The functions a rendered component is handed (`onContinue={handleLogin}`,
 * `onDone={() => save()}`), read from its tag on `line` (1-based). */
export function propHandlers(lines, line) {
  const out = [];
  for (let i = line - 1; i < Math.min(lines.length, line + 40); i++) {
    const t = lines[i];
    for (const m of t.matchAll(/\b(on[A-Z]\w*)=\{\s*([A-Za-z_$][\w$]*)\s*\}/g)) out.push({ prop: m[1], fn: m[2] });
    for (const m of t.matchAll(/\b(on[A-Z]\w*)=\{[^}]*?=>\s*(?:void\s+)?([A-Za-z_$][\w$]*)\s*\(/g)) out.push({ prop: m[1], fn: m[2] });
    if (i > line - 1 && /\/>|^\s*>/.test(t)) break;
  }
  return out;
}

/** Where a function declared in a file runs (1-based lines, from its
 * declaration to its closing brace), or null. */
export function bodyRange(lines, name) {
  const decl = new RegExp('^\\s*(?:export\\s+)?(?:(?:const|let|var)\\s+' + esc(name) + '\\s*=|(?:async\\s+)?function\\s+' + esc(name) + '\\b)');
  const a = lines.findIndex((t) => decl.test(t));
  if (a < 0) return null;
  let depth = 0;
  let opened = false;
  for (let i = a; i < lines.length; i++) {
    for (const ch of lines[i].replace(/(['"`])(?:\\.|(?!\1).)*\1/g, '""').replace(/\/\/.*$/, '')) {
      if (ch === '{' || ch === '(') { depth++; opened = true; } else if (ch === '}' || ch === ')') depth--;
    }
    if (opened && depth <= 0) return [a + 1, i + 1];
  }
  return [a + 1, lines.length];
}

/** Run order between two places in the code: each is the chain of lines
 * from the start file down to the call (where each file runs the next,
 * then the call's own line), so calls made by statements earlier in a
 * function come first, however deep the code they go through. */
export function runOrder(a, b) {
  for (let i = 0; i < Math.min(a.length, b.length); i++) if (a[i] !== b[i]) return a[i] - b[i];
  return a.length - b.length;
}

/** The files a start file reaches, symbol-aware: file to the names used
 * from it (null: all of it), how deep, and the chain of lines (`at`) where
 * each file on the way runs the next. */
export function closure(ix, r, classes, start, { depth = 5, cap = 120, skip = null } = {}) {
  const seen = new Map([[start, { d: 0, names: null, at: [], via: null }]]);
  const queue = [start];
  while (queue.length) {
    const f = queue.shift();
    const { d, names, at, via } = seen.get(f);
    if (d >= depth) continue;
    const body = slice(r, f, names && [...names], ix);
    const edges = [...(ix.imports.get(f) || []), ...hiddenImports(ix, r, classes, f)];
    for (const e of edges) {
      if (isTest(e.to) || NOISE_DIR.test('/' + e.to) || skip?.has(e.to)) continue;
      let use = null;
      if (e.names && e.names.length && !e.names.includes('default') && !e.names.includes('*')) {
        const used = e.names.filter((n) => new RegExp('\\b' + esc(n) + '\\b').test(body));
        if (!used.length) continue;
        const component = /\.(tsx|jsx)$/.test(e.to) && used.some((n) => /^[A-Z]/.test(n));
        const ctx = used.some((n) => readsContext(ix, r, e.to, n));
        use = component || ctx ? null : new Set(used);
      } else if (e.prop && !new RegExp('\\bthis\\.' + esc(e.prop) + '\\b').test(body) && names) continue;
      const had = seen.get(e.to);
      if (had) {
        if (had.names && (!use || [...use].some((n) => !had.names.has(n)))) {
          had.names = use ? new Set([...had.names, ...use]) : null;
          queue.push(e.to);
        }
        continue;
      }
      if (seen.size >= cap) continue;
      const how = e.prop ? ['this.' + e.prop] : e.names;
      seen.set(e.to, { d: d + 1, names: use, at: [...at, useLine(ix, r, f, e.to, how)], via: via || e.to });
      queue.push(e.to);
    }
  }
  return seen;
}

function inUse(ix, r, file, names, line) {
  if (!names) return true;
  const rg = ranges(ix, r, file);
  for (const n of names) {
    const x = rg.get(n);
    if (x && line >= x[0] && line <= x[1]) return true;
  }
  return false;
}

// ── Requests ──────────────────────────────────────────────────────────────

const segs = (p) => p.split('/').filter(Boolean);

/** The route a path reaches: exact segments beat parameters; `*` ends a
 * catch-all. */
export function matchRoute(routes, p, method) {
  let best = null;
  let score = -1;
  const ps = segs(p.replace(/\?.*$/, ''));
  routes.forEach((r, i) => {
    if (!r.p || isTest(r.f)) return;
    const rs = segs(r.p);
    let sc = 0;
    let ok = true;
    for (let k = 0; k < rs.length; k++) {
      const s = rs[k];
      if (s === '*' || /^\*|^\[\.\.\./.test(s)) { sc += 1; break; }
      if (k >= ps.length) { ok = false; break; }
      if (s === ps[k]) sc += 3;
      else if (s.startsWith(':') || /^\[.+\]$/.test(s) || ps[k].startsWith(':') || ps[k].startsWith('${')) sc += 1;
      else { ok = false; break; }
      if (k === rs.length - 1 && rs.length !== ps.length) ok = false;
    }
    if (!rs.length && ps.length) ok = false;
    if (!ok) return;
    if (method && r.m && r.m.toUpperCase() === method.toUpperCase()) sc += 2;
    if (sc > score) { best = i; score = sc; }
  });
  return best;
}

/** The path literal on a call's line, nearest to the route it reached. */
function callPath(lines, line, route) {
  for (let d = 0; d <= 2; d++) {
    for (const l of d ? [line - d, line + d] : [line]) {
      const t = lines?.[l - 1];
      if (!t) continue;
      const lits = [...t.matchAll(/(['"`])((?:\$\{[^}]+\})?\/[^'"`\s]*)\1/g)].map((m) => ({ raw: m[2], at: l }));
      if (!lits.length) continue;
      const tail = segs(route?.p || '').filter((s) => !/^[:*[]/.test(s)).pop();
      const pick = lits.find((x) => tail && x.raw.includes(tail)) || lits[0];
      return pick;
    }
  }
  return null;
}

const normPath = (p) => '/' + segs(p.replace(/\?.*$/, '').replace(/^\$\{[^}]+\}/, '').replace(/\$\{[^}]+\}/g, ':p').replace(/\/:\w+/g, '/:p')).join('/');

// ── Gap closer: the Next route-handler hop ────────────────────────────────

/** Requests a Next route handler (or a pages/api handler) makes to another
 * service: `fetch(`${base}/x`)`, `fetch(url)` with `const url = `${base}/x``. */
export function routeHops(text) {
  const out = [];
  if (!text) return out;
  const lines = text.split('\n');
  const at = (i) => text.slice(0, i).split('\n').length;
  for (const m of text.matchAll(/\b(?:fetch|axios(?:\.\w+)?|\w*[Cc]lient\.(?:get|post|put|patch|delete))\s*(?:<[^>()]*>)?\(\s*(['"`])((?:\$\{[^}]+\})\/[^'"`]*)\1/g)) {
    out.push({ raw: m[2], line: at(m.index + m[0].indexOf(m[2])) });
  }
  for (const m of text.matchAll(/(?:const|let)\s+(\w+)\s*=\s*`(\$\{[^}]+\}\/[^`]*)`/g)) {
    if (new RegExp('\\bfetch\\(\\s*' + esc(m[1]) + '\\b').test(text)) out.push({ raw: m[2], line: at(m.index + m[0].indexOf(m[2])) });
  }
  return out.filter((x) => lines[x.line - 1]?.includes(x.raw));
}

// ── Calls the map has no link for ─────────────────────────────────────────

const HTTP_FN = String.raw`(?:fetch|axios(?:\.\w+)?|\w*[Hh]ttp(?:Client)?\.\w+|\w*[Cc]lient\.(?:get|post|put|patch|delete)|\w*(?:[Gg]et|[Pp]ost|[Pp]ut|[Pp]atch|[Dd]elete|[Rr]equest)|\$fetch)`;
// A call to an HTTP client (or a query hook, or a URL being built).
const HTTP_NEAR = new RegExp(HTTP_FN + String.raw`\s*(?:<[^>()]*>)?\(|\buse\w*(?:Query|Mutation)\s*\(|\bnew\s+URL\(|\burl\s*[:=]`);
const METHOD_NEAR = /\b(?:method:\s*['"](\w+)['"]|\.(get|post|put|patch|delete)\s*[<(]|(?:api|http|mapi)?(Get|Post|Put|Patch|Delete)\s*[<(])/i;

/** Calls written in one file: `fetch('/x')`, `apiPost(`${base}/x`)`,
 * `const URL = '/x'` then `fetch(URL)`, and next-auth's `signIn('p')`.
 * Each with its path, line, the literal on that line and a method guess. */
export function clientCalls(text) {
  const out = [];
  if (!text) return out;
  const at = (i) => text.slice(0, i).split('\n').length;
  const method = (i) => {
    const w = text.slice(Math.max(0, i - 120), i + 300);
    const m = METHOD_NEAR.exec(w);
    return m ? (m[1] || m[2] || m[3]).toUpperCase() : null;
  };
  const push = (raw, i, how) => {
    const p = raw.replace(/\?.*$/, '');
    if (!/^(\$\{[^}]+\})?\/[A-Za-z]/.test(p) || /^\/(_next|static|images?|fonts?|assets)\//.test(p)) return;
    out.push({ raw, path: normPath(p), line: at(i), token: raw, how, method: method(i) });
  };
  for (const m of text.matchAll(new RegExp(HTTP_FN + String.raw`\s*(?:<[^>()]*>)?\(\s*(['"\`])((?:\$\{[^}]+\})?\/[^'"\`]*)\1`, 'g'))) push(m[2], m.index + m[0].indexOf(m[2]), 'direct');
  for (const m of text.matchAll(/(?:const|let)\s+(\w+)\s*=\s*(['"`])((?:\$\{[^}]+\})?\/[\w\-./:${}]+)\2/g)) {
    if (new RegExp(HTTP_FN + String.raw`\s*(?:<[^>()]*>)?\(\s*` + esc(m[1]) + String.raw`\b`).test(text)) push(m[3], m.index + m[0].indexOf(m[3]), 'const');
  }
  for (const m of text.matchAll(/\bsignIn(?:<[^>]*>)?\(\s*['"](\w+)['"]/g)) {
    out.push({ raw: `/api/auth/callback/${m[1]}`, path: `/api/auth/callback/${m[1]}`, line: at(m.index), token: m[0], how: 'next-auth', method: 'POST' });
  }
  return out;
}

/** Objects of paths (`const URLS = { login: '/auth/login', ... }`): name to
 * entries `{ key: { path, line, token } }`, for maps of two or more. */
export function urlMaps(text) {
  const out = new Map();
  if (!text) return out;
  const at = (i) => text.slice(0, i).split('\n').length;
  for (const m of text.matchAll(/(?:export\s+)?const\s+(\w+)\s*(?::[^=]+)?=\s*\{/g)) {
    let depth = 1;
    let i = m.index + m[0].length;
    const start = i;
    while (i < text.length && depth) { const ch = text[i]; if (ch === '{') depth++; else if (ch === '}') depth--; i++; }
    const body = text.slice(start, i);
    const ent = {};
    for (const e of body.matchAll(/(\w+)\s*:\s*(['"`])(\/[\w\-./:${}]+)\2/g)) ent[e[1]] = { path: normPath(e[3]), line: at(start + e.index), token: e[3] };
    if (Object.keys(ent).length >= 2) out.set(m[1], ent);
  }
  return out;
}

/** Calls through an API client or SDK rather than a URL: XRPC-style
 * `client.call(com.example.server.createSession, ...)`, and static SDK
 * methods on a class imported from a package (`PasswordSession.login(`).
 * Each with its id (`xrpc:<method>` or `sdk:<Class>.<method>`), line, token
 * and the package it comes from when known. */
export function sdkCalls(text) {
  const out = [];
  if (!text) return out;
  const at = (i) => text.slice(0, i).split('\n').length;
  const pkgOf = new Map();
  for (const m of text.matchAll(/import\s+(?:type\s+)?\{([^}]*)\}\s+from\s+['"]([^'".][^'"]*)['"]/g)) {
    for (const n of m[1].split(',')) {
      const name = n.replace(/^\s*type\s+/, '').split(/\s+as\s+/).pop().trim();
      if (name) pkgOf.set(name, m[2]);
    }
  }
  for (const m of text.matchAll(/import\s+(\w+)\s+from\s+['"]([^'".][^'"]*)['"]/g)) pkgOf.set(m[1], m[2]);
  const imported = (name) => pkgOf.get(name) || null;
  for (const m of text.matchAll(/\.call\(\s*['"]?((?:com|app|chat|tools)\.[a-z]+\.[a-z]+\.[a-zA-Z]+)/g)) {
    out.push({ id: `xrpc:${m[1]}`, line: at(m.index), token: m[1], pkg: imported(m[1].split('.')[0]) });
  }
  for (const m of text.matchAll(/\b([A-Z]\w*(?:Session|Client|Agent|Api|API|Sdk|SDK|Auth))\.([a-z]\w*)\(/g)) {
    const pkg = imported(m[1]);
    if (!pkg) continue;
    out.push({ id: `sdk:${m[1]}.${m[2]}`, line: at(m.index), token: `${m[1]}.${m[2]}`, pkg });
  }
  return out;
}

/** Calls into an integration helper a handler leans on: a manager object
 * (`eventManager.create(`), or a function that pays, mails, messages or
 * checks out (`handlePayment(`, `sendEmailVerification(`). Declarations
 * are not calls. */
export function helperCalls(text) {
  const out = [];
  if (!text) return out;
  const at = (i) => text.slice(0, i).split('\n').length;
  const decl = (i) => /(function\s*|import\s*\{[^}]*|export\s*\{[^}]*|\bas\s*)$/.test(text.slice(Math.max(0, i - 60), i));
  for (const m of text.matchAll(/\b([a-z]\w*Manager)\.(\w+)\(/g)) {
    out.push({ name: `${m[1].charAt(0).toUpperCase()}${m[1].slice(1)}.${m[2]}`, line: at(m.index), token: `${m[1]}.${m[2]}` });
  }
  for (const m of text.matchAll(/\b((?:handle|send|charge|create|process)\w*(?:Payment|Email|Sms|Mail|Notification|Checkout|Subscription|Invoice)\w*)\s*\(/g)) {
    if (decl(m.index)) continue;
    out.push({ name: m[1], line: at(m.index), token: m[1] });
  }
  return out;
}

const isRouteHandlerFile = (f) => /\/route\.(m?[jt]s)$/.test(f) || /\/pages\/api\//.test(f);

// ── Gap closer: async callbacks ───────────────────────────────────────────

const WEBHOOK_PATH = /(web-?hooks?|callbacks?|notifications?|\bipn\b|\/events?$)/i;

/** Event names with their emit and listen sites in one file. */
export function eventSites(text) {
  const out = { emits: [], listens: [] };
  if (!text) return out;
  const at = (i) => text.slice(0, i).split('\n').length;
  const name = (s) => s.replace(/^['"`]|['"`]$/g, '');
  for (const m of text.matchAll(/\.emit(?:Async)?\(\s*([A-Z][A-Z0-9_]+(?:\.[A-Z0-9_]+)?|['"`][\w.:-]+['"`])/g)) {
    out.emits.push({ name: name(m[1]), line: at(m.index), token: m[1].replace(/^['"`]|['"`]$/g, '') });
  }
  for (const m of text.matchAll(/(?:@OnEvent|\.on|\.once|\.addListener|\.subscribe)\(\s*([A-Z][A-Z0-9_]+(?:\.[A-Z0-9_]+)?|['"`][\w.:-]+['"`])/g)) {
    out.listens.push({ name: name(m[1]), line: at(m.index), token: m[1].replace(/^['"`]|['"`]$/g, '') });
  }
  return out;
}

/** Outgoing webhooks and scheduled callbacks a server function starts. */
export function outgoingHooks(text) {
  const out = [];
  if (!text) return out;
  const at = (i) => text.slice(0, i).split('\n').length;
  for (const m of text.matchAll(/\b((?:handle|send|trigger|schedule|fire|dispatch)\w*Webhooks?\w*)\s*\(/g)) {
    if (/function\s*$|^\s*(async\s+)?$/.test(text.slice(Math.max(0, m.index - 12), m.index))) continue;
    out.push({ line: at(m.index), token: m[1] });
  }
  return out;
}

// ── Gap closer: steps inside one page ─────────────────────────────────────

/** Step components a page switches between (`step === 'x' && <StepX`,
 * `case Forms.Login: return <LoginForm`, `{[Step.A]: <A/>}`), in source
 * order, each with the component name, its line and the step's name. */
export function pageSteps(text) {
  const out = [];
  if (!text) return out;
  const at = (i) => text.slice(0, i).split('\n').length;
  const pats = [
    /(\w+(?:\.\w+)?)[ \t]*===\s*((?:\w+\.)?\w+|['"][\w-]+['"])\s*(?:&&|\?)\s*\(?\s*<([A-Z]\w+)/g,
    /\[\s*((?:\w+\.)?\w+)\s*\]\s*:\s*\(?\s*<([A-Z]\w+)/g,
  ];
  for (const re of pats) {
    for (const m of text.matchAll(re)) {
      const comp = m[m.length - 1];
      const step = (m.length === 4 ? m[2] : m[1]).replace(/^['"]|['"]$/g, '').split('.').pop();
      const i = m.index + m[0].lastIndexOf('<' + comp);
      out.push({ step, comp, line: at(i), token: '<' + comp });
    }
  }
  // `case Forms.Login:` then the first component its branch renders
  // (`return <LoginForm` or `content = (<LoginForm`), before the next case.
  const cases = [...text.matchAll(/\bcase\s+((?:\w+\.)?\w+|['"][\w-]+['"])\s*:/g)];
  cases.forEach((m, k) => {
    const end = Math.min(k + 1 < cases.length ? cases[k + 1].index : text.length, m.index + 1500);
    const body = text.slice(m.index, end);
    const d = body.search(/\bdefault\s*:/);
    const r = /<([A-Z]\w+)/.exec(d >= 0 ? body.slice(0, d) : body);
    if (!r) return;
    const step = m[1].replace(/^['"]|['"]$/g, '').split('.').pop();
    out.push({ step, comp: r[1], line: at(m.index + r.index), token: '<' + r[1] });
  });
  // The last branch of a chain of step ternaries: `) : (<StepCaptcha />)`.
  if (out.length) {
    for (const m of text.matchAll(/===\s*(?:\w+\.)?\w+\s*\?\s*\(\s*<[A-Z]\w+[\s\S]{0,800}?\)\s*:\s*\(\s*<([A-Z]\w+)/g)) {
      const i = m.index + m[0].lastIndexOf('<' + m[1]);
      if (!out.some((x) => x.comp === m[1])) out.push({ step: m[1], comp: m[1], line: at(i), token: '<' + m[1] });
    }
  }
  // Forms reached by a call (`gotoForm(Forms.ForgotPassword)`) name a step
  // even when the render is elsewhere.
  for (const m of text.matchAll(/\b(?:gotoForm|setCurrentForm|setForm|setStep|goToStep|setScreenState|setProgress)\(\s*((?:\w+\.)?\w+|['"][\w-]+['"])/g)) {
    out.push({ step: m[1].replace(/^['"]|['"]$/g, '').split('.').pop(), comp: null, line: at(m.index), token: m[0], goto: true });
  }
  // `?progress=x` / `?step=x` in links and router pushes.
  for (const m of text.matchAll(/[?&](progress|step)=([\w-]+)/g)) {
    out.push({ step: m[2], comp: null, line: at(m.index), token: m[0].slice(1), goto: true });
  }
  out.sort((a, b) => a.line - b.line);
  return out;
}

// ── Navigation in code (used when the map has no nav links) ───────────────

export function navSites(text) {
  const out = [];
  if (!text) return out;
  const at = (i) => text.slice(0, i).split('\n').length;
  for (const m of text.matchAll(/\b(?:navigate|push|replace|popToScreen|reset)\(\s*['"]([A-Z]\w+)['"]/g)) out.push({ kind: 'name', target: m[1], line: at(m.index), token: m[1] });
  for (const m of text.matchAll(/\bname:\s*['"]([A-Z]\w+)['"]/g)) out.push({ kind: 'name', target: m[1], line: at(m.index), token: m[1] });
  for (const m of text.matchAll(/(?:router\.(?:push|replace)|redirect|navigate|href=)\s*\(?\s*\{?\s*(['"`])(\/[^'"`?#\s]*)/g)) out.push({ kind: 'url', target: m[2].replace(/\$\{[^}]+\}/g, ':p'), line: at(m.index), token: m[2].slice(0, 40) });
  return out;
}

const urlMatch = (u, pat) => {
  const a = segs(u);
  const b = segs(pat);
  return a.length === b.length && a.every((x, i) => x === b[i] || /^[:[]/.test(b[i]) || x.startsWith(':'));
};

// ── Candidates ────────────────────────────────────────────────────────────

/** Every step a journey on this map could take, and how they connect.
 * `read` is a `reader(root)`. */
export function candidates(graph, read) {
  const r = read;
  const ix = index(graph);
  const classes = classIndex(ix, r);
  const routes = graph.routes || [];
  const byKey = new Map();
  const list = [];
  const edges = new Map();
  const add = (c) => {
    if (byKey.has(c.key)) return byKey.get(c.key);
    if (!c.evidence.length || c.evidence.some((e) => !e)) return null;
    const prefix = { screen: 'S', request: 'R', outside: 'X', async: 'A' }[c.kind];
    c.id = prefix + (list.filter((x) => x.kind === c.kind).length + 1);
    list.push(c);
    byKey.set(c.key, c);
    return c;
  };
  // An edge, with where the code makes it (closure depth, then line), so
  // a journey's steps can be put in the order the code runs them.
  let seq = 0;
  // `rank` is where: { d: how deep, at: the chain of lines (runOrder) }.
  const link = (a, b, why, rank = null) => {
    if (!a || !b || a === b) return;
    if (!edges.has(a.id)) edges.set(a.id, new Map());
    const had = edges.get(a.id).get(b.id);
    if (!had) edges.get(a.id).set(b.id, { why, rank, i: seq++ });
    // Reached again by a closer or earlier path: that one is where it runs.
    else if (rank && had.rank && (rank.d < had.rank.d || (rank.d === had.rank.d && runOrder(rank.at, had.rank.at) < 0))) had.rank = rank;
  };
  const at = (v, line) => ({ d: v.d, at: [...(v.at || []), line || 0] });

  // Outside services, one candidate per node.
  const outside = (nodeId, viaFile, viaLink) => {
    const n = ix.externals.get(nodeId);
    if (!n) return null;
    // One service behind several APIs (`EXAMPLE_API`, `EXAMPLE_MOBILE_API`):
    // the step names the one this code reads, cited where it reads it.
    const envs = (n.env || []).filter((e) => /^[A-Z][A-Z0-9_]{2,}$/.test(e));
    if (envs.length > 1 && viaFile) {
      const lines = r.lines(viaFile) || [];
      const hit = envs.map((e) => [e, lines.findIndex((t) => new RegExp('\\b' + e + '\\b').test(t))]).filter(([, i]) => i >= 0);
      if (hit.length === 1) {
        const [env, i] = hit[0];
        const key = `n:${nodeId}#${env}`;
        if (byKey.has(key)) return byKey.get(key);
        const ev = cite(r, viaFile, i + 1, [env]);
        if (ev) return add({ kind: 'outside', key, view: 'n:' + nodeId, label: `${n.label || nodeId} (${env})`, evidence: [ev] });
      }
    }
    const key = 'n:' + nodeId;
    if (byKey.has(key)) return byKey.get(key);
    const hints = [...(n.packages || []), n.vendor, n.label, nodeId, ...(n.hosts || [])].filter(Boolean);
    let ev = null;
    for (const e of [...(n.evidence || []), ...(viaLink?.evidence || [])]) {
      if (e?.file && !isTest(e.file)) { ev = cite(r, e.file, e.line, [e.token, ...hints]); if (ev) break; }
    }
    if (!ev && viaFile) {
      const lines = r.lines(viaFile) || [];
      const i = lines.findIndex((t) => hints.some((h) => h && t.includes(h)));
      if (i >= 0) ev = cite(r, viaFile, i + 1, hints);
    }
    if (!ev) return null;
    return add({ kind: 'outside', key, view: key, label: n.label || nodeId, evidence: [ev] });
  };

  // Server work behind a route: the files its handler reaches, with
  // constructor injection narrowed to the services the method uses.
  const serverFiles = new Map();
  const behindRoute = (ri) => {
    if (serverFiles.has(ri)) return serverFiles.get(ri);
    const rt = routes[ri];
    const lines = r.lines(rt.f) || [];
    const text = lines.join('\n');
    let start = rt.f;
    let files;
    if (/@Controller\(/.test(text) && rt.l > 1) {
      const a = rt.l - 1;
      let b = lines.length;
      for (let i = a + 1; i < lines.length; i++) if (/^\s*@(Get|Post|Put|Patch|Delete|All|Head|Options)\(/.test(lines[i])) { b = i; break; }
      const body = lines.slice(a, b).join('\n');
      const props = new Set([...body.matchAll(/this\.(\w+)\./g)].map((m) => m[1]));
      // In the controller itself, only the handler method's own lines.
      files = new Map([[start, { d: 0, names: null, at: [], span: [a + 1, b] }]]);
      for (const h of hiddenImports(ix, r, classes, rt.f)) {
        if (!(h.how === 'di' && props.has(h.prop))) continue;
        // The service runs where the handler first calls it.
        const call = lines.findIndex((t, i) => i >= a && i < b && t.includes('this.' + h.prop + '.'));
        for (const [f, v] of closure(ix, r, classes, h.to, { depth: 3, cap: 40 })) if (!files.has(f)) files.set(f, { ...v, d: v.d + 1, at: [call + 1, ...v.at] });
      }
    } else {
      files = closure(ix, r, classes, start, { depth: 5, cap: 150 });
    }
    serverFiles.set(ri, files);
    return files;
  };

  // Requests: Floorplan's matched calls and the detectors' http links.
  const callsByFile = new Map();
  // Where a route's handler is: a Next route file's exported method or a
  // pages/api handler (a route on line 1 is the file, not the code), else
  // the route's own line with its path or name on it.
  const handlerAt = (ri) => {
    const rt = routes[ri];
    const lines = r.lines(rt.f);
    if (!lines) return null;
    const method = (rt.m || 'ANY').toUpperCase();
    if (isRouteHandlerFile(rt.f)) {
      const pats = [`export async function ${method}`, `export function ${method}`, `export const ${method}`, `${method}:`, 'async function handler', 'function handler', 'export default'];
      for (const p of pats) {
        const l = lines.findIndex((t) => t.includes(p) && (!p.endsWith(':') || /^\s*(export\s+)?(const\s+)?\{?\s*\w*\s*[A-Z]+:/.test(t)));
        if (l >= 0) return cite(r, rt.f, l + 1, [p]);
      }
    }
    const tail = segs(rt.p).filter((s) => !/^[:*[]/.test(s)).pop();
    return cite(r, rt.f, rt.l || 1, [tail && `'${tail}'`, tail && `"${tail}"`, tail, rt.h && rt.h !== 'router' ? rt.h : null, method !== 'ANY' ? method : null].filter(Boolean));
  };
  const request = (ri, site, how) => {
    const rt = routes[ri];
    if (!rt || isTest(rt.f) || isTest(site.file)) return null;
    const lines = r.lines(site.file);
    // A call whose path is not written on its line (`signIn('credentials')`)
    // says it itself.
    const lit = site.path ? { raw: site.path, at: site.line, token: site.token } : callPath(lines, site.line, rt);
    const p = lit ? normPath(lit.raw) : rt.p;
    const method = (rt.m || 'ANY').toUpperCase();
    const key = `req:${method} ${p}`;
    const callEv = lit ? { file: site.file, line: lit.at, token: lit.token || lit.raw } : cite(r, site.file, site.line, [rt.p, ...segs(rt.p).reverse()]);
    const handEv = handlerAt(ri);
    // A path only named near a call (an `endpoint:` in a log record) is
    // not where the request is made: a site with a call to an HTTP client
    // around it is.
    const at0 = (callEv?.line || site.line) - 1;
    const real = how !== 'matched' || !lines || lines.slice(Math.max(0, at0 - 8), at0 + 3).some((t) => HTTP_NEAR.test(t));
    const had = byKey.get(key);
    if (had && real && !had.real && callEv) { had.evidence[0] = callEv; had.real = true; }
    const c = add({ kind: 'request', key, view: `r:${ri}`, route: ri, path: p, method, label: `${method} ${p}${rt.name ? ' (' + rt.name + ')' : ''}`, evidence: [callEv, handEv], how, all: /\*|\[\.\.\./.test(rt.p || '') ? rt.f : null, real });
    if (!c) return null;
    if (!callsByFile.has(site.file)) callsByFile.set(site.file, []);
    callsByFile.get(site.file).push({ c, line: callEv?.line || site.line, real });
    return c;
  };
  for (const l of graph.links || []) {
    if (l.k !== 'http') continue;
    const a = strip(l.a);
    if (!a || isTest(a)) continue;
    for (const [ri, line] of l.calls || []) request(ri, { file: a, line }, 'matched');
    for (const e of l.evidence || []) {
      if (!e?.file || isTest(e.file)) continue;
      const label = (l.labels || [])[0] || '';
      const [m, p] = label.split(' ');
      const ri = p ? matchRoute(routes, p, m) : routes.findIndex((rt) => 'f:' + rt.f === l.b);
      if (ri != null && ri >= 0) {
        const c = request(ri, { file: e.file, line: e.line }, e.how || 'code');
        // A call cited where a wrapper or URL map spells it, used from `a`.
        if (c && e.file !== a) {
          if (!callsByFile.has(a)) callsByFile.set(a, []);
          callsByFile.get(a).push({ c, line: null });
        }
      }
    }
  }

  // Calls the map has no link for, in the app's own code: written in the
  // file, or taken from an object of paths another file defines.
  const routeFiles = new Set(routes.map((rt) => rt.f));
  const codeFiles = [...ix.files].filter((f) => CODE.test(f) && !isTest(f) && !routeFiles.has(f) && !/(^|\/)node_modules\//.test(f));
  const maps = new Map();
  for (const f of codeFiles) {
    const t = r.text(f);
    if (!t || !/:\s*['"`]\//.test(t)) continue;
    for (const [name, ent] of r.once('maps', f, () => urlMaps(t))) if (!maps.has(name)) maps.set(name, { file: f, ent });
  }
  const httpCall = new RegExp(HTTP_FN + String.raw`\s*(?:<[^>()]*>)?\(`);
  for (const f of codeFiles) {
    const t = r.text(f);
    if (!t) continue;
    for (const k of r.once('calls', f, () => clientCalls(t))) {
      const ri = matchRoute(routes, k.path, k.method);
      if (ri != null) request(ri, k.how === 'next-auth' ? { file: f, line: k.line, path: k.path, token: k.token } : { file: f, line: k.line }, k.how);
    }
    if (!maps.size || !httpCall.test(t)) continue;
    for (const m of t.matchAll(/\b(\w+)\.(\w+)\b/g)) {
      const mp = maps.get(m[1]);
      const e = mp?.ent[m[2]];
      if (!e) continue;
      const ri = matchRoute(routes, e.path, METHOD_NEAR.exec(t.slice(m.index - 80, m.index + 200))?.slice(1).find(Boolean)?.toUpperCase());
      if (ri == null) continue;
      const c = request(ri, { file: mp.file, line: e.line }, 'url-map');
      if (c && mp.file !== f) {
        if (!callsByFile.has(f)) callsByFile.set(f, []);
        callsByFile.get(f).push({ c, line: t.slice(0, m.index).split('\n').length });
      }
    }
  }

  // Calls through an API client or SDK: requests with no endpoint of ours,
  // to the package's outside service.
  const pkgNode = new Map();
  for (const [id, n] of ix.externals) for (const p of n.packages || []) pkgNode.set(p, id);
  const pkgOutside = (pkg, file) => {
    const id = pkgNode.get(pkg) || [...pkgNode.entries()].find(([p]) => pkg.startsWith(p + '/'))?.[1];
    if (id) return outside(id, file, null);
    const key = 'pkg:' + pkg;
    if (byKey.has(key)) return byKey.get(key);
    const line = findToken(r.lines(file), 1, `'${pkg}'`) || findToken(r.lines(file), 1, `"${pkg}"`);
    const ev = line && cite(r, file, line, [pkg]);
    return ev ? add({ kind: 'outside', key, view: 'f:' + file, label: `${pkg} (the service this package talks to)`, evidence: [ev] }) : null;
  };
  for (const f of codeFiles) {
    const t = r.text(f);
    if (!t || !(/\.call\(/.test(t) || /[A-Z]\w*(?:Session|Client|Agent|Api|API|Sdk|SDK|Auth)\.[a-z]/.test(t))) continue;
    for (const k of r.once('sdk', f, () => sdkCalls(t))) {
      const ev = cite(r, f, k.line, [k.token]);
      if (!ev) continue;
      const kind = k.id.startsWith('xrpc:') ? 'XRPC' : 'SDK';
      const c = add({ kind: 'request', key: `req:${kind} ${k.id}`, view: 'f:' + f, path: k.id, method: kind, label: `${k.id.replace(/^\w+:/, '')} (${kind === 'XRPC' ? 'an XRPC call' : 'an SDK call'})`, evidence: [ev], how: kind.toLowerCase() });
      if (!c) continue;
      if (!callsByFile.has(f)) callsByFile.set(f, []);
      callsByFile.get(f).push({ c, line: ev.line });
      if (k.pkg) link(c, pkgOutside(k.pkg, f), 'it goes to the service behind this package');
    }
  }

  // The Next route-handler hop: a route that calls another service.
  for (const [ri, rt] of routes.entries()) {
    if (!isRouteHandlerFile(rt.f) || isTest(rt.f)) continue;
    const from = [...callsByFile.values()].flat().find((x) => x.c.route === ri)?.c
      || request(ri, { file: rt.f, line: rt.l || 1 }, 'route');
    for (const f of closure(ix, r, classes, rt.f, { depth: 1, cap: 10 }).keys()) {
      for (const h of routeHops(r.text(f))) {
        const p = normPath(h.raw);
        const ti = matchRoute(routes, p, null);
        if (ti == null || ti === ri) continue;
        const to = request(ti, { file: f, line: h.line }, 'route-hop');
        if (from && to) link(from, to, 'the route handler calls it');
      }
    }
  }

  // Requests to outside services, async callbacks behind them.
  const emitters = new Map();
  const consts = new Map();
  for (const f of ix.files) {
    if (!CODE.test(f) || isTest(f)) continue;
    const t = r.text(f);
    if (!t || !/\.(emit|on|subscribe)\(|@OnEvent|=\s*['"][\w.:-]+['"]/.test(t)) continue;
    for (const m of t.matchAll(/export\s+const\s+([A-Z][A-Z0-9_]+)\s*=\s*['"]([\w.:-]+)['"]/g)) consts.set(m[1], m[2]);
    const ev = eventSites(t);
    for (const e of ev.emits) {
      if (!emitters.has(f)) emitters.set(f, []);
      emitters.get(f).push(e);
    }
  }
  const listeners = new Map();
  for (const f of ix.files) {
    if (!CODE.test(f) || isTest(f)) continue;
    const t = r.text(f);
    if (!t || !/@OnEvent|\.on\(|\.subscribe\(|\.addListener\(/.test(t)) continue;
    for (const e of eventSites(t).listens) {
      const name = consts.get(e.name) || e.name;
      if (!listeners.has(name)) listeners.set(name, []);
      listeners.get(name).push({ file: f, ...e });
    }
  }
  const asyncEvent = (name) => {
    const at = (listeners.get(name) || [])[0];
    if (!at) return null;
    const ev = cite(r, at.file, at.line, [at.token]);
    return ev && add({ kind: 'async', key: `event:${name}`, view: 'f:' + at.file, label: `event ${name}, handled by ${path.posix.basename(at.file)}`, evidence: [ev] });
  };
  const webhookRoutes = [];
  routes.forEach((rt, i) => { if (!isTest(rt.f) && (WEBHOOK_PATH.test(rt.p || '') || /webhook/i.test(rt.f))) webhookRoutes.push(i); });
  const asyncRoute = (ri) => {
    const rt = routes[ri];
    const tail = segs(rt.p).filter((s) => !/^[:*[]/.test(s)).pop();
    const ev = cite(r, rt.f, rt.l || 1, [tail && `'${tail}'`, tail && `'/${segs(rt.p).slice(-2).join('/')}'`, tail, rt.m]);
    return ev && add({ kind: 'async', key: `hook:${(rt.m || 'ANY').toUpperCase()} ${rt.p}`, view: `r:${ri}`, label: `${(rt.m || '').toUpperCase()} ${rt.p} (called back by an outside service)`, evidence: [ev] });
  };
  const vendorWords = (n) => [n.vendor, n.id, n.label, ...(n.packages || [])].filter(Boolean).map((w) => String(w).toLowerCase().replace(/^@/, '').split(/[/\s_-]/)[0]).filter((w) => w.length >= 3);
  const hooksFor = new Map();
  for (const [id, n] of ix.externals) {
    const words = vendorWords(n);
    hooksFor.set(id, webhookRoutes.filter((ri) => words.some((w) => (routes[ri].p + ' ' + routes[ri].f).toLowerCase().includes(w))));
  }

  const usedAt = (f, v, line) => (v.span ? line >= v.span[0] && line <= v.span[1] : inUse(ix, r, f, v.names, line));
  for (const c of [...list]) {
    if (c.kind !== 'request' || c.route == null) continue;
    const files = behindRoute(c.route);
    for (const [f, v] of files) {
      for (const x of ix.ext.get(f) || []) {
        const el = x.link?.evidence?.[0]?.line;
        if (v.span && el && !usedAt(f, v, el)) continue;
        const o = outside(x.node, f, x.link);
        if (!o) continue;
        link(c, o, 'its handler uses it', at(v, x.link?.evidence?.[0]?.line));
        for (const hi of hooksFor.get(x.node) || []) link(o, asyncRoute(hi), 'calls back');
      }
      for (const e of emitters.get(f) || []) {
        if (!usedAt(f, v, e.line)) continue;
        link(c, asyncEvent(consts.get(e.name) || e.name), 'its handler emits the event', at(v, e.line));
      }
      for (const h of r.once('helpers', f, () => helperCalls(r.text(f)))) {
        if (!usedAt(f, v, h.line)) continue;
        const ev = cite(r, f, h.line, [h.token]);
        link(c, ev && add({ kind: 'outside', key: `helper:${f}#${h.name}`, view: 'f:' + f, label: `${h.name} (an integration its handler calls)`, evidence: [ev] }), 'its handler calls it', at(v, h.line));
      }
      for (const h of r.once('hooks', f, () => outgoingHooks(r.text(f)))) {
        if (!usedAt(f, v, h.line)) continue;
        const ev = cite(r, f, h.line, [h.token]);
        link(c, ev && add({ kind: 'async', key: `out:${f}#${h.token}`, view: 'f:' + f, label: `${h.token} (webhooks sent out)`, evidence: [ev] }), 'its handler sends webhooks', at(v, h.line));
      }
    }
  }
  // A webhook endpoint whose handler works with a service: that service
  // calls it back.
  for (const ri of webhookRoutes) {
    const a = asyncRoute(ri);
    if (!a) continue;
    for (const [f] of behindRoute(ri)) for (const x of ix.ext.get(f) || []) link(outside(x.node, f, x.link), a, 'calls back');
  }

  // Screens: the map's own, then steps inside a page and form components
  // no router registers.
  const screens = [];
  const screenOf = new Map();
  const screenAt = (s) => {
    const file = s.file;
    if (!file || isTest(file)) return null;
    let ev = null;
    const navEv = s.nav?.evidence?.[0] || s.nav?.evidence;
    const name = s.nav?.name || s.name || segs(s.url || '').pop() || '';
    if (navEv?.file) ev = cite(r, navEv.file, navEv.line, [navEv.token, `"${segs(s.url || '').pop()}"`, `'${segs(s.url || '').pop()}'`, segs(s.url || '').pop()].filter(Boolean));
    if (!ev) {
      const sy = (ix.syms.get(file) || []).find((x) => x.kind === 'default') || (ix.syms.get(file) || []).find((x) => /^[A-Z]/.test(x.name));
      if (sy) ev = cite(r, file, sy.line, [sy.name !== 'default' ? sy.name : null, 'export default'].filter(Boolean));
    }
    if (!ev) {
      const l = findToken(r.lines(file), 1, 'export default');
      if (l) ev = cite(r, file, l, ['export default']);
    }
    if (!ev) return null;
    const gate = s.nav?.gate ? `, ${s.nav.gate.replace('_', ' ')}` : '';
    const where = s.nav?.navigator ? `, ${s.nav.navigator}` : '';
    const c = add({ kind: 'screen', key: s.id, view: 'f:' + file, file, app: s.app, name, url: s.url, label: `${s.app ? s.app + ' ' : ''}${s.url || name}${where}${gate}`, evidence: [ev] });
    if (c) { screens.push(c); if (!screenOf.has(file)) screenOf.set(file, []); screenOf.get(file).push(c); }
    return c;
  };
  for (const s of graph.screens || []) screenAt(s);

  const stepScreens = new Map();
  const partScreen = (file, comp, ev, parent, step) => {
    const key = `part:${file}#${comp}`;
    if (byKey.has(key)) return byKey.get(key);
    const c = add({ kind: 'screen', key, view: 'f:' + file, file, app: parent?.app, name: comp, label: `${comp}${parent ? ' (step ' + (step || '') + ' of ' + (parent.url || parent.name) + ')' : ' (a form no router registers)'}`, evidence: [ev], part: true });
    if (c) { screens.push(c); if (!screenOf.has(file)) screenOf.set(file, []); screenOf.get(file).push(c); }
    return c;
  };

  // The requests a page runs through the functions it hands a component on
  // `line`: in the handler's own statements first, then down the code
  // each statement runs. Each handler is a way through the step of its
  // own (submit, sign up instead), so its requests keep together, in the
  // order its statements make them.
  const fileClosures = new Map();
  const closureOf = (f) => {
    if (!fileClosures.has(f)) fileClosures.set(f, closure(ix, r, classes, f, { depth: 4, cap: 80 }));
    return fileClosures.get(f);
  };
  const handed = (f, line, to) => {
    const lines = r.lines(f) || [];
    const own = r.lines(to) || [];
    const out = [];
    for (const { prop, fn } of propHandlers(lines, line)) {
      const span = bodyRange(lines, fn);
      if (!span) continue;
      const [a, b] = span;
      // The step runs the handler where its code calls the prop, so the
      // handler's requests come in there among the step's own.
      const called = own.findIndex((t) => new RegExp('\\b(?:props\\.)?' + esc(prop) + '\\s*(?:\\?\\.)?\\(').test(t)) + 1;
      const inSpan = (k) => k >= a && k <= b;
      for (const x of callsByFile.get(f) || []) if (made(x) && x.line != null && inSpan(x.line)) out.push({ c: x.c, rank: { d: 0, at: [called, a, x.line] } });
      const byFile = new Map();
      for (const e of ix.imports.get(f) || []) {
        if (isTest(e.to) || NOISE_DIR.test('/' + e.to)) continue;
        if (!byFile.has(e.to)) byFile.set(e.to, []);
        byFile.get(e.to).push(...(e.names || []));
      }
      for (const [h, imported] of byFile) {
        const names = runNames(lines, imported);
        if (!names.length) continue;
        const alt = names.map(esc).join('|');
        const re = new RegExp('\\b(?:' + alt + ')\\s*(?:<[^>()]*>)?\\(|<(?:' + alt + ')\\b');
        const k = lines.findIndex((t, n) => inSpan(n + 1) && re.test(t));
        if (k < 0) continue;
        for (const [g, w] of closureOf(h)) {
          for (const x of callsByFile.get(g) || []) {
            if (!made(x) || x.line == null || !inUse(ix, r, g, g === h ? new Set(names) : w.names, x.line)) continue;
            out.push({ c: x.c, rank: { d: 0, at: [called, a, k + 1, ...w.at, x.line] } });
          }
        }
      }
    }
    return out;
  };
  // A site that only names a request another site makes for real.
  const made = (x) => x.real !== false || !x.c.real;
  // Per-screen closures, with the steps inside each page.
  const done = new Set();
  const visit = (s) => {
    if (done.has(s.id)) return;
    done.add(s.id);
    const files = closure(ix, r, classes, s.file);
    for (const [f, v] of files) {
      for (const x of callsByFile.get(f) || []) if (made(x) && (x.line == null || inUse(ix, r, f, v.names, x.line))) link(s, x.c, 'the screen makes this request', at(v, x.line));
      for (const x of ix.ext.get(f) || []) {
        if (routes.some((rt) => rt.f === f)) continue;
        link(s, outside(x.node, f, x.link), 'the screen uses it', at(v, x.link?.evidence?.[0]?.line));
      }
      // Step machines sit a few files down (page, then layout, then drawer).
      if (v.d > 3) continue;
      const t = r.text(f);
      // Steps inside the page.
      const steps = r.once('steps', f, () => pageSteps(t));
      const names = new Map((ix.imports.get(f) || []).flatMap((e) => (e.names || []).map((n) => [n, e.to])));
      // The page's own requests (its code and what it uses, the step
      // components left out) belong to each of its steps.
      let own = null;
      const ownRequests = () => {
        if (own) return own;
        const stepFiles = new Set(steps.map((x) => names.get(x.comp)).filter(Boolean));
        own = [];
        for (const [g, w] of closure(ix, r, classes, f, { skip: stepFiles })) {
          for (const x of callsByFile.get(g) || []) if (made(x) && (x.line == null || inUse(ix, r, g, w.names, x.line))) own.push({ c: x.c, rank: at(w, x.line) });
        }
        return own;
      };
      let prev = null;
      for (const st of steps) {
        if (!st.comp) {
          const target = [...stepScreens.values()].find((x) => x.step === st.step && x.parentFile === f);
          if (target) link(prev || s, target.c, 'goes to this step');
          continue;
        }
        // A step is a component of the app's own (not a text or layout
        // component from a package).
        const to = names.get(st.comp);
        if (!to) continue;
        // The step is cited where its component is declared; where the page
        // renders it is the fallback.
        const decl = (ix.syms.get(to) || []).find((x) => x.name === st.comp);
        const ev = (decl && cite(r, to, decl.line, [st.comp])) || cite(r, f, st.line, [st.token]);
        if (!ev) continue;
        const c = partScreen(to, st.comp, ev, s, st.step);
        if (!c) continue;
        stepScreens.set(c.id, { c, step: st.step, parentFile: f });
        link(s, c, 'a step inside the page', at(v, st.line));
        if (prev) link(prev, c, 'the next step');
        prev = c;
        // What the page hands the step to run (its submit handler): those
        // requests are the step's own, in the order the handler makes them.
        for (const q of handed(f, st.line, to)) link(c, q.c, 'the step runs this request', q.rank);
        for (const q of ownRequests()) link(c, q.c, 'the page makes this request for the step', q.rank);
        visit(c);
      }
      // Forms the screen renders that no router registers.
      for (const e of ix.imports.get(f) || []) {
        if (!/\.(tsx|jsx)$/.test(e.to) || isTest(e.to) || screenOf.has(e.to)) continue;
        const base = path.posix.basename(e.to).replace(/\.\w+$/, '');
        const comp = (e.names || []).find((n) => /^[A-Z]/.test(n)) || base;
        if (!/(Form|Step\w*|Modal|Drawer|Sheet|Page|View|Screen)$/.test(comp) && !/(form|step)/i.test(base)) continue;
        if (!Object.values(WORDS).some((w) => w.test(comp) || w.test(base))) continue;
        const sy = (ix.syms.get(e.to) || []).find((x) => x.name === comp) || (ix.syms.get(e.to) || [])[0];
        const ev = sy && cite(r, e.to, sy.line, [comp]);
        if (!ev) continue;
        const c = partScreen(e.to, comp, ev, null, null);
        if (c) { link(s, c, 'the screen shows this form'); visit(c); }
      }
    }
    // Navigation written in the screen's own files.
    for (const [f, v] of files) {
      if (v.d > 1) continue;
      for (const n of r.once('nav', f, () => navSites(r.text(f)))) {
        const t = screens.find((x) => x !== s && !x.part && (n.kind === 'name' ? x.name === n.target || segs(x.url || '').pop() === n.target : x.url && urlMatch(n.target, x.url)));
        if (t) link(s, t, 'goes to this screen');
      }
    }
  };
  for (const s of [...screens]) visit(s);

  // A component that switches between journey forms (a sign-up drawer, a
  // checkout sheet) mounted from a layout or provider no page imports: it
  // is a screen of its own.
  const seenFiles = new Set(screens.map((x) => x.file));
  for (const f of ix.files) {
    if (!/\.(tsx|jsx)$/.test(f) || isTest(f) || seenFiles.has(f)) continue;
    const t = r.text(f);
    if (!t || !/<[A-Z]/.test(t) || !/(===|case\s)/.test(t)) continue;
    const steps = r.once('steps', f, () => pageSteps(t)).filter((x) => x.comp);
    const names = new Map((ix.imports.get(f) || []).flatMap((e) => (e.names || []).map((n) => [n, e.to])));
    const forms = steps.filter((x) => names.has(x.comp) && Object.values(WORDS).some((w) => w.test(x.comp) || w.test(x.step)));
    if (forms.length < 2) continue;
    const sy = (ix.syms.get(f) || []).find((x) => /^[A-Z]/.test(x.name) && (x.kind === 'default' || x.kind === 'function' || x.kind === 'const'));
    const name = sy?.name || path.posix.basename(f).replace(/\.\w+$/, '');
    const ev = sy && cite(r, f, sy.line, [name]);
    if (!ev) continue;
    const host = partScreen(f, name, ev, null, null);
    if (host) visit(host);
  }

  // Navigation the detectors found.
  const screenRef = (ref) => {
    if (typeof ref !== 'string') return [];
    if (ref.startsWith('screen:')) return byKey.has(ref) ? [byKey.get(ref)] : [];
    const f = strip(ref);
    return f ? screenOf.get(f) || [] : [];
  };
  for (const l of graph.links || []) {
    if (l.k !== 'nav') continue;
    for (const a of screenRef(l.a)) for (const b of screenRef(l.b)) link(a, b, 'goes to this screen');
  }
  return { list, edges, byKey };
}

// ── Journeys: entries, subgraphs and their hashes ─────────────────────────

/** The family a name belongs to (sign-in, ...), or null. */
export function family(text) {
  for (const [f, re] of Object.entries(WORDS)) if (re.test(text || '')) return f;
  return null;
}

/** Screens a journey can start at: a screen whose name or address says
 * what the journey is. */
export function entries(c) {
  return c.list.filter((x) => x.kind === 'screen' && family(`${x.name} ${x.url || ''} ${path.posix.basename(x.file || '')}`));
}

/** Entries in turn across families (sign in, sign up, ...), routed screens
 * before forms within each. */
export function fairOrder(list) {
  const fam = new Map();
  for (const e of list) {
    const f = family(`${e.name} ${e.url || ''} ${path.posix.basename(e.file || '')}`) || '?';
    if (!fam.has(f)) fam.set(f, []);
    fam.get(f).push(e);
  }
  for (const l of fam.values()) l.sort((a, b) => Number(!!a.part) - Number(!!b.part));
  const out = [];
  for (let i = 0; out.length < list.length; i++) for (const l of fam.values()) if (l[i]) out.push(l[i]);
  return out;
}

/** The candidates around an entry: screens within 2 navigation hops and
 * everything their requests reach. */
export function subgraph(c, entry, hops = 2) {
  const keep = new Set([entry.id]);
  let ring = [entry.id];
  for (let hop = 0; hop < hops; hop++) {
    const next = [];
    for (const id of ring) for (const [b] of c.edges.get(id) || []) {
      const x = byIdOf(c).get(b);
      if (x?.kind === 'screen' && !keep.has(b)) { keep.add(b); next.push(b); }
    }
    ring = next;
  }
  const grow = [...keep];
  while (grow.length) {
    const id = grow.pop();
    for (const [b] of c.edges.get(id) || []) {
      const x = byIdOf(c).get(b);
      if (x && x.kind !== 'screen' && !keep.has(b)) { keep.add(b); grow.push(b); }
    }
  }
  return keep;
}

/** What a journey stands on, without line numbers: a moved line costs
 * nothing, a changed call or a new step does. */
export function hashOf(c, ids) {
  const byId = byIdOf(c);
  const keys = [...ids].map((id) => byId.get(id)).filter(Boolean).map((x) => `${x.key}|${x.evidence.map((e) => e.file + '#' + e.token).join(',')}`).sort();
  const out = [];
  for (const id of ids) for (const [b] of c.edges.get(id) || []) if (ids.has(b)) out.push(`${byId.get(id)?.key}>${byId.get(b)?.key}`);
  return sha(keys.join('\n') + '\n--\n' + out.sort().join('\n'));
}

// ── The list the model reads ──────────────────────────────────────────────

const BYID = new WeakMap();
function byIdOf(c) {
  if (!BYID.has(c)) BYID.set(c, new Map(c.list.map((x) => [x.id, x])));
  return BYID.get(c);
}

/** One line per candidate, ids first: what the model picks from. */
export function listText(c, ids) {
  const byId = byIdOf(c);
  const order = { screen: 0, request: 1, outside: 2, async: 3 };
  const shown = [...ids].map((id) => byId.get(id)).filter(Boolean).sort((a, b) => order[a.kind] - order[b.kind] || Number(a.id.slice(1)) - Number(b.id.slice(1)));
  const lines = [];
  for (const x of shown) {
    const to = [...(c.edges.get(x.id) || [])].filter(([b]) => ids.has(b));
    const grp = (k) => to.filter(([b]) => byId.get(b)?.kind === k).map(([b]) => b);
    // Where it is, briefly (the checker holds the exact lines).
    const ev = [...new Set(x.evidence.map((e) => e.file.split('/').slice(-2).join('/')))].join(' then ');
    let l = `${x.id} ${x.kind} ${x.label} @ ${ev}`;
    if (grp('screen').length) l += ` next[${grp('screen').join(',')}]`;
    if (grp('request').length) l += ` requests[${grp('request').join(',')}]`;
    if (grp('outside').length) l += ` outside[${grp('outside').join(',')}]`;
    if (grp('async').length) l += ` async[${grp('async').join(',')}]`;
    lines.push(l);
  }
  return lines.join('\n');
}

/** The ids shown to the model for these entries, within MAX_LIST (each
 * candidate's line measured once, with all its edges: a bound). */
export function shownFor(c, entryList, max = MAX_LIST, hops = 2) {
  const ids = new Set();
  const size = new Map();
  const cost = (id) => {
    if (!size.has(id)) size.set(id, listText(c, new Set([id, ...(c.edges.get(id)?.keys() || [])])).split('\n').find((l) => l.startsWith(id + ' '))?.length + 1 || 0);
    return size.get(id);
  };
  let total = 0;
  for (const e of entryList) {
    const add = [...subgraph(c, e, hops)].filter((id) => !ids.has(id));
    const more = add.reduce((n, id) => n + cost(id), 0);
    if (ids.size && total + more > max) break;
    for (const id of add) ids.add(id);
    total += more;
  }
  return ids;
}

// ── Stored journeys, re-checked on every build ────────────────────────────

/** Re-checks stored journeys against this build's code and candidates.
 * Returns the journeys kept (re-lined), the ones dropped with why, and the
 * entries whose subgraph changed. */
export function revalidate(store, c, read) {
  const kept = [];
  const dropped = [];
  const changed = [];
  for (const fl of store?.flows || []) {
    const steps = [];
    let gone = 0;
    for (const st of fl.steps || []) {
      const ev = [];
      let ok = true;
      for (const e of st.evidence || []) {
        const l = findToken(read.lines(e.file), e.line, e.token);
        if (!l) { ok = false; break; }
        ev.push({ ...e, line: l });
      }
      const cand = c.byKey.get(st.key);
      if (!ok || !ev.length || !cand) { gone += 1; continue; }
      steps.push({ ...st, evidence: ev, view: cand.view });
    }
    const total = (fl.steps || []).length || 1;
    const entryOk = c.byKey.has(fl.entry) && steps[0]?.key === fl.entry;
    let why = null;
    if (!entryOk) why = 'its first screen is gone';
    else if (gone / total >= 0.3) why = `${gone} of ${total} steps are gone`;
    else if (steps.length < 3) why = 'fewer than 3 steps are left';
    if (why) { dropped.push({ id: fl.id, title: fl.title, entry: fl.entry, why }); continue; }
    const entry = c.byKey.get(fl.entry);
    const hash = hashOf(c, subgraph(c, entry));
    if (hash !== fl.hash) changed.push(fl.entry);
    kept.push({ ...fl, steps });
  }
  return { kept, dropped, changed };
}

/** A stored journey as the viewer draws it (`graph.flows`). */
export function forViewer(fl) {
  const steps = [];
  const linked = [];
  for (const s of fl.steps) {
    if (!s.view) continue;
    const evidence = (s.evidence || []).map((e) => ({ file: e.file, line: e.line }));
    if (steps.length && steps[steps.length - 1].ref === s.view) {
      const last = steps[steps.length - 1];
      last.note += ` Then ${s.note.charAt(0).toLowerCase()}${s.note.slice(1)}`;
      last.evidence.push(...evidence);
      continue;
    }
    steps.push({ ref: s.view, note: s.note, evidence });
    linked.push(s.linked !== false);
  }
  return { id: fl.id, title: fl.title, description: fl.description || '', steps, custom: true, coderhelm: { entry: fl.entry, linked } };
}

/** The candidate table the worker's checker reads: per id, the step it
 * becomes, and the cited lines as they are now (each line plus or minus
 * 3), so the check doesn't need the code again. */
export function table(c, ids, read) {
  const out = {};
  for (const x of c.list) {
    if (!ids.has(x.id)) continue;
    out[x.id] = {
      key: x.key,
      kind: x.kind,
      view: x.view,
      label: x.label,
      evidence: x.evidence.map((e) => {
        const lines = read.lines(e.file) || [];
        return { ...e, window: lines.slice(Math.max(0, e.line - 4), e.line + 3).map((t) => t.slice(0, 400)), from: Math.max(1, e.line - 3) };
      }),
      // In the order the code runs them (runOrder), then the order they
      // were found.
      next: [...(c.edges.get(x.id) || [])].filter(([b]) => ids.has(b)).sort(([, a], [, b]) => (a.rank && b.rank ? runOrder(a.rank.at, b.rank.at) : 0) || a.i - b.i).map(([b]) => b),
      // How deep in the code behind it each outside service or callback
      // is (0: its own file): the request whose handler calls a service
      // most directly is the one the service step goes with.
      near: Object.fromEntries([...(c.edges.get(x.id) || [])].filter(([b, e]) => ids.has(b) && e.rank && /^[XA]/.test(b)).map(([b, e]) => [b, e.rank.d])),
      // A route that takes every path under it (`/api/*`, a proxy): the
      // file that forwards them (the requests through it all reach what it
      // forwards to).
      ...(x.kind === 'request' && x.all ? { all: x.all } : {}),
      // Requests a step has only because its page makes them: they don't
      // place the request under this step.
      weak: [...(c.edges.get(x.id) || [])].filter(([b, e]) => ids.has(b) && e.why === 'the page makes this request for the step').map(([b]) => b),
    };
  }
  return out;
}

/** The driver's step: re-check what is stored, list the candidates, and
 * say what the model must write. Returns { store, flows (for the graph),
 * todo (null when the model need not run), report }. */
export function prepare(root, graph, prev) {
  const read = reader(root);
  const c = candidates(graph, read);
  const { kept, dropped, changed } = revalidate(prev, c, read);
  const asked = { ...(prev?.asked || {}) };
  const ent = entries(c);
  const hashes = new Map(ent.map((e) => [e.key, hashOf(c, subgraph(c, e))]));
  // Entries the model has not seen in this shape: new ones, ones whose
  // subgraph changed, and the entries of dropped journeys.
  const redo = new Set(changed);
  for (const d of dropped) if (hashes.has(d.entry)) redo.add(d.entry);
  for (const e of ent) if (asked[e.key] !== hashes.get(e.key)) redo.add(e.key);
  // Every kind of journey gets its share of the list: entries taken in
  // turn across families, routed screens before forms.
  const todoEntries = fairOrder(ent.filter((e) => redo.has(e.key)));
  let todo = null;
  if (todoEntries.length) {
    // One model call per journey family per app, each with only the
    // candidates around that family's entries (and its own list cap).
    const fam = (e) => family(`${e.name} ${e.url || ''} ${path.posix.basename(e.file || '')}`) || 'other';
    const appOf = (e) => e.app || (e.file || '').split('/')[0];
    const groups = new Map();
    for (const e of todoEntries) {
      const k = `${fam(e)}\0${appOf(e)}`;
      if (!groups.has(k)) groups.set(k, { family: fam(e), app: appOf(e), entries: [] });
      groups.get(k).entries.push(e);
    }
    const all = new Set();
    const out = [];
    for (const g of groups.values()) {
      // One journey family: the screens one hop away are enough (a form's
      // steps and where it lands), which keeps each call small.
      const ids = shownFor(c, g.entries, MAX_GROUP_LIST, 1);
      if (!ids.size) continue;
      for (const id of ids) all.add(id);
      out.push({ family: g.family, app: g.app, list: listText(c, ids), entries: g.entries.filter((e) => ids.has(e.id)).map((e) => e.id) });
    }
    todo = {
      groups: out,
      table: table(c, all, read),
      entries: todoEntries.filter((e) => all.has(e.id)).map((e) => ({ id: e.id, key: e.key, hash: hashes.get(e.key), family: fam(e), app: appOf(e) })),
      keep: kept.filter((f) => !redo.has(f.entry)).map((f) => ({ id: f.id, title: f.title })),
    };
  }
  const store = { schema: SCHEMA, flows: kept, asked: Object.fromEntries([...Object.entries(asked)].filter(([k]) => hashes.has(k))) };
  return {
    store,
    flows: kept.map(forViewer),
    todo,
    report: { candidates: c.list.length, entries: ent.length, kept: kept.length, dropped, redo: todoEntries.length },
  };
}
