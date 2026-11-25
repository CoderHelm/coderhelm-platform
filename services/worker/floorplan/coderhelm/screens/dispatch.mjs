// Dispatched screens. A dynamic or catch-all route (Next.js app/[...slug]/page and pages/[...slug],
// Remix and React Router flat routes ($.tsx, blog.$slug.tsx), SvelteKit [...rest]/+page, Nuxt and
// Astro pages/[...slug], TanStack and Solid routes, or any route the map already has whose URL ends
// in a parameter or a splat) often renders one of many components picked at run time from the type
// a resolver gave the URL: an object or Map from type to component, a switch on the type, or an if
// chain on it. Each of those components is a screen of its own; the route file alone shows one.
//
// For each such route this finds the dispatch (in the route file or a few imports down), the
// component each type renders (followed through imports, re-exports, barrels, lazy and dynamic
// imports to where it is declared) and, where the code says it, the URL pattern the resolver gives
// that type: an if chain on the URL's segments that returns the type, or a table or switch from
// type to path template or prefix. Without one, the URL is the route's own pattern with the type
// named as a group: /(berth)/*.
//
// Tables of blocks or sections (parts of a page, not pages) are kept apart.
//
// detect(root, { routes }) -> { dispatchers: [{ file, route, base, screens: [...], sections: [...] }] }
import path from 'node:path';
import { listFiles, read, stripComments, closeOf, objectEntries, lineIndex, makeResolver, isTest } from '../lib/text.mjs';

const P = path.posix;
const CODE = /\.(tsx?|jsx?|mjs|cjs|vue|astro|svelte|mdx)$/;
const ROUTE_ROOT = /(?<=^|\/)(?:app|pages|routes|src\/routes|src\/pages)\//g;
const TYPEY = /(_type|\btype|Type|blockType|__typename|\bkind|Kind|component|contentType|template|layout|variant|model)\b/;
const SECTIONY = /block|section|module|slice|widget|component(s)?Map|layout|builder|portable|serializer/i;
const ID = '[A-Za-z_$][\\w$]*';
const LIT = /^(["'`])([^"'`]{1,80})\1$/;

export const norm = (n) => String(n).toLowerCase().replace(/[^a-z0-9]/g, '');

/** A route file's URL, from its path in a file-based router; null when it is not one, or not dynamic. */
export function routeOf(file) {
  if (!CODE.test(file) || /(^|\/)api\//.test(file) || isTest(file)) return null;
  let at = -1, m;
  ROUTE_ROOT.lastIndex = 0;
  while ((m = ROUTE_ROOT.exec(file))) at = m.index + m[0].length;
  if (at < 0) return null;
  const rest = file.slice(at).replace(CODE, '').split('/');
  const leaf = rest[rest.length - 1];
  if (/^(page|index|\+page|route|_index)$/.test(leaf)) rest.pop();
  else if (/^(layout|_layout|\+layout|error|loading|not-found|template|default|_app|_document|root|\+error|\+server)$/.test(leaf) || /^\+/.test(leaf)) return null;
  const segs = [];
  for (const part of rest) {
    // flat routes (Remix, React Router, TanStack): dots split segments, outside brackets
    const pieces = !/^\[.*\]$/.test(part) && /[$.]/.test(part.replace(/\[[^\]]*\]/g, '')) ? part.split(/\.(?![^[]*\])/) : [part];
    for (const p of pieces) {
      if (!p || /^\(.*\)$/.test(p) || /^@/.test(p) || /^_(?!index)/.test(p) || p === '_index') continue;
      if (/^\[\[?\.\.\.[^\]]*\]\]?$/.test(p) || p === '$' || p === '*') segs.push('*');
      else if (/^\[[^\]]+\]$/.test(p)) segs.push(':' + p.slice(1, -1));
      else if (/^\$\w+$/.test(p)) segs.push(':' + p.slice(1));
      else segs.push(p);
    }
  }
  if (!segs.length) return null;
  const last = segs[segs.length - 1];
  if (last !== '*' && !last.startsWith(':')) return null;
  return { url: '/' + segs.join('/'), base: segs.length > 1 ? '/' + segs.slice(0, -1).join('/') : '', last };
}

/** A route the map already has (any router Floorplan reads) whose URL ends in a parameter or a splat. */
function routeOfUrl(url) {
  const segs = String(url || '').split('/').filter(Boolean);
  const last = segs[segs.length - 1];
  if (!last || !(last === '*' || /^[:[]/.test(last) || /^\*/.test(last))) return null;
  const tail = last.startsWith('[') || last.startsWith('*') || last === '*' ? (last.startsWith('[...') || last.startsWith('*') ? '*' : ':' + last.replace(/^\[|\]$/g, '')) : last;
  return { url: '/' + [...segs.slice(0, -1), tail].join('/'), base: segs.length > 1 ? '/' + segs.slice(0, -1).join('/') : '', last: tail };
}

/** "BoatCheckoutRenderer" or "boat-checkout" in plain words: "Boat checkout". */
export function plainName(raw, { fromType = false } = {}) {
  let s = String(raw || '').replace(/^(?:handle|render)(?=[A-Z])/, '');
  if (!fromType) s = s.replace(/(Renderer|Render|Component|Template|Container|View|Screen|Page|Route|Handler|Layout)$/, '') || s;
  const words = s.replace(/([a-z0-9])([A-Z])/g, '$1 $2').replace(/([A-Z]+)([A-Z][a-z])/g, '$1 $2').split(/[\s_\-.]+/).filter(Boolean).map((w) => w.toLowerCase());
  if (!words.length) return 'Page';
  if (words[words.length - 1] === 'index' && words.length > 1) words[words.length - 1] = 'list';
  if (words.length === 1) words.push('page');
  if (words[0] === 'page' && words.length === 2) return 'Content page';
  const out = words.join(' ').replace(/\bpage page$/, 'page');
  return out.charAt(0).toUpperCase() + out.slice(1);
}

/** The text between balanced brackets from `open`, or null. */
const inside = (s, open) => { const c = closeOf(s, open); return c < 0 ? null : [open, c]; };

export function detect(root, { routes = [], maxDepth = 3, maxFiles = 40000 } = {}) {
  const t0 = Date.now();
  const all = listFiles(root, { exts: CODE, max: maxFiles });
  const fileSet = new Set(all);
  const cache = new Map();
  const get = (f) => { if (!cache.has(f)) cache.set(f, stripComments(read(root, f) || '')); return cache.get(f); };
  const resolveSpec = makeResolver(root, all);
  // a spec with its extension (`./Post.vue`) as well as without
  const resolve = (from, spec) => resolveSpec(from, spec) || (CODE.test(spec) ? resolveSpec(from, spec.replace(CODE, '')) : null);

  // the dynamic routes: file-based routers, then any route the map has whose URL ends in a parameter
  const entries = new Map();
  for (const f of all) { const r = routeOf(f); if (r) entries.set(f, r); }
  for (const r of routes) { if (!r?.file || !fileSet.has(r.file) || entries.has(r.file)) continue; const x = routeOfUrl(r.url); if (x) entries.set(r.file, x); }

  // string constants, for keys and case labels written as PAGE_KINDS.BERTH or BERTH_KIND
  const constsOf = new Map();
  const consts = (f) => {
    if (constsOf.has(f)) return constsOf.get(f);
    const s = get(f); const out = new Map();
    for (const m of s.matchAll(new RegExp(`\\b(?:const|let|var)\\s+(${ID})\\s*(?::[^=]{1,80})?=\\s*(["'\`])([^"'\`]{1,80})\\2`, 'g'))) out.set(m[1], m[3]);
    for (const m of s.matchAll(new RegExp(`\\b(?:const|let|var)\\s+(${ID})\\s*(?::[^=]{1,80})?=\\s*\\{`, 'g'))) {
      const open = s.indexOf('{', m.index + m[0].length - 1);
      for (const e of objectEntries(s, open)) { const v = LIT.exec(e.value || ''); if (e.key && v) out.set(`${m[1]}.${e.key}`, v[2]); }
    }
    for (const m of s.matchAll(new RegExp(`\\benum\\s+(${ID})\\s*\\{([^}]*)\\}`, 'g'))) for (const x of m[2].matchAll(new RegExp(`(${ID})\\s*=\\s*(["'])([^"']+)\\2`, 'g'))) out.set(`${m[1]}.${x[1]}`, x[3]);
    constsOf.set(f, out);
    return out;
  };
  /** A key or label as text: 'berth', "berth", PAGE_KINDS.BERTH (through the files the dispatch reads). */
  const keyText = (raw, files) => {
    const t = String(raw || '').trim().replace(/\s+as\s+const$/, '');
    const lit = LIT.exec(t);
    if (lit) return lit[2];
    if (/^[\w-]+$/.test(t) && !/^[A-Z_]+$/.test(t)) return t; // a plain object key
    for (const f of files) { const v = consts(f).get(t); if (v) return v; }
    // a constant from an import of these files
    for (const f of files) for (const m of get(f).matchAll(/(?:import|from)\s*\(?\s*["']([^"']+)["']/g)) { const g = resolve(f, m[1]); const v = g && consts(g).get(t); if (v) return v; }
    return /^[\w-]+$/.test(t) ? t : null;
  };

  // where a component is declared, followed through imports, re-exports and lazy imports
  const declCache = new Map();
  const declIn = (f, name, depth = 0) => {
    const key = `${f}#${name}`;
    if (declCache.has(key)) return declCache.get(key);
    declCache.set(key, null);
    const s = get(f); const L = lineIndex(s);
    let out = null;
    const own = name === 'default'
      ? /\bexport\s+default\s+(?:async\s+)?(?:function|class)?\s*([A-Za-z_$][\w$]*)?/.exec(s)
      : new RegExp(`(?:^|[^\\w$.])(?:export\\s+)?(?:default\\s+)?(?:async\\s+)?(?:function\\s*\\*?\\s*|class\\s+|const\\s+|let\\s+|var\\s+)${name.replace(/\$/g, '\\$')}\\b`).exec(s);
    if (own) {
      const at = own.index + Math.max(0, own[0].search(/(?:export|default|async|function|class|const|let|var)\b/));
      // `const X = dynamic(() => import('./x'))`, `lazy(() => import('./x'))`, `export default X`
      const tail = s.slice(at, at + 300);
      const lazy = /=\s*(?:\w+\.)?(?:dynamic|lazy|loadable|defineAsyncComponent|React\.lazy)\s*\(\s*(?:async\s*)?\(\s*\)\s*=>\s*import\(\s*["']([^"']+)["']/.exec(tail);
      if (lazy && depth < 6) { const g = resolve(f, lazy[1]); if (g) out = declIn(g, 'default', depth + 1) || { file: g, line: 1, symbol: 'default' }; }
      if (!out && name === 'default' && own[1] && !/^(function|class|async)$/.test(own[1]) && !/function|class/.test(own[0])) {
        // `export default Name;`: Name's own declaration
        const inner = declIn(f, own[1], depth + 1);
        if (inner) out = inner;
      }
      if (!out) out = { file: f, line: L(at), symbol: name === 'default' ? own[1] || 'default' : name };
    }
    if (!out && depth < 6) {
      // re-exports: export { A as name } from './x', export { default as name } from './x', export * from './x'
      for (const m of s.matchAll(/\bexport\s*\{([^}]*)\}\s*from\s*["']([^"']+)["']/g)) {
        for (const part of m[1].split(',')) {
          const [orig, as] = part.trim().split(/\s+as\s+/);
          if ((as || orig) !== name) continue;
          const g = resolve(f, m[2]); if (g) out = declIn(g, orig, depth + 1);
          if (out) break;
        }
        if (out) break;
      }
      if (!out) for (const m of s.matchAll(/\bexport\s*\*\s*from\s*["']([^"']+)["']/g)) { const g = resolve(f, m[1]); if (g) out = declIn(g, name, depth + 1); if (out) break; }
    }
    declCache.set(key, out);
    return out;
  };
  /** The component an identifier names in file f: declared there, or imported. */
  const componentAt = (f, ident) => {
    if (!ident) return null;
    const s = get(f);
    const [head, member] = ident.split('.');
    if (member) {
      // import * as R from './renderers'; R.Class
      const ns = new RegExp(`import\\s*\\*\\s*as\\s+${head}\\s+from\\s*["']([^"']+)["']`).exec(s);
      const g = ns && resolve(f, ns[1]);
      return g ? declIn(g, member) : null;
    }
    for (const m of s.matchAll(/\bimport\s+(?!type\b)([^;]*?)\s+from\s*["']([^"']+)["']/g)) {
      const clause = m[1];
      const def = /^\s*([A-Za-z_$][\w$]*)\s*(?:,|$)/.exec(clause);
      const named = /\{([^}]*)\}/.exec(clause);
      let orig = null;
      if (def && def[1] === ident) orig = 'default';
      if (!orig && named) for (const part of named[1].split(',')) { const [a, b] = part.trim().replace(/^type\s+/, '').split(/\s+as\s+/); if ((b || a) === ident) orig = a; }
      if (!orig) continue;
      const g = resolve(f, m[2]);
      if (!g) return null;
      return declIn(g, orig) || { file: g, line: 1, symbol: orig };
    }
    return declIn(f, ident);
  };
  /** The component a table value or a branch names: `X`, `R.X`, `(p) => <X {...p} />`, `memo(X)`, `dynamic(() => import('./x'))`. */
  const rendererOf = (f, value) => {
    const v = String(value || '').trim();
    const imp = /\bimport\(\s*["']([^"']+)["']/.exec(v);
    if (imp) { const g = resolve(f, imp[1]); return g ? { ident: P.basename(g).replace(CODE, ''), decl: declIn(g, 'default') || { file: g, line: 1, symbol: 'default' } } : null; }
    const jsx = /<([A-Z][\w$]*(?:\.[A-Z][\w$]*)?)/.exec(v);
    const plain = /^([A-Za-z_$][\w$]*(?:\.[A-Z][\w$]*)?)$/.exec(v);
    const wrapped = /^\w+\(\s*([A-Z][\w$]*)\s*[,)]/.exec(v);
    const ident = (jsx || plain || wrapped)?.[1];
    if (!ident) return null;
    return { ident: ident.split('.').pop(), decl: componentAt(f, ident) };
  };
  const compish = (v) => /^[A-Z][\w$.]*$/.test(v) || /^[a-z_$][\w$]*\.[A-Z][\w$]*$/.test(v) || /^(?:async\s*)?(?:\([^)]*\)|\w+)\s*=>\s*\(?\s*</.test(v) || /^(?:\w+\.)?(?:dynamic|lazy|loadable|defineAsyncComponent)\(/.test(v) || /^(?:memo|observer|withRouter|React\.memo)\(\s*[A-Z]/.test(v) || /^handle[A-Z]\w*$/.test(v);

  const out = [];
  for (const [page, route] of entries) {
    // the page and what it imports, a few levels down
    const seen = new Map([[page, 0]]);
    const queue = [page];
    while (queue.length) {
      const f = queue.shift(); const d = seen.get(f); if (d >= maxDepth) continue;
      for (const m of get(f).matchAll(/(?:import|from)\s*\(?\s*["']([^"']+)["']/g)) { const t = resolve(f, m[1]); if (t && !seen.has(t)) { seen.set(t, d + 1); queue.push(t); } }
      if (seen.size > 300) break;
    }
    const files = [...seen.keys()];
    const kinds = [], sections = [], typePaths = new Map(), segRules = [];
    const push = (kind, item) => (kind === 'page' ? kinds : sections).push(item);
    for (const [f, depth] of seen) {
      const s = get(f); const L = lineIndex(s);
      // 1) tables: const X = { key: Component, ... } read as X[type] (or typed Record<Type, ...>), and new Map([[key, Component]]) read with .get(type)
      for (const m of s.matchAll(new RegExp(`\\b(?:const|let|var)\\s+(${ID})\\s*(?::\\s*(?:[^=;]|=>){1,300}?)?=\\s*(\\{|new\\s+Map\\s*(?:<[^>]*>)?\\(\\s*\\[)`, 'g'))) {
        const name = m[1];
        const isMap = m[2] !== '{';
        const open = isMap ? s.indexOf('[', m.index + m[0].length - 1) : s.indexOf('{', m.index + m[0].length - 1);
        const span = inside(s, open); if (!span) continue;
        let ents;
        if (isMap) ents = [...s.slice(open, span[1]).matchAll(/\[\s*([^,\]]+?)\s*,\s*([^\]]+?)\s*\]/g)].map((x) => ({ key: x[1], value: x[2], start: open + x.index }));
        else ents = objectEntries(s, open).filter((e) => e.key);
        if (ents.length < 2) continue;
        // path tables: type to a path template or prefix
        const paths = ents.filter((e) => /^(["'`])\/[^"'`]*\1$/.test(e.value || ''));
        if (paths.length >= 2 && paths.length >= ents.length * 0.7) {
          const prefix = /prefix|base|root|segment/i.test(name);
          for (const e of paths) { const k = keyText(e.key, [f]); if (k) typePaths.set(norm(k), { pattern: pathPattern(e.value.slice(1, -1), prefix), file: f, line: L(e.start), how: prefix ? 'prefix' : 'path table' }); }
          continue;
        }
        const comps = ents.filter((e) => compish(e.value));
        if (ents.length < 3 && !isMap) { if (comps.length < 2) continue; }
        if (comps.length < Math.max(2, ents.length * 0.7)) continue;
        const after = s.slice(span[1], span[1] + 160);
        const used = new RegExp(`\\b${name.replace(/\$/g, '\\$')}\\s*(?:\\?\\.)?(?:\\[([^\\]]{1,120})\\]|\\.get\\(([^)]{1,120})\\))`).exec(s);
        const typed = /^\s*[\])]*\s*\}?\s*(satisfies|as)\s+(?:Record|Partial<Record|\{\s*\[)/.test(after) || /Record<\s*[\w.]*(Type|Kind)/.test(m[0]);
        if (!used && !typed) continue;
        const keyExpr = (used?.[1] || used?.[2] || '').replace(/\s+as\s+.*$/s, '');
        const kind = SECTIONY.test(name) || /blockType|_key|blocks?\b|section/i.test(keyExpr) ? 'section' : (TYPEY.test(keyExpr) || typed ? 'page' : 'section');
        const handlers = comps.every((e) => /^handle[A-Z]/.test(e.value));
        for (const e of ents) {
          if (!compish(e.value)) continue;
          const k = keyText(e.key, [f]);
          if (!k) continue;
          push(kind, { type: k, value: e.value.slice(0, 80), file: f, line: L(e.start), table: name, how: isMap ? 'map' : 'object', depth, ...(handlers ? { handler: true } : {}) });
        }
      }
      // 2) switch (doc.type) { case 'post': return <Post/>; case 'a': case 'b': Comp = B; break }; slug builders return '/path'
      for (const m of s.matchAll(/\bswitch\s*\(\s*([^)]{1,100})\)\s*\{/g)) {
        const expr = m[1].trim();
        const open = s.indexOf('{', m.index + m[0].length - 1); const span = inside(s, open); if (!span) continue;
        const body = s.slice(open + 1, span[1]);
        const labels = [...body.matchAll(/(?:^|[;{}\s])(case\s+([^:]{1,100}?)\s*:|default\s*:)/g)].filter((x) => depthAt(body, x.index) === 0);
        let pending = [];
        const comps = [], builds = [];
        labels.forEach((x, i) => {
          const segEnd = i + 1 < labels.length ? labels[i + 1].index : body.length;
          const seg = body.slice(x.index + x[0].length, segEnd);
          if (x[2] === undefined) { pending = []; return; }
          pending.push({ raw: x[2], at: open + 1 + x.index + x[0].indexOf('case') });
          if (!seg.replace(/[\s{}]/g, '')) return; // falls through to the next case
          const r = /\breturn\s*\(?\s*<([A-Z][\w$.]*)/.exec(seg) || /\breturn\s+(?:React\.)?createElement\(\s*([A-Z][\w$.]*)/.exec(seg) || /\breturn\s+([A-Z][\w$]*(?:\.[A-Z][\w$]*)?)\s*(?:\(|;|$)/.exec(seg) || /\b[A-Za-z_$][\w$]*\s*=\s*([A-Z][\w$]*(?:\.[A-Z][\w$]*)?)\s*;/.exec(seg);
          const b = /\breturn\s+(["'`])(\/[^"'`]*)\1/.exec(seg);
          for (const p of pending) { if (r) comps.push({ ...p, comp: r[1] }); if (b) builds.push({ ...p, path: b[2] }); }
          pending = [];
        });
        if (TYPEY.test(expr) && comps.length >= 2) {
          const kind = /block|section/i.test(expr) ? 'section' : 'page';
          for (const c of comps) { const k = keyText(c.raw, [f]); if (k) push(kind, { type: k, value: c.comp, file: f, line: L(c.at), table: `switch (${expr})`, how: 'switch', depth }); }
        }
        if (builds.length >= 2) for (const c of builds) { const k = keyText(c.raw, [f]); if (k && !typePaths.has(norm(k))) typePaths.set(norm(k), { pattern: pathPattern(c.path, false), file: f, line: L(c.at), how: 'path builder' }); }
      }
      // 3) if chains: if (doc.type === 'post') return <Post/>; and {type === 'post' && <Post/>}, type === 'post' ? <Post/> : ...
      const branches = new Map();
      const addBranch = (expr, raw, comp, at) => { const k = expr.replace(/\s+/g, '').replace(/^\(|\)$/g, ''); if (!TYPEY.test(k)) return; if (!branches.has(k)) branches.set(k, []); branches.get(k).push({ raw, comp, at }); };
      const EQ = new RegExp(`^\\s*(?:(${ID}(?:\\??\\.${ID}|\\[["'][\\w-]+["']\\])*)\\s*===?\\s*((["'\`])[\\w-]+\\3|${ID}\\.${ID}|[A-Z_][A-Z0-9_]+))\\s*$|^\\s*(?:((["'\`])[\\w-]+\\5|${ID}\\.${ID})\\s*===?\\s*(${ID}(?:\\??\\.${ID})*))\\s*$`);
      for (const m of s.matchAll(/\bif\s*\(/g)) {
        const po = s.indexOf('(', m.index); const pc = closeOf(s, po); if (pc < 0) continue;
        const cond = s.slice(po + 1, pc);
        const eq = EQ.exec(cond); if (!eq) continue;
        const [expr, raw] = eq[1] ? [eq[1], eq[2]] : [eq[6], eq[4]];
        const stmt = s.slice(pc + 1, pc + 400);
        const r = /^\s*\{?\s*return\s*\(?\s*<([A-Z][\w$.]*)/.exec(stmt) || /^\s*\{?\s*return\s+(?:React\.)?createElement\(\s*([A-Z][\w$.]*)/.exec(stmt) || /^\s*\{?\s*return\s+([A-Z][\w$]*)\s*\(/.exec(stmt);
        if (r) addBranch(expr, raw, r[1], m.index);
      }
      for (const m of s.matchAll(new RegExp(`(${ID}(?:\\??\\.${ID})*)\\s*===?\\s*((["'\`])[\\w-]+\\3|${ID}\\.${ID})\\s*(?:&&|\\?)\\s*\\(?\\s*<([A-Z][\\w$.]*)`, 'g'))) addBranch(m[1], m[2], m[4], m.index);
      for (const [expr, bs] of branches) {
        if (bs.length < 2) continue;
        const kind = /block|section/i.test(expr) ? 'section' : 'page';
        for (const b of bs) { const k = keyText(b.raw, [f]); if (k) push(kind, { type: k, value: b.comp, file: f, line: L(b.at), table: `if (${expr} === ...)`, how: 'if', depth }); }
      }
      // 4) URL rules: an if on the URL's segments that returns a type (the resolver's own checks)
      segRules.push(...urlRules(s, f, L, (raw) => keyText(raw, [f])));
    }

    // renderer tables name the screens; a handler table (type -> handleX) only says which types the resolver knows
    const renderers = kinds.filter((k) => !k.handler);
    if (!renderers.length && !sections.length) continue;
    const byType = new Map();
    for (const k of renderers) if (!byType.has(norm(k.type))) byType.set(norm(k.type), k);
    // a renderer that serves several types is named by the type, not by the renderer
    const served = new Map();
    for (const k of byType.values()) served.set(k.value, (served.get(k.value) || 0) + 1);
    const rules = new Map();
    for (const r of segRules) if (!rules.has(norm(r.type))) rules.set(norm(r.type), r);
    const screens = [];
    for (const [n, k] of byType) {
      const r = rendererOf(k.file, k.value);
      const decl = r?.decl && fileSet.has(r.decl.file) ? r.decl : null;
      const tp = typePaths.get(n);
      const rule = rules.get(n);
      let url, urlFrom = null;
      // a path table or builder writes the whole path; the resolver's checks are on the segments after the route's base
      if (tp) { url = tp.pattern; urlFrom = { file: tp.file, line: tp.line, how: tp.how }; }
      else if (rule) { url = joinUrl(route.base, rule.pattern); urlFrom = { file: rule.file, line: rule.line, how: 'resolver' }; }
      else url = `${route.base}/(${slug(k.type)})/${route.last}`;
      const ident = r?.ident || String(k.value).split('.').pop();
      const shared = (served.get(k.value) || 0) > 1 || !/^[A-Z]/.test(ident) || ident === 'default';
      screens.push({
        type: k.type,
        name: shared ? plainName(k.type, { fromType: true }) : plainName(ident),
        renderer: decl?.symbol && decl.symbol !== 'default' ? decl.symbol : ident,
        file: decl?.file || k.file,
        line: decl?.line || k.line,
        url,
        urlFrom,
        table: { file: k.file, line: k.line, name: k.table, how: k.how },
      });
    }
    // two types with the same URL pattern and renderer are one screen; two different screens never share a URL
    const byUrl = new Map();
    for (const x of screens) {
      const was = byUrl.get(x.url);
      if (!was) { byUrl.set(x.url, x); continue; }
      if (was.file === x.file && was.renderer === x.renderer) { was.types = [...(was.types || [was.type]), x.type]; continue; }
      x.url = `${route.base}/(${slug(x.type)})${x.url.slice(route.base.length)}`;
      byUrl.set(x.url, x);
    }
    // two screens never share a name either: the later one says its type
    const names = new Set();
    for (const x of byUrl.values()) { if (names.has(x.name)) x.name = `${x.name} (${slug(x.type)})`; names.add(x.name); }
    const uniqSections = new Map();
    for (const x of sections) if (!uniqSections.has(norm(x.type))) uniqSections.set(norm(x.type), x);
    out.push({ file: page, route: route.url, base: route.base, screens: [...byUrl.values()], sections: [...uniqSections.values()], resolverOnly: kinds.filter((k) => k.handler && !byType.has(norm(k.type))).map((k) => k.type) });
  }
  return { dispatchers: out, stats: { ms: Date.now() - t0 } };
}

const slug = (t) => String(t).replace(/([a-z0-9])([A-Z])/g, '$1-$2').toLowerCase().replace(/[^a-z0-9-]+/g, '-').replace(/^-|-$/g, '') || 'page';

/** How deep (in braces) offset i of a switch body is. */
function depthAt(body, i) { let d = 0, q = null; for (let k = 0; k < i; k++) { const c = body[k]; if (q) { if (c === '\\') k++; else if (c === q) q = null; continue; } if (c === '"' || c === "'" || c === '`') q = c; else if (c === '{') d++; else if (c === '}') d--; } return d; }

/** A path template from code as a route pattern: `/blog/${post.slug}` -> /blog/:slug; a prefix gets /*. */
export function pathPattern(text, prefix) {
  let p = String(text).replace(/\$\{\s*([^}]*?)\s*\}/g, (_, e) => ':' + ((e.match(/[A-Za-z_$][\w$]*/g) || ['param']).pop()));
  p = p.split(/[?#]/)[0].replace(/\/{2,}/g, '/');
  if (!p.startsWith('/')) p = '/' + p;
  if (prefix || p.endsWith('/')) p = p.replace(/\/$/, '') + '/*';
  return p;
}

function joinUrl(base, pattern) {
  const p = String(pattern || '/');
  return ((base || '') + (p === '/' ? '' : p)) || '/';
}

/**
 * The resolver's own URL checks: every `return <type>` inside if-blocks whose conditions test the
 * URL's segments (`parts.length === 3 && parts[0] === 'blog'`, `LIST.includes(parts[1])`,
 * `path.startsWith('/blog/')`), as a pattern per type.
 */
export function urlRules(s, file, L, keyOf) {
  const ifs = [];
  for (const m of s.matchAll(/\bif\s*\(/g)) {
    const po = s.indexOf('(', m.index); const pc = closeOf(s, po); if (pc < 0) continue;
    const cond = s.slice(po + 1, pc);
    if (!/(slug|segment|part|path|param|pathname|url)\w*\s*(?:\??\.length|\[\s*\d+\s*\]|\??\.startsWith|\s*===?\s*["'`]\/)/i.test(cond) && !/\.includes\(\s*\w[\w.]*\[\s*\d+\s*\]\s*\)/.test(cond)) continue;
    let k = pc + 1; while (/\s/.test(s[k] || '')) k++;
    let end;
    if (s[k] === '{') { end = closeOf(s, k); if (end < 0) continue; } else { end = s.indexOf(';', k); if (end < 0) continue; }
    ifs.push({ start: m.index, from: pc + 1, end, cond });
  }
  if (!ifs.length) return [];
  const out = [];
  const RET = new RegExp(`\\breturn\\s+((["'\`])[\\w-]{1,80}\\2|${ID}\\.${ID}|\\{\\s*type\\s*:\\s*(?:(["'\`])[\\w-]{1,80}\\3|${ID}\\.${ID}))`, 'g');
  for (const r of s.matchAll(RET)) {
    const around = ifs.filter((x) => r.index > x.from && r.index <= x.end);
    if (!around.length) continue;
    // the innermost block: a return inside a nested if (not about the URL) still counts for the URL's ifs
    const raw = r[1].replace(/^\{\s*type\s*:\s*/, '');
    const type = keyOf(raw);
    if (!type) continue;
    const pattern = segPattern(around.map((x) => x.cond));
    if (pattern) out.push({ type, pattern, file, line: L(around[around.length - 1].start) });
  }
  return out;
}

/** Conditions on a URL's segments as one pattern; null when they pin nothing. */
export function segPattern(conds) {
  let len = null, min = 0, prefix = null, exact = null;
  const seg = new Map();
  const V = '[A-Za-z_$][\\w$]*(?:\\??\\.[A-Za-z_$][\\w$]*)*';
  for (const c0 of conds) {
    const c = c0.replace(/\s+/g, ' ');
    for (const m of c.matchAll(new RegExp(`(${V})\\??\\.length\\s*(===?|>=|>)\\s*(\\d+)`, 'g'))) { const n = +m[3]; if (m[2].startsWith('=') && m[2] !== '>=') len = n; else min = Math.max(min, m[2] === '>' ? n + 1 : n); }
    for (const m of c.matchAll(new RegExp(`(${V})\\s*\\[\\s*(\\d+)\\s*\\]\\s*===?\\s*(["'\`])([^"'\`/]+)\\3`, 'g'))) seg.set(+m[2], m[4]);
    for (const m of c.matchAll(new RegExp(`(["'\`])([^"'\`/]+)\\1\\s*===?\\s*(${V})\\s*\\[\\s*(\\d+)\\s*\\]`, 'g'))) seg.set(+m[4], m[2]);
    for (const m of c.matchAll(new RegExp(`(${V})\\.includes\\(\\s*${V}\\s*\\[\\s*(\\d+)\\s*\\]\\s*\\)`, 'g'))) if (!seg.has(+m[2])) seg.set(+m[2], ':' + paramName(m[1]));
    for (const m of c.matchAll(new RegExp(`(${V})\\s*\\[\\s*(\\d+)\\s*\\]\\s*(?:\\??\\.startsWith|\\??\\.match|\\.test)`, 'g'))) if (!seg.has(+m[2])) seg.set(+m[2], ':' + 'segment' + (+m[2] + 1));
    for (const m of c.matchAll(new RegExp(`(?:slug|path|pathname|url)\\w*\\s*\\??\\.startsWith\\(\\s*(["'\`])(\\/[^"'\`]*)\\1`, 'gi'))) prefix = m[2];
    for (const m of c.matchAll(new RegExp(`(?:slug|path|pathname|url)\\w*\\s*===?\\s*(["'\`])(\\/[^"'\`]*)\\1`, 'gi'))) exact = m[2];
  }
  if (exact) return exact.replace(/\/$/, '') || '/';
  if (seg.size || len != null) {
    const n = len ?? Math.max(...seg.keys(), min - 1) + 1;
    const parts = [];
    for (let i = 0; i < n; i++) parts.push(seg.has(i) ? seg.get(i) : ':segment' + (i + 1));
    return '/' + parts.join('/') + (len == null ? '/*' : '');
  }
  if (prefix) return prefix.replace(/\/$/, '') + '/*';
  return null;
}

/** `OPEN_HARBOURS` -> openHarbour, `allowedCities` -> allowedCity. */
function paramName(list) {
  const last = String(list).split('.').pop();
  const words = last.replace(/([a-z0-9])([A-Z])/g, '$1_$2').toLowerCase().split('_').filter(Boolean);
  if (words.length) words[words.length - 1] = words[words.length - 1].replace(/ies$/, 'y').replace(/s$/, '');
  return words.map((w, i) => (i ? w.charAt(0).toUpperCase() + w.slice(1) : w)).join('') || 'segment';
}
