// Calls through wrappers, URL maps, env base URLs, Nest proxies and GraphQL (JS/TS), matched to
// endpoints in any repository of the map:
//   1. clients: fetch, axios, ky, got, superagent, axios.create instances (followed through
//      default imports), and any receiver named *client, *http or *api;
//   2. wrappers: a function whose client call builds its URL from one of its own parameters,
//      then wrappers of wrappers (two passes); the path argument, the method and the env base
//      come from the body;
//   3. URL maps: module-level object literals of path strings, with string constants resolved;
//   4. call sites: wrapper(...), client.verb(...), fetch(...), and a wrapper passed as a value
//      (useSWR(key, fetcher)); the path argument resolves through a literal, a template
//      (a leading ${BASE} dropped), a map member, a local const or 'a' + x;
//   5. endpoints: Floorplan's, plus Nest proxy routes (forRoutes + createProxyMiddleware) and
//      GraphQL resolvers (/graphql#field) matched to gql documents.
// Tests are never evidence.
import path from 'node:path';
import { walk, read, isTest, lineIndex } from './lib/text.mjs';

const JS = new Set(['.ts', '.tsx', '.js', '.jsx', '.mjs', '.cjs']);
const FN_HEAD = /(?:export\s+)?(?:default\s+)?(?:async\s+)?function\s*\*?\s*([A-Za-z_$][\w$]*)\s*(?:<[^>()]*>)?\s*\(|(?:export\s+)?(?:const|let)\s+([A-Za-z_$][\w$]*)\s*(?::[^=\n]+)?=\s*(?:async\s+)?(?:<[^>()]*>)?\s*\(|^[ \t]+(?:public\s+|private\s+|protected\s+|static\s+)*(?:async\s+)?([A-Za-z_$][\w$]*)\s*(?:<[^>()]*>)?\s*\(/gm;
const esc = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

/** Index just past the bracket closing the one opened before i (strings and ${} skipped). */
export function balanced(t, i, open = '(', close = ')') {
  let depth = 1, j = i;
  const n = t.length;
  while (j < n && depth) {
    const c = t[j];
    if (c === '"' || c === "'" || c === '`') {
      const q = c; j++;
      while (j < n && t[j] !== q) {
        if (t[j] === '\\') j++;
        else if (q === '`' && t[j] === '$' && t[j + 1] === '{') { j = balanced(t, j + 2, '{', '}'); continue; }
        j++;
      }
    } else if (c === open) depth++;
    else if (c === close) depth--;
    j++;
  }
  return j;
}

export function splitArgs(s) {
  const out = [];
  let depth = 0, cur = '', q = null;
  for (let i = 0; i < s.length; i++) {
    const c = s[i];
    if (q) { cur += c; if (c === '\\') { cur += s[i + 1] ?? ''; i++; } else if (c === q) q = null; }
    else if (c === '"' || c === "'" || c === '`') { q = c; cur += c; }
    else if ('([{'.includes(c)) { depth++; cur += c; }
    else if (')]}'.includes(c)) { depth--; cur += c; }
    else if (c === ',' && depth === 0) { out.push(cur.trim()); cur = ''; }
    else cur += c;
  }
  if (cur.trim()) out.push(cur.trim());
  return out;
}

function paramsOf(p) {
  const names = [];
  for (const a of splitArgs(p)) {
    const x = a.replace(/[:=][\s\S]*/, '').trim().replace(/^\.+/, '').replace(/^[{ ?]+|[} ?]+$/g, '');
    if (/^[A-Za-z_$][\w$]*$/.test(x)) names.push(x);
  }
  return names;
}

/** [name, params, start, bodyStart, bodyEnd] with balanced parameter lists. */
function* fnDefs(t) {
  for (const m of t.matchAll(FN_HEAD)) {
    const name = m[1] || m[2] || m[3];
    const after = m.index + m[0].length;
    const pend = balanced(t, after);
    const params = t.slice(after, pend - 1).replace(/\/\*[\s\S]*?\*\/|\/\/[^\n]*/g, '');
    const rest = t.slice(pend, pend + 400);
    // the body brace is the first '{' that ends its line (return types may hold type literals)
    const am = /^(\s*(?::[^\n]*?|:[\s\S]*?)?)(=>\s*)?\{[ \t]*\n/.exec(rest);
    if (!am || (m[2] && !am[2]) || (/[;=](?!>)/.test(am[1].replace(/=>/g, '')) && !am[1].trimStart().startsWith(':'))) continue;
    const bstart = t.lastIndexOf('{', pend + am[0].length - 1);
    if (bstart < pend) continue;
    yield [name, params, m.index, bstart, balanced(t, bstart + 1, '{', '}')];
  }
}

export class Project {
  constructor(root, repos) {
    this.root = root;
    this.files = new Map();
    for (const repo of repos) for (const f of walk(path.join(root, repo), JS)) {
      if (f.endsWith('.d.ts')) continue;
      const rel = `${repo}/${f}`;
      const t = read(root, rel);
      if (t != null) this.files.set(rel, t);
    }
    this.consts = new Map(); this.maps = new Map(); this.clients = new Map(); this.instances = new Map(); this.wrappers = new Map();
    this.lines = new Map();
    this.constsAndMaps(); this.findClients(); this.findWrappers();
  }

  lineOf(rel, i) { if (!this.lines.has(rel)) this.lines.set(rel, lineIndex(this.files.get(rel))); return this.lines.get(rel)(i); }

  constsAndMaps() {
    for (const t of this.files.values()) for (const m of t.matchAll(/^(?:export\s+)?const\s+([A-Z][A-Z0-9_]+|[a-z]\w*(?:Path|PATH|Url|URL|Endpoint))\s*(?::\s*\w+)?=\s*(["'`])([^"'`$\n]*)\2/gm)) if (!this.consts.has(m[1])) this.consts.set(m[1], m[3]);
    for (const t of this.files.values()) {
      for (const m of t.matchAll(/^(?:export\s+)?const\s+([A-Za-z_$][\w$]*)\s*(?::[^=]+)?=\s*\{/gm)) {
        const start = m.index + m[0].length;
        const end = balanced(t, start, '{', '}');
        const body = t.slice(start, end - 1);
        const entries = new Map();
        for (const e of body.matchAll(/^\s*["']?([A-Za-z_$][\w$-]*)["']?\s*:\s*(?:\([^)]*\)\s*=>\s*)?(["'`])((?:(?!\2).)*)\2/gm)) entries.set(e[1], e[3]);
        for (const e of body.matchAll(/^\s*([A-Za-z_$][\w$]*)\s*:\s*([A-Z][A-Z0-9_]+)\s*,/gm)) if (this.consts.has(e[2])) entries.set(e[1], this.consts.get(e[2]));
        const paths = [...entries].filter(([, v]) => /^\/?[a-zA-Z0-9_-]+(\/|\?|$)/.test(v) && v.includes('/'));
        if (paths.length >= 2 && paths.length >= entries.size * 0.5) this.maps.set(m[1], new Map(paths));
      }
    }
  }

  resolveImport(rel, spec) {
    let base;
    if (spec.startsWith('.')) base = path.posix.normalize(path.posix.join(path.posix.dirname(rel), spec));
    else if (/^(@src|@|~)\//.test(spec)) {
      const repo = rel.split('/')[0], sub = spec.split('/').slice(1).join('/');
      for (const c of [`${repo}/src/${sub}`, `${repo}/apps/web/src/${sub}`]) for (const ext of ['.ts', '.tsx', '.js', '/index.ts']) if (this.files.has(c + ext)) return c + ext;
      return null;
    } else return null;
    for (const ext of ['.ts', '.tsx', '.js', '.mjs', '/index.ts', '/index.js', '']) if (this.files.has(base + ext)) return base + ext;
    return null;
  }

  findClients() {
    const instFiles = new Map();
    const stem = (f) => f.replace(/\.[^./]+$/, '');
    for (const [rel, t] of this.files) {
      for (const m of t.matchAll(/(?:const|let)\s+([A-Za-z_$][\w$]*)\s*=\s*axios\.create\(\s*\{([^}]*)/g)) {
        const env = /([A-Z][A-Z0-9_]{3,})/.exec(m[2])?.[1] || null;
        if (!this.instances.has(rel)) this.instances.set(rel, new Map());
        this.instances.get(rel).set(m[1], env);
        if (new RegExp(`export\\s+default\\s+${esc(m[1])}\\b`).test(t) || new RegExp(`export\\s+(const|\\{[^}]*\\b${esc(m[1])})`).test(t)) instFiles.set(stem(rel), env);
      }
    }
    for (const [rel, t] of this.files) {
      const names = new Map([['axios', null], ['ky', null], ['got', null], ['superagent', null], ...(this.instances.get(rel) || [])]);
      for (const m of t.matchAll(/import\s+([A-Za-z_$][\w$]*)\s*(?:,\s*\{[^}]*\})?\s*from\s*["']([^"']+)["']/g)) {
        const tgt = this.resolveImport(rel, m[2]);
        if (tgt && instFiles.has(stem(tgt))) names.set(m[1], instFiles.get(stem(tgt)));
      }
      this.clients.set(rel, names);
    }
  }

  /** [pos, method, argExpr, base] for direct client calls in t[start:end]. */
  clientCalls(rel, t, start = 0, end = t.length) {
    const names = this.clients.get(rel) || new Map();
    const out = [];
    const seg = t.slice(start, end);
    for (const m of seg.matchAll(/(?<![\w.$])(fetch|\$fetch|ofetch)\s*(?:<[^>()]*>)?\(/g)) {
      const at = start + m.index + m[0].length;
      const a = splitArgs(t.slice(at, balanced(t, at) - 1));
      if (!a.length) continue;
      const meth = a.length > 1 ? /method\s*:\s*["'](\w+)/.exec(a[1]) : null;
      out.push([start + m.index, meth ? meth[1].toUpperCase() : 'GET', a[0], null]);
    }
    for (const m of seg.matchAll(/(?<![\w$])([A-Za-z_$][\w$]*)\s*\.(get|post|put|patch|delete|head|request)\s*(?:<[^()]*?>)?\(/g)) {
      if (!names.has(m[1]) && !/(client|http|api|axios)$/i.test(m[1])) continue;
      const at = start + m.index + m[0].length;
      const a = splitArgs(t.slice(at, balanced(t, at) - 1));
      if (a.length) out.push([start + m.index, m[2].toUpperCase(), a[0], names.get(m[1]) ?? null]);
    }
    return out;
  }

  findWrappers() {
    const SKIP = new Set(['if', 'for', 'while', 'switch', 'catch', 'constructor', 'get', 'post', 'put', 'patch', 'delete', 'request']);
    for (let pass = 0; pass < 2; pass++) {
      for (const [rel, t] of this.files) {
        if (isTest(rel)) continue;
        for (const [name, praw, mstart, bstart, bend] of fnDefs(t)) {
          const ps = paramsOf(praw);
          if (!name || !ps.length || SKIP.has(name) || this.wrappers.has(name)) continue;
          const calls = this.clientCalls(rel, t, bstart, bend);
          const body = t.slice(bstart, bend);
          for (const [wn, w] of this.wrappers) {
            for (const cm of body.matchAll(new RegExp(`(?<![\\w.$])${esc(wn)}\\s*(?:<[^()]*?>)?\\(`, 'g'))) {
              const at = bstart + cm.index + cm[0].length;
              const a = splitArgs(t.slice(at, balanced(t, at) - 1));
              if (a.length > w.idx) calls.push([bstart + cm.index, w.method, a[w.idx], w.base]);
            }
          }
          for (const [pos, meth, arg, base] of calls) {
            let expr = arg;
            if (/^[A-Za-z_$][\w$]*$/.test(arg.trim())) {
              const dm = new RegExp(`(?:const|let)\\s+${esc(arg.trim())}\\s*(?::[^=]+)?=\\s*([^;\\n]+)`).exec(t.slice(bstart, pos));
              if (dm) expr = dm[1];
            }
            const idx = ps.findIndex((p) => new RegExp(`(?<![\\w.$])${esc(p)}(?![\\w$])`).test(expr));
            if (idx < 0) continue;
            const env = /([A-Z][A-Z0-9_]{3,})/.exec(expr)?.[1] || /process\.env\.([A-Z][A-Z0-9_]+)/.exec(body)?.[1] || null;
            const nm = /(get|post|put|patch|delete)/i.exec(name);
            this.wrappers.set(name, { file: rel, line: this.lineOf(rel, mstart), idx, method: nm && meth === 'GET' ? nm[1].toUpperCase() : meth, base: base || env });
            break;
          }
        }
      }
    }
  }

  /** [path, how] for an argument expression, or null. */
  resolve(rel, t, pos, expr, depth = 0) {
    const e = expr.trim();
    if (depth > 3) return null;
    let m = /^(["'])([^"']*)\1$/.exec(e);
    if (m) return [m[2], 'exact'];
    m = /^`([^`]*)`$/s.exec(e);
    if (m) {
      const s = m[1].replace(/^\$\{[^}]*\}/, '');
      let out = '';
      for (const p of s.split(/(\$\{[^}]*\})/)) {
        if (!p.startsWith('${')) { out += p; continue; }
        const inner = p.slice(2, -1).trim();
        const r = /^[\w$.]+$/.test(inner) && (this.consts.has(inner) || inner.includes('.')) ? this.resolve(rel, t, pos, inner, depth + 1) : null;
        out += r ? r[0] : ':param';
      }
      return [out, 'exact'];
    }
    m = /^([A-Za-z_$][\w$]*)\.([A-Za-z_$][\w$]*)(?:\([^)]*\))?$/.exec(e);
    if (m && this.maps.get(m[1])?.has(m[2])) return [this.maps.get(m[1]).get(m[2]).replace(/\$\{[^}]*\}/g, ':param'), 'exact'];
    if (/^[A-Z][A-Z0-9_]+$/.test(e) && this.consts.has(e)) return [this.consts.get(e), 'exact'];
    if (/^[A-Za-z_$][\w$]*$/.test(e)) {
      const back = t.slice(Math.max(0, pos - 3000), pos);
      let dm = null;
      for (const x of back.matchAll(new RegExp(`(?:const|let)\\s+${esc(e)}\\s*(?::[^=]+)?=\\s*([^;\\n]+)`, 'g'))) dm = x;
      if (dm) { const r = this.resolve(rel, t, pos, dm[1].trim().replace(/,$/, ''), depth + 1); return r ? [r[0], 'inferred'] : null; }
    }
    m = /^(.+?)\s*\+\s*(["'`][\s\S]*)$/.exec(e);
    if (m) { const r = this.resolve(rel, t, pos, m[2], depth + 1); return r ? [r[0], 'inferred'] : null; }
    return null;
  }

  calls() {
    const out = [];
    const wnames = [...this.wrappers.keys()].sort((a, b) => b.length - a.length);
    const alt = wnames.map(esc).join('|');
    for (const [rel, t] of this.files) {
      if (isTest(rel)) continue;
      const found = this.clientCalls(rel, t).map((x) => [...x, 'client']);
      if (wnames.length) {
        for (const m of t.matchAll(new RegExp(`(?<![\\w.$])(${alt})\\s*(?:<[^()]*?>)?\\(`, 'g'))) {
          const w = this.wrappers.get(m[1]);
          if (w.file === rel && this.lineOf(rel, m.index) === w.line) continue;
          const at = m.index + m[0].length;
          const a = splitArgs(t.slice(at, balanced(t, at) - 1));
          if (a.length > w.idx) found.push([m.index, w.method, a[w.idx], w.base, 'wrapper:' + m[1]]);
        }
        // a wrapper passed as a value: useSWR(key, fetcher)
        for (const m of t.matchAll(new RegExp(`(?<![\\w.$])([A-Za-z_$][\\w$]*)\\s*(?:<[^()]*?>)?\\(\\s*([\`"'][^\`"']*[\`"'])\\s*,\\s*(${alt})\\b`, 'g'))) {
          const w = this.wrappers.get(m[3]);
          found.push([m.index, w.method, m[2], w.base, 'passed:' + m[3]]);
        }
      }
      for (const [pos, method, arg, base, via] of found) {
        const r = this.resolve(rel, t, pos, arg);
        if (!r) continue;
        let [p, how] = r;
        p = p.split('?')[0];
        if (!p.startsWith('/')) {
          if (/^[a-z][\w-]*(\/|$)/.test(p) && !/^(http|data:)/.test(p)) p = '/' + p;
          else if (p.startsWith('http')) p = p.split('/').length >= 4 ? '/' + p.split('/').slice(3).join('/') : '/';
          else continue;
        }
        out.push({ file: rel, line: this.lineOf(rel, pos), method, path: p.replace(/\/+$/, '') || '/', base, via, how });
      }
    }
    return out;
  }
}

/** Nest proxy routes: forRoutes(...) in a module that applies createProxyMiddleware({target: env}). */
export function nestProxies(p) {
  const out = [];
  for (const [rel, t] of p.files) {
    if (!t.includes('forRoutes') || isTest(rel)) continue;
    const tgt = /createProxyMiddleware\(\s*\{[^}]*target\s*:\s*[^,}]*?([A-Z][A-Z0-9_]{3,})/s.exec(t);
    if (!tgt) continue; // forRoutes of ordinary middleware (auth, raw body) adds no route
    for (const m of t.matchAll(/forRoutes\(\s*(?:\{\s*path\s*:\s*)?(["'`])([^"'`]+)\1/g)) {
      const pth = m[2].replace(/\$\{([A-Z][A-Z0-9_]+)\}/g, (_, k) => p.consts.get(k) ?? ':param');
      out.push({ m: 'ALL', p: '/' + pth.replace(/^\/+/, ''), f: rel, l: p.lineOf(rel, m.index), kind: 'nest_proxy', targetEnv: tgt[1] });
    }
  }
  return out;
}

/** GraphQL: resolvers (@Query/@Mutation, names passed into resolver factories) and client operations. */
export function graphql(p) {
  const resolvers = [], ops = [];
  const factories = new Map();
  for (const [rel, t] of p.files) {
    if (isTest(rel) || !/@(Query|Mutation)/.test(t)) continue;
    for (const [name, praw, , bstart, bend] of fnDefs(t)) {
      const ps = paramsOf(praw);
      const body = t.slice(bstart, bend);
      const idx = [...body.matchAll(/name\s*:\s*([A-Za-z_]\w*)\s*[,}]/g)].map((x) => ps.indexOf(x[1])).filter((i) => i >= 0);
      if (idx.length && /@(Query|Mutation)/.test(body)) factories.set(name, idx);
    }
  }
  for (const [rel, t] of p.files) {
    if (isTest(rel)) continue;
    if (/@nestjs\/graphql|type-graphql|@Resolver/.test(t)) {
      for (const m of t.matchAll(/@(Query|Mutation|Subscription)\(/g)) {
        const at = m.index + m[0].length, close = balanced(t, at);
        const a = t.slice(at, close - 1);
        const nm = /name\s*:\s*["']([^"']+)/.exec(a);
        const after = t.slice(close, close + 300).replace(/@\w+\((?:[^()]|\([^()]*\))*\)/g, '');
        const mm = /(?:async\s+)?([A-Za-z_$][\w$]*)\s*\(/.exec(after);
        const field = nm ? nm[1] : mm && !/name\s*:/.test(a) ? mm[1] : null;
        if (field) resolvers.push({ m: 'POST', p: '/graphql#' + field, f: rel, l: p.lineOf(rel, m.index), kind: 'graphql', name: field });
      }
    }
    for (const [fname, idx] of factories) {
      for (const m of t.matchAll(new RegExp(`extends\\s+${esc(fname)}\\s*\\(`, 'g'))) {
        const at = m.index + m[0].length;
        const a = splitArgs(t.slice(at, balanced(t, at) - 1));
        for (const i of idx) if (i < a.length && /^["'][^"']+["']$/.test(a[i])) resolvers.push({ m: 'POST', p: '/graphql#' + a[i].slice(1, -1), f: rel, l: p.lineOf(rel, m.index), kind: 'graphql_factory', name: a[i].slice(1, -1) });
      }
    }
    for (const m of t.matchAll(/(?<![\w.])(?:gql|graphql)\s*`/g)) {
      let j = m.index + m[0].length, doc = '';
      while (j < t.length && t[j] !== '`') {
        if (t[j] === '$' && t[j + 1] === '{') { j = balanced(t, j + 2, '{', '}'); continue; }
        if (t[j] === '\\') { j += 2; continue; }
        doc += t[j]; j++;
      }
      doc = doc.replace(/#[^\n]*/g, '');
      const om = /\b(query|mutation|subscription)\b[^{]*\{/.exec(doc) || (!/\bfragment\b/.test(doc.slice(0, 40)) ? /^\s*\{/.exec(doc) : null);
      if (!om) continue;
      let k = om.index + om[0].length, depth = 1;
      const fields = [];
      while (k < doc.length && depth) {
        const c = doc[k];
        if (c === '{') depth++;
        else if (c === '}') depth--;
        else if (c === '(') { k = balanced(doc, k + 1); continue; }
        else if (depth === 1) {
          const fm = /^([A-Za-z_]\w*)\s*(?::\s*([A-Za-z_]\w*))?/.exec(doc.slice(k, k + 200));
          if (fm) { if (fm[1] !== 'on' && fm[1] !== 'fragment' && doc.slice(k - 3, k) !== '...') fields.push(fm[2] || fm[1]); k += fm[0].length; continue; }
        }
        k++;
      }
      for (const f of new Set(fields)) ops.push({ file: rel, line: p.lineOf(rel, m.index), method: 'POST', path: '/graphql#' + f, via: 'gql', how: 'exact', base: null });
    }
  }
  return { resolvers, ops };
}

function routeRegex(p) {
  let s = esc(p.replace(/\/+$/, '') || '/');
  s = s.replace(/:[A-Za-z_]\w*\\\?/g, '[^/]*').replace(/:[A-Za-z_]\w*/g, '[^/]+').replace(/\\\*/g, '.*');
  return new RegExp('^' + s + '$');
}

/** Each call matched to its best endpoint: specific beats :param beats a wildcard. */
export function match(calls, routes) {
  const compiled = routes.map((r, i) => [i, routeRegex(r.p), r.p.replace(/:\w+|\*/g, '').length, r.p.includes('*'), r]);
  const out = [];
  for (const c of calls) {
    const cp = c.path.replace(/:param/g, 'X');
    let best = null;
    for (const [i, rx, lit, wild, r] of compiled) {
      if (!rx.test(cp)) continue;
      const score = [wild ? 0 : 1, lit, r.m === c.method || r.m === 'ALL' ? 1 : 0];
      if (!best || score[0] > best[0][0] || (score[0] === best[0][0] && (score[1] > best[0][1] || (score[1] === best[0][1] && score[2] > best[0][2])))) best = [score, i, r];
    }
    if (best) out.push({ ...c, route: best[1], routeP: best[2].p, routeF: best[2].f, wildcard: best[0][0] === 0 });
  }
  return out;
}

/** Same-repository links only into the app's own route handlers (Next route.ts, pages/api, SvelteKit +server). */
export function crossesApps(l) {
  if (l.routeF === l.file) return false;
  if (l.routeF.split('/')[0] !== l.file.split('/')[0]) return true;
  return /(^|\/)(route\.[jt]s|\+server\.[jt]s)$|(^|\/)pages\/api\//.test(l.routeF);
}

/**
 * @param routes Floorplan's routes ({m, p, f})
 * @returns { links, extraRoutes, stats }: links in the prototype's shape; extraRoutes are proxy
 *          and GraphQL endpoints Floorplan doesn't list (to append to graph.routes)
 */
export function findCalls(root, repos, routes) {
  const p = new Project(root, repos);
  const proxies = nestProxies(p);
  const { resolvers, ops } = graphql(p);
  const extraRoutes = [...proxies, ...resolvers];
  const all = [...routes.map((r, i) => ({ ...r, idx: i })), ...extraRoutes.map((r, i) => ({ ...r, idx: routes.length + i }))];
  const calls = p.calls();
  const links = match([...calls, ...ops], all).filter(crossesApps).map((l) => ({ ...l, route: all[l.route].idx }));
  return { links, extraRoutes, stats: { files: p.files.size, wrappers: p.wrappers.size, maps: p.maps.size, proxies: proxies.length, resolvers: resolvers.length, gqlOps: ops.length, calls: calls.length } };
}
