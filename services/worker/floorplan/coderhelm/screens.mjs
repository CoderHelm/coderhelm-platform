// Screens Floorplan doesn't find (React Navigation, Expo Router at its real root, Sanity Studio,
// screens a dynamic route dispatches to by type, Flutter, Android, iOS), merged into graph.screens through Floorplan's own
// exported buildScreens: each detected screen becomes a hint ({fw, url, target}), and Floorplan
// computes what is behind it with its own code. Our fields go on `nav`.
//
// Floorplan's Expo rule (any app/**.tsx that imports react-native) reads a React Native app whose
// source folder is called app/ as hundreds of fake screens; in a React Native repository its
// expo and remix screens are kept only under a router root our Expo detector found.
import fs from 'node:fs';
import path from 'node:path';
import { listFiles, read, isTest, ev, startClock, stopClock } from './lib/text.mjs';
import { fp } from './lib/fp.mjs';
import * as RN from './screens/react-navigation.mjs';
import * as EXPO from './screens/expo-router.mjs';
import * as SANITY from './screens/sanity.mjs';
import * as DISPATCH from './screens/dispatch.mjs';
import * as FLUTTER from './screens/flutter.mjs';
import * as ANDROID from './screens/android.mjs';
import * as IOS from './screens/ios.mjs';

export const CAPS = { perApp: 400, total: 1500, edgesPerScreen: 12, edgesTotal: 4000, detectorMs: 20000 };
const GATE = { 'signed-in': 'signed_in', 'signed-out': 'signed_out' };

/** Which detectors a repository needs, from cheap signals in its manifests and file names, and
 *  the dynamic routes the map already has there. */
function signals(root, repo, routes = []) {
  const files = listFiles(path.join(root, repo), { exts: /(^package\.json$|^pubspec\.yaml$|^AndroidManifest\.xml$|\.swift$|\.storyboard$|^sanity\.config\.[jt]sx?$|\.(tsx?|jsx?|vue|astro|svelte)$)/, max: 60000 });
  const pkgs = files.filter((f) => /(^|\/)package\.json$/.test(f)).map((f) => read(path.join(root, repo), f) || '').join('\n');
  const rnApp = /"(react-native|expo)"\s*:/.test(pkgs);
  const flutter = files.some((f) => /(^|\/)pubspec\.yaml$/.test(f));
  return {
    rn: /"@react-navigation\//.test(pkgs),
    expo: /"expo-router"\s*:/.test(pkgs),
    rnApp: rnApp || /"react-native"/.test(pkgs),
    sanity: files.some((f) => /(^|\/)sanity\.config\.[jt]sx?$/.test(f)),
    dispatch: routes.length > 0 || files.some((f) => DISPATCH.routeOf(f)),
    flutter,
    android: !rnApp && !flutter && files.some((f) => /(^|\/)AndroidManifest\.xml$/.test(f)),
    ios: !rnApp && !flutter && files.some((f) => /\.(swift|storyboard)$/.test(f)),
  };
}

function timed(name, fn, notes) {
  startClock(CAPS.detectorMs);
  const t = Date.now();
  let r = null;
  try { r = fn(); } catch (e) { notes.push(`The ${name} screen detector failed: ${String(e.message || e).slice(0, 160)}`); }
  if (stopClock()) notes.push(`The ${name} screen detector stopped at its ${CAPS.detectorMs / 1000} s limit; its list may be short.`);
  return [r, Date.now() - t];
}

const pseudo = (group, name) => `/(${String(group || 'App').replace(/[^\w-]/g, '_')})/${String(name).replace(/[\s#?]+/g, '_')}`;

/** Every repository's detector results, as hints in Floorplan's shape plus our fields. `screens`
 *  are the ones Floorplan found (any router it reads): a dynamic one may dispatch by type. */
export function detect(root, repos, { screens = [] } = {}) {
  const notes = [], ms = {}, hints = [], edges = [], expoRoots = new Map(), rnRepos = new Set();
  let unnamed = 0;
  const add = (repo, h) => { if (h.target && h.url) hints.push({ ...h, repo, target: `${repo}/${h.target}` }); };
  for (const repo of repos) {
    const routes = screens.filter((s) => String(s.file || '').startsWith(repo + '/') && /(^|\/)(\*|:[^/]+|\[[^\]]+\])$/.test(String(s.url || ''))).map((s) => ({ file: s.file.slice(repo.length + 1), url: s.url }));
    const sig = signals(root, repo, routes);
    const dir = path.join(root, repo);
    if (sig.rnApp) rnRepos.add(repo);
    const run = (name, fn) => { const [r, t] = timed(name, fn, notes); ms[name] = (ms[name] || 0) + t; return r; };
    if (sig.rn) {
      const r = run('react-navigation', () => RN.detect(dir));
      const seen = new Map();
      for (const s of r?.screens || []) {
        if (!s.leaf) continue;
        if (!s.name) { unnamed++; continue; }
        const target = s.componentFile || s.file;
        const key = `${s.name}|${target}`;
        if (seen.has(key)) { seen.get(key).nav.navigators.push(s.navigator); continue; }
        const h = { fw: 'react-navigation', url: s.route || pseudo(s.navigator, s.name), target, name: s.name, kind: 'screen', nav: { navigator: s.navigator, navigators: [s.navigator], group: s.group || null, gate: GATE[s.gate] || null, route: s.route || null, file: `${repo}/${s.file}`, line: s.line, gateFrom: s.gate ? `${repo}/${s.file}` : null } };
        seen.set(key, h);
        add(repo, h);
      }
      for (const e of r?.edges || []) edges.push({ repo, kind: e.kind, fromFile: e.fromFile, line: e.line, toName: e.to });
    }
    if (sig.expo) {
      const r = run('expo-router', () => EXPO.detect(dir));
      expoRoots.set(repo, (r?.roots || []).map((x) => `${repo}/${x}`));
      for (const s of r?.screens || []) {
        const m = /^(.+?):(\d+) /.exec(s.gateEvidence || '');
        add(repo, { fw: 'expo-router', url: s.route, target: s.file, name: s.name, kind: 'screen', nav: { navigator: s.navigator || null, group: s.group, gate: GATE[s.gate] || null, route: s.route, file: `${repo}/${s.file}`, line: 1, gateAt: m ? [`${repo}/${m[1]}`, +m[2]] : null } });
      }
      for (const e of r?.edges || []) edges.push({ repo, kind: e.kind, fromFile: e.fromFile, line: e.line, toRoute: e.to });
    }
    if (sig.sanity) {
      const r = run('sanity', () => SANITY.detect(dir));
      for (const st of r?.studios || []) {
        const base = st.workspaces[0]?.basePath === '/' ? '' : st.workspaces[0]?.basePath || '';
        for (const t of st.tools) add(repo, { fw: 'sanity-tool', url: `${base}/${t.name}`, target: t.file, name: t.title || t.name, kind: 'tool', nav: { navigator: 'studio', group: 'tools', gate: null, file: `${repo}/${t.file}`, line: t.line } });
        for (const k of st.pageKinds) add(repo, { fw: 'sanity-doc', url: `/(structure)/${k.name}`, target: k.file, name: k.title || k.name, kind: 'page-kind', nav: { navigator: 'studio', group: 'structure', gate: null, file: `${repo}/${k.file}`, line: k.line, singleton: k.singleton || undefined } });
      }
    }
    if (sig.dispatch) {
      // each screen a dynamic route dispatches to: its renderer's file and line, the URL the resolver
      // gives its type (route = null when only the route's own pattern is known), and where the
      // dispatch and the URL were read
      const r = run('dispatch', () => DISPATCH.detect(dir, { routes }));
      for (const c of r?.dispatchers || []) for (const k of c.screens) add(repo, { fw: 'dispatch', url: k.url, target: k.file, name: k.name, kind: 'screen', nav: { navigator: c.route, group: 'dispatched', gate: null, route: k.urlFrom ? k.url : null, type: k.type, types: k.types, renderer: k.renderer, dispatcher: `${repo}/${c.file}`, urlFrom: k.urlFrom ? k.urlFrom.how : 'route', file: `${repo}/${k.file}`, line: k.line, also: [[`${repo}/${k.table.file}`, k.table.line, 0.9], ...(k.urlFrom ? [[`${repo}/${k.urlFrom.file}`, k.urlFrom.line, 0.85]] : [])] } });
    }
    if (sig.flutter) {
      const r = run('flutter', () => FLUTTER.detect(dir));
      for (const s of r?.screens || []) add(repo, { fw: `flutter-${s.router}`, url: s.route && s.route.startsWith('/') ? s.route : pseudo(s.router, s.name), target: s.widgetFile || s.file, name: s.widget || s.name, kind: 'screen', nav: { navigator: s.router, group: s.group || null, gate: GATE[s.gate] || null, guards: s.guards?.length ? s.guards : undefined, route: s.route || null, file: `${repo}/${s.file}`, line: s.line } });
      for (const e of r?.edges || []) edges.push({ repo, kind: e.kind, fromFile: e.fromFile, line: e.line, toRoute: e.to.startsWith('/') ? e.to : null, toName: e.to.startsWith('/') ? null : e.to });
    }
    if (sig.android || sig.ios) {
      // native classes live in their own files: MainActivity -> .../MainActivity.kt
      const byClass = new Map();
      for (const f of listFiles(dir, { exts: /\.(kt|java|swift|m)$/, max: 60000 })) byClass.set(path.basename(f).replace(/\.\w+$/, ''), f);
      if (sig.android) {
        const r = run('android', () => ANDROID.detect(dir));
        for (const s of r?.screens || []) {
          if (s.include || s.kind === 'graph') continue;
          const cls = (s.cls || s.composable || '').split('.').pop();
          add(repo, { fw: `android-${s.kind}`, url: pseudo(s.navigator, s.name), target: byClass.get(cls) || s.file, name: s.label || s.name, kind: 'screen', nav: { navigator: s.navigator || null, group: s.group || null, gate: null, route: s.route || null, entry: s.entry || undefined, file: `${repo}/${s.file}`, line: s.line } });
        }
        for (const e of r?.edges || []) edges.push({ repo, kind: e.kind, fromFile: e.fromFile || e.file, line: e.line, fromName: e.from || null, toName: e.to });
      }
      if (sig.ios) {
        const r = run('ios', () => IOS.detect(dir));
        for (const s of r?.screens || []) add(repo, { fw: `ios-${s.kind}`, url: pseudo(s.group || s.kind, s.name), target: (s.view && byClass.get(s.view)) || byClass.get(s.name) || s.file, name: s.name, kind: 'screen', nav: { navigator: s.kind, group: s.group || null, gate: null, file: `${repo}/${s.file}`, line: s.line } });
        for (const e of r?.edges || []) edges.push({ repo, kind: e.kind, fromFile: e.fromFile || e.file, line: e.line, fromName: e.from || null, toName: e.to });
      }
    }
  }
  if (unnamed) notes.push(`${unnamed} screens are registered with a name the code builds at run time; they are not listed.`);
  return { hints, edges, expoRoots, rnRepos, notes, ms };
}

/** Screens per app capped: entries, routed and gated screens first; the rest counted in a note. */
function capHints(hints, notes) {
  const byRepo = new Map();
  for (const h of hints) { if (!byRepo.has(h.repo)) byRepo.set(h.repo, []); byRepo.get(h.repo).push(h); }
  const out = [];
  for (const [repo, hs] of byRepo) {
    const rank = (h) => (h.nav.entry ? 0 : 3) + (h.nav.route ? 0 : 1) + (h.nav.gate ? 0 : 1);
    const sorted = [...hs].sort((a, b) => rank(a) - rank(b));
    if (sorted.length > CAPS.perApp) notes.push(`${sorted.length - CAPS.perApp} more screens in ${repo} are not listed (the map lists ${CAPS.perApp} per app).`);
    out.push(...sorted.slice(0, CAPS.perApp));
  }
  if (out.length > CAPS.total) notes.push(`${out.length - CAPS.total} more screens are not listed (the map lists ${CAPS.total}).`);
  return out.slice(0, CAPS.total);
}

/**
 * Merges detected screens into graph.screens (in place) and adds nav links.
 * @returns {added, removed, navLinks, ms}
 */
export function mergeScreens({ graph, scan, root, found }) {
  const notes = found.notes;
  const t0 = Date.now();
  // Floorplan's expo/remix screens in a React Native repository are trusted only under a real router root
  const before = graph.screens?.length || 0;
  const roots = [...found.expoRoots.values()].flat();
  graph.screens = (graph.screens || []).filter((s) => {
    if (s.fw !== 'expo' && s.fw !== 'remix') return true;
    const repo = String(s.file).split('/')[0];
    // our Expo detector found the router root here: its screens replace Floorplan's
    if ((found.expoRoots.get(repo) || []).length) return false;
    if (!found.rnRepos.has(repo)) return true;
    return roots.some((r) => s.file.startsWith(r + '/'));
  });
  const removed = before - graph.screens.length;
  if (removed) notes.push(`${removed} screens Floorplan read from a React Native app's app/ folder were dropped: no router root holds them.`);
  const fileSet = new Set(graph.files.map((f) => f.p));
  const floorplanFiles = new Set(graph.screens.map((s) => s.file));
  // ours give way to Floorplan's screen for the same file (its strong routers), except where
  // its screens were just found to be fake
  // (a screen a route dispatches to is its own screen even in a file Floorplan lists: the renderers
  // often sit in the route file, or several in one file)
  const hints = capHints(found.hints.filter((h) => fileSet.has(h.target) && !isTest(h.target) && (h.fw === 'dispatch' || !floorplanFiles.has(h.target))), notes);
  const files = new Map();
  for (const h of hints) { if (!files.has(h.target)) files.set(h.target, { path: h.target, screens: [] }); files.get(h.target).screens.push({ fw: h.fw, url: h.url, target: h.target }); }
  const ours = fp.buildScreens({ graph, scan: { ...scan, files: [...files.values()] }, root });
  const norm = (u) => { let s = ('/' + String(u).split(/[?#]/)[0].replace(/\(([^()]*)\)/g, (m0, inner) => (/^[\w-]+$/.test(inner) ? m0 : ''))).replace(/\/{2,}/g, '/'); if (s.length > 1) s = s.replace(/\/$/, ''); return s; };
  const byKey = new Map(hints.map((h) => [`${h.target}|${norm(h.url)}`, h]));
  const ids = new Set(graph.screens.map((s) => s.id));
  const added = [];
  for (const s of ours) {
    const h = byKey.get(`${s.file}|${s.url}`);
    if (!h || ids.has(s.id)) continue;
    ids.add(s.id);
    const { file, line, gateAt, gateFrom, also, ...nav } = h.nav;
    const evidence = [ev(file, line, 'code', 0.95)];
    for (const [f, l, c] of also || []) if (fileSet.has(f)) evidence.push(ev(f, l, 'code', c));
    if (gateAt) evidence.push(ev(gateAt[0], gateAt[1], 'code', 0.85));
    else if (h.nav.gate && gateFrom && gateFrom !== file) evidence.push(ev(gateFrom, line, 'code', 0.8));
    s.name = h.name || s.name;
    s.kind = h.kind;
    s.source = 'coderhelm';
    s.nav = { app: h.repo, ...Object.fromEntries(Object.entries(nav).filter(([, v]) => v !== undefined)), evidence };
    if (s.nav.navigators && s.nav.navigators.length < 2) delete s.nav.navigators;
    // what is behind a dispatched screen is what its renderer uses, not its whole file
    if (h.fw === 'dispatch' && h.nav.renderer) { try { s.behind = focusedBehind(graph, root, s.file, h.nav.renderer); } catch { /* Floorplan's walk stays */ } }
    added.push(s);
  }
  graph.screens.push(...added);
  const navLinks = navEdges(graph, found, added);
  return { added: added.length, removed, navLinks: navLinks.length, ms: Date.now() - t0, links: navLinks };
}

/** Screen-to-screen links (k "nav"): from the file where the navigation call sits to the target screen's file. */
function navEdges(graph, found, added) {
  const fileSet = new Set(graph.files.map((f) => f.p));
  const byName = new Map(), byRoute = new Map(), byFile = new Map();
  for (const s of added) {
    const repo = s.nav.app;
    if (!byName.has(`${repo}|${s.name}`)) byName.set(`${repo}|${s.name}`, s);
    const base = path.basename(s.file).replace(/\.\w+$/, '');
    if (!byName.has(`${repo}|${base}`)) byName.set(`${repo}|${base}`, s);
    if (s.nav.route) byRoute.set(`${repo}|${s.nav.route}`, s);
    if (s.url && !s.url.startsWith('/(')) byRoute.set(`${repo}|${s.url}`, s);
    if (!byFile.has(s.file)) byFile.set(s.file, []);
    byFile.get(s.file).push(s);
  }
  // a screen also owns the files Floorplan walked behind it (its parts)
  const owners = new Map();
  for (const s of added) for (const b of s.behind || []) if (b.kind === 'screen' || b.kind === 'part') { const f = b.id.replace(/^f:/, ''); if (!owners.has(f)) owners.set(f, []); owners.get(f).push(s.id); }
  const routeOf = (repo, to) => {
    const exact = byRoute.get(`${repo}|${to}`);
    if (exact) return exact;
    for (const [k, s] of byRoute) {
      if (!k.startsWith(repo + '|')) continue;
      const pat = k.slice(repo.length + 1);
      const rx = new RegExp('^' + pat.replace(/[.+?^${}()|[\]\\]/g, '\\$&').replace(/:[^/]+/g, '[^/]+').replace(/\*/g, '.*') + '/?$');
      if (rx.test(to)) return s;
    }
    return null;
  };
  const links = new Map(), perFrom = new Map();
  for (const e of found.edges) {
    const to = e.toRoute ? routeOf(e.repo, e.toRoute) : e.toName ? byName.get(`${e.repo}|${String(e.toName).replace(/^\./, '').split('.').pop()}`) || byName.get(`${e.repo}|${e.toName}`) : null;
    if (!to) continue;
    let fromFile = e.fromFile ? `${e.repo}/${e.fromFile}` : null;
    if (!fromFile && e.fromName) fromFile = byName.get(`${e.repo}|${e.fromName}`)?.file || null;
    if (!fromFile || !fileSet.has(fromFile) || isTest(fromFile) || fromFile === to.file) continue;
    const key = `${fromFile}>${to.file}`;
    if (links.has(key)) { const l = links.get(key); if (l.evidence.length < 3) l.evidence.push(ev(fromFile, e.line, 'code', 0.9)); l.w++; continue; }
    if ((perFrom.get(fromFile) || 0) >= CAPS.edgesPerScreen || links.size >= CAPS.edgesTotal) continue;
    perFrom.set(fromFile, (perFrom.get(fromFile) || 0) + 1);
    const from = [...new Set([...(byFile.get(fromFile) || []).map((s) => s.id), ...(owners.get(fromFile) || [])])].slice(0, 6);
    links.set(key, { a: 'f:' + fromFile, b: 'f:' + to.file, k: 'nav', w: 1, labels: [String(e.kind || 'navigate')], from, to: to.id, inferred: false, evidence: [ev(fromFile, e.line, 'code', 0.9)] });
  }
  return [...links.values()];
}

/**
 * What is behind one renderer of a file that holds several (a dispatched screen): the files its own
 * code uses (followed only through the names it uses, a few levels down), the requests made inside
 * those parts, the server code that handles them and the outside services they reach. Same items
 * and notes as Floorplan's own walk.
 */
export function focusedBehind(graph, root, file, symbol, { depth = 3, cap = 60 } = {}) {
  const files = new Map(graph.files.map((f) => [f.p, f]));
  const routes = graph.routes || [];
  const outLinks = new Map();
  for (const l of graph.links || []) { if (!outLinks.has(l.a)) outLinks.set(l.a, []); outLinks.get(l.a).push(l); }
  const lines = new Map();
  const linesOf = (p) => { if (!lines.has(p)) { const t = read(root, p); lines.set(p, t == null ? null : t.split('\n')); } return lines.get(p); };
  const rangesOf = (p, focus) => {
    if (!focus) return null;
    const s = (files.get(p)?.s || []).map((x) => ({ name: x[0], line: x[2] })).sort((a, b) => a.line - b.line);
    const n = linesOf(p)?.length || 0;
    const rs = [];
    s.forEach((x, i) => { if (focus.has(x.name)) rs.push([x.line, (s[i + 1]?.line || n + 1) - 1]); });
    if (!rs.length) {
      // a symbol Floorplan does not list: from its declaration to the next top-level line
      const ls = linesOf(p) || [];
      for (const name of focus) {
        const at = ls.findIndex((l) => new RegExp(`^(?:export\\s+)?(?:default\\s+)?(?:async\\s+)?(?:function|const|let|var|class)\\s*\\*?\\s*${name.replace(/\$/g, '\\$')}\\b`).test(l));
        if (at < 0) continue;
        let end = at + 1;
        while (end < ls.length && !/^(?:export\s|function\s|const\s|let\s|var\s|class\s|async\s)/.test(ls[end])) end++;
        rs.push([at + 1, end]);
      }
    }
    return rs.length ? rs : null;
  };
  const textOf = (p, rs) => { const ls = linesOf(p); if (!ls) return ''; return rs ? rs.map(([a, b]) => ls.slice(a - 1, b).join('\n')).join('\n') : ls.join('\n'); };
  const inRanges = (line, rs) => !rs || rs.some(([a, b]) => line >= a && line <= b);
  const esc = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const stem = (p) => { const b = path.posix.basename(p).replace(/\.[^.]+$/, ''); return b === 'index' ? path.posix.basename(path.posix.dirname(p)) : b; };

  const seen = new Map([[file, { d: 0, focus: new Set([symbol]), parent: null }]]);
  const queue = [file];
  while (queue.length) {
    const p = queue.shift();
    const v = seen.get(p);
    if (v.d >= depth) continue;
    const rs = rangesOf(p, v.focus);
    const text = textOf(p, rs);
    const whole = v.focus ? textOf(p, null) : text;
    for (const l of outLinks.get('f:' + p) || []) {
      if (l.k !== 'import' || !l.b.startsWith('f:')) continue;
      const to = l.b.slice(2);
      if (!files.has(to) || isTest(to) || to === file) continue;
      const names = (l.names || []).filter((n) => n !== 'default' && n !== '*');
      let used = names.filter((n) => new RegExp(`(^|[^\\w$.])${esc(n)}\\b`).test(text));
      let focus = used.length ? new Set(used) : null;
      if (!used.length) {
        // a default or namespace import: the local name it is imported as, used in the text
        const st = esc(stem(to));
        const alias = [...whole.matchAll(new RegExp(`import\\s+(?:\\*\\s+as\\s+)?([A-Za-z_$][\\w$]*)[^;]*?from\\s*["'][^"']*${st}(?:\\.\\w+)?["']`, 'g'))].map((m) => m[1]);
        if (!alias.some((a) => new RegExp(`\\b${esc(a)}\\b`).test(text))) continue;
        focus = null;
      }
      const had = seen.get(to);
      if (had) {
        if (had.focus && (!focus || [...focus].some((n) => !had.focus.has(n)))) { had.focus = focus ? new Set([...had.focus, ...focus]) : null; queue.push(to); }
        continue;
      }
      if (seen.size >= cap) continue;
      seen.set(to, { d: v.d + 1, focus, parent: p });
      queue.push(to);
    }
  }
  const nameOf = (p) => files.get(p)?.name || stem(p);
  const items = [{ id: 'f:' + file, note: 'The screen itself', kind: 'screen' }];
  // shared building blocks (a UI kit, type files, helpers half the app imports) are not this screen's parts
  const indeg = new Map();
  for (const l of graph.links || []) if (l.k === 'import' && l.b.startsWith('f:')) indeg.set(l.b, (indeg.get(l.b) || 0) + 1);
  const appFiles = graph.files.filter((f) => f.n === files.get(file)?.n).length;
  const noise = (p) => /(^|\/)(ui|icons?|primitives|types?)\//i.test(p) || /\.(d|types?)\.[jt]sx?$|(^|\/)(types?|utils?|constants?)\.[jt]sx?$/i.test(p) || (indeg.get('f:' + p) || 0) >= Math.max(6, Math.round(appFiles * 0.05));
  const parts = [...seen.entries()].filter(([p]) => p !== file && !noise(p))
    .sort((a, b) => a[1].d - b[1].d || (files.get(b[0])?.loc || 0) - (files.get(a[0])?.loc || 0));
  for (const [p, v] of parts.slice(0, 4)) {
    const b = path.posix.basename(p);
    const http = (outLinks.get('f:' + p) || []).some((l) => l.k === 'http');
    const note = /^use[A-Z]/.test(b) || /(^|\/)hooks?\//.test(p) ? 'Logic this screen uses'
      : /^[a-z]/.test(b) && /\.(ts|js|mjs)$/.test(b) ? (http ? 'Talks to the server for this screen' : 'Helper code this screen uses')
        : v.d <= 1 ? 'A component on this screen' : `Part of ${nameOf(v.parent)}`;
    items.push({ id: 'f:' + p, note, kind: 'part' });
  }
  const reqs = [];
  const exts = new Map();
  for (const [p, v] of [...seen.entries()].sort((a, b) => a[1].d - b[1].d)) {
    const rs = rangesOf(p, v.focus);
    for (const l of outLinks.get('f:' + p) || []) {
      if (l.k === 'http') for (const [ri, line] of l.calls || []) { if (ri != null && routes[ri] && inRanges(line, rs) && !reqs.includes(ri)) reqs.push(ri); }
      if (l.b.startsWith('n:') && !exts.has(l.b)) exts.set(l.b, p);
    }
  }
  const routeName = (r) => String(r.name || `${r.m || ''} ${r.p || ''}`).trim();
  for (const ri of reqs.slice(0, 6)) items.push({ id: 'r:' + ri, note: `Asks the server for ${routeName(routes[ri]).replace(/^./, (c) => c.toLowerCase())}`, kind: 'request' });
  const servers = new Map();
  for (const ri of reqs.slice(0, 6)) { const f = routes[ri].f; if (f && files.has(f) && !servers.has(f)) servers.set(f, routeName(routes[ri])); }
  for (const [f, n] of [...servers].slice(0, 4)) {
    items.push({ id: 'f:' + f, note: `Server code that handles ${n.replace(/^./, (c) => c.toLowerCase())}`, kind: 'server' });
    for (const l of outLinks.get('f:' + f) || []) if (l.b.startsWith('n:') && !exts.has(l.b)) exts.set(l.b, f);
  }
  const nodes = new Map(graph.nodes.map((n) => [n.id, n]));
  for (const [id, via] of [...exts].filter(([id]) => nodes.get(id.slice(2))?.kind === 'external').slice(0, 3)) items.push({ id, note: `Outside service, used by ${nameOf(via)}`, kind: 'outside' });
  return items;
}
