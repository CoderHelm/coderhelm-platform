// Screens Floorplan doesn't find (React Navigation, Expo Router at its real root, Sanity Studio,
// CMS page kinds, Flutter, Android, iOS), merged into graph.screens through Floorplan's own
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
import * as CMS from './screens/cms-routes.mjs';
import * as FLUTTER from './screens/flutter.mjs';
import * as ANDROID from './screens/android.mjs';
import * as IOS from './screens/ios.mjs';

export const CAPS = { perApp: 400, total: 1500, edgesPerScreen: 12, edgesTotal: 4000, detectorMs: 20000 };
const GATE = { 'signed-in': 'signed_in', 'signed-out': 'signed_out' };

/** Which detectors a repository needs, from cheap signals in its manifests and file names. */
function signals(root, repo) {
  const files = listFiles(path.join(root, repo), { exts: /(^package\.json$|^pubspec\.yaml$|^AndroidManifest\.xml$|\.swift$|\.storyboard$|^sanity\.config\.[jt]sx?$|\.(tsx?|jsx?|vue|astro|svelte)$)/, max: 60000 });
  const pkgs = files.filter((f) => /(^|\/)package\.json$/.test(f)).map((f) => read(path.join(root, repo), f) || '').join('\n');
  const rnApp = /"(react-native|expo)"\s*:/.test(pkgs);
  const flutter = files.some((f) => /(^|\/)pubspec\.yaml$/.test(f));
  return {
    rn: /"@react-navigation\//.test(pkgs),
    expo: /"expo-router"\s*:/.test(pkgs),
    rnApp: rnApp || /"react-native"/.test(pkgs),
    sanity: files.some((f) => /(^|\/)sanity\.config\.[jt]sx?$/.test(f)),
    cms: files.some((f) => /(\[\[?\.\.\.[^\]]+\]\]?|\[[^\]/]+\]|\$)(\/(page|index|\+page))?\.(tsx?|jsx?|vue|astro|svelte)$/.test(f)),
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

/** Every repository's detector results, as hints in Floorplan's shape plus our fields. */
export function detect(root, repos) {
  const notes = [], ms = {}, hints = [], edges = [], expoRoots = new Map(), rnRepos = new Set();
  let unnamed = 0;
  const add = (repo, h) => { if (h.target && h.url) hints.push({ ...h, repo, target: `${repo}/${h.target}` }); };
  for (const repo of repos) {
    const sig = signals(root, repo);
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
    if (sig.cms) {
      const r = run('cms-routes', () => CMS.detect(dir));
      for (const c of r?.catchAlls || []) for (const k of c.pageKinds.slice(0, 80)) add(repo, { fw: 'cms-page-kind', url: `${c.route.replace(/\/\*$/, '').replace(/\/:\w+$/, '')}/(${k.name})`, target: k.file, name: k.name, kind: 'page-kind', nav: { navigator: c.route, group: 'page kinds', gate: null, file: `${repo}/${k.file}`, line: k.line, catchAll: `${repo}/${c.file}`, renderer: k.renderer } });
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
  const hints = capHints(found.hints.filter((h) => fileSet.has(h.target) && !isTest(h.target) && !floorplanFiles.has(h.target)), notes);
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
    const { file, line, gateAt, gateFrom, ...nav } = h.nav;
    const evidence = [ev(file, line, 'code', 0.95)];
    if (gateAt) evidence.push(ev(gateAt[0], gateAt[1], 'code', 0.85));
    else if (h.nav.gate && gateFrom && gateFrom !== file) evidence.push(ev(gateFrom, line, 'code', 0.8));
    s.name = h.name || s.name;
    s.kind = h.kind;
    s.source = 'coderhelm';
    s.nav = { app: h.repo, ...Object.fromEntries(Object.entries(nav).filter(([, v]) => v !== undefined)), evidence };
    if (s.nav.navigators && s.nav.navigators.length < 2) delete s.nav.navigators;
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
