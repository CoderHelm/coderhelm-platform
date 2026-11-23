// Native Android screen detector.
//  - AndroidManifest.xml <activity>: each activity is a screen; LAUNCHER = entry; <data> = deep links.
//  - res/navigation/*.xml: <fragment|dialog|activity> destinations, <action app:destination> edges,
//    <deepLink app:uri> routes, nested <navigation> and <include> as groups.
//  - Jetpack Compose Navigation 2: composable("route")/composable<Route>/dialog/navigation(...) and
//    Navigation 3: entry<Key> { ... }; edges from navController.navigate(...) / backStack.add(...).
// detect(root) -> { screens:[{name, kind, route, file, line, navigator, group, gate}], edges:[...] }
import path from 'node:path';
import { listFiles, read, stripComments, closeOf, lineIndex } from '../lib/text.mjs';

const P = path.posix;
const attr = (tag, k) => new RegExp(`(?:android|app):${k}\\s*=\\s*"([^"]*)"`).exec(tag)?.[1] ?? null;

export function detect(root, { maxFiles = 60000 } = {}) {
  const t0 = Date.now();
  const files = listFiles(root, { exts: /\.(xml|kt|java)$/, max: maxFiles });
  const screens = [], edges = [];
  for (const f of files.filter((x) => /(^|\/)AndroidManifest\.xml$/.test(x) && !/\/(test|androidTest|debug)\//.test(x))) {
    const s = read(root, f) || ''; const L = lineIndex(s);
    const pkg = /\bpackage\s*=\s*"([^"]+)"/.exec(s)?.[1] || '';
    for (const m of s.matchAll(/<activity(?=[\s>/])([^>]*?)(\/>|>([\s\S]*?)<\/activity>)/g)) {
      let name = attr(m[1], 'name'); if (!name) continue;
      if (name.startsWith('.')) name = pkg + name;
      const body = m[3] || '';
      const launcher = /android\.intent\.category\.LAUNCHER/.test(body);
      const links = [...body.matchAll(/<data\b([^>]*)\/?>/g)].map((d) => { const sc = attr(d[1], 'scheme'); const h = attr(d[1], 'host'); const p = attr(d[1], 'path') || attr(d[1], 'pathPrefix') || attr(d[1], 'pathPattern') || ''; return sc || h ? `${sc || '*'}://${h || ''}${p}` : null; }).filter(Boolean);
      screens.push({ name: name.split('.').pop(), cls: name, kind: 'activity', route: links[0] || null, deepLinks: links, entry: launcher, exported: attr(m[1], 'exported') === 'true', file: f, line: L(m.index), navigator: 'manifest', group: null, gate: null });
    }
  }
  // navigation graphs
  for (const f of files.filter((x) => /(^|\/)res\/navigation[^/]*\/[^/]+\.xml$/.test(x))) {
    const s = read(root, f) || ''; const L = lineIndex(s);
    const graph = P.basename(f, '.xml');
    const stack = [];
    for (const m of s.matchAll(/<(\/?)(navigation|fragment|dialog|activity|include|action|deepLink)\b([^>]*?)(\/?)>/g)) {
      const [, close, tag, attrs, self] = m;
      if (close) { if (tag === 'navigation' || tag === 'fragment' || tag === 'dialog' || tag === 'activity') stack.pop(); continue; }
      const id = (attr(attrs, 'id') || '').replace(/^@\+?id\//, '');
      if (tag === 'navigation') { if (!self) stack.push({ tag, id: id || graph, start: attr(attrs, 'startDestination') }); continue; }
      if (tag === 'include') { screens.push({ name: (attr(attrs, 'graph') || '').replace(/^@navigation\//, ''), kind: 'include', route: null, file: f, line: L(m.index), navigator: graph, group: stack.filter((x) => x.tag === 'navigation').map((x) => x.id).slice(1).join('/') || null, include: true }); continue; }
      if (tag === 'action') { const from = [...stack].reverse().find((x) => x.tag !== 'navigation')?.id || stack[stack.length - 1]?.id; edges.push({ from, to: (attr(attrs, 'destination') || '').replace(/^@id\//, '') || '(pop)', id, file: f, line: L(m.index), kind: 'action' }); continue; }
      if (tag === 'deepLink') { const host = [...stack].reverse().find((x) => x.tag !== 'navigation'); const sc = screens.find((x) => x.file === f && x.id === host?.id); if (sc) { sc.route = sc.route || attr(attrs, 'uri'); } continue; }
      const cls = attr(attrs, 'name');
      screens.push({ name: id || (cls || '').split('.').pop(), id, cls, label: attr(attrs, 'label'), kind: tag, route: null, file: f, line: L(m.index), navigator: graph, group: stack.filter((x) => x.tag === 'navigation').map((x) => x.id).slice(1).join('/') || null, gate: null });
      if (!self) stack.push({ tag, id });
    }
  }
  // Compose navigation (Nav2 + Nav3)
  for (const f of files.filter((x) => /\.kt$/.test(x) && !/\/(test|androidTest)\//.test(x))) {
    const raw = read(root, f) || ''; if (!/composable|NavHost|entry<|navigation\(|navigation</.test(raw)) continue;
    const s = stripComments(raw); const L = lineIndex(s);
    for (const m of s.matchAll(/\b(composable|dialog|navigation|entry|bottomSheet)\s*(?:<\s*([\w.]+)\s*>)?\s*(?:\(\s*(?:route\s*=\s*)?(?:"([^"]*)"|([\w.]+)\s*(?:[,)]))?|(?=\{))/g)) {
      const [, fn, typeArg, strRoute, identRoute] = m;
      if (fn === 'entry' && (!typeArg || !/navigation3|entryProvider/.test(s))) continue;
      if (fn !== 'entry' && !/androidx\.navigation|NavHost|NavGraphBuilder/.test(s)) continue;
      if (!typeArg && strRoute == null && !identRoute) continue;
      if (fn === 'navigation' && !/startDestination/.test(s.slice(m.index, m.index + 300))) continue;
      // the destination's composable: first PascalCase call in its lambda
      const brace = s.indexOf('{', m.index); const close = brace > 0 ? closeOf(s, brace) : -1;
      const body = close > 0 ? s.slice(brace + 1, Math.min(close, brace + 1500)) : '';
      const screenFn = /\b([A-Z]\w*(?:Screen|Route|Page|Dialog|Sheet)\w*)\s*\(/.exec(body)?.[1] || /\b([A-Z]\w+)\s*\(/.exec(body)?.[1] || null;
      screens.push({ name: typeArg || strRoute || identRoute, kind: fn === 'navigation' ? 'graph' : `compose-${fn}`, route: strRoute ?? null, composable: screenFn, file: f, line: L(m.index), navigator: fn === 'entry' ? 'nav3' : 'NavHost', group: null, gate: null });
    }
    for (const m of s.matchAll(/\b(?:navController|navigator|backStack|navigationState)\??\.(?:navigate|add|navigateTo\w*|push)\s*\(\s*(?:"([^"$]*)|([A-Z][\w.]*))/g)) edges.push({ fromFile: f, line: L(m.index), to: m[1] ?? m[2], kind: 'compose' });
  }
  // Activity starts from code: startActivity(Intent(this, X::class.java)) / new Intent(ctx, X.class)
  for (const f of files.filter((x) => /\.(kt|java)$/.test(x) && !/\/(test|androidTest)\//.test(x))) {
    const s = read(root, f) || ''; if (!/Intent\(/.test(s)) continue;
    const L = lineIndex(s);
    for (const m of s.matchAll(/Intent\(\s*[^,()]+(?:\([^)]*\))?\s*,\s*([A-Z]\w*)(?:::class\.java|\.class)/g)) edges.push({ fromFile: f, line: L(m.index), to: m[1], kind: 'intent' });
  }
  return { screens, edges, stats: { files: files.length, ms: Date.now() - t0 } };
}

