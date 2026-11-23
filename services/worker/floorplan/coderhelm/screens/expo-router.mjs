// Expo Router screen detector. Unlike Floorplan's rule (any app/**.tsx importing
// react-native), it first finds the router ROOT, so a React Native app whose source folder is
// called app/ is not read as 1,000 routes:
//   1) require.context('<dir>') handed to <ExpoRoot>;  2) the expo-router plugin's `root` option;
//   3) app/ or src/app/ beside a package.json that depends on expo-router.
// detect(root) -> { roots, screens: [{name, route, file, line, navigator, group, gate, gateEvidence}], edges }
import fs from 'node:fs';
import path from 'node:path';
import { listFiles, read, stripComments, lineIndex, gateFromCondition, SIGNED_OUT } from '../lib/text.mjs';

const P = path.posix;
const SPECIAL = /^(_layout|\+html|\+not-found|\+native-intent|\+middleware|\+api)$|\+api$/;

export function detect(root, { maxFiles = 40000 } = {}) {
  const t0 = Date.now();
  const all = listFiles(root, { exts: /\.(tsx?|jsx?|json)$|^app\.config\.[jt]s$/, max: maxFiles });
  const roots = new Set();
  for (const pj of all.filter((f) => /(^|\/)package\.json$/.test(f))) {
    const s = read(root, pj) || '';
    if (!/"expo-router"\s*:/.test(s)) continue;
    const dir = P.dirname(pj) === '.' ? '' : P.dirname(pj);
    const pre = dir ? dir + '/' : '';
    // explicit roots: require.context in the entry, plugin option
    let explicit = false;
    for (const f of all.filter((x) => x.startsWith(pre) && /\.(tsx?|jsx?)$/.test(x) && !x.slice(pre.length).includes('/'))) {
      const t = read(root, f) || '';
      for (const m of t.matchAll(/require\.context\(\s*["']([^"']+)["']/g)) if (/ExpoRoot/.test(t)) { roots.add(P.normalize(P.join(dir, m[1]))); explicit = true; }
    }
    for (const cfg of ['app.json', 'app.config.js', 'app.config.ts']) {
      const t = read(root, pre + cfg) || '';
      const m = /["']expo-router["']\s*,\s*\{[^}]*\broot\s*:?\s*["']?:?\s*["']([^"']+)["']/.exec(t);
      if (m) { roots.add(P.normalize(P.join(dir, m[1]))); explicit = true; }
    }
    // no explicit root: app/ or src/app/, at its shallowest _layout (app/routes/_layout means app/routes)
    if (!explicit) for (const cand of ['src/app', 'app']) {
      const lays = all.filter((x) => x.startsWith(pre + cand + '/') && /\/_layout\.[jt]sx?$/.test(x)).map((x) => P.dirname(x)).sort((x, y) => x.split('/').length - y.split('/').length);
      if (lays.length) { roots.add(lays[0]); break; }
    }
  }
  const screens = [], edges = [], layouts = new Map();
  const text = (f) => { const s = read(root, f); return s == null ? '' : stripComments(s); };
  for (const r of roots) {
    const files = all.filter((f) => f.startsWith(r + '/') && /\.(tsx?|jsx?)$/.test(f) && !/\.(test|spec|stories)\.|\.d\.ts$|(^|\/)__/.test(f));
    for (const f of files.filter((x) => /\/_layout\.[jt]sx?$/.test(x))) {
      const s = text(f), L = lineIndex(s);
      const nav = /<(Stack|Tabs|Drawer|Slot|NativeTabs|JsStack)\b/.exec(s)?.[1] || null;
      // a Redirect under a condition gates every screen below this layout
      let gate = null, evidence = null;
      const reds = [...s.matchAll(/if\s*\(([^)]{1,200})\)\s*\{?\s*return\s*<Redirect\s+href\s*=\s*\{?\s*["'`]([^"'`]+)/g)];
      const red = reds.find((m) => gateFromCondition(m[1]) || SIGNED_OUT.test(m[2])) || null;
      if (red) {
        // redirecting away WHEN the condition holds: screens here need the opposite
        const target = red[2];
        const g = gateFromCondition(red[1]);
        gate = g ? (g === 'signed-in' ? 'signed-out' : 'signed-in') : SIGNED_OUT.test(target) ? 'signed-in' : null;
        evidence = `${f}:${L(red.index)} if (${red[1].trim()}) <Redirect href="${target}">`;
      }
      const protectedGuards = [...s.matchAll(/<\w+\.Protected\s+guard\s*=\s*\{([^}]+)\}\s*>([\s\S]*?)<\/\w+\.Protected>/g)].map((m) => ({ gate: gateFromCondition(m[1]), names: [...m[2].matchAll(/name\s*=\s*["']([^"']+)["']/g)].map((x) => x[1]), line: L(m.index), cond: m[1] }));
      layouts.set(P.dirname(f), { file: f, nav, gate, evidence, protectedGuards });
    }
    for (const f of files) {
      const rel = f.slice(r.length + 1).replace(/\.[jt]sx?$/, '');
      const base = P.basename(rel);
      if (SPECIAL.test(base) || base.startsWith('_')) continue;
      const segs = rel.split('/');
      const groups = segs.filter((x) => /^\(.+\)$/.test(x));
      const url = '/' + segs.filter((x) => !/^\(.+\)$/.test(x) && x !== 'index').map((x) => x.replace(/^\[\.\.\.(.+)\]$/, '*').replace(/^\[(.+)\]$/, ':$1')).join('/');
      // nearest layouts, innermost first
      let dir = P.dirname(f), nav = null, gate = null, evidence = null;
      for (let guard = 0; guard < 20 && dir.length >= r.length; guard++, dir = P.dirname(dir)) {
        const lay = layouts.get(dir); if (!lay) { if (dir === r) break; continue; }
        nav = nav || lay.nav;
        const pg = lay.protectedGuards.find((g) => g.names.some((n) => rel.startsWith(n) || rel.split('/').includes(n)));
        if (!gate && pg?.gate) { gate = pg.gate; evidence = `${lay.file}:${pg.line} Protected guard={${pg.cond.trim()}}`; }
        if (!gate && lay.gate) { gate = lay.gate; evidence = lay.evidence; }
        if (dir === r) break;
      }
      if (!gate && groups.length) { const g = gateFromCondition(groups.join(' ')); if (g) { gate = g; evidence = `group name ${groups.join('/')}`; } }
      screens.push({ name: rel, route: url.replace(/\/$/, '') || '/', file: f, line: 1, navigator: nav, group: groups.join('/') || null, gate, gateEvidence: evidence });
    }
    // edges: router.push/replace/navigate('/x'), <Link href="/x">, <Redirect href="/x">, {pathname: '/x'}
    const routeRx = screens.filter((x) => x.file.startsWith(r + '/')).map((x) => ({ x, rx: new RegExp('^' + x.route.replace(/[.+?^${}()|\\]/g, '\\$&').replace(/:[^/]+/g, '[^/]+').replace(/\*/g, '.*') + '/?$') }));
    const appDir = r.replace(/\/?(src\/)?app$/, '').replace(/\/app\/routes$/, '');
    for (const f of all.filter((x) => x.startsWith(appDir) && /\.(tsx?|jsx?)$/.test(x))) {
      const s = text(f); if (!/router|Link|Redirect|href/.test(s)) continue;
      const L = lineIndex(s);
      for (const m of s.matchAll(/\b(?:router\.(?:push|replace|navigate|dismissTo|setParams)|navigate|push|replace)\s*\(\s*(?:\{\s*pathname\s*:\s*)?["'`]([^"'`$]*)|\bhref\s*=\s*\{?\s*(?:\{\s*pathname\s*:\s*)?["'`]([^"'`$]*)/g)) {
        const to = (m[1] ?? m[2] ?? '').replace(/\/\([^)]+\)/g, '').split(/[?#]/)[0] || '/';
        if (!to.startsWith('/')) continue;
        const hit = routeRx.find((y) => y.rx.test(to));
        if (!hit) continue;
        const from = screens.find((x) => x.file === f)?.route || null;
        edges.push({ from, fromFile: f, line: L(m.index), to: hit.x.route, kind: /href/.test(m[0]) ? (/Redirect/.test(s.slice(Math.max(0, m.index - 30), m.index)) ? 'redirect' : 'link') : 'push' });
      }
    }
  }
  return { roots: [...roots], screens, edges, stats: { ms: Date.now() - t0 } };
}

