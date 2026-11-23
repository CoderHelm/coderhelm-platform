// Flutter screen detector: go_router (GoRoute/ShellRoute/StatefulShellRoute, nested
// paths, redirect), auto_route (AutoRoute(page: XRoute.page, path:) + @RoutePage classes, guards),
// Navigator named routes (routes: {'/x': (ctx) => X()}), GetX GetPage, and imperative pushes
// (MaterialPageRoute(builder: (_) => X())) as edges.
// detect(root) -> { screens:[{name, route, file, line, router, parent, gate, guard?}], edges:[...] }
import { listFiles, read, stripComments, closeOf, lineIndex, gateFromCondition } from '../lib/text.mjs';

export function detect(root, { maxFiles = 40000 } = {}) {
  const t0 = Date.now();
  const files = listFiles(root, { exts: /\.dart$/, max: maxFiles }).filter((f) => !/(_test|\.g|\.gr|\.freezed|\.mocks)\.dart$/.test(f));
  const text = (f) => stripComments(read(root, f) || '');
  const screens = [], edges = [];
  const routePages = new Map(); // auto_route: class name -> file:line
  // static string constants: class X { static const routeName = '/x'; }  -> X.routeName
  const consts = new Map();
  for (const f of files) {
    const s = text(f); if (!/static\s+const/.test(s)) continue;
    for (const c of s.matchAll(/\bclass\s+(\w+)[^{]*\{/g)) {
      const open = s.indexOf('{', c.index); const close = closeOf(s, open); if (close < 0) continue;
      for (const k of s.slice(open, close).matchAll(/static\s+const\s+(?:String\s+)?(\w+)\s*=\s*['"]([^'"$]*)['"]/g)) consts.set(`${c[1]}.${k[1]}`, k[2]);
    }
  }
  const val = (x) => (x && x.startsWith('{') ? consts.get(x.slice(1, -1)) ?? x : x);
  for (const f of files) {
    const s = text(f); const L = lineIndex(s);
    // ---- auto_route: @RoutePage() class X  -> route name XRoute (or the name: argument)
    for (const m of s.matchAll(/@RoutePage(?:<[^>]*>)?\(\s*(?:name\s*:\s*['"](\w+)['"])?[^)]*\)\s*(?:@\w+(?:\([^)]*\))?\s*)*class\s+(\w+)/g)) {
      const route = m[1] || m[2].replace(/(Page|Screen|View)?$/, (x) => (x ? 'Route' : 'Route'));
      routePages.set(route, { cls: m[2], file: f, line: L(m.index) });
    }
    // ---- go_router: GoRoute(path: '/x', name: 'n', builder: (c, s) => X(), routes: [...])
    if (/\bGoRoute\s*\(|\bShellRoute\s*\(|StatefulShellRoute/.test(s)) {
      const recs = [];
      for (const m of s.matchAll(/\b(GoRoute|ShellRoute|StatefulShellRoute(?:\.indexedStack)?|StatefulShellBranch)\s*\(/g)) {
        const open = s.indexOf('(', m.index + m[1].length); const close = closeOf(s, open); if (close < 0) continue;
        const own = maskInner(s.slice(open + 1, close));
        const path = /\bpath\s*:\s*(?:['"]([^'"]*)['"]|(\w[\w.]*))/.exec(own);
        const name = /\bname\s*:\s*(?:['"]([^'"]*)['"]|(\w[\w.]*))/.exec(own);
        const body = s.slice(open + 1, close);
        const widget = /\b(?:builder|pageBuilder)\s*:\s*\([^)]*\)\s*(?:=>|\{[\s\S]{0,400}?return)\s*(?:const\s+)?(?:\w+Page\s*\(\s*(?:key:[^,]*,\s*)?child\s*:\s*(?:const\s+)?)?([A-Z]\w*)\s*[(.]/.exec(body)?.[1] || null;
        const redirect = /\bredirect\s*:\s*\(([^)]*)\)\s*(?:=>|\{)([\s\S]{0,300})/.exec(own);
        recs.push({ kind: m[1], open, close, line: L(m.index), path: path?.[1] ?? (path?.[2] ? val(`{${path[2]}}`) : null), name: name?.[1] ?? (name?.[2] ? val(`{${name[2]}}`) : null), widget, redirect: redirect?.[2] || null });
      }
      // top-level GoRouter(redirect:) gates everything under it
      const top = /GoRouter\s*\(([\s\S]{0,4000}?)\broutes\s*:/.exec(s);
      const topRedirect = top && /\bredirect\s*:\s*[\s\S]{0,600}?(?:return|=>)\s*([^;]{0,200})/.exec(top[1]);
      for (const r of recs) {
        if (r.kind !== 'GoRoute') continue;
        const parents = recs.filter((o) => o !== r && o.open < r.open && o.close > r.close).sort((a, b) => a.open - b.open);
        let url = '';
        for (const p of parents) if (p.path != null) url = join(url, p.path);
        url = join(url, r.path ?? '');
        const shell = parents.filter((p) => p.kind !== 'GoRoute').map((p) => p.kind).pop() || null;
        const redirectCond = r.redirect || parents.map((p) => p.redirect).find(Boolean) || null;
        let gate = redirectCond ? gateFromCondition(redirectCond) : null;
        if (!gate && topRedirect) gate = 'mixed (global redirect)';
        screens.push({ name: r.name || r.widget || url, route: url, file: f, line: r.line, router: 'go_router', widget: r.widget, parent: parents.filter((p) => p.kind === 'GoRoute').pop()?.path ?? null, group: shell, gate });
      }
    }
    // ---- auto_route config: AutoRoute(page: XRoute.page, path: '/x', guards: [AuthGuard], children: [...])
    if (/\b(?:AutoRoute|CustomRoute|AdaptiveRoute|MaterialRoute|CupertinoRoute|RedirectRoute)\s*\(/.test(s)) {
      const recs = [];
      for (const m of s.matchAll(/\b(AutoRoute|CustomRoute(?:<[^>]*>)?|AdaptiveRoute|MaterialRoute|CupertinoRoute)\s*\(/g)) {
        const open = s.indexOf('(', m.index + m[1].length); const close = closeOf(s, open); if (close < 0) continue;
        const own = maskInner(s.slice(open + 1, close));
        const page = /\bpage\s*:\s*(\w+)\.page/.exec(own)?.[1];
        const path = /\bpath\s*:\s*['"]([^'"]*)['"]/.exec(own)?.[1] ?? null;
        const guards = /\bguards\s*:\s*\[([^\]]*)\]/.exec(s.slice(open, close))?.[1] || '';
        recs.push({ open, close, line: L(m.index), page, path, guards: guards.split(',').map((x) => x.trim().replace(/^_+/, '')).filter(Boolean) });
      }
      for (const r of recs) {
        if (!r.page) continue;
        const parents = recs.filter((o) => o !== r && o.open < r.open && o.close > r.close).sort((a, b) => a.open - b.open);
        let url = ''; for (const p of parents) if (p.path != null) url = join(url, p.path);
        url = r.path != null ? join(url, r.path) : null;
        const guards = [...new Set([...parents.flatMap((p) => p.guards), ...r.guards])];
        const gate = guards.length ? (gateFromCondition(guards.join(' ')) || 'guarded') : null;
        screens.push({ name: r.page, route: url, file: f, line: r.line, router: 'auto_route', parent: parents.pop()?.page ?? null, gate, guards });
      }
    }
    // ---- Navigator named routes: routes: { '/x': (context) => X() }  /  GetX GetPage(name:, page:)
    for (const m of s.matchAll(/\broutes\s*:\s*(?:<[^>]*>)?\s*\{/g)) {
      const open = s.indexOf('{', m.index); const close = closeOf(s, open); if (close < 0) continue;
      for (const e of s.slice(open, close).matchAll(/['"](\/[^'"]*)['"]\s*:\s*\([^)]*\)\s*=>\s*(?:const\s+)?([A-Z]\w*)/g)) screens.push({ name: e[2], route: e[1], file: f, line: L(open + e.index), router: 'navigator-named' });
    }
    for (const m of s.matchAll(/\bGetPage\s*\(\s*name\s*:\s*([^,]+),\s*page\s*:\s*\(\)\s*=>\s*(?:const\s+)?([A-Z]\w*)/g)) screens.push({ name: m[2], route: m[1].trim().replace(/^['"]|['"]$/g, ''), file: f, line: L(m.index), router: 'getx' });
    // ---- edges
    for (const m of s.matchAll(/\b(?:context\.)?(?:go|push|pushReplacement|replace|goNamed|pushNamed|pushReplacementNamed|navigate)\s*\(\s*(?:context\s*,\s*)?(?:['"]([^'"$]+)['"]|([A-Z]\w*\.\w+))/g)) { const to = m[1] ?? consts.get(m[2]); if (to) edges.push({ fromFile: f, line: L(m.index), to, kind: 'route' }); }
    for (const m of s.matchAll(/\b(?:router|context\.router|AutoRouter\.of\(context\))\.(?:push|replace|navigate|pushAndPopUntil)\s*\(\s*(?:const\s+)?([A-Z]\w*Route)\b/g)) edges.push({ fromFile: f, line: L(m.index), to: m[1], kind: 'auto_route' });
    for (const m of s.matchAll(/\b(?:MaterialPageRoute|CupertinoPageRoute|PageRouteBuilder)(?:<[^>]*>)?\s*\(\s*(?:[^()]*?)builder\s*:\s*\([^)]*\)\s*=>\s*(?:const\s+)?([A-Z]\w*)/g)) edges.push({ fromFile: f, line: L(m.index), to: m[1], kind: 'push-widget' });
  }
  // auto_route: config entries named XRoute -> the @RoutePage class and its file
  for (const x of screens) if (x.router === 'auto_route') { const rp = routePages.get(x.name); if (rp) { x.widget = rp.cls; x.widgetFile = rp.file; } }
  const unrouted = [...routePages.entries()].filter(([r]) => !screens.some((x) => x.name === r)).map(([r, v]) => ({ name: r, ...v }));
  return { screens, edges, routePages: routePages.size, unroutedPages: unrouted, stats: { files: files.length, ms: Date.now() - t0 } };
}

function join(a, b) { if (b == null || b === '') return a || '/'; if (b.startsWith('/')) return b; return ((a || '').replace(/\/$/, '') + '/' + b).replace(/\/+/g, '/'); }
/** Keep only an argument list's own text: nested (), [], {} blanked. */
function maskInner(s) { let out = '', d = 0; for (const c of s) { if ('([{'.includes(c)) { d++; out += d === 1 ? c : ' '; continue; } if (')]}'.includes(c)) { out += d === 1 ? c : ' '; d = Math.max(0, d - 1); continue; } out += d ? ' ' : c; } return out; }

