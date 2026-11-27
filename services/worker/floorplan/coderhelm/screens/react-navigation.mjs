// React Navigation screen detector. Static API (createXNavigator({screens, groups}))
// and dynamic API (<X.Screen name component>), nested navigators, auth-gated groups, linking
// paths, and outgoing navigate() edges. No AST: bracket matching like Floorplan's screens.mjs.
//
// detect(root) -> { screens: [...], navigators: [...], edges: [...], stats }
//   screen: { name, route, file, line, component, componentFile, navigator, group, gate, leaf, dynamic?, via? }
import path from 'node:path';
import { listFiles, read, stripComments, closeOf, tagEnd, objectEntries, lineIndex, gateFromCondition } from '../lib/text.mjs';


const CODE = /\.(tsx?|jsx?|mjs)$/;
const NAV_FACTORY = /\b(?:const|let|var)\s+([A-Za-z_$][\w$]*)\s*(?::[^=]{0,200})?=\s*(create(\w*?)Navigator\w*)\s*(?:<[^>]{0,400}>)?\s*\(/g;

export function detect(root, { maxFiles = 20000, files: given } = {}) {
  const t0 = Date.now();
  const files = (given || listFiles(root, { exts: CODE, max: maxFiles })).filter((f) => !/\.(test|spec|stories)\.|\.d\.ts$/.test(f));
  const texts = new Map();
  const get = (f) => { if (!texts.has(f)) { const s = read(root, f); texts.set(f, s == null ? null : stripComments(s)); } return texts.get(f); };
  // pass 0: only files that mention react-navigation (or a navigator factory) are read in depth
  const navFiles = [], consts = new Map(), defaultExport = new Map(), hookBodies = new Map();
  for (const f of files) {
    const s = get(f); if (!s) continue;
    if (/create\w*Navigator|\.Screen\b|NavigationContainer|createStaticNavigation/.test(s)) navFiles.push(f);
    // string constants (Screens.HOME = 'Home'), used when a name is not a literal
    for (const m of s.matchAll(/\b(?:export\s+)?(?:const|enum)\s+([A-Z][\w$]*)\s*(?::[^=]{0,80})?=?\s*\{/g)) {
      const open = s.indexOf('{', m.index + m[0].length - 1);
      const ents = objectEntries(s, open);
      if (ents.length > 400) continue;
      for (const e of ents) { const v = /^["'`]([^"'`$]+)["'`]$/.exec(e.value); if (v) consts.set(`${m[1]}.${e.key}`, v[1]); }
    }
    const d = /export\s+default\s+([A-Za-z_$][\w$]*)\s*;?/.exec(s); if (d) defaultExport.set(f, d[1]);
    for (const m of s.matchAll(/\b(?:function\s+(use\w+)\s*\([^)]*\)\s*\{|const\s+(use\w+)\s*=\s*\([^)]*\)\s*=>\s*\{?)/g)) {
      const body = s.slice(m.index, m.index + 600);
      const ret = /return\s+([^;\n}]+)/.exec(body)?.[1] || /=>\s*([^{][^;\n]+)/.exec(body)?.[1];
      hookBodies.set(m[1] || m[2], ret || '');
    }
  }
  // pass 1: navigator bindings, across files
  const navs = new Map(); // binding -> { name, kind, file, line, static }
  const navOfFile = new Map();
  for (const f of navFiles) {
    const s = get(f), L = lineIndex(s);
    for (const m of s.matchAll(NAV_FACTORY)) {
      const open = m.index + m[0].length - 1;
      const close = closeOf(s, open);
      const args = close > 0 ? s.slice(open + 1, close).trim() : '';
      const kind = (m[3] || 'Stack').replace(/^Native/, '') || 'Stack';
      navs.set(`${f}#${m[1]}`, { binding: m[1], kind, file: f, line: L(m.index), static: args.startsWith('{'), open: args.startsWith('{') ? s.indexOf('{', open) : -1 });
      if (!navOfFile.has(f)) navOfFile.set(f, []);
      navOfFile.get(f).push(m[1]);
    }
  }
  const bindingNames = new Set([...navs.values()].map((n) => n.binding));
  // parameters typed as a navigator (function commonScreens(Stack: typeof Flat))
  for (const f of navFiles) for (const m of get(f).matchAll(/\b([A-Za-z_$][\w$]*)\s*:\s*typeof\s+([A-Za-z_$][\w$]*)/g)) if (bindingNames.has(m[2])) bindingNames.add(m[1]);

  // imports: local name -> resolved file (relative + common aliases)
  const fileSet = new Set(files);
  const stemIndex = new Map();
  for (const f of files) { const st = f.replace(/\.(tsx?|jsx?|mjs)$/, '').replace(/\/index$/, ''); stemIndex.set(st, f); }
  const bySuffix = (spec) => { const tail = spec.replace(/^[@~#]\w*\//, '').replace(/^(\.\.?\/)+/, ''); for (const [st, f] of stemIndex) if (st.endsWith('/' + tail) || st === tail) return f; return null; };
  const resolveSpec = (from, spec) => {
    if (spec.startsWith('.')) { const st = path.posix.normalize(path.posix.join(path.posix.dirname(from), spec)).replace(/\.(tsx?|jsx?|mjs)$/, '').replace(/\/index$/, ''); return stemIndex.get(st) || null; }
    if (/^[@~#]\/|^(src|app)\//.test(spec)) return bySuffix(spec);
    return null;
  };
  const importsOf = new Map();
  const imports = (f) => {
    if (importsOf.has(f)) return importsOf.get(f);
    const s = get(f) || '', map = new Map();
    for (const m of s.matchAll(/import\s+(?:type\s+)?([\s\S]{0,600}?)\s+from\s+["']([^"']+)["']/g)) {
      const clause = m[1], file = resolveSpec(f, m[2]);
      const def = /^([A-Za-z_$][\w$]*)/.exec(clause.trim()); if (def) map.set(def[1], { file, name: 'default' });
      const named = /\{([^}]*)\}/.exec(clause); if (named) for (const p of named[1].split(',')) { const [a, b] = p.trim().replace(/^type\s+/, '').split(/\s+as\s+/); if (a) map.set((b || a).trim(), { file, name: a.trim() }); }
    }
    for (const m of s.matchAll(/(?:const|let)\s+([A-Za-z_$][\w$]*)\s*=\s*(?:React\.)?lazy\(\s*\(\)\s*=>\s*import\(\s*["']([^"']+)["']/g)) map.set(m[1], { file: resolveSpec(f, m[2]), name: 'default' });
    importsOf.set(f, map);
    return map;
  };
  const isNavigatorComp = (f, ident, depth = 0) => {
    if (!ident || depth > 3) return false;
    if ((navOfFile.get(f) || []).includes(ident)) return true;
    const s = get(f) || '';
    // a component defined here that renders <X.Navigator> or another navigator component
    const def = new RegExp(`(?:function\\s+${ident}\\b|(?:const|let)\\s+${ident}\\s*[:=])`).exec(s);
    if (def) {
      const body = s.slice(def.index, def.index + 4000);
      const end = body.search(/\n(?:export\s+)?(?:function|const|let|class)\s/);
      const own = end > 0 ? body.slice(0, end + 1) : body;
      if (/<\w+\.Navigator\b/.test(own)) return true;
      for (const m of own.matchAll(/<([A-Z][\w$]*)\b/g)) if (m[1] !== ident && isNavigatorComp(f, m[1], depth + 1)) return true;
      return false;
    }
    const imp = imports(f).get(ident);
    if (imp?.file) { const name = imp.name === 'default' ? defaultExport.get(imp.file) : imp.name; return name ? isNavigatorComp(imp.file, name, depth + 1) : false; }
    return false;
  };
  const compFile = (f, ident) => {
    if (!ident || !/^[A-Za-z_$][\w$.]*$/.test(ident)) return null;
    const base = ident.split('.')[0];
    const imp = imports(f).get(base);
    if (imp?.file) return imp.file;
    return new RegExp(`(?:function|const|let|class)\\s+${base}\\b`).test(get(f) || '') ? f : null;
  };
  const nameOf = (raw) => {
    if (!raw) return { dyn: 'missing' };
    const r = raw.trim().replace(/\s+as\s+[\w'".]+$/, '').replace(/^\{\s*|\s*\}$/g, '').replace(/\s+as\s+[\w'".]+$/, '').trim();
    const lit = /^["'`]([^"'`$]+)["'`]$/.exec(r); if (lit) return { v: lit[1] };
    const k = r.replace(/\s/g, ''); const tail = k.split('.').slice(-2).join('.');
    if (consts.has(k)) return { v: consts.get(k), via: k };
    if (consts.has(tail)) return { v: consts.get(tail), via: k };
    return { dyn: r.slice(0, 60) };
  };
  const gateOfHook = (h) => { if (!h) return null; const byName = gateFromCondition(h); if (byName) return byName; const body = hookBodies.get(h); return body ? gateFromCondition(body) : null; };

  const screens = [];
  // pass 2a: static configs
  for (const n of navs.values()) {
    if (!n.static) continue;
    const s = get(n.file), L = lineIndex(s);
    const take = (objOpen, group, gate) => {
      for (const e of objectEntries(s, objOpen)) {
        if (e.key === 'screens' && e.value.startsWith('{')) {
          for (const sc of objectEntries(s, e.vOffset)) {
            if (sc.spread != null) { screens.push({ name: null, dynamic: 'spread: ' + sc.spread.replace(/\s+/g, ' ').slice(0, 80), route: null, file: n.file, line: L(sc.start), navigator: n.binding, kind: n.kind, group, gate, leaf: true }); continue; }
            let v = sc.value, route = null, comp = v;
            const call = /^create\w*Screen\s*\(\s*\{/.exec(v);
            const objStart = call ? sc.vOffset + v.indexOf('{') : v.startsWith('{') ? sc.vOffset : -1;
            if (objStart >= 0) {
              const inner = objectEntries(s, objStart);
              comp = inner.find((x) => x.key === 'screen')?.value || null;
              const lk = inner.find((x) => x.key === 'linking')?.value;
              if (lk) route = /^["'`]([^"'`]*)["'`]$/.exec(lk)?.[1] ?? /path\s*:\s*["'`]([^"'`]*)/.exec(lk)?.[1] ?? null;
              const iff = inner.find((x) => x.key === 'if')?.value; if (iff) gate = gateOfHook(iff) || gate;
            }
            comp = comp ? comp.replace(/\s+as\s+\w+$/, '').trim() : null;
            const nested = comp && /^[A-Za-z_$][\w$]*$/.test(comp) && isNavigatorComp(n.file, comp);
            screens.push({ name: sc.key, route, file: n.file, line: L(sc.start), component: comp, componentFile: compFile(n.file, comp), navigator: n.binding, kind: n.kind, group, gate, leaf: !nested });
          }
        }
        if (e.key === 'groups' && e.value.startsWith('{')) {
          for (const g of objectEntries(s, e.vOffset)) {
            if (!g.value.startsWith('{')) continue;
            const iff = objectEntries(s, g.vOffset).find((x) => x.key === 'if')?.value;
            take(g.vOffset, g.key, gateOfHook(iff) || gateFromCondition(g.key) || gate);
          }
        }
      }
    };
    take(n.open, null, null);
  }
  // pass 2b: dynamic JSX <X.Screen ...>
  for (const f of navFiles) {
    const s = get(f), L = lineIndex(s);
    for (const m of s.matchAll(/<([A-Za-z_$][\w$]*)\.(Screen|Group|Protected)\b/g)) {
      if (!bindingNames.has(m[1]) || m[2] !== 'Screen') continue;
      const end = tagEnd(s, m.index); if (end < 0) continue;
      const attrs = s.slice(m.index, end);
      const attr = (k) => { const a = new RegExp(`\\b${k}\\s*=\\s*`).exec(attrs); if (!a) return null; const at = a.index + a[0].length; if (attrs[at] === '{') { const c = closeOf(attrs, at); return attrs.slice(at, c + 1); } return /^["'][^"']*["']/.exec(attrs.slice(at))?.[0] || null; };
      const nm = nameOf(attr('name'));
      let comp = attr('component') || attr('getComponent');
      comp = comp ? comp.replace(/^\{\s*(?:\(\)\s*=>\s*)?|\s*\}$/g, '').replace(/\s+as\s+\w+$/, '').trim() : null;
      const opts = attr('options') || '';
      // context: enclosing <X.Group> and enclosing {cond ? (...) : (...)} / {cond && (...)}
      const ctx = jsxContext(s, m.index);
      let gate = /\brequire[sd]?Auth\w*\s*:\s*true|\bauth(Required)?\s*:\s*true|\bprotected\s*:\s*true/.test(opts) ? 'signed-in' : null;
      gate = gate || ctx.gate;
      const nested = comp && /^[A-Za-z_$][\w$]*$/.test(comp) && isNavigatorComp(f, comp);
      screens.push({ name: nm.v ?? null, ...(nm.dyn ? { dynamic: nm.dyn } : {}), ...(nm.via ? { via: nm.via } : {}), route: null, file: f, line: L(m.index), component: comp, componentFile: compFile(f, comp), navigator: m[1], group: ctx.group, gate, leaf: !nested });
    }
  }
  // linking: `screens: { Name: 'path' | { path: 'x', screens: {...} } }` inside a linking/config object,
  // or any object literal whose keys are mostly screen names and values are path strings (custom routers)
  const known = new Set(screens.map((x) => x.name).filter(Boolean));
  const routeOf = new Map();
  const linkFiles = files.filter((f) => { const s = get(f); return s && /\bprefixes\s*:|\blinking\b|getStateFromPath|['"]\/[\w:-]*['"]/.test(s) && [...known].some((k) => s.includes(k)); });
  for (const f of linkFiles.slice(0, 400)) {
    const s = get(f);
    const walk = (open, base, depth) => {
      if (depth > 6) return;
      const ents = objectEntries(s, open);
      const hits = ents.filter((e) => known.has(e.key));
      if (hits.length && hits.length >= Math.max(1, ents.length * 0.5)) {
        for (const e of ents) {
          const str = /^["'`]([^"'`]*)["'`]$/.exec(e.value)?.[1] ?? (Array.isArray(null) ? null : /^\[\s*["'`]([^"'`]*)["'`]/.exec(e.value)?.[1]);
          if (str != null && known.has(e.key)) { if (!routeOf.has(e.key)) routeOf.set(e.key, joinPath(base, str)); continue; }
          if (e.value.startsWith('{')) {
            const inner = objectEntries(s, e.vOffset);
            const p = inner.find((x) => x.key === 'path')?.value; const ps = p && /^["'`]([^"'`]*)["'`]$/.exec(p)?.[1];
            if (ps != null && known.has(e.key) && !routeOf.has(e.key)) routeOf.set(e.key, joinPath(base, ps));
            const sc = inner.find((x) => x.key === 'screens');
            if (sc && sc.value.startsWith('{')) walk(sc.vOffset, ps != null ? joinPath(base, ps) : base, depth + 1);
          }
        }
        return;
      }
      for (const e of ents) if (e.value.startsWith('{')) walk(e.vOffset, base, depth + 1);
    };
    for (const m of s.matchAll(/(?:\bscreens\s*:\s*|new\s+\w*Router\s*(?:<[^>]{0,200}>)?\s*\(\s*|=\s*)\{/g)) {
      const open = s.indexOf('{', m.index + m[0].length - 1);
      walk(open, '', 0);
    }
  }
  for (const x of screens) if (!x.route && routeOf.has(x.name)) x.route = routeOf.get(x.name);
  // nesting: a navigator screen's gate (and group) flow down to the screens of the navigator it renders
  const childNav = (x) => {
    if (x.leaf || !x.component) return null;
    const file = x.componentFile || x.file;
    const local = (navOfFile.get(file) || []);
    const imp = imports(x.file).get(x.component);
    const name = imp ? (imp.name === 'default' ? defaultExport.get(imp.file) : imp.name) : x.component;
    if (local.includes(name)) return `${file}#${name}`;
    // a component that renders <B.Navigator>: take the first binding in that file
    return local.length ? `${file}#${local[local.length - 1]}` : null;
  };
  const parentOf = new Map();
  for (const x of screens) { const c = childNav(x); if (c && !parentOf.has(c)) parentOf.set(c, x); }
  for (const x of screens) {
    let key = `${x.file}#${x.navigator}`, guard = 0;
    const chain = [];
    while (parentOf.has(key) && guard++ < 8) { const p = parentOf.get(key); chain.push(p); key = `${p.file}#${p.navigator}`; }
    if (!x.gate) x.gate = chain.find((p) => p.gate)?.gate || null;
    if (!x.group) x.group = chain.find((p) => p.group)?.group || null;
    x.path = [...chain.reverse().map((p) => p.name), x.name].join(' > ');
  }

  // edges: navigate('X'), push('X'), replace('X'), navigate({name:'X'}), <Link screen="X">, reset routes
  const edges = [];
  const compToScreens = new Map();
  for (const x of screens) if (x.componentFile && x.leaf) { if (!compToScreens.has(x.componentFile)) compToScreens.set(x.componentFile, []); compToScreens.get(x.componentFile).push(x.name); }
  const EDGE = /\b(?:navigate|push|replace|jumpTo|popTo|navigateDeprecated|popToScreen)\s*\(\s*(?:\{\s*name\s*:\s*)?(["'`])([A-Za-z][\w-]*)\1|\bscreen\s*[=:]\s*\{?\s*(["'])([A-Za-z][\w-]*)\3|\bname\s*:\s*(["'])([A-Za-z][\w-]*)\5(?=[^}]{0,80}\}\s*\]\s*[,}])/g;
  const SCREENY = /navigation|navigate|useNavigation|router|Link/;
  for (const f of files) {
    const s = get(f); if (!s || !SCREENY.test(s)) continue;
    const L = lineIndex(s);
    for (const m of s.matchAll(EDGE)) {
      const to = m[2] || m[4] || m[6];
      if (!known.has(to)) continue;
      edges.push({ from: compToScreens.get(f) || null, fromFile: f, line: L(m.index), to, kind: m[2] ? 'navigate' : m[4] ? 'link' : 'reset' });
      if (edges.length > 50000) break;
    }
  }
  const navigators = [...navs.values()].map(({ binding, kind, file, line, static: st }) => ({ binding, kind, file, line, static: st }));
  return { screens, navigators, edges, stats: { files: files.length, navFiles: navFiles.length, ms: Date.now() - t0 } };
}

function joinPath(base, p) { if (p == null) return base || null; if (p.startsWith('/')) return p; return ('/' + [base, p].filter(Boolean).join('/')).replace(/\/+/g, '/').replace(/^\/\//, '/'); }

/** Group and auth gate around position i in JSX: nearest enclosing <X.Group>, and {cond ? A : B} / {cond && A}. */
function jsxContext(s, i) {
  let group = null, gate = null;
  // enclosing <X.Group ...> not yet closed before i
  const before = s.slice(Math.max(0, i - 60000), i);
  const opens = [...before.matchAll(/<(\w+)\.Group\b([^>]*)>|<\/\w+\.Group>/g)];
  let depth = 0;
  for (let k = opens.length - 1; k >= 0; k--) {
    if (opens[k][0].startsWith('</')) { depth++; continue; }
    if (depth) { depth--; continue; }
    group = /navigationKey\s*=\s*\{?\s*["'`]?([^"'`}\s]+)/.exec(opens[k][2])?.[1] || /screenOptions/.test(opens[k][2]) && 'group' || 'group';
    break;
  }
  // walk outwards through braces; at a JSX expression container, look for `cond ?` or `cond &&`
  let d = 0;
  for (let k = i - 1, stop = Math.max(0, i - 60000); k >= stop; k--) {
    const c = s[k];
    if (c === ')' || c === '}' || c === ']') d++;
    else if (c === '(' || c === '[') { if (d) d--; }
    else if (c === '{') {
      if (d) { d--; continue; }
      const expr = s.slice(k + 1, i);
      const tern = /^\s*([^{}?]{1,200}?)\s*\?\s*\(?/.exec(expr);
      const and = /^\s*([^{}&]{1,200}?)\s*&&\s*\(?/.exec(expr);
      if (tern) {
        // which branch are we in? a ':' at depth 0 between the '?' and i means the else branch
        const rest = expr.slice(tern[0].length);
        // a parenthesised then-branch opened inside the match: its ')' closes before the ':'
        let dd = /\(\s*$/.test(tern[0]) ? 1 : 0, inElse = false;
        for (const ch of rest) { if ('([{'.includes(ch)) dd++; else if (')]}'.includes(ch)) dd--; else if (ch === ':' && dd === 0) inElse = true; }
        gate = gateFromCondition(tern[1], !inElse);
      } else if (and) gate = gateFromCondition(and[1], true);
      if (gate) break;
      // stop at the component boundary
      if (/^\s*return\b|=>\s*$/.test(s.slice(Math.max(0, k - 20), k))) break;
    }
  }
  return { group, gate };
}

