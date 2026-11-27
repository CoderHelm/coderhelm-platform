// iOS screen detector.
//  - Storyboards/XIB: every scene's controller (<viewController>, <tableViewController>, ...) with
//    storyboardIdentifier and customClass; <segue destination kind identifier> as edges.
//  - UIKit code: classes that inherit (transitively, within the repo) from UIViewController & co.
//  - SwiftUI: route enums handed to .navigationDestination(for:) (each case is a screen; the
//    switch maps it to a View), NavigationLink(destination:)/{ X() }, .sheet/.fullScreenCover.
//  - Edges: pushViewController/present(X(...)), performSegue(withIdentifier:), NavigationLink,
//    router.navigate(to: .case).
// detect(root) -> { screens:[{name, kind, file, line, route?, group?}], edges:[...] }
import { listFiles, read, stripComments, closeOf, lineIndex } from '../lib/text.mjs';

const VC_BASES = /^(UIViewController|UITableViewController|UICollectionViewController|UINavigationController|UITabBarController|UIPageViewController|UISplitViewController|UIHostingController|UIAlertController|UISearchController|UIDocumentPickerViewController|UIImagePickerController|SFSafariViewController|MFMailComposeViewController|AVPlayerViewController|QLPreviewController)$/;
const CONTAINERS = /^(UINavigationController|UITabBarController|UISplitViewController|UIPageViewController)$/;

export function detect(root, { maxFiles = 60000 } = {}) {
  const t0 = Date.now();
  const files = listFiles(root, { exts: /\.(swift|storyboard|xib|m)$/, max: maxFiles }).filter((f) => !/(Tests?|UITests|Snapshot)\//.test(f) && !/Tests?\.swift$/.test(f));
  const screens = [], edges = [];
  // ---- storyboards
  for (const f of files.filter((x) => /\.storyboard$/.test(x))) {
    const s = read(root, f) || ''; const L = lineIndex(s);
    const sceneOf = new Map();
    for (const m of s.matchAll(/<(viewController|tableViewController|collectionViewController|navigationController|tabBarController|pageViewController|splitViewController|hostingController|avPlayerViewController)\b([^>]*)>/g)) {
      const id = /\bid="([^"]+)"/.exec(m[2])?.[1];
      const cls = /\bcustomClass="([^"]+)"/.exec(m[2])?.[1] || null;
      const sbid = /\bstoryboardIdentifier="([^"]+)"/.exec(m[2])?.[1] || null;
      const container = /navigationController|tabBarController|splitViewController|pageViewController/.test(m[1]);
      sceneOf.set(id, cls || sbid || id);
      screens.push({ name: cls || sbid || `${m[1]}#${id}`, kind: 'storyboard', container, file: f, line: L(m.index), storyboardId: sbid, sceneId: id, group: f.split('/').pop().replace(/\.storyboard$/, '') });
    }
    for (const m of s.matchAll(/<segue\b([^>]*)>/g)) {
      const dest = /\bdestination="([^"]+)"/.exec(m[1])?.[1];
      // the source is the scene whose <connections> holds this segue: nearest controller opened before it
      const before = s.slice(0, m.index); const src = [...before.matchAll(/<(?:\w*[vV]iewController|navigationController|tabBarController)\b[^>]*\bid="([^"]+)"/g)].pop()?.[1];
      edges.push({ from: sceneOf.get(src) || src, to: sceneOf.get(dest) || dest, kind: `segue:${/\bkind="([^"]+)"/.exec(m[1])?.[1] || 'show'}`, id: /\bidentifier="([^"]+)"/.exec(m[1])?.[1] || null, file: f, line: L(m.index) });
    }
  }
  // ---- Swift: class graph for UIKit controllers
  const swift = files.filter((x) => /\.swift$/.test(x));
  const texts = new Map();
  const get = (f) => { if (!texts.has(f)) texts.set(f, stripComments(read(root, f) || '')); return texts.get(f); };
  const parent = new Map(), where = new Map();
  for (const f of swift) {
    const s = get(f); const L = lineIndex(s);
    for (const m of s.matchAll(/\b(?:final\s+|open\s+|public\s+|private\s+|fileprivate\s+|internal\s+|@\w+(?:\([^)]*\))?\s+)*class\s+(\w+)(?:<[^>]*>)?\s*:\s*([\w.]+)/g)) { parent.set(m[1], m[2].split('.').pop()); where.set(m[1], { file: f, line: L(m.index) }); }
  }
  for (const f of files.filter((x) => /\.m$/.test(x))) {
    const s = read(root, f) || ''; const L = lineIndex(s);
    for (const m of s.matchAll(/@interface\s+(\w+)\s*:\s*(\w+)/g)) { parent.set(m[1], m[2]); where.set(m[1], { file: f, line: L(m.index) }); }
  }
  const isVC = (c, d = 0) => { if (!c || d > 12) return false; if (VC_BASES.test(c)) return true; return isVC(parent.get(c), d + 1); };
  const rootBase = (c, d = 0) => (VC_BASES.test(c) || !parent.has(c) || d > 12 ? c : rootBase(parent.get(c), d + 1));
  const inStoryboard = new Set(screens.map((x) => x.name));
  for (const [c, w] of where) if (isVC(c) && !inStoryboard.has(c)) screens.push({ name: c, kind: 'uikit', container: CONTAINERS.test(rootBase(c)), base: rootBase(c), file: w.file, line: w.line });
  // ---- SwiftUI: route enums and their destination switch
  const views = new Set();
  for (const f of swift) for (const m of get(f).matchAll(/\bstruct\s+(\w+)\s*(?:<[^>]*>)?\s*:\s*(?:[\w.,\s]*\b)?View\b/g)) views.add(m[1]);
  const routeEnums = new Set();
  for (const f of swift) for (const m of get(f).matchAll(/\bnavigationDestination\(\s*for\s*:\s*([\w.]+)\.self/g)) routeEnums.add(m[1].split('.').pop());
  for (const f of swift) for (const m of get(f).matchAll(/\b(?:sheet|fullScreenCover)\(\s*item\s*:\s*\$[\w.]+/g)) {
    // the item's type: @State var x: T? / @Binding
    const name = /\$([\w.]+)/.exec(m[0])[1].split('.').pop();
    const rx = new RegExp(`\\b(?:var|let)\\s+${name}\\s*:\\s*([\\w.]+)\\?`);
    for (const g of swift) { const t = rx.exec(get(g))?.[1]; if (t) { routeEnums.add(t.split('.').pop()); break; } }
  }
  // a switch whose cases build Views (case .x: SomeView(...)) marks the enum it switches over as a
  // route enum, wherever it is presented (custom sheet modifiers included)
  const enumCases = new Map();
  for (const f of swift) { const s = get(f); for (const m of s.matchAll(/\benum\s+(\w+)\s*(?::[^{]*)?\{/g)) { const open = s.indexOf('{', m.index); const close = closeOf(s, open); if (close < 0) continue; enumCases.set(m[1], new Set([...s.slice(open, close).matchAll(/\bcase\s+([\w\s,()?:.<>\[\]]+?)(?=\n|$)/g)].flatMap((c) => splitCases(c[1])).filter((n) => /^[a-z]\w*$/.test(n)))); } }
  for (const f of swift) {
    const s = get(f);
    for (const m of s.matchAll(/\bswitch\s+[^{]{1,80}\{/g)) {
      const open = s.indexOf('{', m.index); const close = closeOf(s, open); if (close < 0) continue;
      const names = [...s.slice(open, close).matchAll(/case\s+\.(\w+)[^:]*:\s*(?:return\s+)?(?:AnyView\()?([A-Z]\w*)\s*\(/g)].filter((c) => views.has(c[2])).map((c) => c[1]);
      if (names.length < 3) continue;
      for (const [e, cs] of enumCases) if (names.filter((n) => cs.has(n)).length >= names.length * 0.8) routeEnums.add(e);
    }
  }
  for (const f of swift) {
    const s = get(f); const L = lineIndex(s);
    for (const m of s.matchAll(/\benum\s+(\w+)\s*(?::[^{]*)?\{/g)) {
      if (!routeEnums.has(m[1])) continue;
      const open = s.indexOf('{', m.index); const close = closeOf(s, open); if (close < 0) continue;
      const body = s.slice(open, close);
      const cases = [...body.matchAll(/\bcase\s+([\w\s,()?:.<>\[\]]+?)(?=\n|$)/g)].flatMap((c) => splitCases(c[1])).filter((n) => /^[a-z]\w*$/.test(n));
      for (const c of [...new Set(cases)]) {
        // view for the case: `case .name...: X(` in any switch over this enum
        let view = null;
        for (const g of swift) { const t = get(g); const v = new RegExp(`case\\s+\\.${c}\\b[^:]*:\\s*(?:return\\s+)?(?:AnyView\\()?([A-Z]\\w*)\\s*\\(`).exec(t); if (v && views.has(v[1])) { view = v[1]; break; } }
        screens.push({ name: `${m[1]}.${c}`, kind: 'swiftui-route', view, file: f, line: L(open + body.indexOf(c)), group: m[1] });
      }
    }
    for (const m of s.matchAll(/NavigationLink\s*\(\s*(?:destination\s*:\s*([A-Z]\w*)\s*\(|[^)]{0,120}\)\s*\{\s*([A-Z]\w*)\s*\()/g)) { const to = m[1] || m[2]; if (views.has(to)) edges.push({ fromFile: f, line: L(m.index), to, kind: 'NavigationLink' }); }
    for (const m of s.matchAll(/\.(sheet|fullScreenCover|popover)\s*\([^)]*\)\s*\{\s*(?:NavigationStack\s*\{\s*)?([A-Z]\w*)\s*\(/g)) if (views.has(m[2])) edges.push({ fromFile: f, line: L(m.index), to: m[2], kind: m[1] });
    for (const m of s.matchAll(/\b(?:pushViewController|present|show|showDetailViewController|setViewControllers)\s*\(\s*\[?\s*([A-Z]\w*)\s*\(/g)) if (isVC(m[1])) edges.push({ fromFile: f, line: L(m.index), to: m[1], kind: 'uikit' });
    for (const m of s.matchAll(/performSegue\s*\(\s*withIdentifier\s*:\s*"([^"]+)"/g)) edges.push({ fromFile: f, line: L(m.index), to: `segue:${m[1]}`, kind: 'performSegue' });
    for (const m of s.matchAll(/\b(?:navigate|push|presentedSheet\s*=|path\.append)\s*\(?\s*(?:to\s*:\s*)?\.(\w+)/g)) edges.push({ fromFile: f, line: L(m.index), to: `.${m[1]}`, kind: 'route-enum' });
  }
  // SwiftUI views used only as destinations (NavigationLink/sheet) count as screens too
  const named = new Set(screens.map((x) => x.view || x.name));
  for (const e of edges) if (/NavigationLink|sheet|fullScreenCover|popover/.test(e.kind) && !named.has(e.to)) { named.add(e.to); screens.push({ name: e.to, kind: 'swiftui-view', file: e.fromFile, line: e.line }); }
  return { screens, edges, stats: { files: files.length, ms: Date.now() - t0 } };
}

function splitCases(s) { const out = []; let d = 0, cur = ''; for (const ch of s) { if ('(<['.includes(ch)) d++; if (')>]'.includes(ch)) d--; if (ch === ',' && d === 0) { out.push(cur); cur = ''; } else cur += ch; } out.push(cur); return out.map((x) => x.trim().split(/[(\s=]/)[0]); }

