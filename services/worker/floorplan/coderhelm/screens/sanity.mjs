// Sanity Studio detector. A studio's "screens" are: workspaces (basePath), tools
// (structure, presentation, vision, custom), desk entries (structure builder list items), and the
// editors for each document type ("page kinds"). Also reads Presentation's locations / mainDocuments
// to join document types to the web URLs they render on (edges studio -> web).
//
// detect(root) -> { studios: [{ dir, workspaces, tools, pageKinds, desk, webLocations }] }
import path from 'node:path';
import { listFiles, read, stripComments, closeOf, objectEntries, lineIndex } from '../lib/text.mjs';

const P = path.posix;
const lit = (v) => /^["'`]([^"'`$]*)["'`]$/.exec((v || '').trim())?.[1] ?? null;

export function detect(root, { maxFiles = 40000 } = {}) {
  const t0 = Date.now();
  const all = listFiles(root, { exts: /\.(tsx?|jsx?|mjs)$/, max: maxFiles });
  const configs = all.filter((f) => /(^|\/)sanity\.config\.(tsx?|jsx?|mjs)$/.test(f));
  const studios = [];
  for (const cfg of configs) {
    const dir = P.dirname(cfg) === '.' ? '' : P.dirname(cfg);
    const files = all.filter((f) => (dir ? f.startsWith(dir + '/') : true) && !/(^|\/)(node_modules|dist|\.sanity)\//.test(f) && !/\.(test|spec)\./.test(f));
    const text = new Map(files.map((f) => [f, stripComments(read(root, f) || '')]));
    const c = text.get(cfg);
    const Lc = lineIndex(c);
    // workspaces: every object with a basePath (or the single defineConfig)
    const workspaces = [...c.matchAll(/\bbasePath\s*:\s*([^,\n}]+)/g)].map((m) => ({ basePath: lit(m[1]) ?? m[1].trim(), line: Lc(m.index) }));
    if (!workspaces.length) workspaces.push({ basePath: '/', line: 1 });
    // tools: xxxTool(...) in plugins, well-known tool plugins, and tools: [{name}]
    const tools = [];
    const seenTool = new Set();
    for (const f of files) {
      const s = text.get(f); if (!/plugins\s*:|tools\s*:|definePlugin|structureTool|deskTool/.test(s)) continue;
      const L = lineIndex(s);
      for (const m of s.matchAll(/\b(structureTool|deskTool|presentationTool|visionTool|dashboardTool|media|mediaLibrary|scheduledPublishing|releasesTool|\w+Tool)\s*\(/g)) {
        if (/^(define|create|use)/.test(m[1]) || /import|from/.test(s.slice(Math.max(0, m.index - 40), m.index))) continue;
        const name = { structureTool: 'structure', deskTool: 'desk', presentationTool: 'presentation', visionTool: 'vision', media: 'media', mediaLibrary: 'media-library', dashboardTool: 'dashboard', scheduledPublishing: 'schedules', releasesTool: 'releases' }[m[1]] || m[1].replace(/Tool$/, '');
        const open = s.indexOf('(', m.index); const close = closeOf(s, open);
        const named = lit(/\bname\s*:\s*([^,\n}]+)/.exec(s.slice(open, close > 0 ? Math.min(close, open + 600) : open + 600))?.[1]);
        const key = named || name;
        if (seenTool.has(key)) continue; seenTool.add(key);
        tools.push({ name: key, route: `/${key}`, file: f, line: L(m.index) });
      }
      for (const m of s.matchAll(/\btools\s*:\s*\[/g)) {
        const open = s.indexOf('[', m.index); const close = closeOf(s, open);
        for (const t of s.slice(open, close).matchAll(/\{\s*[^{}]*?\bname\s*:\s*["'`]([\w-]+)["'`][^{}]*?\btitle\s*:\s*["'`]([^"'`]+)/g)) {
          if (seenTool.has(t[1])) continue; seenTool.add(t[1]);
          tools.push({ name: t[1], title: t[2], route: `/${t[1]}`, file: f, line: L(open + t.index) });
        }
      }
    }
    // page kinds: document types
    const pageKinds = [];
    const kindSet = new Set();
    for (const f of files) {
      const s = text.get(f); if (!/type\s*:\s*["']document["']/.test(s)) continue;
      const L = lineIndex(s);
      for (const m of s.matchAll(/\btype\s*:\s*["']document["']/g)) {
        // the enclosing object literal, and its own name:
        let depth = 0, open = -1;
        for (let k = m.index - 1; k >= Math.max(0, m.index - 20000); k--) { const ch = s[k]; if (ch === '}' || ch === ']' || ch === ')') depth++; else if (ch === '{' || ch === '[' || ch === '(') { if (depth === 0) { open = ch === '{' ? k : -1; break; } depth--; } }
        if (open < 0) continue;
        const ents = objectEntries(s, open);
        const name = lit(ents.find((e) => e.key === 'name')?.value);
        if (!name || kindSet.has(name)) continue;
        kindSet.add(name);
        pageKinds.push({ name, title: lit(ents.find((e) => e.key === 'title')?.value), file: f, line: L(open), singleton: false });
      }
    }
    // desk: structure builder entries
    const desk = [];
    const helperFns = new Set();
    for (const f of files) {
      const s = text.get(f); if (!/\bS\.(list|listItem|documentTypeList|documentTypeListItem|documentTypeListItems|document|editor)\(/.test(s)) continue;
      const L = lineIndex(s);
      // helper functions that build an item from a `type` argument (createList({S, type, title}))
      for (const m of s.matchAll(/(?:const|function)\s+(\w+)\s*=?\s*(?:\([^)]*\)|\w+)\s*(?:=>|\{)/g)) {
        const body = s.slice(m.index, m.index + 1500);
        if (/\bS\.(listItem|documentTypeList|document)\(/.test(body) && /\btype\b/.test(body.slice(0, 200))) helperFns.add(m[1]);
      }
      const parentOf = (i) => {
        // nearest enclosing S.list().title('X') whose .items([ ... ]) contains i
        let best = null;
        for (const m of s.matchAll(/S\.list\(\)\s*(?:\.\w+\([^)]*\)\s*)*?\.title\(\s*([^)]+)\)[\s\S]{0,300}?\.items\(\s*\[/g)) {
          const open = s.indexOf('[', m.index + m[0].length - 1); const close = closeOf(s, open);
          if (open < i && close > i) best = lit(m[1]) ?? m[1].trim();
        }
        return best;
      };
      // inside the callback of .map/.flatMap/.forEach: walk out through unmatched '(' and look at what calls it
      const inLoop = (i) => {
        let d = 0;
        for (let k = i - 1; k >= Math.max(0, i - 3000); k--) {
          const ch = s[k];
          if (ch === ')' || ch === ']' || ch === '}') d++;
          else if (ch === '(' || ch === '[' || ch === '{') {
            if (d) { d--; continue; }
            if (ch === '(' && /\.(map|flatMap|forEach|reduce)\s*$/.test(s.slice(Math.max(0, k - 12), k))) return true;
            if (ch === '[' && /\.items\(\s*$/.test(s.slice(Math.max(0, k - 12), k))) return false; // reached the list itself
          }
        }
        return false;
      };
      for (const m of s.matchAll(/\bS\.(listItem|documentTypeListItem|documentTypeList|documentTypeListItems|divider)\(\s*([^)]*)\)/g)) {
        if (m[1] === 'divider') continue;
        const tail = s.slice(m.index, m.index + 900);
        const chainEnd = tail.search(/\n\s*\n|,\s*\n\s*(?:S\.|\/\/|\.\.\.)|\)\s*,\s*$/m);
        const chain = chainEnd > 0 ? tail.slice(0, chainEnd + 1) : tail.slice(0, 500);
        const titleRaw = /\.title\(\s*([^)]+?)\s*\)/.exec(chain)?.[1];
        const idRaw = /\.id\(\s*([^)]+?)\s*\)/.exec(chain)?.[1];
        const typ = lit(m[2]) ?? lit(/\.schemaType\(\s*([^)]+?)\s*\)/.exec(chain)?.[1]) ?? lit(/documentTypeList\(\s*([^)]+?)\s*\)/.exec(chain)?.[1]);
        const single = /S\.(document|editor)\(\)[\s\S]{0,120}\.documentId\(/.test(chain) || /S\.defaultDocument\(/.test(chain);
        if (m[1] === 'documentTypeListItems') {
          // one item per document type, minus a literal filter list when one is visible
          const filt = /\.filter\(/.test(tail.slice(0, 300)) ? [...s.matchAll(/\[\s*((?:["'][\w.-]+["']\s*,?\s*)+)\]/g)].flatMap((x) => [...x[1].matchAll(/["']([\w.-]+)["']/g)].map((y) => y[1])) : [];
          for (const k of pageKinds) if (!filt.includes(k.name)) desk.push({ title: k.title || k.name, type: k.name, route: `/structure/${k.name}`, file: f, line: L(m.index), group: parentOf(m.index), expandedFrom: 'S.documentTypeListItems()' });
          continue;
        }
        const title = lit(titleRaw);
        const dynamic = (!title && titleRaw) || inLoop(m.index) || (titleRaw && /\$\{/.test(titleRaw));
        const id = lit(idRaw) || (title ? title.replace(/[^A-Za-z0-9]+(.)?/g, (_, ch) => (ch ? ch.toUpperCase() : '')).replace(/^./, (x) => x.toLowerCase()) : typ);
        // helper bodies are templates, reported through their call sites instead
        const encl = [...s.slice(0, m.index).matchAll(/(?:const|function)\s+(\w+)\s*=?\s*(?:\([^)]*\)|\w+)\s*(?:=>|\{)/g)].pop();
        if (encl && helperFns.has(encl[1]) && /\btype\b/.test(titleRaw || '') + /\btype\b/.test(m[2] || '')) continue;
        desk.push({ title: title || (titleRaw ? `{${titleRaw.slice(0, 50)}}` : typ), type: typ, singleton: single, route: id ? `/structure/${id}` : null, file: f, line: L(m.index), group: parentOf(m.index), ...(dynamic ? { dynamic: true } : {}) });
      }
      // helper call sites: createList({ S, type: 'x', title: 'Y' }) / orderableDocumentListDeskItem({ type: 'x' })
      for (const m of s.matchAll(/\b(\w+)\(\s*\{([^{}]{0,400})\}\s*\)/g)) {
        if (!helperFns.has(m[1]) && !/DeskItem$|ListItem$/.test(m[1])) continue;
        const typ = lit(/\btype\s*:\s*([^,\n}]+)/.exec(m[2])?.[1]);
        if (!typ) continue;
        const title = lit(/\btitle\s*:\s*([^,\n}]+)/.exec(m[2])?.[1]);
        const id = lit(/\bid\s*:\s*([^,\n}]+)/.exec(m[2])?.[1]) || typ;
        desk.push({ title: title || typ, type: typ, singleton: /singleton/i.test(m[1]), route: `/structure/${id}`, file: f, line: L(m.index), group: parentOf(m.index), via: m[1], ...(inLoop(m.index) ? { dynamic: true } : {}) });
      }
    }
    // singletons mark their page kind
    for (const d of desk) if (d.singleton) { const k = pageKinds.find((x) => x.name === d.type); if (k) k.singleton = true; }
    // web locations: presentation resolve (defineLocations / mainDocuments route + filter _type)
    const webLocations = [];
    for (const f of files) {
      const s = text.get(f); if (!/defineLocations|mainDocuments|defineDocuments|locations\s*:/.test(s)) continue;
      const L = lineIndex(s);
      for (const m of s.matchAll(/\broute\s*:\s*["'`]([^"'`]+)["'`][^}]{0,300}?_type\s*==\s*["\\']*([\w.-]+)/g)) webLocations.push({ type: m[2], route: m[1], file: f, line: L(m.index), via: 'mainDocuments' });
      for (const m of s.matchAll(/\b(\w+)\s*:\s*defineLocations\(\s*\{([\s\S]{0,1500}?)\}\s*\)/g)) {
        for (const h of m[2].matchAll(/href\s*:\s*[`"']([^`"']+)[`"']/g)) webLocations.push({ type: m[1], route: h[1].replace(/\$\{[^}]*\}/g, ':param'), file: f, line: L(m.index), via: 'defineLocations' });
      }
    }
    const previewUrl = /previewUrl\s*:\s*\{?[^}]*?(?:origin|initial)\s*:\s*([^,\n}]+)/.exec(c)?.[1]?.trim() || null;
    studios.push({ dir: dir || '.', config: cfg, workspaces, tools, pageKinds, desk, webLocations, previewUrl });
  }
  return { studios, stats: { ms: Date.now() - t0 } };
}

