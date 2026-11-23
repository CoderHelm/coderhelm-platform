// CMS-driven routes. A catch-all route (/[...slug], /[[...slug]], /[slug], Remix $,
// Nuxt/Astro [...slug]) whose page is chosen at runtime from CMS data has no static URL list. What
// the code does show is the DISPATCH: a table or switch from a content type to a renderer. This
// lists those types as "page kinds" behind the catch-all, separately from block/section tables
// (which are parts of a page, not pages), and the literal URL shapes the resolver tests.
//
// detect(root, { cmsTypes }) -> { catchAlls: [{ file, route, pageKinds:[...], sections:[...], urlShapes:[...] }] }
import path from 'node:path';
import { listFiles, read, stripComments, closeOf, objectEntries, lineIndex, makeResolver } from '../lib/text.mjs';

const P = path.posix;
const CATCH = /(?:^|\/)(?:app|pages|routes|src\/routes|src\/pages)\/(?:.*\/)?(\[\[?\.\.\.[^\]]+\]\]?|\[[^\]/]+\]|\$)(?:\/(?:page|index|\+page))?\.(tsx?|jsx?|vue|astro|svelte|mdx)$/;
const TYPEY = /(_type|\btype|Type|blockType|__typename|\bkind|component|contentType|template|layout)\b/;
const SECTIONY = /block|section|module|slice|widget|component(s)?Map|layout|builder|portable|serializer/i;

export function detect(root, { cmsTypes = [], maxDepth = 3, maxFiles = 40000 } = {}) {
  const t0 = Date.now();
  const all = listFiles(root, { exts: /\.(tsx?|jsx?|mjs|vue|astro|svelte)$/, max: maxFiles });
  const fileSet = new Set(all);
  const cache = new Map();
  const get = (f) => { if (!cache.has(f)) cache.set(f, stripComments(read(root, f) || '')); return cache.get(f); };
  const resolve = makeResolver(root, all);
  const cms = new Set(cmsTypes.map((x) => x.toLowerCase().replace(/[-_]/g, '')));
  const out = [];
  for (const page of all.filter((f) => CATCH.test(f) && !/\/api\//.test(f))) {
    const seg = CATCH.exec(page)[1];
    const route = '/' + P.dirname(page).replace(/^(?:.*?\/)?(?:app|pages|routes)(?:\/|$)/, '').split('/').filter((s) => s && !/^\(.*\)$/.test(s) && !/^\[|^\$/.test(s)).join('/');
    const url = (route === '/' ? '' : route) + '/' + (seg.startsWith('[[') || seg.startsWith('[...') || seg === '$' ? '*' : ':' + seg.replace(/^\[|\]$/g, ''));
    // the page and what it imports, a few levels down
    const seen = new Map([[page, 0]]);
    const queue = [page];
    while (queue.length) {
      const f = queue.shift(); const d = seen.get(f); if (d >= maxDepth) continue;
      for (const m of get(f).matchAll(/(?:import|from)\s*\(?\s*["']([^"']+)["']/g)) { const t = resolve(f, m[1]); if (t && !seen.has(t)) { seen.set(t, d + 1); queue.push(t); } }
      if (seen.size > 300) break;
    }
    const pageKinds = [], sections = [], urlShapes = [];
    for (const [f, depth] of seen) {
      const s = get(f); const L = lineIndex(s);
      // tables: const X = { key: Component, ... } used as X[...type...] or satisfies Record<...>
      for (const m of s.matchAll(/\b(?:const|let|var)\s+([A-Za-z_$][\w$]*)\s*(?::\s*[^=]{1,200})?=\s*\{/g)) {
        const open = s.indexOf('{', m.index + m[0].length - 1);
        const close = closeOf(s, open); if (close < 0) continue;
        const ents = objectEntries(s, open).filter((e) => e.key);
        if (ents.length < 3) continue;
        const compish = ents.filter((e) => /^[A-Z][\w$.]*$/.test(e.value) || /^(?:\([^)]*\)|\w+)\s*=>\s*</.test(e.value) || /^(?:dynamic|lazy)\(/.test(e.value) || /^handle[A-Z]\w*$/.test(e.value));
        if (compish.length < Math.max(3, ents.length * 0.7)) continue;
        const name = m[1];
        const after = s.slice(close, close + 120);
        const indexed = new RegExp(`\\b${name}\\s*\\[([^\\]]{1,80})\\]`).exec(s);
        const typed = /^\s*\}\s*(satisfies|as)\s+Record</.test(after) || /Record<\s*\w*(Type|Kind)/.test(m[0]);
        if (!indexed && !typed) continue;
        const keyExpr = indexed?.[1] || '';
        const kind = SECTIONY.test(name) || /blockType|_key|blocks?\b|section/i.test(keyExpr) ? 'section' : (TYPEY.test(keyExpr) || typed ? 'page' : 'section');
        const handlerTable = compish.every((e) => /^handle[A-Z]/.test(e.value));
        for (const e of ents) {
          const item = { name: e.key, renderer: e.value.slice(0, 60), file: f, line: L(e.start), table: name, depth, cmsType: cms.has(e.key.toLowerCase().replace(/[-_]/g, '')), ...(handlerTable ? { handler: true } : {}) };
          (kind === 'page' ? pageKinds : sections).push(item);
        }
      }
      // switch (doc._type) { case 'post': return <Post/> }
      for (const m of s.matchAll(/switch\s*\(\s*([^)]{1,80})\)\s*\{/g)) {
        if (!TYPEY.test(m[1])) continue;
        const open = s.indexOf('{', m.index + m[0].length - 1); const close = closeOf(s, open); if (close < 0) continue;
        const body = s.slice(open, close);
        const cases = [...body.matchAll(/case\s+["'`]([\w-]+)["'`]\s*:\s*(?:\{?\s*)?return\s*(<[A-Z][\w.]*|[A-Z][\w$]*\s*\(|null)/g)];
        if (cases.length < 3) continue;
        const kind = /block|section/i.test(m[1]) ? 'section' : 'page';
        for (const c of cases) (kind === 'page' ? pageKinds : sections).push({ name: c[1], renderer: c[2].replace(/^</, ''), file: f, line: L(open + c.index), table: `switch(${m[1].trim()})`, depth, cmsType: cms.has(c[1].toLowerCase().replace(/[-_]/g, '')) });
      }
      // literal URL shapes the resolver tests: parts[0] === 'blog', segments[1] === "classes"
      for (const m of s.matchAll(/\b(\w*(?:slug|segment|part|path)\w*)\s*\[\s*(\d+)\s*\]\s*===?\s*["'`]([\w-]+)["'`]/gi)) urlShapes.push({ index: +m[2], literal: m[3], file: f, line: L(m.index) });
    }
    // a page kind listed twice (renderer table + handler table) is one kind
    const norm = (n) => n.toLowerCase().replace(/[-_]/g, '');
    const uniq = (xs) => { const m = new Map(); for (const x of xs) if (!m.has(norm(x.name))) m.set(norm(x.name), x); return [...m.values()]; };
    // renderer tables name the page kinds; a handler table (type -> handleX) only adds the CMS names
    // it resolves, so a kind is not counted twice under two spellings
    const renderers = pageKinds.filter((k) => !k.handler);
    const handlers = pageKinds.filter((k) => k.handler);
    let kinds = uniq(renderers.length ? renderers : handlers);
    const resolverOnly = renderers.length ? uniq(handlers).filter((h) => !kinds.some((k) => norm(k.name) === norm(h.name))) : [];
    for (const h of handlers) { const k = kinds.find((x) => norm(x.name) === norm(h.name)); if (k && h.cmsType) k.cmsType = true; }
    if (kinds.length || sections.length) out.push({ file: page, route: url, pageKinds: kinds, resolverOnly, sections: uniq(sections), urlShapes });
  }
  return { catchAlls: out, stats: { ms: Date.now() - t0 } };
}

