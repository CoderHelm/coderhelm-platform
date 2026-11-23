// Inbound webhooks (an outside service calls one of our endpoints back) and data links (an ETL
// job writes or reads a table that an app's ORM entity holds).
import path from 'node:path';
import { walk, read, isTest, lineIndex } from './lib/text.mjs';
import { CATALOG } from './externals.mjs';

const HOOK_SEG = /^(webhooks?|hooks?|callbacks?|notifications?|notify|ipn|events?|inbound)$/i;
const SIG = /hmac|signature|x-[\w-]*-signature|verifyWebhook|constructEvent|validateHmac|shared.?secret|webhook.?secret|SkipApiKeyCheck|Public\(\)|@Public/i;
// the app's own nouns next to a hook word are its product API, not a callback
const OWN_NOUN = /^(api|v\d+|admin|internal|me|user|users|account|accounts|test|tests|status|health|list|new|create|update|delete)$/i;

function vendorOfSegment(seg, services) {
  const s = seg.toLowerCase();
  for (const [v, [, , stems]] of Object.entries(CATALOG)) {
    if (s === v || s.replace(/-/g, '') === v.replace(/[-.]/g, '') || stems.some((x) => x.replace(/_+$/, '').toLowerCase() === s)) return v;
  }
  for (const k of Object.keys(services)) if (k.startsWith('env:') && k.slice(4) === s) return k;
  return null;
}

/**
 * Webhook endpoints: POST/PUT routes with a hook word in the path. The vendor is a neighbouring
 * segment that names a catalog vendor or an env-named service; a vendor not in the catalog is
 * named from the segment beside the hook word (never from :params or the app's own nouns).
 * @param routes graph.routes; services: findServices().services
 */
export function webhooks(root, routes, services) {
  const out = [];
  routes.forEach((r, i) => {
    if (!r || !r.p || !r.f || isTest(r.f)) return;
    if (!['POST', 'PUT', 'ALL', undefined, null, '*'].includes(r.m)) return;
    const segs = r.p.split('#')[0].split('/').filter(Boolean);
    const hookAt = segs.findIndex((s) => HOOK_SEG.test(s));
    if (hookAt < 0) return;
    const named = segs.filter((s) => !s.startsWith(':') && !s.startsWith('[') && s !== '*' && !HOOK_SEG.test(s));
    let vendor = null;
    for (const s of named) { vendor = vendorOfSegment(s, services); if (vendor) break; }
    let guessed = null;
    if (!vendor) {
      // the segment right beside the hook word: /webhooks/acme, /acme/callback
      const near = [segs[hookAt + 1], segs[hookAt - 1]].find((s) => s && /^[a-z][a-z0-9-]{2,}$/i.test(s) && !OWN_NOUN.test(s) && !HOOK_SEG.test(s));
      if (near) guessed = near.toLowerCase();
    }
    // /events or /webhooks alone, or with only :params, is usually the product's own API
    if (!vendor && !guessed) return;
    const text = read(root, r.f) || '';
    const sig = SIG.test(text);
    const conf = vendor && sig ? 0.9 : vendor ? 0.7 : sig ? 0.6 : 0.4;
    out.push({ route: i, vendor, guessed, method: r.m || 'POST', path: r.p, file: r.f, line: r.l || 1, signature: sig, conf });
  });
  return out;
}

const ENTITY = [/@Entity\(\s*["']([\w.]+)["']/g, /@Entity\(\s*\{[^}]*name\s*:\s*["']([\w.]+)["']/g, /__tablename__\s*=\s*["']([\w.]+)["']/g, /db_table\s*=\s*["']([\w.]+)["']/g, /@@map\(\s*"([\w.]+)"\s*\)/g, /@Table\(\s*(?:name\s*=\s*)?["']([\w.]+)["']/g];
const WRITE = /\b(?:INSERT\s+INTO|UPDATE|MERGE\s+INTO|COPY|TRUNCATE(?:\s+TABLE)?|DELETE\s+FROM|UPSERT\s+INTO)\s+(?:ONLY\s+)?["`]?(\w+(?:\.\w+)?)["`]?/gi;
const READ = /\b(?:FROM|JOIN)\s+["`]?([a-z_]\w*(?:\.\w+)?)["`]?/gi;
const PYW = /\.(?:to_sql|insert_rows|bulk_load|copy_expert)\(\s*["']?(?:table\s*=\s*)?["']([\w.]+)["']/g;
const CONN = /(?:conn_id|postgres_conn_id|mysql_conn_id|snowflake_conn_id|redshift_conn_id)\s*=\s*["']([\w-]+)["']|get_connection\(\s*["']([\w-]+)["']|Hook\(\s*["']([\w-]+)["']/g;
const ETL_DIRS = ['dags', 'airflow', 'etl', 'pipelines', 'jobs', 'sql', 'dbt', 'models'];

/** ETL files to the ORM entities whose tables they write or read, across repositories only. */
export function dataLinks(root, repos) {
  const entities = new Map();
  const files = (exts) => repos.flatMap((r) => walk(path.join(root, r), new Set(exts)).map((f) => `${r}/${f}`));
  for (const rel of files(['.ts', '.py', '.prisma', '.java', '.kt', '.rb'])) {
    if (isTest(rel)) continue;
    const t = read(root, rel); if (t == null || !/@Entity|__tablename__|db_table|@@map|@Table/.test(t)) continue;
    const L = lineIndex(t);
    for (const rx of ENTITY) for (const m of t.matchAll(rx)) { const name = m[1].split('.').pop().toLowerCase(); if (!entities.has(name)) entities.set(name, []); entities.get(name).push([rel, L(m.index)]); }
  }
  const out = [];
  if (!entities.size) return { links: out, entities: 0 };
  for (const rel of files(['.py', '.sql'])) {
    if (isTest(rel) || !ETL_DIRS.some((g) => ('/' + rel).includes('/' + g + '/'))) continue;
    const t = read(root, rel); if (t == null) continue;
    const L = lineIndex(t);
    const conns = [...new Set([...t.matchAll(CONN)].map((m) => m.slice(1).find(Boolean)))].sort();
    const seen = new Set();
    for (const [rx, mode] of [[WRITE, 'write'], [PYW, 'write'], [READ, 'read']]) {
      for (const m of t.matchAll(rx)) {
        const tbl = m[1].split('.').pop().toLowerCase();
        if (!entities.has(tbl) || seen.has(`${tbl}|${mode}`) || (mode === 'read' && seen.has(`${tbl}|write`))) continue;
        seen.add(`${tbl}|${mode}`);
        const [ef, el] = entities.get(tbl)[0];
        if (ef.split('/')[0] === rel.split('/')[0]) continue; // an app reading its own tables is not a link between systems
        out.push({ file: rel, line: L(m.index), entity: ef, entityLine: el, table: tbl, mode, conns, conf: mode === 'write' ? 0.85 : 0.6 });
      }
    }
  }
  return { links: out, entities: entities.size };
}
