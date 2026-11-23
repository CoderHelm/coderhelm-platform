// Joins infrastructure to code. A unit's source comes from the IaC itself (a source folder, an
// archive, a handler), or, for placeholder packages, from the pipeline that deploys it (a
// function name, an ECS service or an image a workflow builds). Env keys the IaC sets and the
// code reads confirm a join. Domains a unit sits behind join the code that calls them.
import fs from 'node:fs';
import path from 'node:path';
import { walk, read, isTest, lineIndex } from '../lib/text.mjs';
import { CODE_EXT } from '../externals.mjs';
import * as terraform from './terraform.mjs';
import * as cfn from './cfn.mjs';
import * as cdk from './cdk.mjs';
import * as deploys from './deploys.mjs';
import { edge } from './edge.mjs';

const P = path.posix;
const ENV_READ = /process\.env\.([A-Z][A-Z0-9_]+)|process\.env\[["']([A-Z][A-Z0-9_]+)|os\.environ(?:\.get)?\(?\[?["']([A-Z][A-Z0-9_]+)|os\.getenv\(["']([A-Z][A-Z0-9_]+)|import\.meta\.env\.([A-Z][A-Z0-9_]+)|getenv\(["']([A-Z][A-Z0-9_]+)/g;
const DOMAIN = /https?:\/\/([a-z0-9][a-z0-9\-.]+\.[a-z]{2,})/g;

const isFile = (root, p) => { try { return fs.statSync(path.join(root, p)).isFile(); } catch { return false; } };
const isDir = (root, p) => { try { return fs.statSync(path.join(root, p)).isDirectory(); } catch { return false; } };

/** folder + index.handler -> folder/index.(mjs|js|ts|py) when it exists; else the folder. */
function handlerFile(root, folder, handler) {
  if (folder == null) return null;
  if (isFile(root, folder)) return folder;
  for (const ext of ['.ts', '.js', '.mjs', '.py']) if (isFile(root, folder + ext)) return folder + ext;
  const mod = (handler || 'index.handler').replace(/\.[^.]+$/, '');
  for (const ext of ['.mjs', '.js', '.ts', '.py', '.cjs', '']) { const p = P.normalize(P.join(folder || '.', mod + ext)); if (isFile(root, p)) return p; }
  return isDir(root, folder) && folder ? folder : null;
}

function globMatch(pattern, name) {
  if (!pattern.includes('*')) return [pattern === name, 'exact'];
  if (pattern.replace(/[*\-_.]/g, '').length < 3) return [false, null];
  const rx = new RegExp('^' + pattern.split('*').map((x) => x.replace(/[.+?^${}()|[\]\\]/g, '\\$&')).join('.*') + '$');
  return [rx.test(name), 'inferred'];
}

function envReads(root, folder) {
  const keys = new Map();
  if (!isDir(root, folder)) return keys;
  for (const rel of walk(path.join(root, folder), CODE_EXT)) {
    if (isTest(rel)) continue;
    const t = read(path.join(root, folder), rel); if (t == null) continue;
    const L = lineIndex(t);
    for (const m of t.matchAll(ENV_READ)) { const k = m.slice(1).find(Boolean); if (!keys.has(k)) keys.set(k, []); keys.get(k).push([P.join(folder, rel), L(m.index)]); }
  }
  return keys;
}

export function analyse(root) {
  const tf = terraform.analyse(root);
  const cf = cfn.analyse(root);
  const ck = cdk.analyse(root);
  const units = new Map(), ecs = new Map();
  for (const m of [tf.lambdas, cf.lambdas, ck.lambdas]) for (const [k, u] of m) units.set(k, { ...u, kind: 'lambda' });
  for (const m of [tf.ecs, cf.ecs, ck.ecs]) for (const [k, u] of m) ecs.set(k, { ...u, kind: 'ecs' });
  const edges = [...tf.edges, ...cf.edges, ...ck.edges];

  // 1. source paths written in the IaC
  const src = [];
  for (const [uid, u] of units) {
    if (u.placeholder) continue;
    const seen = new Set();
    for (const [p, f, ln] of [...u.src].sort((a, b) => (a.join() < b.join() ? -1 : 1))) {
      if (seen.has(`${p}|${f}|${ln}`)) continue;
      seen.add(`${p}|${f}|${ln}`);
      let tgt = null;
      if (u.handlerFile) for (const ext of ['.js', '.ts', '.mjs', '.py']) if (isFile(root, u.handlerFile + ext)) tgt = u.handlerFile + ext;
      tgt = tgt || handlerFile(root, p, u.handler);
      if (tgt) src.push(edge('f:' + tgt, uid, 'config', 'iac_source', 'exact', 0.95, [[f, ln]]));
    }
  }
  // 2. deploy pipelines: function names and ECS services joined to the evaluated IaC names
  const recs = deploys.analyse(root);
  const ecrByFile = new Map();
  for (const r of recs) if (r.kind === 'ecr') { if (!ecrByFile.has(r.file)) ecrByFile.set(r.file, []); ecrByFile.get(r.file).push(r); }
  for (const r of recs) {
    const pool = r.kind === 'lambda' ? units : r.kind === 'ecs' ? ecs : null;
    if (!pool) continue;
    for (const [uid, u] of pool) {
      let hit = null;
      for (const pat of u.names) { const [ok, how] = globMatch(pat, r.name); if (ok) { hit = how; break; } }
      if (!hit) continue;
      let folder = r.folder;
      if (r.kind === 'ecs' && ecrByFile.get(r.file)) folder = ecrByFile.get(r.file)[0].folder;
      if (r.kind === 'lambda' && r.image) {
        const b = recs.filter((x) => x.kind === 'ecr' && x.name === r.image);
        if (b.length) folder = b[0].folder;
        else if (r.imageRef) {
          // the image path names a folder with a Dockerfile (matrix builds, monorepos)
          const parts = r.imageRef.split(':')[0].split('/').filter((x) => x && !x.includes('.') && !x.includes('*'));
          for (let i = 0; i < parts.length; i++) { const cand = parts.slice(i).join('/'); if (isFile(root, P.join(cand, 'Dockerfile'))) { folder = cand; break; } }
        }
      }
      const tgt = folder != null ? handlerFile(root, folder, null) : null;
      if (!tgt) continue;
      const how = hit === 'exact' && r.how === 'exact' ? 'exact' : 'inferred';
      src.push(edge('f:' + tgt, uid, 'config', 'deploy_workflow', how, how === 'exact' ? 0.9 : 0.7, [[r.file, r.line], [u.file || uid.split(':')[0], u.line || 1]], { name: r.name }));
    }
  }
  // 2b. an ECS or Lambda image that names a repository a workflow builds
  const joined = new Set(src.map((e) => e.b));
  for (const pool of [ecs, units]) for (const [uid, u] of pool) {
    if (joined.has(uid)) continue;
    for (const im of u.images || []) for (const r of recs) {
      if (r.kind !== 'ecr' || !new RegExp('/' + r.name.replace(/[.+?^${}()|[\]\\]/g, '\\$&') + '(?::|$)').test(im)) continue;
      const tgt = handlerFile(root, r.folder, null);
      if (tgt) src.push(edge('f:' + tgt, uid, 'config', 'image_build', 'inferred', 0.7, [[r.file, r.line]], { name: r.name }));
    }
  }
  // 3. env keys: the IaC sets them, the source folder reads them
  for (const e of src) {
    const u = units.get(e.b);
    if (!u || !u.envKeys.size) continue;
    const p = e.a.slice(2);
    const folder = isDir(root, p) ? p : P.dirname(p);
    const got = envReads(root, folder);
    const hit = [...u.envKeys].filter((k) => got.has(k)).sort();
    if (!hit.length) continue;
    e.conf = Math.min(0.99, Math.round((e.conf + 0.05) * 100) / 100);
    const [f, l] = got.get(hit[0])[0];
    e.evidence.push({ file: f, line: l });
    e.envMatch = hit;
  }
  // 4. domains in code whose origin is a unit (a function behind CloudFront, a function URL)
  const dom2unit = new Map();
  for (const e of edges) if (e.a.startsWith('domain:') && e.via === 'cloudfront_origin' && units.has(e.b)) { const d = e.a.slice(7); if (!dom2unit.has(d)) dom2unit.set(d, new Set()); dom2unit.get(d).add(e.b); }
  const calls = [];
  if (dom2unit.size) {
    for (const rel of walk(root, new Set([...CODE_EXT, '.json', '.env', '.example']))) {
      if (isTest(rel) || /(\.tf|package-lock\.json)$/.test(rel) || /(^|\/)(csp|content-security|cors|allow-?list)[^/]*$/i.test(rel)) continue;
      const t = read(root, rel); if (t == null || !t.includes('://')) continue;
      const L = lineIndex(t);
      for (const m of t.matchAll(DOMAIN)) for (const b of dom2unit.get(m[1]) || []) calls.push(edge('f:' + rel, 'domain:' + m[1], 'http', 'domain_literal', 'inferred', 0.8, [[rel, L(m.index)]], { unit: b }));
    }
  }
  // permissions confirm the trigger they allow; they stand alone only for load balancers
  const trig = edges.filter((e) => e.via !== 'lambda_permission');
  const out = [];
  for (const e of edges) {
    if (e.via === 'lambda_permission') {
      const pr = e.a.split(':')[1];
      const conf = trig.find((t) => t.b === e.b && ((pr === 's3' && t.via === 's3_notification') || (pr === 'apigateway' && t.via === 'api_integration') || (pr === 'events' && ['eventbridge_target', 'scheduler'].includes(t.via)) || (pr === 'sns' && t.via === 'sns_subscription') || (pr === 'cloudfront' && ['cloudfront_origin', 'function_url'].includes(t.via))));
      if (conf) { conf.evidence.push(...e.evidence); continue; }
    }
    out.push(e);
  }
  const keyed = new Map();
  for (const e of [...out, ...src, ...calls]) {
    const k = `${e.a}|${e.b}|${e.via}`;
    const was = keyed.get(k);
    if (!was) { keyed.set(k, e); continue; }
    for (const x of e.evidence) if (!was.evidence.some((y) => y.file === x.file && y.line === x.line)) was.evidence.push(x);
    was.conf = Math.max(was.conf, e.conf);
  }
  const plain = (m) => Object.fromEntries([...m].map(([k, u]) => [k, Object.fromEntries(Object.entries(u).map(([kk, v]) => [kk, v instanceof Set ? [...v].sort() : v]))]));
  return { units: plain(units), ecs: plain(ecs), cdns: [...tf.cdns.values()].map((c) => ({ id: c.id, file: c.file, line: c.line, domains: [...c.domains].sort() })), deploys: recs, edges: [...keyed.values()] };
}
