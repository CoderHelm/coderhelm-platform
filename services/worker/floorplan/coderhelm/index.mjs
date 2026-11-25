// CoderHelm's additions to a built Floorplan map (driver hook H2, after build.mjs): screens,
// outside services, calls through wrappers and GraphQL, infra joined to code, inbound webhooks
// and data links. Deterministic, no model, nothing from the repositories is run.
//
// What it adds (the graph contract; the viewer ignores fields it doesn't know):
//   nodes[]  every node gets `level` (system | part | code); outside services are
//            kind "external" with {vendor, how: catalog|sdk|env, evidence[]}; infra resources
//            that join code are kind "infra" with {resource, evidence[]}
//   links[]  k: nav (screen to screen), trigger (infra to code), deploy (workflow to function to
//            source), webhook (vendor to our endpoint), data (pipeline to entity), and http/import
//            with more coverage; every added link has evidence[] and inferred
//   screens[] from Floorplan's buildScreens with our hints, plus nav {app, navigator, group,
//            gate: signed_in|signed_out|null, evidence[]}
//   routes[] proxy and GraphQL endpoints Floorplan doesn't list, appended (indexes stay put)
//   extract  what ran, how long, notes, and outside hosts nobody named (never drawn as boxes)
// Evidence is {file, line, how: code|inferred, confidence}. Tests are never evidence.
import fs from 'node:fs';
import path from 'node:path';
import { isTest, ev, startClock, stopClock } from './lib/text.mjs';
import { loadFloorplan } from './lib/fp.mjs';
import { findServices, CODE_EXT } from './externals.mjs';
import { findCalls } from './calls.mjs';
import * as infra from './infra/join.mjs';
import { webhooks, dataLinks } from './hooks.mjs';
import * as screens from './screens.mjs';

export const CAPS = { infraNodes: 40, serviceLinks: 12, httpPerFile: 60, linksTotal: 4000, detectorMs: 20000 };

const slug = (s) => String(s).toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '').slice(0, 60) || 'x';
const humanize = (s) => { const t = String(s).replace(/[-_]+/g, ' ').trim(); return t.charAt(0).toUpperCase() + t.slice(1); };

function run(name, fn, out) {
  startClock(CAPS.detectorMs);
  const t = Date.now();
  let r = null;
  try { r = fn(); } catch (e) { out.notes.push(`The ${name} detector failed: ${String(e.message || e).slice(0, 160)}`); }
  if (stopClock()) out.notes.push(`The ${name} detector stopped at its ${CAPS.detectorMs / 1000} s limit; what it lists may be short.`);
  out.ms[name] = Date.now() - t;
  return r;
}

function newNode(id, parent, label, kind, summary, extra) {
  return { id, parent, label, kind, summary, details: '', tags: ['coderhelm'], paths: [], packages: [], hosts: [], stats: { files: 0, loc: 0, langs: {}, repos: [] }, ...extra };
}

/** Floorplan's levels for every node: top-level parts are the system, their children parts, deeper ones code. */
function setLevels(graph) {
  const byId = new Map(graph.nodes.map((n) => [n.id, n]));
  for (const n of graph.nodes) {
    let d = 0, p = n.parent, guard = 0;
    while (p && byId.has(p) && guard++ < 50) { d++; p = byId.get(p).parent; }
    n.level = d === 0 ? 'system' : d === 1 ? 'part' : 'code';
  }
}

/**
 * @param root  the map root (one folder per repository, plus floorplan/)
 * @param skill the vendored Floorplan skill folder (for buildScreens and its YAML reader)
 * @param graph graph.json as build.mjs wrote it (changed in place and returned)
 * @param scan  floorplan/.cache/scan.json
 */
export async function enrich({ root, skill, graph, scan }) {
  const t0 = Date.now();
  await loadFloorplan(skill);
  const out = { version: 1, ms: {}, notes: [], counts: {} };
  const repos = fs.readdirSync(root, { withFileTypes: true }).filter((d) => d.isDirectory() && d.name !== 'floorplan' && !d.name.startsWith('.')).map((d) => d.name).sort();
  const fileSet = new Set(graph.files.map((f) => f.p));
  const nodeIds = new Set(graph.nodes.map((n) => n.id));
  const linkKey = (l) => `${l.a}>${l.b}>${l.k}`;
  let have = new Map();
  let added = 0;
  const addLink = (l) => {
    if (added >= CAPS.linksTotal) return null;
    const was = have.get(linkKey(l));
    if (was) {
      was.evidence = [...(was.evidence || []), ...l.evidence].slice(0, 8);
      if (l.labels) was.labels = [...new Set([...(was.labels || []), ...l.labels])].slice(0, 12);
      if (l.calls) was.calls = [...(was.calls || []), ...l.calls].slice(0, 40);
      return was;
    }
    graph.links.push(l); have.set(linkKey(l), l); added++;
    return l;
  };
  const codeFile = (f) => f && fileSet.has(f) && !isTest(f);

  // Tests are never evidence: Floorplan's http links made from test files are dropped.
  const httpBefore = graph.links.length;
  graph.links = graph.links.filter((l) => !(l.k === 'http' && l.a.startsWith('f:') && isTest(l.a.slice(2))));
  out.counts.testHttpDropped = httpBefore - graph.links.length;
  have = new Map(graph.links.map((l) => [linkKey(l), l]));

  // ---------- infra ----------
  const inf = run('infra', () => infra.analyse(root), out) || { units: {}, ecs: {}, edges: [], deploys: [] };

  // ---------- screens (detection; merged after the services so `behind` reaches them) ----------
  const found = run('screens', () => screens.detect(root, repos, { screens: graph.screens || [] }), out) || { hints: [], edges: [], expoRoots: new Map(), rnRepos: new Set(), notes: [], ms: {} };
  out.notes.push(...found.notes);
  Object.assign(out.ms, Object.fromEntries(Object.entries(found.ms).map(([k, v]) => [`screens:${k}`, v])));
  const tools = found.hints.filter((h) => h.kind === 'tool').map((h) => h.name);

  // ---------- outside services ----------
  const svc = run('services', () => findServices(root, repos, { scan, own: tools }), out) || { services: {}, otherHosts: [] };
  const serviceNode = new Map();
  const externals = graph.nodes.filter((n) => n.kind === 'external');
  const awsServices = Object.values(svc.services).filter((s) => s.key.startsWith('aws:'));
  const merged = Object.values(svc.services).filter((s) => !s.key.startsWith('aws:'));
  if (awsServices.length) {
    // the AWS SDK clients are one box with the services as labels, not one box each
    merged.push({ key: 'aws', vendor: 'aws', label: 'AWS', category: 'cloud', how: 'sdk', inferred: false, packages: [...new Set(awsServices.flatMap((s) => s.packages))].slice(0, 8), hosts: [], env: [], services: awsServices.map((s) => s.key.slice(4)).sort(), repos: [...new Set(awsServices.flatMap((s) => s.repos))].sort(), evidence: awsServices.flatMap((s) => s.evidence.slice(0, 2)).slice(0, 12) });
  }
  for (const s of merged.sort((a, b) => (a.key < b.key ? -1 : 1))) {
    // the architecture job may already have drawn it (by id, label or a shared package)
    let n = externals.find((x) => x.id === s.vendor || slug(x.label) === slug(s.label) || slug(x.label) === slug(s.vendor) || (x.packages || []).some((p) => s.packages.includes(p)));
    if (!n) {
      let id = `ext-${slug(s.vendor)}`;
      while (nodeIds.has(id)) id += '-x';
      n = newNode(id, null, s.label, 'external', s.how === 'env' ? `An outside service the code reaches through ${s.env.slice(0, 3).join(', ')}.` : `${s.label} (${s.category}).`, { packages: s.packages, hosts: s.hosts });
      graph.nodes.push(n); nodeIds.add(id);
    }
    Object.assign(n, { vendor: s.vendor, how: s.how, inferred: s.inferred, category: s.category, env: s.env, evidence: [...(n.evidence || []), ...s.evidence].slice(0, 12) });
    if (s.services) n.services = s.services;
    n.stats = { ...(n.stats || {}), repos: [...new Set([...(n.stats?.repos || []), ...s.repos])].sort() };
    serviceNode.set(s.key, n);
    if (s.key === 'aws') for (const a of awsServices) serviceNode.set(a.key, n);
    // the code that uses it: import links for packages and SDK clients, http links for env-named services
    const k = s.how === 'env' ? 'http' : 'import';
    let c = 0;
    for (const e of s.evidence) {
      if (c >= CAPS.serviceLinks || !codeFile(e.file) || !CODE_EXT.has(path.extname(e.file))) continue;
      if (addLink({ a: 'f:' + e.file, b: 'n:' + n.id, k, w: 1, labels: s.env.length && k === 'http' ? s.env.slice(0, 3) : undefined, inferred: s.how === 'env', evidence: [e] })) c++;
    }
  }
  out.otherHosts = svc.otherHosts.slice(0, 40);
  out.counts.services = merged.length;

  // ---------- calls through wrappers, URL maps, Nest proxies and GraphQL ----------
  const calls = run('calls', () => findCalls(root, repos, graph.routes.map((r) => ({ m: r.m, p: r.p, f: r.f }))), out) || { links: [], extraRoutes: [] };
  const routeBase = graph.routes.length;
  for (const r of calls.extraRoutes) {
    graph.routes.push({ f: r.f, m: r.m, p: r.p, l: r.l, h: r.kind === 'nest_proxy' ? 'proxy' : 'graphql', name: r.kind === 'nest_proxy' ? `Proxy to ${r.targetEnv}` : r.name, d: '', source: 'coderhelm', evidence: [ev(r.f, r.l, 'code', 0.9)] });
    if (r.kind === 'nest_proxy' && codeFile(r.f)) {
      const target = [...serviceNode.values()].find((n) => (n.env || []).includes(r.targetEnv));
      if (target) addLink({ a: 'f:' + r.f, b: 'n:' + target.id, k: 'http', w: 1, labels: [`${r.m} ${r.p}`], inferred: true, evidence: [ev(r.f, r.l, 'inferred', 0.7)] });
    }
  }
  const byPair = new Map(), perFile = new Map();
  for (const l of calls.links) {
    if (!codeFile(l.file) || !codeFile(l.routeF)) continue;
    const key = `${l.file}>${l.routeF}`;
    if (!byPair.has(key)) {
      if ((perFile.get(l.file) || 0) >= CAPS.httpPerFile) continue;
      perFile.set(l.file, (perFile.get(l.file) || 0) + 1);
      byPair.set(key, { a: 'f:' + l.file, b: 'f:' + l.routeF, k: 'http', w: 0, labels: [], calls: [], inferred: false, evidence: [] });
    }
    const x = byPair.get(key);
    x.w++;
    // the endpoint's own path, except on a wildcard, where the specific path the client asks for says more
    const label = `${l.method} ${l.wildcard ? l.path : l.routeP}`;
    if (!x.labels.includes(label) && x.labels.length < 12) x.labels.push(label);
    if (x.calls.length < 40) x.calls.push([l.route, l.line]);
    const how = l.how === 'exact' && !l.wildcard ? 'code' : 'inferred';
    if (how === 'inferred') x.inferred = true;
    if (x.evidence.length < 6) x.evidence.push(ev(l.file, l.line, how, how === 'code' ? 0.9 : 0.7));
  }
  for (const l of byPair.values()) addLink(l);
  out.counts.callLinks = byPair.size;
  out.counts.extraRoutes = graph.routes.length - routeBase;

  // ---------- infra joined to code ----------
  out.counts.infra = addInfra(graph, inf, { addLink, codeFile, nodeIds, out });

  // ---------- inbound webhooks and data links ----------
  const hooks = run('webhooks', () => webhooks(root, graph.routes, svc.services), out) || [];
  let wh = 0;
  const unnamed = [];
  for (const h of hooks) {
    const n = h.vendor ? serviceNode.get(h.vendor) : null;
    if (!n || !codeFile(h.file)) { if (h.guessed) unnamed.push({ vendor: h.guessed, route: `r:${h.route}`, path: h.path, evidence: [ev(h.file, h.line, 'inferred', h.conf)] }); continue; }
    if (addLink({ a: 'n:' + n.id, b: 'f:' + h.file, k: 'webhook', w: 1, labels: [`${h.method} ${h.path}`], route: `r:${h.route}`, calls: [[h.route, h.line]], signature: h.signature, inferred: true, evidence: [ev(h.file, h.line, 'inferred', h.conf)] })) wh++;
  }
  out.webhooksUnnamed = unnamed.slice(0, 20);
  out.counts.webhooks = wh;
  const data = run('data', () => dataLinks(root, repos), out) || { links: [] };
  let dl = 0;
  for (const d of data.links) {
    if (!codeFile(d.file) || !codeFile(d.entity)) continue;
    const l = addLink({ a: 'f:' + d.file, b: 'f:' + d.entity, k: 'data', w: 1, labels: [`${d.mode}s ${d.table}`], tables: [d.table], conns: d.conns.length ? d.conns : undefined, inferred: true, evidence: [ev(d.file, d.line, 'inferred', d.conf), ev(d.entity, d.entityLine, 'code', 0.95)] });
    if (l && l.tables && !l.tables.includes(d.table)) l.tables.push(d.table);
    if (l) dl++;
  }
  out.counts.dataLinks = dl;

  // ---------- screens: merged now, so Floorplan's `behind` reaches the services above ----------
  setLevels(graph);
  const sm = run('screens-merge', () => screens.mergeScreens({ graph, scan, root, found }), out);
  if (sm) { for (const l of sm.links) addLink(l); out.counts.screensAdded = sm.added; out.counts.fakeScreensDropped = sm.removed; out.counts.navLinks = sm.navLinks; }
  out.ms.total = Date.now() - t0;
  out.notes = [...new Set(out.notes)].slice(0, 40);
  graph.extract = out;
  return graph;
}

/** Infra nodes (compute units that join code, and the resources in front of them) and their links. */
function addInfra(graph, inf, { addLink, codeFile, nodeIds, out }) {
  const units = { ...inf.units, ...inf.ecs };
  // a folder's entry file: where its code starts (main, index, server, app), else its Dockerfile
  const ENTRY = ['src/main', 'main', 'src/index', 'index', 'src/server', 'server', 'src/app', 'app', 'handler', 'lambda_function'];
  const entryFile = (p) => {
    if (codeFile(p)) return p;
    const inside = graph.files.filter((x) => x.p.startsWith(p + '/') && !isTest(x.p)).map((x) => x.p);
    if (!inside.length) return null;
    for (const e of ENTRY) { const hit = inside.find((x) => x.slice(p.length + 1).replace(/\.[^./]+$/, '') === e && CODE_EXT.has(path.extname(x))); if (hit) return hit; }
    const own = (x) => x.slice(p.length + 1);
    return inside.find((x) => own(x) === 'Dockerfile') || inside.find((x) => own(x) === 'package.json') || inside.find((x) => CODE_EXT.has(path.extname(x)) && !own(x).includes('/') && !own(x).startsWith('.')) || null;
  };
  // where a unit's code is: the first source join, by confidence
  const codeOf = new Map(), deployLinks = [];
  for (const e of [...inf.edges].sort((a, b) => b.conf - a.conf)) {
    if (!['iac_source', 'deploy_workflow', 'image_build'].includes(e.via)) continue;
    const f = e.a.slice(2);
    if (!entryFile(f)) continue;
    if (!codeOf.has(e.b)) codeOf.set(e.b, e);
    deployLinks.push(e);
  }
  // Lambda copies made per app (one function, many CloudFront instances) are one box; ECS services stay apart
  const groupOf = (uid) => (uid.includes('aws_ecs_service') ? uid : uid.split('#')[0]);
  const joined = [...codeOf.keys()];
  if (!joined.length) return { nodes: 0, links: 0 };
  // the parent: the architecture's own infra part when it holds the IaC, else a new top-level part
  const iacFile = (units[joined[0]]?.file) || joined[0].split(':')[0];
  const owner = graph.files.find((f) => f.p === iacFile)?.n;
  const byId = new Map(graph.nodes.map((n) => [n.id, n]));
  let top = owner; let g = 0;
  while (top && byId.get(top)?.parent && g++ < 50) top = byId.get(top).parent;
  let parent = top && byId.get(top)?.kind === 'infra' ? top : graph.nodes.find((n) => n.kind === 'infra' && !n.parent)?.id;
  if (!parent) { parent = nodeIds.has('cloud') ? 'cloud-setup' : 'cloud'; graph.nodes.push(newNode(parent, null, 'Cloud setup', 'infra', 'What the code runs on, from the infrastructure code: functions, services and what triggers them.', { evidence: [ev(iacFile, 1, 'code', 0.9)] })); nodeIds.add(parent); }
  const nodes = new Map(); // key -> node
  let budget = CAPS.infraNodes;
  const mk = (key, label, resource, evidence, extra = {}) => {
    if (nodes.has(key)) return nodes.get(key);
    if (budget <= 0) return null;
    budget--;
    let id = `infra-${slug(key.startsWith('sites:') ? `sites-${key.split(':')[1].replace(/^infra-/, '')}` : label)}`;
    while (nodeIds.has(id)) id += '-x';
    nodeIds.add(id);
    const n = newNode(id, parent, label, 'infra', '', { resource, evidence: evidence.slice(0, 6), ...extra });
    nodes.set(key, n);
    graph.nodes.push(n);
    return n;
  };
  const clean = (x) => x.replace(/(^|[-_.])(prod|production|staging|stage|stg|dev|development)(?=$|[-_.])/g, '$1').replace(/^[-_.*]+|[-_.*]+$/g, '').replace(/[-_.]{2,}/g, '-');
  const logicalName = (uids) => {
    const names = uids.map((uid) => [...(units[uid]?.names || [])].map(clean).filter((x) => x && x !== '*').sort((a, b) => a.length - b.length)[0]).filter(Boolean);
    if (!names.length) return uids[0].split(':').pop().split('.').pop().split('#')[0];
    if (names.length === 1) return names[0];
    // copies of one function made per app: the words every copy shares (site-a-redirects, site-b-redirects)
    const toks = names.map((n) => n.split(/[-_.]/));
    const tail = [];
    for (let i = 1; i <= Math.min(...toks.map((t) => t.length)); i++) { const w = toks[0][toks[0].length - i]; if (toks.every((t) => t[t.length - i] === w)) tail.unshift(w); else break; }
    return tail.join('-') || names[0];
  };
  const unitLabel = (uids, isEcs) => {
    let n = logicalName(uids);
    for (let i = 0; i < 3; i++) n = n.replace(/(^|[-_.])(ecs[-_.])?(service|function|lambda|fn)$/i, '');
    return `${humanize(n || logicalName(uids))} ${isEcs ? 'service' : 'function'}`;
  };
  // 1. compute units that join code, one box per group
  const unitNode = new Map(), groups = new Map();
  for (const uid of joined) if (units[uid]) { const k = groupOf(uid); if (!groups.has(k)) groups.set(k, []); groups.get(k).push(uid); }
  for (const [key, uids] of groups) {
    const u = units[uids[0]];
    const isEcs = uids[0].includes('aws_ecs_service');
    const n = mk(key, unitLabel(uids, isEcs), isEcs ? 'ecs_service' : 'lambda', [ev(u.file, u.line, 'code', 0.95)], { names: [...new Set(uids.flatMap((x) => [...(units[x].names || [])]))].slice(0, 6), copies: uids.length > 1 ? uids.length : undefined });
    if (n) for (const uid of uids) unitNode.set(uid, n);
  }
  let links = 0;
  // 2. deploy: workflow to function, function to its source
  for (const e of deployLinks) {
    const n = unitNode.get(e.b); if (!n) continue;
    const f = e.a.slice(2);
    const target = entryFile(f);
    if (!target) continue;
    const how = e.how === 'exact' ? 'code' : 'inferred';
    const evidence = e.evidence.map((x) => ev(x.file, x.line, how, e.conf));
    if (addLink({ a: 'n:' + n.id, b: 'f:' + target, k: 'deploy', w: 1, labels: [e.via === 'iac_source' ? 'source' : e.via === 'image_build' ? 'image' : 'deploys'], via: e.via, inferred: how === 'inferred', evidence })) links++;
    if (e.via === 'deploy_workflow' || e.via === 'image_build') {
      const wf = e.evidence[0]?.file;
      if (codeFile(wf) || (wf && graph.files.some((x) => x.p === wf))) if (addLink({ a: 'f:' + wf, b: 'n:' + n.id, k: 'deploy', w: 1, labels: [e.name ? `deploys ${e.name}` : 'deploys'], via: e.via, inferred: how === 'inferred', evidence: evidence.slice(0, 1) })) links++;
    }
  }
  // 3. triggers: the resource in front of a unit (CDN, function URL, bucket, queue, rule) to its code
  const RESOURCE = { s3: ['bucket', 'Bucket'], url: ['function_url', 'Function URL'], events: ['rule', 'Event rule'], schedule: ['rule', 'Schedule'], apigw: ['api', 'API'], sns: ['topic', 'Topic'], sqs: ['queue', 'Queue'], queue: ['queue', 'Queue'], kinesis: ['stream', 'Stream'], dynamodb: ['stream', 'Stream'], domain: ['cloudfront', 'CDN'], perm: ['load_balancer', 'Load balancer'] };
  const sourceLabel = (a) => {
    const [kind, ...rest] = a.split(':');
    const val = rest.join(':');
    if (kind === 'domain') return val.startsWith('cdn:') ? 'CDN' : val.replace(/^(prod|production|staging|stage|dev|preview)[-.]/, '');
    if (kind === 'url') return 'Function URL';
    const tail = val.split(/[:/]/).pop().replace(/^(aws_\w+|data)\./, '').replace(/^aws_[a-z0-9_]+\./, '');
    return `${RESOURCE[kind]?.[1] || humanize(kind)} ${tail.split('.').pop()}`.trim();
  };
  const TRIG = new Set(['s3_notification', 'event_source_mapping', 'function_url', 'api_integration', 'eventbridge_target', 'scheduler', 'sns_subscription', 'cloudfront_origin', 'lambda_at_edge', 'alb_route', 'lambda_permission']);
  // a domain that code calls first, then the others; Lambda@Edge copies in front of every site last
  const called = new Set(inf.edges.filter((e) => e.via === 'domain_literal').map((e) => e.a.startsWith('f:') ? e.b : e.a));
  const trig = inf.edges.filter((e) => TRIG.has(e.via) && units[e.b] && unitNode.has(e.b) && !(e.via === 'lambda_permission' && !e.a.includes('elasticloadbalancing')))
    .sort((x, y) => (called.has(y.a) - called.has(x.a)) || ((x.via === 'lambda_at_edge') - (y.via === 'lambda_at_edge')) || (y.conf - x.conf));
  // a function in front of many sites (one Lambda@Edge per CloudFront) gets one "sites" box, not one per domain
  const perUnit = new Map();
  for (const e of trig) { const k = `${unitNode.get(e.b).id}|${e.via}`; if (!perUnit.has(k)) perUnit.set(k, new Set()); perUnit.get(k).add(e.a.startsWith('domain:') ? sourceLabel(e.a) : e.a); }
  const many = (e) => e.a.startsWith('domain:') && perUnit.get(`${unitNode.get(e.b).id}|${e.via}`).size > 4;
  const srcKey = (a, e) => { const [kind, ...rest] = a.split(':'); if (e && many(e)) return `sites:${unitNode.get(e.b).id}:${e.via}`; return kind === 'domain' ? `domain:${sourceLabel(a)}` : `${kind}:${rest.join(':')}`; };
  for (const e of trig) {
    const code = codeOf.get(e.b);
    const unit = unitNode.get(e.b);
    const kind = e.a.split(':')[0];
    const grouped = many(e);
    const src = mk(srcKey(e.a, e), grouped ? `${perUnit.get(`${unit.id}|${e.via}`).size} sites` : kind === 'url' ? `${unit.label} URL` : sourceLabel(e.a), RESOURCE[kind]?.[0] || kind, e.evidence.map((x) => ev(x.file, x.line, e.how === 'exact' ? 'code' : 'inferred', e.conf)));
    if (!src) continue;
    if (kind === 'domain' && !src.domains) src.domains = [];
    if (kind === 'domain' && src.domains.length < 8 && !e.a.slice(7).startsWith('cdn:') && !src.domains.includes(e.a.slice(7))) src.domains.push(e.a.slice(7));
    const codeFileOf = code ? entryFile(code.a.slice(2)) : null;
    const how = e.how === 'exact' ? 'code' : 'inferred';
    const l = addLink({ a: 'n:' + src.id, b: codeFileOf ? 'f:' + codeFileOf : 'n:' + unit.id, k: 'trigger', w: 1, labels: [(e.label || '').replace(/[[\]"]/g, '').trim() || e.via.replace(/_/g, ' ')], via: e.via, unit: 'n:' + unit.id, inferred: how === 'inferred', evidence: e.evidence.slice(0, 6).map((x) => ev(x.file, x.line, how, e.conf)) });
    if (l) links++;
  }
  // 4. code that calls a domain whose origin is a unit
  for (const e of inf.edges.filter((x) => x.via === 'domain_literal')) {
    const f = e.a.slice(2);
    const src = nodes.get(srcKey(e.b));
    if (!src || !codeFile(f)) continue;
    if (addLink({ a: 'f:' + f, b: 'n:' + src.id, k: 'http', w: 1, labels: [e.b.slice(7)], inferred: true, evidence: e.evidence.slice(0, 3).map((x) => ev(x.file, x.line, 'inferred', e.conf)) })) links++;
  }
  if (budget <= 0) out.notes.push(`The map draws ${CAPS.infraNodes} infrastructure boxes; more resources are in the infrastructure code.`);
  return { nodes: nodes.size, links };
}
