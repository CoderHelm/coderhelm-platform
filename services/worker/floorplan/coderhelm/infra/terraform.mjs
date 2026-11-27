// Terraform without the terraform binary: modules are instantiated from each root with the
// call's arguments bound, string templates are evaluated ("${var.env}-x" becomes "prod-x" and
// "staging-x"), references are followed through module outputs and locals, and connector
// resources become edges (compute units are nodes, connectors are edges, as InfraMap draws them).
//
// Units: lambda {id, file, line, names, src, placeholder, images, envKeys}, ecs {id, names,
// images, tgs}, cdn {id, domains, origins, edgeFns}, ecr {id, names}.
import path from 'node:path';
import { walk, read } from '../lib/text.mjs';
import * as hcl from './hcl.mjs';
import { edge } from './edge.mjs';

const P = path.posix;
const LAMBDA_ATTRS = ['source_path', 'image_uri', 'handler', 'function_name', 'runtime', 'package_type', 'lambda_at_edge'];
export const CONNECTORS = new Set(['aws_s3_bucket_notification', 'aws_lambda_permission', 'aws_lambda_event_source_mapping', 'aws_lambda_function_url',
  'aws_apigatewayv2_integration', 'aws_api_gateway_integration', 'aws_cloudwatch_event_target', 'aws_sns_topic_subscription', 'aws_scheduler_schedule',
  'aws_lb_listener_rule', 'aws_pipes_pipe', 'aws_sfn_state_machine']);
const ENV_TOK = /(^|[-_.])(prod|production|staging|stage|stg|dev|development|preview|test|qa|uat|sandbox)(?=$|[-_.])/g;

/** A name with environment words taken out: prod-image-resizer and staging-image-resizer are one. */
export const logical = (name) => (name || '*').replace(ENV_TOK, '$1').replace(/^[-_.]+|[-_.]+$/g, '') || '*';

class Dir {
  constructor(p) { this.path = p; this.res = new Map(); this.mods = new Map(); this.locals = new Map(); this.vars = new Map(); this.outs = new Map(); }
}

function load(root) {
  const dirs = new Map();
  const pool = new Map(); // variable or input name -> literal values seen anywhere (.hcl inputs, tfvars)
  for (const rel of walk(root, new Set(['.tf', '.hcl', '.tfvars']))) {
    const text = read(root, rel); if (text == null) continue;
    const tree = hcl.parse(text, rel);
    if (!rel.endsWith('.tf')) {
      for (const b of tree.walk()) for (const [k, [raw]] of b.attrs) { const v = hcl.literal(raw); if (v != null) { if (!pool.has(k)) pool.set(k, new Set()); pool.get(k).add(v); } }
      continue;
    }
    const dn = P.dirname(rel) === '.' ? '' : P.dirname(rel);
    if (!dirs.has(dn)) dirs.set(dn, new Dir(dn));
    const d = dirs.get(dn);
    for (const b of tree.blocks) {
      if (b.type === 'resource' && b.labels.length === 2) d.res.set(`${b.labels[0]}.${b.labels[1]}`, b);
      else if (b.type === 'data' && b.labels.length === 2) d.res.set(`data.${b.labels[0]}.${b.labels[1]}`, b);
      else if (b.type === 'module' && b.labels.length) d.mods.set(b.labels[0], b);
      else if (b.type === 'locals') for (const [k, [raw, ln]] of b.attrs) d.locals.set(k, [raw, ln, rel]);
      else if (b.type === 'variable' && b.labels.length) d.vars.set(b.labels[0], b.get('default'));
      else if (b.type === 'output' && b.labels.length) d.outs.set(b.labels[0], b.get('value'));
    }
  }
  return { dirs, pool };
}

function modDir(d, src, dirs) {
  const s = hcl.literal(src) || '';
  if (!s.startsWith('.')) return null;
  const p = P.normalize(P.join(d.path || '.', s)).replace(/^\.\/?$/, '');
  return dirs.has(p) ? p : null;
}

class Ctx { constructor(d, p, args, parent) { this.d = d; this.path = p; this.args = args; this.parent = parent; } }

export class TF {
  constructor(root) {
    this.root = root;
    const { dirs, pool } = load(root);
    this.dirs = dirs; this.pool = pool;
    const used = new Set();
    for (const d of dirs.values()) for (const m of d.mods.values()) { const md = modDir(d, m.get('source'), dirs); if (md != null) used.add(md); }
    this.roots = [...dirs.entries()].filter(([p, d]) => !used.has(p) && (d.res.size || d.mods.size)).map(([p]) => p);
    this.ctxs = []; this.weak = 0;
    for (const r of this.roots) this.inst(new Ctx(dirs.get(r), r, new Map(), null), 0);
  }

  inst(ctx, depth) {
    this.ctxs.push(ctx);
    if (depth > 6 || this.ctxs.length > 5000) return;
    for (const [name, m] of ctx.d.mods) {
      const md = modDir(ctx.d, m.get('source'), this.dirs);
      if (md != null) this.inst(new Ctx(this.dirs.get(md), `${ctx.path}/module.${name}`, m.attrs, ctx), depth + 1);
    }
  }

  /** A string with unknown parts as '*', or null. */
  ev(ctx, raw, depth = 0) {
    if (raw == null || depth > 8) return null;
    raw = raw.trim();
    const lit = hcl.literal(raw);
    if (lit != null) return lit;
    if (raw.startsWith('"') && raw.endsWith('"')) return raw.slice(1, -1).replace(/\$\{([^}]*)\}/g, (_, x) => this.ev(ctx, x, depth + 1) || '*');
    let m = /^var\.([A-Za-z0-9_-]+)$/.exec(raw);
    if (m) {
      const v = m[1];
      if (ctx.args.has(v) && ctx.parent) return this.ev(ctx.parent, ctx.args.get(v)[0], depth + 1);
      if (ctx.d.vars.get(v) != null) return this.ev(ctx, ctx.d.vars.get(v), depth + 1);
      const vals = this.pool.get(v);
      if (vals && vals.size === 1) return [...vals][0];
      return '*';
    }
    m = /^local\.([A-Za-z0-9_-]+)$/.exec(raw);
    if (m && ctx.d.locals.has(m[1])) return this.ev(ctx, ctx.d.locals.get(m[1])[0], depth + 1);
    if (/^path\.(module|root)$/.test(raw)) return ctx.d.path;
    return '*';
  }

  /** Resource instances an expression points at in the end: [[ctx, addr, attr]]. */
  targets(ctx, raw, depth = 0, seen = new Set()) {
    const out = [];
    if (raw == null || depth > 10) return out;
    for (const [addr, attr] of hcl.refs(raw)) {
      const key = `${ctx.path}|${addr}|${attr}`;
      if (seen.has(key)) continue;
      seen.add(key);
      if (addr.startsWith('local.')) {
        const l = ctx.d.locals.get(addr.slice(6)); if (l) out.push(...this.targets(ctx, l[0], depth + 1, seen));
      } else if (addr.startsWith('var.')) {
        const a = ctx.args.get(addr.slice(4));
        if (a && ctx.parent) out.push(...this.targets(ctx.parent, a[0], depth + 1, seen));
        else if (!ctx.parent) {
          // an unbound root variable is usually fed by an output of the same name in another
          // root (terragrunt dependency, remote state): follow it, and mark what it finds weak
          const v = addr.slice(4);
          const hits = this.ctxs.filter((c) => c.parent == null && c !== ctx && c.d.outs.has(v));
          if (hits.length > 0 && hits.length <= 2) { this.weak++; for (const c of hits) out.push(...this.targets(c, c.d.outs.get(v), depth + 1, seen)); }
        }
      } else if (addr.startsWith('module.')) {
        const parts = addr.split('.');
        const name = parts[1];
        const outname = parts.length > 2 ? parts[2] : (attr ? attr.split('.')[0] : '');
        const child = this.ctxs.find((c) => c.parent === ctx && c.path.endsWith('/module.' + name));
        if (child && child.d.outs.has(outname)) out.push(...this.targets(child, child.d.outs.get(outname), depth + 1, seen));
        else if (ctx.d.mods.has(name)) out.push([ctx, 'module.' + name, outname]);
      } else if (ctx.d.res.has(addr)) out.push([ctx, addr, attr]);
    }
    return out;
  }

  uid(ctx, addr) { return `${ctx.d.path}:${addr}`; }

  name(ctx, addr) {
    const b = addr.startsWith('module.') ? ctx.d.mods.get(addr.slice(7)) : ctx.d.res.get(addr);
    if (!b) return '*';
    return this.ev(ctx, b.get('function_name') || b.get('name')) || '*';
  }

  /** A module used for several apps (one ECS module, many services) gets one unit per logical name. */
  key(ctx, addr) {
    const base = this.uid(ctx, addr);
    return this.multi.has(base) ? `${base}#${logical(this.name(ctx, addr))}` : base;
  }

  findMulti() {
    const seen = new Map();
    for (const ctx of this.ctxs) for (const addr of ctx.d.res.keys()) {
      const t = addr.split('.')[0];
      if (t !== 'aws_lambda_function' && t !== 'aws_ecs_service') continue;
      const k = this.uid(ctx, addr);
      if (!seen.has(k)) seen.set(k, new Set());
      seen.get(k).add(logical(this.name(ctx, addr)));
    }
    this.multi = new Set([...seen].filter(([, v]) => [...v].filter((x) => x.replace(/[*-]/g, '')).length > 1).map(([k]) => k));
  }

  units() {
    this.findMulti();
    const lambdas = new Map(), cdns = new Map(), ecs = new Map(), ecr = new Map(), conn = [];
    const lambdaUnit = (id, b) => { if (!lambdas.has(id)) lambdas.set(id, { id, file: b.file, line: b.line, names: new Set(), src: [], placeholder: false, images: new Set(), envKeys: new Set() }); return lambdas.get(id); };
    for (const ctx of this.ctxs) {
      const d = ctx.d;
      for (const [addr, b] of d.res) {
        const t = addr.startsWith('data.') ? '' : addr.split('.')[0];
        if (t === 'aws_lambda_function') {
          const u = lambdaUnit(this.key(ctx, addr), b);
          u.names.add(this.ev(ctx, b.get('function_name')) || '*');
          u.handler = hcl.literal(b.get('handler'));
          this.lambdaCode(ctx, b, u);
        } else if (t === 'aws_cloudfront_distribution') {
          const k = `${this.uid(ctx, addr)}@${ctx.path}`;
          if (!cdns.has(k)) cdns.set(k, { id: this.uid(ctx, addr), inst: ctx.path, file: b.file, line: b.line, domains: new Set(), origins: [], edgeFns: [] });
          const u = cdns.get(k);
          const al = b.get('aliases');
          if (al) for (const m of al.matchAll(/("(?:[^"\\]|\\.)*")|(var\.[A-Za-z0-9_]+|local\.[A-Za-z0-9_]+)/g)) { const v = this.ev(ctx, m[1] || m[2]); if (v && v.replace(/[*.]/g, '') && v.includes('.')) u.domains.add(v); }
          for (const ob of [...b.walk()].filter((x) => x.type === 'origin' || x.type === 'content')) { const dn = ob.get('domain_name'); if (dn) u.origins.push([dn, ob.file, ob.line, ctx]); }
          for (const x of b.walk()) {
            if (x.type === 'lambda_function_association' || x.type === 'function_association' || (x.type === 'dynamic' && ['lambda_function_association', 'function_association'].includes(x.labels[0]))) {
              for (const y of [x, ...x.sub('content')]) { const arn = y.get('lambda_arn') || y.get('function_arn'); if (arn) u.edgeFns.push([arn, y.file, y.line, ctx]); }
            }
          }
        } else if (t === 'aws_ecs_service') {
          const id = this.key(ctx, addr);
          if (!ecs.has(id)) ecs.set(id, { id, file: b.file, line: b.line, names: new Set(), images: new Set(), tgs: new Set() });
          const u = ecs.get(id);
          for (const lbb of b.sub('load_balancer')) for (const [c2, a2] of this.targets(ctx, lbb.get('target_group_arn'))) u.tgs.add(`${c2.path}|${a2}`);
          u.names.add(this.ev(ctx, b.get('name')) || '*');
          for (const [c2, a2] of this.targets(ctx, b.get('task_definition'))) {
            const td = c2.d.res.get(a2); if (!td) continue;
            for (const [c3, a3] of this.targets(c2, td.get('container_definitions'))) if (/^(data\.)?aws_ecr_repository/.test(a3)) u.images.add(this.uid(c3, a3));
            for (const m of (td.get('container_definitions') || '').matchAll(/image\s*=\s*("[^"]*"|[^\n,}]+)/g)) { const v = this.ev(c2, m[1].trim()); if (v) u.images.add('img:' + v); }
          }
        } else if (t === 'aws_ecr_repository' || addr.startsWith('data.aws_ecr_repository')) {
          const id = this.uid(ctx, addr);
          if (!ecr.has(id)) ecr.set(id, { id, names: new Set() });
          ecr.get(id).names.add(this.ev(ctx, b.get('name')) || '*');
        }
        if (CONNECTORS.has(t)) conn.push([ctx, addr, b]);
      }
      // a remote Lambda module (terraform-aws-modules/lambda, a team wrapper): the call is the unit
      for (const [name, m] of d.mods) {
        if (modDir(d, m.get('source'), this.dirs) != null) continue;
        const src = (hcl.literal(m.get('source')) || '').toLowerCase();
        if (!src.includes('lambda') || !LAMBDA_ATTRS.some((a) => m.attrs.has(a))) continue;
        const u = lambdaUnit(this.uid(ctx, 'module.' + name), m);
        u.names.add(this.ev(ctx, m.get('function_name') || m.get('name')) || '*');
        this.lambdaCode(ctx, m, u);
      }
    }
    return { lambdas, cdns, ecs, ecr, conn };
  }

  lambdaCode(ctx, b, u) {
    let ign = '';
    for (const lc of b.sub('lifecycle')) ign = lc.get('ignore_changes') || '';
    if (/\b(filename|source_code_hash|s3_key|image_uri)\b/.test(ign)) u.placeholder = true;
    for (const key of ['filename', 'source_path', 's3_key', 'source_code_hash']) {
      const raw = b.get(key); if (!raw) continue;
      let hit = false;
      let tl = this.targets(ctx, raw);
      if (!tl.length && key === 'filename') {
        // a literal zip path: the archive_file in the same module whose output_path evaluates the same
        const fv = this.ev(ctx, raw);
        tl = [...ctx.d.res].filter(([a, x]) => a.includes('archive_file') && this.ev(ctx, x.get('output_path')) === fv).map(([a]) => [ctx, a, '']);
      }
      for (const [c2, a2] of tl) {
        const ab = c2.d.res.get(a2);
        if (!ab || !a2.includes('archive_file')) continue;
        const sd = ab.get('source_dir') || ab.get('source_file');
        const v = sd ? this.ev(c2, sd) : null;
        if (!sd) {
          // inline sources: source { content = templatefile("${path.module}/x.js", ...) }
          for (const sb of ab.sub('source')) {
            const fm = /(?:templatefile|file)\(\s*("[^"]+")/.exec(sb.get('content') || '');
            if (fm) { const f = this.ev(c2, fm[1]); if (f && !f.includes('*')) u.src.push([P.normalize(f), ab.file, ab.line]); }
          }
        }
        if (v && v.toLowerCase().includes('placeholder')) u.placeholder = true;
        else if (v) u.src.push([P.normalize(v.replace(/\*/g, '')), ab.file, ab.line]);
        hit = true;
      }
      if (!hit && key === 'source_path') {
        const v = this.ev(ctx, raw);
        if (v && !v.includes('*')) u.src.push([v.startsWith(ctx.d.path) ? v : P.normalize(P.join(ctx.d.path || '.', v)), b.file, b.line]);
      }
    }
    const img = b.get('image_uri');
    if (img) {
      for (const [c2, a2] of this.targets(ctx, img)) u.images.add(this.uid(c2, a2));
      const v = this.ev(ctx, img); if (v) u.images.add('img:' + v);
    }
    for (const e of [...b.walk()].filter((x) => x.type === 'environment')) {
      const vb = e.attrs.get('variables');
      if (vb) for (const m of vb[0].matchAll(/^\s*"?([A-Z][A-Z0-9_]+)"?\s*=/gm)) u.envKeys.add(m[1]);
    }
    const evs = b.attrs.get('environment_variables');
    if (evs) for (const m of evs[0].matchAll(/^\s*"?([A-Z][A-Z0-9_]+)"?\s*=/gm)) u.envKeys.add(m[1]);
  }
}

export function analyse(root) {
  const tf = new TF(root);
  const { lambdas, cdns, ecs, ecr, conn } = tf.units();
  const edges = [];
  const lamOf = (ctx, raw) => {
    const out = new Set();
    for (const [c2, a2] of tf.targets(ctx, raw)) {
      const k = tf.key(c2, a2);
      if (lambdas.has(k)) out.add(k);
      else if (a2.startsWith('aws_lambda_alias') || a2.startsWith('aws_lambda_function_url')) for (const x of lamOf(c2, c2.d.res.get(a2).get('function_name'))) out.add(x);
    }
    return [...out].sort();
  };
  const thing = (ctx, raw) => { const t = tf.targets(ctx, raw); return t.length ? t.map(([c, a]) => tf.uid(c, a)) : ['lit:' + (tf.ev(ctx, raw) || '?').slice(0, 80)]; };
  const fnUrls = new Map();
  for (const [ctx, addr, b] of conn) {
    const t = addr.split('.')[0];
    const at = [[b.file, b.line]];
    if (t === 'aws_s3_bucket_notification') {
      const bucket = thing(ctx, b.get('bucket'))[0];
      for (const sb of b.blocks) {
        const kind = { lambda_function: 'lambda', queue: 'queue', topic: 'topic' }[sb.type]; if (!kind) continue;
        const arn = sb.get('lambda_function_arn') || sb.get('queue_arn') || sb.get('topic_arn');
        for (const x of kind === 'lambda' ? lamOf(ctx, arn) : thing(ctx, arn)) edges.push(edge('s3:' + bucket, x, 'event', 's3_notification', 'exact', 0.95, [[sb.file, sb.line]], { label: `${sb.get('events') || ''} ${sb.get('filter_suffix') || ''}`.trim() }));
      }
    } else if (t === 'aws_lambda_event_source_mapping') {
      const src = thing(ctx, b.get('event_source_arn'))[0];
      for (const x of lamOf(ctx, b.get('function_name'))) edges.push(edge(src, x, 'queue', 'event_source_mapping', 'exact', 0.95, at));
    } else if (t === 'aws_lambda_function_url') {
      for (const x of lamOf(ctx, b.get('function_name'))) { fnUrls.set(tf.uid(ctx, addr), x); edges.push(edge('url:' + x, x, 'http', 'function_url', 'exact', 0.95, at, { label: 'auth ' + (hcl.literal(b.get('authorization_type')) || '?') })); }
    } else if (t === 'aws_apigatewayv2_integration' || t === 'aws_api_gateway_integration') {
      for (const x of lamOf(ctx, b.get('integration_uri') || b.get('uri'))) edges.push(edge('apigw:' + tf.uid(ctx, addr), x, 'http', 'api_integration', 'exact', 0.95, at));
    } else if (t === 'aws_cloudwatch_event_target') {
      const rule = thing(ctx, b.get('rule'))[0];
      for (const x of lamOf(ctx, b.get('arn'))) edges.push(edge('events:' + rule, x, 'event', 'eventbridge_target', 'exact', 0.95, at));
    } else if (t === 'aws_scheduler_schedule') {
      for (const tb of b.sub('target')) for (const x of lamOf(ctx, tb.get('arn'))) edges.push(edge('schedule:' + tf.uid(ctx, addr), x, 'event', 'scheduler', 'exact', 0.95, at));
    } else if (t === 'aws_sns_topic_subscription') {
      const proto = hcl.literal(b.get('protocol'));
      const topic = thing(ctx, b.get('topic_arn'))[0];
      if (proto === 'lambda') for (const x of lamOf(ctx, b.get('endpoint'))) edges.push(edge('sns:' + topic, x, 'event', 'sns_subscription', 'exact', 0.95, at));
      else if (proto === 'sqs') for (const x of thing(ctx, b.get('endpoint'))) edges.push(edge('sns:' + topic, x, 'queue', 'sns_subscription', 'exact', 0.9, at));
    } else if (t === 'aws_lambda_permission') {
      const pr = hcl.literal(b.get('principal')) || '';
      if (!pr || pr === '*') continue;
      const src = b.get('source_arn');
      for (const x of lamOf(ctx, b.get('function_name'))) edges.push(edge(`perm:${pr.split('.')[0]}:${src ? thing(ctx, src)[0] : pr}`, x, 'event', 'lambda_permission', 'exact', 0.6, at));
    }
  }
  // CloudFront: the origin's domain traced to a function URL, a Lambda, a bucket or a load balancer
  for (const [cid, c] of cdns) {
    for (const [raw, f, ln, ctx] of c.origins) {
      const w0 = tf.weak;
      const tl = tf.targets(ctx, raw);
      const how = tf.weak > w0 ? 'inferred' : 'exact';
      for (const [c2, a2] of tl) {
        const k = tf.key(c2, a2);
        let tgt = fnUrls.get(k) || null;
        if (!tgt && a2.startsWith('aws_lambda_function_url')) tgt = lamOf(c2, c2.d.res.get(a2).get('function_name'))[0] || null;
        tgt = tgt || (lambdas.has(k) ? k : null) || k;
        for (const dom of c.domains.size ? [...c.domains].sort() : ['cdn:' + cid]) edges.push(edge('domain:' + dom, tgt, 'http', 'cloudfront_origin', how, how === 'exact' ? 0.9 : 0.7, [[f, ln]], { cdn: c.id, inst: [c2.path, a2] }));
      }
    }
    for (const [raw, f, ln, ctx] of c.edgeFns) for (const x of lamOf(ctx, raw)) for (const dom of c.domains.size ? [...c.domains].sort() : ['cdn:' + cid]) edges.push(edge('domain:' + dom, x, 'http', 'lambda_at_edge', 'exact', 0.9, [[f, ln]]));
  }
  // load balancer listener rules to the ECS service behind their target group
  const tg2svc = new Map();
  for (const [sid, su] of ecs) for (const tg of su.tgs) { if (!tg2svc.has(tg)) tg2svc.set(tg, new Set()); tg2svc.get(tg).add(sid); }
  const lbRoutes = [];
  for (const ctx of tf.ctxs) {
    for (const [addr, b] of ctx.d.res) {
      const t = addr.split('.')[0];
      if (t !== 'aws_lb_listener' && t !== 'aws_lb_listener_rule') continue;
      let lbs = [];
      if (t === 'aws_lb_listener') lbs = tf.targets(ctx, b.get('load_balancer_arn')).map(([c, a]) => `${c.path}|${a}`);
      else for (const [c, a] of tf.targets(ctx, b.get('listener_arn'))) { const lb = c.d.res.get(a); if (lb) lbs.push(...tf.targets(c, lb.get('load_balancer_arn')).map(([c3, a3]) => `${c3.path}|${a3}`)); }
      const paths = [];
      for (const cond of [...b.walk()].filter((x) => x.type === 'path_pattern')) paths.push(...[...(cond.get('values') || '').matchAll(/"([^"]+)"/g)].map((m) => m[1]));
      const tgs = new Set();
      for (const x of b.walk()) if (x.get('target_group_arn')) for (const [c, a] of tf.targets(ctx, x.get('target_group_arn'))) tgs.add(`${c.path}|${a}`);
      for (const tg of tgs) for (const sid of tg2svc.get(tg) || []) for (const lb of lbs) lbRoutes.push([lb, paths, sid, b.file, b.line]);
    }
  }
  for (const e of [...edges]) {
    if (e.via !== 'cloudfront_origin' || !e.inst) continue;
    const lb = `${e.inst[0]}|${e.inst[1]}`;
    for (const [l2, paths, sid, f, ln] of lbRoutes) if (l2 === lb) edges.push(edge(e.a, sid, 'http', 'alb_route', 'exact', 0.85, [...e.evidence.map((x) => [x.file, x.line]), [f, ln]], { label: paths.join(' ') || 'default' }));
  }
  return { tf, lambdas, cdns, ecs, ecr, edges };
}
