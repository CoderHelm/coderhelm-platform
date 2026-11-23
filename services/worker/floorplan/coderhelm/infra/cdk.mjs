// AWS CDK without synth. A construct is `new X(scope, 'Id', {props})`; a Lambda unit is any
// construct whose props carry entry/code/handler. Variables bind to units by assignment
// (`const fn = new ...`, `this.fn = new ...`); a reference from another file falls back to its
// last property name (`props.resizeFunction`), which is marked inferred.
import path from 'node:path';
import { walk, read, lineIndex, isTest } from '../lib/text.mjs';
import { edge } from './edge.mjs';

const P = path.posix;
const NEW = /(?:(?:const|let|var)\s+([A-Za-z_$][\w$]*)\s*(?::\s*[\w.<>[\]]+)?\s*=\s*|(?:this\.)([A-Za-z_$][\w$]*)\s*=\s*|([A-Za-z_$][\w$]*)\s*=\s*)?new\s+([\w.]+)\s*\(/g;

/** text[i] is just past '('; the argument text up to the matching ')'. */
function args(text, i) {
  let depth = 1, j = i;
  while (j < text.length && depth) {
    const c = text[j];
    if ('([{'.includes(c)) depth++;
    else if (')]}'.includes(c)) depth--;
    else if (c === '"' || c === "'" || c === '`') { const q = c; j++; while (j < text.length && text[j] !== q) j += text[j] === '\\' ? 2 : 1; }
    j++;
  }
  return text.slice(i, j - 1);
}

/** path.join(__dirname, '..', 'x'), require.resolve('./x') or '../x' as a repository path. */
function toPath(expr, fdir) {
  let m = /path\.(?:join|resolve)\s*\(\s*__dirname\s*,\s*([\s\S]+)\)/.exec(expr);
  if (m) { const parts = [...m[1].matchAll(/["']([^"']+)["']/g)].map((x) => x[1]); return parts.length ? P.normalize(P.join(fdir, ...parts)) : null; }
  m = /require\.resolve\(\s*["']([^"']+)["']/.exec(expr);
  if (m) return P.normalize(P.join(fdir, m[1]));
  m = /^\s*["']([^"']+)["']/.exec(expr);
  return m ? P.normalize(m[1]) : null; // relative to the CDK app folder; fixed up by the caller
}

function prop(a, name) {
  const m = new RegExp(`\\b${name}\\s*:\\s*`).exec(a);
  if (!m) return null;
  let depth = 0, k = m.index + m[0].length;
  for (; k < a.length; k++) {
    const c = a[k];
    if ('([{'.includes(c)) depth++;
    else if (')]}'.includes(c)) { if (depth === 0) break; depth--; }
    else if (c === ',' && depth === 0) break;
  }
  return a.slice(m.index + m[0].length, k).trim();
}

export function analyse(root) {
  const lambdas = new Map(), edges = [];
  const files = walk(root, new Set(['.ts', '.js', '.py'])).filter((r) => !r.endsWith('.d.ts'));
  const cdkFiles = [];
  for (const rel of files) {
    const t = read(root, rel); if (t == null) continue;
    if (/aws-cdk-lib|@aws-cdk\/|from aws_cdk|import aws_cdk/.test(t) && !isTest(rel)) cdkFiles.push([rel, t]);
  }
  const var2unit = new Map(), name2unit = new Map();
  const appDirs = walk(root, new Set(['cdk.json'])).map((r) => P.dirname(r));
  const appDir = (rel) => appDirs.filter((a) => (rel + '/').startsWith(a + '/')).sort((a, b) => b.length - a.length)[0] || '';
  for (const [rel, t] of cdkFiles) {
    const fdir = P.dirname(rel), L = lineIndex(t);
    for (const m of t.matchAll(NEW)) {
      const v = m[1] || m[2] || m[3];
      const cls = m[4];
      const a = args(t, m.index + m[0].length);
      const cid = /^\s*[\w.]+\s*,\s*["']([^"']+)["']/.exec(a)?.[1] || v || cls;
      const ln = L(m.index);
      let isFn = /Function$|Lambda/.test(cls.split('.').pop()) && !/cloudfront|CfnFunction|StepFunction|FunctionUrl|Api$|Integration$|Destination$|EventSource$/i.test(cls) && /\b(entry|code|handler)\s*:/.test(a);
      if (isFn && /cloudfront\.Function|FunctionCode/.test(a)) isFn = false;
      if (isFn) {
        const id = `${rel}:${cid}`;
        const u = { id, file: rel, line: ln, names: new Set([(prop(a, 'functionName') || '').replace(/^["']|["']$/g, '') || `*${cid}*`]), src: [], placeholder: false, images: new Set(), envKeys: new Set() };
        const entry = prop(a, 'entry'), code = prop(a, 'code');
        if (entry) { const p = toPath(entry, fdir); if (p) u.src.push([/^\s*(path|require)/.test(entry) ? p : P.normalize(P.join(appDir(rel) || '.', p)), rel, ln]); }
        if (code) {
          const cm = /Code\.from(?:Asset|DockerBuild|AssetImage)\s*\(([\s\S]*)\)/.exec(code) || /DockerImageCode\.fromImageAsset\s*\(([\s\S]*)\)/.exec(code);
          if (cm) { const p = toPath(cm[1], fdir); if (p) u.src.push([/path\.|require/.test(cm[1]) ? p : P.normalize(P.join(appDir(rel) || '.', p)), rel, ln]); }
          else if (code.includes('fromInline')) u.src.push([rel, rel, ln]);
        }
        const env = prop(a, 'environment');
        if (env) for (const x of env.matchAll(/\b([A-Z][A-Z0-9_]{2,})\s*:/g)) u.envKeys.add(x[1]);
        lambdas.set(id, u);
        if (v) { var2unit.set(`${rel}|${v}`, id); if (!name2unit.has(v)) name2unit.set(v, []); name2unit.get(v).push(id); }
      }
      if (v && !isFn && !var2unit.has(`${rel}|${v}`)) var2unit.set(`${rel}|${v}`, `${rel}:${cid}:${cls.split('.').pop()}`);
    }
  }
  const unitOf = (rel, expr) => {
    const m = /^(?:this\.|props\.|\w+\.)*([A-Za-z_$][\w$]*)$/.exec((expr || '').trim());
    if (!m) return [null, null];
    const id = var2unit.get(`${rel}|${m[1]}`);
    if (id && lambdas.has(id)) return [id, 'exact'];
    const c = name2unit.get(m[1]);
    return c && c.length === 1 ? [c[0], 'inferred'] : [null, null];
  };
  for (const [rel, t] of cdkFiles) {
    const L = lineIndex(t);
    const at = (i) => [[rel, L(i)]];
    for (const m of t.matchAll(/([\w.$]+)\.addEventSource\(\s*new\s+(\w+)EventSource\(\s*([\w.$]+)/g)) {
      const [u, how] = unitOf(rel, m[1]); if (!u) continue;
      edges.push(edge(`${m[2].toLowerCase()}:${m[3]}`, u, ['Sqs', 'Kinesis', 'Dynamo'].includes(m[2]) ? 'queue' : 'event', m[2] !== 'S3' ? 'event_source_mapping' : 's3_notification', how, how === 'exact' ? 0.9 : 0.6, at(m.index)));
    }
    for (const m of t.matchAll(/([\w.$]+)\.addEventNotification\([^,]+,\s*new\s+[\w.]*LambdaDestination\(\s*([\w.$]+)/g)) {
      const [u, how] = unitOf(rel, m[2]); if (u) edges.push(edge(`s3:${m[1]}`, u, 'event', 's3_notification', how, 0.9, at(m.index)));
    }
    for (const m of t.matchAll(/new\s+[\w.]*(LambdaRestApi|LambdaIntegration|HttpLambdaIntegration)\s*\(/g)) {
      const a = args(t, m.index + m[0].length);
      const parts = a.split(',');
      const target = m[1] === 'LambdaRestApi' ? prop(a, 'handler') : parts[m[1] === 'HttpLambdaIntegration' ? parts.length - 1 : 0];
      const [u, how] = unitOf(rel, target);
      if (u) edges.push(edge(`apigw:${rel}`, u, 'http', 'api_integration', how, how === 'exact' ? 0.9 : 0.6, at(m.index)));
    }
    for (const m of t.matchAll(/([\w.$]+)\.addFunctionUrl\(|new\s+[\w.]*FunctionUrl\(\s*[\w.]+\s*,\s*["'][^"']+["']\s*,\s*\{\s*function\s*:\s*([\w.$]+)/g)) {
      const [u, how] = unitOf(rel, m[1] || m[2]); if (u) edges.push(edge('url:' + u, u, 'http', 'function_url', how, 0.9, at(m.index)));
    }
    // AWS Solutions Constructs: new CloudFrontToApiGatewayToLambda(scope, id, {existingLambdaObj: fn})
    for (const m of t.matchAll(/new\s+(?:\w+\.)*(\w+To\w*Lambda)\s*\(/g)) {
      const fnx = prop(args(t, m.index + m[0].length), 'existingLambdaObj');
      const [u, how] = fnx ? unitOf(rel, fnx) : [null, null];
      if (!u) continue;
      for (const pk of m[1].split('To').slice(0, -1).map((x) => x.toLowerCase())) {
        if (pk === 'cloudfront') { edges.push(edge('domain:cdn:' + rel, u, 'http', 'cloudfront_origin', 'inferred', 0.7, at(m.index), { label: m[1] })); continue; }
        const via = { s3: 's3_notification', sqs: 'event_source_mapping', sns: 'sns_subscription', eventbridge: 'eventbridge_target', apigateway: 'api_integration', dynamodbstreams: 'event_source_mapping', kinesisstreams: 'event_source_mapping' }[pk] || 'solutions_construct';
        edges.push(edge(`${pk}:${rel}`, u, pk === 'apigateway' ? 'http' : 'event', via, how, how === 'exact' ? 0.85 : 0.6, at(m.index), { label: m[1] }));
      }
    }
    for (const m of t.matchAll(/new\s+[\w.]*LambdaFunction\(\s*([\w.$]+)/g)) { const [u, how] = unitOf(rel, m[1]); if (u) edges.push(edge(`events:${rel}`, u, 'event', 'eventbridge_target', how, 0.85, at(m.index))); }
    for (const m of t.matchAll(/new\s+[\w.]*LambdaSubscription\(\s*([\w.$]+)/g)) { const [u, how] = unitOf(rel, m[1]); if (u) edges.push(edge(`sns:${rel}`, u, 'event', 'sns_subscription', how, 0.85, at(m.index))); }
    // CloudFront Distribution with an origin built from an API or a function URL, or edge Lambdas
    for (const m of t.matchAll(/new\s+[\w.]*Distribution\s*\(/g)) {
      const a = args(t, m.index + m[0].length);
      const dn = /domainNames\s*:\s*\[([^\]]*)\]/.exec(a);
      const doms = dn ? [...dn[1].matchAll(/["']([^"']+)["']/g)].map((x) => x[1]) : [];
      const dom = doms.length ? doms : [`cdn:${rel}:${L(m.index)}`];
      for (const vm of t.matchAll(/(?:const|let)\s+(\w+)\s*(?::\s*\w+)?\s*=\s*(?:new\s+[\w.]*(HttpOrigin|RestApiOrigin|FunctionUrlOrigin)\(|[\w.]*FunctionUrlOrigin\.withOriginAccessControl\()\s*`?\$?\{?([\w.]+)/g)) {
        if (!new RegExp(`\\b${vm[1]}\\b`).test(a) && !a.includes('origin')) continue;
        const ref = vm[3].split('.')[0];
        const am = new RegExp(`(?:const|let|this\\.)\\s*${ref.replace(/\$/g, '\\$')}\\s*(?::\\s*\\w+)?\\s*=\\s*new\\s+[\\w.]*LambdaRestApi\\(`).exec(t);
        const [u] = am ? unitOf(rel, prop(args(t, am.index + am[0].length), 'handler')) : unitOf(rel, ref);
        if (u) for (const d of dom) edges.push(edge('domain:' + d, u, 'http', 'cloudfront_origin', 'inferred', 0.7, at(m.index)));
      }
      for (const em of a.matchAll(/functionVersion\s*:\s*([\w.$]+)/g)) {
        const [u, how] = unitOf(rel, em[1].replace('.currentVersion', ''));
        if (u) for (const d of dom) edges.push(edge('domain:' + d, u, 'http', 'lambda_at_edge', how, 0.85, at(m.index)));
      }
    }
  }
  return { lambdas, ecs: new Map(), edges };
}
