// CloudFormation, SAM and the Serverless Framework: functions are units, event sources and
// connector resources are edges (same shapes as terraform.mjs). YAML or JSON.
import path from 'node:path';
import { walk, read } from '../lib/text.mjs';
import { yaml } from '../lib/fp.mjs';
import { edge } from './edge.mjs';

const P = path.posix;
const isObj = (v) => v && typeof v === 'object' && !Array.isArray(v);
const lit = (v) => (typeof v === 'string' ? v : null);

/** Logical ids a value references (Ref, GetAtt, ${X} in Sub). */
export function refs(v) {
  const out = [];
  if (isObj(v)) {
    for (const [k, x] of Object.entries(v)) {
      if (k === 'Ref' && typeof x === 'string') out.push(x);
      else if (k === 'Fn::GetAtt') out.push(Array.isArray(x) ? x[0] : String(x).split('.')[0]);
      else if (k === 'Fn::Sub') {
        const s = Array.isArray(x) ? x[0] : x;
        if (typeof s === 'string') for (const m of s.matchAll(/\$\{([A-Za-z0-9_.:]+)\}/g)) if (!m[1].startsWith('AWS::')) out.push(m[1].split('.')[0]);
        if (Array.isArray(x) && x.length > 1) out.push(...refs(x[1]));
      } else out.push(...refs(x));
    }
  } else if (Array.isArray(v)) for (const x of v) out.push(...refs(x));
  return out;
}

const lineOfText = (text, needle) => { const i = needle ? text.indexOf(needle) : -1; if (i < 0) return 1; let n = 1; for (let k = 0; k < i; k++) if (text.charCodeAt(k) === 10) n++; return n; };

export function analyse(root) {
  const lambdas = new Map(), ecs = new Map(), edges = [];
  for (const rel of walk(root, new Set(['.yml', '.yaml', '.json', '.template']))) {
    if (rel.includes('.github/') || /(package\.json|package-lock\.json|tsconfig\.json)$/.test(rel)) continue;
    const text = read(root, rel); if (text == null) continue;
    if (!text.includes('AWSTemplateFormatVersion') && !text.includes('Resources') && !text.includes('functions:')) continue;
    let doc = null;
    if (rel.endsWith('.json')) { try { doc = JSON.parse(text); } catch { doc = null; } } else doc = yaml(text);
    if (!isObj(doc)) continue;
    const d = P.dirname(rel);
    let res = doc.Resources;
    if (!isObj(res) && isObj(doc.resources)) res = doc.resources.Resources;
    res = isObj(res) ? res : {};
    const globals = isObj(doc.Globals) && isObj(doc.Globals.Function) ? doc.Globals.Function : {};
    const uid = (lid) => `${rel}:${lid}`;
    const unit = (id, line) => { if (!lambdas.has(id)) lambdas.set(id, { id, file: rel, line, names: new Set(), src: [], placeholder: false, images: new Set(), envKeys: new Set() }); return lambdas.get(id); };

    // Serverless Framework functions
    if (isObj(doc.functions) && ('provider' in doc || 'service' in doc)) {
      const svc = typeof doc.service === 'string' ? doc.service : '';
      for (const [fname, f] of Object.entries(doc.functions)) {
        if (!isObj(f)) continue;
        const h = typeof f.handler === 'string' ? f.handler : '';
        const mod = h ? h.replace(/\.[^.]+$/, '') : '';
        const lid = uid(fname);
        const u = unit(lid, lineOfText(text, `${fname}:`));
        u.names.add(lit(f.name) || `${svc}-*-${fname}`);
        if (h) { u.src.push([P.normalize(P.join(d, P.dirname(mod) || '.')), rel, lineOfText(text, h)]); u.handlerFile = P.normalize(P.join(d, mod)); }
        for (const k of Object.keys(isObj(f.environment) ? f.environment : {})) u.envKeys.add(k);
        for (const k of Object.keys(isObj(doc.provider?.environment) ? doc.provider.environment : {})) u.envKeys.add(k);
        for (const evt of Array.isArray(f.events) ? f.events : []) {
          if (!isObj(evt)) continue;
          for (const [et, p] of Object.entries(evt)) {
            const at = [[rel, lineOfText(text, `- ${et}`)]];
            if (et === 'http' || et === 'httpApi') {
              const pth = typeof p === 'string' ? p : (p?.path || '');
              const meth = isObj(p) ? (p.method || '*') : '';
              edges.push(edge(`apigw:${rel}:${et}`, lid, 'http', 'api_integration', 'exact', 0.95, at, { label: `${meth} ${pth}`.trim() }));
            } else if (et === 's3') edges.push(edge(`s3:${typeof p === 'string' ? p : p?.bucket}`, lid, 'event', 's3_notification', 'exact', 0.95, at));
            else if (['sqs', 'stream', 'kafka', 'msk'].includes(et)) { const a = typeof p === 'string' ? p : p?.arn; const r = refs(a); edges.push(edge(`${et}:${r[0] || a}`, lid, 'queue', 'event_source_mapping', 'exact', 0.95, at)); }
            else if (et === 'sns') edges.push(edge(`sns:${typeof p === 'string' ? p : p?.topicName || p?.arn}`, lid, 'event', 'sns_subscription', 'exact', 0.95, at));
            else if (['schedule', 'eventBridge', 'cloudwatchEvent'].includes(et)) edges.push(edge(`events:${rel}:${fname}`, lid, 'event', 'eventbridge_target', 'exact', 0.95, at));
            else if (et === 'cloudFront') edges.push(edge(`cdn:${rel}`, lid, 'http', 'lambda_at_edge', 'exact', 0.9, at));
          }
        }
      }
    }

    for (const [lid, r] of Object.entries(res)) {
      if (!isObj(r)) continue;
      const t = r.Type || '';
      const p = isObj(r.Properties) ? r.Properties : {};
      const ln = rel.endsWith('.json') ? lineOfText(text, `"${lid}"`) : lineOfText(text, `${lid}:`);
      const at = [[rel, ln]];
      if (t === 'AWS::Serverless::Function' || t === 'AWS::Lambda::Function') {
        const u = unit(uid(lid), ln);
        u.names.add(lit(p.FunctionName) || `*${lid}*`);
        u.handler = lit(p.Handler) || lit(globals.Handler);
        const code = t.endsWith('Serverless::Function') ? (p.CodeUri ?? globals.CodeUri) : p.Code;
        if (typeof code === 'string') u.src.push([P.normalize(P.join(d, code)), rel, ln]);
        else if (isObj(code)) {
          if ('ZipFile' in code) u.src.push([rel, rel, ln]);
          if ('ImageUri' in code) u.images.add('img:' + JSON.stringify(code.ImageUri));
          if ('S3Key' in code || 'Bucket' in code) u.placeholder = true;
        }
        if (p.ImageUri || p.PackageType === 'Image') { const dc = r.Metadata?.DockerContext; if (typeof dc === 'string') u.src.push([P.normalize(P.join(d, dc)), rel, ln]); }
        for (const k of Object.keys(isObj(p.Environment?.Variables) ? p.Environment.Variables : {})) u.envKeys.add(k);
        for (const [en, e] of Object.entries(isObj(p.Events) ? p.Events : {})) {
          const et = e?.Type; const ep = isObj(e?.Properties) ? e.Properties : {};
          const eat = [[rel, lineOfText(text, `${en}:`)]];
          if (et === 'Api' || et === 'HttpApi') edges.push(edge(`apigw:${rel}:${refs(ep.RestApiId || ep.ApiId).join(',') || 'implicit'}`, uid(lid), 'http', 'api_integration', 'exact', 0.95, eat, { label: `${ep.Method || ''} ${ep.Path || ''}`.trim() }));
          else if (et === 'S3') edges.push(edge(`s3:${uid(refs(ep.Bucket)[0] || '?')}`, uid(lid), 'event', 's3_notification', 'exact', 0.95, eat));
          else if (['SQS', 'Kinesis', 'DynamoDB', 'MSK'].includes(et)) edges.push(edge(`${et.toLowerCase()}:${uid(refs(ep.Queue || ep.Stream)[0] || '?')}`, uid(lid), 'queue', 'event_source_mapping', 'exact', 0.95, eat));
          else if (et === 'SNS') edges.push(edge(`sns:${uid(refs(ep.Topic)[0] || '?')}`, uid(lid), 'event', 'sns_subscription', 'exact', 0.95, eat));
          else if (['Schedule', 'ScheduleV2', 'EventBridgeRule', 'CloudWatchEvent'].includes(et)) edges.push(edge(`events:${uid(lid)}:${en}`, uid(lid), 'event', 'eventbridge_target', 'exact', 0.95, eat));
        }
        if (p.FunctionUrlConfig) edges.push(edge('url:' + uid(lid), uid(lid), 'http', 'function_url', 'exact', 0.95, at));
      } else if (t === 'AWS::Lambda::EventSourceMapping') {
        for (const f of refs(p.FunctionName)) edges.push(edge('queue:' + uid(refs(p.EventSourceArn)[0] || '?'), uid(f), 'queue', 'event_source_mapping', 'exact', 0.95, at));
      } else if (t === 'AWS::Lambda::Url') {
        for (const f of refs(p.TargetFunctionArn)) edges.push(edge('url:' + uid(f), uid(f), 'http', 'function_url', 'exact', 0.95, at));
      } else if (t === 'AWS::Lambda::Permission') {
        for (const f of refs(p.FunctionName)) { const s = refs(p.SourceArn); edges.push(edge(`perm:${String(p.Principal).split('.')[0]}:${s.length ? uid(s[0]) : '?'}`, uid(f), 'event', 'lambda_permission', 'exact', 0.6, at)); }
      } else if (t === 'AWS::S3::Bucket') {
        const nc = isObj(p.NotificationConfiguration) ? p.NotificationConfiguration : {};
        for (const c of nc.LambdaConfigurations || []) for (const f of refs(c?.Function)) edges.push(edge('s3:' + uid(lid), uid(f), 'event', 's3_notification', 'exact', 0.95, at));
        for (const c of nc.QueueConfigurations || []) for (const f of refs(c?.Queue)) edges.push(edge('s3:' + uid(lid), uid(f), 'queue', 's3_notification', 'exact', 0.95, at));
        for (const c of nc.TopicConfigurations || []) for (const f of refs(c?.Topic)) edges.push(edge('s3:' + uid(lid), uid(f), 'event', 's3_notification', 'exact', 0.95, at));
      } else if (t === 'AWS::ApiGateway::Method' || t === 'AWS::ApiGatewayV2::Integration') {
        const uri = (isObj(p.Integration) ? p.Integration.Uri : null) || p.IntegrationUri;
        for (const f of refs(uri)) if (isObj(res[f]) && String(res[f].Type || '').includes('Function')) edges.push(edge('apigw:' + uid(lid), uid(f), 'http', 'api_integration', 'exact', 0.95, at));
      } else if (t === 'AWS::SNS::Subscription') {
        for (const f of refs(p.Endpoint).filter((x) => x in res)) edges.push(edge('sns:' + uid(refs(p.TopicArn)[0] || '?'), uid(f), p.Protocol === 'lambda' ? 'event' : 'queue', 'sns_subscription', 'exact', 0.95, at));
      } else if (t === 'AWS::SNS::Topic') {
        for (const s of Array.isArray(p.Subscription) ? p.Subscription : []) for (const f of refs(s?.Endpoint).filter((x) => x in res)) edges.push(edge('sns:' + uid(lid), uid(f), 'event', 'sns_subscription', 'exact', 0.95, at));
      } else if (t === 'AWS::Events::Rule' || t === 'AWS::Scheduler::Schedule') {
        const tg = Array.isArray(p.Targets) ? p.Targets : p.Target ? [p.Target] : [];
        for (const x of tg) for (const f of refs(x?.Arn)) edges.push(edge('events:' + uid(lid), uid(f), 'event', 'eventbridge_target', 'exact', 0.95, at));
      } else if (t === 'AWS::CloudFront::Distribution') {
        const dc = isObj(p.DistributionConfig) ? p.DistributionConfig : {};
        const doms = (Array.isArray(dc.Aliases) ? dc.Aliases : []).filter((a) => typeof a === 'string');
        const ds = doms.length ? doms : ['cdn:' + uid(lid)];
        for (const o of Array.isArray(dc.Origins) ? dc.Origins : []) for (const f of refs(o?.DomainName)) for (const dm of ds) edges.push(edge('domain:' + dm, uid(f), 'http', 'cloudfront_origin', 'exact', 0.9, at));
        for (const beh of [dc.DefaultCacheBehavior || {}, ...(Array.isArray(dc.CacheBehaviors) ? dc.CacheBehaviors : [])]) {
          for (const a of Array.isArray(beh?.LambdaFunctionAssociations) ? beh.LambdaFunctionAssociations : []) for (const f of refs(a?.LambdaFunctionARN)) for (const dm of ds) edges.push(edge('domain:' + dm, uid(f), 'http', 'lambda_at_edge', 'exact', 0.9, at));
        }
      } else if (t === 'AWS::ECS::Service') {
        const imgs = new Set();
        for (const td of refs(p.TaskDefinition)) for (const c of res[td]?.Properties?.ContainerDefinitions || []) {
          let im = c?.Image; if (isObj(im)) im = im['Fn::Sub'];
          if (typeof im === 'string') imgs.add('img:' + im);
        }
        ecs.set(uid(lid), { id: uid(lid), file: rel, line: ln, names: new Set([lit(p.ServiceName) || `*${lid}*`]), images: imgs });
      }
    }
  }
  return { lambdas, ecs, edges };
}
