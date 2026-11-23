// Deploy pipelines (GitHub Actions workflows, plus buildspec, Makefile and shell scripts): what
// each one pushes where, and from which folder. Records {kind, name, folder, file, line, how}:
//   lambda  a function name   (aws lambda update-function-code, deploy actions)
//   ecs     a service name    (aws ecs update-service, amazon-ecs-deploy-task-definition)
//   ecr     a repository      (docker build -t, docker/build-push-action tags, ECR_REPOSITORY)
//   s3      a bucket          (aws s3 sync|cp <dir> s3://bucket)
//   stack   a tool            (sam/serverless/cdk deploy, terraform/terragrunt apply)
import path from 'node:path';
import { walk, read } from '../lib/text.mjs';
import { yaml } from '../lib/fp.mjs';

const P = path.posix;
const isObj = (v) => v && typeof v === 'object' && !Array.isArray(v);
const norm = (p) => { const n = P.normalize(p || '.'); return n === '.' ? '' : n.replace(/\/$/, ''); };

function subst(s, env) {
  if (!s) return s;
  for (let i = 0; i < 3; i++) {
    s = s.replace(/\$\{\{\s*(?:env|vars|inputs)\.([A-Za-z0-9_]+)\s*\}\}/g, (_, k) => env[k] ?? '*');
    s = s.replace(/\$\{\{\s*matrix\.([A-Za-z0-9_]+)\s*\}\}/g, (_, k) => env['matrix.' + k] ?? '*');
    s = s.replace(/\$\{\{[^}]*\}\}/g, '*');
    s = s.replace(/\$\{?([A-Za-z_][A-Za-z0-9_]*)\}?/g, (_, k) => env[k] ?? '*');
  }
  return s;
}

const lineOf = (text, needle) => { const i = needle ? text.indexOf(needle) : -1; if (i < 0) return 1; let n = 1; for (let k = 0; k < i; k++) if (text.charCodeAt(k) === 10) n++; return n; };

function commonPrefix(paths) {
  const ps = paths.filter((p) => typeof p === 'string' && p && !p.startsWith('!')).map((p) => p.split('*')[0].replace(/\/+$/, '')).filter((p) => p && !p.startsWith('.github'));
  if (!ps.length) return null;
  if (ps.length === 1) return ps[0];
  const parts = ps.map((p) => p.split('/'));
  const out = [];
  for (let i = 0; i < parts[0].length; i++) { if (parts.every((x) => x[i] === parts[0][i])) out.push(parts[0][i]); else break; }
  return out.join('/') || null;
}

/** `cwd` is where the script starts; `guess` (a workflow's paths: prefix) names the folder until a `cd` says otherwise. */
function shell(run, env, cwd, rel, text, out, howFolder, guess = null) {
  run = run.replace(/\\\n/g, ' ');
  let wd = guess ?? cwd;
  for (const line of run.split('\n')) {
    const cd = /^\s*cd\s+([^\s;&]+)/.exec(line);
    if (cd && !cd[1].startsWith('$') && cd[1] !== '..' && cd[1] !== '-') { cwd = norm(P.join(cwd || '', cd[1])); wd = cwd; }
    const s = subst(line, env);
    let m = /aws\s+lambda\s+(update-function-code|create-function)\b.*?--function-name[ =]+["']?([^\s"']+)/.exec(s);
    if (m) {
      const img = /--image-uri[ =]+["']?([^\s"']+)/.exec(s)?.[1] || null;
      out.push({ kind: 'lambda', name: m[2], folder: wd, file: rel, line: text.includes('--function-name ' + m[2]) ? lineOf(text, '--function-name ' + m[2]) : lineOf(text, '--function-name'), how: wd ? howFolder : 'none', image: img ? img.split(':')[0].split('/').pop() : null, imageRef: img });
    }
    m = /aws\s+ecs\s+update-service\b(.*)/.exec(s);
    if (m) { const svc = /--service[ =]+["']?([^\s"']+)/.exec(m[1]); if (svc) out.push({ kind: 'ecs', name: svc[1], folder: wd, file: rel, line: lineOf(text, 'update-service'), how: howFolder }); }
    for (const d of s.matchAll(/docker\s+(?:buildx\s+)?build\b([^\n]*)/g)) {
      const a = d[1];
      const tags = [...a.matchAll(/(?:-t|--tag)[ =]+["']?([^\s"']+)/g)].map((x) => x[1]);
      const f = /(?:-f|--file)[ =]+["']?([^\s"']+)/.exec(a);
      const ctx = a.replace(/(?:-t|--tag|-f|--file|--build-arg|--platform|--target|--cache-from|--label)[ =]+\S+/g, '').split(/\s+/).filter((x) => x && !x.startsWith('-'));
      let folder = ctx.length ? norm(P.join(wd || '', ctx[ctx.length - 1])) : wd;
      if (f && (!ctx.length || ctx[ctx.length - 1] === '.')) folder = norm(P.join(wd || '', P.dirname(f[1]) || '.'));
      for (const t of tags) { const repo = t.split(':')[0].split('/').pop(); if (repo && repo !== '*') out.push({ kind: 'ecr', name: repo, folder, file: rel, line: lineOf(text, 'docker build'), how: 'exact' }); }
    }
    m = /aws\s+s3\s+(?:sync|cp)\s+([^\s]+)\s+s3:\/\/([^/\s]+)/.exec(s);
    if (m) out.push({ kind: 's3', name: m[2], folder: norm(P.join(wd || '', m[1])), file: rel, line: lineOf(text, 's3://'), how: 'exact' });
    m = /\b(sam\s+deploy|serverless\s+deploy|sls\s+deploy|cdk\s+deploy|terraform\s+apply|terragrunt\s+(?:run-all\s+)?apply)\b/.exec(s);
    if (m) out.push({ kind: 'stack', name: m[1].split(/\s+/)[0], folder: wd, file: rel, line: lineOf(text, m[1].split(/\s+/)[0]), how: howFolder });
  }
}

export function analyse(root) {
  const out = [];
  for (const rel of walk(root, new Set(['.yml', '.yaml', '.sh', 'Makefile']))) {
    const text = read(root, rel); if (text == null) continue;
    const isWorkflow = rel.includes('.github/workflows/');
    if (/\.sh$|Makefile$/.test(rel) || P.basename(rel).startsWith('buildspec')) { shell(text, {}, rel.includes('.github') ? '' : norm(P.dirname(rel)), rel, text, out, 'inferred'); continue; }
    if (!isWorkflow) continue;
    const prefix = rel.split('.github/workflows/')[0].replace(/\/+$/, '');
    const doc = yaml(text);
    if (!isObj(doc)) continue;
    const on = isObj(doc.on) ? doc.on : isObj(doc.true) ? doc.true : {};
    const paths = [];
    for (const e of ['push', 'pull_request', 'workflow_dispatch']) if (isObj(on[e]) && Array.isArray(on[e].paths)) paths.push(...on[e].paths);
    const pfx = commonPrefix(paths);
    const strs = (o) => Object.fromEntries(Object.entries(isObj(o) ? o : {}).map(([k, v]) => [k, String(v)]));
    const wenv = strs(doc.env);
    for (const job of Object.values(isObj(doc.jobs) ? doc.jobs : {})) {
      if (!isObj(job)) continue;
      const jenv = { ...wenv, ...strs(job.env) };
      const jwd = job.defaults?.run?.['working-directory'];
      for (const st of Array.isArray(job.steps) ? job.steps : []) {
        if (!isObj(st)) continue;
        let senv = { ...jenv, ...strs(st.env) };
        for (const m of String(st.if || '').matchAll(/matrix\.(\w+)\s*==\s*'([^']+)'/g)) senv['matrix.' + m[1]] = m[2];
        senv = Object.fromEntries(Object.entries(senv).map(([k, v]) => [k, subst(v, senv)]));
        const explicit = st['working-directory'] || jwd;
        const how = explicit ? 'exact' : 'inferred';
        const at = (d) => norm(P.join(prefix || '.', d.startsWith('./') ? d.slice(2) : d));
        const cwd = explicit ? at(subst(String(explicit), senv)) : prefix || '';
        const guess = !explicit && pfx ? at(pfx) : null;
        const wd = guess ?? cwd;
        const uses = String(st.uses || '');
        const w = isObj(st.with) ? st.with : {};
        if (typeof st.run === 'string') shell(st.run, senv, cwd, rel, text, out, how, guess);
        if (uses.includes('amazon-ecs-deploy-task-definition') && w.service) out.push({ kind: 'ecs', name: subst(String(w.service), senv), folder: wd, file: rel, line: lineOf(text, 'amazon-ecs-deploy-task-definition'), how: 'inferred' });
        if (uses.includes('lambda') && (w.function_name || w['function-name'])) out.push({ kind: 'lambda', name: subst(String(w.function_name || w['function-name']), senv), folder: wd, file: rel, line: lineOf(text, uses), how });
        if (uses.includes('docker/build-push-action')) {
          for (const t of subst(String(w.tags || ''), senv).split(/[\s,]+/)) { const repo = t.split(':')[0].split('/').pop(); if (repo && repo !== '*') out.push({ kind: 'ecr', name: repo, folder: norm(P.join(prefix || '.', String(w.context || '.'))), file: rel, line: lineOf(text, 'build-push-action'), how: 'exact' }); }
        }
        for (const k of ['ECR_REPOSITORY', 'ECR_REPO', 'IMAGE_NAME', 'REPOSITORY']) {
          const v = senv[k];
          if (!v || v.includes('*') || v.includes('$')) continue;
          const name = v.split('/').pop();
          if (!out.some((o) => o.kind === 'ecr' && o.name === name && o.file === rel)) out.push({ kind: 'ecr', name, folder: wd || prefix || '', file: rel, line: lineOf(text, k), how: 'inferred' });
        }
      }
    }
  }
  const seen = new Set();
  return out.filter((o) => {
    const k = `${o.kind}|${o.name}|${o.folder}|${o.file}`;
    if (seen.has(k) || !o.name || o.name === '*' || o.name.includes('$')) return false;
    seen.add(k);
    return true;
  });
}
