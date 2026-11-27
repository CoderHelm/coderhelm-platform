// CoderHelm's driver for the vendored Floorplan (vendor/floorplan, MIT) in the
// run sandbox. It never calls a model: the worker (passes/app_map.rs) does
// the model jobs Floorplan's workflow.json describes and sends their answers
// in job.json; this script writes them, runs Floorplan's own scripts in
// unattended mode, and hands back their JSON results and the map.
//
// Layout of the folder it runs in (the sandbox extracts the worker's tarball
// and runs `node driver.mjs` there):
//   driver.mjs, job.json
//   fp/                     the vendored Floorplan release (skills/floorplan/...)
//   root/<repo>/...         each repository at its default branch's head
//   root/floorplan/...      the map's files from the last run (architecture,
//                           descriptions, the scan cache Floorplan's refresh
//                           compares against)
//
// What it writes to $CODERHELM_OUT (text files only, base64 where binary):
//   result.b64    every script's --json result, the worksheets and the files
//                 the model jobs need (gzipped JSON, base64)
//   state.b64     root/floorplan as a gzipped tar (base64), for the next run
//   graph.b64     floorplan/graph.json with CoderHelm's additions (coderhelm/index.mjs:
//                 screens, outside services, calls, infra, webhooks and data links),
//                 gzipped (base64), when the build wrote one
//   Each is split into parts (`result.b64.000`, `.001`, ...) when it is
//   larger than PART, so a large map stays under the sandbox's cap on one
//   returned file; the worker joins the parts in order.
//
// Journeys (coderhelm/flows.mjs) are re-checked against this build's code
// and kept in root/floorplan/coderhelm/flows.json; what the model must
// (re)write goes back in result.flows.todo.
//
// Vendored files are never edited; everything CoderHelm-specific is here and in coderhelm/.
// Floorplan's own graph.json stays as its build wrote it (refresh compares against it); the
// additions go only into the copy handed back.
import fs from 'node:fs';
import path from 'node:path';
import zlib from 'node:zlib';
import { spawnSync } from 'node:child_process';
import { prepare as prepareFlows, STORE as FLOWS_STORE } from './coderhelm/flows.mjs';

const HERE = process.cwd();
const SKILL = path.join(HERE, 'fp', 'skills', 'floorplan');
const ROOT = path.join(HERE, 'root');
const MAP = path.join(ROOT, 'floorplan');
const OUT = process.env.CODERHELM_OUT || path.join(HERE, 'out');
const job = JSON.parse(fs.readFileSync(path.join(HERE, 'job.json'), 'utf8'));
const env = { ...process.env, FLOORPLAN_UNATTENDED: '1', FLOORPLAN_AGENT: 'none', GIT_TERMINAL_PROMPT: '0' };

/** The only files an answer may become, inside the map folder. */
const ANSWER_PATH = /^floorplan\/(architecture\.json|recent-work\.json|descriptions\/[A-Za-z0-9_.-]{1,120}\.json)$/;
/** One returned file, at most (the sandbox keeps files up to 5 MiB). */
const PART = Number(process.env.CODERHELM_OUT_PART) || 4 * 1024 * 1024;

function git(cwd, args) {
  return spawnSync('git', ['-c', 'safe.directory=*', '-c', 'user.email=map@coderhelm.invalid', '-c', 'user.name=App map', ...args], { cwd, env, encoding: 'utf8' });
}

// Floorplan lists files with git; the worker's snapshots have no .git, so
// each repository gets one commit of exactly what was sent.
for (const name of fs.readdirSync(ROOT)) {
  const dir = path.join(ROOT, name);
  if (name === 'floorplan' || !fs.statSync(dir).isDirectory() || fs.existsSync(path.join(dir, '.git'))) continue;
  git(dir, ['init', '-q', '-b', job.branches?.[name] || 'main']);
  git(dir, ['add', '-A']);
  git(dir, ['commit', '-q', '--no-verify', '--allow-empty', '-m', job.heads?.[name] ? `snapshot ${job.heads[name]}` : 'snapshot']);
}

// The model jobs' answers, already checked against workflow.json's schemas
// by the worker.
for (const [rel, value] of Object.entries(job.answers || {})) {
  if (!ANSWER_PATH.test(rel)) throw new Error(`refusing to write ${rel}`);
  const file = path.join(ROOT, rel);
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, JSON.stringify(value, null, 2) + '\n');
}

function run(script, ...args) {
  const r = spawnSync(process.execPath, [path.join(SKILL, 'scripts', script), ROOT, ...args, '--json'], { env, encoding: 'utf8', maxBuffer: 256 * 1024 * 1024 });
  let json = null;
  try { json = JSON.parse(r.stdout); } catch { json = { ok: false, errors: [`${script} printed no result`] }; }
  if (r.stderr) process.stderr.write(r.stderr.slice(-4000));
  return { code: r.status, ...json };
}

const result = { schema: 'coderhelm/app-map-run@1', floorplan: fs.readFileSync(path.join(HERE, 'fp', 'VERSION'), 'utf8').trim() };
const arch = path.join(MAP, 'architecture.json');
const haveMap = fs.existsSync(path.join(MAP, 'graph.json'));
result.status = run('status.mjs');
if (job.mode === 'refresh' && haveMap && fs.existsSync(arch)) {
  // Floorplan's own change detection: carries descriptions over, flags what
  // changed and lists what is left to do (NEXT).
  result.refresh = run('refresh.mjs');
  result.build = result.refresh.build;
} else if (!fs.existsSync(arch) || job.mode === 'scan' || !fs.existsSync(path.join(MAP, '.cache', 'scan.json'))) {
  result.scan = run('scan.mjs');
}
if (fs.existsSync(arch) && !result.build) result.build = run('build.mjs');
if (result.build?.ok) {
  result.describe = run('describe.mjs');
  result.worksheets = (result.describe.worksheets || []).slice(0, 200).map((w) => ({
    key: w.key,
    node: w.node,
    worksheet: fs.readFileSync(path.join(ROOT, w.worksheet), 'utf8'),
    descriptionsPath: w.descriptions,
    descriptions: JSON.parse(fs.readFileSync(path.join(ROOT, w.descriptions), 'utf8')),
  }));
}
if (fs.existsSync(arch)) result.architecture = JSON.parse(fs.readFileSync(arch, 'utf8'));

// The map as the page gets it: Floorplan's build, then CoderHelm's
// detectors (hook H2), then the journeys on top of both. Floorplan's own
// graph.json in the map folder stays as Floorplan wrote it.
const graphFile = path.join(MAP, 'graph.json');
let graphOut = null;
if (result.build?.ok && fs.existsSync(graphFile)) {
  let g = JSON.parse(fs.readFileSync(graphFile, 'utf8'));
  // hook H2: CoderHelm's detectors on the built map; a failure keeps Floorplan's map as it is
  let t = Date.now();
  try {
    const { enrich } = await import('./coderhelm/index.mjs');
    const scanFile = path.join(MAP, '.cache', 'scan.json');
    const scan = fs.existsSync(scanFile) ? JSON.parse(fs.readFileSync(scanFile, 'utf8')) : {};
    g = await enrich({ root: ROOT, skill: SKILL, graph: g, scan });
    result.coderhelm = { ok: true, ms: Date.now() - t, counts: g.extract?.counts, notes: g.extract?.notes };
  } catch (e) {
    result.coderhelm = { ok: false, ms: Date.now() - t, error: String(e?.stack || e).slice(0, 2000) };
  }
  // Journeys: the stored ones re-checked against this build (0 tokens),
  // and the entries the model must (re)write, with the candidates it may
  // use.
  const storeFile = path.join(ROOT, FLOWS_STORE);
  t = Date.now();
  try {
    const prev = fs.existsSync(storeFile) ? JSON.parse(fs.readFileSync(storeFile, 'utf8')) : null;
    const p = prepareFlows(ROOT, g, prev);
    fs.mkdirSync(path.dirname(storeFile), { recursive: true });
    fs.writeFileSync(storeFile, JSON.stringify(p.store) + '\n');
    if (job.flows === 'visible') g.flows = [...(g.flows || []).filter((f) => !f.coderhelm), ...p.flows];
    result.flows = { report: { ...p.report, ms: Date.now() - t }, todo: p.todo, kept: p.store.flows, viewer: p.flows };
  } catch (e) {
    result.flows = { error: String(e?.stack || e).slice(0, 2000) };
  }
  graphOut = JSON.stringify(g);
  if (result.coderhelm) result.coderhelm.bytes = graphOut.length;
}

fs.mkdirSync(OUT, { recursive: true });
/** Writes one output, split into parts when it is larger than PART. */
function out(name, b64) {
  if (b64.length <= PART) return fs.writeFileSync(path.join(OUT, name), b64);
  for (let i = 0; i * PART < b64.length; i++) fs.writeFileSync(path.join(OUT, `${name}.${String(i).padStart(3, '0')}`), b64.slice(i * PART, (i + 1) * PART));
}
out('result.b64', zlib.gzipSync(JSON.stringify(result)).toString('base64'));
// The map's own files for the next run; the generated page and its helpers
// are left out (the dashboard has its own copy of the viewer).
const tgz = path.join(HERE, 'state.tgz');
const tar = spawnSync('tar', ['czf', tgz, '-C', ROOT, '--exclude=floorplan/index.html', '--exclude=floorplan/artifact.html', '--exclude=floorplan/serve.mjs', '--exclude=floorplan/query.mjs', '--exclude=floorplan/page-safety.mjs', 'floorplan'], { env });
if (tar.status === 0) out('state.b64', fs.readFileSync(tgz).toString('base64'));
if (graphOut) out('graph.b64', zlib.gzipSync(graphOut).toString('base64'));
