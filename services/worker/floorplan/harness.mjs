// Runs CoderHelm's Floorplan driver (driver.mjs) the way the run sandbox
// does: a folder with the driver, job.json, the vendored Floorplan release in
// fp/ and the repositories in root/. Shared by driver.test.mjs and
// make-fixture.mjs.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import zlib from 'node:zlib';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

export const HERE = path.dirname(fileURLToPath(import.meta.url));
export const VENDOR = path.resolve(HERE, '../../../vendor/floorplan');

export const read = (f) => JSON.parse(fs.readFileSync(path.join(HERE, f), 'utf8'));

/** The answers the worker's model jobs would send for the harbor fixture. */
export const harborAnswers = () => ({ 'floorplan/architecture.json': read('architecture.harbor.json'), ...read('descriptions.harbor.json') });

export function workdir(answers, mode) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'app-map-'));
  fs.cpSync(path.join(HERE, 'fixtures', 'harbor'), path.join(dir, 'root'), { recursive: true });
  fs.cpSync(VENDOR, path.join(dir, 'fp'), { recursive: true });
  fs.copyFileSync(path.join(HERE, 'driver.mjs'), path.join(dir, 'driver.mjs'));
  fs.writeFileSync(path.join(dir, 'job.json'), JSON.stringify({ mode, answers, heads: { 'harbor-web': 'a1', 'harbor-api': 'b2' } }));
  return dir;
}

export function run(dir) {
  return spawnSync(process.execPath, ['driver.mjs'], { cwd: dir, encoding: 'utf8', env: { ...process.env, CODERHELM_OUT: path.join(dir, 'out') } });
}

export function drive(dir) {
  const r = run(dir);
  assert.equal(r.status, 0, r.stderr);
  const result = JSON.parse(zlib.gunzipSync(Buffer.from(fs.readFileSync(path.join(dir, 'out', 'result.b64'), 'utf8'), 'base64')));
  const g = path.join(dir, 'out', 'graph.b64');
  const graph = fs.existsSync(g) ? JSON.parse(zlib.gunzipSync(Buffer.from(fs.readFileSync(g, 'utf8'), 'base64'))) : null;
  return { result, graph };
}

/** A map as committed for the Rust adapter's tests: nothing that changes run to run. */
export function stable(graph) {
  return { ...graph, meta: { ...graph.meta, generatedAt: '2026-11-21T00:00:00.000Z', scanAt: '2026-11-21T00:00:00.000Z', root: '/sandbox/root' } };
}

/** The parts of a map the adapter relies on. */
export function shape(graph) {
  return {
    schema: graph.schema,
    nodes: graph.nodes.map((n) => [n.id, n.kind, n.label]),
    files: graph.files.map((f) => [f.p, f.n, f.r, f.name]),
    routes: graph.routes.map((r) => [r.f, r.m, r.p, r.name]),
    links: graph.links.map((l) => [l.a, l.b, l.k]).sort(),
    screens: (graph.screens || []).map((s) => [s.id, s.file, s.behind.map((b) => b.id)]),
    health: (graph.health?.issues || []).map((i) => [i.id, i.kind, i.severity, i.items]),
    tour: graph.tour.map((t) => t.ref),
  };
}
