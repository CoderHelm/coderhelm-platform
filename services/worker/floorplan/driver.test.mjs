// CoderHelm's Floorplan driver on the made-up harbor app in fixtures/harbor
// (a web app and an API in two repositories), with the vendored release:
// - a first map, given the architecture and description answers the worker's
//   model jobs would send, builds clean;
// - the screen leads to the endpoint in the other repository and the outside
//   service behind it (Floorplan's own request matching and screens);
// - a refresh with nothing changed carries every description over;
// - fixtures/harbor-graph.json (what the Rust adapter's tests read) still has
//   the shape this release writes, so a bump that changes the format fails
//   here (regenerate with make-fixture.mjs, then fix the adapter).
//
//   node --test services/worker/floorplan/
import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { drive, harborAnswers, read, run, shape, workdir } from './harness.mjs';

test('a first map builds, and the screen reaches the endpoint and the service behind it', () => {
  const dir = workdir(harborAnswers(), 'start');
  const { result, graph } = drive(dir);
  assert.equal(result.build.ok, true, JSON.stringify(result.build.errors));
  assert.equal(result.build.descriptions.complete, true);
  assert.equal(graph.schema, 'floorplan/graph@1');
  const http = graph.links.find((l) => l.k === 'http');
  assert.deepEqual([http.a, http.b], ['f:harbor-web/lib/slips.ts', 'f:harbor-api/src/bookings.js']);
  const screen = graph.screens.find((s) => s.url === '/slips/:id');
  const behind = screen.behind.map((b) => b.id);
  assert.ok(behind.includes('f:harbor-api/src/bookings.js'), behind.join(', '));
  assert.ok(graph.links.some((l) => l.a === 'f:harbor-api/src/billing.js' && l.b === 'n:stripe'));
  assert.equal(graph.routes[0].name, 'Book a slip');
  assert.ok(fs.existsSync(path.join(dir, 'out', 'state.b64')));
  assert.deepEqual(shape(read('fixtures/harbor-graph.json')), shape(graph));
});

test('a first run with no architecture scans and asks for one', () => {
  const { result, graph } = drive(workdir({}, 'start'));
  assert.equal(result.scan.ok, true);
  assert.match(result.scan.summary, /harbor-api/);
  assert.equal(result.build, undefined);
  assert.equal(graph, null);
});

test('a refresh with nothing changed carries every description over', () => {
  const first = workdir(harborAnswers(), 'start');
  drive(first);
  // The next run starts from the state the first one handed back.
  const second = workdir({}, 'refresh');
  fs.writeFileSync(path.join(second, 'state.tgz'), Buffer.from(fs.readFileSync(path.join(first, 'out', 'state.b64'), 'utf8'), 'base64'));
  assert.equal(spawnSync('tar', ['xzf', 'state.tgz', '-C', 'root'], { cwd: second }).status, 0);
  const { result, graph } = drive(second);
  assert.equal(result.refresh.ok, true, JSON.stringify(result.refresh.errors));
  assert.equal(result.build.descriptions.complete, true);
  assert.equal(graph.routes[0].name, 'Book a slip');
});

test("answers can only become the map's own files", () => {
  const r = run(workdir({ '../driver.mjs': {} }, 'start'));
  assert.notEqual(r.status, 0);
  assert.match(r.stderr, /refusing to write/);
});
