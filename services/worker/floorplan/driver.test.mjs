// CoderHelm's Floorplan driver on the made-up harbor app in fixtures/harbor
// (a web app and an API in two repositories), with the vendored release:
// - a first map, given the architecture and description answers the worker's
//   model jobs would send, builds clean;
// - the screen leads to the endpoint in the other repository and the outside
//   service behind it (Floorplan's own request matching and screens);
// - a refresh with nothing changed carries every description over, and a
//   full scan of a map made by another extractor keeps them too;
// - CoderHelm's detectors (coderhelm/) add mobile screens, outside services,
//   calls, infra, webhooks and data links, each with its evidence;
// - journeys kept from the last map are re-checked and drawn, and a large
//   output comes back in parts the worker joins;
// - fixtures/harbor-graph.json (what the Rust adapter's tests read) still has
//   the shape this release writes, so a bump that changes the format fails
//   here (regenerate with make-fixture.mjs, then fix the adapter);
// - on the made-up library website in fixtures/library, each page its catch-all
//   route dispatches to by type is a screen with its URL and its renderer,
//   and a journey runs through them (fixtures/library-graph.json likewise).
//
//   node --test services/worker/floorplan/
import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { drive, harborAnswers, libraryAnswers, output, read, run, shape, workdir } from './harness.mjs';
import * as flows from './coderhelm/flows.mjs';

test('a first map builds, and the screen reaches the endpoint and the service behind it', () => {
  const dir = workdir(harborAnswers(), 'start');
  const { result, graph } = drive(dir);
  assert.equal(result.build.ok, true, JSON.stringify(result.build.errors));
  assert.equal(result.build.descriptions.complete, true);
  assert.equal(graph.schema, 'floorplan/graph@1');
  assert.ok(graph.links.some((l) => l.k === 'http' && l.a === 'f:harbor-web/lib/slots.ts' && l.b === 'f:harbor-api/src/bookings.js'));
  const screen = graph.screens.find((s) => s.url === '/slots/:id');
  const behind = screen.behind.map((b) => b.id);
  assert.ok(behind.includes('f:harbor-api/src/bookings.js'), behind.join(', '));
  assert.ok(graph.links.some((l) => l.a === 'f:harbor-api/src/billing.js' && l.b === 'n:stripe'));
  assert.equal(graph.routes[0].name, 'Book a slot');
  assert.ok(fs.existsSync(path.join(dir, 'out', 'state.b64')));
  assert.deepEqual(shape(read('fixtures/harbor-graph.json')), shape(graph));
});

test("CoderHelm's detectors add screens, services, calls, infra, webhooks and data links", () => {
  const { result, graph } = drive(workdir(harborAnswers(), 'start'));
  assert.equal(result.coderhelm.ok, true, result.coderhelm.error);
  const link = (k, a, b) => graph.links.find((l) => l.k === k && l.a === a && l.b === b);
  const added = (l) => l && l.evidence?.length && typeof l.inferred === 'boolean' && l.evidence.every((e) => e.file && e.line && ['code', 'inferred'].includes(e.how) && e.confidence > 0);
  // every node has a level
  assert.ok(graph.nodes.every((n) => ['system', 'part', 'code'].includes(n.level)));
  // screens: React Navigation in an app/ folder, with sign-in gates; Floorplan's fake Expo screens are gone
  const mobile = graph.screens.filter((s) => s.app === 'mobile');
  assert.deepEqual(mobile.map((s) => [s.url, s.nav.gate]).sort(), [['/(Stack)/SignIn', 'signed_out'], ['/(Stack)/SlotDetail', 'signed_in'], ['/(Stack)/Slots', 'signed_in']]);
  assert.ok(!graph.screens.some((s) => s.fw === 'expo'), 'fake expo screens');
  assert.ok(mobile.every((s) => s.nav.evidence[0].file === 'harbor-mobile/app/navigation/RootNavigator.tsx'));
  // Floorplan's own buildScreens filled what is behind the screen: the request, the server code, Stripe
  const detail = mobile.find((s) => s.url.endsWith('/SlotDetail'));
  for (const id of ['f:harbor-mobile/app/lib/api.ts', 'f:harbor-api/src/bookings.js', 'n:stripe']) assert.ok(detail.behind.some((b) => b.id === id), id);
  assert.ok(added(link('nav', 'f:harbor-mobile/app/screens/Slots.tsx', 'f:harbor-mobile/app/screens/SlotDetail.tsx')));
  // calls through a wrapper reach the endpoint in the other repository
  assert.ok(link('http', 'f:harbor-mobile/app/screens/SlotDetail.tsx', 'f:harbor-api/src/bookings.js'));
  // outside services: the architecture's Stripe gains its evidence; an env-only service is named; AWS SDK found
  const node = (id) => graph.nodes.find((n) => n.id === id);
  assert.equal(node('stripe').how, 'catalog');
  assert.equal(node('ext-tidewatch').how, 'env');
  assert.equal(node('ext-tidewatch').inferred, true);
  assert.equal(node('ext-s3').how, 'sdk');
  assert.ok(!graph.nodes.some((n) => /harbor/i.test(n.vendor || '')), 'the team\'s own endpoint is not an outside service');
  assert.ok(added(link('http', 'f:harbor-api/src/tides.js', 'n:ext-tidewatch')));
  // infra joined to code: the bucket triggers the function's code; the function deploys from its folder and workflow
  assert.equal(node('infra-harbor-receipts-function').parent, 'cloud');
  const trig = link('trigger', 'n:infra-bucket-uploads', 'f:harbor-api/src/receipts/index.js');
  assert.ok(added(trig));
  assert.equal(trig.inferred, false);
  const dep = link('deploy', 'n:infra-harbor-receipts-function', 'f:harbor-api/src/receipts/index.js');
  assert.ok(added(dep));
  assert.ok(dep.evidence.some((e) => e.file === 'harbor-api/.github/workflows/deploy-receipts.yml'));
  // a vendor's callback and a nightly load into a table an entity holds
  const hook = link('webhook', 'n:stripe', 'f:harbor-api/src/webhooks.js');
  assert.ok(added(hook));
  assert.deepEqual(hook.labels, ['POST /webhooks/stripe']);
  assert.ok(added(link('data', 'f:harbor-infra/dags/nightly_slots.py', 'f:harbor-api/src/entities/slot.entity.ts')));
  // nothing added comes from a test file
  assert.ok(graph.links.filter((l) => l.evidence).every((l) => l.evidence.every((e) => !/(^|\/)(tests?|__tests__)\/|\.(test|spec)\./.test(e.file))));
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
  assert.equal(graph.routes[0].name, 'Book a slot');
});

test("a map made by another extractor is scanned again in full, keeping the parts and descriptions", () => {
  const first = workdir(harborAnswers(), 'start');
  drive(first);
  // The worker's `scan` mode, from the state the last map handed back: a
  // whole new scan (not Floorplan's refresh), and nothing for the model.
  const again = workdir({}, 'scan');
  fs.writeFileSync(path.join(again, 'state.tgz'), Buffer.from(fs.readFileSync(path.join(first, 'out', 'state.b64'), 'utf8'), 'base64'));
  assert.equal(spawnSync('tar', ['xzf', 'state.tgz', '-C', 'root'], { cwd: again }).status, 0);
  const { result, graph } = drive(again);
  assert.equal(result.refresh, undefined);
  assert.equal(result.scan.ok, true, JSON.stringify(result.scan.errors));
  assert.equal(result.build.ok, true, JSON.stringify(result.build.errors));
  assert.equal(result.build.descriptions.complete, true);
  assert.equal(result.coderhelm.ok, true, result.coderhelm.error);
  assert.equal(graph.routes[0].name, 'Book a slot');
  assert.ok(graph.screens.some((s) => s.app === 'mobile'));
});

test("answers can only become the map's own files", () => {
  const r = run(workdir({ '../driver.mjs': {} }, 'start'));
  assert.notEqual(r.status, 0);
  assert.match(r.stderr, /refusing to write/);
});

test('journeys kept from the last map are re-checked and drawn on it', () => {
  const first = workdir(harborAnswers(), 'start');
  const { result, graph } = drive(first);
  // The mobile app's sign-in screen (from the detectors) starts a journey,
  // and so does the slot screen, whose own code books a slot: the model is
  // asked about both, with the candidates around each.
  assert.deepEqual(result.flows.todo.entries.map((e) => e.key), ['screen:mobile:/(Stack)/SignIn', 'screen:mobile:/(Stack)/SlotDetail'], JSON.stringify(result.flows.report));
  assert.deepEqual(result.flows.todo.groups.map((g) => [g.family, g.app]), [["sign-in", "mobile"], ["booking", "mobile"]]);
  // One navigation hop: the screen it lands on, not the booking further on.
  assert.match(result.flows.todo.groups[0].list, /screen mobile \/\(Stack\)\/Slots/);
  assert.doesNotMatch(result.flows.todo.groups[0].list, /bookings/);
  assert.ok(result.flows.report.candidates >= 3);
  // A journey as the worker's model step would have stored it.
  const c = flows.candidates(graph, flows.reader(path.join(first, 'root')));
  const step = (key, note) => { const x = c.list.find((y) => y.key === key); assert.ok(x, key); return { key, kind: x.kind, view: x.view, note, evidence: x.evidence, linked: true }; };
  const request = c.list.find((x) => x.kind === 'request');
  const store = { schema: flows.SCHEMA, asked: {}, flows: [{ id: 'book-a-slot', title: 'Book a slot', entry: 'screen:web:/slots/:id', origin: 'model', steps: [
    step('screen:web:/slots/:id', 'The patron opens a slot.'),
    step(request.key, 'The page asks the API to book it.'),
    step('n:stripe', 'Stripe charges the card.'),
  ] }] };
  const second = workdir({}, 'refresh', { flows: 'visible' });
  fs.writeFileSync(path.join(second, 'state.tgz'), output(first, 'state.b64'));
  assert.equal(spawnSync('tar', ['xzf', 'state.tgz', '-C', 'root'], { cwd: second }).status, 0);
  fs.mkdirSync(path.join(second, 'root', 'floorplan', 'coderhelm'), { recursive: true });
  fs.writeFileSync(path.join(second, 'root', flows.STORE), JSON.stringify(store));
  // Parts: every output larger than 2,000 characters comes back split.
  const again = drive(second, { CODERHELM_OUT_PART: '2000' });
  assert.ok(fs.readdirSync(path.join(second, 'out')).includes('graph.b64.000'));
  const drawn = again.graph.flows.find((f) => f.id === 'book-a-slot');
  assert.deepEqual(drawn.steps.map((s) => s.ref), ['f:harbor-web/app/slots/[id]/page.tsx', 'r:0', 'n:stripe']);
  // The request cites a call site and the handler that answers it.
  assert.equal(drawn.steps[1].evidence[1].file, 'harbor-api/src/bookings.js');
  assert.equal(again.result.flows.kept.length, 1);
  // Hidden (the default): kept and re-checked, not drawn.
  const third = workdir({}, 'refresh');
  fs.writeFileSync(path.join(third, 'state.tgz'), output(second, 'state.b64'));
  assert.equal(spawnSync('tar', ['xzf', 'state.tgz', '-C', 'root'], { cwd: third }).status, 0);
  const hidden = drive(third);
  assert.equal(hidden.result.flows.kept.length, 1);
  assert.ok(!(hidden.graph.flows || []).some((f) => f.coderhelm));
});

test('the pages a catch-all route dispatches to by type are screens, and a journey runs through them', () => {
  const first = workdir(libraryAnswers(), 'start', {}, 'library');
  const { result, graph } = drive(first);
  assert.equal(result.build.ok, true, JSON.stringify(result.build.errors));
  assert.equal(result.coderhelm.ok, true, result.coderhelm.error);
  assert.deepEqual(shape(read('fixtures/library-graph.json')), shape(graph));
  const dispatched = graph.screens.filter((s) => s.fw === 'dispatch');
  assert.deepEqual(dispatched.map((s) => [s.name, s.url, s.nav.renderer]).sort(), [
    ['Booth list', '/(booth-index)/*', 'BoothListRenderer'],
    ['Booth page', '/booths/:segment2', 'BoothRenderer'],
    ['Member checkout', '/members/checkout', 'MemberCheckoutRenderer'],
    ['Member signup', '/members/join', 'MemberSignupRenderer'],
    ['Notice page', '/(notice)/*', 'NoticeRenderer'],
  ]);
  // the catch-all route itself stays a screen
  assert.ok(graph.screens.some((s) => s.url === '/*' && s.fw === 'next-app'));
  const signup = dispatched.find((s) => s.url === '/members/join');
  assert.equal(signup.file, 'library-web/lib/renderers.tsx');
  assert.deepEqual(signup.nav.evidence.map((e) => [e.file, e.line]), [['library-web/lib/renderers.tsx', 19], ['library-web/app/[...slug]/page.tsx', 10], ['library-web/lib/resolve.ts', 13]]);
  // what is behind it is what its renderer uses, not every renderer in the file
  assert.deepEqual(signup.behind.map((b) => b.id), ['f:library-web/lib/renderers.tsx', 'f:library-web/components/SignupForm.tsx', 'r:1', 'f:library-web/app/api/members/route.ts']);
  // journeys start at the dispatched sign-up page, and the form there goes on to the checkout page
  const keys = result.flows.todo.entries.map((e) => e.key);
  assert.ok(keys.includes('screen:site:/members/join'), keys.join(', '));
  const c = flows.candidates(graph, flows.reader(path.join(first, 'root')));
  const at = (key) => c.list.find((x) => x.key === key) || assert.fail(key);
  const join = at('screen:site:/members/join');
  const checkout = at('screen:site:/members/checkout');
  assert.ok(c.edges.get(join.id)?.has(checkout.id) || [...(c.edges.get(join.id)?.keys() || [])].some((k) => c.list.find((x) => x.id === k)?.key === checkout.key), 'the sign-up form goes on to the checkout page');
  assert.deepEqual(join.span, [19, 23]);
  // a journey through both, as the worker would store it, is drawn with each step on its own screen
  const step = (key, note) => { const x = at(key); return { key, kind: x.kind, view: x.view, note, evidence: x.evidence, linked: true }; };
  const member = c.list.find((x) => x.kind === 'request' && x.key.includes('/api/members'));
  const store = { schema: flows.SCHEMA, asked: {}, flows: [{ id: 'join-the-library', title: 'Join the library', entry: 'screen:site:/members/join', origin: 'model', steps: [
    step('screen:site:/members/join', 'The patron fills in the sign-up form.'),
    step(member.key, 'The form saves the new member.'),
    step('screen:site:/members/checkout', 'The patron pays for the season.'),
  ] }] };
  const second = workdir({}, 'refresh', { flows: 'visible' }, 'library');
  fs.writeFileSync(path.join(second, 'state.tgz'), output(first, 'state.b64'));
  assert.equal(spawnSync('tar', ['xzf', 'state.tgz', '-C', 'root'], { cwd: second }).status, 0);
  fs.mkdirSync(path.join(second, 'root', 'floorplan', 'coderhelm'), { recursive: true });
  fs.writeFileSync(path.join(second, 'root', flows.STORE), JSON.stringify(store));
  const again = drive(second);
  assert.equal(again.result.flows.kept.length, 1, JSON.stringify(again.result.flows.report));
  const drawn = again.graph.flows.find((f) => f.id === 'join-the-library');
  assert.deepEqual(drawn.steps.map((s) => [s.ref, s.screen || null]), [
    ['f:library-web/lib/renderers.tsx', 'screen:site:/members/join'],
    ['r:1', null],
    ['f:library-web/lib/renderers.tsx', 'screen:site:/members/checkout'],
  ]);
});
