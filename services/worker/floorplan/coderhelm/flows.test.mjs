// Journeys on a made-up harbor app (synthetic: a Next site and a Nest API),
// shaped the way Floorplan and CoderHelm's detectors leave the map:
// - the five things the map alone misses become candidates: a step inside
//   the sign-in drawer, the session hook whose work is in its provider, the
//   Next route that calls the API, the service the controller gets by
//   injection, and the event its listener handles (plus a webhook);
// - every candidate cites a line that holds its token;
// - stored journeys are re-lined when code moves, lose steps whose code is
//   gone, and are dropped by the rules; a rebuild that changed nothing a
//   journey stands on asks the model for nothing, and so does a release
//   that changed only the extractor (renamed and split screens re-pointed);
// - new screens are asked for first, then changed code, then the rest.
//
//   node --test services/worker/floorplan/coderhelm/flows.test.mjs
import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import * as F from './flows.mjs';

const FILES = {
  'harbor-web/app/login/page.tsx': `import { LoginDrawer } from '../../components/LoginDrawer';

export default function LoginPage() {
  return <LoginDrawer />;
}
`,
  'harbor-web/components/LoginDrawer.tsx': `import { useState } from 'react';
import { useSession } from '../lib/session';
import { LoginForm } from './LoginForm';
import { ResetForm } from './ResetForm';

export function LoginDrawer() {
  const { login } = useSession();
  const [progress, setProgress] = useState('login');
  return (
    <div>
      {progress === 'login' && <LoginForm onSubmit={login} onForgot={() => setProgress('reset')} />}
      {progress === 'reset' && <ResetForm />}
    </div>
  );
}
`,
  'harbor-web/components/LoginForm.tsx': `import { useRouter } from 'next/navigation';

export function LoginForm({ onSubmit }) {
  const router = useRouter();
  const go = async () => { await onSubmit(); router.push('/slips'); };
  return <form onSubmit={go} />;
}
`,
  'harbor-web/components/ResetForm.tsx': `export function ResetForm() {
  return <form />;
}
`,
  'harbor-web/lib/session.tsx': `import { createContext, useContext } from 'react';

const SessionContext = createContext(null);

export function useSession() {
  return useContext(SessionContext);
}

export function SessionProvider({ children }) {
  const login = async () => {
    await fetch('/api/session', { method: 'POST' });
  };
  return <SessionContext.Provider value={{ login }}>{children}</SessionContext.Provider>;
}
`,
  'harbor-web/app/api/session/route.ts': `const API = process.env.HARBOR_API_URL;

export async function POST(req) {
  const r = await fetch(\`\${API}/auth/login\`, { method: 'POST' });
  return Response.json(await r.json());
}
`,
  'harbor-web/app/slips/page.tsx': `export default function SlipsPage() {
  return <main />;
}
`,
  'harbor-api/src/auth.controller.ts': `import { Controller, Post } from '@nestjs/common';
import { AuthService } from './auth.service';

@Controller('auth')
export class AuthController {
  constructor(private readonly auth: AuthService) {}

  @Post('login')
  login() {
    return this.auth.login();
  }
}
`,
  'harbor-api/src/auth.service.ts': `import { OktaAuth } from '@okta/okta-auth-js';

export const SIGNED_IN = 'user.signed_in';

export class AuthService {
  constructor(private readonly events: EventEmitter2) {}

  login() {
    this.events.emit(SIGNED_IN, {});
    return new OktaAuth({}).signIn();
  }
}
`,
  'harbor-api/src/welcome.listener.ts': `import { OnEvent } from '@nestjs/event-emitter';
import { SIGNED_IN } from './auth.service';

export class WelcomeListener {
  @OnEvent(SIGNED_IN)
  send() {}
}
`,
  'harbor-api/src/payments.controller.ts': `import { Controller, Post } from '@nestjs/common';
import Stripe from 'stripe';

@Controller('stripe')
export class PaymentsController {
  @Post('webhook')
  paid() {}
}
`,
};

const sym = (name, kind, line) => [name, kind, line, kind === 'default' ? 1 : 0, ''];
const GRAPH = {
  schema: 'floorplan/graph@1',
  nodes: [
    { id: 'web', kind: 'ui', label: 'Harbor website' },
    { id: 'api', kind: 'service', label: 'Booking service' },
    { id: 'okta', kind: 'external', label: 'Okta', packages: ['@okta/okta-auth-js'] },
    { id: 'stripe', kind: 'external', label: 'Stripe', packages: ['stripe'] },
  ],
  files: [
    { p: 'harbor-web/app/login/page.tsx', s: [sym('LoginPage', 'default', 3)] },
    { p: 'harbor-web/components/LoginDrawer.tsx', s: [sym('LoginDrawer', 'function', 6)] },
    { p: 'harbor-web/components/LoginForm.tsx', s: [sym('LoginForm', 'function', 3)] },
    { p: 'harbor-web/components/ResetForm.tsx', s: [sym('ResetForm', 'function', 1)] },
    { p: 'harbor-web/lib/session.tsx', s: [sym('useSession', 'function', 5), sym('SessionProvider', 'function', 9)] },
    { p: 'harbor-web/app/api/session/route.ts', s: [sym('POST', 'function', 3)] },
    { p: 'harbor-web/app/slips/page.tsx', s: [sym('SlipsPage', 'default', 1)] },
    { p: 'harbor-api/src/auth.controller.ts', s: [sym('AuthController', 'class', 5)] },
    { p: 'harbor-api/src/auth.service.ts', s: [sym('SIGNED_IN', 'const', 3), sym('AuthService', 'class', 5)] },
    { p: 'harbor-api/src/welcome.listener.ts', s: [sym('WelcomeListener', 'class', 4)] },
    { p: 'harbor-api/src/payments.controller.ts', s: [sym('PaymentsController', 'class', 5)] },
  ],
  routes: [
    { f: 'harbor-web/app/api/session/route.ts', m: 'POST', p: '/api/session', l: 1, h: 'POST' },
    { f: 'harbor-api/src/auth.controller.ts', m: 'POST', p: '/auth/login', l: 8, h: 'login' },
    { f: 'harbor-api/src/payments.controller.ts', m: 'POST', p: '/stripe/webhook', l: 6, h: 'paid' },
  ],
  links: [
    { a: 'f:harbor-web/app/login/page.tsx', b: 'f:harbor-web/components/LoginDrawer.tsx', k: 'import', names: ['LoginDrawer'] },
    { a: 'f:harbor-web/components/LoginDrawer.tsx', b: 'f:harbor-web/lib/session.tsx', k: 'import', names: ['useSession'] },
    { a: 'f:harbor-web/components/LoginDrawer.tsx', b: 'f:harbor-web/components/LoginForm.tsx', k: 'import', names: ['LoginForm'] },
    { a: 'f:harbor-web/components/LoginDrawer.tsx', b: 'f:harbor-web/components/ResetForm.tsx', k: 'import', names: ['ResetForm'] },
    { a: 'f:harbor-api/src/auth.controller.ts', b: 'f:harbor-api/src/auth.service.ts', k: 'import', names: ['AuthService'] },
    { a: 'f:harbor-api/src/welcome.listener.ts', b: 'f:harbor-api/src/auth.service.ts', k: 'import', names: ['SIGNED_IN'] },
    { a: 'f:harbor-web/lib/session.tsx', b: 'f:harbor-web/app/api/session/route.ts', k: 'http', labels: ['POST /api/session'], calls: [[0, 11]] },
    { a: 'f:harbor-api/src/auth.service.ts', b: 'n:okta', k: 'import', names: ['OktaAuth'] },
    { a: 'f:harbor-api/src/payments.controller.ts', b: 'n:stripe', k: 'import', names: ['default'] },
  ],
  screens: [
    { id: 'screen:web:/login', url: '/login', file: 'harbor-web/app/login/page.tsx', app: 'web', name: 'Sign in' },
    { id: 'screen:web:/slips', url: '/slips', file: 'harbor-web/app/slips/page.tsx', app: 'web', name: 'Slips' },
  ],
};

function root(files = FILES) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'journeys-'));
  for (const [p, t] of Object.entries(files)) {
    fs.mkdirSync(path.dirname(path.join(dir, p)), { recursive: true });
    fs.writeFileSync(path.join(dir, p), t);
  }
  return dir;
}

const by = (c, key) => c.list.find((x) => x.key === key);
const leads = (c, a, b) => c.edges.get(a.id)?.has(b.id) || false;

test('the five gaps become candidates, each citing a line that holds its token', () => {
  const dir = root();
  const read = F.reader(dir);
  const c = F.candidates(GRAPH, read);
  const login = by(c, 'screen:web:/login');
  const form = by(c, 'part:harbor-web/components/LoginForm.tsx#LoginForm');
  const reset = by(c, 'part:harbor-web/components/ResetForm.tsx#ResetForm');
  const session = by(c, 'req:POST /api/session');
  const api = by(c, 'req:POST /auth/login');
  const okta = by(c, 'n:okta');
  const event = by(c, 'event:user.signed_in');
  const hook = by(c, 'hook:POST /stripe/webhook');
  for (const x of [login, form, reset, session, api, okta, event, hook]) assert.ok(x, JSON.stringify(c.list.map((y) => y.key)));
  // Steps inside one page: the drawer's forms, in order, with the page's
  // own requests.
  assert.ok(leads(c, login, form) && leads(c, form, reset));
  // React context: the hook's work is in its provider.
  assert.ok(leads(c, login, session), 'the screen reaches the provider\'s request');
  assert.ok(leads(c, form, session), 'the drawer\'s step makes the page\'s request');
  // The Next route-handler hop.
  assert.ok(leads(c, session, api));
  assert.deepEqual(api.evidence.map((e) => e.file), ['harbor-web/app/api/session/route.ts', 'harbor-api/src/auth.controller.ts']);
  // Injection: the controller's service uses Okta and emits the event.
  assert.ok(leads(c, api, okta) && leads(c, api, event));
  assert.equal(event.evidence[0].file, 'harbor-api/src/welcome.listener.ts');
  // A webhook endpoint called back by the service it is named for.
  assert.ok(leads(c, by(c, 'n:stripe'), hook));
  // Navigation written in the step's code.
  assert.ok(leads(c, form, by(c, 'screen:web:/slips')));
  // Every citation holds its token on its line.
  for (const x of c.list) {
    for (const e of x.evidence) {
      const line = read.lines(e.file)[e.line - 1];
      assert.ok(line.includes(e.token), `${x.key}: ${e.file}:${e.line} lacks ${e.token}`);
    }
  }
  // The list the model reads names ids only and holds no test files.
  const text = F.listText(c, new Set(c.list.map((x) => x.id)));
  assert.match(text, new RegExp(`^${login.id} screen web /login`, 'm'));
  assert.match(text, new RegExp(`${session.id} request POST /api/session.*requests\\[${api.id}\\]`));
});

test('helpers read the code the way the closers need', () => {
  assert.deepEqual(F.pageSteps("{step === Step.Info && <StepInfo />}\ncase Forms.Login: return <LoginForm />\ngotoForm(Forms.ForgotPassword)").map((s) => [s.step, s.comp]),
    [['Info', 'StepInfo'], ['Login', 'LoginForm'], ['ForgotPassword', null]]);
  assert.deepEqual(F.eventSites("this.events.emit('a.b', x)\n@OnEvent(EMAIL_SEEN)").emits.map((e) => e.name), ['a.b']);
  assert.deepEqual(F.eventSites("this.events.emit('a.b', x)\n@OnEvent(EMAIL_SEEN)").listens.map((e) => e.name), ['EMAIL_SEEN']);
  assert.deepEqual(F.outgoingHooks('await handleWebhookTrigger({});\nasync function sendWebhook() {}').map((h) => h.token), ['handleWebhookTrigger']);
  assert.deepEqual(F.routeHops('const r = await fetch(`${apiBase}/payments/sessions/${type}`)').map((h) => h.raw), ['${apiBase}/payments/sessions/${type}']);
  const routes = [{ f: 'a.ts', m: 'POST', p: '/harbor/*' }, { f: 'b.ts', m: 'POST', p: '/harbor/boaters/createBoater' }, { f: 'c.ts', m: 'GET', p: '/slips/:id' }];
  assert.equal(F.matchRoute(routes, '/harbor/boaters/createBoater', 'POST'), 1);
  assert.equal(F.matchRoute(routes, '/harbor/boaters/getBoater', 'POST'), 0);
  assert.equal(F.matchRoute(routes, '/slips/:p', 'GET'), 2);
  assert.equal(F.matchRoute(routes, '/nowhere', 'GET'), null);
  assert.equal(F.findToken(['a', 'b', 'x token', 'c'], 1, 'token'), 3);
  assert.equal(F.findToken(['a', 'b', 'c', 'd', 'e', 'f', 'token'], 1, 'token'), 7, 'moved further than 3 lines: found anywhere');
  assert.equal(F.findToken(['a'], 1, 'token'), null);
});

/** What the worker's model step leaves in the store, made by hand: a
 * sign-in journey over the candidates, and every entry remembered. */
function stored(c, todo) {
  const step = (key, note) => {
    const x = by(c, key);
    return { key, kind: x.kind, view: x.view, note, evidence: x.evidence, linked: true };
  };
  const entry = todo.entries.find((e) => e.key === 'screen:web:/login');
  return {
    schema: F.SCHEMA,
    flows: [{
      id: 'sign-in', title: 'Sign in', entry: 'screen:web:/login', hash: entry.hash, origin: 'model',
      steps: [
        step('screen:web:/login', 'The boater opens the sign-in drawer.'),
        step('part:harbor-web/components/LoginForm.tsx#LoginForm', 'They type their email and password.'),
        step('req:POST /api/session', 'The site starts a session.'),
        step('req:POST /auth/login', 'The session route asks the API to sign them in.'),
        step('n:okta', 'Okta checks the password.'),
        step('screen:web:/slips', 'They land on their slips.'),
      ],
    }],
    asked: Object.fromEntries(todo.entries.map((e) => [e.key, e.hash])),
  };
}

test('a rebuild that changed nothing a journey stands on asks the model for nothing', () => {
  const dir = root();
  const first = F.prepare(dir, GRAPH, null);
  assert.ok(first.todo, 'a first map asks for every entry');
  assert.ok(first.todo.entries.some((e) => e.key === 'screen:web:/login'));
  assert.ok(first.todo.groups.length > 0 && first.todo.groups.every((g) => g.list.length > 0 && g.entries.length > 0) && Object.keys(first.todo.table).length > 0);
  // The cited lines go with the table, so the worker's checker needs no code.
  const t = Object.values(first.todo.table)[0];
  assert.ok(t.evidence[0].window.some((l) => l.includes(t.evidence[0].token)));
  const c = F.candidates(GRAPH, F.reader(dir));
  const store = stored(c, first.todo);
  // Lines move (a comment added at the top of two files): re-lined, no model.
  const moved = { ...FILES };
  moved['harbor-api/src/auth.controller.ts'] = '// Sign in.\n// Through Okta.\n' + FILES['harbor-api/src/auth.controller.ts'];
  moved['harbor-web/lib/session.tsx'] = '// The session.\n' + FILES['harbor-web/lib/session.tsx'];
  const g2 = structuredClone(GRAPH);
  g2.routes[1].l += 2;
  g2.links.find((l) => l.k === 'http').calls = [[0, 12]];
  g2.files.find((f) => f.p === 'harbor-web/lib/session.tsx').s = [sym('useSession', 'function', 6), sym('SessionProvider', 'function', 10)];
  const again = F.prepare(root(moved), g2, store);
  assert.equal(again.todo, null, JSON.stringify(again.report));
  const fl = again.store.flows[0];
  assert.equal(fl.steps.length, 6);
  assert.equal(fl.steps[3].evidence[1].line, 10, 'the handler was re-lined');
  assert.equal(again.flows[0].steps.length, 6);
  assert.equal(again.flows[0].steps[3].ref, 'r:1');
  assert.deepEqual(again.flows[0].coderhelm.linked, [true, true, true, true, true, true]);
});

test('steps whose code is gone are dropped, and a journey is dropped by the rules', () => {
  const dir = root();
  const first = F.prepare(dir, GRAPH, null);
  const store = stored(F.candidates(GRAPH, F.reader(dir)), first.todo);
  // Okta is gone from the service: one step of six goes, and the entry is
  // asked again because what it stands on changed.
  const noOkta = { ...FILES, 'harbor-api/src/auth.service.ts': FILES['harbor-api/src/auth.service.ts'].replace("import { OktaAuth } from '@okta/okta-auth-js';", "import { Auth } from './auth';") };
  const g2 = structuredClone(GRAPH);
  g2.links = g2.links.filter((l) => l.b !== 'n:okta');
  const one = F.prepare(root(noOkta), g2, store);
  assert.equal(one.store.flows[0].steps.length, 5);
  assert.ok(!one.store.flows[0].steps.some((s) => s.key === 'n:okta'));
  assert.ok(one.todo?.entries.some((e) => e.key === 'screen:web:/login'));
  // The session route and the API's login both gone: 3 of 6 steps (50%).
  const g3 = structuredClone(g2);
  g3.routes = g3.routes.slice(2);
  g3.links = g3.links.filter((l) => l.k !== 'http');
  const two = F.prepare(root(noOkta), g3, store);
  assert.equal(two.store.flows.length, 0);
  assert.match(two.report.dropped[0].why, /3 of 6 steps are gone/);
  // The first screen gone: the journey goes with it.
  const g4 = structuredClone(GRAPH);
  g4.screens = g4.screens.slice(1);
  const three = F.prepare(dir, g4, store);
  assert.equal(three.store.flows.length, 0);
  assert.equal(three.report.dropped[0].why, 'its first screen is gone');
});

/** What the worker's trace step leaves in the store after a first map, by
 * hand: a traced sign-in journey, and every entry asked about remembered
 * with its hash and the files it stands on (as `app_map_flows::merge`
 * writes them). */
function traced(first, c) {
  const step = (key, note) => {
    const x = by(c, key);
    return { key, kind: x.kind, view: x.view, note, evidence: x.evidence, linked: true };
  };
  const entry = first.todo.entries.find((e) => e.key === 'screen:web:/login');
  const store = structuredClone(first.store);
  store.flows = [{
    id: 'sign-in', title: 'Sign in', entry: 'screen:web:/login', hash: entry.hash, origin: 'trace',
    steps: [
      step('screen:web:/login', 'The boater opens the sign-in drawer.'),
      step('part:harbor-web/components/LoginForm.tsx#LoginForm', 'They type their email and password.'),
      step('req:POST /api/session', 'The site starts a session.'),
      step('req:POST /auth/login', 'The session route asks the API to sign them in.'),
      step('n:okta', 'Okta checks the password.'),
      step('screen:web:/slips', 'They land on their slips.'),
    ],
  }];
  store.asked = Object.fromEntries(first.todo.entries.map((e) => [e.key, { hash: e.hash, files: e.files }]));
  return store;
}

test('a release that changed only the extractor traces nothing, spends nothing and keeps every journey', () => {
  const dir = root();
  const first = F.prepare(dir, GRAPH, null);
  const store = traced(first, F.candidates(GRAPH, F.reader(dir)));
  // The same code, read by an extractor that names one screen differently
  // and finds one more link: every entry's candidates hash differently.
  const g2 = structuredClone(GRAPH);
  g2.screens.find((x) => x.id === 'screen:web:/slips').id = 'screen:web:slips';
  g2.links.push({ a: 'f:harbor-web/app/slips/page.tsx', b: 'f:harbor-web/lib/session.tsx', k: 'import', names: ['useSession'] });
  const bumped = F.prepare(root(), g2, store);
  const hashes = new Map(F.prepare(root(), g2, null).todo.entries.map((e) => [e.key, e.hash]));
  assert.ok(first.todo.entries.some((e) => hashes.has(e.key) && hashes.get(e.key) !== e.hash), 'the new extractor changes what the hashes stand on');
  assert.equal(bumped.todo, null, JSON.stringify(bumped.report));
  assert.equal(bumped.report.redo, 0);
  assert.equal(bumped.store.flows.length, 1);
  const fl = bumped.store.flows[0];
  assert.equal(fl.steps.length, 6, 'no step is lost');
  assert.equal(fl.steps[5].key, 'screen:web:slips', 'the renamed screen is re-pointed by its code');
  assert.equal(bumped.report.repointed, 1);
  // Stamped with the new extractor's hashes: the next build with this
  // extractor compares like with like.
  for (const [k, a] of Object.entries(bumped.store.asked)) assert.equal(a.hash, hashes.get(k) ?? a.hash);
  const again = F.prepare(root(), g2, bumped.store);
  assert.equal(again.todo, null);
  // The code changed under the journey: it is traced again.
  const edited = { ...FILES, 'harbor-api/src/auth.service.ts': FILES['harbor-api/src/auth.service.ts'].replace("import { OktaAuth } from '@okta/okta-auth-js';", "import { Auth } from './auth';") };
  const g3 = structuredClone(g2);
  g3.links = g3.links.filter((l) => l.b !== 'n:okta');
  const moved = F.prepare(root(edited), g3, bumped.store);
  assert.ok(moved.todo?.entries.some((e) => e.key === 'screen:web:/login' && e.why === 'changed'), JSON.stringify(moved.report));
});

test('a step on a catch-all screen now split by type is re-pointed to its type\'s screen by its lines', () => {
  const dir = root({
    'harbor-web/app/login/page.tsx': FILES['harbor-web/app/login/page.tsx'],
    'harbor-web/app/[...slug]/page.tsx': `export default function Page({ type }) {
  if (type === 'slip') return <SlipPage />;
  return <EventPage />;
}
function SlipPage() {
  const book = () => fetch('/api/book');
  return <form onSubmit={book} />;
}
function EventPage() {
  const join = () => fetch('/api/join');
  return <div onClick={join} />;
}
`,
  });
  const read = F.reader(dir);
  const ev = (file, line, token) => ({ file, line, token });
  const screen = (key, file, line, token, span) => ({ id: key, kind: 'screen', key, view: 'f:' + file, file, evidence: [ev(file, line, token)], ...(span ? { span } : {}) });
  const slug = 'harbor-web/app/[...slug]/page.tsx';
  const list = [
    screen('screen:web:/login', 'harbor-web/app/login/page.tsx', 3, 'LoginPage'),
    screen('screen:web:/slips/slip', slug, 5, 'SlipPage', [5, 8]),
    screen('screen:web:/slips/event', slug, 9, 'EventPage', [9, 12]),
    { id: 'R1', kind: 'request', key: 'req:POST /api/book', view: 'r:1', evidence: [ev(slug, 6, "fetch('/api/book')")] },
  ];
  const c = { list, byKey: new Map(list.map((x) => [x.key, x])), edges: new Map() };
  const st = (key, kind, view, e, note) => ({ key, kind, view, note, evidence: [e], linked: true });
  const store = { flows: [{
    id: 'book-a-slip', title: 'Book a slip', entry: 'screen:web:/login', origin: 'trace', steps: [
      st('screen:web:/login', 'screen', 'f:harbor-web/app/login/page.tsx', ev('harbor-web/app/login/page.tsx', 3, 'LoginPage'), 'They sign in.'),
      st('screen:web:/[...slug]', 'screen', 'f:' + slug, ev(slug, 7, '<form onSubmit={book}'), 'They open a slip and press Book.'),
      st('req:POST /api/book', 'request', 'r:1', ev(slug, 6, "fetch('/api/book')"), 'The page asks to book it.'),
    ],
  }] };
  const out = F.revalidate(store, c, read);
  assert.equal(out.kept.length, 1, JSON.stringify(out.dropped));
  assert.equal(out.kept[0].steps[1].key, 'screen:web:/slips/slip', 'the renderer whose lines hold the step');
  assert.equal(out.repointed, 1);
  assert.deepEqual(out.changed, [], 'the code is as it was: nothing to trace again');
  // The same step cited inside the other renderer lands there instead.
  store.flows[0].steps[1].evidence = [ev(slug, 11, '<div onClick={join}')];
  assert.equal(F.revalidate(store, c, read).kept[0].steps[1].key, 'screen:web:/slips/event');
  // A journey whose own first screen was split moves to the renderer it
  // starts in, and that entry is traced again as a new one.
  const fromSplit = { flows: [{ ...structuredClone(store.flows[0]), entry: 'screen:web:/[...slug]', steps: structuredClone(store.flows[0].steps).slice(1).concat([st('req:POST /api/book', 'request', 'r:1', ev(slug, 6, "fetch('/api/book')"), 'Again.')]) }] };
  fromSplit.flows[0].steps[0].key = 'screen:web:/[...slug]';
  fromSplit.flows[0].steps[0].evidence = [ev(slug, 6, "fetch('/api/book')")];
  const moved = F.revalidate(fromSplit, c, read);
  assert.equal(moved.kept[0]?.entry, 'screen:web:/slips/slip', JSON.stringify(moved));
  assert.deepEqual(moved.split, ['screen:web:/slips/slip']);
});

test('new screens are traced first, then changed code, then the rest', () => {
  const files = { ...FILES, 'harbor-web/app/signup/page.tsx': 'export default function SignUpPage() {\n  return <form />;\n}\n' };
  const first = F.prepare(root(), GRAPH, null);
  // Only the sign-in screen was traced; the rest are still waiting.
  const store = structuredClone(first.store);
  const login = first.todo.entries.find((e) => e.key === 'screen:web:/login');
  store.asked = { [login.key]: { hash: login.hash, files: login.files } };
  // The next build: a new sign-up screen, and the sign-in code changed.
  const edited = { ...files, 'harbor-api/src/auth.service.ts': files['harbor-api/src/auth.service.ts'].replace("import { OktaAuth } from '@okta/okta-auth-js';", "import { Auth } from './auth';") };
  const g2 = structuredClone(GRAPH);
  g2.files.push({ p: 'harbor-web/app/signup/page.tsx', s: [sym('SignUpPage', 'default', 1)] });
  g2.screens.push({ id: 'screen:web:/signup', url: '/signup', file: 'harbor-web/app/signup/page.tsx', app: 'web', name: 'Sign up' });
  g2.links = g2.links.filter((l) => l.b !== 'n:okta');
  const next = F.prepare(root(edited), g2, store);
  const whys = next.todo.entries.map((e) => e.why);
  assert.equal(next.todo.entries[0].key, 'screen:web:/signup', JSON.stringify(next.todo.entries));
  assert.equal(whys[0], 'new');
  assert.ok(whys.indexOf('changed') > 0 && whys.includes('missing'), JSON.stringify(whys));
  const rank = { new: 0, changed: 1, missing: 2 };
  assert.deepEqual(whys, [...whys].sort((a, b) => rank[a] - rank[b]));
  assert.equal(next.todo.entries.find((e) => e.key === 'screen:web:/login').why, 'changed');
  assert.deepEqual([next.report.new, next.report.changed], [1, 1]);
});

test('near-identical journeys are one, and no two share a title (the cases the worker is tested on too)', () => {
  const cases = JSON.parse(fs.readFileSync(path.join(import.meta.dirname, '..', 'fixtures', 'journeys-dedupe.json'), 'utf8'));
  for (const c of cases) {
    const out = F.dedupe(structuredClone(c.flows));
    assert.deepEqual(out.map((f) => f.title), c.want.titles, c.name);
    assert.deepEqual(out.map((f) => f.id), c.want.ids, c.name);
    assert.deepEqual(out.map((f) => f.from), c.want.from, c.name);
    assert.ok(out.every((f) => !('_fresh' in f)));
    assert.equal(new Set(out.map((f) => F.titleWords(f.title))).size, out.length, `${c.name}: no two share a title`);
    assert.deepEqual(F.dedupe(structuredClone(out)), out, `${c.name}: once is enough`);
  }
});

test('a stored map with duplicate journeys is cleaned on the next build, with no model call', () => {
  const dir = root();
  const first = F.prepare(dir, GRAPH, null);
  const c = F.candidates(GRAPH, F.reader(dir));
  const store = traced(first, c);
  // The same journey stored several times over (as an older release left it),
  // and once more from the form's entry with one step fewer.
  const one = store.flows[0];
  store.flows = [0, 1, 2, 3, 4].map((n) => ({ ...structuredClone(one), id: n ? `sign-in-${n + 1}` : 'sign-in' }));
  store.flows.push({ ...structuredClone(one), id: 'a-sign-in', title: 'A sign in', entry: 'part:harbor-web/components/LoginForm.tsx#LoginForm', steps: structuredClone(one.steps).slice(1) });
  const again = F.prepare(root(), GRAPH, store);
  assert.equal(again.todo, null, JSON.stringify(again.report));
  assert.equal(again.store.flows.length, 1);
  assert.deepEqual([again.store.flows[0].id, again.store.flows[0].title, again.store.flows[0].steps.length], ['sign-in', 'Sign in', 6]);
  assert.deepEqual(again.store.flows[0].from, ['part:harbor-web/components/LoginForm.tsx#LoginForm', 'screen:web:/login']);
  assert.equal(again.report.merged, 5);
  assert.equal(again.flows.length, 1);
});

test('a journey from a catch-all route now split by type names the screens whose trace retires it', () => {
  const route = 'harbor-web/app/[...slug]/page.tsx';
  const dir = root({ [route]: 'export default function Page() {\n  return <Resolve />;\n}\n', 'harbor-web/components/EventPage.tsx': 'export function EventPage() {\n  return <form />;\n}\n', 'harbor-web/app/api/events/route.ts': 'export async function POST() {}\n' });
  const ev = (file, line, token) => ({ file, line, token });
  const list = [
    { id: 'S1', kind: 'screen', key: 'screen:web:/[...slug]', view: 'f:' + route, file: route, evidence: [ev(route, 1, 'export default')] },
    { id: 'S2', kind: 'screen', key: 'screen:web:/events/(event)', view: 'f:harbor-web/components/EventPage.tsx', file: 'harbor-web/components/EventPage.tsx', evidence: [ev('harbor-web/components/EventPage.tsx', 1, 'EventPage')], focus: ['EventPage'], dispatcher: route },
    { id: 'R1', kind: 'request', key: 'req:POST /api/events', view: 'r:1', evidence: [ev('harbor-web/app/api/events/route.ts', 1, 'POST')] },
  ];
  const c = { list, byKey: new Map(list.map((x) => [x.key, x])), edges: new Map() };
  const st = (x) => ({ key: x.key, kind: x.kind, view: x.view, note: 'A step.', evidence: x.evidence, linked: true });
  const store = { flows: [{ id: 'book-an-event', title: 'Book an event', entry: 'screen:web:/[...slug]', origin: 'trace', steps: [st(list[0]), st(list[2]), st(list[1])] }] };
  const out = F.revalidate(store, c, F.reader(dir));
  assert.deepEqual(out.kept[0].split, ['screen:web:/events/(event)']);
  assert.deepEqual(out.kept[0].from, ['screen:web:/[...slug]']);
  // The renderer's own trace, with the same goal, takes its place.
  const fresh = { id: 'book-an-event', title: 'Book an event', entry: 'screen:web:/events/(event)', origin: 'trace', _fresh: true, steps: [st(list[1]), st(list[2])] };
  const merged = F.dedupe([out.kept[0], fresh]);
  assert.equal(merged.length, 1);
  assert.equal(merged[0].entry, 'screen:web:/events/(event)');
});

// The second round of gap closers, on a made-up marina app: an outside
// service behind two APIs, calls written directly or through an object of
// paths, a call through an SDK, a Next route cited at its exported method,
// and a join drawer mounted from a layout no page imports.
const MARINA = {
  'marina-web/app/api/dock/route.ts': `import { dock } from '../../../lib/dock';

export async function POST(req) {
  return Response.json(await dock(req));
}
`,
  'marina-web/lib/dock.ts': `export async function dock(req) {
  return fetch('/api/dock', { method: 'POST' });
}
`,
  'marina-mobile/src/api/urls.ts': `export const URLS = {
  book: '/slips/:id/bookings',
  cancel: '/slips/:id/cancel',
};
`,
  'marina-mobile/src/screens/SlipScreen.tsx': `import { URLS } from '../api/urls';
import { apiPost } from '../api/client';

export default function SlipScreen() {
  const book = () => apiPost(URLS.book, {});
  return <Button onPress={book} />;
}
`,
  'marina-mobile/src/api/client.ts': `export function apiPost(url, body) {
  return fetch(url, { method: 'POST', body });
}
`,
  'marina-mobile/src/state/session.ts': `import { HarborSession } from '@marina/session-sdk';

export async function signIn(email, password) {
  return HarborSession.login({ email, password });
}
`,
  'marina-api/src/bookings.controller.ts': `import { Controller, Post } from '@nestjs/common';
import { TidesService } from './tides.service';

@Controller('slips')
export class BookingsController {
  constructor(private readonly tides: TidesService) {}

  @Post(':id/bookings')
  book() {
    return this.tides.check();
  }
}
`,
  'marina-api/src/tides.service.ts': `export class TidesService {
  check() {
    return fetch(process.env.TIDEWATCH_API_URL + '/now');
  }
}
`,
  'marina-web/components/JoinDrawer.tsx': `import { SignupForm } from './SignupForm';
import { PaymentForm } from './PaymentForm';

export function JoinDrawer({ step }) {
  switch (step) {
    case 'signup':
      return <SignupForm />;
    case 'payment':
      return <PaymentForm />;
  }
}
`,
  'marina-web/components/SignupForm.tsx': `export function SignupForm() {
  return <form />;
}
`,
  'marina-web/components/PaymentForm.tsx': `export function PaymentForm() {
  return <form />;
}
`,
};
const MARINA_GRAPH = {
  schema: 'floorplan/graph@1',
  nodes: [{ id: 'ext-tidewatch', kind: 'external', label: 'Tidewatch', env: ['TIDEWATCH_API_URL', 'TIDEWATCH_MOBILE_API_URL'], evidence: [{ file: 'marina-api/src/tides.service.ts', line: 3 }] }],
  files: Object.keys(MARINA).map((p) => ({ p, s: [] })).map((f) => ({ ...f, s: {
    'marina-mobile/src/screens/SlipScreen.tsx': [sym('SlipScreen', 'default', 4)],
    'marina-api/src/bookings.controller.ts': [sym('BookingsController', 'class', 5)],
    'marina-api/src/tides.service.ts': [sym('TidesService', 'class', 1)],
    'marina-web/components/JoinDrawer.tsx': [sym('JoinDrawer', 'function', 4)],
    'marina-web/components/SignupForm.tsx': [sym('SignupForm', 'function', 1)],
    'marina-web/components/PaymentForm.tsx': [sym('PaymentForm', 'function', 1)],
  }[f.p] || [] })),
  routes: [
    { f: 'marina-web/app/api/dock/route.ts', m: 'POST', p: '/api/dock', l: 1, h: 'POST' },
    { f: 'marina-api/src/bookings.controller.ts', m: 'POST', p: '/slips/:id/bookings', l: 8, h: 'book' },
  ],
  links: [
    { a: 'f:marina-mobile/src/screens/SlipScreen.tsx', b: 'f:marina-mobile/src/api/urls.ts', k: 'import', names: ['URLS'] },
    { a: 'f:marina-mobile/src/screens/SlipScreen.tsx', b: 'f:marina-mobile/src/api/client.ts', k: 'import', names: ['apiPost'] },
    { a: 'f:marina-api/src/bookings.controller.ts', b: 'f:marina-api/src/tides.service.ts', k: 'import', names: ['TidesService'] },
    { a: 'f:marina-api/src/tides.service.ts', b: 'n:ext-tidewatch', k: 'http', labels: ['TIDEWATCH_API_URL'] },
    { a: 'f:marina-web/components/JoinDrawer.tsx', b: 'f:marina-web/components/SignupForm.tsx', k: 'import', names: ['SignupForm'] },
    { a: 'f:marina-web/components/JoinDrawer.tsx', b: 'f:marina-web/components/PaymentForm.tsx', k: 'import', names: ['PaymentForm'] },
  ],
  screens: [{ id: 'screen:mobile:/(Stack)/Slip', url: '/(Stack)/Slip', file: 'marina-mobile/src/screens/SlipScreen.tsx', app: 'mobile', name: 'Slip' }],
};

test('second-round closers: split services, direct and URL-map calls, SDK calls, route methods, drawers', () => {
  const dir = root(MARINA);
  const read = F.reader(dir);
  const c = F.candidates(MARINA_GRAPH, read);
  // The service is named by the API its code reads, cited there.
  const tide = by(c, 'n:ext-tidewatch#TIDEWATCH_API_URL');
  assert.ok(tide, JSON.stringify(c.list.map((x) => x.key)));
  assert.equal(tide.view, 'n:ext-tidewatch');
  assert.deepEqual(tide.evidence[0], { file: 'marina-api/src/tides.service.ts', line: 3, token: 'TIDEWATCH_API_URL' });
  // A call through an object of paths: cited at the path, used by the screen.
  const book = by(c, 'req:POST /slips/:p/bookings');
  assert.equal(book.evidence[0].file, 'marina-mobile/src/api/urls.ts');
  assert.ok(leads(c, by(c, 'screen:mobile:/(Stack)/Slip'), book));
  assert.ok(leads(c, book, tide));
  // A direct fetch to a Next route, whose handler is its exported method.
  const dock = by(c, 'req:POST /api/dock');
  assert.deepEqual(dock.evidence[1], { file: 'marina-web/app/api/dock/route.ts', line: 3, token: 'export async function POST' });
  // A call through an SDK, to the service behind its package.
  const login = by(c, 'req:SDK sdk:HarborSession.login');
  assert.ok(login && leads(c, login, by(c, 'pkg:@marina/session-sdk')));
  // A drawer no page imports is a screen, with its forms as steps cited
  // where they are declared.
  const drawer = by(c, 'part:marina-web/components/JoinDrawer.tsx#JoinDrawer');
  const signup = by(c, 'part:marina-web/components/SignupForm.tsx#SignupForm');
  assert.ok(drawer && signup && leads(c, drawer, signup));
  assert.deepEqual(signup.evidence[0], { file: 'marina-web/components/SignupForm.tsx', line: 1, token: 'SignupForm' });
  for (const x of c.list) for (const e of x.evidence) assert.ok(read.lines(e.file)[e.line - 1].includes(e.token), `${x.key}: ${e.file}:${e.line}`);
});

test('run order: calls come in the order the code runs them, however deep', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'run-order-'));
  const files = {
    // The sign-up call sits two files down, the sign-in call in the form
    // itself, and a mutation hook is declared before the one that runs
    // first: the order is the statements', not the depth's or the hooks'.
    'web/SignupForm.tsx': `import { signIn } from 'next-auth/react';
import { useVerify } from './useVerify';
import {
  fetchSignup,
} from './fetchSignup';

export function SignupForm() {
  const { mutate: verify } = useVerify();
  const onSubmit = async (data) => {
    await fetchSignup(data);
    await signIn('credentials', data);
    verify(data);
  };
  return <form onSubmit={onSubmit} />;
}
`,
    'web/fetchSignup.ts': `export async function fetchSignup(data) {
  return fetch('/api/signup', { method: 'POST', body: JSON.stringify(data) });
}
`,
    'web/useVerify.ts': `export function useVerify() {
  return useMutation({ mutationFn: () => fetch('/api/verify') });
}
`,
  };
  for (const [f, t] of Object.entries(files)) {
    fs.mkdirSync(path.join(dir, path.dirname(f)), { recursive: true });
    fs.writeFileSync(path.join(dir, f), t);
  }
  const r = F.reader(dir);
  const ix = { syms: new Map() };
  const signup = F.useLine(ix, r, 'web/SignupForm.tsx', 'web/fetchSignup.ts', ['fetchSignup']);
  const verify = F.useLine(ix, r, 'web/SignupForm.tsx', 'web/useVerify.ts', ['useVerify']);
  assert.deepEqual([signup, verify], [10, 12]);
  // The chains: sign-up (form line 10, then its fetch on line 2), sign-in
  // (the form's own line 11), verify (form line 12, then line 2).
  const chains = { signup: [signup, 2], signin: [11], verify: [verify, 2] };
  const sorted = Object.entries(chains).sort(([, a], [, b]) => F.runOrder(a, b)).map(([k]) => k);
  assert.deepEqual(sorted, ['signup', 'signin', 'verify']);
  fs.rmSync(dir, { recursive: true, force: true });
});
