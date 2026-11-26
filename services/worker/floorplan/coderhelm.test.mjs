// CoderHelm's detectors (coderhelm/) on small made-up sources, one folder per case:
// outside services, calls through wrappers and GraphQL, infra to code (Terraform,
// CloudFormation/SAM, Serverless, CDK, deploy workflows), webhooks, data links and screens
// (React Navigation, Expo Router at its real root, Sanity, screens a dynamic route dispatches to, Flutter, Android,
// iOS). The driver test (driver.test.mjs) runs them all on the harbor fixture.
//
//   node --test services/worker/floorplan/coderhelm.test.mjs
import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { loadFloorplan } from './coderhelm/lib/fp.mjs';
import { findServices } from './coderhelm/externals.mjs';
import { findCalls } from './coderhelm/calls.mjs';
import * as infra from './coderhelm/infra/join.mjs';
import { webhooks, dataLinks } from './coderhelm/hooks.mjs';
import * as screens from './coderhelm/screens.mjs';
import * as EXPO from './coderhelm/screens/expo-router.mjs';
import * as RN from './coderhelm/screens/react-navigation.mjs';
import * as SANITY from './coderhelm/screens/sanity.mjs';
import * as DISPATCH from './coderhelm/screens/dispatch.mjs';
import * as FLUTTER from './coderhelm/screens/flutter.mjs';
import * as ANDROID from './coderhelm/screens/android.mjs';
import * as IOS from './coderhelm/screens/ios.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
await loadFloorplan(path.resolve(HERE, '../../../vendor/floorplan/skills/floorplan'));

/** A folder holding `files` ({path: text}). */
function tree(files) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ch-detect-'));
  for (const [p, t] of Object.entries(files)) { fs.mkdirSync(path.dirname(path.join(dir, p)), { recursive: true }); fs.writeFileSync(path.join(dir, p), t); }
  return dir;
}

test('outside services: catalog packages, SDK clients, env families, and no boxes for unknown hosts', () => {
  const root = tree({
    'shop/package.json': JSON.stringify({ dependencies: { '@sentry/node': '8.0.0', '@shopify/flash-list': '1.0.0' } }),
    'shop/src/pay.ts': "import { SQSClient } from '@aws-sdk/client-sqs';\nconst base = process.env.ACMEPAY_API_URL;\nexport const go = () => fetch(`${base}/charge`);\n",
    'shop/src/own.ts': 'const api = process.env.NEXT_PUBLIC_SHOP_API_URL;\nconst video = process.env.VIDEO_ENCODER_BASE_URL;\n',
    'shop/src/links.ts': "export const icon = fetch('https://cdn.unknownhost.example-assets.io/x.png');\n",
    'shop/src/pay.test.ts': "const k = process.env.TESTVENDOR_API_URL;\n",
    'shop/lambdas/video-encoder/index.js': 'exports.handler = async () => ({});\n',
  });
  const { services, otherHosts, dropped } = findServices(root, ['shop']);
  assert.equal(services.sentry.how, 'catalog');
  assert.ok(!services.shopify, 'a UI package is not the vendor');
  assert.equal(services['aws:sqs'].how, 'sdk');
  assert.equal(services['env:acmepay'].how, 'env');
  assert.equal(services['env:acmepay'].inferred, true);
  assert.deepEqual(services['env:acmepay'].evidence[0], { file: 'shop/src/pay.ts', line: 2, how: 'inferred', confidence: 0.6 });
  // the team's own endpoints: a repository name and a deployed function's folder
  assert.ok(!services['env:shop'] && !services['env:video']);
  assert.deepEqual(dropped, ['NEXT_PUBLIC_SHOP_API_URL', 'VIDEO_ENCODER_BASE_URL']);
  assert.ok(!services['env:testvendor'], 'tests are never evidence');
  assert.ok(!Object.keys(services).some((k) => k.startsWith('host:')));
  assert.deepEqual(otherHosts.map((h) => h.host), ['example-assets.io']);
});

test('calls: URL maps, wrappers of wrappers, a wrapper passed as a value, Nest proxies and GraphQL', () => {
  const root = tree({
    'web/src/urls.ts': "export const accountUrlMap = {\n  profile: '/members/profile',\n  cards: '/members/cards',\n};\n",
    'web/src/http.ts': "export async function apiGet(path: string) {\n  const res = await fetch(`${process.env.NEXT_PUBLIC_API_BASE}${path}`);\n  return res.json();\n}\nexport async function getJson(path: string) {\n  return apiGet(path);\n}\n",
    'web/src/profile.ts': "import { accountUrlMap } from './urls';\nimport { apiGet, getJson } from './http';\nexport const p = () => apiGet(accountUrlMap.profile);\nexport const c = () => getJson('/members/cards');\nexport const s = () => useSWR('/members/settings', apiGet);\n",
    'web/src/schedule.ts': "import { gql } from '@apollo/client';\nexport const Q = gql`\n  query Classes($day: String) {\n    # the day's classes\n    classes(day: $day) { id }\n  }\n`;\n",
    'web/src/profile.test.ts': "apiGet('/members/secret');\n",
    'api/src/resolver.ts': "import { Resolver, Query } from '@nestjs/graphql';\n@Resolver()\nexport class ClassResolver {\n  @Query(() => [String])\n  classes() { return []; }\n}\n",
    'api/src/proxy.module.ts': "import { createProxyMiddleware } from 'http-proxy-middleware';\nconst PARTNER_PATH = 'partner';\nexport class ProxyModule {\n  configure(c) {\n    c.apply(createProxyMiddleware({ target: process.env.PARTNER_API })).forRoutes({ path: `/${PARTNER_PATH}*` });\n  }\n}\n",
    'web/src/partner.ts': "import { apiGet } from './http';\nexport const x = () => apiGet('/partner/orders/7');\n",
  });
  const routes = [{ m: 'GET', p: '/members/profile', f: 'api/src/members.ts' }, { m: 'GET', p: '/members/:id', f: 'api/src/members.ts' }, { m: 'GET', p: '/members/settings', f: 'api/src/members.ts' }];
  const { links, extraRoutes } = findCalls(root, ['api', 'web'], routes);
  const by = (file, p) => links.find((l) => l.file === file && l.routeP === p);
  assert.equal(by('web/src/profile.ts', '/members/profile').via, 'wrapper:apiGet');
  assert.equal(by('web/src/profile.ts', '/members/:id').path, '/members/cards', 'wrapper of a wrapper, matched to the :param route');
  assert.match(by('web/src/profile.ts', '/members/settings').via, /^passed:apiGet/);
  assert.deepEqual(extraRoutes.map((r) => [r.p, r.kind]).sort(), [['/graphql#classes', 'graphql'], ['/partner*', 'nest_proxy']]);
  assert.equal(extraRoutes.find((r) => r.kind === 'nest_proxy').targetEnv, 'PARTNER_API');
  const gq = by('web/src/schedule.ts', '/graphql#classes');
  assert.equal(gq.routeF, 'api/src/resolver.ts');
  const proxied = by('web/src/partner.ts', '/partner*');
  assert.equal(proxied.wildcard, true);
  assert.equal(proxied.path, '/partner/orders/7', 'a wildcard keeps the path the client asks for');
  assert.ok(!links.some((l) => /\.test\./.test(l.file)));
});

test('Terraform: module outputs, CloudFront origins, triggers, placeholder code joined through the deploy workflow', () => {
  const root = tree({
    'infra/envs/prod/main.tf': 'module "resizer" {\n  source = "../../modules/resizer"\n  env    = "prod"\n}\nmodule "cdn" {\n  source = "../../modules/cdn"\n  origin = module.resizer.url_domain\n  alias  = "img.example-harbor.dev"\n}\n',
    'infra/modules/resizer/main.tf': 'variable "env" {}\nresource "aws_lambda_function" "resize" {\n  function_name = "${var.env}-resizer"\n  filename      = "placeholder.zip"\n  handler       = "index.handler"\n  lifecycle {\n    ignore_changes = [filename, source_code_hash]\n  }\n}\nresource "aws_lambda_function_url" "resize" {\n  function_name      = aws_lambda_function.resize.function_name\n  authorization_type = "NONE"\n}\nresource "aws_cloudwatch_event_rule" "nightly" {\n  schedule_expression = "rate(1 day)"\n}\nresource "aws_cloudwatch_event_target" "nightly" {\n  rule = aws_cloudwatch_event_rule.nightly.name\n  arn  = aws_lambda_function.resize.arn\n}\n',
    'infra/modules/resizer/outputs.tf': 'output "url_domain" {\n  value = aws_lambda_function_url.resize.function_url\n}\n',
    'infra/modules/cdn/main.tf': 'variable "origin" {}\nvariable "alias" {}\nresource "aws_cloudfront_distribution" "this" {\n  aliases = [var.alias]\n  origin {\n    domain_name = var.origin\n    origin_id   = "o"\n  }\n}\n',
    'app/lambdas/resizer/index.mjs': 'export const handler = async () => ({ statusCode: 200 });\n',
    'app/src/image.ts': "export const src = (p: string) => `https://img.example-harbor.dev/${p}`;\n",
    'app/.github/workflows/resizer.yml': 'on:\n  push:\n    paths: ["lambdas/resizer/**"]\njobs:\n  deploy:\n    runs-on: ubuntu-latest\n    steps:\n      - run: |\n          cd lambdas/resizer\n          zip -r f.zip .\n          aws lambda update-function-code --function-name prod-resizer --zip-file fileb://f.zip\n',
  });
  const o = infra.analyse(root);
  const unit = 'infra/modules/resizer:aws_lambda_function.resize';
  assert.equal(o.units[unit].placeholder, true);
  const via = (v) => o.edges.filter((e) => e.via === v);
  assert.deepEqual(via('function_url').map((e) => e.b), [unit]);
  assert.deepEqual(via('eventbridge_target').map((e) => e.b), [unit]);
  const origin = via('cloudfront_origin')[0];
  assert.deepEqual([origin.a, origin.b, origin.how], ['domain:img.example-harbor.dev', unit, 'exact']);
  const deploy = via('deploy_workflow')[0];
  // the name is exact, the folder comes from a `cd` in a step with no working-directory: inferred
  assert.deepEqual([deploy.a, deploy.b, deploy.how], ['f:app/lambdas/resizer/index.mjs', unit, 'inferred']);
  assert.ok(!via('iac_source').length, 'a placeholder package names no source');
  const call = via('domain_literal')[0];
  assert.deepEqual([call.a, call.unit], ['f:app/src/image.ts', unit]);
});

test('CloudFormation, SAM, Serverless and CDK: functions, their sources and their event sources', () => {
  const root = tree({
    'sam/template.yaml': "AWSTemplateFormatVersion: '2010-09-09'\nTransform: AWS::Serverless-2016-10-31\nResources:\n  Uploads:\n    Type: AWS::S3::Bucket\n  Thumb:\n    Type: AWS::Serverless::Function\n    Properties:\n      CodeUri: src/thumb/\n      Handler: app.handler\n      Events:\n        Upload:\n          Type: S3\n          Properties:\n            Bucket: !Ref Uploads\n            Events: s3:ObjectCreated:*\n",
    'sam/src/thumb/app.js': 'exports.handler = async () => {};\n',
    'sls/serverless.yml': 'service: notes\nprovider:\n  name: aws\nfunctions:\n  list:\n    handler: handlers/list.main\n    events:\n      - http:\n          path: notes\n          method: get\n',
    'sls/handlers/list.js': 'module.exports.main = async () => ({});\n',
    'cdk/cdk.json': '{"app": "npx ts-node bin/app.ts"}\n',
    'cdk/lib/stack.ts': "import * as lambda from 'aws-cdk-lib/aws-lambda';\nimport { SqsEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';\nimport * as path from 'path';\nexport class S {\n  constructor(scope) {\n    const worker = new lambda.Function(this, 'Worker', {\n      handler: 'index.handler',\n      code: lambda.Code.fromAsset(path.join(__dirname, '../functions/worker')),\n    });\n    worker.addEventSource(new SqsEventSource(queue));\n  }\n}\n",
    'cdk/functions/worker/index.js': 'exports.handler = async () => {};\n',
  });
  const o = infra.analyse(root);
  const src = o.edges.filter((e) => e.via === 'iac_source').map((e) => [e.a, e.b]).sort();
  assert.deepEqual(src, [['f:cdk/functions/worker/index.js', 'cdk/lib/stack.ts:Worker'], ['f:sam/src/thumb/app.js', 'sam/template.yaml:Thumb'], ['f:sls/handlers/list.js', 'sls/serverless.yml:list']]);
  assert.ok(o.edges.some((e) => e.via === 's3_notification' && e.a === 's3:sam/template.yaml:Uploads' && e.b === 'sam/template.yaml:Thumb'));
  assert.ok(o.edges.some((e) => e.via === 'api_integration' && e.b === 'sls/serverless.yml:list' && e.label === 'get notes'));
  assert.ok(o.edges.some((e) => e.via === 'event_source_mapping' && e.b === 'cdk/lib/stack.ts:Worker'));
});

test('webhooks are named from the vendor beside the hook word; data links join ETL writes to ORM entities', () => {
  const root = tree({
    'api/src/hooks.ts': "// verifies the stripe-signature header\nexport const h = (req) => stripe.webhooks.constructEvent(req.body, req.headers['stripe-signature'], 'whsec');\n",
    'api/src/events.ts': 'export const list = () => [];\n',
    'api/src/slot.entity.ts': "@Entity('slot')\nexport class Slot {}\n",
    'etl/dags/load.py': 'def run(h):\n    h.run("INSERT INTO slot (id) VALUES (1)")\n    h.run("SELECT * FROM slot")\n',
  });
  const routes = [{ m: 'POST', p: '/webhooks/stripe', f: 'api/src/hooks.ts', l: 2 }, { m: 'POST', p: '/api/events', f: 'api/src/events.ts' }, { m: 'GET', p: '/webhooks/stripe/status', f: 'api/src/hooks.ts' }];
  const w = webhooks(root, routes, {});
  assert.deepEqual(w.map((x) => [x.vendor, x.route, x.signature]), [['stripe', 0, true]]);
  const d = dataLinks(root, ['api', 'etl']);
  assert.deepEqual(d.links.map((l) => [l.file, l.entity, l.mode, l.table]), [['etl/dags/load.py', 'api/src/slot.entity.ts', 'write', 'slot']]);
});

test('screens: React Navigation gates, Expo Router at its real root, Sanity, dispatched screens, Flutter, Android, iOS', () => {
  const root = tree({
    'rn/package.json': JSON.stringify({ dependencies: { 'react-native': '0.76.0', '@react-navigation/native-stack': '7.0.0' } }),
    'rn/app/Root.tsx': "import { createNativeStackNavigator } from '@react-navigation/native-stack';\nimport Home from './Home';\nimport Login from './Login';\nconst Stack = createNativeStackNavigator();\nexport default function Root({ token }) {\n  return (\n    <Stack.Navigator>\n      {token ? (\n        <Stack.Screen name=\"Home\" component={Home} />\n      ) : (\n        <Stack.Screen name=\"Login\" component={Login} />\n      )}\n    </Stack.Navigator>\n  );\n}\n",
    'rn/app/Home.tsx': "import { View } from 'react-native';\nexport default function Home() { return <View />; }\n",
    'rn/app/Login.tsx': "import { View } from 'react-native';\nexport default function Login({ navigation }) { navigation.navigate('Home'); return <View />; }\n",
    'ex/package.json': JSON.stringify({ dependencies: { 'expo-router': '4.0.0' } }),
    'ex/app/routes/_layout.tsx': "import { Stack } from 'expo-router';\nexport default function L() { return <Stack />; }\n",
    'ex/app/routes/(app)/_layout.tsx': "import { Redirect, Stack } from 'expo-router';\nexport default function L({ isSignedIn }) {\n  if (!isSignedIn) return <Redirect href=\"/sign-in\" />;\n  return <Stack />;\n}\n",
    'ex/app/routes/(app)/index.tsx': "import { Link } from 'expo-router';\nexport default function I() { return <Link href=\"/sign-in\" />; }\n",
    'ex/app/routes/sign-in.tsx': 'export default function S() { return null; }\n',
    'ex/app/components/Card.tsx': "import { View } from 'react-native';\nexport default function Card() { return <View />; }\n",
    'studio/sanity.config.ts': "import { defineConfig } from 'sanity';\nimport { structureTool } from 'sanity/structure';\nexport default defineConfig({ basePath: '/studio', plugins: [structureTool()] });\n",
    'studio/schemas/post.ts': "export default { name: 'post', title: 'Post', type: 'document', fields: [] };\n",
    'site/app/[...slug]/page.tsx': "const pages = { post: Post, venue: Venue, club: Club };\nexport default function P({ doc }) { const C = pages[doc._type]; return <C />; }\n",
    'fl/pubspec.yaml': 'name: fl\n',
    'fl/lib/router.dart': "final router = GoRouter(routes: [\n  GoRoute(path: '/', builder: (c, s) => const HomePage()),\n  GoRoute(path: '/settings', builder: (c, s) => const SettingsPage()),\n]);\n",
    'droid/app/src/main/AndroidManifest.xml': '<manifest package="com.example.wing"><application><activity android:name=".MainActivity"><intent-filter><category android:name="android.intent.category.LAUNCHER"/></intent-filter></activity></application></manifest>\n',
    'droid/app/src/main/res/navigation/nav.xml': '<navigation xmlns:android="x" xmlns:app="y" android:id="@+id/nav" app:startDestination="@id/list"><fragment android:id="@+id/list" android:name="com.example.wing.ListFragment"><action android:id="@+id/open" app:destination="@id/detail"/></fragment><fragment android:id="@+id/detail" android:name="com.example.wing.DetailFragment"/></navigation>\n',
    'ios/App/Main.storyboard': '<document><scenes><scene><objects><viewController id="a1" customClass="InboxViewController" storyboardIdentifier="Inbox"/></objects></scene></scenes></document>\n',
    'ios/App/InboxViewController.swift': 'import UIKit\nclass InboxViewController: UIViewController {}\nclass DetailViewController: UIViewController {}\n',
  });
  const rn = RN.detect(path.join(root, 'rn'));
  assert.deepEqual(rn.screens.map((s) => [s.name, s.gate]).sort(), [['Home', 'signed-in'], ['Login', 'signed-out']]);
  assert.deepEqual(rn.edges.map((e) => [e.fromFile, e.to]), [['app/Login.tsx', 'Home']]);
  const ex = EXPO.detect(path.join(root, 'ex'));
  assert.deepEqual(ex.roots, ['app/routes'], 'the shallowest _layout is the root, not app/');
  assert.deepEqual(ex.screens.map((s) => [s.route, s.gate]).sort(), [['/', 'signed-in'], ['/sign-in', null]]);
  const st = SANITY.detect(path.join(root, 'studio')).studios[0];
  assert.deepEqual([st.tools.map((t) => t.name), st.pageKinds.map((k) => k.name)], [['structure'], ['post']]);
  assert.deepEqual(DISPATCH.detect(path.join(root, 'site')).dispatchers[0].screens.map((k) => k.type), ['post', 'venue', 'club']);
  assert.deepEqual(FLUTTER.detect(path.join(root, 'fl')).screens.map((s) => s.route), ['/', '/settings']);
  const droid = ANDROID.detect(path.join(root, 'droid'));
  assert.deepEqual(droid.screens.map((s) => s.name).sort(), ['MainActivity', 'detail', 'list']);
  assert.deepEqual(droid.edges.map((e) => [e.from, e.to]), [['list', 'detail']]);
  assert.deepEqual(IOS.detect(path.join(root, 'ios')).screens.map((s) => s.name).sort(), ['DetailViewController', 'InboxViewController']);
  // the orchestrator: which detectors ran, and hints in Floorplan's shape with pseudo URLs for app screens
  const found = screens.detect(root, ['droid', 'ex', 'fl', 'ios', 'rn', 'site', 'studio']);
  const fw = (f) => found.hints.filter((h) => h.fw === f).map((h) => h.url).sort();
  assert.deepEqual(fw('react-navigation'), ['/(Stack)/Home', '/(Stack)/Login']);
  assert.deepEqual(fw('expo-router'), ['/', '/sign-in']);
  assert.deepEqual(fw('sanity-tool'), ['/studio/structure']);
  assert.deepEqual(fw('dispatch'), ['/(club)/*', '/(post)/*', '/(venue)/*']);
  assert.deepEqual(found.expoRoots.get('ex'), ['ex/app/routes']);
  assert.ok(found.rnRepos.has('rn'));
  assert.ok(found.hints.every((h) => !h.url.includes('#')), 'pseudo URLs survive Floorplan\'s normalizeUrl');
});

test("each detector stops reading at its time limit and says so", async () => {
  const text = await import('./coderhelm/lib/text.mjs');
  const root = tree({ 'a/x.ts': 'x', 'a/y.ts': 'y' });
  text.startClock(-1);
  assert.equal(text.read(root, 'a/x.ts'), null);
  assert.deepEqual(text.listFiles(root), []);
  assert.equal(text.stopClock(), true);
  assert.equal(text.read(root, 'a/x.ts'), 'x');
});

// ── Screens a dynamic route dispatches to by type ─────────────────────────

const only = (r, file) => r.dispatchers.find((d) => d.file === file) || assert.fail(`no dispatcher in ${file}: ${r.dispatchers.map((d) => d.file)}`);
const byType = (d) => Object.fromEntries(d.screens.map((s) => [s.type, s]));

test('dispatch: an object map in a catch-all, its renderers followed through imports, barrels, namespaces and lazy imports', () => {
  const root = tree({
    'app/[...path]/page.tsx': [
      "import { resolveAddress } from '../../lib/resolve';",
      "import { TideRenderer } from '../../lib/renderers';",
      "import * as R from '../../lib/more';",
      "import dynamic from 'next/dynamic';",
      "const Gallery = dynamic(() => import('../../components/Gallery'));",
      "const views = {",
      "  tide: TideRenderer,",
      "  'harbour-office': R.HarbourOfficeView,",
      "  gallery: Gallery,",
      "  notice: (p) => <Notice {...p} />,",
      "} satisfies Record<ViewType, unknown>;",
      "function Notice() { return null; }",
      "export default async function Page({ params }) {",
      "  const hit = await resolveAddress(params.path);",
      "  const View = views[hit.type];",
      "  return <View hit={hit} />;",
      "}",
    ].join('\n'),
    'lib/resolve.ts': "export async function resolveAddress(parts) { return { type: 'tide' }; }\n",
    'lib/renderers.ts': "export { TideRenderer } from './tide/TideRenderer';\nexport * from './office';\n",
    'lib/tide/TideRenderer.tsx': "\n\nexport function TideRenderer() {\n  return <p>tide</p>;\n}\n",
    'lib/office.tsx': 'export const HarbourOfficeView = () => null;\n',
    'lib/more.ts': "export { HarbourOfficeView } from './office';\n",
    'components/Gallery.tsx': '\nexport default function Gallery() { return null; }\n',
  });
  const d = only(DISPATCH.detect(root), 'app/[...path]/page.tsx');
  assert.equal(d.route, '/*');
  const t = byType(d);
  assert.deepEqual(Object.keys(t).sort(), ['gallery', 'harbour-office', 'notice', 'tide']);
  assert.deepEqual([t.tide.file, t.tide.line, t.tide.renderer, t.tide.name], ['lib/tide/TideRenderer.tsx', 3, 'TideRenderer', 'Tide page']);
  assert.deepEqual([t['harbour-office'].file, t['harbour-office'].renderer, t['harbour-office'].name], ['lib/office.tsx', 'HarbourOfficeView', 'Harbour office']);
  assert.deepEqual([t.gallery.file, t.gallery.line, t.gallery.renderer], ['components/Gallery.tsx', 2, 'Gallery']);
  assert.deepEqual([t.notice.file, t.notice.renderer], ['app/[...path]/page.tsx', 'Notice']);
  // with no URL in the code, the catch-all's own pattern with the type named
  assert.equal(t.tide.url, '/(tide)/*');
  assert.equal(t.tide.urlFrom, null);
  assert.deepEqual([t.tide.table.file, t.tide.table.how], ['app/[...path]/page.tsx', 'object']);
});

test('dispatch: a switch on the type (with fall-through cases) and an if chain, in a component the route renders', () => {
  const root = tree({
    'pages/[...slug].tsx': "import { Router } from '../components/Router';\nexport default function P(props) { return <Router {...props} />; }\n",
    'components/Router.tsx': [
      "import { Wing } from './Wing';",
      "import { Crew, CrewList } from './Crew';",
      "export function Router({ entry }) {",
      "  switch (entry.kind) {",
      "    case 'wing':",
      "      return <Wing entry={entry} />;",
      "    case 'crew':",
      "    case 'writer':",
      "      return <Crew entry={entry} />;",
      "    case KINDS.CREW_LIST: {",
      "      return <CrewList />;",
      "    }",
      "    default:",
      "      return null;",
      "  }",
      "}",
      "const KINDS = { CREW_LIST: 'crew-list' };",
    ].join('\n'),
    'components/Wing.tsx': 'export function Wing() { return null; }\n',
    'components/Crew.tsx': 'export function Crew() { return null; }\nexport function CrewList() { return null; }\n',
    'routes/$.tsx': "import { Lamp } from '../ui/Lamp';\nimport { Carrel } from '../ui/Carrel';\nexport default function Splat({ data }) {\n  if (data.template === 'lamp') return <Lamp />;\n  if (data.template === 'carrel') {\n    return <Carrel />;\n  }\n  return null;\n}\n",
    'ui/Lamp.tsx': 'export function Lamp() { return null; }\n',
    'ui/Carrel.tsx': 'export function Carrel() { return null; }\n',
    'src/routes/charts/[...rest]/+page.svelte': "<script>\n  import Chart from '$lib/Chart.svelte';\n  import Legend from '$lib/Legend.svelte';\n  export let data;\n  const parts = { chart: Chart, legend: Legend, key: Legend };\n</script>\n<svelte:component this={parts[data.type]} />\n",
  });
  const r = DISPATCH.detect(root);
  const sw = byType(only(r, 'pages/[...slug].tsx'));
  assert.deepEqual(Object.keys(sw).sort(), ['crew', 'crew-list', 'wing', 'writer']);
  assert.deepEqual([sw.wing.file, sw.wing.renderer, sw.wing.table.how], ['components/Wing.tsx', 'Wing', 'switch']);
  // two types one renderer draws are named by their types
  assert.deepEqual([sw.crew.name, sw.writer.name, sw.crew.renderer], ['Crew page', 'Writer page', 'Crew']);
  assert.equal(sw['crew-list'].name, 'Crew list');
  // Remix / React Router flat routes: the splat
  const ifs = only(r, 'routes/$.tsx');
  assert.equal(ifs.route, '/*');
  assert.deepEqual(ifs.screens.map((s) => [s.type, s.file, s.table.how]), [['lamp', 'ui/Lamp.tsx', 'if'], ['carrel', 'ui/Carrel.tsx', 'if']]);
  // SvelteKit: the route's own base stays in front of the type
  const sk = only(r, 'src/routes/charts/[...rest]/+page.svelte');
  assert.equal(sk.route, '/charts/*');
  assert.deepEqual(sk.screens.map((s) => s.url).sort(), ['/charts/(chart)/*', '/charts/(key)/*', '/charts/(legend)/*']);
});

test('dispatch: the URL each type gets, read from the resolver, a path table, a slug builder and a prefix table', () => {
  const root = tree({
    'app/[[...slug]]/page.tsx': "import { find } from '../../lib/find';\nimport { Booth, Club, Event, Join, Pay, Story } from '../../lib/views';\nconst views = { booth: Booth, club: Club, event: Event, join: Join, pay: Pay, story: Story };\nexport default async function P({ params }) { const m = await find(params.slug); const V = views[m.type]; return <V />; }\n",
    'lib/views.tsx': ['Booth', 'Club', 'Event', 'Join', 'Pay', 'Story'].map((n) => `export function ${n}() { return null; }`).join('\n') + '\n',
    'lib/find.ts': [
      "import { pathFor } from './paths';",
      "import { PREFIXES } from './prefixes';",
      "const CLUBS = ['north', 'south'];",
      "export async function find(parts) {",
      "  if (parts.length === 2 && parts[0] === 'members' && parts[1] === 'join') {",
      "    return { type: 'join' };",
      "  }",
      "  if (parts.length === 3 && CLUBS.includes(parts[0]) && parts[1] === 'pay') {",
      "    if (await open(parts[2])) return 'pay';",
      "  }",
      "  return lookup(parts);",
      "}",
    ].join('\n'),
    'lib/paths.ts': "export function pathFor(doc) {\n  switch (doc.type) {\n    case 'booth':\n      return `/booths/${doc.number}`;\n    case 'event':\n      return `/events/${doc.slug}`;\n  }\n}\n",
    'lib/prefixes.ts': "export const PREFIXES = {\n  story: '/stories',\n  club: '/clubs',\n};\n",
  });
  const t = byType(only(DISPATCH.detect(root), 'app/[[...slug]]/page.tsx'));
  assert.deepEqual([t.join.url, t.join.urlFrom.how, t.join.urlFrom.file, t.join.urlFrom.line], ['/members/join', 'resolver', 'lib/find.ts', 5]);
  assert.equal(t.pay.url, '/:club/pay/:segment3');
  assert.deepEqual([t.booth.url, t.booth.urlFrom.how], ['/booths/:number', 'path builder']);
  assert.equal(t.event.url, '/events/:slug');
  assert.deepEqual([t.story.url, t.story.urlFrom.how], ['/stories/*', 'prefix']);
  assert.equal(t.club.url, '/clubs/*');
});

test('dispatch: the pieces on their own (route files, plain names, URL rules)', () => {
  assert.deepEqual(DISPATCH.routeOf('web/app/[locale]/[...slug]/page.tsx'), { url: '/:locale/*', base: '/:locale', last: '*' });
  assert.deepEqual(DISPATCH.routeOf('web/app/(site)/docs/[slug]/page.mdx'), { url: '/docs/:slug', base: '/docs', last: ':slug' });
  assert.deepEqual(DISPATCH.routeOf('web/pages/guides/[...slug].vue'), { url: '/guides/*', base: '/guides', last: '*' });
  assert.deepEqual(DISPATCH.routeOf('src/pages/[...path].astro'), { url: '/*', base: '', last: '*' });
  assert.deepEqual(DISPATCH.routeOf('app/routes/docs.$.tsx'), { url: '/docs/*', base: '/docs', last: '*' });
  assert.deepEqual(DISPATCH.routeOf('app/routes/lockers.$lockerId.tsx'), { url: '/lockers/:lockerId', base: '/lockers', last: ':lockerId' });
  assert.equal(DISPATCH.routeOf('web/app/about/page.tsx'), null, 'a static route dispatches nothing');
  assert.equal(DISPATCH.routeOf('web/app/[...slug]/layout.tsx'), null);
  assert.equal(DISPATCH.routeOf('web/app/api/[...route]/route.ts'), null);
  assert.equal(DISPATCH.plainName('LockerCheckoutRenderer'), 'Locker checkout');
  assert.equal(DISPATCH.plainName('WingRenderer'), 'Wing page');
  assert.equal(DISPATCH.plainName('WingsIndexRenderer'), 'Wings list');
  assert.equal(DISPATCH.plainName('PageRenderer'), 'Content page');
  assert.equal(DISPATCH.plainName('reading-course-index', { fromType: true }), 'Reading course list');
  assert.equal(DISPATCH.segPattern(["slug.startsWith('/news/')"]), '/news/*');
  assert.equal(DISPATCH.segPattern(["parts[0] === 'a' && parts.length >= 2"]), '/a/:segment2/*');
  assert.equal(DISPATCH.segPattern(['user.isAdmin']), null);
  assert.equal(DISPATCH.pathPattern('/clubs/${club.slug}/events/${id}', false), '/clubs/:slug/events/:id');
});

test('dispatch: a route the map already has from a router config (React Router, Vue Router) dispatches too, and blocks are not screens', () => {
  const root = tree({
    'src/AppRoutes.tsx': "import { ContentPage } from './ContentPage';\nexport const routes = [{ path: '/help/*', element: <ContentPage /> }];\n",
    'src/ContentPage.tsx': "import { Faq } from './Faq';\nimport { Guide } from './Guide';\nimport { Hero, Banner, Cards } from './blocks';\nconst pageTypes = { faq: Faq, guide: Guide };\nconst blockMap = { hero: Hero, banner: Banner, cards: Cards };\nexport function ContentPage({ doc }) {\n  const P = pageTypes[doc.type];\n  return <P>{doc.blocks.map((b) => { const B = blockMap[b.blockType]; return <B />; })}</P>;\n}\n",
    'src/Faq.tsx': 'export const Faq = () => null;\n',
    'src/Guide.tsx': 'export const Guide = () => null;\n',
    'src/blocks.tsx': 'export const Hero = () => null;\nexport const Banner = () => null;\nexport const Cards = () => null;\n',
  });
  assert.deepEqual(DISPATCH.detect(root).dispatchers, [], 'no file-based route here');
  const d = only(DISPATCH.detect(root, { routes: [{ file: 'src/ContentPage.tsx', url: '/help/*' }] }), 'src/ContentPage.tsx');
  assert.deepEqual(d.screens.map((s) => [s.type, s.url, s.file]), [['faq', '/help/(faq)/*', 'src/Faq.tsx'], ['guide', '/help/(guide)/*', 'src/Guide.tsx']]);
  assert.deepEqual(d.sections.map((s) => s.type), ['hero', 'banner', 'cards']);
});

test('dispatch: a catch-all over 32 types is 32 screens on the map, each its own renderer, URL and parts', () => {
  const types = Array.from({ length: 32 }, (_, i) => `kind-${String.fromCharCode(97 + (i % 26))}${i >= 26 ? i : ''}`);
  const comp = (t) => t.replace(/(^|-)(\w)/g, (_, __, c) => c.toUpperCase()) + 'Renderer';
  const files = {
    'web/package.json': JSON.stringify({ dependencies: { next: '14.2.5', react: '18.3.1' } }),
    'web/app/[...slug]/page.tsx': `import { resolve } from '../../lib/resolve';\nimport { ${types.map(comp).join(', ')} } from '../../lib/renderers';\nconst renderers = {\n${types.map((t) => `  '${t}': ${comp(t)},`).join('\n')}\n};\nexport default async function P({ params }) { const m = await resolve(params.slug); const R = renderers[m.type]; return <R />; }\n`,
    'web/lib/resolve.ts': "export async function resolve(parts) {\n  if (parts.length === 1 && parts[0] === 'first') return 'kind-a';\n  return null;\n}\n",
    'web/lib/renderers.tsx': types.map((t, i) => `import { Part${i} } from '../components/Part${i}';\nexport function ${comp(t)}() {\n  return <Part${i} />;\n}\n`).join(''),
  };
  types.forEach((_, i) => { files[`web/components/Part${i}.tsx`] = `export function Part${i}() { return null; }\n`; });
  const root = tree(files);
  const found = screens.detect(root, ['web']);
  const hints = found.hints.filter((h) => h.fw === 'dispatch');
  assert.equal(hints.length, 32);
  // merged into a map the way the driver does it: each is a screen, and what is behind it is its own renderer's code
  const graph = {
    files: [
      { p: 'web/app/[...slug]/page.tsx', n: 'web' }, { p: 'web/lib/resolve.ts', n: 'web' },
      { p: 'web/lib/renderers.tsx', n: 'web', s: types.map((t, i) => [comp(t), 'function', 1 + i * 4]) },
      ...types.map((_, i) => ({ p: `web/components/Part${i}.tsx`, n: 'web' })),
    ],
    nodes: [{ id: 'web', kind: 'ui', label: 'Web' }],
    links: types.map((_, i) => ({ a: 'f:web/lib/renderers.tsx', b: `f:web/components/Part${i}.tsx`, k: 'import', names: [`Part${i}`] })),
    routes: [],
    screens: [{ id: 'screen:web:/*', url: '/*', file: 'web/app/[...slug]/page.tsx', app: 'web', fw: 'next-app', name: 'Page', behind: [] }],
  };
  const m = screens.mergeScreens({ graph, scan: { files: [] }, root, found });
  assert.equal(m.added, 32);
  const added = graph.screens.filter((s) => s.fw === 'dispatch');
  assert.equal(new Set(added.map((s) => s.id)).size, 32);
  assert.equal(new Set(added.map((s) => s.name)).size, 32);
  const first = added.find((s) => s.nav.type === 'kind-a');
  assert.deepEqual([first.url, first.kind, first.file, first.nav.route, first.nav.renderer], ['/first', 'screen', 'web/lib/renderers.tsx', '/first', 'KindARenderer']);
  assert.deepEqual(first.nav.evidence.map((e) => [e.file, e.line]), [['web/lib/renderers.tsx', 2], ['web/app/[...slug]/page.tsx', 4], ['web/lib/resolve.ts', 2]]);
  assert.deepEqual(first.behind.map((b) => b.id), ['f:web/lib/renderers.tsx', 'f:web/components/Part0.tsx']);
  const last = added.find((s) => s.nav.type === types[31]);
  assert.deepEqual(last.behind.map((b) => b.id), ['f:web/lib/renderers.tsx', 'f:web/components/Part31.tsx']);
  assert.equal(last.url, `/(${types[31]})/*`);
});
