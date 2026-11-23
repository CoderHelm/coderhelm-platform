// CoderHelm's detectors (coderhelm/) on small made-up sources, one folder per case:
// outside services, calls through wrappers and GraphQL, infra to code (Terraform,
// CloudFormation/SAM, Serverless, CDK, deploy workflows), webhooks, data links and screens
// (React Navigation, Expo Router at its real root, Sanity, CMS page kinds, Flutter, Android,
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
import * as CMS from './coderhelm/screens/cms-routes.mjs';
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
    'api/src/slip.entity.ts': "@Entity('slip')\nexport class Slip {}\n",
    'etl/dags/load.py': 'def run(h):\n    h.run("INSERT INTO slip (id) VALUES (1)")\n    h.run("SELECT * FROM slip")\n',
  });
  const routes = [{ m: 'POST', p: '/webhooks/stripe', f: 'api/src/hooks.ts', l: 2 }, { m: 'POST', p: '/api/events', f: 'api/src/events.ts' }, { m: 'GET', p: '/webhooks/stripe/status', f: 'api/src/hooks.ts' }];
  const w = webhooks(root, routes, {});
  assert.deepEqual(w.map((x) => [x.vendor, x.route, x.signature]), [['stripe', 0, true]]);
  const d = dataLinks(root, ['api', 'etl']);
  assert.deepEqual(d.links.map((l) => [l.file, l.entity, l.mode, l.table]), [['etl/dags/load.py', 'api/src/slip.entity.ts', 'write', 'slip']]);
});

test('screens: React Navigation gates, Expo Router at its real root, Sanity, CMS page kinds, Flutter, Android, iOS', () => {
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
    'droid/app/src/main/AndroidManifest.xml': '<manifest package="com.example.dock"><application><activity android:name=".MainActivity"><intent-filter><category android:name="android.intent.category.LAUNCHER"/></intent-filter></activity></application></manifest>\n',
    'droid/app/src/main/res/navigation/nav.xml': '<navigation xmlns:android="x" xmlns:app="y" android:id="@+id/nav" app:startDestination="@id/list"><fragment android:id="@+id/list" android:name="com.example.dock.ListFragment"><action android:id="@+id/open" app:destination="@id/detail"/></fragment><fragment android:id="@+id/detail" android:name="com.example.dock.DetailFragment"/></navigation>\n',
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
  assert.deepEqual(CMS.detect(path.join(root, 'site')).catchAlls[0].pageKinds.map((k) => k.name), ['post', 'venue', 'club']);
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
  assert.deepEqual(fw('cms-page-kind'), ['/(club)', '/(post)', '/(venue)']);
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
