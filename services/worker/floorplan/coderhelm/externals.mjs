// Outside services, found without a model, in order of trust:
//   1. SDK packages in manifests and imports, mapped through a catalog          (code)
//   2. AWS SDK clients, which name the service (boto3.client('sqs'), @aws-sdk/client-sqs) (code)
//   3. hosts in code and config, mapped by the catalog's host suffixes          (code)
//   4. env families (EXAMPLE_API_URL with no catalog entry) become a named service (inferred)
//   5. Floorplan's own scan.json hosts are merged as more evidence
// Unknown hosts are never services: they come back in `otherHosts` (a list, never boxes).
// The team's own endpoints are dropped from the env family: a stem that names one of the team's
// repositories, deployed functions, infra modules or studio tools, or whose sample value is
// localhost or one of the team's own domains.
import fs from 'node:fs';
import path from 'node:path';
import { walk, read, isTest, lineIndex, ev } from './lib/text.mjs';

// vendor: [package prefixes, host suffixes, env stems, kind, label]
export const CATALOG = {
  stripe: [['stripe', '@stripe/'], ['stripe.com'], ['STRIPE'], 'payments', 'Stripe'],
  adyen: [['@adyen/', 'adyen'], ['adyen.com', 'adyenpayments.com'], ['ADYEN'], 'payments', 'Adyen'],
  braintree: [['braintree'], ['braintreegateway.com'], ['BRAINTREE'], 'payments', 'Braintree'],
  paypal: [['@paypal/', 'paypal'], ['paypal.com'], ['PAYPAL'], 'payments', 'PayPal'],
  auth0: [['auth0', '@auth0/'], ['auth0.com'], ['AUTH0'], 'auth', 'Auth0'],
  okta: [['@okta/', 'okta'], ['okta.com', 'oktapreview.com'], ['OKTA'], 'auth', 'Okta'],
  cognito: [['amazon-cognito-identity-js', '@aws-sdk/client-cognito', 'aws-amplify'], ['amazoncognito.com'], ['COGNITO'], 'auth', 'Amazon Cognito'],
  clerk: [['@clerk/'], ['clerk.com', 'clerk.dev'], ['CLERK'], 'auth', 'Clerk'],
  firebase: [['firebase', 'firebase-admin', '@react-native-firebase/'], ['firebaseio.com'], ['FIREBASE'], 'platform', 'Firebase'],
  supabase: [['@supabase/'], ['supabase.co'], ['SUPABASE'], 'platform', 'Supabase'],
  twilio: [['twilio'], ['twilio.com'], ['TWILIO'], 'messaging', 'Twilio'],
  sendgrid: [['@sendgrid/', 'sendgrid'], ['sendgrid.com', 'sendgrid.net'], ['SENDGRID'], 'email', 'SendGrid'],
  mailgun: [['mailgun'], ['mailgun.net'], ['MAILGUN'], 'email', 'Mailgun'],
  postmark: [['postmark'], ['postmarkapp.com'], ['POSTMARK'], 'email', 'Postmark'],
  'customer.io': [['customerio-node', 'customerio-reactnative', 'customerio'], ['customer.io', 'customerio.com'], ['CUSTOMERIO', 'CUSTOMER_IO', 'CIO'], 'marketing', 'Customer.io'],
  zoho: [['zoho'], ['zoho.com', 'zohoapis.com', 'zoho.eu', 'zeptomail.com', 'zohopublic.com', 'zohobookings.com', 'pagesense.io'], ['ZOHO', 'ZEPTO'], 'crm', 'Zoho'],
  salesforce: [['jsforce', 'simple-salesforce'], ['salesforce.com', 'force.com'], ['SALESFORCE', 'SFDC'], 'crm', 'Salesforce'],
  hubspot: [['@hubspot/'], ['hubspot.com', 'hubapi.com'], ['HUBSPOT'], 'crm', 'HubSpot'],
  sanity: [['@sanity/', 'sanity', 'next-sanity'], ['sanity.io'], ['SANITY'], 'cms', 'Sanity'],
  contentful: [['contentful'], ['contentful.com', 'ctfassets.net'], ['CONTENTFUL'], 'cms', 'Contentful'],
  segment: [['@segment/', 'analytics-node'], ['segment.com', 'segment.io'], ['SEGMENT'], 'analytics', 'Segment'],
  heap: [['@heap/', 'heap-api'], ['heapanalytics.com', 'heap.io', 'heap-api.com'], ['HEAP'], 'analytics', 'Heap'],
  mixpanel: [['mixpanel'], ['mixpanel.com'], ['MIXPANEL'], 'analytics', 'Mixpanel'],
  amplitude: [['@amplitude/'], ['amplitude.com'], ['AMPLITUDE'], 'analytics', 'Amplitude'],
  'google-analytics': [['@next/third-parties', 'react-ga'], ['google-analytics.com', 'googletagmanager.com'], ['GA_', 'GTM'], 'analytics', 'Google Analytics'],
  sentry: [['@sentry/', 'sentry-sdk', 'sentry_sdk'], ['sentry.io', 'ingest.sentry.io'], ['SENTRY'], 'monitoring', 'Sentry'],
  datadog: [['dd-trace', '@datadog/', 'datadog'], ['datadoghq.com'], ['DATADOG', 'DD_'], 'monitoring', 'Datadog'],
  newrelic: [['newrelic'], ['newrelic.com', 'nr-data.net'], ['NEW_RELIC', 'NEWRELIC'], 'monitoring', 'New Relic'],
  opensearch: [['@opensearch-project/'], ['es.amazonaws.com', 'aoss.amazonaws.com'], ['OPENSEARCH', 'ELASTICSEARCH'], 'datastore', 'OpenSearch'],
  redis: [['ioredis', 'redis', '@upstash/redis'], ['upstash.io'], ['REDIS'], 'datastore', 'Redis'],
  postgres: [['pg', 'postgres', 'psycopg2', 'psycopg', 'typeorm', '@prisma/client', 'sequelize', 'asyncpg'], [], ['DATABASE', 'POSTGRES', 'PG', 'DB'], 'datastore', 'PostgreSQL'],
  mysql: [['mysql', 'mysql2', 'pymysql', 'mariadb'], [], ['MYSQL', 'MARIADB'], 'datastore', 'MySQL'],
  mongodb: [['mongodb', 'mongoose', 'pymongo'], ['mongodb.net'], ['MONGO'], 'datastore', 'MongoDB'],
  dynamodb: [['@aws-sdk/client-dynamodb', '@aws-sdk/lib-dynamodb'], [], ['DYNAMODB', 'DYNAMO'], 'datastore', 'DynamoDB'],
  s3: [['@aws-sdk/client-s3'], ['s3.amazonaws.com'], ['S3_BUCKET', 'BUCKET'], 'datastore', 'Amazon S3'],
  'secrets-manager': [['@aws-sdk/client-secrets-manager'], [], [], 'platform', 'AWS Secrets Manager'],
  snowflake: [['snowflake-sdk', 'snowflake-connector-python', 'snowflake'], ['snowflakecomputing.com'], ['SNOWFLAKE'], 'datastore', 'Snowflake'],
  redshift: [['redshift_connector'], ['redshift.amazonaws.com'], ['REDSHIFT'], 'datastore', 'Amazon Redshift'],
  shopify: [['@shopify/shopify-api', '@shopify/admin-api-client', '@shopify/storefront-api-client', 'shopify-api-node'], ['myshopify.com', 'shopify.com'], ['SHOPIFY'], 'commerce', 'Shopify'],
  growthbook: [['@growthbook/'], ['growthbook.io'], ['GROWTHBOOK'], 'feature-flags', 'GrowthBook'],
  launchdarkly: [['launchdarkly-', '@launchdarkly/'], ['launchdarkly.com'], ['LAUNCHDARKLY', 'LD_'], 'feature-flags', 'LaunchDarkly'],
  c15t: [['@c15t/', 'c15t'], ['c15t.dev', 'consent.io'], ['C15T'], 'consent', 'c15t'],
  medallia: [[], ['medallia.com'], ['MEDALLIA'], 'surveys', 'Medallia'],
  eventbrite: [['eventbrite'], ['eventbriteapi.com', 'eventbrite.com'], ['EVENTBRITE'], 'ticketing', 'Eventbrite'],
  toast: [[], ['toasttab.com'], ['TOAST'], 'pos', 'Toast'],
  algolia: [['algoliasearch', 'react-instantsearch'], ['algolia.net', 'algolianet.com'], ['ALGOLIA'], 'search', 'Algolia'],
  openai: [['openai'], ['openai.com'], ['OPENAI'], 'ai', 'OpenAI'],
  anthropic: [['@anthropic-ai/', 'anthropic'], ['anthropic.com'], ['ANTHROPIC'], 'ai', 'Anthropic'],
  slack: [['@slack/', 'slack_sdk'], ['slack.com', 'hooks.slack.com'], ['SLACK'], 'messaging', 'Slack'],
  onesignal: [['react-native-onesignal', 'onesignal'], ['onesignal.com'], ['ONESIGNAL'], 'messaging', 'OneSignal'],
  'expo-push': [['expo-notifications', 'expo-server-sdk'], ['exp.host'], [], 'messaging', 'Expo push'],
  'google-maps': [['@react-google-maps/', '@googlemaps/', 'react-native-maps'], ['maps.googleapis.com'], ['GOOGLE_MAPS', 'MAPS_API'], 'maps', 'Google Maps'],
  mapbox: [['mapbox-gl', '@rnmapbox/'], ['mapbox.com'], ['MAPBOX'], 'maps', 'Mapbox'],
  cloudinary: [['cloudinary'], ['cloudinary.com'], ['CLOUDINARY'], 'media', 'Cloudinary'],
  mux: [['@mux/'], ['mux.com'], ['MUX'], 'media', 'Mux'],
  'bond-sports': [[], ['bondsports.co'], ['BOND'], 'booking', 'Bond Sports'],
  sevenrooms: [[], ['sevenrooms.com'], ['SEVEN_ROOMS', 'SEVENROOMS'], 'booking', 'SevenRooms'],
  daysmart: [[], ['daysmartrecreation.com', 'dash-platform.com', 'dashplatform.com'], ['DAYSMART'], 'booking', 'DaySmart'],
  bouncify: [[], ['bouncify.io'], ['BOUNCIFY'], 'email', 'Bouncify'],
  'gc-notify': [['notifications-python-client'], ['notification.canada.ca'], ['NOTIFY'], 'email', 'GC Notify'],
  clamav: [[], [], ['CLAMAV'], 'security', 'ClamAV'],
};

const NOT_SERVICE_HOST = /(^|\.)(w3\.org|schema\.org|example\.(com|org|net)|localhost|github\.com|githubusercontent\.com|npmjs\.(com|org)|mozilla\.org|apple\.com|developer\.|docs\.|fonts\.(googleapis|gstatic)\.com|creativecommons\.org|opensource\.org|apache\.org|json-schema\.org|reactjs\.org|nextjs\.org|vercel\.com|youtube\.com|youtu\.be|vimeo\.com|instagram\.com|facebook\.com|twitter\.com|x\.com|linkedin\.com|tiktok\.com|wikipedia\.org|stackoverflow\.com|amazon\.com|aws\.amazon\.com|docker\.(io|com)|python\.org|nodejs\.org|127\.0\.0\.1|0\.0\.0\.0|test|invalid|local|example)$/;
const PUBLIC_PREFIX = /^(NEXT_PUBLIC_|EXPO_PUBLIC_|PUBLIC_|VITE_|REACT_APP_|SANITY_STUDIO_|NUXT_PUBLIC_|GATSBY_)/;
const URLISH = /_(API|URL|URI|HOST|ENDPOINT|DOMAIN|BASE|BASE_URL|API_URL|API_HOST|SERVER|WEBHOOK_URL)$/;
const GENERIC_STEMS = new Set(('API APP BASE SITE PUBLIC WEB SERVER BACKEND FRONTEND HOST URL NEXT EXPO CDN ASSET ASSETS IMAGE IMAGES MEDIA STATIC AUTH CALLBACK '
  + 'REDIRECT RETURN PREVIEW ORIGIN CORS LOCAL INTERNAL EXTERNAL PROXY GRAPHQL SOCKET WS HEALTH METRICS LOG LOGS AWS LAMBDA SQS SNS QUEUE BUCKET S3 ECS VPC '
  + 'DOMAIN HOSTNAME PORT NODE ENV MAIN DEFAULT SHORTENER UPLOAD DOWNLOAD GET SIGNED SMTP MAIL EMAIL TEST E2E CI GITHUB DATABASE DB REDIS PUT POST DELETE '
  + 'PATCH CSP REPORT SOURCE TARGET DEST DESTINATION VERCEL NETLIFY RENDER HEROKU RAILWAY FLY CF AMPLIFY').split(' '));
const CTX = /fetch|axios|request|http|\.get\(|\.post\(|url|uri|endpoint|base|host|api|src|script|iframe|connect|webview|client/i;
const DEV_FILES = /(^|\/)(\.?mcp\.json|components\.json|turbo\.json|[^/]*postman[^/]*|apollo\.config\.[a-z]+|codegen\.[a-z]+|renovate\.json|[^/]*\.md)$/i;
const ALLOW_FILES = /(^|\/)(csp|content-security|cors|allow-?list)[^/]*$/i;
const ENV_RE = /process\.env\.([A-Z][A-Z0-9_]+)|process\.env\[["']([A-Z][A-Z0-9_]+)|import\.meta\.env\.([A-Z][A-Z0-9_]+)|os\.environ(?:\.get)?\(?\[?\s*["']([A-Z][A-Z0-9_]+)|os\.getenv\(\s*["']([A-Z][A-Z0-9_]+)|(?:configService|config|this\.config|cfg)\.(?:get|getOrThrow)(?:<[^>]*>)?\(\s*["']([A-Z][A-Z0-9_]+)|Variable\.get\(\s*["']([A-Za-z][A-Za-z0-9_]+)|\$env\/(?:static|dynamic)\/(?:public|private)["'][^\n]*?\b([A-Z][A-Z0-9_]+)|Constants\.expoConfig\??\.extra\??\.([A-Z][A-Z0-9_]+)|\bextra\??\.([A-Z][A-Z0-9_]+)|\benv\(\s*["']([A-Z][A-Z0-9_]+)|ENV\[["']([A-Z][A-Z0-9_]+)/g;
const HOST_RE = /https?:\/\/([a-zA-Z0-9][a-zA-Z0-9\-.{}$]*\.[a-zA-Z]{2,})/g;
const IMPORT_RE = /(?:from\s+["']([^"'./][^"']*)["']|require\(\s*["']([^"'./][^"']*)["']\)|^\s*import\s+([a-zA-Z_][\w.]*)|^\s*from\s+([a-zA-Z_][\w.]*)\s+import)/gm;
const AWS_RE = /boto3\.(?:client|resource)\(\s*["']([a-z0-9-]+)|@aws-sdk\/client-([a-z0-9-]+)|new\s+AWS\.([A-Z][A-Za-z0-9]+)\(|aws-sdk\/clients\/(\w+)/g;
const AWS_KEY = { dynamodb: 'dynamodb', s3: 's3', secretsmanager: 'secrets-manager', documentclient: 'dynamodb', cognitoidentityprovider: 'cognito' };
export const CODE_EXT = new Set(['.ts', '.tsx', '.js', '.jsx', '.mjs', '.cjs', '.py', '.go', '.java', '.kt', '.rb', '.php', '.cs', '.rs', '.svelte', '.vue']);
const SCAN_EXT = new Set([...CODE_EXT, '.env', '.example', '.sample', '.json', '.yml', '.yaml', '.toml', '.txt', '.cfg', '.ini']);

export function registrable(h) {
  const parts = h.toLowerCase().split(':')[0].split('.');
  if (parts.length >= 3 && ['co', 'com', 'org', 'net', 'gov'].includes(parts[parts.length - 2]) && parts[parts.length - 1].length === 2) return parts.slice(-3).join('.');
  return parts.slice(-2).join('.');
}

export function stemOf(name) {
  const n = name.replace(PUBLIC_PREFIX, '').replace(/_(API_URL|API_HOST|BASE_URL|WEBHOOK_URL|API|URL|URI|HOST|ENDPOINT|DOMAIN|BASE|SERVER|KEY|SECRET|TOKEN|ID|USERNAME|USER|PASSWORD|PASS)$/, '');
  return n.split('_').filter(Boolean);
}

export function vendorForPackage(p) {
  for (const [v, [pk]] of Object.entries(CATALOG)) {
    for (const x of pk) if (p === x || (x.endsWith('/') && p.startsWith(x)) || p.startsWith(x + '/') || (x.endsWith('-') && p.startsWith(x))) return v;
  }
  return null;
}

export function vendorForHost(h) {
  for (const [v, [, hs]] of Object.entries(CATALOG)) for (const x of hs) if (h === x || h.endsWith('.' + x)) return v;
  return null;
}

export function vendorForStem(toks) {
  if (!toks.length) return null;
  for (const [v, [, , st]] of Object.entries(CATALOG)) {
    for (const s of st) { const s2 = s.replace(/_+$/, '').split('_'); if (s2.every((x, i) => toks[i] === x)) return v; }
  }
  return null;
}

/** The team's own names: repository folders, folders of deployed functions and infra modules,
 *  plus anything the caller knows (studio tools, IaC unit names). Tokens of 4+ letters. */
export function ownTokens(root, repos, extra = []) {
  const toks = new Set();
  const add = (s) => { for (const t of String(s).toLowerCase().split(/[^a-z0-9]+/)) if (t.length >= 4) toks.add(t); };
  for (const r of repos) add(r);
  for (const x of extra) add(x);
  // folders that hold the team's own deployables: <repo>/(lambdas|functions|services|modules)/<name>,
  // one and two levels down (terraform/modules/x, infra/lambdas/x)
  const kids = (d) => { try { return fs.readdirSync(path.join(root, d), { withFileTypes: true }).filter((e) => e.isDirectory()).map((e) => e.name); } catch { return []; } };
  for (const r of repos) {
    for (const a of kids(r)) {
      const scan = (dir, depth) => { for (const b of kids(dir)) { if (/^(lambdas?|functions?|services|modules)$/i.test(b)) for (const c of kids(`${dir}/${b}`)) add(c); else if (depth < 2) scan(`${dir}/${b}`, depth + 1); } };
      if (/^(lambdas?|functions?|services|modules)$/i.test(a)) for (const c of kids(`${r}/${a}`)) add(c);
      else scan(`${r}/${a}`, 1);
    }
  }
  return toks;
}

/**
 * @param root   the map root (one folder per repository)
 * @param repos  repository folder names
 * @param scan   Floorplan's scan.json (hosts are merged), optional
 * @param own    extra own names (studio tools, IaC unit names)
 * @returns { services: {key: {...}}, otherHosts: [...], ownDomains: [...] }
 */
export function findServices(root, repos, { scan = null, own = [] } = {}) {
  const ownToks = ownTokens(root, repos, own);
  const nodes = new Map();
  const node = (k) => { if (!nodes.has(k)) nodes.set(k, { packages: new Set(), hosts: new Set(), env: new Set(), evidence: [], how: new Set(), repos: new Set() }); return nodes.get(k); };
  const ownDomains = new Set();
  const files = [];
  for (const repo of repos) for (const f of walk(path.join(root, repo), SCAN_EXT)) files.push(`${repo}/${f}`);
  const iac = [];
  for (const repo of repos) for (const f of walk(path.join(root, repo), new Set(['.tf', '.hcl']))) iac.push(`${repo}/${f}`);
  for (const rel of iac) {
    for (const m of (read(root, rel) || '').matchAll(/(?:domain_name|name|aliases|zone_name|domain)\s*=\s*\[?\s*"([a-z0-9\-.]+\.[a-z]{2,})"/g)) {
      if (!/\.(js|mjs|py|ts|json|zip|sh|txt|yml|yaml)$/.test(m[1])) ownDomains.add(registrable(m[1]));
    }
  }
  const envReads = new Map();
  const envValues = new Map();
  const addEnv = (k, site) => { if (!envReads.has(k)) envReads.set(k, []); envReads.get(k).push(site); };
  for (const rel of files) {
    if (isTest(rel) || rel.includes('.github/') || /(package-lock\.json|yarn\.lock|pnpm-lock\.yaml|tsconfig\.json)$/.test(rel)) continue;
    const repo = rel.split('/')[0];
    const t = read(root, rel); if (t == null) continue;
    const L = lineIndex(t);
    const base = path.basename(rel);
    if (base === 'package.json') {
      let pj; try { pj = JSON.parse(t); } catch { continue; }
      for (const sec of ['dependencies', 'peerDependencies']) for (const p of Object.keys(pj?.[sec] || {})) {
        const v = vendorForPackage(p); if (!v) continue;
        const n = node(v); n.packages.add(p); n.how.add('code'); n.repos.add(repo); n.evidence.push([rel, L(Math.max(0, t.indexOf(`"${p}"`)))]);
      }
      continue;
    }
    if (/^requirements.*\.txt$|^pyproject\.toml$|^Pipfile$/.test(base)) {
      for (const m of t.matchAll(/^\s*([A-Za-z0-9_\-[\]]+)\s*[=<>~!]/gm)) {
        const p = m[1].toLowerCase().split('[')[0]; const v = vendorForPackage(p); if (!v) continue;
        const n = node(v); n.packages.add(p); n.how.add('code'); n.repos.add(repo); n.evidence.push([rel, L(m.index)]);
      }
      continue;
    }
    const envFile = base.startsWith('.env') || /\.(example|sample)$/.test(base) || base === 'sample.env' || base === 'env.example';
    if (envFile) {
      for (const m of t.matchAll(/^\s*(?:export\s+)?([A-Z][A-Z0-9_]+)\s*=\s*["']?([^\n"']*)/gm)) {
        addEnv(m[1], [rel, L(m.index), repo]);
        if (m[2]) { if (!envValues.has(m[1])) envValues.set(m[1], []); envValues.get(m[1]).push(m[2]); }
      }
    }
    if (CODE_EXT.has(path.extname(rel))) {
      for (const m of t.matchAll(ENV_RE)) addEnv(m.slice(1).find(Boolean), [rel, L(m.index), repo]);
      for (const m of t.matchAll(AWS_RE)) {
        const svc = m.slice(1).find(Boolean).toLowerCase().replace(/-/g, '');
        if (/command$|client$|^\w{1,2}$/.test(svc)) continue;
        const key = AWS_KEY[svc] || 'aws:' + svc;
        const n = node(key); n.packages.add(m[0].slice(0, 40)); n.how.add('code'); n.repos.add(repo); n.evidence.push([rel, L(m.index)]);
      }
      for (const m of t.matchAll(IMPORT_RE)) {
        const p = m.slice(1).find(Boolean) || '';
        const v = vendorForPackage(p.includes('/') ? p : p.split('.')[0]) || vendorForPackage(p);
        if (!v) continue;
        const n = node(v); n.packages.add(p); n.how.add('code'); n.repos.add(repo); n.evidence.push([rel, L(m.index)]);
      }
    }
    const lines = t.split('\n');
    for (const m of t.matchAll(HOST_RE)) {
      const h = m[1].toLowerCase();
      if (h.includes('{') || h.includes('$') || NOT_SERVICE_HOST.test(h) || ownDomains.has(registrable(h)) || h.endsWith('.internal') || h.endsWith('amazonaws.com')) continue;
      const ln = L(m.index);
      const line = lines[ln - 1] || '';
      if (/^\s*(\/\/|#|\*|\/\*)/.test(line) || line.includes('href=') || line.includes('$schema')) continue;
      const v = vendorForHost(h);
      if (!v && (DEV_FILES.test(rel) || ALLOW_FILES.test(rel) || !CTX.test(line.replace(m[0], '')))) continue;
      const n = node(v || 'host:' + registrable(h)); n.hosts.add(h); n.how.add(v ? 'code' : 'inferred'); n.repos.add(repo); n.evidence.push([rel, ln]);
    }
  }
  // env families
  const ownValue = (k) => (envValues.get(k) || []).some((v) => /localhost|127\.0\.0\.1|^\//.test(v) || [...ownDomains].some((d) => v.includes(d)));
  const dropped = [];
  for (const [k, sites] of envReads) {
    const toks = stemOf(k);
    const v = vendorForStem(toks);
    if (v) {
      const n = node(v); n.env.add(k); n.how.add(n.packages.size || n.hosts.size ? 'code' : 'inferred');
      n.evidence.push(...sites.slice(0, 3).map(([f, l]) => [f, l])); for (const s of sites) n.repos.add(s[2]);
      continue;
    }
    if (!URLISH.test(k.replace(PUBLIC_PREFIX, '')) || !toks.length) continue;
    const st = toks[0];
    if (GENERIC_STEMS.has(st) || st.length < 3) continue;
    // the team's own endpoints are not outside services
    if (ownToks.has(st.toLowerCase()) || ownValue(k)) { dropped.push(k); continue; }
    const n = node('env:' + st.toLowerCase()); n.env.add(k); n.how.add('inferred');
    for (const s of sites) n.repos.add(s[2]);
    n.evidence.push(...sites.slice(0, 3).map(([f, l]) => [f, l]));
  }
  if (scan) {
    for (const h of scan.hosts || []) {
      const hh = String(h.host || '').toLowerCase();
      if (!hh || NOT_SERVICE_HOST.test(hh) || ownDomains.has(registrable(hh)) || isTest(h.from || '')) continue;
      const v = vendorForHost(hh);
      const n = node(v || 'host:' + registrable(hh)); n.hosts.add(hh); n.how.add(v ? 'code' : 'inferred'); n.evidence.push([h.from, h.line || 1]); n.repos.add(String(h.from).split('/')[0]);
    }
  }
  // a host of an env-named service joins it (example.vendor.com + VENDOR_API)
  for (const k of [...nodes.keys()].filter((x) => x.startsWith('host:'))) {
    const stem = k.slice(5).split('.')[0];
    const e = nodes.get('env:' + stem); if (!e) continue;
    const h = nodes.get(k);
    for (const x of h.hosts) e.hosts.add(x); for (const x of h.repos) e.repos.add(x); for (const x of h.how) e.how.add(x);
    e.evidence.push(...h.evidence);
    nodes.delete(k);
  }
  const services = {}, otherHosts = [];
  for (const [k, n] of nodes) {
    const evidence = [...new Map(n.evidence.filter(([f]) => f && !isTest(f)).map(([f, l]) => [`${f}:${l}`, [f, l]])).values()].sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : a[1] - b[1])).slice(0, 12);
    if (!evidence.length) continue;
    if (k.startsWith('host:')) { otherHosts.push({ host: k.slice(5), hosts: [...n.hosts].sort().slice(0, 8), evidence: evidence.slice(0, 4).map(([f, l]) => ev(f, l, 'inferred', 0.2)) }); continue; }
    const cat = CATALOG[k];
    const how = k.startsWith('env:') ? 'env' : k.startsWith('aws:') ? 'sdk' : n.packages.size && [...n.packages].some((p) => /^@aws-sdk|boto3|aws-sdk/.test(p)) ? 'sdk' : 'catalog';
    const code = n.how.has('code') && !k.startsWith('env:');
    const confidence = code && cat ? 0.95 : k.startsWith('aws:') ? 0.9 : k.startsWith('env:') ? (n.repos.size > 1 ? 0.7 : 0.6) : 0.6;
    services[k] = {
      key: k,
      vendor: k.startsWith('env:') ? k.slice(4) : k.startsWith('aws:') ? 'aws' : k,
      label: cat?.[4] || (k.startsWith('aws:') ? 'AWS ' + k.slice(4) : k.slice(4).replace(/(^|[-_])(\w)/g, (_, a, c) => (a ? ' ' : '') + c.toUpperCase())),
      category: cat?.[3] || (k.startsWith('aws:') ? 'cloud' : 'service'),
      how,
      inferred: !code,
      packages: [...n.packages].sort().slice(0, 8),
      hosts: [...n.hosts].sort().slice(0, 8),
      env: [...n.env].sort().slice(0, 12),
      repos: [...n.repos].sort(),
      evidence: evidence.map(([f, l]) => ev(f, l, code ? 'code' : 'inferred', confidence)),
    };
  }
  otherHosts.sort((a, b) => (a.host < b.host ? -1 : 1));
  return { services, otherHosts, ownDomains: [...ownDomains].sort(), dropped: dropped.sort() };
}
