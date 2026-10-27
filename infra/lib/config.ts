/**
 * The deployment config: one JSON file holds everything that differs between
 * one CoderHelm deployment and another (account, region, names, domain, model
 * provider, email, integrations). Nothing deployment-specific lives in the
 * stacks; the hosted deployment is `config/hosted.prod.json`, and a
 * self-hosted one is whatever `coderhelm.config.json` the installer wrote.
 *
 * `loadConfig` reads and validates the file; `resolveConfig` turns it into the
 * values the stacks use (names, URLs, environment variables). Validation
 * collects every problem and reports them together, so a person fixes their
 * file once instead of one error per synth.
 */
import * as fs from "fs";
import * as path from "path";

export type ModelProvider = "anthropic" | "bedrock";

/**
 * Where the dashboard, the API and the chat stream are served from.
 *
 * - `single-origin` (the self-host default): one CloudFront distribution
 *   serves the dashboard and forwards /api, /auth, /webhooks, /email and
 *   /integrations to the API and the chat stream to its function. One
 *   hostname (yours, or CloudFront's default `*.cloudfront.net` name when no
 *   domain is set), so sign-in cookies are first-party and no CORS is needed.
 * - `split`: the dashboard, API, stream and sign-in each on their own
 *   hostname (the hosted deployment; the dashboard is deployed separately).
 */
export type Layout = "single-origin" | "split";

export interface SingleOriginDomain {
  /** The hostname people open, e.g. `coderhelm.example.com`. */
  hostname: string;
  /**
   * The Route 53 public hosted zone the hostname is in, e.g. `example.com`.
   * With it, certificates are validated and DNS records are created for you.
   * Without it, give `certificateArn` and create the DNS record yourself
   * (the deploy prints it).
   */
  hostedZone?: string;
  /** The zone's id, to skip the lookup (and the lookup role) at synth. */
  hostedZoneId?: string;
  /** An ACM certificate in us-east-1 covering `hostname` (CloudFront needs us-east-1). */
  certificateArn?: string;
}

export interface SplitLayout {
  /** Route 53 zone for the custom domains below; null when there are none (local development). */
  zone: string | null;
  /** The dashboard's URL (deployed separately in this layout). */
  appUrl: string;
  /** The API's public URL. */
  apiUrl: string;
  /** Custom domain for the API (needs `zone`). */
  apiDomain?: string | null;
  /** Custom domain for the chat stream (needs `zone`). */
  streamDomain?: string | null;
  /** Custom domain for the sign-in pages (needs `zone`). */
  authDomain?: string | null;
  /** Origins the API answers cross-origin requests from. */
  corsOrigins: string[];
  /** Origins the chat stream answers cross-origin requests from. */
  streamCorsOrigins: string[];
}

export interface HybridConfig {
  /**
   * The parent zone, e.g. `coderhelm.com`. Moving customer addresses to a
   * separate domain later is this setting (and its zone), not a code change.
   */
  zone: string;
  /** The zone's id, to skip the lookup at synth. */
  zoneId?: string;
}

export interface CoderhelmConfig {
  $schema?: string;
  /** The 12-digit AWS account to deploy to. */
  account: string;
  /** The AWS region to deploy to. */
  region: string;
  /** A short name for this deployment (lowercase letters, digits, hyphens). */
  stage: string;
  /** Prefix of every resource name. Default `coderhelm-<stage>`. */
  resourcePrefix?: string;
  /**
   * Added to the names that must be unique across all of AWS (S3 buckets,
   * the sign-in domain prefix). Default `-<account>`; `""` for none.
   */
  globalNameSuffix?: string;
  /** Default `single-origin`. */
  layout?: Layout;
  /** single-origin only: your own hostname, or null/absent for CloudFront's default URL. */
  domain?: SingleOriginDomain | null;
  /** split only. */
  split?: SplitLayout;
  /**
   * The hosted side of hybrid self-hosting (CoderHelm's own deployment only):
   * the zone `{name}.<zone>` addresses live in, the provisioning API's Route
   * 53 rights there, and the fallback page every unclaimed name shows.
   */
  hybrid?: HybridConfig | null;
  /**
   * Self-hosted installs: CoderHelm's hosted API, used only when an admin
   * turns on a hybrid part (a CoderHelm address, CoderHelm's apps).
   * Default `https://api.coderhelm.com`; null turns hybrid off entirely.
   */
  hybridUrl?: string | null;
  /**
   * Keep tables, buckets and keys when a stack is deleted, and turn on
   * deletion protection. Default true.
   */
  retainData?: boolean;
  models?: {
    /** `anthropic` (each team adds its own API key) or `bedrock` (this AWS account's Bedrock). Default `anthropic`. */
    provider?: ModelProvider;
    /** Model for the main work (implementation, reviews). */
    primary?: string;
    /** Model for lighter work (triage, plan chat, log analysis). */
    light?: string;
    /** bedrock only: inference profile prefix such as `us.`, `eu.` or `apac.`; `""` for in-region model ids. Default `us.`. */
    bedrockInferencePrefix?: string;
    /** bedrock only: exact Bedrock ids for model names, e.g. {"claude-haiku-4-5": "us.anthropic.claude-haiku-4-5-20251001-v1:0"}. */
    bedrockModelIds?: Record<string, string>;
  };
  email?: {
    /** The address emails come from. Must be in a verified SES identity. */
    from: string;
    /** Display name. Default `CoderHelm`. */
    fromName?: string;
    /** Where support requests from the dashboard go. Default: `from`. */
    supportInbox?: string;
    /**
     * The SES identity to create: a domain (`example.com`) or an address.
     * Default: the domain of `from`. null to manage the identity yourself.
     */
    identity?: string | null;
    /** Region of the SES identity. Default: `region`. */
    sesRegion?: string;
    /**
     * Send sign-up and password emails through SES (needs the identity
     * verified before the first deploy). Default false: Amazon Cognito sends
     * them from its own address, limited to 50 a day.
     */
    cognitoUsesSes?: boolean;
    /** Logo shown in emails. Default `<app url>/favicon.svg`. */
    logoUrl?: string;
    /** Privacy policy link in email footers. Default: none. */
    privacyUrl?: string | null;
    /**
     * Self-hosted installs: how email leaves until an admin changes it in
     * Settings, Emails. `ses` (default): this install's own Amazon SES.
     * `coderhelm`: CoderHelm's mail service (hybrid; needs the install
     * registered with CoderHelm).
     */
    transport?: "ses" | "coderhelm";
  };
  /** People with admin rights over the whole deployment (the first one is you). */
  admins?: string[];
  /**
   * Who may create an account (admins always may). On a self-hosted install
   * this is the first boot's list: the addresses become invites and the
   * domains the allowed domains of the one organization, which admins change
   * after that in Settings, Sign-in. Empty means only people the admins
   * invite.
   */
  signup?: {
    /** Exact addresses. */
    emails?: string[];
    /** Whole email domains, e.g. `example.com`. */
    domains?: string[];
    /** Start with two-factor sign-in required for everyone. Default false. */
    requireTwoFactor?: boolean;
  };
  integrations?: {
    /** "Sign in with Google" (needs google_client_id/secret in the secret and a domain). Default false. */
    googleSignIn?: boolean;
    /** The build sandbox that runs a repo's checks (CodeBuild). Default true. */
    sandbox?: boolean;
    /** The scheduled log analyzer for connected AWS accounts. Default true. */
    logAnalyzer?: boolean;
    /**
     * The Monday summary email. Hosted deployment: default true. Self-hosted
     * installs: default false, the starting value until an install admin
     * turns it on or off in Settings, Emails.
     */
    weeklySummary?: boolean;
    /** While true, only teams with a rollout list get the Monday summary. Default false. */
    weeklySummaryListedTeamsOnly?: boolean;
  };
  /** single-origin only: the dashboard this deployment serves. */
  dashboard?: {
    /** A built dashboard (static export) to upload. The installer fills it in. */
    assetDir?: string;
  };
  /**
   * Updates (single-origin only). The updater runs in this account: it
   * checks for releases hourly, emails admins about them, and applies them
   * when auto-update allows. Admins change these from Settings → System;
   * the values here are the starting point.
   */
  updates?: {
    /** Create the updater (Lambda, CodeBuild project, hourly check). Default true. */
    enabled?: boolean;
    /** Apply new releases on their own. Default false (admins are told and update with one click). */
    auto?: boolean;
    /** `stable`, or `preview` for release candidates too. Default `stable`. */
    channel?: "stable" | "preview";
    /** Maintenance window: `"Sun 03:00"`, `"daily 02:00"`. Default `"Sun 03:00"`. */
    window?: string;
    /** IANA time zone of the window. Default `UTC`. */
    timezone?: string;
    /** `ask`: majors wait for an admin; `auto`: majors apply too. Default `ask`. */
    majors?: "ask" | "auto";
    /** A Slack or Teams incoming webhook for update notices, besides email to admins. */
    webhookUrl?: string;
  };
  /** Anonymous install and upgrade pings from the installer. Off unless set to true. */
  telemetry?: { enabled?: boolean; endpoint?: string };
  /**
   * The CloudFormation template teams use to connect an AWS account for log
   * reading. Default: the public copy CoderHelm publishes.
   */
  logReaderTemplateUrl?: string;
  /** Create the public bucket that serves the log reader template (hosted deployment). */
  publicBucketName?: string | null;
  /** The marketing site linked from emails, or null. */
  siteUrl?: string | null;
  /** Hosted deployment only: CI roles that exist for the hosted repository. */
  ci?: {
    /** `owner/repo` whose main branch may run the nightly reviewer evals on Bedrock. */
    reviewerEvalsRepo?: string;
  };
}

/** The values the stacks use, derived once from the config. */
export interface ResolvedConfig {
  raw: CoderhelmConfig;
  account: string;
  region: string;
  stage: string;
  /** Prefix of every resource name, e.g. `coderhelm-prod`. */
  prefix: string;
  /** Suffix for globally unique names (buckets, sign-in domain prefix). */
  suffix: string;
  layout: Layout;
  retainData: boolean;
  /**
   * The public URLs when they are known at synth. null in single-origin
   * without a domain: CloudFront picks the hostname, the web stack stores it
   * in SSM (`publicUrlParameter`) and the services read it at start.
   */
  appUrl: string | null;
  apiUrl: string | null;
  /** SSM parameter holding the public URL (single-origin). */
  publicUrlParameter: string;
  /**
   * SSM parameter holding CloudFront's own hostname (`dxxxx.cloudfront.net`,
   * single-origin): the value of the DNS record people add for their own
   * domain. The updater reads it (docs/self-host/domains.md).
   */
  distributionDomainParameter: string;
  singleOrigin: SingleOriginDomain | null;
  split: SplitLayout | null;
  /** The hosted side of hybrid, or null (every self-hosted install). */
  hybrid: HybridConfig | null;
  /** Self-hosted: the hosted API hybrid parts talk to; null when turned off. */
  hybridUrl: string | null;
  models: {
    provider: ModelProvider;
    primary: string;
    light: string;
    bedrockInferencePrefix: string;
    bedrockModelIds: Record<string, string>;
  };
  email: {
    from: string;
    fromName: string;
    supportInbox: string;
    identity: string | null;
    sesRegion: string;
    cognitoUsesSes: boolean;
    logoUrl: string | null;
    privacyUrl: string | null;
    transport: "ses" | "coderhelm";
  };
  /**
   * The hosted side of CoderHelm's mail relay (only with `hybrid`): its own
   * sending subdomain, so relayed mail never touches the main domain's
   * reputation. null everywhere else.
   */
  mailRelay: { domain: string; from: string } | null;
  admins: string[];
  signup: { emails: string[]; domains: string[]; requireTwoFactor: boolean };
  integrations: Required<NonNullable<CoderhelmConfig["integrations"]>>;
  dashboardAssetDir: string | null;
  updates: {
    enabled: boolean;
    webhookUrl: string | null;
    /** The updater's starting settings (lambda/updater/handler.py). */
    defaults: { auto: boolean; channel: "stable" | "preview"; day: string; hour: number; timezone: string; majors: "ask" | "auto" };
  };
  /** The config's own template URL; null for the default (see service-env.ts, logReaderTemplateUrlFor). */
  logReaderTemplateUrl: string | null;
  publicBucketName: string | null;
  siteUrl: string | null;
  reviewerEvalsRepo: string | null;
  /** Secrets Manager names. */
  secretName: string;
  anthropicAdminSecretName: string;
  /** Data bucket name. */
  dataBucketName: string;
  sandboxBucketName: string;
  cognitoDomainPrefix: string;
}

export class ConfigError extends Error {
  constructor(public readonly problems: string[]) {
    super(`Invalid CoderHelm config:\n  - ${problems.join("\n  - ")}`);
    this.name = "ConfigError";
  }
}

/**
 * The public bucket CloudFormation's Launch Stack links open templates from
 * (they only open templates stored in S3).
 */
export const PUBLIC_TEMPLATE_BUCKET = "coderhelm-public";

/** The hosted deployment's copy of the log reader template (follows main). */
export const DEFAULT_LOG_READER_TEMPLATE_URL =
  `https://${PUBLIC_TEMPLATE_BUCKET}.s3.amazonaws.com/cfn/coderhelm-log-reader.yaml`;

/**
 * A self-hosted release's own copy of the log reader template, published by
 * the release workflow next to its Launch Stack template, byte for byte the
 * one built into that release (services/common/src/log_reader.rs).
 */
export function releaseLogReaderTemplateUrl(version: string): string {
  return `https://${PUBLIC_TEMPLATE_BUCKET}.s3.amazonaws.com/launch/${version}/coderhelm-log-reader.yaml`;
}

const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const HOST_RE = /^(?=.{1,253}$)([a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}$/;
const REGION_RE = /^[a-z]{2}(-gov)?-[a-z]+-\d$/;
const URL_RE = /^https?:\/\/[^\s/]+(\/[^\s]*)?$/;
const KNOWN_TOP_KEYS = new Set([
  "$schema", "account", "region", "stage", "resourcePrefix", "globalNameSuffix", "layout",
  "domain", "split", "hybrid", "hybridUrl", "retainData", "models", "email", "admins", "signup", "integrations",
  "dashboard", "telemetry", "logReaderTemplateUrl", "publicBucketName", "siteUrl", "ci", "updates",
]);

/** Every problem with a config, empty when it is valid. */
export function validateConfig(c: unknown): string[] {
  const p: string[] = [];
  if (typeof c !== "object" || c === null || Array.isArray(c)) {
    return ["the config must be a JSON object"];
  }
  const cfg = c as Record<string, unknown> & CoderhelmConfig;
  for (const k of Object.keys(cfg)) {
    if (!KNOWN_TOP_KEYS.has(k)) p.push(`unknown setting "${k}" (check the spelling)`);
  }
  if (typeof cfg.account !== "string" || !/^\d{12}$/.test(cfg.account)) {
    p.push("account: must be a 12-digit AWS account id, as a string");
  }
  if (typeof cfg.region !== "string" || !REGION_RE.test(cfg.region)) {
    p.push("region: must be an AWS region such as us-east-1");
  }
  if (typeof cfg.stage !== "string" || !/^[a-z][a-z0-9-]{0,19}$/.test(cfg.stage)) {
    p.push("stage: 1-20 characters, lowercase letters, digits and hyphens, starting with a letter");
  }
  if (cfg.resourcePrefix !== undefined && !/^[a-z][a-z0-9-]{1,30}$/.test(String(cfg.resourcePrefix))) {
    p.push("resourcePrefix: 2-31 characters, lowercase letters, digits and hyphens");
  }
  if (cfg.globalNameSuffix !== undefined && !/^(-[a-z0-9]{1,20})?$/.test(String(cfg.globalNameSuffix))) {
    p.push('globalNameSuffix: "" or a hyphen followed by up to 20 lowercase letters or digits');
  }
  const layout = cfg.layout ?? "single-origin";
  if (layout !== "single-origin" && layout !== "split") {
    p.push('layout: "single-origin" or "split"');
  }
  if (layout === "single-origin") {
    if (cfg.split !== undefined) p.push('split: only used with layout "split"');
    const d = cfg.domain;
    if (d !== undefined && d !== null) {
      if (typeof d !== "object") {
        p.push("domain: an object with hostname (and hostedZone or certificateArn), or null");
      } else {
        if (typeof d.hostname !== "string" || !HOST_RE.test(d.hostname)) {
          p.push("domain.hostname: a hostname such as coderhelm.example.com (lowercase)");
        }
        if (d.hostedZone !== undefined) {
          if (typeof d.hostedZone !== "string" || !HOST_RE.test(d.hostedZone)) {
            p.push("domain.hostedZone: a zone name such as example.com");
          } else if (
            typeof d.hostname === "string" &&
            d.hostname !== d.hostedZone &&
            !d.hostname.endsWith(`.${d.hostedZone}`)
          ) {
            p.push(`domain.hostname: ${d.hostname} is not inside the zone ${d.hostedZone}`);
          }
        }
        if (d.hostedZoneId !== undefined && !/^Z[A-Z0-9]{1,32}$/.test(String(d.hostedZoneId))) {
          p.push("domain.hostedZoneId: a Route 53 zone id such as Z0123456789ABC");
        }
        if (d.hostedZoneId !== undefined && d.hostedZone === undefined) {
          p.push("domain.hostedZoneId: also set domain.hostedZone");
        }
        if (d.certificateArn !== undefined) {
          if (!/^arn:aws[a-z-]*:acm:us-east-1:\d{12}:certificate\/[0-9a-f-]+$/.test(String(d.certificateArn))) {
            p.push("domain.certificateArn: an ACM certificate ARN in us-east-1 (CloudFront only uses us-east-1 certificates)");
          }
        }
        if (d.hostedZone === undefined && d.certificateArn === undefined) {
          p.push("domain: set hostedZone (Route 53 manages the certificate and DNS) or certificateArn (you manage DNS)");
        }
      }
    }
  } else if (layout === "split") {
    const s = cfg.split;
    if (!s || typeof s !== "object") {
      p.push('split: required with layout "split"');
    } else {
      if (typeof s.appUrl !== "string" || !URL_RE.test(s.appUrl)) p.push("split.appUrl: a URL");
      if (typeof s.apiUrl !== "string" || !URL_RE.test(s.apiUrl)) p.push("split.apiUrl: a URL");
      if (!Array.isArray(s.corsOrigins)) p.push("split.corsOrigins: a list of origins");
      if (!Array.isArray(s.streamCorsOrigins)) p.push("split.streamCorsOrigins: a list of origins");
      for (const k of ["apiDomain", "streamDomain", "authDomain"] as const) {
        const v = s[k];
        if (v !== undefined && v !== null) {
          if (typeof v !== "string" || !HOST_RE.test(v)) p.push(`split.${k}: a hostname`);
          if (!s.zone) p.push(`split.${k}: needs split.zone`);
        }
      }
    }
    if (cfg.domain !== undefined && cfg.domain !== null) {
      p.push('domain: only used with layout "single-origin" (use split.* for the split layout)');
    }
    if (cfg.dashboard?.assetDir) {
      p.push('dashboard.assetDir: the split layout does not serve the dashboard (it is deployed separately)');
    }
  }
  if (cfg.hybrid !== undefined && cfg.hybrid !== null) {
    const h = cfg.hybrid;
    if (layout !== "split") p.push("hybrid: only the hosted deployment (layout split) serves hybrid installs");
    if (typeof h !== "object" || typeof h.zone !== "string" || !HOST_RE.test(h.zone)) p.push("hybrid.zone: a zone name such as coderhelm.com");
    if (h.zoneId !== undefined && !/^Z[A-Z0-9]{5,32}$/.test(String(h.zoneId))) p.push("hybrid.zoneId: a Route 53 zone id such as Z0123456789ABC");
    // The fallback site's wildcard certificate must be in us-east-1 for CloudFront.
    if (cfg.region !== "us-east-1") p.push("hybrid: deploy the hosted side in us-east-1 (CloudFront certificates live there)");
  }
  if (cfg.hybridUrl !== undefined && cfg.hybridUrl !== null && (typeof cfg.hybridUrl !== "string" || !/^https:\/\/[^\s/]+$/.test(cfg.hybridUrl))) {
    p.push("hybridUrl: an https origin such as https://api.coderhelm.com, or null to turn hybrid off");
  }

  const m = cfg.models ?? {};
  if (m.provider !== undefined && m.provider !== "anthropic" && m.provider !== "bedrock") {
    p.push('models.provider: "anthropic" or "bedrock"');
  }
  for (const k of ["primary", "light"] as const) {
    if (m[k] !== undefined && (typeof m[k] !== "string" || !/^[a-z0-9][a-z0-9.:_-]{2,120}$/.test(m[k] as string))) {
      p.push(`models.${k}: a model id such as claude-sonnet-5-5`);
    }
  }
  if (m.bedrockInferencePrefix !== undefined && !/^([a-z]{2,4}\.)?$/.test(String(m.bedrockInferencePrefix))) {
    p.push('models.bedrockInferencePrefix: "", "us.", "eu.", "apac." and so on');
  }
  if (m.bedrockModelIds !== undefined) {
    if (typeof m.bedrockModelIds !== "object" || m.bedrockModelIds === null) {
      p.push("models.bedrockModelIds: an object of model name to Bedrock id");
    } else {
      for (const [k, v] of Object.entries(m.bedrockModelIds)) {
        if (typeof v !== "string" || !v.includes("anthropic.")) {
          p.push(`models.bedrockModelIds.${k}: a Bedrock Anthropic model or inference profile id`);
        }
      }
    }
  }

  const e = cfg.email;
  if (!e || typeof e !== "object") {
    p.push("email.from: required (the address emails are sent from)");
  } else {
    if (typeof e.from !== "string" || !EMAIL_RE.test(e.from)) p.push("email.from: an email address");
    if (e.supportInbox !== undefined && !EMAIL_RE.test(String(e.supportInbox))) {
      p.push("email.supportInbox: an email address");
    }
    if (e.identity !== undefined && e.identity !== null) {
      const id = String(e.identity);
      const ok = EMAIL_RE.test(id) || HOST_RE.test(id);
      if (!ok) p.push("email.identity: a domain or an email address, or null");
      else if (typeof e.from === "string" && EMAIL_RE.test(e.from)) {
        const fromDomain = e.from.split("@")[1];
        if (EMAIL_RE.test(id) ? id !== e.from : fromDomain !== id && !fromDomain.endsWith(`.${id}`)) {
          p.push(`email.identity: ${id} does not cover the sender ${e.from}`);
        }
      }
    }
    if (e.sesRegion !== undefined && !REGION_RE.test(String(e.sesRegion))) {
      p.push("email.sesRegion: an AWS region");
    }
    for (const k of ["logoUrl", "privacyUrl"] as const) {
      const v = e[k];
      if (v !== undefined && v !== null && !URL_RE.test(String(v))) p.push(`email.${k}: a URL`);
    }
    if (e.transport !== undefined && e.transport !== "ses" && e.transport !== "coderhelm") {
      p.push('email.transport: "ses" or "coderhelm"');
    }
  }

  if (cfg.admins !== undefined) {
    if (!Array.isArray(cfg.admins) || cfg.admins.some((a) => typeof a !== "string" || !EMAIL_RE.test(a))) {
      p.push("admins: a list of email addresses");
    }
  }
  if (layout === "single-origin" && (!Array.isArray(cfg.admins) || cfg.admins.length === 0)) {
    p.push("admins: add at least one email address (the first admin)");
  }
  if (cfg.signup !== undefined) {
    const s = cfg.signup;
    if (s.emails !== undefined && (!Array.isArray(s.emails) || s.emails.some((x) => !EMAIL_RE.test(String(x))))) {
      p.push("signup.emails: a list of email addresses");
    }
    if (s.domains !== undefined && (!Array.isArray(s.domains) || s.domains.some((x) => !HOST_RE.test(String(x).toLowerCase())))) {
      p.push("signup.domains: a list of email domains such as example.com");
    }
    if (s.requireTwoFactor !== undefined && typeof s.requireTwoFactor !== "boolean") {
      p.push("signup.requireTwoFactor: true or false");
    }
  }
  const i = cfg.integrations ?? {};
  for (const [k, v] of Object.entries(i)) {
    if (typeof v !== "boolean") p.push(`integrations.${k}: true or false`);
  }
  if (i.googleSignIn && layout === "single-origin" && !cfg.domain) {
    p.push("integrations.googleSignIn: needs a domain (Google requires a fixed sign-in callback URL)");
  }
  if (cfg.telemetry !== undefined) {
    if (cfg.telemetry.enabled !== undefined && typeof cfg.telemetry.enabled !== "boolean") {
      p.push("telemetry.enabled: true or false");
    }
    if (cfg.telemetry.endpoint !== undefined && !/^https:\/\//.test(String(cfg.telemetry.endpoint))) {
      p.push("telemetry.endpoint: an https URL");
    }
  }
  if (cfg.updates !== undefined) {
    const u = cfg.updates;
    if (typeof u !== "object" || u === null) p.push("updates: an object");
    else {
      for (const k of ["enabled", "auto"] as const) {
        if (u[k] !== undefined && typeof u[k] !== "boolean") p.push(`updates.${k}: true or false`);
      }
      if (u.channel !== undefined && u.channel !== "stable" && u.channel !== "preview") p.push('updates.channel: "stable" or "preview"');
      if (u.majors !== undefined && u.majors !== "ask" && u.majors !== "auto") p.push('updates.majors: "ask" or "auto"');
      if (u.window !== undefined && !parseWindow(String(u.window))) p.push('updates.window: a day and time such as "Sun 03:00" or "daily 02:00"');
      if (u.timezone !== undefined && !/^(UTC|[A-Za-z_]+(\/[A-Za-z0-9_+-]+)+)$/.test(String(u.timezone))) p.push("updates.timezone: an IANA time zone such as Europe/Berlin");
      if (u.webhookUrl !== undefined && !/^https:\/\/\S+$/.test(String(u.webhookUrl))) p.push("updates.webhookUrl: an https URL");
      if (layout === "split" && u.enabled) p.push('updates.enabled: only for layout "single-origin" (the hosted deployment deploys from CI)');
    }
  }
  if (cfg.logReaderTemplateUrl !== undefined && !/^https:\/\/[^\s]+\.ya?ml$/.test(String(cfg.logReaderTemplateUrl))) {
    p.push("logReaderTemplateUrl: an https URL of a .yaml template in S3");
  }
  if (cfg.publicBucketName !== undefined && cfg.publicBucketName !== null &&
      !/^[a-z0-9][a-z0-9.-]{2,62}$/.test(String(cfg.publicBucketName))) {
    p.push("publicBucketName: an S3 bucket name");
  }
  if (cfg.siteUrl !== undefined && cfg.siteUrl !== null && !URL_RE.test(String(cfg.siteUrl))) {
    p.push("siteUrl: a URL or null");
  }
  if (cfg.ci?.reviewerEvalsRepo !== undefined && !/^[\w.-]+\/[\w.-]+$/.test(String(cfg.ci.reviewerEvalsRepo))) {
    p.push("ci.reviewerEvalsRepo: owner/repo");
  }

  // Names that must fit AWS limits once built.
  if (p.length === 0) {
    const r = resolveConfig(cfg, { skipValidation: true });
    if (r.dataBucketName.length > 63 || r.sandboxBucketName.length > 63 || `${r.prefix}-dashboard${r.suffix}`.length > 63) {
      p.push("resourcePrefix + globalNameSuffix: bucket names would be over 63 characters; shorten them");
    }
    if (r.cognitoDomainPrefix.length > 63 || /aws|amazon|cognito/.test(r.cognitoDomainPrefix)) {
      p.push('resourcePrefix + globalNameSuffix: the sign-in domain prefix must be under 64 characters and must not contain "aws", "amazon" or "cognito"');
    }
  }
  return p;
}

/** The config's values with every default filled in. */
export function resolveConfig(cfg: CoderhelmConfig, opts: { skipValidation?: boolean } = {}): ResolvedConfig {
  if (!opts.skipValidation) {
    const problems = validateConfig(cfg);
    if (problems.length) throw new ConfigError(problems);
  }
  const prefix = cfg.resourcePrefix ?? `coderhelm-${cfg.stage}`;
  const suffix = cfg.globalNameSuffix ?? `-${cfg.account}`;
  const layout: Layout = cfg.layout ?? "single-origin";
  const singleOrigin = layout === "single-origin" ? cfg.domain ?? null : null;
  const split = layout === "split" ? cfg.split ?? null : null;

  let appUrl: string | null;
  let apiUrl: string | null;
  if (split) {
    appUrl = split.appUrl;
    apiUrl = split.apiUrl;
  } else if (singleOrigin) {
    appUrl = `https://${singleOrigin.hostname}`;
    apiUrl = appUrl;
  } else {
    appUrl = null;
    apiUrl = null;
  }

  const m = cfg.models ?? {};
  const e = cfg.email ?? { from: "" };
  const fromDomain = e.from.includes("@") ? e.from.split("@")[1] : "";
  const i = cfg.integrations ?? {};
  const admins = cfg.admins ?? [];

  return {
    raw: cfg,
    account: cfg.account,
    region: cfg.region,
    stage: cfg.stage,
    prefix,
    suffix,
    layout,
    retainData: cfg.retainData ?? true,
    appUrl,
    apiUrl,
    publicUrlParameter: `/${prefix}/public-url`,
    distributionDomainParameter: `/${prefix}/distribution-domain`,
    singleOrigin,
    split,
    hybrid: cfg.hybrid ? { zone: cfg.hybrid.zone.toLowerCase(), ...(cfg.hybrid.zoneId ? { zoneId: cfg.hybrid.zoneId } : {}) } : null,
    hybridUrl: layout === "split" ? null : cfg.hybridUrl === undefined ? "https://api.coderhelm.com" : cfg.hybridUrl,
    models: {
      provider: m.provider ?? "anthropic",
      primary: m.primary ?? "claude-opus-5-5",
      light: m.light ?? "claude-sonnet-5-5",
      bedrockInferencePrefix: m.bedrockInferencePrefix ?? "us.",
      bedrockModelIds: m.bedrockModelIds ?? {},
    },
    email: {
      from: e.from,
      fromName: e.fromName ?? "CoderHelm",
      supportInbox: e.supportInbox ?? e.from,
      identity: e.identity === undefined ? fromDomain || null : e.identity,
      sesRegion: e.sesRegion ?? cfg.region,
      cognitoUsesSes: e.cognitoUsesSes ?? false,
      logoUrl: e.logoUrl ?? null,
      privacyUrl: e.privacyUrl ?? null,
      transport: e.transport ?? "ses",
    },
    mailRelay: cfg.hybrid
      ? {
          domain: `notify.${cfg.hybrid.zone.toLowerCase()}`,
          from: `notifications@notify.${cfg.hybrid.zone.toLowerCase()}`,
        }
      : null,
    admins,
    signup: {
      emails: [...new Set([...admins, ...(cfg.signup?.emails ?? [])].map((x) => x.toLowerCase()))],
      domains: [...new Set((cfg.signup?.domains ?? []).map((x) => x.toLowerCase()))],
      requireTwoFactor: cfg.signup?.requireTwoFactor === true,
    },
    integrations: {
      googleSignIn: i.googleSignIn ?? false,
      sandbox: i.sandbox ?? true,
      logAnalyzer: i.logAnalyzer ?? true,
      // Optional email is opt-in on a self-hosted install, on as before for
      // the hosted deployment (split layout).
      weeklySummary: i.weeklySummary ?? layout === "split",
      weeklySummaryListedTeamsOnly: i.weeklySummaryListedTeamsOnly ?? false,
    },
    dashboardAssetDir: cfg.dashboard?.assetDir ?? null,
    updates: (() => {
      const u = cfg.updates ?? {};
      const w = parseWindow(u.window ?? "Sun 03:00") ?? { day: "sun", hour: 3 };
      return {
        enabled: layout === "single-origin" && u.enabled !== false,
        webhookUrl: u.webhookUrl ?? null,
        defaults: {
          auto: u.auto ?? false,
          channel: u.channel ?? "stable",
          day: w.day,
          hour: w.hour,
          timezone: u.timezone ?? "UTC",
          majors: u.majors ?? "ask",
        },
      };
    })(),
    logReaderTemplateUrl: cfg.logReaderTemplateUrl ?? null,
    publicBucketName: cfg.publicBucketName ?? null,
    siteUrl: cfg.siteUrl ?? null,
    reviewerEvalsRepo: cfg.ci?.reviewerEvalsRepo ?? null,
    secretName: `${secretPath(cfg.stage, prefix)}/secrets`,
    anthropicAdminSecretName: `${secretPath(cfg.stage, prefix)}/anthropic-admin-keys`,
    dataBucketName: `${prefix}-data${suffix}`,
    sandboxBucketName: `${prefix}-sandbox${suffix}`,
    cognitoDomainPrefix: `${prefix}${suffix}`,
  };
}

/** `"Sun 03:00"` → { day: "sun", hour: 3 }; `"daily 02:00"` → { day: "daily", hour: 2 }. */
export function parseWindow(w: string): { day: string; hour: number } | null {
  const m = w.trim().toLowerCase().match(/^(mon|tue|wed|thu|fri|sat|sun|daily)[a-z]*\s+(\d{1,2}):00$/);
  if (!m) return null;
  const hour = Number(m[2]);
  return hour <= 23 ? { day: m[1], hour } : null;
}

/** `coderhelm/<stage>` for the default prefix, else the prefix itself. */
function secretPath(stage: string, prefix: string): string {
  return prefix === `coderhelm-${stage}` ? `coderhelm/${stage}` : prefix;
}

/** Reads a config file (JSON), substituting `${ENV}` references, and resolves it. */
export function loadConfigFile(file: string, env: NodeJS.ProcessEnv = process.env): ResolvedConfig {
  const text = fs.readFileSync(file, "utf8");
  let parsed: unknown;
  try {
    parsed = JSON.parse(substituteEnv(text, env));
  } catch (err) {
    throw new ConfigError([`${file} is not valid JSON: ${(err as Error).message}`]);
  }
  return resolveConfig(parsed as CoderhelmConfig);
}

/** `${NAME}` in the file is replaced by the environment variable NAME (missing ones are an error). */
export function substituteEnv(text: string, env: NodeJS.ProcessEnv): string {
  const missing: string[] = [];
  const out = text.replace(/\$\{([A-Z0-9_]+)\}/g, (_, name: string) => {
    const v = env[name];
    if (v === undefined) {
      missing.push(name);
      return "";
    }
    return v;
  });
  if (missing.length) {
    throw new ConfigError(missing.map((n) => `environment variable ${n} is used in the config but not set`));
  }
  return out;
}

/**
 * Which config file to use: `-c config=<path>`, else CODERHELM_CONFIG, else
 * the hosted deployment's files (`config/hosted.prod.json` for stage prod,
 * `config/local.json` with the stage filled in for any other stage).
 */
export function chooseConfigFile(opts: {
  contextConfig?: string;
  envConfig?: string;
  stage?: string;
  infraDir: string;
}): { file: string; stage?: string } {
  const pick = opts.contextConfig || opts.envConfig;
  if (pick) return { file: path.resolve(pick) };
  const stage = opts.stage ?? "prod";
  if (stage === "prod") return { file: path.join(opts.infraDir, "config", "hosted.prod.json") };
  return { file: path.join(opts.infraDir, "config", "local.json"), stage };
}
