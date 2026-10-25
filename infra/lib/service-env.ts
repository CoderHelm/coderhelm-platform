/**
 * What the Rust services (gateway, streaming, worker) learn from the config
 * at runtime: their public URLs, the model provider and the deployment's own
 * AWS account. One function so the three functions can't drift apart.
 */
import * as cdk from "aws-cdk-lib";
import * as iam from "aws-cdk-lib/aws-iam";
import { ResolvedConfig } from "./config";
import { releaseStamp, stampEnvironment } from "./release";

/** Environment variables shared by gateway, streaming and worker. */
export function serviceEnvironment(cfg: ResolvedConfig, account: string): Record<string, string> {
  const env: Record<string, string> = {};
  // Public URLs. Known at synth in the split layout and with a domain; in
  // single-origin without a domain CloudFront picks the hostname, the web
  // stack writes it to SSM, and the services read it there at start.
  if (cfg.appUrl && cfg.apiUrl) {
    env.APP_URL = cfg.appUrl;
    env.API_URL = cfg.apiUrl;
  } else {
    env.PUBLIC_URL_PARAMETER = cfg.publicUrlParameter;
  }
  // The marketing site linked from email footers ("" for none).
  env.SITE_URL = cfg.siteUrl ?? "";
  // The account teams' read-only roles trust, and the template they create
  // them from.
  env.CODERHELM_ACCOUNT_ID = account;
  env.LOG_READER_TEMPLATE_URL = cfg.logReaderTemplateUrl;
  if (cfg.admins.length) env.PLATFORM_ADMIN_EMAILS = cfg.admins.join(",");
  Object.assign(env, modelProviderEnvironment(cfg));
  // Self-hosted installs: which release this is (Settings → System), and
  // the updater the system routes ask. The hosted deployment has neither.
  if (cfg.layout === "single-origin") {
    Object.assign(env, stampEnvironment(releaseStamp(cfg)));
    if (cfg.updates.enabled) env.UPDATER_FUNCTION_NAME = `${cfg.prefix}-updater`;
  }
  // The services read the secret once at start. The installer sets this to
  // the secret's version after it stores new keys (the GitHub App), so the
  // functions restart and read them. Unset in normal deploys.
  if (process.env.CODERHELM_SECRETS_REVISION) {
    env.SECRETS_REVISION = process.env.CODERHELM_SECRETS_REVISION;
  }
  return env;
}

/** MODEL_PROVIDER and the Bedrock model mapping (nothing for the default Anthropic API). */
export function modelProviderEnvironment(cfg: ResolvedConfig): Record<string, string> {
  if (cfg.models.provider !== "bedrock") return {};
  return {
    MODEL_PROVIDER: "bedrock",
    BEDROCK_INFERENCE_PREFIX: cfg.models.bedrockInferencePrefix,
    BEDROCK_MODEL_IDS: JSON.stringify(cfg.models.bedrockModelIds),
  };
}

/** Bedrock access for Claude models when the provider is Bedrock. */
export function modelPolicyStatements(cfg: ResolvedConfig, scope: cdk.Stack): iam.PolicyStatement[] {
  if (cfg.models.provider !== "bedrock") return [];
  return [
    new iam.PolicyStatement({
      actions: ["bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream"],
      resources: [
        // Anthropic models in any region an inference profile routes to,
        // and this account's inference profiles.
        "arn:aws:bedrock:*::foundation-model/anthropic.*",
        `arn:aws:bedrock:*:${scope.account}:inference-profile/*`,
        `arn:aws:bedrock:*:${scope.account}:application-inference-profile/*`,
      ],
    }),
  ];
}

/** Reading the public URL from SSM (single-origin without a domain). */
export function publicUrlReadStatement(cfg: ResolvedConfig, scope: cdk.Stack): iam.PolicyStatement | undefined {
  if (cfg.appUrl && cfg.apiUrl) return undefined;
  return new iam.PolicyStatement({
    actions: ["ssm:GetParameter"],
    resources: [`arn:aws:ssm:${scope.region}:${scope.account}:parameter${cfg.publicUrlParameter}`],
  });
}

/** The record name of `host` inside `zone` ("" for the apex). */
export function recordNameIn(host: string, zone: string): string {
  if (host === zone) return "";
  if (!host.endsWith(`.${zone}`)) throw new Error(`${host} is not inside ${zone}`);
  return host.slice(0, -(zone.length + 1));
}

/** Asking the updater (gateway, when the install has one). */
export function updaterInvokeStatement(cfg: ResolvedConfig, scope: cdk.Stack): iam.PolicyStatement | undefined {
  if (!cfg.updates.enabled) return undefined;
  return new iam.PolicyStatement({
    actions: ["lambda:InvokeFunction"],
    resources: [`arn:aws:lambda:${scope.region}:${scope.account}:function:${cfg.prefix}-updater`],
  });
}
