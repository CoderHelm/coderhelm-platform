/**
 * The config a native release is synthesized from (docs: DESIGN, "How the
 * children are made").
 *
 * A native release is one set of templates for every install, so nothing
 * install-specific may be fixed at synth:
 *
 * - the account is CDK's own token (AWS::AccountId), the region a sentinel;
 * - the install name is a sentinel stage as long as the longest stage, so
 *   every name derived from it fits AWS's limits for any real stage; the
 *   packager (./package.ts) rewrites it to `${Stage}`;
 * - each launch parameter is a `Ref` to a template parameter of the same
 *   name (./params.ts), which the packager declares in the child templates
 *   that use it and feeds from the parent.
 *
 * The values a stack branches on at synth are fixed to the shape every
 * native install has: single-origin, hardened (the parent's boundary), no
 * domain at synth (the Domain custom resource asks the updater for one),
 * Bedrock allowed (the provider is a runtime default, switchable in
 * Settings), data kept by default (the packager makes the policies follow
 * KeepDataOnDelete).
 */
import * as cdk from "aws-cdk-lib";
import { accessNames } from "../access";
import { CoderhelmConfig, ResolvedConfig, resolveConfig, validateConfig } from "../config";

/** 20 characters: the longest stage, so derived names fit for every real one. */
export const STAGE_SENTINEL = "zqxstagesentinelzqx0";
/**
 * The region, as a string CDK treats as a real region (TableV2 replicas
 * refuse a region-agnostic stack); the packager rewrites it to
 * `${AWS::Region}`. Its partition is `aws`, like every supported region.
 */
export const REGION_SENTINEL = "us-zqxregion-9";
/** Where the install's own assets bucket name goes (./package.ts makes it a Ref). */
export const ASSETS_BUCKET_SENTINEL = "zqxassetsbucketsentinelzqx";

/** A Ref to a parent parameter that the packager wires into each child. */
export function param(name: string): string {
  return cdk.Fn.ref(name);
}

/** The native config the stacks see: tokens and the sentinel, validated with stand-ins first. */
export function nativeConfig(opts: { dashboardDir?: string } = {}): ResolvedConfig {
  const raw: CoderhelmConfig = {
    account: "000000000000",
    region: "us-east-1",
    stage: STAGE_SENTINEL,
    domain: null,
    retainData: true,
    models: { provider: "bedrock", bedrockInferencePrefix: "us." },
    email: { from: "native@example.com", identity: "native@example.com" },
    admins: ["native@example.com"],
    integrations: { googleSignIn: false, sandbox: true, logAnalyzer: true, weeklySummary: false },
    telemetry: { enabled: false },
    updates: { enabled: true },
    ...(opts.dashboardDir ? { dashboard: { assetDir: opts.dashboardDir } } : {}),
  };
  const problems = validateConfig(raw);
  if (problems.length) throw new Error(`native config: ${problems.join("; ")}`);

  const account = cdk.Aws.ACCOUNT_ID;
  const region = REGION_SENTINEL;
  const tokenRaw: CoderhelmConfig = { ...raw, account, region };
  const base = resolveConfig(tokenRaw, { skipValidation: true });
  const admin = param("AdminEmail");
  const from = param("EmailFromResolved");
  const prefix = base.prefix;
  return {
    ...base,
    admins: [admin],
    signup: { emails: [admin], domains: [], requireTwoFactor: false },
    email: {
      ...base.email,
      from,
      supportInbox: from,
      identity: from,
      sesRegion: region,
      transport: "ses",
    },
    models: {
      ...base.models,
      provider: "bedrock",
      bedrockInferencePrefix: param("BedrockPrefixResolved"),
    },
    access: {
      owner: "launch",
      // "-stack": the parent's own boundary (infra/lib/native/parent.ts, NATIVE_ID).
      ...accessNames({ account, region, partition: "aws", prefix, suffix: base.suffix, secretName: base.secretName, id: "stack" }),
      // The parent's assets bucket (a Ref after packaging).
      assetsBucket: ASSETS_BUCKET_SENTINEL,
    },
    native: true,
  };
}
