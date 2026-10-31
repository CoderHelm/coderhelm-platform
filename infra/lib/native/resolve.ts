/**
 * A native template with one install's values put in: what CloudFormation
 * would see for stage `prod` in a given account and region. Used by the
 * IAM simulator script (infra/scripts/deploy-role-sim.py --native) and the
 * tests, never by a release.
 *
 * Fn::Sub, Ref to parameters and pseudo parameters, Fn::If (each condition's
 * value given, else true), Fn::Join of strings; anything else stays as it is.
 * A Ref to an import parameter becomes the Fn::ImportValue it replaced, so
 * tools written for the multi-stack layout read the parts unchanged.
 */
import { Json } from "./package";

export interface ResolveContext {
  values: Record<string, string>;
  conditions?: Record<string, boolean>;
  /** Import parameter -> the export name it replaced. */
  imports?: Record<string, string>;
}

export function resolveValue(v: Json, ctx: ResolveContext): Json {
  if (Array.isArray(v)) return v.map((x) => resolveValue(x, ctx));
  if (!v || typeof v !== "object") return v;
  const keys = Object.keys(v);
  if (keys.length === 1) {
    const [k] = keys;
    const x = v[k];
    if (k === "Ref" && typeof x === "string") {
      if (ctx.imports?.[x]) return { "Fn::ImportValue": ctx.imports[x] };
      if (x in ctx.values) return ctx.values[x];
      return v;
    }
    if (k === "Fn::Sub") {
      const [text, vars] = typeof x === "string" ? [x, {}] : [x[0], resolveValue(x[1], ctx)];
      let unresolved = false;
      const out = String(text).replace(/\$\{([^}]+)\}/g, (all: string, name: string) => {
        if (name.startsWith("!")) return "${" + name.slice(1) + "}";
        if (typeof vars[name] === "string") return vars[name];
        if (name in ctx.values) return ctx.values[name];
        unresolved = true;
        return all;
      });
      return unresolved ? v : out;
    }
    if (k === "Fn::If") {
      const on = ctx.conditions?.[x[0]] ?? true;
      return resolveValue(on ? x[1] : x[2], ctx);
    }
    if (k === "Fn::Join") {
      const parts = resolveValue(x[1], ctx);
      if (Array.isArray(parts) && parts.every((p: Json) => typeof p === "string")) return parts.join(x[0]);
      return { "Fn::Join": [x[0], parts] };
    }
  }
  return Object.fromEntries(Object.entries(v).map(([a, b]) => [a, resolveValue(b, ctx)]));
}

/** The usual values for stage prod. */
export function prodValues(account: string, region: string): Record<string, string> {
  return {
    Stage: "prod",
    "AWS::AccountId": account,
    "AWS::Region": region,
    "AWS::Partition": "aws",
    "AWS::URLSuffix": "amazonaws.com",
    "AWS::StackName": "coderhelm-prod",
    AssetsBucket: `coderhelm-prod-release-${account}`,
    RootStackName: "coderhelm-prod",
    AdminEmail: "owner@example.org",
    EmailFromResolved: "owner@example.org",
    EmailTransport: "ses",
    ModelProvider: "bedrock",
    BedrockPrefixResolved: "us.",
    UpdateDefaultsJson: '{"auto":false,"channel":"stable","day":"sun","hour":3,"timezone":"UTC","majors":"ask"}',
    DeployRoleArn: `arn:aws:iam::${account}:role/coderhelm-prod-deploy`,
    DataDeletionPolicy: "RetainExceptOnCreate",
    DataReplacePolicy: "Retain",
    DomainName: "",
    DomainCertificateArn: "",
    DomainZoneId: "",
  };
}
