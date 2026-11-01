/**
 * The hosted side of hybrid self-hosting (CoderHelm's own deployment only).
 *
 * - The provisioning API (in the gateway) may change records in the parent
 *   zone, and only: CNAMEs, plus the A/AAAA alias that points a name still
 *   being set up at the fallback site, only for names one label under the
 *   zone (and `_<token>.<name>` for certificate validation), never a
 *   reserved name. IAM enforces what it can express; the gateway checks the
 *   rest in code (common::hybrid::names) before every change.
 * - A wildcard `*.<zone>` (A and AAAA only) sends every name without its
 *   own record to a small CloudFront site whose only origin is the
 *   gateway's `/hybrid/fallback` page: "There's no CoderHelm here yet" (404)
 *   or "still being set up" (503), never indexed. Explicit records always
 *   win over a wildcard, so app, api, auth, stream, docs, email and every
 *   claimed name keep answering as before; `_` records and MX/TXT records
 *   are untouched (the wildcard has neither type).
 */
import * as fs from "node:fs";
import * as path from "node:path";
import * as cdk from "aws-cdk-lib";
import * as acm from "aws-cdk-lib/aws-certificatemanager";
import * as cloudfront from "aws-cdk-lib/aws-cloudfront";
import * as origins from "aws-cdk-lib/aws-cloudfront-origins";
import * as iam from "aws-cdk-lib/aws-iam";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as route53 from "aws-cdk-lib/aws-route53";
import * as targets from "aws-cdk-lib/aws-route53-targets";
import * as apigwv2 from "aws-cdk-lib/aws-apigatewayv2";
import { Construct } from "constructs";
import { ResolvedConfig } from "./config";

/** contract/reserved_names.json, shared with the gateway (common::hybrid::names). */
export const RESERVED_NAMES_FILE = path.join(__dirname, "..", "..", "services", "common", "contract", "reserved_names.json");

export function reservedNames(): { zoneRecords: string[]; blocklist: string[] } {
  // Compiled to dist/lib: the contract sits beside services/ either way.
  const candidates = [RESERVED_NAMES_FILE, path.join(__dirname, "..", "..", "..", "services", "common", "contract", "reserved_names.json")];
  const file = candidates.find((f) => fs.existsSync(f));
  if (!file) throw new Error("services/common/contract/reserved_names.json is missing");
  const v = JSON.parse(fs.readFileSync(file, "utf8"));
  return { zoneRecords: v.zone_records, blocklist: v.blocklist };
}

/** The viewer-request code: keep the host and path for the page, ask for the page. */
export const FALLBACK_FUNCTION = `function handler(event) {
  var r = event.request;
  var host = r.headers.host ? r.headers.host.value : "";
  r.headers["x-coderhelm-host"] = { value: host };
  r.headers["x-coderhelm-path"] = { value: r.uri };
  r.uri = "/hybrid/fallback";
  r.querystring = {};
  return r;
}`;

/**
 * What may change records for `{name}.<zone>` (the provisioning API and the
 * name check): CNAME/A/AAAA one label under the zone or `_<token>.<name>`,
 * never the apex, the wildcard, a reserved name or anything deeper.
 */
export function grantNameRecords(fn: lambda.IFunction, zoneArn: string, zone: string): void {
  const reserved = reservedNames();
  fn.addToRolePolicy(
    new iam.PolicyStatement({
      sid: "HybridNameRecords",
      actions: ["route53:ChangeResourceRecordSets"],
      resources: [zoneArn],
      conditions: {
        "ForAllValues:StringEquals": {
          "route53:ChangeResourceRecordSetsRecordTypes": ["CNAME", "A", "AAAA"],
          "route53:ChangeResourceRecordSetsActions": ["CREATE", "UPSERT", "DELETE"],
        },
        // Names one label under the zone, or `_<token>.<name>`.
        "ForAllValues:StringLike": {
          "route53:ChangeResourceRecordSetsNormalizedRecordNames": [`*.${zone}`],
        },
      },
    }),
  );
  // Never the wildcard itself, the zone apex or a reserved name.
  fn.addToRolePolicy(
    new iam.PolicyStatement({
      sid: "HybridNeverReserved",
      effect: iam.Effect.DENY,
      actions: ["route53:ChangeResourceRecordSets"],
      resources: [zoneArn],
      conditions: {
        "ForAnyValue:StringEquals": {
          "route53:ChangeResourceRecordSetsNormalizedRecordNames": [
            zone,
            `\\052.${zone}`,
            `*.${zone}`,
            ...[...reserved.zoneRecords, ...reserved.blocklist].map((n) => `${n}.${zone}`),
          ],
        },
      },
    }),
  );
  // IAM's `*` crosses dots, so the allow above also matches deeper names.
  // A separate deny (conditions inside one statement would AND together)
  // fences what the code never writes: anything under a host CoderHelm serves
  // (such as `_acme-challenge.app.<zone>`, which could get a certificate
  // for the dashboard), mail and certificate-authority records anywhere in
  // the zone, and names three or more labels deep.
  fn.addToRolePolicy(
    new iam.PolicyStatement({
      sid: "HybridNeverDeeper",
      effect: iam.Effect.DENY,
      actions: ["route53:ChangeResourceRecordSets"],
      resources: [zoneArn],
      conditions: {
        "ForAnyValue:StringLike": {
          "route53:ChangeResourceRecordSetsNormalizedRecordNames": [
            `*.*.*.${zone}`,
            "*_domainkey*",
            "*_dmarc*",
            "*_mta-sts*",
            "*_smtp*",
            "*_acme-challenge*",
            "*_spf*",
            ...reserved.zoneRecords.map((n) => `*.${n}.${zone}`),
          ],
        },
      },
    }),
  );
}

export interface HybridProps {
  cfg: ResolvedConfig;
  httpApi: apigwv2.HttpApi;
  gatewayFunction: lambda.Function;
}

export class Hybrid extends Construct {
  public readonly fallback: cloudfront.Distribution;
  /** The `{name}.<zone>` hosted zone (the worker's name check reads it). */
  public readonly zoneId: string;
  public readonly zoneArn: string;

  constructor(scope: Construct, id: string, props: HybridProps) {
    super(scope, id);
    const { cfg } = props;
    const h = cfg.hybrid!;
    const zone = h.zoneId
      ? route53.HostedZone.fromHostedZoneAttributes(this, "Zone", { hostedZoneId: h.zoneId, zoneName: h.zone })
      : route53.HostedZone.fromLookup(this, "Zone", { domainName: h.zone });

    // --- The fallback site for every name without its own record ---
    const cert = new acm.Certificate(this, "WildcardCertificate", {
      domainName: `*.${h.zone}`,
      validation: acm.CertificateValidation.fromDns(zone),
    });
    const fn = new cloudfront.Function(this, "FallbackFunction", {
      functionName: `${cfg.prefix}-hybrid-fallback`,
      code: cloudfront.FunctionCode.fromInline(FALLBACK_FUNCTION),
      runtime: cloudfront.FunctionRuntime.JS_2_0,
    });
    const apiHost = cdk.Fn.select(2, cdk.Fn.split("/", props.httpApi.apiEndpoint));
    const cache = new cloudfront.CachePolicy(this, "FallbackCache", {
      cachePolicyName: `${cfg.prefix}-hybrid-fallback`,
      defaultTtl: cdk.Duration.seconds(60),
      minTtl: cdk.Duration.seconds(0),
      maxTtl: cdk.Duration.minutes(5),
      headerBehavior: cloudfront.CacheHeaderBehavior.allowList("x-coderhelm-host", "x-coderhelm-path"),
      queryStringBehavior: cloudfront.CacheQueryStringBehavior.none(),
      cookieBehavior: cloudfront.CacheCookieBehavior.none(),
    });
    this.fallback = new cloudfront.Distribution(this, "Fallback", {
      comment: `${cfg.prefix} {name}.${h.zone} fallback page`,
      domainNames: [`*.${h.zone}`],
      certificate: cert,
      minimumProtocolVersion: cloudfront.SecurityPolicyProtocol.TLS_V1_2_2021,
      defaultBehavior: {
        origin: new origins.HttpOrigin(apiHost, { protocolPolicy: cloudfront.OriginProtocolPolicy.HTTPS_ONLY }),
        viewerProtocolPolicy: cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
        allowedMethods: cloudfront.AllowedMethods.ALLOW_GET_HEAD,
        cachePolicy: cache,
        originRequestPolicy: cloudfront.OriginRequestPolicy.ALL_VIEWER_EXCEPT_HOST_HEADER,
        functionAssociations: [{ function: fn, eventType: cloudfront.FunctionEventType.VIEWER_REQUEST }],
      },
    });
    const target = route53.RecordTarget.fromAlias(new targets.CloudFrontTarget(this.fallback));
    new route53.ARecord(this, "WildcardA", { zone, recordName: `*.${h.zone}`, target });
    new route53.AaaaRecord(this, "WildcardAaaa", { zone, recordName: `*.${h.zone}`, target });

    // --- What the provisioning API may change in the zone ---
    const zoneArn = cdk.Stack.of(this).formatArn({ service: "route53", account: "", region: "", resource: "hostedzone", resourceName: zone.hostedZoneId });
    this.zoneId = zone.hostedZoneId;
    this.zoneArn = zoneArn;
    const fn_ = props.gatewayFunction;
    grantNameRecords(fn_, zoneArn, h.zone);
    fn_.addEnvironment("HYBRID_ZONE_ID", zone.hostedZoneId);
    fn_.addEnvironment("HYBRID_FALLBACK_DOMAIN", this.fallback.distributionDomainName);

    new cdk.CfnOutput(this, "FallbackDomain", { value: this.fallback.distributionDomainName, description: `The {name}.${h.zone} fallback site` });
  }
}
