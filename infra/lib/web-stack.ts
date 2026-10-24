/**
 * Single-origin web front door (self-hosted deployments).
 *
 * One CloudFront distribution serves the dashboard from S3 and forwards the
 * service paths to their backends, all on one hostname:
 *
 *   /api/plans/chat/stream   → the streaming function URL (SSE)
 *   /api/* /auth/* /webhooks/* /email/* /integrations/*   → the HTTP API
 *   everything else          → the dashboard (static export in S3)
 *
 * One hostname means the session cookie (`__Host-`, SameSite=Strict) is
 * first-party and no CORS is needed. Without a custom domain the hostname is
 * CloudFront's own `dxxxx.cloudfront.net`; it is only known after the first
 * deploy, so it is written to SSM (`cfg.publicUrlParameter`) for the
 * services to read at start.
 */
import * as fs from "fs";
import * as path from "path";
import * as cdk from "aws-cdk-lib";
import * as acm from "aws-cdk-lib/aws-certificatemanager";
import * as apigatewayv2 from "aws-cdk-lib/aws-apigatewayv2";
import * as cloudfront from "aws-cdk-lib/aws-cloudfront";
import * as origins from "aws-cdk-lib/aws-cloudfront-origins";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as route53 from "aws-cdk-lib/aws-route53";
import * as route53Targets from "aws-cdk-lib/aws-route53-targets";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as s3deploy from "aws-cdk-lib/aws-s3-deployment";
import * as ssm from "aws-cdk-lib/aws-ssm";
import { Construct } from "constructs";
import { ResolvedConfig } from "./config";
import { recordNameIn } from "./service-env";

/** Paths the HTTP API serves (each also matches its sub-paths). */
export const API_PATHS = ["/api/*", "/auth/*", "/webhooks/*", "/email/*", "/integrations/*"];
/** The chat stream (SSE, served by the streaming function). */
export const STREAM_PATH = "/api/plans/chat/stream";

/** Hashed build files: cached for a year. */
const ASSET_CACHE_CONTROL = "public, max-age=31536000, immutable";
/** Pages and the runtime config: revalidated on every visit. */
const PAGE_CACHE_CONTROL = "public, max-age=0, s-maxage=60, must-revalidate";

/**
 * Viewer-request code for the dashboard: `/runs/detail` and `/runs/detail/`
 * are `/runs/detail/index.html` in the static export.
 */
export const STATIC_PATHS_FUNCTION = `
function handler(event) {
  var request = event.request;
  var uri = request.uri;
  if (uri.endsWith('/')) {
    request.uri = uri + 'index.html';
  } else if (uri.lastIndexOf('.') < uri.lastIndexOf('/')) {
    request.uri = uri + '/index.html';
  }
  return request;
}
`;

/** The page shown until a dashboard build is uploaded. */
const PLACEHOLDER_HTML = `<!doctype html><html><head><meta charset="utf-8"><title>CoderHelm</title>
<meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="font-family:system-ui,sans-serif;background:#0a0a0a;color:#ddd;padding:48px">
<h1>CoderHelm is deployed</h1>
<p>The dashboard has not been uploaded yet. Run <code>coderhelm-selfhost deploy</code>
(it downloads the dashboard for this release), or set <code>dashboard.assetDir</code>
in your config to a dashboard build and deploy again.</p></body></html>`;

/** What the dashboard reads at start (public/runtime-config.js in its build). */
export function runtimeConfigScript(cfg: ResolvedConfig): string {
  const value = {
    // Same origin: the dashboard calls /api/... on its own hostname.
    apiUrl: "",
    streamUrl: "",
    selfHosted: true,
    supportEmail: cfg.email.supportInbox,
    siteUrl: cfg.siteUrl,
  };
  return `window.__CODERHELM_CONFIG__ = ${JSON.stringify(value)};\n`;
}

export interface WebStackProps extends cdk.StackProps {
  cfg: ResolvedConfig;
  httpApi: apigatewayv2.HttpApi;
  streamingFunctionUrl: lambda.FunctionUrl;
}

export class WebStack extends cdk.Stack {
  /** `https://<hostname>`: the custom domain, or CloudFront's own name. */
  public readonly publicUrl: string;
  public readonly distribution: cloudfront.Distribution;

  constructor(scope: Construct, id: string, props: WebStackProps) {
    super(scope, id, props);
    const cfg = props.cfg;
    const prefix = cfg.prefix;
    const domain = cfg.singleOrigin;

    // --- Dashboard bucket (private; CloudFront reads it through OAC) ---
    const bucket = new s3.Bucket(this, "DashboardBucket", {
      bucketName: `${prefix}-dashboard${cfg.suffix}`,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
      enforceSSL: true,
      // The build is re-uploaded on every deploy, so it never needs keeping.
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });

    // --- Certificate and DNS (custom domain only) ---
    let certificate: acm.ICertificate | undefined;
    let zone: route53.IHostedZone | undefined;
    if (domain) {
      if (domain.hostedZone) {
        zone = domain.hostedZoneId
          ? route53.HostedZone.fromHostedZoneAttributes(this, "Zone", {
              zoneName: domain.hostedZone,
              hostedZoneId: domain.hostedZoneId,
            })
          : route53.HostedZone.fromLookup(this, "Zone", { domainName: domain.hostedZone });
      }
      if (domain.certificateArn) {
        certificate = acm.Certificate.fromCertificateArn(this, "Certificate", domain.certificateArn);
      } else if (cdk.Stack.of(this).region === "us-east-1") {
        certificate = new acm.Certificate(this, "Certificate", {
          domainName: domain.hostname,
          validation: acm.CertificateValidation.fromDns(zone),
        });
      } else {
        // CloudFront only uses certificates from us-east-1.
        const certStack = new cdk.Stack(scope, `${id}-certificate`, {
          env: { account: cfg.account, region: "us-east-1" },
          crossRegionReferences: true,
        });
        const certZone = domain.hostedZoneId
          ? route53.HostedZone.fromHostedZoneAttributes(certStack, "Zone", {
              zoneName: domain.hostedZone!,
              hostedZoneId: domain.hostedZoneId,
            })
          : route53.HostedZone.fromLookup(certStack, "Zone", { domainName: domain.hostedZone! });
        const cert = new acm.Certificate(certStack, "Certificate", {
          domainName: domain.hostname,
          validation: acm.CertificateValidation.fromDns(certZone),
        });
        this.addDependency(certStack);
        certificate = acm.Certificate.fromCertificateArn(this, "Certificate", cert.certificateArn);
      }
    }

    // --- Origins ---
    const apiDomain = `${props.httpApi.apiId}.execute-api.${this.region}.${this.urlSuffix}`;
    const apiOrigin = new origins.HttpOrigin(apiDomain, {
      protocolPolicy: cloudfront.OriginProtocolPolicy.HTTPS_ONLY,
      originSslProtocols: [cloudfront.OriginSslPolicy.TLS_V1_2],
    });
    const streamDomain = cdk.Fn.select(2, cdk.Fn.split("/", props.streamingFunctionUrl.url));
    const streamOrigin = new origins.HttpOrigin(streamDomain, {
      protocolPolicy: cloudfront.OriginProtocolPolicy.HTTPS_ONLY,
      originSslProtocols: [cloudfront.OriginSslPolicy.TLS_V1_2],
      readTimeout: cdk.Duration.seconds(60),
    });
    const passThrough: Omit<cloudfront.BehaviorOptions, "origin"> = {
      viewerProtocolPolicy: cloudfront.ViewerProtocolPolicy.HTTPS_ONLY,
      allowedMethods: cloudfront.AllowedMethods.ALLOW_ALL,
      cachePolicy: cloudfront.CachePolicy.CACHING_DISABLED,
      // Cookies, the Authorization header and query strings reach the API;
      // Host is the API's own (API Gateway routes on it).
      originRequestPolicy: cloudfront.OriginRequestPolicy.ALL_VIEWER_EXCEPT_HOST_HEADER,
      compress: false,
    };

    const securityHeaders = new cloudfront.ResponseHeadersPolicy(this, "SecurityHeaders", {
      responseHeadersPolicyName: `${prefix}-web-security-headers`,
      securityHeadersBehavior: {
        strictTransportSecurity: {
          accessControlMaxAge: cdk.Duration.days(365),
          includeSubdomains: false,
          override: true,
        },
        contentTypeOptions: { override: true },
        frameOptions: { frameOption: cloudfront.HeadersFrameOption.DENY, override: true },
        referrerPolicy: {
          referrerPolicy: cloudfront.HeadersReferrerPolicy.STRICT_ORIGIN_WHEN_CROSS_ORIGIN,
          override: true,
        },
      },
    });

    const staticPaths = new cloudfront.Function(this, "StaticPaths", {
      functionName: `${prefix}-web-paths`,
      code: cloudfront.FunctionCode.fromInline(STATIC_PATHS_FUNCTION),
      runtime: cloudfront.FunctionRuntime.JS_2_0,
    });

    const additionalBehaviors: Record<string, cloudfront.BehaviorOptions> = {
      // Most specific first: CloudFront matches behaviors in order.
      [STREAM_PATH]: { origin: streamOrigin, ...passThrough },
    };
    for (const p of API_PATHS) {
      additionalBehaviors[p] = { origin: apiOrigin, ...passThrough };
    }

    this.distribution = new cloudfront.Distribution(this, "Distribution", {
      comment: `${prefix} dashboard and API`,
      ...(domain && certificate ? { domainNames: [domain.hostname], certificate } : {}),
      defaultRootObject: "index.html",
      httpVersion: cloudfront.HttpVersion.HTTP2_AND_3,
      minimumProtocolVersion: cloudfront.SecurityPolicyProtocol.TLS_V1_2_2021,
      defaultBehavior: {
        origin: origins.S3BucketOrigin.withOriginAccessControl(bucket),
        viewerProtocolPolicy: cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
        cachePolicy: cloudfront.CachePolicy.CACHING_OPTIMIZED,
        responseHeadersPolicy: securityHeaders,
        functionAssociations: [
          { function: staticPaths, eventType: cloudfront.FunctionEventType.VIEWER_REQUEST },
        ],
      },
      additionalBehaviors,
    });

    const hostname = domain ? domain.hostname : this.distribution.distributionDomainName;
    this.publicUrl = `https://${hostname}`;

    if (domain && zone) {
      const recordName = recordNameIn(domain.hostname, domain.hostedZone!);
      const target = route53.RecordTarget.fromAlias(new route53Targets.CloudFrontTarget(this.distribution));
      new route53.ARecord(this, "AliasRecord", { zone, recordName, target });
      new route53.AaaaRecord(this, "AliasRecordV6", { zone, recordName, target });
    }

    // Read by gateway, streaming and worker at start when the URL wasn't
    // known at synth (no custom domain).
    new ssm.StringParameter(this, "PublicUrlParameter", {
      parameterName: cfg.publicUrlParameter,
      stringValue: this.publicUrl,
      description: "CoderHelm's public URL (dashboard and API)",
    });

    // --- Dashboard upload ---
    const runtimeConfig = s3deploy.Source.data("runtime-config.js", runtimeConfigScript(cfg));
    const assetDir = cfg.dashboardAssetDir ? path.resolve(cfg.dashboardAssetDir) : null;
    if (assetDir && !fs.existsSync(path.join(assetDir, "index.html"))) {
      throw new Error(`dashboard.assetDir ${assetDir} has no index.html: point it at a dashboard build (the static export).`);
    }
    if (assetDir) {
      // Hashed files first and never pruned, so pages already open keep
      // loading their scripts across a deploy.
      const assets = new s3deploy.BucketDeployment(this, "DashboardAssets", {
        sources: [s3deploy.Source.asset(assetDir)],
        destinationBucket: bucket,
        exclude: ["*"],
        include: ["_next/*"],
        prune: false,
        cacheControl: [s3deploy.CacheControl.fromString(ASSET_CACHE_CONTROL)],
        memoryLimit: 512,
      });
      const pages = new s3deploy.BucketDeployment(this, "DashboardPages", {
        sources: [s3deploy.Source.asset(assetDir, { exclude: ["_next/**", "runtime-config.js"] }), runtimeConfig],
        destinationBucket: bucket,
        exclude: ["_next/*"],
        prune: true,
        cacheControl: [s3deploy.CacheControl.fromString(PAGE_CACHE_CONTROL)],
        distribution: this.distribution,
        distributionPaths: ["/*"],
        memoryLimit: 512,
      });
      pages.node.addDependency(assets);
    } else {
      new s3deploy.BucketDeployment(this, "DashboardPlaceholder", {
        sources: [s3deploy.Source.data("index.html", PLACEHOLDER_HTML), runtimeConfig],
        destinationBucket: bucket,
        cacheControl: [s3deploy.CacheControl.fromString(PAGE_CACHE_CONTROL)],
        distribution: this.distribution,
        distributionPaths: ["/*"],
      });
    }

    new cdk.CfnOutput(this, "PublicUrl", { value: this.publicUrl, description: "Open this to use CoderHelm" });
    new cdk.CfnOutput(this, "SignUpUrl", { value: `${this.publicUrl}/signup/`, description: "The first admin signs up here" });
    new cdk.CfnOutput(this, "GithubWebhookUrl", { value: `${this.publicUrl}/webhooks/github` });
    new cdk.CfnOutput(this, "DistributionId", { value: this.distribution.distributionId });
    new cdk.CfnOutput(this, "DashboardBucketName", { value: bucket.bucketName });
    if (domain && !zone) {
      new cdk.CfnOutput(this, "DnsRecord", {
        value: `${domain.hostname} CNAME ${this.distribution.distributionDomainName}`,
        description: "Create this DNS record at your DNS provider",
      });
    }
  }
}
