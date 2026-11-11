/**
 * Single-origin web front door (self-hosted deployments).
 *
 * One CloudFront distribution serves the dashboard from S3 and forwards the
 * service paths to their backends, all on one hostname:
 *
 *   /api/plans/chat/stream /api/brain/stream   → the streaming function URL (SSE),
 *       signed by CloudFront (origin access control; the URL takes only signed calls)
 *   /api/* /auth/* /webhooks/* /email/* /integrations/* /mcp /oauth/*   → the HTTP API
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
import * as s3 from "aws-cdk-lib/aws-s3";
import * as s3deploy from "aws-cdk-lib/aws-s3-deployment";
import * as ssm from "aws-cdk-lib/aws-ssm";
import { Construct } from "constructs";
import { ResolvedConfig } from "./config";
import { releaseStamp } from "./release";
import { logAccessTo, logBucketOf } from "./storage-stack";

/**
 * Paths the HTTP API serves (each also matches its sub-paths). Includes the
 * signed approval links in Slack, Teams and email (`/agent-approvals/link`,
 * `/plugins/approve`); only that one plugin path, since the dashboard's own
 * pages live under `/plugins/`.
 */
export const API_PATHS = [
  "/api/*",
  "/auth/*",
  "/webhooks/*",
  "/email/*",
  "/integrations/*",
  "/agent-approvals/*",
  "/plugins/approve*",
  "/openapi.json",
  // The MCP server and how its clients sign in (the dashboard's consent
  // page is /authorize, outside these).
  "/mcp",
  "/.well-known/oauth-*",
  "/oauth/*",
];
/** The chat stream (SSE, served by the streaming function). */
export const STREAM_PATH = "/api/plans/chat/stream";
/** Chat's answer stream: answers that read code run past the HTTP API's 30-second limit. */
export const BRAIN_STREAM_PATH = "/api/brain/stream";

/**
 * The lowest TLS version and ciphers viewers may use: CloudFront's newest
 * TLS 1.2 policy (TLS 1.2 with AES-GCM and ChaCha20 only, and TLS 1.3).
 */
export const VIEWER_TLS_POLICY = cloudfront.SecurityPolicyProtocol.TLS_V1_2_2025;

/**
 * The regions CloudFront's standard logs (legacy, to S3) can be delivered to:
 * every region enabled by default. It delivers none to a bucket in an opt-in
 * region, so an install there has no CloudFront logs (S3 access logs still).
 */
export const CLOUDFRONT_LOG_REGIONS = [
  "us-east-1", "us-east-2", "us-west-1", "us-west-2", "ca-central-1", "sa-east-1",
  "eu-west-1", "eu-west-2", "eu-west-3", "eu-central-1", "eu-north-1",
  "ap-northeast-1", "ap-northeast-2", "ap-northeast-3", "ap-southeast-1", "ap-southeast-2", "ap-south-1",
];

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
/** A file in infra/error-pages (from lib/ or the compiled dist/lib/). */
export function errorPage(name: string): string {
  const a = path.join(__dirname, "..", "error-pages", name);
  return fs.existsSync(a) ? a : path.join(__dirname, "..", "..", "error-pages", name);
}

const PLACEHOLDER_HTML = `<!doctype html><html><head><meta charset="utf-8"><title>CoderHelm</title>
<meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="font-family:system-ui,sans-serif;background:#0a0a0a;color:#ddd;padding:48px">
<h1>CoderHelm is deployed</h1>
<p>The dashboard has not been uploaded yet. Run <code>coderhelm-selfhost deploy</code>
(it downloads the dashboard for this release), or set <code>dashboard.assetDir</code>
in your config to a dashboard build and deploy again.</p></body></html>`;

/**
 * The origin the dashboard build writes into its link preview tags (og:url,
 * og:image, twitter:image; CoderHelm/coderhelm dashboard/src/lib/link-preview.ts).
 * One build serves every install, so the pages upload swaps it for this
 * install's own public URL: a link pasted into Teams or Slack previews with a
 * picture from the install itself, never from CoderHelm's servers.
 */
export const DASHBOARD_URL_PLACEHOLDER = "https://coderhelm-install.invalid";

/**
 * `source`, with every occurrence of `placeholder` in its files replaced by
 * `value` when the upload runs (the bucket deployment's marker substitution,
 * the same one `Source.data` uses for deploy-time values). `value` may be a
 * token: a native release's address is only known when the stack deploys, and
 * a later domain change re-runs the upload with the new one.
 */
export function withDeployTimeValue(source: s3deploy.ISource, placeholder: string, value: string): s3deploy.ISource {
  return {
    bind(scope, context) {
      const config = source.bind(scope, context);
      return { ...config, markers: { ...(config.markers ?? {}), [placeholder]: value } };
    },
  };
}

/** What the dashboard reads at start (public/runtime-config.js in its build). */
export function runtimeConfigScript(cfg: ResolvedConfig): string {
  const stamp = releaseStamp(cfg);
  const value = {
    // Same origin: the dashboard calls /api/... on its own hostname.
    apiUrl: "",
    streamUrl: "",
    selfHosted: true,
    supportEmail: cfg.email.supportInbox,
    siteUrl: cfg.siteUrl,
    // Which release this is (shown in the account menu and Settings → System).
    version: stamp.version,
    commit: stamp.commit,
    releasedAt: stamp.releasedAt,
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
      lifecycleRules: [{ id: "abort-uploads", abortIncompleteMultipartUploadAfter: cdk.Duration.days(1) }],
    });
    // Into the storage stack's log bucket (this stack depends on it: app.ts).
    logAccessTo(bucket, cfg, "dashboard");

    // --- Certificate and DNS (custom domain only) ---
    let certificate: acm.ICertificate | undefined;
    // CoderHelm never changes DNS: the certificate is the admin's (config
    // requires certificateArn with a domain) and the records are printed.
    if (domain?.certificateArn) {
      certificate = acm.Certificate.fromCertificateArn(this, "Certificate", domain.certificateArn);
    }

    // --- Origins ---
    const apiDomain = `${props.httpApi.apiId}.execute-api.${this.region}.${this.urlSuffix}`;
    const apiOrigin = new origins.HttpOrigin(apiDomain, {
      protocolPolicy: cloudfront.OriginProtocolPolicy.HTTPS_ONLY,
      originSslProtocols: [cloudfront.OriginSslPolicy.TLS_V1_2],
    });
    // The chat streams: the function URL takes only calls signed with AWS
    // credentials (AWS_IAM, streaming-stack.ts), and CloudFront signs each one
    // through origin access control. The dashboard sends the stream token in
    // its own header (CloudFront's signature is the Authorization header) and
    // the body's SHA-256 in x-amz-content-sha256, which Lambda requires of a
    // signed POST. Response streaming passes through as before.
    const streamOrigin = origins.FunctionUrlOrigin.withOriginAccessControl(props.streamingFunctionUrl, {
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
      [BRAIN_STREAM_PATH]: { origin: streamOrigin, ...passThrough },
    };
    for (const p of API_PATHS) {
      additionalBehaviors[p] = { origin: apiOrigin, ...passThrough };
    }

    this.distribution = new cloudfront.Distribution(this, "Distribution", {
      comment: `${prefix} dashboard and API`,
      ...(domain && certificate ? { domainNames: [domain.hostname], certificate } : {}),
      defaultRootObject: "index.html",
      httpVersion: cloudfront.HttpVersion.HTTP2_AND_3,
      minimumProtocolVersion: VIEWER_TLS_POLICY,
      defaultBehavior: {
        // List too, so a file that isn't there is a 404 (not S3's 403),
        // which the error response below turns into the install's own page.
        origin: origins.S3BucketOrigin.withOriginAccessControl(bucket, {
          originAccessLevels: [cloudfront.AccessLevel.READ, cloudfront.AccessLevel.LIST],
        }),
        viewerProtocolPolicy: cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
        cachePolicy: cloudfront.CachePolicy.CACHING_OPTIMIZED,
        responseHeadersPolicy: securityHeaders,
        functionAssociations: [
          { function: staticPaths, eventType: cloudfront.FunctionEventType.VIEWER_REQUEST },
        ],
      },
      additionalBehaviors,
      // A page that isn't there, on any address the install answers on (its
      // own domain, a CoderHelm address or the AWS address): the install's
      // 404 page, with a 404. CloudFront applies this to the whole
      // distribution, so an API 404 also arrives with this page as its body
      // (the status is unchanged and the dashboard reads the status); other
      // API errors keep their JSON answers, which is why only 404 is mapped.
      errorResponses: [
        { httpStatus: 404, responseHttpStatus: 404, responsePagePath: "/_errors/404.html", ttl: cdk.Duration.seconds(60) },
      ],
    });

    this.standardLogs(cfg);

    // Origin access control signs the stream's calls as CloudFront; Lambda
    // needs both lambda:InvokeFunctionUrl and lambda:InvokeFunction (with
    // InvokedViaFunctionUrl) for a function URL. FunctionUrlOrigin adds both,
    // scoped to this distribution, since aws-cdk-lib 2.25x.

    let hostname = domain ? domain.hostname : this.distribution.distributionDomainName;
    if (cfg.native) hostname = this.nativeDomain(this.distribution);
    this.publicUrl = `https://${hostname}`;

    // Read by gateway, streaming and worker at start when the URL wasn't
    // known at synth (no custom domain).
    const publicUrlParameter = new ssm.StringParameter(this, "PublicUrlParameter", {
      parameterName: cfg.publicUrlParameter,
      stringValue: this.publicUrl,
      description: "CoderHelm's public URL (dashboard and API)",
    });
    // A native release's URL depends on a condition, which can't cross into
    // another part: the other parts read it from the parameter (an output).
    if (cfg.native) this.publicUrl = publicUrlParameter.stringValue;

    // CloudFront's own hostname, the target of the DNS record for a domain
    // added later (the updater shows it; docs/self-host/domains.md).
    new ssm.StringParameter(this, "DistributionDomainParameter", {
      parameterName: cfg.distributionDomainParameter,
      stringValue: this.distribution.distributionDomainName,
      description: "CloudFront's own hostname for CoderHelm (the CNAME target for your own domain)",
    });

    // --- Dashboard upload ---
    const runtimeConfig = s3deploy.Source.data("runtime-config.js", runtimeConfigScript(cfg));
    // The install's own 404 page (infra/error-pages/404.html).
    const notFound = s3deploy.Source.data("_errors/404.html", fs.readFileSync(errorPage("404.html"), "utf8"));
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
        sources: [
          withDeployTimeValue(
            s3deploy.Source.asset(assetDir, { exclude: ["_next/**", "runtime-config.js"] }),
            DASHBOARD_URL_PLACEHOLDER,
            this.publicUrl,
          ),
          runtimeConfig,
          notFound,
        ],
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
        sources: [s3deploy.Source.data("index.html", PLACEHOLDER_HTML), runtimeConfig, notFound],
        destinationBucket: bucket,
        cacheControl: [s3deploy.CacheControl.fromString(PAGE_CACHE_CONTROL)],
        distribution: this.distribution,
        distributionPaths: ["/*"],
      });
    }

    new cdk.CfnOutput(this, "PublicUrl", { value: this.publicUrl, description: "Open this to use CoderHelm" });
    new cdk.CfnOutput(this, "SignUpUrl", { value: `${this.publicUrl}/?signup=1`, description: "The first admin signs up here" });
    new cdk.CfnOutput(this, "GithubWebhookUrl", { value: `${this.publicUrl}/webhooks/github` });
    new cdk.CfnOutput(this, "DistributionId", { value: this.distribution.distributionId });
    new cdk.CfnOutput(this, "DashboardBucketName", { value: bucket.bucketName });
    if (domain && !cfg.native) {
      new cdk.CfnOutput(this, "DnsRecord", {
        value: `${domain.hostname} CNAME ${this.distribution.distributionDomainName}`,
        description: "Create this DNS record at your DNS provider",
      });
    }
  }

  /**
   * Standard logs into the storage stack's log bucket (`cloudfront/`, kept
   * 90 days), where CloudFront can deliver them: a native release's one
   * template decides by its region (a condition), any other install at synth.
   */
  private standardLogs(cfg: ResolvedConfig): void {
    const logging = {
      Bucket: logBucketOf(this, cfg).bucketRegionalDomainName,
      IncludeCookies: false,
      Prefix: "cloudfront/",
    };
    const cfn = this.distribution.node.defaultChild as cloudfront.CfnDistribution;
    if (cfg.native) {
      const eq = (r: string) => cdk.Fn.conditionEquals(cdk.Aws.REGION, r);
      const half = Math.ceil(CLOUDFRONT_LOG_REGIONS.length / 2);
      const supported = new cdk.CfnCondition(this, "CloudFrontLogsRegion", {
        // Fn::Or takes at most 10.
        expression: cdk.Fn.conditionOr(
          cdk.Fn.conditionOr(...CLOUDFRONT_LOG_REGIONS.slice(0, half).map(eq)),
          cdk.Fn.conditionOr(...CLOUDFRONT_LOG_REGIONS.slice(half).map(eq)),
        ),
      });
      cfn.addPropertyOverride("DistributionConfig.Logging", cdk.Fn.conditionIf(supported.logicalId, logging, cdk.Aws.NO_VALUE));
    } else if (CLOUDFRONT_LOG_REGIONS.includes(cfg.region)) {
      cfn.addPropertyOverride("DistributionConfig.Logging", logging);
    }
  }

  /**
   * A native release's own domain: stack parameters, not a redeploy. The
   * updater requests the certificate (Settings, System, or the Hostname
   * launch parameter through the parent's Domain custom resource), then
   * proposes a stack update that sets DomainName and DomainCertificateArn
   * (DomainZoneId is legacy and left empty: CoderHelm never changes DNS).
   * Empty values: CloudFront's own address.
   */
  private nativeDomain(distribution: cloudfront.Distribution): string {
    const name = new cdk.CfnParameter(this, "DomainName", { type: "String", default: "" });
    const cert = new cdk.CfnParameter(this, "DomainCertificateArn", { type: "String", default: "" });
    const zoneId = new cdk.CfnParameter(this, "DomainZoneId", { type: "String", default: "" });
    const has = new cdk.CfnCondition(this, "HasDomain", {
      expression: cdk.Fn.conditionAnd(
        cdk.Fn.conditionNot(cdk.Fn.conditionEquals(name.valueAsString, "")),
        cdk.Fn.conditionNot(cdk.Fn.conditionEquals(cert.valueAsString, "")),
      ),
    });
    const inZone = new cdk.CfnCondition(this, "HasDomainZone", {
      expression: cdk.Fn.conditionAnd(has, cdk.Fn.conditionNot(cdk.Fn.conditionEquals(zoneId.valueAsString, ""))),
    });
    const cfn = distribution.node.defaultChild as cloudfront.CfnDistribution;
    cfn.addPropertyOverride("DistributionConfig.Aliases", cdk.Fn.conditionIf(has.logicalId, [name.valueAsString], cdk.Aws.NO_VALUE));
    cfn.addPropertyOverride(
      "DistributionConfig.ViewerCertificate",
      cdk.Fn.conditionIf(
        has.logicalId,
        { AcmCertificateArn: cert.valueAsString, SslSupportMethod: "sni-only", MinimumProtocolVersion: VIEWER_TLS_POLICY },
        cdk.Aws.NO_VALUE,
      ),
    );
    // Legacy only. CoderHelm no longer changes DNS (the install's roles may not:
    // see access.ts NoDnsChanges), so nothing sets DomainZoneId and new installs
    // never get these. An older install that already has them keeps them as
    // they are; retained, so a later update never tries to delete DNS records.
    for (const type of ["A", "AAAA"]) {
      const r = new route53.CfnRecordSet(this, `NativeAlias${type}`, {
        hostedZoneId: zoneId.valueAsString,
        name: name.valueAsString,
        type,
        aliasTarget: { dnsName: distribution.distributionDomainName, hostedZoneId: "Z2FDTNDATAQYW2" },
      });
      r.cfnOptions.condition = inZone;
      r.cfnOptions.deletionPolicy = cdk.CfnDeletionPolicy.RETAIN;
      r.cfnOptions.updateReplacePolicy = cdk.CfnDeletionPolicy.RETAIN;
    }
    return cdk.Token.asString(cdk.Fn.conditionIf(has.logicalId, name.valueAsString, distribution.distributionDomainName));
  }
}
