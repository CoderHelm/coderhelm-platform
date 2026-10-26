import * as cdk from "aws-cdk-lib";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as cloudfront from "aws-cdk-lib/aws-cloudfront";
import * as origins from "aws-cdk-lib/aws-cloudfront-origins";
import * as acm from "aws-cdk-lib/aws-certificatemanager";
import * as route53 from "aws-cdk-lib/aws-route53";
import * as targets from "aws-cdk-lib/aws-route53-targets";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as secretsmanager from "aws-cdk-lib/aws-secretsmanager";
import * as iam from "aws-cdk-lib/aws-iam";
import * as kms from "aws-cdk-lib/aws-kms";
import * as logs from "aws-cdk-lib/aws-logs";
import * as sqs from "aws-cdk-lib/aws-sqs";
import { Construct } from "constructs";
import { ResolvedConfig } from "./config";
import { repoPath } from "./paths";
import { serviceEnvironment, modelPolicyStatements, publicUrlReadStatement, recordNameIn } from "./service-env";

interface StreamingStackProps extends cdk.StackProps {
  stage: string;
  cfg: ResolvedConfig;
  table: dynamodb.TableV2;
  plansTable: dynamodb.TableV2;
  reposTable: dynamodb.TableV2;
  settingsTable: dynamodb.TableV2;
  mcpConfigsTable: dynamodb.TableV2;
  bucket: s3.Bucket;
  mcpProxyFunction: lambda.Function;
  secretsKey: kms.Key;
  /** Where a plan chat call whose usage couldn't be written is queued. */
  ticketQueue: sqs.IQueue;
  /** Outbound webhook events (a route this function serves may emit one). */
  webhookQueue: sqs.IQueue;
}

export class StreamingStack extends cdk.Stack {
  public readonly streamingFunction: lambda.Function;
  /** The chat stream's function URL (the single-origin web stack forwards to it). */
  public readonly functionUrl: lambda.FunctionUrl;

  constructor(scope: Construct, id: string, props: StreamingStackProps) {
    super(scope, id, props);

    const cfg = props.cfg;
    const prefix = cfg.prefix;
    const gatewayAssetPath =
      process.env.GATEWAY_ZIP ?? repoPath("services/gateway/target/lambda/gateway");

    // --- Lambda Web Adapter Layer ---
    // Enables response streaming for non-Node.js runtimes (Rust/Axum)
    // https://github.com/awslabs/aws-lambda-web-adapter
    const webAdapterLayer = lambda.LayerVersion.fromLayerVersionArn(
      this,
      "WebAdapterLayer",
      `arn:aws:lambda:${this.region}:753240598075:layer:LambdaAdapterLayerArm64:24`
    );

    // --- Streaming Gateway Lambda ---

    const logGroup = new logs.LogGroup(this, "StreamingLogGroup", {
      logGroupName: `/aws/lambda/${prefix}-streaming`,
      retention: logs.RetentionDays.ONE_MONTH,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const secrets = secretsmanager.Secret.fromSecretNameV2(
      this,
      "Secrets",
      `${props.cfg.secretName}`
    );

    this.streamingFunction = new lambda.Function(this, "Streaming", {
      functionName: `${prefix}-streaming`,
      runtime: lambda.Runtime.PROVIDED_AL2023,
      architecture: lambda.Architecture.ARM_64,
      handler: "bootstrap",
      code: lambda.Code.fromAsset(gatewayAssetPath),
      memorySize: 1024,
      timeout: cdk.Duration.minutes(5),
      logGroup,
      layers: [webAdapterLayer],
      environment: {
        // Lambda Web Adapter config
        AWS_LWA_INVOKE_MODE: "response_stream",
        AWS_LWA_PORT: "8080",
        // App config
        STAGE: props.stage,
        STREAMING_MODE: "true",
        TABLE_NAME: props.table.tableName,
        TEAMS_TABLE_NAME: `${prefix}-teams`,
        PLANS_TABLE_NAME: props.plansTable.tableName,
        REPOS_TABLE_NAME: props.reposTable.tableName,
        SETTINGS_TABLE_NAME: props.settingsTable.tableName,
        MCP_CONFIGS_TABLE_NAME: props.mcpConfigsTable.tableName,
        ANALYTICS_TABLE_NAME: `${prefix}-analytics`,
        RUNS_TABLE_NAME: `${prefix}-runs`,
        EVENTS_TABLE_NAME: `${prefix}-events`,
        USERS_TABLE_NAME: `${prefix}-users`,
        JIRA_TOKENS_TABLE_NAME: `${prefix}-jira-tokens`,
        JIRA_EVENTS_TABLE_NAME: `${prefix}-jira-events`,
        JIRA_CONFIG_TABLE_NAME: `${prefix}-jira-config`,
        AWS_INSIGHTS_TABLE_NAME: `${prefix}-aws-insights`,
        INFRA_TABLE_NAME: `${prefix}-infra`,
        BANNERS_TABLE_NAME: `${prefix}-banners`,
        WAITLIST_TABLE_NAME: `${prefix}-waitlist`,
        BUCKET_NAME: props.bucket.bucketName,
        SECRETS_NAME: `${props.cfg.secretName}`,
        MODEL_ID: cfg.models.light,
        MCP_PROXY_FUNCTION_NAME: props.mcpProxyFunction.functionName,
        // The plan chat's skills, agents and data source tools run in the
        // worker (by name: the worker stack depends on the API stack).
        WORKER_FUNCTION_NAME: `${prefix}-worker`,
        // A model call whose usage couldn't be written waits here
        // (`usage_record`), for the worker to add once.
        TICKET_QUEUE_URL: props.ticketQueue.queueUrl,
        WEBHOOK_QUEUE_URL: props.webhookQueue.queueUrl,
        // Unused by streaming but required by shared binary
        CI_FIX_QUEUE_URL: "unused",
        FEEDBACK_QUEUE_URL: "unused",
        COGNITO_USER_POOL_ID: "unused",
        COGNITO_CLIENT_ID: "unused",
        COGNITO_DOMAIN: "unused",
        RUST_LOG: "info",
        ...serviceEnvironment(cfg, this.account),
      },
    });
    for (const st of [...modelPolicyStatements(cfg, this), publicUrlReadStatement(cfg, this)]) {
      if (st) this.streamingFunction.addToRolePolicy(st);
    }

    // --- Permissions ---

    props.table.grantReadData(this.streamingFunction);
    props.plansTable.grantReadWriteData(this.streamingFunction);
    props.reposTable.grantReadData(this.streamingFunction);
    // Read: plan chat settings. Write: the brain chat keeps its threads and
    // messages here (BRAIN#T# / BRAIN#M# records).
    props.settingsTable.grantReadWriteData(this.streamingFunction);
    props.mcpConfigsTable.grantReadData(this.streamingFunction);
    // Plan chat opens a team's sealed MCP credentials for a tool call; it
    // never seals (the gateway does), so decrypt only.
    props.secretsKey.grantDecrypt(this.streamingFunction);
    props.bucket.grantRead(this.streamingFunction);
    secrets.grantRead(this.streamingFunction);

    // Analytics table (write token usage)
    const analyticsTable = dynamodb.TableV2.fromTableName(
      this,
      "AnalyticsTableRef",
      `${prefix}-analytics`
    );
    analyticsTable.grantReadWriteData(this.streamingFunction);

    this.streamingFunction.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["lambda:InvokeFunction"],
        resources: [`arn:aws:lambda:${this.region}:${this.account}:function:${prefix}-worker`],
      })
    );

    props.ticketQueue.grantSendMessages(this.streamingFunction);
    props.webhookQueue.grantSendMessages(this.streamingFunction);

    // Bedrock: Titan embeddings only (the shared gateway binary's memory
    // routes, metered in common::embed). Plan chat calls Anthropic directly
    // with the team's key; no Claude model on Bedrock is called from here.
    this.streamingFunction.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["bedrock:InvokeModel"],
        resources: [`arn:aws:bedrock:*::foundation-model/amazon.titan-embed-text-v2:0`],
      })
    );

    // MCP proxy invocation
    props.mcpProxyFunction.grantInvoke(this.streamingFunction);

    // Lambda invoke (for MCP proxy)
    this.streamingFunction.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["lambda:InvokeFunction"],
        resources: [props.mcpProxyFunction.functionArn],
      })
    );

    // --- Function URL with Response Streaming ---

    const functionUrl = this.streamingFunction.addFunctionUrl({
      authType: lambda.FunctionUrlAuthType.NONE,
      invokeMode: lambda.InvokeMode.RESPONSE_STREAM,
      // Cross-origin only in the split layout (single-origin forwards the
      // stream from the dashboard's own hostname).
      cors: cfg.split
        ? {
            allowedOrigins: cfg.split.streamCorsOrigins,
            allowedMethods: [lambda.HttpMethod.POST],
            allowedHeaders: ["Content-Type", "Authorization", "Cookie"],
            allowCredentials: true,
            maxAge: cdk.Duration.hours(1),
          }
        : undefined,
    });
    this.functionUrl = functionUrl;

    // --- CloudFront + DNS (split layout with a stream domain) ---

    const streamDomainName = cfg.split?.streamDomain;
    if (streamDomainName && cfg.split?.zone) {
      const streamDomain = streamDomainName;

      const hostedZone = route53.HostedZone.fromLookup(this, "Zone", {
        domainName: cfg.split.zone,
      });

      const certificate = new acm.Certificate(this, "StreamCertificate", {
        domainName: streamDomain,
        validation: acm.CertificateValidation.fromDns(hostedZone),
      });

      // Extract the Function URL domain (strip https:// and trailing /)
      const fnUrlDomain = cdk.Fn.select(
        2,
        cdk.Fn.split("/", functionUrl.url)
      );

      const corsPolicy = new cloudfront.ResponseHeadersPolicy(
        this,
        "StreamCorsPolicy",
        {
          responseHeadersPolicyName: `${prefix}-stream-cors`,
          corsBehavior: {
            accessControlAllowOrigins: cfg.split.streamCorsOrigins,
            accessControlAllowMethods: ["POST", "OPTIONS"],
            accessControlAllowHeaders: [
              "Content-Type",
              "Authorization",
              "Cookie",
            ],
            accessControlAllowCredentials: true,
            accessControlMaxAge: cdk.Duration.hours(1),
            originOverride: true,
          },
        }
      );

      // CloudFront distribution fronting the Function URL
      const distribution = new cloudfront.Distribution(
        this,
        "StreamDistribution",
        {
          domainNames: [streamDomain],
          certificate,
          defaultBehavior: {
            origin: new origins.HttpOrigin(fnUrlDomain, {
              protocolPolicy: cloudfront.OriginProtocolPolicy.HTTPS_ONLY,
              originSslProtocols: [cloudfront.OriginSslPolicy.TLS_V1_2],
            }),
            viewerProtocolPolicy:
              cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
            cachePolicy: cloudfront.CachePolicy.CACHING_DISABLED,
            originRequestPolicy:
              cloudfront.OriginRequestPolicy.ALL_VIEWER_EXCEPT_HOST_HEADER,
            allowedMethods: cloudfront.AllowedMethods.ALLOW_ALL,
            responseHeadersPolicy: corsPolicy,
          },
          httpVersion: cloudfront.HttpVersion.HTTP2_AND_3,
        }
      );

      // DNS: the stream domain → CloudFront
      new route53.ARecord(this, "StreamAliasRecord", {
        zone: hostedZone,
        recordName: recordNameIn(streamDomain, cfg.split.zone),
        target: route53.RecordTarget.fromAlias(
          new targets.CloudFrontTarget(distribution)
        ),
      });

      new cdk.CfnOutput(this, "StreamUrl", {
        value: `https://${streamDomain}`,
      });
    }

    // --- Outputs ---

    new cdk.CfnOutput(this, "FunctionUrl", {
      value: functionUrl.url,
      description: "Lambda Function URL (direct, for dev/testing)",
    });

    new cdk.CfnOutput(this, "StreamingFunctionArn", {
      value: this.streamingFunction.functionArn,
    });
  }
}
