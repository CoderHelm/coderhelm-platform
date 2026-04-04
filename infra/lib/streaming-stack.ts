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
import * as logs from "aws-cdk-lib/aws-logs";
import { Construct } from "constructs";

interface StreamingStackProps extends cdk.StackProps {
  stage: string;
  table: dynamodb.TableV2;
  plansTable: dynamodb.TableV2;
  reposTable: dynamodb.TableV2;
  settingsTable: dynamodb.TableV2;
  mcpConfigsTable: dynamodb.TableV2;
  bucket: s3.Bucket;
  mcpProxyFunction: lambda.Function;
}

export class StreamingStack extends cdk.Stack {
  public readonly streamingFunction: lambda.Function;

  constructor(scope: Construct, id: string, props: StreamingStackProps) {
    super(scope, id, props);

    const prefix = `coderhelm-${props.stage}`;
    const gatewayAssetPath =
      process.env.GATEWAY_ZIP ?? "../services/gateway/target/lambda/gateway";

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
      `coderhelm/${props.stage}/secrets`
    );

    this.streamingFunction = new lambda.Function(this, "Streaming", {
      functionName: `${prefix}-streaming`,
      runtime: lambda.Runtime.PROVIDED_AL2023,
      architecture: lambda.Architecture.ARM_64,
      handler: "bootstrap",
      code: lambda.Code.fromAsset(gatewayAssetPath),
      memorySize: 512,
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
        PLANS_TABLE_NAME: props.plansTable.tableName,
        REPOS_TABLE_NAME: props.reposTable.tableName,
        SETTINGS_TABLE_NAME: props.settingsTable.tableName,
        MCP_CONFIGS_TABLE_NAME: props.mcpConfigsTable.tableName,
        ANALYTICS_TABLE_NAME: `${prefix}-analytics`,
        BUCKET_NAME: props.bucket.bucketName,
        SECRETS_NAME: `coderhelm/${props.stage}/secrets`,
        MODEL_ID: "us.anthropic.claude-sonnet-4-6",
        MCP_PROXY_FUNCTION_NAME: props.mcpProxyFunction.functionName,
        RUST_LOG: "info",
      },
    });

    // --- Permissions ---

    props.table.grantReadData(this.streamingFunction);
    props.plansTable.grantReadWriteData(this.streamingFunction);
    props.reposTable.grantReadData(this.streamingFunction);
    props.settingsTable.grantReadData(this.streamingFunction);
    props.mcpConfigsTable.grantReadData(this.streamingFunction);
    props.bucket.grantRead(this.streamingFunction);
    secrets.grantRead(this.streamingFunction);

    // Analytics table (write token usage)
    const analyticsTable = dynamodb.TableV2.fromTableName(
      this,
      "AnalyticsTableRef",
      `${prefix}-analytics`
    );
    analyticsTable.grantReadWriteData(this.streamingFunction);

    // Bedrock streaming access
    this.streamingFunction.addToRolePolicy(
      new iam.PolicyStatement({
        actions: [
          "bedrock:InvokeModel",
          "bedrock:InvokeModelWithResponseStream",
          "bedrock:Converse",
          "bedrock:ConverseStream",
        ],
        resources: [
          `arn:aws:bedrock:*::foundation-model/*`,
          `arn:aws:bedrock:*:${this.account}:inference-profile/*`,
        ],
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
      cors: {
        allowedOrigins: props.stage === "prod"
          ? ["https://app.coderhelm.com", "https://coderhelm.com"]
          : ["http://localhost:3000", "http://localhost:3001"],
        allowedMethods: [lambda.HttpMethod.POST, lambda.HttpMethod.OPTIONS],
        allowedHeaders: ["Content-Type", "Authorization", "Cookie"],
        allowCredentials: true,
        maxAge: cdk.Duration.hours(1),
      },
    });

    // --- CloudFront + DNS (prod only) ---

    if (props.stage === "prod") {
      const streamDomain = "stream.coderhelm.com";

      const hostedZone = route53.HostedZone.fromLookup(this, "Zone", {
        domainName: "coderhelm.com",
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
            accessControlAllowOrigins: props.stage === "prod"
              ? ["https://app.coderhelm.com", "https://coderhelm.com"]
              : ["http://localhost:3000", "http://localhost:3001"],
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

      // DNS: stream.coderhelm.com → CloudFront
      new route53.ARecord(this, "StreamAliasRecord", {
        zone: hostedZone,
        recordName: "stream",
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
