import * as cdk from "aws-cdk-lib";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as apigatewayv2 from "aws-cdk-lib/aws-apigatewayv2";
import * as integrations from "aws-cdk-lib/aws-apigatewayv2-integrations";
import * as acm from "aws-cdk-lib/aws-certificatemanager";
import * as route53 from "aws-cdk-lib/aws-route53";
import * as route53Targets from "aws-cdk-lib/aws-route53-targets";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as secretsmanager from "aws-cdk-lib/aws-secretsmanager";
import * as iam from "aws-cdk-lib/aws-iam";
import * as logs from "aws-cdk-lib/aws-logs";
import { Construct } from "constructs";

interface ApiStackProps extends cdk.StackProps {
  stage: string;
  table: dynamodb.TableV2;
  runsTable: dynamodb.TableV2;
  analyticsTable: dynamodb.TableV2;
  bucket: s3.Bucket;
}

export class ApiStack extends cdk.Stack {
  public readonly gatewayFunction: lambda.Function;
  public readonly ticketQueue: sqs.Queue;
  public readonly ciFixQueue: sqs.Queue;
  public readonly feedbackQueue: sqs.Queue;

  constructor(scope: Construct, id: string, props: ApiStackProps) {
    super(scope, id, props);

    const prefix = `coderhelm-${props.stage}`;
    const gatewayAssetPath =
      process.env.GATEWAY_ZIP ?? "../services/gateway/target/lambda/gateway";

    // --- SQS Queues ---

    // Dead letter queue (shared)
    const dlq = new sqs.Queue(this, "DLQ", {
      queueName: `${prefix}-dlq`,
      retentionPeriod: cdk.Duration.days(14),
      encryption: sqs.QueueEncryption.SQS_MANAGED,
    });

    this.ticketQueue = new sqs.Queue(this, "TicketQueue", {
      queueName: `${prefix}-tickets`,
      visibilityTimeout: cdk.Duration.minutes(16), // > Lambda timeout
      encryption: sqs.QueueEncryption.SQS_MANAGED,
      deadLetterQueue: { queue: dlq, maxReceiveCount: 3 },
    });

    this.ciFixQueue = new sqs.Queue(this, "CiFixQueue", {
      queueName: `${prefix}-ci-fix`,
      visibilityTimeout: cdk.Duration.minutes(16),
      encryption: sqs.QueueEncryption.SQS_MANAGED,
      deadLetterQueue: { queue: dlq, maxReceiveCount: 3 },
    });

    this.feedbackQueue = new sqs.Queue(this, "FeedbackQueue", {
      queueName: `${prefix}-feedback`,
      visibilityTimeout: cdk.Duration.minutes(16),
      encryption: sqs.QueueEncryption.SQS_MANAGED,
      deadLetterQueue: { queue: dlq, maxReceiveCount: 3 },
    });

    // --- Secrets ---

    const secrets = secretsmanager.Secret.fromSecretNameV2(
      this,
      "Secrets",
      `coderhelm/${props.stage}/secrets`
    );

    // --- Gateway Lambda (Rust) ---

    const gatewayLogGroup = new logs.LogGroup(this, "GatewayLogGroup", {
      logGroupName: `/aws/lambda/${prefix}-gateway`,
      retention: logs.RetentionDays.ONE_MONTH,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    this.gatewayFunction = new lambda.Function(this, "Gateway", {
      functionName: `${prefix}-gateway`,
      runtime: lambda.Runtime.PROVIDED_AL2023,
      architecture: lambda.Architecture.ARM_64,
      handler: "bootstrap",
      code: lambda.Code.fromAsset(gatewayAssetPath),
      memorySize: 128,
      timeout: cdk.Duration.seconds(10),
      logGroup: gatewayLogGroup,
      environment: {
        STAGE: props.stage,
        TABLE_NAME: props.table.tableName,
        RUNS_TABLE_NAME: props.runsTable.tableName,
        ANALYTICS_TABLE_NAME: props.analyticsTable.tableName,
        BUCKET_NAME: props.bucket.bucketName,
        TICKET_QUEUE_URL: this.ticketQueue.queueUrl,
        CI_FIX_QUEUE_URL: this.ciFixQueue.queueUrl,
        FEEDBACK_QUEUE_URL: this.feedbackQueue.queueUrl,
        DLQ_URL: dlq.queueUrl,
        SECRETS_NAME: `coderhelm/${props.stage}/secrets`,
        SES_FROM_ADDRESS: "notifications@coderhelm.com",
        SES_TEMPLATE_PREFIX: `coderhelm-${props.stage}`,
        RUST_LOG: "info",
      },
    });

    // Permissions
    props.table.grantReadWriteData(this.gatewayFunction);
    props.runsTable.grantReadData(this.gatewayFunction);
    props.analyticsTable.grantReadData(this.gatewayFunction);
    props.bucket.grantRead(this.gatewayFunction);
    this.ticketQueue.grantSendMessages(this.gatewayFunction);
    this.ciFixQueue.grantSendMessages(this.gatewayFunction);
    this.feedbackQueue.grantSendMessages(this.gatewayFunction);
    dlq.grantConsumeMessages(this.gatewayFunction);
    secrets.grantRead(this.gatewayFunction);

    // --- HTTP API Gateway ---

    const httpApi = new apigatewayv2.HttpApi(this, "HttpApi", {
      apiName: `${prefix}-api`,
      corsPreflight: {
        allowOrigins: [
          props.stage === "prod"
            ? "https://app.coderhelm.com"
            : "http://localhost:3000",
        ],
        allowMethods: [
          apigatewayv2.CorsHttpMethod.GET,
          apigatewayv2.CorsHttpMethod.POST,
          apigatewayv2.CorsHttpMethod.PUT,
          apigatewayv2.CorsHttpMethod.DELETE,
          apigatewayv2.CorsHttpMethod.OPTIONS,
        ],
        allowHeaders: ["Content-Type", "Authorization", "Cookie"],
        allowCredentials: true,
      },
    });

    const lambdaIntegration =
      new integrations.HttpLambdaIntegration(
        "GatewayIntegration",
        this.gatewayFunction
      );

    // Webhook routes
    httpApi.addRoutes({
      path: "/webhooks/github",
      methods: [apigatewayv2.HttpMethod.POST],
      integration: lambdaIntegration,
    });

    httpApi.addRoutes({
      path: "/webhooks/jira",
      methods: [apigatewayv2.HttpMethod.POST],
      integration: lambdaIntegration,
    });

    httpApi.addRoutes({
      path: "/webhooks/stripe",
      methods: [apigatewayv2.HttpMethod.POST],
      integration: lambdaIntegration,
    });

    // Auth routes
    httpApi.addRoutes({
      path: "/auth/{proxy+}",
      methods: [apigatewayv2.HttpMethod.GET, apigatewayv2.HttpMethod.POST],
      integration: lambdaIntegration,
    });

    // Dashboard API routes
    httpApi.addRoutes({
      path: "/api/{proxy+}",
      methods: [
        apigatewayv2.HttpMethod.GET,
        apigatewayv2.HttpMethod.POST,
        apigatewayv2.HttpMethod.PUT,
        apigatewayv2.HttpMethod.DELETE,
      ],
      integration: lambdaIntegration,
    });

    // Outputs
    new cdk.CfnOutput(this, "ApiUrl", {
      value: httpApi.apiEndpoint,
    });

    // --- Custom domain: api.coderhelm.com ---
    if (props.stage === "prod") {
      const apiDomain = "api.coderhelm.com";

      const hostedZone = route53.HostedZone.fromLookup(this, "Zone", {
        domainName: "coderhelm.com",
      });

      const certificate = new acm.Certificate(this, "ApiCertificate", {
        domainName: apiDomain,
        validation: acm.CertificateValidation.fromDns(hostedZone),
      });

      const customDomain = new apigatewayv2.DomainName(this, "ApiCustomDomain", {
        domainName: apiDomain,
        certificate,
      });

      new apigatewayv2.ApiMapping(this, "ApiMapping", {
        api: httpApi,
        domainName: customDomain,
      });

      new route53.ARecord(this, "ApiAliasRecord", {
        zone: hostedZone,
        recordName: "api",
        target: route53.RecordTarget.fromAlias(
          new route53Targets.ApiGatewayv2DomainProperties(
            customDomain.regionalDomainName,
            customDomain.regionalHostedZoneId
          )
        ),
      });

      new cdk.CfnOutput(this, "ApiCustomDomainUrl", {
        value: `https://${apiDomain}`,
      });
    }
  }
}
