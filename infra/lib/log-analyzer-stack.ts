import * as cdk from "aws-cdk-lib";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as events from "aws-cdk-lib/aws-events";
import * as targets from "aws-cdk-lib/aws-events-targets";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as iam from "aws-cdk-lib/aws-iam";
import * as kms from "aws-cdk-lib/aws-kms";
import * as logs from "aws-cdk-lib/aws-logs";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as s3deploy from "aws-cdk-lib/aws-s3-deployment";
import * as sqs from "aws-cdk-lib/aws-sqs";
import { Construct } from "constructs";
import { LOG_ANALYZER_ROLE, RUNTIME_ROLE_TAG } from "./access";
import { ResolvedConfig } from "./config";
import { repoPath } from "./paths";
import { modelProviderEnvironment, modelPolicyStatements } from "./service-env";
import * as path from "path";

interface LogAnalyzerStackProps extends cdk.StackProps {
  stage: string;
  cfg: ResolvedConfig;
  /** The dashboard's URL (links in alert messages). */
  appUrl: string;
  awsInsightsTable: dynamodb.TableV2;
  settingsTable: dynamodb.TableV2;
  /** Opens the sealed Teams/Slack webhooks new findings post to. */
  secretsKey: kms.Key;
  /** New and resolved findings go here as webhook events (log_finding.*). */
  webhookQueue: sqs.IQueue;
  /** The worker's queue: a watch that stores a new finding asks the
   *  automatic-fix sweep to act now ({"type": "log_autofix"}). */
  ticketQueue: sqs.IQueue;
}

export class LogAnalyzerStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: LogAnalyzerStackProps) {
    super(scope, id, props);

    const prefix = `${props.cfg.prefix}`;

    // --- Public S3 bucket for the CloudFormation templates (optional) ---
    // The hosted deployment publishes infra/cfn here; a self-hosted one
    // links to that public copy (logReaderTemplateUrl) unless it names its
    // own bucket.
    if (props.cfg.publicBucketName) {
      const publicBucket = new s3.Bucket(this, "PublicBucket", {
        bucketName: props.cfg.publicBucketName,
        publicReadAccess: true,
        blockPublicAccess: new s3.BlockPublicAccess({
          blockPublicAcls: false,
          ignorePublicAcls: false,
          blockPublicPolicy: false,
          restrictPublicBuckets: false,
        }),
        // The install guide (docs.coderhelm.com) reads launch/latest.json
        // from the browser; the Launch Stack templates under launch/<tag>/
        // are written by the release workflow, not by this stack.
        cors: [{ allowedMethods: [s3.HttpMethods.GET, s3.HttpMethods.HEAD], allowedOrigins: ["*"], maxAge: 300 }],
        removalPolicy: cdk.RemovalPolicy.RETAIN,
      });

      // Deploy the CFn template to the public bucket
      new s3deploy.BucketDeployment(this, "DeployCfnTemplate", {
        sources: [s3deploy.Source.asset(repoPath("infra/cfn"))],
        destinationBucket: publicBucket,
        destinationKeyPrefix: "cfn",
      });
    }

    // --- Log Analyzer Lambda ---
    const analyzerLogGroup = new logs.LogGroup(this, "AnalyzerLogGroup", {
      logGroupName: `/aws/lambda/${prefix}-log-analyzer`,
      retention: logs.RetentionDays.ONE_MONTH,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const analyzerFunction = new lambda.Function(this, "LogAnalyzer", {
      functionName: `${prefix}-log-analyzer`,
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: "handler.handler",
      code: lambda.Code.fromAsset(
        repoPath("lambda/log-analyzer")
      ),
      memorySize: 256,
      timeout: cdk.Duration.minutes(10),
      logGroup: analyzerLogGroup,
      environment: {
        AWS_INSIGHTS_TABLE_NAME: props.awsInsightsTable.tableName,
        SETTINGS_TABLE_NAME: props.settingsTable.tableName,
        // Team usage counters: every analysis call is counted (tokens + cost).
        ANALYTICS_TABLE_NAME: `${prefix}-analytics`,
        MODEL_ID: props.cfg.models.light,
        CODERHELM_ACCOUNT_ID: this.account,
        LOOKBACK_HOURS: "6",
        DASHBOARD_URL: props.appUrl,
        WEBHOOK_QUEUE_URL: props.webhookQueue.queueUrl,
        TICKET_QUEUE_URL: props.ticketQueue.queueUrl,
        WATCH_MINUTES: "5",
        ...modelProviderEnvironment(props.cfg),
      },
    });

    for (const st of modelPolicyStatements(props.cfg, this)) analyzerFunction.addToRolePolicy(st);

    // DynamoDB permissions
    props.awsInsightsTable.grantReadWriteData(analyzerFunction);
    props.settingsTable.grantReadData(analyzerFunction);
    // Webhook URLs are sealed with the secrets key (bound to the team), like
    // the worker's: the analyzer only opens them.
    props.secretsKey.grantDecrypt(analyzerFunction);
    props.webhookQueue.grantSendMessages(analyzerFunction);
    props.ticketQueue.grantSendMessages(analyzerFunction);
    // Decoupled by name (like the worker) to avoid a cross-stack export.
    dynamodb.TableV2.fromTableName(this, "AnalyticsTableRef", `${prefix}-analytics`)
      .grantReadWriteData(analyzerFunction);

    // STS AssumeRole — the analyzer needs to assume roles in customer accounts
    analyzerFunction.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["sts:AssumeRole"],
        resources: ["arn:aws:iam::*:role/CoderHelmLogReader"],
        conditions: {
          StringLike: {
            "sts:ExternalId": "*",
          },
        },
      })
    );

    // On a hardened install in the same account as the logs it reads, the
    // boundary lets this tag take on the read roles there
    // (infra/lib/access.ts READ_ROLE_CALLERS); the statement above keeps it
    // to the log reader.
    cdk.Tags.of(analyzerFunction.role!).add(RUNTIME_ROLE_TAG, LOG_ANALYZER_ROLE);

    // --- EventBridge Schedule: every 6 hours ---
    new events.Rule(this, "AnalyzerSchedule", {
      ruleName: `${prefix}-log-analyzer-schedule`,
      schedule: events.Schedule.rate(cdk.Duration.hours(6)),
      targets: [new targets.LambdaFunction(analyzerFunction)],
    });

    // --- Watch: every 5 minutes ---
    // One count of error lines per log group over the last 5 minutes (a
    // Logs Insights query per 50 groups, in the team's account), and a
    // focused analysis at once for a group that spiked, so a new error is
    // seen in minutes instead of at the next 6-hourly analysis. Insights
    // bills the team's account $0.005 per GB read: the watch reads each
    // log byte about once, so about $1 a month per 200 GB of logs a month.
    new events.Rule(this, "AnalyzerWatch", {
      ruleName: `${prefix}-log-analyzer-watch`,
      description: "Count error lines per log group and analyze spikes at once",
      schedule: events.Schedule.rate(cdk.Duration.minutes(5)),
      targets: [
        new targets.LambdaFunction(analyzerFunction, {
          event: events.RuleTargetInput.fromObject({ mode: "watch" }),
          retryAttempts: 0,
        }),
      ],
    });
  }
}
