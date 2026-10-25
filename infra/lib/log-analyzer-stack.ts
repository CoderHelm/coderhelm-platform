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
import { Construct } from "constructs";
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
    // Decoupled by name (like the worker) to avoid a cross-stack export.
    dynamodb.TableV2.fromTableName(this, "AnalyticsTableRef", `${prefix}-analytics`)
      .grantWriteData(analyzerFunction);

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

    // --- EventBridge Schedule: every 6 hours ---
    new events.Rule(this, "AnalyzerSchedule", {
      ruleName: `${prefix}-log-analyzer-schedule`,
      schedule: events.Schedule.rate(cdk.Duration.hours(6)),
      targets: [new targets.LambdaFunction(analyzerFunction)],
    });
  }
}
