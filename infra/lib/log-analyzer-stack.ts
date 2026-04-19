import * as cdk from "aws-cdk-lib";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as events from "aws-cdk-lib/aws-events";
import * as targets from "aws-cdk-lib/aws-events-targets";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as iam from "aws-cdk-lib/aws-iam";
import * as logs from "aws-cdk-lib/aws-logs";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as s3deploy from "aws-cdk-lib/aws-s3-deployment";
import { Construct } from "constructs";
import * as path from "path";

interface LogAnalyzerStackProps extends cdk.StackProps {
  stage: string;
  awsInsightsTable: dynamodb.TableV2;
  settingsTable: dynamodb.TableV2;
}

export class LogAnalyzerStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: LogAnalyzerStackProps) {
    super(scope, id, props);

    const prefix = `coderhelm-${props.stage}`;

    // --- Public S3 bucket for CloudFormation template ---
    const publicBucket = new s3.Bucket(this, "PublicBucket", {
      bucketName: "coderhelm-public",
      publicReadAccess: true,
      blockPublicAccess: new s3.BlockPublicAccess({
        blockPublicAcls: false,
        ignorePublicAcls: false,
        blockPublicPolicy: false,
        restrictPublicBuckets: false,
      }),
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    // Deploy the CFn template to the public bucket
    new s3deploy.BucketDeployment(this, "DeployCfnTemplate", {
      sources: [s3deploy.Source.asset(path.join(__dirname, "../cfn"))],
      destinationBucket: publicBucket,
      destinationKeyPrefix: "cfn",
    });

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
        path.join(__dirname, "../../lambda/log-analyzer")
      ),
      memorySize: 256,
      timeout: cdk.Duration.minutes(10),
      logGroup: analyzerLogGroup,
      environment: {
        AWS_INSIGHTS_TABLE_NAME: props.awsInsightsTable.tableName,
        SETTINGS_TABLE_NAME: props.settingsTable.tableName,
        MODEL_ID: "claude-sonnet-4-20250514",
        CODERHELM_ACCOUNT_ID: this.account,
        LOOKBACK_HOURS: "6",
      },
    });

    // DynamoDB permissions
    props.awsInsightsTable.grantReadWriteData(analyzerFunction);
    props.settingsTable.grantReadData(analyzerFunction);

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
