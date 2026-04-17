import * as cdk from "aws-cdk-lib";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as secretsmanager from "aws-cdk-lib/aws-secretsmanager";
import * as iam from "aws-cdk-lib/aws-iam";
import * as logs from "aws-cdk-lib/aws-logs";
import * as eventsources from "aws-cdk-lib/aws-lambda-event-sources";
import { Construct } from "constructs";

interface WorkerStackProps extends cdk.StackProps {
  stage: string;
  table: dynamodb.TableV2;
  usersTable: dynamodb.TableV2;
  plansTable: dynamodb.TableV2;
  jiraConfigTable: dynamodb.TableV2;
  reposTable: dynamodb.TableV2;
  settingsTable: dynamodb.TableV2;
  infraTable: dynamodb.TableV2;
  billingTable: dynamodb.TableV2; // Retained — cross-stack ref still needed for CF migration
  mcpConfigsTable: dynamodb.TableV2;
  tracesTable: dynamodb.TableV2;
  checkpointsTable: dynamodb.TableV2;
  bucket: s3.Bucket;
  ticketQueue: sqs.Queue;
  ciFixQueue: sqs.Queue;
  feedbackQueue: sqs.Queue;
  mcpProxyFunction: lambda.Function;
}

export class WorkerStack extends cdk.Stack {
  public readonly workerFunction: lambda.Function;

  constructor(scope: Construct, id: string, props: WorkerStackProps) {
    super(scope, id, props);

    const prefix = `coderhelm-${props.stage}`;
    const workerAssetPath =
      process.env.WORKER_ZIP ?? "../services/worker/target/lambda/worker";

    const secrets = secretsmanager.Secret.fromSecretNameV2(
      this,
      "Secrets",
      `coderhelm/${props.stage}/secrets`
    );

    // --- Worker Lambda (Rust) ---

    const workerLogGroup = new logs.LogGroup(this, "WorkerLogGroup", {
      logGroupName: `/aws/lambda/${prefix}-worker`,
      retention: logs.RetentionDays.ONE_MONTH,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    this.workerFunction = new lambda.Function(this, "Worker", {
      functionName: `${prefix}-worker`,
      runtime: lambda.Runtime.PROVIDED_AL2023,
      architecture: lambda.Architecture.ARM_64,
      handler: "bootstrap",
      code: lambda.Code.fromAsset(workerAssetPath),
      memorySize: 512,
      timeout: cdk.Duration.minutes(15),
      ephemeralStorageSize: cdk.Size.mebibytes(1024),
      logGroup: workerLogGroup,
        environment: {
        STAGE: props.stage,
        MEMORY_ENABLED: "true",
        TABLE_NAME: props.table.tableName,
        TEAMS_TABLE_NAME: `coderhelm-${props.stage}-teams`,
        RUNS_TABLE_NAME: `coderhelm-${props.stage}-runs`,
        ANALYTICS_TABLE_NAME: `coderhelm-${props.stage}-analytics`,
        USERS_TABLE_NAME: props.usersTable.tableName,
        PLANS_TABLE_NAME: props.plansTable.tableName,
        JIRA_CONFIG_TABLE_NAME: props.jiraConfigTable.tableName,
        REPOS_TABLE_NAME: props.reposTable.tableName,
        SETTINGS_TABLE_NAME: props.settingsTable.tableName,
        MCP_CONFIGS_TABLE_NAME: props.mcpConfigsTable.tableName,
        INFRA_TABLE_NAME: props.infraTable.tableName,
        TRACES_TABLE_NAME: props.tracesTable.tableName,
        CHECKPOINTS_TABLE_NAME: props.checkpointsTable.tableName,
        BUCKET_NAME: props.bucket.bucketName,
        SECRETS_NAME: `coderhelm/${props.stage}/secrets`,
        MODEL_ID: process.env.MODEL_ID || "claude-sonnet-4-20250514",
        LIGHT_MODEL_ID: process.env.LIGHT_MODEL_ID || "claude-sonnet-4-20250514",
        SES_FROM_ADDRESS: "noreply@coderhelm.com",
        SES_TEMPLATE_PREFIX: `coderhelm-${props.stage}`,
        RUST_LOG: "info",
      },
    });

    // DynamoDB: read/write all tables
    props.table.grantReadWriteData(this.workerFunction);

    // Decoupled tables — use fromTableName to avoid cross-stack exports
    const teamsTable = dynamodb.TableV2.fromTableName(this, "TeamsTableRef", `coderhelm-${props.stage}-teams`);
    const runsTable = dynamodb.TableV2.fromTableName(this, "RunsTableRef", `coderhelm-${props.stage}-runs`);
    const analyticsTable = dynamodb.TableV2.fromTableName(this, "AnalyticsTableRef", `coderhelm-${props.stage}-analytics`);
    teamsTable.grantReadWriteData(this.workerFunction);
    runsTable.grantReadWriteData(this.workerFunction);
    // fromTableName doesn't know about GSIs, so grant index access explicitly
    this.workerFunction.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["dynamodb:Query"],
        resources: [`arn:aws:dynamodb:${this.region}:${this.account}:table/coderhelm-${props.stage}-runs/index/*`],
      })
    );
    analyticsTable.grantReadWriteData(this.workerFunction);

    props.usersTable.grantReadData(this.workerFunction);
    props.plansTable.grantReadWriteData(this.workerFunction);
    props.jiraConfigTable.grantReadData(this.workerFunction);
    props.reposTable.grantReadWriteData(this.workerFunction);
    props.settingsTable.grantReadWriteData(this.workerFunction);
    props.mcpConfigsTable.grantReadData(this.workerFunction);
    props.infraTable.grantReadWriteData(this.workerFunction);
    props.billingTable.grantReadWriteData(this.workerFunction); // Retained for CF migration
    props.tracesTable.grantReadWriteData(this.workerFunction);
    props.checkpointsTable.grantReadWriteData(this.workerFunction);
    props.bucket.grantReadWrite(this.workerFunction);
    secrets.grantRead(this.workerFunction);

    // Bedrock: converse + invoke model
    this.workerFunction.addToRolePolicy(
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

    // SQS event sources — one message at a time for isolation
    this.workerFunction.addEventSource(
      new eventsources.SqsEventSource(props.ticketQueue, {
        batchSize: 1,
        maxConcurrency: 10,
      })
    );

    this.workerFunction.addEventSource(
      new eventsources.SqsEventSource(props.ciFixQueue, {
        batchSize: 1,
        maxConcurrency: 5,
      })
    );

    this.workerFunction.addEventSource(
      new eventsources.SqsEventSource(props.feedbackQueue, {
        batchSize: 1,
        maxConcurrency: 5,
      })
    );

    // Grant worker permission to invoke the MCP proxy (owned by api-stack)
    props.mcpProxyFunction.grantInvoke(this.workerFunction);
    this.workerFunction.addEnvironment(
      "MCP_PROXY_FUNCTION_NAME",
      props.mcpProxyFunction.functionName
    );
  }
}
