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
  runsTable: dynamodb.TableV2;
  analyticsTable: dynamodb.TableV2;
  usersTable: dynamodb.TableV2;
  plansTable: dynamodb.TableV2;
  jiraConfigTable: dynamodb.TableV2;
  reposTable: dynamodb.TableV2;
  settingsTable: dynamodb.TableV2;
  infraTable: dynamodb.TableV2;
  billingTable: dynamodb.TableV2;
  mcpConfigsTable: dynamodb.TableV2;
  bucket: s3.Bucket;
  ticketQueue: sqs.Queue;
  ciFixQueue: sqs.Queue;
  feedbackQueue: sqs.Queue;
}

export class WorkerStack extends cdk.Stack {
  public readonly workerFunction: lambda.Function;
  public readonly mcpProxyFunction: lambda.Function;

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
      memorySize: 256,
      timeout: cdk.Duration.minutes(15),
      logGroup: workerLogGroup,
        environment: {
        STAGE: props.stage,
        TABLE_NAME: props.table.tableName,
        RUNS_TABLE_NAME: props.runsTable.tableName,
        ANALYTICS_TABLE_NAME: props.analyticsTable.tableName,
        USERS_TABLE_NAME: props.usersTable.tableName,
        PLANS_TABLE_NAME: props.plansTable.tableName,
        JIRA_CONFIG_TABLE_NAME: props.jiraConfigTable.tableName,
        REPOS_TABLE_NAME: props.reposTable.tableName,
        SETTINGS_TABLE_NAME: props.settingsTable.tableName,
        MCP_CONFIGS_TABLE_NAME: props.mcpConfigsTable.tableName,
        INFRA_TABLE_NAME: props.infraTable.tableName,
        BILLING_TABLE_NAME: props.billingTable.tableName,
        BUCKET_NAME: props.bucket.bucketName,
        SECRETS_NAME: `coderhelm/${props.stage}/secrets`,
        MODEL_ID: process.env.MODEL_ID || "us.anthropic.claude-opus-4-6-v1",
        LIGHT_MODEL_ID: process.env.LIGHT_MODEL_ID || "us.anthropic.claude-sonnet-4-6",
        SES_FROM_ADDRESS: "noreply@coderhelm.com",
        SES_TEMPLATE_PREFIX: `coderhelm-${props.stage}`,
        RUST_LOG: "info",
      },
    });

    // DynamoDB: read/write all tables
    props.table.grantReadWriteData(this.workerFunction);
    props.runsTable.grantReadWriteData(this.workerFunction);
    props.analyticsTable.grantReadWriteData(this.workerFunction);
    props.usersTable.grantReadData(this.workerFunction);
    props.plansTable.grantReadWriteData(this.workerFunction);
    props.jiraConfigTable.grantReadData(this.workerFunction);
    props.reposTable.grantReadWriteData(this.workerFunction);
    props.settingsTable.grantReadWriteData(this.workerFunction);
    props.mcpConfigsTable.grantReadData(this.workerFunction);
    props.infraTable.grantReadWriteData(this.workerFunction);
    props.billingTable.grantReadWriteData(this.workerFunction);
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

    // --- MCP Proxy Lambda (Node.js) ---
    // Spawns MCP server processes via stdio to execute tool calls.
    // Invoked directly by gateway (plan_chat) and worker (passes).

    const mcpProxyLogGroup = new logs.LogGroup(this, "McpProxyLogGroup", {
      logGroupName: `/aws/lambda/${prefix}-mcp-proxy`,
      retention: logs.RetentionDays.ONE_MONTH,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    this.mcpProxyFunction = new lambda.Function(this, "McpProxy", {
      functionName: `${prefix}-mcp-proxy`,
      runtime: lambda.Runtime.NODEJS_20_X,
      architecture: lambda.Architecture.ARM_64,
      handler: "handler.handler",
      code: lambda.Code.fromAsset("../lambda/mcp-proxy/dist"),
      memorySize: 512,
      timeout: cdk.Duration.minutes(2),
      logGroup: mcpProxyLogGroup,
      environment: {
        BUCKET_NAME: props.bucket.bucketName,
        NODE_ENV: "production",
      },
    });

    // MCP proxy needs S3 write for tool schema caching
    props.bucket.grantReadWrite(this.mcpProxyFunction);

    // Grant both gateway and worker permission to invoke the MCP proxy
    this.mcpProxyFunction.grantInvoke(this.workerFunction);

    // Pass MCP proxy function name to worker as env var
    this.workerFunction.addEnvironment(
      "MCP_PROXY_FUNCTION_NAME",
      this.mcpProxyFunction.functionName
    );
  }
}
