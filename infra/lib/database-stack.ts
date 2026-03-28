import * as cdk from "aws-cdk-lib";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as kms from "aws-cdk-lib/aws-kms";
import { Construct } from "constructs";

interface DatabaseStackProps extends cdk.StackProps {
  stage: string;
}

export class DatabaseStack extends cdk.Stack {
  public readonly table: dynamodb.TableV2;
  public readonly runsTable: dynamodb.TableV2;
  public readonly analyticsTable: dynamodb.TableV2;
  public readonly encryptionKey: kms.Key;

  constructor(scope: Construct, id: string, props: DatabaseStackProps) {
    super(scope, id, props);

    const isProd = props.stage === "prod";

    // KMS key for DynamoDB encryption
    this.encryptionKey = new kms.Key(this, "TableKey", {
      alias: `d3ftly-${props.stage}-dynamo`,
      description: "d3ftly DynamoDB encryption key",
      enableKeyRotation: true,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    // ──────────────────────────────────────────────
    // Main table: tenants, users, repos, instructions, notification prefs
    // Low-volume config data — single-table design
    // ──────────────────────────────────────────────
    this.table = new dynamodb.TableV2(this, "Table", {
      tableName: `d3ftly-${props.stage}`,
      partitionKey: { name: "pk", type: dynamodb.AttributeType.STRING },
      sortKey: { name: "sk", type: dynamodb.AttributeType.STRING },
      billing: dynamodb.Billing.onDemand(),
      encryption: dynamodb.TableEncryptionV2.customerManagedKey(
        this.encryptionKey
      ),
      pointInTimeRecoverySpecification: {
        pointInTimeRecoveryEnabled: true,
      },
      deletionProtection: isProd,
      removalPolicy: isProd
        ? cdk.RemovalPolicy.RETAIN
        : cdk.RemovalPolicy.DESTROY,
      timeToLiveAttribute: "expires_at",
    });

    // GSI1: Look up tenants by github_id (for OAuth login)
    this.table.addGlobalSecondaryIndex({
      indexName: "gsi1",
      partitionKey: { name: "gsi1pk", type: dynamodb.AttributeType.STRING },
      sortKey: { name: "gsi1sk", type: dynamodb.AttributeType.STRING },
    });

    // GSI2: Look up tenants by stripe_customer_id (for Stripe webhooks)
    this.table.addGlobalSecondaryIndex({
      indexName: "gsi2",
      partitionKey: { name: "gsi2pk", type: dynamodb.AttributeType.STRING },
      sortKey: { name: "gsi2sk", type: dynamodb.AttributeType.STRING },
    });

    // ──────────────────────────────────────────────
    // Runs table: high-volume run records
    // PK = tenant_id, SK = run_id (ULID — time-ordered)
    // Designed for millions of records per tenant
    // ──────────────────────────────────────────────
    this.runsTable = new dynamodb.TableV2(this, "RunsTable", {
      tableName: `d3ftly-${props.stage}-runs`,
      partitionKey: {
        name: "tenant_id",
        type: dynamodb.AttributeType.STRING,
      },
      sortKey: { name: "run_id", type: dynamodb.AttributeType.STRING },
      billing: dynamodb.Billing.onDemand(),
      encryption: dynamodb.TableEncryptionV2.customerManagedKey(
        this.encryptionKey
      ),
      pointInTimeRecoverySpecification: {
        pointInTimeRecoveryEnabled: true,
      },
      deletionProtection: isProd,
      removalPolicy: isProd
        ? cdk.RemovalPolicy.RETAIN
        : cdk.RemovalPolicy.DESTROY,
      timeToLiveAttribute: "expires_at",
    });

    // GSI: query runs by status (e.g. all currently running)
    this.runsTable.addGlobalSecondaryIndex({
      indexName: "status-index",
      partitionKey: {
        name: "tenant_id",
        type: dynamodb.AttributeType.STRING,
      },
      sortKey: { name: "status_run_id", type: dynamodb.AttributeType.STRING },
    });

    // GSI: look up run by repo + run_id (for CI fix / feedback lookups)
    this.runsTable.addGlobalSecondaryIndex({
      indexName: "repo-index",
      partitionKey: {
        name: "tenant_repo",
        type: dynamodb.AttributeType.STRING,
      },
      sortKey: { name: "run_id", type: dynamodb.AttributeType.STRING },
    });

    // ──────────────────────────────────────────────
    // Analytics table: pre-computed aggregates
    // PK = tenant_id, SK = period (e.g. "2026-03", "ALL_TIME")
    // Atomic counters — O(1) reads for dashboard stats
    // ──────────────────────────────────────────────
    this.analyticsTable = new dynamodb.TableV2(this, "AnalyticsTable", {
      tableName: `d3ftly-${props.stage}-analytics`,
      partitionKey: {
        name: "tenant_id",
        type: dynamodb.AttributeType.STRING,
      },
      sortKey: { name: "period", type: dynamodb.AttributeType.STRING },
      billing: dynamodb.Billing.onDemand(),
      encryption: dynamodb.TableEncryptionV2.customerManagedKey(
        this.encryptionKey
      ),
      pointInTimeRecoverySpecification: {
        pointInTimeRecoveryEnabled: true,
      },
      deletionProtection: isProd,
      removalPolicy: isProd
        ? cdk.RemovalPolicy.RETAIN
        : cdk.RemovalPolicy.DESTROY,
    });

    // Outputs
    new cdk.CfnOutput(this, "TableName", { value: this.table.tableName });
    new cdk.CfnOutput(this, "TableArn", { value: this.table.tableArn });
    new cdk.CfnOutput(this, "RunsTableName", {
      value: this.runsTable.tableName,
    });
    new cdk.CfnOutput(this, "AnalyticsTableName", {
      value: this.analyticsTable.tableName,
    });
  }
}
