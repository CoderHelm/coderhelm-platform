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
  public readonly eventsTable: dynamodb.TableV2;
  public readonly usersTable: dynamodb.TableV2;
  public readonly jiraTokensTable: dynamodb.TableV2;
  public readonly jiraEventsTable: dynamodb.TableV2;
  public readonly plansTable: dynamodb.TableV2;
  public readonly jiraConfigTable: dynamodb.TableV2;
  public readonly reposTable: dynamodb.TableV2;
  public readonly settingsTable: dynamodb.TableV2;
  public readonly infraTable: dynamodb.TableV2;
  public readonly billingTable: dynamodb.TableV2;
  public readonly bannersTable: dynamodb.TableV2;
  public readonly encryptionKey: kms.Key;

  constructor(scope: Construct, id: string, props: DatabaseStackProps) {
    super(scope, id, props);

    const isProd = props.stage === "prod";

    // KMS key for DynamoDB encryption
    this.encryptionKey = new kms.Key(this, "TableKey", {
      alias: `coderhelm-${props.stage}-dynamo`,
      description: "Coderhelm DynamoDB encryption key",
      enableKeyRotation: true,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    // ──────────────────────────────────────────────
    // Main table: tenant identity only (META, TENANT, WELCOME_SENT)
    // ──────────────────────────────────────────────
    this.table = new dynamodb.TableV2(this, "Table", {
      tableName: `coderhelm-${props.stage}`,
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
      tableName: `coderhelm-${props.stage}-runs`,
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
      tableName: `coderhelm-${props.stage}-analytics`,
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

    // ──────────────────────────────────────────────
    // Events table: ephemeral Stripe data
    // STRIPE_EVENTS idempotency, PAYMENT#, INVOICE#, STRIPE# mapping
    // TTL auto-cleanup for transient records
    // ──────────────────────────────────────────────
    this.eventsTable = new dynamodb.TableV2(this, "EventsTable", {
      tableName: `coderhelm-${props.stage}-events`,
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

    // ──────────────────────────────────────────────
    // Users table: user records per tenant
    // PK = tenant_id, SK = USER#<github_id>
    // GSI1: reverse lookup by github_id → tenant
    // ──────────────────────────────────────────────
    this.usersTable = new dynamodb.TableV2(this, "UsersTable", {
      tableName: `coderhelm-${props.stage}-users`,
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
    });

    // GSI1: Look up user by github_id (for OAuth login tenant resolution)
    this.usersTable.addGlobalSecondaryIndex({
      indexName: "gsi1",
      partitionKey: { name: "gsi1pk", type: dynamodb.AttributeType.STRING },
      sortKey: { name: "gsi1sk", type: dynamodb.AttributeType.STRING },
    });

    // GSI2: Look up user by email (for Cognito login tenant resolution)
    this.usersTable.addGlobalSecondaryIndex({
      indexName: "gsi2",
      partitionKey: { name: "gsi2pk", type: dynamodb.AttributeType.STRING },
      sortKey: { name: "gsi2sk", type: dynamodb.AttributeType.STRING },
    });

    // ──────────────────────────────────────────────
    // Jira tokens table: opaque webhook token → tenant mapping
    // PK = token (the random 40-char string)
    // ──────────────────────────────────────────────
    this.jiraTokensTable = new dynamodb.TableV2(this, "JiraTokensTable", {
      tableName: `coderhelm-${props.stage}-jira-tokens`,
      partitionKey: { name: "token", type: dynamodb.AttributeType.STRING },
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

    // ──────────────────────────────────────────────
    // Jira events table: log every webhook event received
    // PK = tenant_id, SK = event_id (ULID — time-ordered)
    // TTL for auto-cleanup of old events
    // ──────────────────────────────────────────────
    this.jiraEventsTable = new dynamodb.TableV2(this, "JiraEventsTable", {
      tableName: `coderhelm-${props.stage}-jira-events`,
      partitionKey: {
        name: "tenant_id",
        type: dynamodb.AttributeType.STRING,
      },
      sortKey: { name: "event_id", type: dynamodb.AttributeType.STRING },
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

    // ──────────────────────────────────────────────
    // Plans table: plans + tasks
    // PK = tenant_id, SK = PLAN#<plan_id> or PLAN#<plan_id>#TASK#<task_id>
    // ──────────────────────────────────────────────
    this.plansTable = new dynamodb.TableV2(this, "PlansTable", {
      tableName: `coderhelm-${props.stage}-plans`,
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
    });

    // ──────────────────────────────────────────────
    // Jira config table: JIRA_SECRET, JIRA#config, JIRA#PROJECT#<key>
    // PK = tenant_id, SK = config key
    // ──────────────────────────────────────────────
    this.jiraConfigTable = new dynamodb.TableV2(this, "JiraConfigTable", {
      tableName: `coderhelm-${props.stage}-jira-config`,
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
    });

    // ──────────────────────────────────────────────
    // Repos table: repository records
    // PK = tenant_id, SK = REPO#<owner>/<repo>
    // ──────────────────────────────────────────────
    this.reposTable = new dynamodb.TableV2(this, "ReposTable", {
      tableName: `coderhelm-${props.stage}-repos`,
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
    });

    // ──────────────────────────────────────────────
    // Settings table: instructions, rules, voice, agents, notifications, budget, workflow
    // PK = tenant_id, SK = setting key
    // ──────────────────────────────────────────────
    this.settingsTable = new dynamodb.TableV2(this, "SettingsTable", {
      tableName: `coderhelm-${props.stage}-settings`,
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
    });

    // ──────────────────────────────────────────────
    // Infra table: infrastructure analysis results
    // PK = tenant_id, SK = INFRA#analysis or INFRA#REPO#<owner>/<repo>
    // ──────────────────────────────────────────────
    this.infraTable = new dynamodb.TableV2(this, "InfraTable", {
      tableName: `coderhelm-${props.stage}-infra`,
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
    });

    // ──────────────────────────────────────────────
    // Billing table: subscription & payment data
    // PK = tenant_id, SK = BILLING
    // ──────────────────────────────────────────────
    this.billingTable = new dynamodb.TableV2(this, "BillingTable", {
      tableName: `coderhelm-${props.stage}-billing-data`,
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
    });

    // ──────────────────────────────────────────────
    // Banners table: dynamic UI banners
    // PK = scope (BANNER#GLOBAL or BANNER#<tenant_id>), SK = banner_id
    // ──────────────────────────────────────────────
    this.bannersTable = new dynamodb.TableV2(this, "BannersTable", {
      tableName: `coderhelm-${props.stage}-banners`,
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

    // Outputs
    new cdk.CfnOutput(this, "TableName", { value: this.table.tableName });
    new cdk.CfnOutput(this, "TableArn", { value: this.table.tableArn });
    new cdk.CfnOutput(this, "RunsTableName", {
      value: this.runsTable.tableName,
    });
    new cdk.CfnOutput(this, "AnalyticsTableName", {
      value: this.analyticsTable.tableName,
    });
    new cdk.CfnOutput(this, "EventsTableName", {
      value: this.eventsTable.tableName,
    });
    new cdk.CfnOutput(this, "UsersTableName", {
      value: this.usersTable.tableName,
    });
    new cdk.CfnOutput(this, "JiraTokensTableName", {
      value: this.jiraTokensTable.tableName,
    });
    new cdk.CfnOutput(this, "JiraEventsTableName", {
      value: this.jiraEventsTable.tableName,
    });
    new cdk.CfnOutput(this, "PlansTableName", {
      value: this.plansTable.tableName,
    });
    new cdk.CfnOutput(this, "JiraConfigTableName", {
      value: this.jiraConfigTable.tableName,
    });
    new cdk.CfnOutput(this, "ReposTableName", {
      value: this.reposTable.tableName,
    });
    new cdk.CfnOutput(this, "SettingsTableName", {
      value: this.settingsTable.tableName,
    });
    new cdk.CfnOutput(this, "InfraTableName", {
      value: this.infraTable.tableName,
    });
    new cdk.CfnOutput(this, "BillingTableName", {
      value: this.billingTable.tableName,
    });
    new cdk.CfnOutput(this, "BannersTableName", {
      value: this.bannersTable.tableName,
    });
  }
}
