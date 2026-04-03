import * as cdk from "aws-cdk-lib";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as kms from "aws-cdk-lib/aws-kms";
import { Construct } from "constructs";

interface DatabaseStackProps extends cdk.StackProps {
  stage: string;
}

export class DatabaseStack extends cdk.Stack {
  public readonly table: dynamodb.TableV2;
  public readonly teamsTable: dynamodb.TableV2;
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
  public readonly mcpConfigsTable: dynamodb.TableV2;
  public readonly waitlistTable: dynamodb.TableV2;
  public readonly tracesTable: dynamodb.TableV2;
  public readonly checkpointsTable: dynamodb.TableV2;
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
    // Main table: team identity (META, TEAM, WELCOME_SENT)
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

    // GSI1: Look up teams by github_id (for OAuth login)
    this.table.addGlobalSecondaryIndex({
      indexName: "gsi1",
      partitionKey: { name: "gsi1pk", type: dynamodb.AttributeType.STRING },
      sortKey: { name: "gsi1sk", type: dynamodb.AttributeType.STRING },
    });

    // GSI2: Look up teams by stripe_customer_id (for Stripe webhooks)
    this.table.addGlobalSecondaryIndex({
      indexName: "gsi2",
      partitionKey: { name: "gsi2pk", type: dynamodb.AttributeType.STRING },
      sortKey: { name: "gsi2sk", type: dynamodb.AttributeType.STRING },
    });

    // ──────────────────────────────────────────────
    // Teams table: team records with connection metadata
    // PK = team_id (TEAM#<nanoid>), SK = META | CONNECTION#github | etc.
    // GSI: github_installation_id → team_id (for webhook lookup)
    // ──────────────────────────────────────────────
    this.teamsTable = new dynamodb.TableV2(this, "TeamsTable", {
      tableName: `coderhelm-${props.stage}-teams`,
      partitionKey: { name: "team_id", type: dynamodb.AttributeType.STRING },
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

    // GSI: Look up team by GitHub installation ID (for webhooks)
    this.teamsTable.addGlobalSecondaryIndex({
      indexName: "github-installation-index",
      partitionKey: {
        name: "github_installation_id",
        type: dynamodb.AttributeType.NUMBER,
      },
    });

    // GSI: Look up team by Stripe customer ID (for Stripe webhooks)
    this.teamsTable.addGlobalSecondaryIndex({
      indexName: "stripe-customer-index",
      partitionKey: {
        name: "stripe_customer_id",
        type: dynamodb.AttributeType.STRING,
      },
    });

    // ──────────────────────────────────────────────
    // Runs table: code-review run records
    // PK = team_id, SK = run_id
    // GSI: status-index (team_id + status_run_id), repo-index (team_repo + run_id)
    // ──────────────────────────────────────────────
    this.runsTable = new dynamodb.TableV2(this, "RunsTableV2", {
      tableName: `coderhelm-${props.stage}-runs`,
      partitionKey: { name: "team_id", type: dynamodb.AttributeType.STRING },
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

    this.runsTable.addGlobalSecondaryIndex({
      indexName: "status-index",
      partitionKey: { name: "team_id", type: dynamodb.AttributeType.STRING },
      sortKey: {
        name: "status_run_id",
        type: dynamodb.AttributeType.STRING,
      },
    });

    this.runsTable.addGlobalSecondaryIndex({
      indexName: "repo-index",
      partitionKey: {
        name: "team_repo",
        type: dynamodb.AttributeType.STRING,
      },
      sortKey: { name: "run_id", type: dynamodb.AttributeType.STRING },
    });

    // ──────────────────────────────────────────────
    // Analytics table: usage analytics per team
    // PK = team_id, SK = period
    // ──────────────────────────────────────────────
    this.analyticsTable = new dynamodb.TableV2(this, "AnalyticsTableV2", {
      tableName: `coderhelm-${props.stage}-analytics`,
      partitionKey: { name: "team_id", type: dynamodb.AttributeType.STRING },
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
    // Users table: user records per team
    // PK = team_id, SK = USER#<github_id>
    // GSI1: reverse lookup by github_id → team
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

    // GSI1: Look up user by github_id (for OAuth login team resolution)
    this.usersTable.addGlobalSecondaryIndex({
      indexName: "gsi1",
      partitionKey: { name: "gsi1pk", type: dynamodb.AttributeType.STRING },
      sortKey: { name: "gsi1sk", type: dynamodb.AttributeType.STRING },
    });

    // GSI2: Look up user by email (for Cognito login team resolution)
    this.usersTable.addGlobalSecondaryIndex({
      indexName: "gsi2",
      partitionKey: { name: "gsi2pk", type: dynamodb.AttributeType.STRING },
      sortKey: { name: "gsi2sk", type: dynamodb.AttributeType.STRING },
    });

    // ──────────────────────────────────────────────
    // Jira tokens table: opaque webhook token → team mapping
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
    // Jira events table: Jira webhook event log
    // PK = team_id, SK = event_id
    // ──────────────────────────────────────────────
    this.jiraEventsTable = new dynamodb.TableV2(this, "JiraEventsTableV2", {
      tableName: `coderhelm-${props.stage}-jira-events`,
      partitionKey: { name: "team_id", type: dynamodb.AttributeType.STRING },
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
    // PK = team_id, SK = PLAN#<plan_id> or PLAN#<plan_id>#TASK#<task_id>
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
    // PK = team_id, SK = config key
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
    // PK = team_id, SK = REPO#<owner>/<repo>
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
    // PK = team_id, SK = setting key
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
      timeToLiveAttribute: "ttl",
      deletionProtection: isProd,
      removalPolicy: isProd
        ? cdk.RemovalPolicy.RETAIN
        : cdk.RemovalPolicy.DESTROY,
    });

    // ──────────────────────────────────────────────
    // MCP Configs table: plugin enable/disable state + encrypted credentials
    // PK = team_id, SK = PLUGIN#{plugin_id}
    // Separated from settings to isolate secret material
    // ──────────────────────────────────────────────
    this.mcpConfigsTable = new dynamodb.TableV2(this, "McpConfigsTable", {
      tableName: `coderhelm-${props.stage}-mcp-configs`,
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
    // Waitlist table: beta waitlist signups
    // PK = EMAIL#{normalized_email}
    // ──────────────────────────────────────────────
    this.waitlistTable = new dynamodb.TableV2(this, "WaitlistTable", {
      tableName: `coderhelm-${props.stage}-waitlist`,
      partitionKey: { name: "email", type: dynamodb.AttributeType.STRING },
      billing: dynamodb.Billing.onDemand(),
      encryption: dynamodb.TableEncryptionV2.customerManagedKey(
        this.encryptionKey
      ),
      deletionProtection: isProd,
      removalPolicy: isProd
        ? cdk.RemovalPolicy.RETAIN
        : cdk.RemovalPolicy.DESTROY,
    });

    // ──────────────────────────────────────────────
    // Infra table: infrastructure analysis results
    // PK = team_id, SK = INFRA#analysis or INFRA#REPO#<owner>/<repo>
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
    // PK = team_id, SK = BILLING
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
    // PK = scope (BANNER#GLOBAL or BANNER#<team_id>), SK = banner_id
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

    // ──────────────────────────────────────────────
    // Traces table: per-pass observability traces
    // PK = team_id, SK = RUN#{run_id}#PASS#{pass}
    // TTL: 30 days
    // ──────────────────────────────────────────────
    this.tracesTable = new dynamodb.TableV2(this, "TracesTable", {
      tableName: `coderhelm-${props.stage}-traces`,
      partitionKey: { name: "team_id", type: dynamodb.AttributeType.STRING },
      sortKey: { name: "sk", type: dynamodb.AttributeType.STRING },
      billing: dynamodb.Billing.onDemand(),
      encryption: dynamodb.TableEncryptionV2.customerManagedKey(
        this.encryptionKey
      ),
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      timeToLiveAttribute: "ttl",
    });

    // ──────────────────────────────────────────────
    // Checkpoints table: pipeline resume state
    // PK = team_id, SK = RUN#{run_id}
    // TTL: 7 days
    // ──────────────────────────────────────────────
    this.checkpointsTable = new dynamodb.TableV2(this, "CheckpointsTable", {
      tableName: `coderhelm-${props.stage}-checkpoints`,
      partitionKey: { name: "team_id", type: dynamodb.AttributeType.STRING },
      sortKey: { name: "sk", type: dynamodb.AttributeType.STRING },
      billing: dynamodb.Billing.onDemand(),
      encryption: dynamodb.TableEncryptionV2.customerManagedKey(
        this.encryptionKey
      ),
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      timeToLiveAttribute: "ttl",
    });

    // Outputs
    new cdk.CfnOutput(this, "TableName", { value: this.table.tableName });
    new cdk.CfnOutput(this, "TableArn", { value: this.table.tableArn });
    new cdk.CfnOutput(this, "TeamsTableName", {
      value: this.teamsTable.tableName,
    });
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
    new cdk.CfnOutput(this, "McpConfigsTableName", {
      value: this.mcpConfigsTable.tableName,
    });
    new cdk.CfnOutput(this, "WaitlistTableName", {
      value: this.waitlistTable.tableName,
    });
    new cdk.CfnOutput(this, "TracesTableName", {
      value: this.tracesTable.tableName,
    });
    new cdk.CfnOutput(this, "CheckpointsTableName", {
      value: this.checkpointsTable.tableName,
    });
  }
}
