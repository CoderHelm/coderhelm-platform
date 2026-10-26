import * as cdk from "aws-cdk-lib";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as kms from "aws-cdk-lib/aws-kms";
import * as cr from "aws-cdk-lib/custom-resources";
import * as iam from "aws-cdk-lib/aws-iam";
import { Construct } from "constructs";
import { ResolvedConfig } from "./config";

interface DatabaseStackProps extends cdk.StackProps {
  stage: string;
  cfg: ResolvedConfig;
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
  public readonly awsInsightsTable: dynamodb.TableV2;
  public readonly waitlistTable: dynamodb.TableV2;
  public readonly tracesTable: dynamodb.TableV2;
  public readonly checkpointsTable: dynamodb.TableV2;
  public readonly testsTable: dynamodb.TableV2;
  public readonly encryptionKey: kms.Key;
  public readonly secretsKey: kms.Key;

  constructor(scope: Construct, id: string, props: DatabaseStackProps) {
    super(scope, id, props);

    const isProd = props.cfg.retainData;

    // KMS key for DynamoDB encryption
    this.encryptionKey = new kms.Key(this, "TableKey", {
      alias: `${props.cfg.prefix}-dynamo`,
      description: "Coderhelm DynamoDB encryption key",
      enableKeyRotation: true,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    // KMS key that seals the keys teams give Coderhelm for their own systems
    // (data sources). Separate from the table key so each service gets only the
    // direction it needs: the gateway seals and opens, the worker only opens.
    this.secretsKey = new kms.Key(this, "SecretsKey", {
      alias: `${props.cfg.prefix}-secrets`,
      description: "Coderhelm: seals team-provided keys for data sources",
      enableKeyRotation: true,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    // ──────────────────────────────────────────────
    // Main table: team identity (META, TEAM, WELCOME_SENT)
    // ──────────────────────────────────────────────
    this.table = new dynamodb.TableV2(this, "Table", {
      tableName: `${props.cfg.prefix}`,
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

    // ──────────────────────────────────────────────
    // Teams table: team records with connection metadata
    // PK = team_id (TEAM#<nanoid>), SK = META | CONNECTION#github | etc.
    // GSI: github_installation_id → team_id (for webhook lookup)
    // ──────────────────────────────────────────────
    this.teamsTable = new dynamodb.TableV2(this, "TeamsTable", {
      tableName: `${props.cfg.prefix}-teams`,
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

    // ──────────────────────────────────────────────
    // Runs table: code-review run records
    // PK = team_id, SK = run_id
    // GSI: status-index (team_id + status_run_id), repo-index (team_repo + run_id)
    // ──────────────────────────────────────────────
    this.runsTable = new dynamodb.TableV2(this, "RunsTableV2", {
      tableName: `${props.cfg.prefix}-runs`,
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

    // Webhook dedup: "does this ticket already have a run?" A key-condition
    // query here replaces the old team-partition scan whose Limit applied
    // before the ticket filter and silently checked only the 5 oldest runs.
    this.runsTable.addGlobalSecondaryIndex({
      indexName: "ticket-index",
      partitionKey: { name: "team_id", type: dynamodb.AttributeType.STRING },
      sortKey: { name: "ticket_id", type: dynamodb.AttributeType.STRING },
    });

    // ──────────────────────────────────────────────
    // Analytics table: usage analytics per team
    // PK = team_id, SK = period
    // ──────────────────────────────────────────────
    this.analyticsTable = new dynamodb.TableV2(this, "AnalyticsTableV2", {
      tableName: `${props.cfg.prefix}-analytics`,
      partitionKey: { name: "team_id", type: dynamodb.AttributeType.STRING },
      sortKey: { name: "period", type: dynamodb.AttributeType.STRING },
      billing: dynamodb.Billing.onDemand(),
      encryption: dynamodb.TableEncryptionV2.customerManagedKey(
        this.encryptionKey
      ),
      pointInTimeRecoverySpecification: {
        pointInTimeRecoveryEnabled: true,
      },
      // Only the once-only markers of model calls (`USAGE_CALL#<id>`) carry
      // `expires_at` (35 days); team counters never expire.
      timeToLiveAttribute: "expires_at",
      deletionProtection: isProd,
      removalPolicy: isProd
        ? cdk.RemovalPolicy.RETAIN
        : cdk.RemovalPolicy.DESTROY,
    });

    // ──────────────────────────────────────────────
    // Events table: ephemeral event data
    // TTL auto-cleanup for transient records
    // ──────────────────────────────────────────────
    this.eventsTable = new dynamodb.TableV2(this, "EventsTable", {
      tableName: `${props.cfg.prefix}-events`,
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
      tableName: `${props.cfg.prefix}-users`,
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
      tableName: `${props.cfg.prefix}-jira-tokens`,
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
      tableName: `${props.cfg.prefix}-jira-events`,
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
      tableName: `${props.cfg.prefix}-plans`,
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
      tableName: `${props.cfg.prefix}-jira-config`,
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
      tableName: `${props.cfg.prefix}-repos`,
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
      tableName: `${props.cfg.prefix}-settings`,
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
    // Tests table: E2E runs, tests, failure groups, recommendations
    // PK/SK patterns are documented in services/common/src/e2e/store.rs.
    // Runs and PR triage expire through `ttl`.
    // ──────────────────────────────────────────────
    this.testsTable = new dynamodb.TableV2(this, "TestsTable", {
      tableName: `${props.cfg.prefix}-tests`,
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
    // AWS Insights table: AWS connections + log analyzer recommendations
    // PK = team_id, SK = AWS_CONN#{account_id} or REC#{ulid}
    // Separated from settings to isolate cross-account IAM data
    // ──────────────────────────────────────────────
    this.awsInsightsTable = new dynamodb.TableV2(this, "AwsInsightsTable", {
      tableName: `${props.cfg.prefix}-aws-insights`,
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
      tableName: `${props.cfg.prefix}-mcp-configs`,
      partitionKey: { name: "pk", type: dynamodb.AttributeType.STRING },
      sortKey: { name: "sk", type: dynamodb.AttributeType.STRING },
      billing: dynamodb.Billing.onDemand(),
      // Remote MCP sign-ins in progress (ten minutes) and the tool-call audit
      // trail (90 days) carry `ttl`; server and token records never do.
      timeToLiveAttribute: "ttl",
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
      tableName: `${props.cfg.prefix}-waitlist`,
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
      tableName: `${props.cfg.prefix}-infra`,
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
      tableName: `${props.cfg.prefix}-banners`,
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
      tableName: `${props.cfg.prefix}-traces`,
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
      tableName: `${props.cfg.prefix}-checkpoints`,
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
    // Retained temporarily — CF export still referenced by previously deployed stacks.
    // Safe to remove after one successful deploy cycle.
    this.billingTable = new dynamodb.TableV2(this, "BillingTable", {
      tableName: `${props.cfg.prefix}-billing-data`,
      partitionKey: { name: "pk", type: dynamodb.AttributeType.STRING },
      sortKey: { name: "sk", type: dynamodb.AttributeType.STRING },
      billing: dynamodb.Billing.onDemand(),
      removalPolicy: cdk.RemovalPolicy.RETAIN,
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
    new cdk.CfnOutput(this, "AwsInsightsTableName", {
      value: this.awsInsightsTable.tableName,
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

    // Who may sign up: the config's admins, addresses and domains go on the
    // allowlist the sign-up check reads (settings table, pk ALLOWLIST).
    // Entries are only ever added here; remove one from the dashboard.
    // On a self-hosted install this is the first boot's list: the first admin
    // comes from it, and when the organization is made its other addresses
    // become invites and its domains the allowed domains (changed after that
    // in Settings, Sign-in). `kind` tells an admin from an invite.
    const admins = new Set(props.cfg.admins.map((a) => a.toLowerCase()));
    const allow: { sk: string; kind: string }[] = [
      ...props.cfg.signup.emails.map((e) => ({ sk: `EMAIL#${e}`, kind: admins.has(e) ? "admin" : "invite" })),
      ...props.cfg.signup.domains.map((d) => ({ sk: `EMAIL#*@${d}`, kind: "domain" })),
      ...(props.cfg.signup.requireTwoFactor ? [{ sk: "TWO_FACTOR", kind: "rule" }] : []),
    ];
    for (let i = 0; i < allow.length; i += 25) {
      const chunk = allow.slice(i, i + 25);
      const call: cr.AwsSdkCall = {
        service: "DynamoDB",
        action: "batchWriteItem",
        parameters: {
          RequestItems: {
            [this.settingsTable.tableName]: chunk.map(({ sk, kind }) => ({
              PutRequest: {
                Item: {
                  pk: { S: "ALLOWLIST" },
                  sk: { S: sk },
                  kind: { S: kind },
                  added_by: { S: "config" },
                  ...(sk === "TWO_FACTOR" ? { require: { BOOL: true } } : {}),
                },
              },
            })),
          },
        },
        physicalResourceId: cr.PhysicalResourceId.of(`allowlist-${chunk.map((c) => c.sk).join(",")}`.slice(0, 1000)),
      };
      const seed = new cr.AwsCustomResource(this, `SignupAllowlist${i / 25}`, {
        onCreate: call,
        onUpdate: call,
        policy: cr.AwsCustomResourcePolicy.fromStatements([
          new iam.PolicyStatement({
            actions: ["dynamodb:BatchWriteItem"],
            resources: [this.settingsTable.tableArn],
          }),
          new iam.PolicyStatement({
            actions: ["kms:Encrypt", "kms:Decrypt", "kms:GenerateDataKey*", "kms:DescribeKey"],
            resources: [this.encryptionKey.keyArn],
          }),
        ]),
        installLatestAwsSdk: false,
      });
    }
  }
}
