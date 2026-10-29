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
import * as kms from "aws-cdk-lib/aws-kms";
import * as logs from "aws-cdk-lib/aws-logs";
import * as cognito from "aws-cdk-lib/aws-cognito";
import * as ses from "aws-cdk-lib/aws-ses";
import * as events from "aws-cdk-lib/aws-events";
import * as targets from "aws-cdk-lib/aws-events-targets";
import * as scheduler from "aws-cdk-lib/aws-scheduler";
import { Construct } from "constructs";
import { ResolvedConfig } from "./config";
import { repoPath } from "./paths";
import { Hybrid } from "./hybrid";
import { serviceEnvironment, modelPolicyStatements, publicUrlReadStatement, recordNameIn, setupEnvironment, setupStatements, updaterInvokeStatement, weeklySummaryScheduled } from "./service-env";
import { MailRelay, relaySendStatement } from "./mail-relay";

interface ApiStackProps extends cdk.StackProps {
  stage: string;
  cfg: ResolvedConfig;
  table: dynamodb.TableV2;
  eventsTable: dynamodb.TableV2;
  usersTable: dynamodb.TableV2;
  jiraTokensTable: dynamodb.TableV2;
  plansTable: dynamodb.TableV2;
  jiraConfigTable: dynamodb.TableV2;
  reposTable: dynamodb.TableV2;
  settingsTable: dynamodb.TableV2;
  secretsKey: kms.Key;
  infraTable: dynamodb.TableV2;
  billingTable: dynamodb.TableV2; // Retained — cross-stack ref still needed for CF migration
  bannersTable: dynamodb.TableV2;
  mcpConfigsTable: dynamodb.TableV2;
  awsInsightsTable: dynamodb.TableV2;
  testsTable: dynamodb.TableV2;
  waitlistTable: dynamodb.TableV2;
  bucket: s3.Bucket;
}

export class ApiStack extends cdk.Stack {
  public readonly gatewayFunction: lambda.Function;
  public readonly mcpProxyFunction: lambda.Function;
  public readonly ticketQueue: sqs.Queue;
  /** The mail relay's bounces and complaints (hosted side of hybrid only). */
  public readonly ciFixQueue: sqs.Queue;
  public readonly feedbackQueue: sqs.Queue;
  /** Outbound webhook events and their retries (common::webhooks). */
  public readonly webhookQueue: sqs.Queue;
  /** The shared dead-letter queue (the worker's DLQ cleanup reads it). */
  public readonly dlq: sqs.Queue;
  /** The HTTP API (the single-origin web stack forwards to it). */
  public readonly httpApi: apigatewayv2.HttpApi;

  constructor(scope: Construct, id: string, props: ApiStackProps) {
    super(scope, id, props);

    const cfg = props.cfg;
    const prefix = cfg.prefix;
    const gatewayAssetPath =
      process.env.GATEWAY_ZIP ?? repoPath("services/gateway/target/lambda/gateway");

    // --- Cognito User Pool ---

    const userPool = new cognito.UserPool(this, "UserPool", {
      userPoolName: `${prefix}-users`,
      selfSignUpEnabled: true,
      signInAliases: { email: true },
      autoVerify: { email: true },
      standardAttributes: {
        email: { required: true, mutable: true },
        fullname: { required: false, mutable: true },
      },
      passwordPolicy: {
        minLength: 8,
        requireUppercase: true,
        requireLowercase: true,
        requireDigits: true,
        requireSymbols: false,
      },
      mfa: cognito.Mfa.OPTIONAL,
      mfaSecondFactor: { sms: false, otp: true },
      accountRecovery: cognito.AccountRecovery.EMAIL_ONLY,
      // Through SES once the sender's identity is verified (cognitoUsesSes);
      // until then Cognito sends from its own address (50 emails a day).
      email: cfg.email.cognitoUsesSes
        ? cognito.UserPoolEmail.withSES({
            fromEmail: cfg.email.from,
            fromName: cfg.email.fromName,
            sesRegion: cfg.email.sesRegion,
            ...(cfg.email.identity && !cfg.email.identity.includes("@")
              ? { sesVerifiedDomain: cfg.email.identity }
              : {}),
          })
        : cognito.UserPoolEmail.withCognito(),
      removalPolicy: cfg.retainData
        ? cdk.RemovalPolicy.RETAIN
        : cdk.RemovalPolicy.DESTROY,
    });

    // Custom message Lambda — sends branded SES emails for verification and password reset
    const customMessageFn = new lambda.Function(this, "CustomMessageFn", {
      functionName: `${prefix}-custom-message`,
      runtime: lambda.Runtime.NODEJS_22_X,
      handler: "handler.handler",
      code: lambda.Code.fromAsset(repoPath("lambda/custom-message")),
      timeout: cdk.Duration.seconds(10),
      memorySize: 128,
      environment: {
        SES_TEMPLATE_PREFIX: prefix,
        SES_FROM_ADDRESS: cfg.email.from,
      },
      logRetention: logs.RetentionDays.ONE_MONTH,
    });

    customMessageFn.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["ses:GetEmailTemplate"],
        resources: ["*"],
      })
    );

    userPool.addTrigger(
      cognito.UserPoolOperation.CUSTOM_MESSAGE,
      customMessageFn
    );

    // A self-hosted install is one organization: every account the user pool
    // makes (sign-up with the app client, Google, AdminCreateUser) is checked
    // against its rules first, so even a direct Cognito SignUp call is
    // refused unless the person is invited, at an allowed domain, or the
    // first admin. The gateway checks the same rules (routes/sign_in.rs).
    if (cfg.layout === "single-origin") {
      const preSignUpFn = new lambda.Function(this, "PreSignUpFn", {
        functionName: `${prefix}-pre-signup`,
        runtime: lambda.Runtime.NODEJS_22_X,
        handler: "handler.handler",
        code: lambda.Code.fromAsset(repoPath("lambda/pre-signup"), {
          exclude: ["*.test.js", "cases.json"],
        }),
        timeout: cdk.Duration.seconds(5),
        memorySize: 128,
        environment: {
          SETTINGS_TABLE_NAME: props.settingsTable.tableName,
          USERS_TABLE_NAME: props.usersTable.tableName,
        },
        logRetention: logs.RetentionDays.ONE_MONTH,
      });
      props.settingsTable.grantReadData(preSignUpFn);
      props.usersTable.grantReadData(preSignUpFn);
      userPool.addTrigger(cognito.UserPoolOperation.PRE_SIGN_UP, preSignUpFn);
    }

    // Google identity provider (optional: integrations.googleSignIn).
    // google_client_id and google_client_secret live in the deployment's
    // secret (cfg.secretName). Callback URL:
    //   https://<sign-in domain>/oauth2/idpresponse
    let googleProvider: cognito.UserPoolIdentityProviderGoogle | undefined;
    if (cfg.integrations.googleSignIn) {
      const googleSecrets = secretsmanager.Secret.fromSecretNameV2(
        this,
        "GoogleSecrets",
        `${cfg.secretName}`
      );

      googleProvider = new cognito.UserPoolIdentityProviderGoogle(
        this,
        "GoogleProvider",
        {
          userPool,
          clientId: googleSecrets
            .secretValueFromJson("google_client_id")
            .unsafeUnwrap(),
          clientSecretValue: googleSecrets.secretValueFromJson(
            "google_client_secret"
          ),
          scopes: ["openid", "email", "profile"],
          attributeMapping: {
            email: cognito.ProviderAttribute.GOOGLE_EMAIL,
            fullname: cognito.ProviderAttribute.GOOGLE_NAME,
            profilePicture: cognito.ProviderAttribute.GOOGLE_PICTURE,
          },
        }
      );
    }

    const userPoolClient = new cognito.UserPoolClient(this, "UserPoolClient", {
      userPool,
      userPoolClientName: `${prefix}-dashboard`,
      authFlows: {
        userPassword: true,
        userSrp: true,
      },
      // The hosted sign-in pages are only used for Google; without it
      // (validation requires a known URL when it is on) OAuth stays off.
      ...(googleProvider && cfg.apiUrl && cfg.appUrl
        ? {
            oAuth: {
              flows: { authorizationCodeGrant: true },
              scopes: [
                cognito.OAuthScope.OPENID,
                cognito.OAuthScope.EMAIL,
                cognito.OAuthScope.PROFILE,
                cognito.OAuthScope.COGNITO_ADMIN,
              ],
              callbackUrls: [`${cfg.apiUrl}/auth/google/callback`],
              logoutUrls: [cfg.appUrl],
            },
            supportedIdentityProviders: [
              cognito.UserPoolClientIdentityProvider.COGNITO,
              cognito.UserPoolClientIdentityProvider.GOOGLE,
            ],
          }
        : {
            disableOAuth: true,
            supportedIdentityProviders: [
              cognito.UserPoolClientIdentityProvider.COGNITO,
            ],
          }),
      generateSecret: true,
    });

    if (googleProvider) userPoolClient.node.addDependency(googleProvider);

    // Sign-in domain: a custom one when the split layout names it, else
    // Cognito's own prefix domain.
    let userPoolDomain: cognito.UserPoolDomain;
    const authDomain = cfg.split?.authDomain;
    if (authDomain && cfg.split?.zone) {
      const hostedZone = route53.HostedZone.fromLookup(this, "AuthZone", {
        domainName: cfg.split.zone,
      });

      const authCert = new acm.Certificate(this, "AuthCertificate", {
        domainName: authDomain,
        validation: acm.CertificateValidation.fromDns(hostedZone),
      });

      userPoolDomain = userPool.addDomain("UserPoolDomain", {
        customDomain: {
          domainName: authDomain,
          certificate: authCert,
        },
      });

      new route53.ARecord(this, "AuthAliasRecord", {
        zone: hostedZone,
        recordName: recordNameIn(authDomain, cfg.split.zone),
        target: route53.RecordTarget.fromAlias(
          new route53Targets.UserPoolDomainTarget(userPoolDomain)
        ),
      });
    } else {
      userPoolDomain = userPool.addDomain("UserPoolDomain", {
        cognitoDomain: {
          domainPrefix: cfg.cognitoDomainPrefix,
        },
      });
    }

    // --- SQS Queues ---

    // Queues the worker reads (tickets, CI fix, feedback, webhooks). AWS
    // asks for a visibility timeout of at least six times the function's
    // timeout (900 s) and a redrive count of at least 5. A ticket run hands
    // off to a NEW message before 900 s, so the visibility only matters
    // after a crash: the message comes back after 90 minutes, and the
    // stuck-run sweep (20 minutes without an update) has failed the run with
    // a reason by then.
    const WORKER_QUEUE_VISIBILITY = cdk.Duration.seconds(6 * 900);
    const WORKER_QUEUE_MAX_RECEIVES = 5;

    // Dead letter queue (shared)
    const dlq = new sqs.Queue(this, "DLQ", {
      queueName: `${prefix}-dlq`,
      retentionPeriod: cdk.Duration.days(14),
      encryption: sqs.QueueEncryption.SQS_MANAGED,
    });
    this.dlq = dlq;

    this.ticketQueue = new sqs.Queue(this, "TicketQueue", {
      queueName: `${prefix}-tickets`,
      visibilityTimeout: WORKER_QUEUE_VISIBILITY,
      encryption: sqs.QueueEncryption.SQS_MANAGED,
      deadLetterQueue: { queue: dlq, maxReceiveCount: WORKER_QUEUE_MAX_RECEIVES },
    });

    // CoderHelm's mail relay (hosted side of hybrid only): its sending
    // subdomain, DNS, configuration set, and bounces to the worker.
    if (cfg.mailRelay) {
      new MailRelay(this, "MailRelay", { cfg, ticketQueue: this.ticketQueue });
    }

    this.ciFixQueue = new sqs.Queue(this, "CiFixQueue", {
      queueName: `${prefix}-ci-fix`,
      visibilityTimeout: WORKER_QUEUE_VISIBILITY,
      encryption: sqs.QueueEncryption.SQS_MANAGED,
      deadLetterQueue: { queue: dlq, maxReceiveCount: WORKER_QUEUE_MAX_RECEIVES },
    });

    this.feedbackQueue = new sqs.Queue(this, "FeedbackQueue", {
      queueName: `${prefix}-feedback`,
      visibilityTimeout: WORKER_QUEUE_VISIBILITY,
      encryption: sqs.QueueEncryption.SQS_MANAGED,
      deadLetterQueue: { queue: dlq, maxReceiveCount: WORKER_QUEUE_MAX_RECEIVES },
    });

    // Outbound webhooks: events the work emits (`webhook_emit`) and the
    // delayed retries of their deliveries (`webhook_send`). Its own queue so
    // a delivery never waits behind long runs on the tickets queue.
    this.webhookQueue = new sqs.Queue(this, "WebhookQueue", {
      queueName: `${prefix}-webhooks`,
      visibilityTimeout: WORKER_QUEUE_VISIBILITY,
      encryption: sqs.QueueEncryption.SQS_MANAGED,
      deadLetterQueue: { queue: dlq, maxReceiveCount: WORKER_QUEUE_MAX_RECEIVES },
    });

    // Review-reminder sweep: every 20 min, drop a constant message on the ticket
    // queue; the worker's ReviewReminder handler scans repos with reminders
    // configured and nudges their Teams webhook about PRs still awaiting review.
    // Defined here (same stack as the queue) to avoid a worker↔api dependency
    // cycle; the SqsQueue target auto-grants send permission.
    new events.Rule(this, "ReviewReminderSchedule", {
      ruleName: `${prefix}-review-reminder`,
      schedule: events.Schedule.rate(cdk.Duration.minutes(20)),
      targets: [
        new targets.SqsQueue(this.ticketQueue, {
          message: events.RuleTargetInput.fromObject({
            type: "review_reminder",
          }),
        }),
      ],
    });

    // Plugins: one-off schedules (publish an approved draft at its time,
    // read results back) and recurring schedule triggers live in one
    // EventBridge Scheduler group and send to the ticket queue with a role
    // that may only do that. The gateway and worker create and delete them.
    new scheduler.CfnScheduleGroup(this, "PluginScheduleGroup", {
      name: `${prefix}-plugins`,
    });
    const pluginSchedulerRole = new iam.Role(this, "PluginSchedulerRole", {
      roleName: `${prefix}-plugin-scheduler`,
      assumedBy: new iam.ServicePrincipal("scheduler.amazonaws.com", {
        conditions: { StringEquals: { "aws:SourceAccount": this.account } },
      }),
      description: "Lets plugin schedules send their message to the ticket queue",
    });
    this.ticketQueue.grantSendMessages(pluginSchedulerRole);

    // Plugin sources: every 15 min, poll the sources no webhook covers
    // (public repositories, and installation ones as a backstop). Does
    // nothing for teams whose plugins flag is off.
    new events.Rule(this, "PluginSweepSchedule", {
      ruleName: `${prefix}-plugin-sweep`,
      description: "Poll plugin sources for new versions",
      schedule: events.Schedule.rate(cdk.Duration.minutes(15)),
      targets: [
        new targets.SqsQueue(this.ticketQueue, {
          message: events.RuleTargetInput.fromObject({
            type: "plugin_sweep",
          }),
        }),
      ],
    });

    // Stuck-review sweep (permanent): every 15 min, re-run the review of open
    // PRs whose standing CoderHelm review didn't finish (a service error),
    // once per head. Idempotent: a head is re-run at most once a day.
    new events.Rule(this, "ReviewSweepSchedule", {
      ruleName: `${prefix}-review-sweep`,
      description: "Re-run reviews stuck on a review that didn't finish",
      schedule: events.Schedule.rate(cdk.Duration.minutes(15)),
      targets: [
        new targets.SqsQueue(this.ticketQueue, {
          message: events.RuleTargetInput.fromObject({
            type: "review_sweep",
          }),
        }),
      ],
    });

    // Paused agent work: every 5 min, send again the work kept while a
    // team's agents were paused once the pause has ended (an until-time
    // passing needs nobody to click anything). Does nothing when no team is
    // paused.
    new events.Rule(this, "AgentResumeSweepSchedule", {
      ruleName: `${prefix}-agent-resume-sweep`,
      description: "Resume agent work kept while a team's agents were paused",
      schedule: events.Schedule.rate(cdk.Duration.minutes(5)),
      targets: [
        new targets.SqsQueue(this.ticketQueue, {
          message: events.RuleTargetInput.fromObject({
            type: "agent_resume_sweep",
          }),
        }),
      ],
    });

    // Weekly summary email: Mondays 13:00 UTC, one constant message on the
    // ticket queue; the worker sends each team's summary to members who have
    // it on. Ships disabled (see flags.ts) until a test email is approved.
    new events.Rule(this, "WeeklySummarySchedule", {
      ruleName: `${prefix}-weekly-summary`,
      description: "Weekly summary email (Mondays 13:00 UTC)",
      // Always there on a self-hosted install: the worker sends nothing
      // until an install admin turns the summary on (Settings, Emails).
      enabled: weeklySummaryScheduled(cfg),
      schedule: events.Schedule.cron({ minute: "0", hour: "13", weekDay: "MON" }),
      targets: [
        new targets.SqsQueue(this.ticketQueue, {
          message: events.RuleTargetInput.fromObject({
            type: "weekly_summary",
          }),
        }),
      ],
    });

    // Automatic fix PRs for new log findings: hourly. The worker only acts
    // for teams that turned automatic fixes on (off by default), and applies
    // their severity floor, log groups, quiet hours and daily cap.
    new events.Rule(this, "LogAutofixSchedule", {
      ruleName: `${prefix}-log-autofix`,
      description: "Start fix PRs for new log findings where a team turned it on",
      schedule: events.Schedule.rate(cdk.Duration.hours(1)),
      targets: [
        new targets.SqsQueue(this.ticketQueue, {
          message: events.RuleTargetInput.fromObject({
            type: "log_autofix",
          }),
        }),
      ],
    });

    // AWS permissions: daily 14:00 UTC, check every connected account's
    // read-only role against the permissions this release uses, so the AWS
    // page is current and each team hears once when a release needs an
    // update. Read-only in the teams' accounts; does nothing without
    // connected accounts.
    new events.Rule(this, "AwsPermissionsSchedule", {
      ruleName: `${prefix}-aws-permissions`,
      description: "Check connected AWS accounts against this release's read-only permissions (daily)",
      schedule: events.Schedule.cron({ minute: "0", hour: "14" }),
      targets: [
        new targets.SqsQueue(this.ticketQueue, {
          message: events.RuleTargetInput.fromObject({
            type: "aws_permissions_sweep",
          }),
        }),
      ],
    });

    // Usage reconciliation: daily 06:00 UTC, compares the previous day's
    // recorded model usage with Anthropic's usage/cost report. The worker
    // does nothing unless the coderhelm/<stage>/anthropic-admin-keys secret
    // exists, so this is safe to run before that secret is added.
    new events.Rule(this, "UsageReconcileSchedule", {
      ruleName: `${prefix}-usage-reconcile`,
      description: "Compare recorded model usage with Anthropic's report (daily)",
      schedule: events.Schedule.cron({ minute: "0", hour: "6" }),
      targets: [
        new targets.SqsQueue(this.ticketQueue, {
          message: events.RuleTargetInput.fromObject({
            type: "usage_reconcile",
          }),
        }),
      ],
    });

    // Knowledge sources: every 15 min, queue a sync for each connected
    // source whose next scheduled sync is due (hourly without webhooks,
    // every 6 hours as a backstop with them). Does nothing without sources.
    new events.Rule(this, "KnowledgeSyncSchedule", {
      ruleName: `${prefix}-knowledge-sync`,
      description: "Queue scheduled syncs of knowledge sources that are due",
      schedule: events.Schedule.rate(cdk.Duration.minutes(15)),
      targets: [
        new targets.SqsQueue(this.ticketQueue, {
          message: events.RuleTargetInput.fromObject({
            type: "knowledge_sync_sweep",
          }),
        }),
      ],
    });

    // DLQ cleanup: daily 07:00 UTC. The worker archives to S3, then deletes,
    // DLQ messages it can't parse (an unknown type, not JSON) once they are
    // 7 days old (common::dlq::CLEANUP_AFTER_DAYS), before the DLQ's 14-day
    // retention drops them with no record. Messages it can read are never
    // touched.
    new events.Rule(this, "DlqCleanupSchedule", {
      ruleName: `${prefix}-dlq-cleanup`,
      description: "Archive and delete DLQ messages the worker can't parse (daily)",
      schedule: events.Schedule.cron({ minute: "0", hour: "7" }),
      targets: [
        new targets.SqsQueue(this.ticketQueue, {
          message: events.RuleTargetInput.fromObject({
            type: "dlq_cleanup",
          }),
        }),
      ],
    });

    // --- Secrets ---

    const secrets = secretsmanager.Secret.fromSecretNameV2(
      this,
      "Secrets",
      `${props.cfg.secretName}`
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
      memorySize: 256,
      timeout: cdk.Duration.seconds(30),
      logGroup: gatewayLogGroup,
      environment: {
        STAGE: props.stage,
        TABLE_NAME: props.table.tableName,
        TEAMS_TABLE_NAME: `${props.cfg.prefix}-teams`,
        RUNS_TABLE_NAME: `${props.cfg.prefix}-runs`,
        ANALYTICS_TABLE_NAME: `${props.cfg.prefix}-analytics`,
        EVENTS_TABLE_NAME: props.eventsTable.tableName,
        USERS_TABLE_NAME: props.usersTable.tableName,
        JIRA_TOKENS_TABLE_NAME: props.jiraTokensTable.tableName,
        JIRA_EVENTS_TABLE_NAME: `${props.cfg.prefix}-jira-events`,
        PLANS_TABLE_NAME: props.plansTable.tableName,
        JIRA_CONFIG_TABLE_NAME: props.jiraConfigTable.tableName,
        REPOS_TABLE_NAME: props.reposTable.tableName,
        SETTINGS_TABLE_NAME: props.settingsTable.tableName,
        SECRETS_KEY_ID: props.secretsKey.keyId,
        AWS_INSIGHTS_TABLE_NAME: props.awsInsightsTable.tableName,
        TESTS_TABLE_NAME: props.testsTable.tableName,
        MCP_CONFIGS_TABLE_NAME: props.mcpConfigsTable.tableName,
        INFRA_TABLE_NAME: props.infraTable.tableName,
        BILLING_TABLE_NAME: props.billingTable.tableName, // Retained for CF migration
        BANNERS_TABLE_NAME: props.bannersTable.tableName,
        BUCKET_NAME: props.bucket.bucketName,
        WAITLIST_TABLE_NAME: props.waitlistTable.tableName,
        TICKET_QUEUE_URL: this.ticketQueue.queueUrl,
        TICKET_QUEUE_ARN: this.ticketQueue.queueArn,
        PLUGIN_SCHEDULE_GROUP: `${prefix}-plugins`,
        PLUGIN_SCHEDULER_ROLE_ARN: pluginSchedulerRole.roleArn,
        CI_FIX_QUEUE_URL: this.ciFixQueue.queueUrl,
        FEEDBACK_QUEUE_URL: this.feedbackQueue.queueUrl,
        WEBHOOK_QUEUE_URL: this.webhookQueue.queueUrl,
        DLQ_URL: dlq.queueUrl,
        SECRETS_NAME: `${props.cfg.secretName}`,
        SES_FROM_ADDRESS: cfg.email.from,
        // Where new support tickets and customer replies are emailed.
        SUPPORT_INBOX: cfg.email.supportInbox,
        SES_TEMPLATE_PREFIX: `${props.cfg.prefix}`,
        MODEL_ID: cfg.models.light,
        COGNITO_USER_POOL_ID: userPool.userPoolId,
        COGNITO_CLIENT_ID: userPoolClient.userPoolClientId,
        COGNITO_DOMAIN: userPoolDomain.domainName,
        RUST_LOG: "info",
        TRACES_TABLE_NAME: `${props.cfg.prefix}-traces`,
        ...serviceEnvironment(cfg, this.account),
      },
    });
    for (const st of [...modelPolicyStatements(cfg, this), publicUrlReadStatement(cfg, this), updaterInvokeStatement(cfg, this), relaySendStatement(cfg, this.account)]) {
      if (st) this.gatewayFunction.addToRolePolicy(st);
    }
    // A self-hosted install's GitHub App manifest landing (routes/setup.rs,
    // docs/self-host/setup-checks.md): stores the new app's keys in this
    // install's secret.
    for (const [k, v] of Object.entries(setupEnvironment(cfg))) this.gatewayFunction.addEnvironment(k, v);
    for (const st of setupStatements(cfg, this)) this.gatewayFunction.addToRolePolicy(st);

    // Permissions
    props.table.grantReadWriteData(this.gatewayFunction);

    // Decoupled tables — use fromTableName to avoid cross-stack exports
    const teamsTable = dynamodb.TableV2.fromTableName(this, "TeamsTableRef", `${props.cfg.prefix}-teams`);
    const runsTable = dynamodb.TableV2.fromTableName(this, "RunsTableRef", `${props.cfg.prefix}-runs`);
    const analyticsTable = dynamodb.TableV2.fromTableName(this, "AnalyticsTableRef", `${props.cfg.prefix}-analytics`);
    const jiraEventsTable = dynamodb.TableV2.fromTableName(this, "JiraEventsTableRef", `${props.cfg.prefix}-jira-events`);
    const tracesTable = dynamodb.TableV2.fromTableName(this, "TracesTableRef", `${props.cfg.prefix}-traces`);
    teamsTable.grantReadWriteData(this.gatewayFunction);
    // fromTableName doesn't know about GSIs, so grant index access explicitly
    this.gatewayFunction.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["dynamodb:Query"],
        resources: [`arn:aws:dynamodb:${this.region}:${this.account}:table/${props.cfg.prefix}-teams/index/*`],
      })
    );
    runsTable.grantReadWriteData(this.gatewayFunction);
    this.gatewayFunction.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["dynamodb:Query"],
        resources: [`arn:aws:dynamodb:${this.region}:${this.account}:table/${props.cfg.prefix}-runs/index/*`],
      })
    );
    // Write: plan chat counts its model calls in the team's usage counters
    // (read-only before, so those writes silently failed).
    analyticsTable.grantReadWriteData(this.gatewayFunction);
    jiraEventsTable.grantReadWriteData(this.gatewayFunction);
    tracesTable.grantReadData(this.gatewayFunction);

    props.eventsTable.grantReadWriteData(this.gatewayFunction);
    props.usersTable.grantReadWriteData(this.gatewayFunction);
    props.jiraTokensTable.grantReadWriteData(this.gatewayFunction);
    props.plansTable.grantReadWriteData(this.gatewayFunction);
    props.jiraConfigTable.grantReadWriteData(this.gatewayFunction);
    props.reposTable.grantReadWriteData(this.gatewayFunction);
    props.settingsTable.grantReadWriteData(this.gatewayFunction);
    props.secretsKey.grantEncryptDecrypt(this.gatewayFunction);
    props.awsInsightsTable.grantReadWriteData(this.gatewayFunction);
    props.testsTable.grantReadWriteData(this.gatewayFunction);
    props.infraTable.grantReadWriteData(this.gatewayFunction);
    props.billingTable.grantReadWriteData(this.gatewayFunction); // Retained for CF migration
    props.bannersTable.grantReadData(this.gatewayFunction);
    props.mcpConfigsTable.grantReadWriteData(this.gatewayFunction);
    props.waitlistTable.grantReadWriteData(this.gatewayFunction);
    props.bucket.grantReadWrite(this.gatewayFunction);
    this.ticketQueue.grantSendMessages(this.gatewayFunction);
    // Plugin schedules: create and delete in the plugins group only, and
    // hand them the scheduler role.
    this.gatewayFunction.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["scheduler:CreateSchedule", "scheduler:DeleteSchedule", "scheduler:GetSchedule"],
        resources: [
          `arn:aws:scheduler:${this.region}:${this.account}:schedule/${prefix}-plugins/*`,
        ],
      })
    );
    this.gatewayFunction.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["iam:PassRole"],
        conditions: { StringEquals: { "iam:PassedToService": "scheduler.amazonaws.com" } },
        resources: [pluginSchedulerRole.roleArn],
      })
    );
    this.ciFixQueue.grantSendMessages(this.gatewayFunction);
    this.feedbackQueue.grantSendMessages(this.gatewayFunction);
    this.webhookQueue.grantSendMessages(this.gatewayFunction);
    dlq.grantConsumeMessages(this.gatewayFunction);
    this.gatewayFunction.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["sqs:PurgeQueue"],
        resources: [dlq.queueArn],
      }),
    );
    secrets.grantRead(this.gatewayFunction);

    // STS AssumeRole — gateway validates customer IAM roles on AWS connection
    // setup and runs data-source lookups a person tries. Only Coderhelm's own
    // read-only roles: the log reader and the data-source role.
    this.gatewayFunction.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["sts:AssumeRole"],
        resources: ["arn:aws:iam::*:role/CoderHelmLogReader", "arn:aws:iam::*:role/CoderhelmReadOnly*"],
      })
    );

    // Bedrock: Titan embeddings only (memory pages, metered in
    // common::embed). Plan chat calls Anthropic directly with the team's key.
    this.gatewayFunction.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["bedrock:InvokeModel"],
        resources: [`arn:aws:bedrock:*::foundation-model/amazon.titan-embed-text-v2:0`],
      })
    );

    // The setup checklist (Settings, then Setup): the sender identity's
    // verification and DKIM, and whether the account left the SES sandbox.
    // Read only; these actions take no resource-level scoping.
    this.gatewayFunction.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["ses:GetAccount", "ses:GetEmailIdentity"],
        resources: ["*"],
      })
    );

    // Cognito access for auth flows
    this.gatewayFunction.addToRolePolicy(
      new iam.PolicyStatement({
        actions: [
          "cognito-idp:SignUp",
          "cognito-idp:ConfirmSignUp",
          "cognito-idp:InitiateAuth",
          "cognito-idp:RespondToAuthChallenge",
          "cognito-idp:ForgotPassword",
          "cognito-idp:ConfirmForgotPassword",
          "cognito-idp:ChangePassword",
          "cognito-idp:GetUser",
          "cognito-idp:AdminGetUser",
          "cognito-idp:AdminUpdateUserAttributes",
          "cognito-idp:AdminDisableUser",
          "cognito-idp:AdminDeleteUser",
          "cognito-idp:AdminCreateUser",
          "cognito-idp:AdminSetUserMFAPreference",
          "cognito-idp:AssociateSoftwareToken",
          "cognito-idp:VerifySoftwareToken",
          "cognito-idp:DescribeUserPoolClient",
        ],
        resources: [userPool.userPoolArn],
      })
    );

    // --- MCP Proxy Lambda (Node.js) ---
    // Spawns MCP server processes via stdio to execute tool calls.
    // Lives here (api-stack) so gateway can invoke it without a cross-stack cycle.

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
      // MCP_PROXY_DIR: a prebuilt proxy (release artifact) instead of a local build.
      code: lambda.Code.fromAsset(process.env.MCP_PROXY_DIR ?? repoPath("lambda/mcp-proxy/dist")),
      memorySize: 512,
      timeout: cdk.Duration.minutes(2),
      logGroup: mcpProxyLogGroup,
      environment: {
        BUCKET_NAME: props.bucket.bucketName,
        NODE_ENV: "production",
      },
    });

    props.bucket.grantReadWrite(this.mcpProxyFunction);
    this.mcpProxyFunction.grantInvoke(this.gatewayFunction);
    // The plan chat's skills, agents and data source tools run in the
    // worker, named rather than referenced: the worker stack depends on this
    // one.
    this.gatewayFunction.addEnvironment("WORKER_FUNCTION_NAME", `${prefix}-worker`);
    this.gatewayFunction.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["lambda:InvokeFunction"],
        resources: [`arn:aws:lambda:${this.region}:${this.account}:function:${prefix}-worker`],
      })
    );
    this.gatewayFunction.addEnvironment(
      "MCP_PROXY_FUNCTION_NAME",
      this.mcpProxyFunction.functionName
    );

    // --- HTTP API Gateway ---

    // No CORS setting here: with one on, API Gateway drops the gateway's own
    // CORS headers and answers every route with one origin list and one
    // credentials flag. The gateway owns CORS (middleware/cors.rs): the
    // dashboard's origins (CORS_ORIGINS) get credentials, and the public
    // routes the docs install guide calls get `*` without credentials.
    const httpApi = new apigatewayv2.HttpApi(this, "HttpApi", {
      apiName: `${prefix}-api`,
    });
    this.httpApi = httpApi;

    // Stage-level throttling: 1000 req/sec burst, 500 req/sec sustained
    const defaultStage = httpApi.defaultStage?.node.defaultChild as cdk.aws_apigatewayv2.CfnStage;
    if (defaultStage) {
      defaultStage.defaultRouteSettings = {
        throttlingBurstLimit: 1000,
        throttlingRateLimit: 500,
      };
    }

    const lambdaIntegration =
      new integrations.HttpLambdaIntegration(
        "GatewayIntegration",
        this.gatewayFunction
      );

    // Cross-origin only in the split layout (single-origin serves the
    // dashboard and the API from one hostname): the dashboard's origins, and
    // every preflight goes to the gateway, which answers it.
    if (cfg.split) {
      this.gatewayFunction.addEnvironment("CORS_ORIGINS", cfg.split.corsOrigins.join(","));
      httpApi.addRoutes({
        path: "/{proxy+}",
        methods: [apigatewayv2.HttpMethod.OPTIONS],
        integration: lambdaIntegration,
      });
    }

    // Webhook routes
    httpApi.addRoutes({
      path: "/webhooks/github",
      methods: [apigatewayv2.HttpMethod.POST],
      integration: lambdaIntegration,
    });

    httpApi.addRoutes({
      path: "/webhooks/jira/{proxy+}",
      methods: [apigatewayv2.HttpMethod.POST],
      integration: lambdaIntegration,
    });

    httpApi.addRoutes({
      path: "/webhooks/jira",
      methods: [apigatewayv2.HttpMethod.POST],
      integration: lambdaIntegration,
    });

    // Alert intake: an SNS HTTPS subscription (messages verified by signature)
    httpApi.addRoutes({
      path: "/webhooks/alerts/sns",
      methods: [apigatewayv2.HttpMethod.POST],
      integration: lambdaIntegration,
    });

    // Knowledge sources: providers (Sanity, Notion) say pages changed. Each
    // request is verified by its signature before a sync starts.
    httpApi.addRoutes({
      path: "/webhooks/knowledge/{proxy+}",
      methods: [apigatewayv2.HttpMethod.POST],
      integration: lambdaIntegration,
    });

    // Hybrid installs: events CoderHelm's relay forwards (signed with the
    // install's relay key) and the ownership proof the hosted side asks for.
    httpApi.addRoutes({
      path: "/webhooks/relay",
      methods: [apigatewayv2.HttpMethod.POST],
      integration: lambdaIntegration,
    });
    httpApi.addRoutes({
      path: "/webhooks/relay/proof",
      methods: [apigatewayv2.HttpMethod.GET],
      integration: lambdaIntegration,
    });
    // The hosted side of hybrid: what self-hosted installs call (signed with
    // their registration secret), the OAuth broker and the fallback page.
    if (cfg.hybrid) {
      httpApi.addRoutes({
        path: "/hybrid/{proxy+}",
        methods: [
          apigatewayv2.HttpMethod.GET,
          apigatewayv2.HttpMethod.POST,
          apigatewayv2.HttpMethod.PUT,
          apigatewayv2.HttpMethod.DELETE,
        ],
        integration: lambdaIntegration,
      });
    }

    // CI uploads and the quarantine list, signed with a GitHub Actions OIDC token
    httpApi.addRoutes({
      path: "/webhooks/ci/{proxy+}",
      methods: [apigatewayv2.HttpMethod.GET, apigatewayv2.HttpMethod.POST],
      integration: lambdaIntegration,
    });

    // The public API's OpenAPI document (public, no credentials)
    httpApi.addRoutes({
      path: "/openapi.json",
      methods: [apigatewayv2.HttpMethod.GET],
      integration: lambdaIntegration,
    });

    httpApi.addRoutes({
      path: "/integrations/jira/forge-register",
      methods: [apigatewayv2.HttpMethod.POST],
      integration: lambdaIntegration,
    });

    // Auth routes
    httpApi.addRoutes({
      path: "/auth/{proxy+}",
      methods: [apigatewayv2.HttpMethod.GET, apigatewayv2.HttpMethod.POST],
      integration: lambdaIntegration,
    });

    // Unsubscribe links in emails (public; a signed token is the credential).
    // GET is the link, POST is a mail app's one-click unsubscribe (RFC 8058).
    httpApi.addRoutes({
      path: "/email/{proxy+}",
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

    // The hosted side of hybrid: Route 53 rights for {name}.<zone> and the
    // fallback page for every unclaimed name (lib/hybrid.ts).
    if (cfg.hybrid) {
      new Hybrid(this, "Hybrid", { cfg, httpApi, gatewayFunction: this.gatewayFunction });
    }

    // Outputs
    new cdk.CfnOutput(this, "ApiUrl", {
      value: httpApi.apiEndpoint,
    });

    new cdk.CfnOutput(this, "UserPoolId", {
      value: userPool.userPoolId,
    });

    new cdk.CfnOutput(this, "UserPoolClientId", {
      value: userPoolClient.userPoolClientId,
    });

    // Self-hosted installs: the sign-in domain Google's OAuth client needs.
    if (cfg.layout === "single-origin") {
      new cdk.CfnOutput(this, "SignInDomain", { value: userPoolDomain.baseUrl() });
    }

    // --- Custom domain for the API (split layout) ---
    const apiDomainName = cfg.split?.apiDomain;
    if (apiDomainName && cfg.split?.zone) {
      const apiDomain = apiDomainName;

      const hostedZone = route53.HostedZone.fromLookup(this, "Zone", {
        domainName: cfg.split.zone,
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
        recordName: recordNameIn(apiDomain, cfg.split.zone),
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
