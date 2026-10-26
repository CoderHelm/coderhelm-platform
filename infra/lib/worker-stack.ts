import * as cdk from "aws-cdk-lib";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as secretsmanager from "aws-cdk-lib/aws-secretsmanager";
import * as kms from "aws-cdk-lib/aws-kms";
import * as iam from "aws-cdk-lib/aws-iam";
import * as logs from "aws-cdk-lib/aws-logs";
import * as events from "aws-cdk-lib/aws-events";
import * as targets from "aws-cdk-lib/aws-events-targets";
import * as eventsources from "aws-cdk-lib/aws-lambda-event-sources";
import * as codebuild from "aws-cdk-lib/aws-codebuild";
import { Construct } from "constructs";
import { ResolvedConfig } from "./config";
import { repoPath } from "./paths";
import * as path from "path";
import { serviceEnvironment, modelPolicyStatements, publicUrlReadStatement } from "./service-env";

interface WorkerStackProps extends cdk.StackProps {
  stage: string;
  cfg: ResolvedConfig;
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
  eventsTable: dynamodb.TableV2;
  testsTable: dynamodb.TableV2;
  bucket: s3.Bucket;
  ticketQueue: sqs.Queue;
  ciFixQueue: sqs.Queue;
  feedbackQueue: sqs.Queue;
  /** Outbound webhook events and retries: the worker sends them and queues retries. */
  webhookQueue: sqs.Queue;
  /** The shared DLQ: the daily cleanup archives and deletes unreadable messages. */
  dlq: sqs.Queue;
  mcpProxyFunction: lambda.Function;
  encryptionKey: kms.Key;
  secretsKey: kms.Key;
}


/** The repository whose tag builds publish releases (release.yml). */
export const RELEASE_REPO = "CoderHelm/coderhelm-platform";
export class WorkerStack extends cdk.Stack {
  public readonly workerFunction: lambda.Function;

  constructor(scope: Construct, id: string, props: WorkerStackProps) {
    super(scope, id, props);

    const cfg = props.cfg;
    const prefix = cfg.prefix;
    const workerAssetPath =
      process.env.WORKER_ZIP ?? repoPath("services/worker/target/lambda/worker");

    const secrets = secretsmanager.Secret.fromSecretNameV2(
      this,
      "Secrets",
      `${props.cfg.secretName}`
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
      // Repo snapshots (tarball-backed search) are held in memory alongside
      // the tarball during extraction — 2048 gives headroom for large repos.
      memorySize: 2048,
      timeout: cdk.Duration.minutes(15),
      ephemeralStorageSize: cdk.Size.mebibytes(1024),
      logGroup: workerLogGroup,
      recursiveLoop: lambda.RecursiveLoop.ALLOW,
        environment: {
        STAGE: props.stage,
        MEMORY_ENABLED: "true",
        TABLE_NAME: props.table.tableName,
        TEAMS_TABLE_NAME: `${props.cfg.prefix}-teams`,
        RUNS_TABLE_NAME: `${props.cfg.prefix}-runs`,
        ANALYTICS_TABLE_NAME: `${props.cfg.prefix}-analytics`,
        USERS_TABLE_NAME: props.usersTable.tableName,
        PLANS_TABLE_NAME: props.plansTable.tableName,
        JIRA_CONFIG_TABLE_NAME: props.jiraConfigTable.tableName,
        REPOS_TABLE_NAME: props.reposTable.tableName,
        SETTINGS_TABLE_NAME: props.settingsTable.tableName,
        // Connected AWS accounts and what each can do (logs, metrics, Airflow).
        AWS_INSIGHTS_TABLE_NAME: `${props.cfg.prefix}-aws-insights`,
        MCP_CONFIGS_TABLE_NAME: props.mcpConfigsTable.tableName,
        // Seals a remote MCP server's renewed sign-in (see the grant below).
        SECRETS_KEY_ID: props.secretsKey.keyId,
        INFRA_TABLE_NAME: props.infraTable.tableName,
        BILLING_TABLE_NAME: props.billingTable.tableName, // Retained for CF migration
        TRACES_TABLE_NAME: props.tracesTable.tableName,
        CHECKPOINTS_TABLE_NAME: props.checkpointsTable.tableName,
        EVENTS_TABLE_NAME: props.eventsTable.tableName,
        TESTS_TABLE_NAME: props.testsTable.tableName,
        BUCKET_NAME: props.bucket.bucketName,
        SECRETS_NAME: `${props.cfg.secretName}`,
        // The deploy environment may override the config (MODEL_ID=...).
        MODEL_ID: process.env.MODEL_ID || cfg.models.primary,
        LIGHT_MODEL_ID: process.env.LIGHT_MODEL_ID || cfg.models.light,
        SES_FROM_ADDRESS: cfg.email.from,
        SES_TEMPLATE_PREFIX: `${props.cfg.prefix}`,
        // The Monday weekly summary sends nothing unless this is "true".
        WEEKLY_SUMMARY_ENABLED: cfg.integrations.weeklySummary ? "true" : "false",
        // While "true", only teams with a rollout list get the summary.
        WEEKLY_SUMMARY_LISTED_TEAMS_ONLY: cfg.integrations.weeklySummaryListedTeamsOnly ? "true" : "false",
        CI_FIX_QUEUE_URL: props.ciFixQueue.queueUrl,
        TICKET_QUEUE_URL: props.ticketQueue.queueUrl,
        // Plugin schedules (made in the API stack, named the same here).
        TICKET_QUEUE_ARN: props.ticketQueue.queueArn,
        PLUGIN_SCHEDULE_GROUP: `${prefix}-plugins`,
        PLUGIN_SCHEDULER_ROLE_ARN: `arn:aws:iam::${this.account}:role/${prefix}-plugin-scheduler`,
        FEEDBACK_QUEUE_URL: props.feedbackQueue.queueUrl,
        WEBHOOK_QUEUE_URL: props.webhookQueue.queueUrl,
        DLQ_URL: props.dlq.queueUrl,
        RUST_LOG: "info",
        ...serviceEnvironment(cfg, this.account),
      },
    });
    for (const st of [...modelPolicyStatements(cfg, this), publicUrlReadStatement(cfg, this)]) {
      if (st) this.workerFunction.addToRolePolicy(st);
    }

    // DynamoDB: read/write all tables
    props.table.grantReadWriteData(this.workerFunction);

    // Decoupled tables — use fromTableName to avoid cross-stack exports
    const teamsTable = dynamodb.TableV2.fromTableName(this, "TeamsTableRef", `${props.cfg.prefix}-teams`);
    const runsTable = dynamodb.TableV2.fromTableName(this, "RunsTableRef", `${props.cfg.prefix}-runs`);
    const analyticsTable = dynamodb.TableV2.fromTableName(this, "AnalyticsTableRef", `${props.cfg.prefix}-analytics`);
    teamsTable.grantReadWriteData(this.workerFunction);
    runsTable.grantReadWriteData(this.workerFunction);
    // fromTableName doesn't know about GSIs, so grant index access explicitly
    this.workerFunction.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["dynamodb:Query"],
        resources: [`arn:aws:dynamodb:${this.region}:${this.account}:table/${props.cfg.prefix}-runs/index/*`],
      })
    );
    analyticsTable.grantReadWriteData(this.workerFunction);
    // Agents read a connected AWS account's abilities (metrics, Airflow) from
    // its record; they never change it.
    const awsInsightsTable = dynamodb.TableV2.fromTableName(this, "AwsInsightsTableRef", `${props.cfg.prefix}-aws-insights`);
    // Read/write: the automatic log-finding sweep records on each finding the
    // fix it started or why it didn't.
    awsInsightsTable.grantReadWriteData(this.workerFunction);

    props.usersTable.grantReadData(this.workerFunction);
    props.plansTable.grantReadWriteData(this.workerFunction);
    props.jiraConfigTable.grantReadData(this.workerFunction);
    props.reposTable.grantReadWriteData(this.workerFunction);
    props.settingsTable.grantReadWriteData(this.workerFunction);
    props.testsTable.grantReadWriteData(this.workerFunction);
    // Data-source keys and MCP server credentials: the worker opens them to
    // run lookups and tool calls. It seals only renewed remote MCP sign-ins
    // (below).
    props.secretsKey.grantDecrypt(this.workerFunction);

    // Data sources in a team's own AWS account (logs, a database): the agents
    // take on the read-only role the team created. Limited to those roles'
    // names, so the worker can't be pointed at any other role.
    this.workerFunction.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["sts:AssumeRole"],
        resources: ["arn:aws:iam::*:role/CoderHelmLogReader", "arn:aws:iam::*:role/CoderhelmReadOnly*"],
      }),
    );
    // MCP servers: read their settings; write back a remote server's renewed
    // tokens, its status and the tool-call audit trail.
    props.mcpConfigsTable.grantReadWriteData(this.workerFunction);
    // A renewed remote MCP sign-in is sealed before it is saved. The worker
    // may seal only that kind of value: nothing else it could encrypt would
    // carry this purpose in its encryption context.
    this.workerFunction.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["kms:Encrypt"],
        resources: [props.secretsKey.keyArn],
        conditions: {
          StringEquals: { "kms:EncryptionContext:purpose": "mcp-oauth-token" },
        },
      }),
    );
    props.infraTable.grantReadWriteData(this.workerFunction);
    props.billingTable.grantReadWriteData(this.workerFunction); // Retained for CF migration
    props.tracesTable.grantReadWriteData(this.workerFunction);
    props.checkpointsTable.grantReadWriteData(this.workerFunction);
    props.eventsTable.grantReadWriteData(this.workerFunction);
    props.bucket.grantReadWrite(this.workerFunction);
    secrets.grantRead(this.workerFunction);
    // Usage reconciliation reads Anthropic Admin API keys from this secret
    // (optional: reconciliation is off until it exists). Secrets Manager adds
    // a 6-character suffix to secret ARNs, hence the wildcard.
    this.workerFunction.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["secretsmanager:GetSecretValue"],
        resources: [
          `arn:aws:secretsmanager:${this.region}:${this.account}:secret:${props.cfg.anthropicAdminSecretName}-??????`,
        ],
      })
    );

    // Bedrock: Titan embeddings only (agent memory, metered in
    // common::embed). Claude is called through the Anthropic API with the
    // team's key; the Bedrock Claude path is the nightly evals' own role.
    this.workerFunction.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["bedrock:InvokeModel"],
        resources: [`arn:aws:bedrock:*::foundation-model/amazon.titan-embed-text-v2:0`],
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

    // Worker needs to send delayed resume messages back to ci-fix queue
    props.ciFixQueue.grantSendMessages(this.workerFunction);

    // Worker needs to re-trigger tickets (e.g. wrong-repo re-routing)
    props.ticketQueue.grantSendMessages(this.workerFunction);
    // Plugin schedules: recurring triggers and results checks, in the
    // plugins group only.
    this.workerFunction.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["scheduler:CreateSchedule", "scheduler:DeleteSchedule", "scheduler:GetSchedule"],
        resources: [
          `arn:aws:scheduler:${this.region}:${this.account}:schedule/${prefix}-plugins/*`,
        ],
      })
    );
    this.workerFunction.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["iam:PassRole"],
        conditions: { StringEquals: { "iam:PassedToService": "scheduler.amazonaws.com" } },
        resources: [`arn:aws:iam::${this.account}:role/${prefix}-plugin-scheduler`],
      })
    );

    // Worker needs to defer feedback back onto the feedback queue when it loses
    // the per-run writer-slot race (delayed re-enqueue instead of failing the run)
    props.feedbackQueue.grantSendMessages(this.workerFunction);

    // The DLQ cleanup receives, gives back (visibility 0) and deletes DLQ
    // messages; the archive goes to the data bucket (granted above).
    props.dlq.grantConsumeMessages(this.workerFunction);

    this.workerFunction.addEventSource(
      new eventsources.SqsEventSource(props.feedbackQueue, {
        batchSize: 1,
        maxConcurrency: 5,
      })
    );

    // Outbound webhooks: one event (or one retry) per message. The worker
    // queues each delivery's next try back on this queue.
    this.workerFunction.addEventSource(
      new eventsources.SqsEventSource(props.webhookQueue, {
        batchSize: 1,
        maxConcurrency: 5,
      })
    );
    props.webhookQueue.grantSendMessages(this.workerFunction);

    // Grant worker permission to invoke the MCP proxy (owned by api-stack)
    props.mcpProxyFunction.grantInvoke(this.workerFunction);
    this.workerFunction.addEnvironment(
      "MCP_PROXY_FUNCTION_NAME",
      props.mcpProxyFunction.functionName
    );

    // --- Execution sandbox (CodeBuild), optional: integrations.sandbox ---
    // Without it the worker skips running a repo's checks (empty
    // SANDBOX_* variables turn the sandbox off).
    if (cfg.integrations.sandbox) {
      // --- Execution sandbox (CodeBuild) ---
      // Runs each repo's REAL build/lint/tests in an isolated container so the
      // agent iterates against ground truth instead of guessing at CI. The
      // sandbox runs untrusted customer build scripts, so its role is minimal by
      // design: write its own logs, nothing else. No S3, no GitHub token, no
      // DynamoDB, no KMS. The worker hands each build presigned URLs for exactly
      // its own input tarball (GET) and, for codegen, its own output key (PUT),
      // so a build script can never read or overwrite another build's objects.
      // SSE-S3 (not the account CMK) on the bucket is what lets those URLs work
      // without KMS.
      const sandboxLogGroup = new logs.LogGroup(this, "SandboxLogGroup", {
        logGroupName: `/aws/codebuild/${prefix}-sandbox`,
        retention: logs.RetentionDays.ONE_WEEK,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      });

      const sandboxBucket = new s3.Bucket(this, "SandboxBucket", {
        bucketName: cfg.sandboxBucketName,
        encryption: s3.BucketEncryption.S3_MANAGED,
        blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
        enforceSSL: true,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
        autoDeleteObjects: true,
        // Source tarballs are transient: the worker uploads one per commit per
        // run and reuses it for every check of that commit; this expiry is
        // what removes them.
        lifecycleRules: [{ expiration: cdk.Duration.days(1) }],
      });

      const sandboxProject = new codebuild.Project(this, "SandboxProject", {
        projectName: `${prefix}-sandbox`,
        timeout: cdk.Duration.minutes(15),
        environment: {
          buildImage: codebuild.LinuxBuildImage.STANDARD_7_0,
          // MEDIUM (7 GB) not SMALL (3 GB): a real `next build` can OOM at 3 GB,
          // and an OOM-kill would masquerade as a code failure. Pair with a
          // generous Node heap so the sandbox reports real errors, not memory.
          computeType: codebuild.ComputeType.MEDIUM,
          environmentVariables: {
            NODE_OPTIONS: { value: "--max-old-space-size=6144" },
          },
        },
        logging: { cloudWatch: { logGroup: sandboxLogGroup } },
        // NO_SOURCE: the worker passes a presigned URL for the tarball + the
        // derived check command as env overrides. The buildspec fetches, extracts, runs the
        // checks, and emits a parseable verdict marker. The verdict is read from
        // the marker (not the build status), so "checks failed" is cleanly
        // distinguishable from "the sandbox itself broke".
        // Two modes in one buildspec:
        //  - CHECKS  (default): run the repo's build/type-check, emit a verdict
        //    marker read by the worker (not the build status).
        //  - CODEGEN (when $CODEGEN_CMD is set): run the repo's codegen, capture
        //    ONLY the files it changed (find -newer), and upload them through the
        //    presigned PUT URL so the worker can commit them back — making the sandbox bidirectional so the
        //    agent can regenerate generated files (Sanity types, GraphQL, etc.).
        // `n` switches Node to the repo's version; the cd is gated so a bad
        // extract fails the build (no marker) instead of running in the wrong dir.
        buildSpec: codebuild.BuildSpec.fromObject({
          version: "0.2",
          phases: {
            build: {
              commands: [
                [
                  "set +e",
                  'curl -fsS --retry 3 -o /tmp/src.tgz "$SANDBOX_GET_URL"',
                  "mkdir -p /tmp/src && tar xzf /tmp/src.tgz -C /tmp/src",
                  "cd /tmp/src/$(ls /tmp/src) || exit 1",
                  'n "${NODE_VERSION:-20}" >/dev/null 2>&1 || true; hash -r 2>/dev/null || true',
                  "# Terraform: the image has none. Install the exact release the worker",
                  "# read from the repo (setup-terraform pin, .terraform-version or",
                  "# required_version), only digits and dots, after verifying HashiCorp's",
                  "# signature on the checksums and the zip's checksum. Checks only ever run",
                  "# fmt/init -backend=false/validate: never plan or apply.",
                  'if [ -n "$TERRAFORM_VERSION" ]; then',
                  '  case "$TERRAFORM_VERSION" in *[!0-9.]*) echo "ignoring malformed TERRAFORM_VERSION";; *)',
                  '  ( set -e; V="$TERRAFORM_VERSION"; A=amd64; [ "$(uname -m)" = aarch64 ] && A=arm64',
                  '    D=$(mktemp -d); cd "$D"; B="https://releases.hashicorp.com/terraform/$V"',
                  '    curl -fsSLo tf.zip "$B/terraform_${V}_linux_${A}.zip"',
                  '    curl -fsSLo SUMS "$B/terraform_${V}_SHA256SUMS"',
                  '    curl -fsSLo SUMS.sig "$B/terraform_${V}_SHA256SUMS.sig"',
                  '    export GNUPGHOME="$D/gnupg"; mkdir -m 700 "$GNUPGHOME"',
                  "    curl -fsSL https://www.hashicorp.com/.well-known/pgp-key.txt | gpg --batch --quiet --import",
                  '    gpg --batch --status-fd 1 --verify SUMS.sig SUMS 2>/dev/null | grep -q "VALIDSIG .* C874011F0AB405110D02105534365D9472D7468F$"',
                  '    grep " terraform_${V}_linux_${A}.zip$" SUMS | sed "s/terraform_.*\\.zip$/tf.zip/" | sha256sum -c -',
                  "    unzip -o -q tf.zip terraform -d /usr/local/bin )",
                  '  if [ $? -eq 0 ]; then echo "terraform $TERRAFORM_VERSION installed (signature and checksum verified)"; else echo "terraform $TERRAFORM_VERSION could not be installed and verified"; fi;;',
                  "  esac",
                  "fi",
                  "# The team's sandbox network allowlist: the worker sends the proxy and",
                  "# the list when the team limits the network. Tools that honor proxy",
                  "# settings (npm, pip, cargo, go, curl, git) reach only listed domains; a",
                  "# refused host is logged as CODERHELM_EGRESS_BLOCKED and reported.",
                  'if [ -n "$EGRESS_PROXY_B64" ]; then',
                  '  echo "$EGRESS_PROXY_B64" | base64 -d > /tmp/egress_proxy.py',
                  '  (python3 /tmp/egress_proxy.py > /tmp/egress.log 2>&1 &)',
                  '  for i in 1 2 3 4 5 6 7 8 9 10; do grep -q CODERHELM_EGRESS_READY /tmp/egress.log 2>/dev/null && break; sleep 0.5; done',
                  '  P=http://127.0.0.1:3128',
                  '  export HTTP_PROXY=$P HTTPS_PROXY=$P http_proxy=$P https_proxy=$P NO_PROXY=localhost,127.0.0.1 no_proxy=localhost,127.0.0.1',
                  '  export npm_config_proxy=$P npm_config_https_proxy=$P YARN_HTTP_PROXY=$P YARN_HTTPS_PROXY=$P',
                  '  echo "Sandbox network limited to the team allowlist"',
                  "fi",
                  'if [ -n "$CODEGEN_CMD" ]; then',
                  '  # Turbo 2.x prunes env vars in its default STRICT mode, stripping the',
                  '  # injected codegen config (e.g. Sanity projectId) before the task runs.',
                  '  # Loose mode passes them through. Harmless for non-turbo codegen, and',
                  '  # this is a one-shot build so turbo cache-correctness (why strict exists)',
                  '  # is irrelevant.',
                  '  export TURBO_ENV_MODE=loose',
                  '  # Snapshot pre-existing files so we commit ONLY tracked files that',
                  '  # codegen modifies — never node_modules (nested workspaces included)',
                  '  # or fresh intermediates (schema.json, caches) that pollute the PR.',
                  '  find . -type f -not -path "*/node_modules/*" -not -path "*/.git/*" | sort > /tmp/before.txt',
                  '  MARK=/tmp/mark; touch "$MARK"; sleep 1',
                  '  echo "===CODERHELM_CODEGEN_START==="; node --version',
                  '  (eval "$CODEGEN_CMD"); CODE=$?',
                  '  echo "===CODERHELM_CODEGEN_END exit=$CODE==="',
                  '  find . -type f -newer "$MARK" -not -path "*/node_modules/*" -not -path "*/.git/*" | sort > /tmp/newer.txt',
                  "  comm -12 /tmp/newer.txt /tmp/before.txt | sed 's|^\\./||' > /tmp/changed.txt",
                  '  echo "CHANGED:"; cat /tmp/changed.txt',
                  "  # The upload is CoderHelm's own hand-back through the URL the worker",
                  "  # signed, not the repo's traffic: it skips the team's allowlist proxy.",
                  '  if [ -s /tmp/changed.txt ] && [ -n "$SANDBOX_PUT_URL" ]; then',
                  '    if tar czf /tmp/out.tgz -T /tmp/changed.txt && curl -fsS --retry 3 --noproxy "*" -T /tmp/out.tgz "$SANDBOX_PUT_URL"; then echo "===CODERHELM_CODEGEN_UPLOADED==="; else echo "===CODERHELM_CODEGEN_UPLOAD_FAILED==="; fi',
                  "  fi",
                  "else",
                  '  echo "===CODERHELM_CHECKS_START==="; node --version',
                  '  if [ -n "$TERRAFORM_VERSION" ]; then terraform version 2>&1 | head -1; fi',
                  '  (eval "$CHECKS_CMD"); CODE=$?',
                  '  echo "===CODERHELM_CHECKS_END exit=$CODE==="',
                  "fi",
                  'if [ -f /tmp/egress.log ]; then grep CODERHELM_EGRESS_BLOCKED /tmp/egress.log | sort -u | head -20; fi',
                ].join("\n"),
              ],
            },
          },
        }),
      });

      // No bucket grant for the CodeBuild role: builds reach S3 only through
      // the presigned URLs the worker signs (with its own role) per build.

      // Worker: upload tarballs, sign each build's URLs, drive builds, read
      // their logs.
      sandboxBucket.grantReadWrite(this.workerFunction);
      this.workerFunction.addToRolePolicy(
        new iam.PolicyStatement({
          actions: [
            "codebuild:StartBuild",
            "codebuild:BatchGetBuilds",
            "codebuild:StopBuild",
          ],
          resources: [sandboxProject.projectArn],
        })
      );
      sandboxLogGroup.grantRead(this.workerFunction);
      this.workerFunction.addEnvironment(
        "SANDBOX_BUCKET_NAME",
        sandboxBucket.bucketName
      );
      this.workerFunction.addEnvironment(
        "SANDBOX_PROJECT_NAME",
        sandboxProject.projectName
      );
    }

    // --- Stuck Run Cleanup Lambda (Python) ---
    const cleanupLogGroup = new logs.LogGroup(this, "CleanupLogGroup", {
      logGroupName: `/aws/lambda/${prefix}-stuck-run-cleanup`,
      retention: logs.RetentionDays.ONE_MONTH,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const cleanupFunction = new lambda.Function(this, "StuckRunCleanup", {
      functionName: `${prefix}-stuck-run-cleanup`,
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: "handler.handler",
      code: lambda.Code.fromAsset(
        repoPath("lambda/stuck-run-cleanup")
      ),
      memorySize: 128,
      timeout: cdk.Duration.seconds(30),
      logGroup: cleanupLogGroup,
      environment: {
        RUNS_TABLE_NAME: `${props.cfg.prefix}-runs`,
        MAX_RUN_MINUTES: "20",
        // The last pass a stuck run reached when its record names none.
        TRACES_TABLE_NAME: props.tracesTable.tableName,
        CHECKPOINTS_TABLE_NAME: props.checkpointsTable.tableName,
      },
    });

    runsTable.grantReadWriteData(cleanupFunction);
    props.tracesTable.grantReadData(cleanupFunction);
    props.checkpointsTable.grantReadData(cleanupFunction);
    // The runs table is CMK-encrypted; grantReadWriteData on a fromTableName
    // ref does NOT include KMS, so every scan failed with AccessDenied and the
    // reaper silently never cleaned a single stuck run. Grant decrypt/encrypt.
    props.encryptionKey.grantEncryptDecrypt(cleanupFunction);

    new events.Rule(this, "StuckRunCleanupSchedule", {
      ruleName: `${prefix}-stuck-run-cleanup`,
      schedule: events.Schedule.rate(cdk.Duration.minutes(10)),
      targets: [new targets.LambdaFunction(cleanupFunction)],
    });
    // NOTE: the review-reminder EventBridge → SQS schedule lives in the API stack
    // (where the ticket queue is defined). Putting it here created a worker→api
    // dependency cycle (api already depends on worker).

    // Hosted deployment only (ci.reviewerEvalsRepo): the nightly evals'
    // role for that repository's CI.
    if (cfg.reviewerEvalsRepo) {
      // ── Nightly reviewer evals (.github/workflows/reviewer-evals.yml) ──
      // The live evals call the model through Amazon Bedrock with this role's
      // short-lived credentials, so CI stores no model key and never touches a
      // team's key. Least privilege: GitHub's OIDC token from this repository's
      // main branch only (the nightly schedule and manual runs both run there),
      // and the one permission to invoke two models: the cheap eval model for
      // the classification checks, and the model the reviewer runs on in
      // production for the diff reviews (a small model passes cases the
      // production reviewer fails). Nothing else in the account is readable.
      const githubOidc = iam.OpenIdConnectProvider.fromOpenIdConnectProviderArn(
        this,
        "GithubActionsOidc",
        `arn:aws:iam::${this.account}:oidc-provider/token.actions.githubusercontent.com`,
      );
      const evalModel = "anthropic.claude-haiku-4-5-20251001-v1:0";
      // The reviewer's model (the worker's MODEL_ID default above, which
      // generate_review and the critic run on). Keep EVAL_REVIEW_MODEL in
      // .github/workflows/reviewer-evals.yml in step with it.
      const evalReviewModel = "anthropic.claude-opus-5-5";
      const evalRole = new iam.Role(this, "ReviewerEvalsRole", {
        roleName: `${prefix}-reviewer-evals`,
        description:
          "Nightly reviewer evals (GitHub Actions, main branch): invoke the eval and reviewer models on Bedrock",
        maxSessionDuration: cdk.Duration.hours(1),
        assumedBy: new iam.WebIdentityPrincipal(githubOidc.openIdConnectProviderArn, {
          StringEquals: {
            "token.actions.githubusercontent.com:aud": "sts.amazonaws.com",
            "token.actions.githubusercontent.com:sub":
              `repo:${cfg.reviewerEvalsRepo}:ref:refs/heads/main`,
          },
        }),
      });
      evalRole.addToPolicy(
        new iam.PolicyStatement({
          actions: ["bedrock:InvokeModel"],
          // For each model, the US cross-region inference profile and the
          // model it routes to (any of the profile's regions).
          resources: [evalModel, evalReviewModel].flatMap((model) => [
            `arn:aws:bedrock:${this.region}:${this.account}:inference-profile/us.${model}`,
            `arn:aws:bedrock:*::foundation-model/${model}`,
          ]),
        }),
      );
      new cdk.CfnOutput(this, "ReviewerEvalsRoleArn", { value: evalRole.roleArn });

      // ── Release publishing (.github/workflows/release.yml, launch-template job) ──
      // A tag's Launch Stack template goes to the public bucket under
      // launch/<tag>/ (CloudFormation only opens templates stored in S3).
      // Only tag builds of the platform repository may assume it, and it may
      // only write objects under launch/ (the bucket's policy makes them
      // public; no ACLs).
      if (cfg.publicBucketName) {
        const publisher = new iam.Role(this, "ReleasePublisherRole", {
          roleName: `${prefix}-release-publisher`,
          description: "Release workflow (GitHub Actions, release environment): publishes the Launch Stack template to the public bucket",
          maxSessionDuration: cdk.Duration.hours(1),
          assumedBy: new iam.WebIdentityPrincipal(githubOidc.openIdConnectProviderArn, {
            // Only a job in the repository's "release" environment (which
            // needs a reviewer's approval, .github/workflows/release.yml):
            // with an environment, GitHub's token names it, not the tag.
            StringEquals: {
              "token.actions.githubusercontent.com:aud": "sts.amazonaws.com",
              "token.actions.githubusercontent.com:sub": `repo:${RELEASE_REPO}:environment:release`,
            },
          }),
        });
        publisher.addToPolicy(
          new iam.PolicyStatement({
            actions: ["s3:PutObject"],
            resources: [`arn:aws:s3:::${cfg.publicBucketName}/launch/*`],
          }),
        );
        new cdk.CfnOutput(this, "ReleasePublisherRoleArn", { value: publisher.roleArn });
      }
    }
  }
}
