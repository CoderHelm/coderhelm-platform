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
import * as path from "path";
import { WEEKLY_SUMMARY_ENABLED, WEEKLY_SUMMARY_LISTED_TEAMS_ONLY } from "./flags";

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
  eventsTable: dynamodb.TableV2;
  testsTable: dynamodb.TableV2;
  bucket: s3.Bucket;
  ticketQueue: sqs.Queue;
  ciFixQueue: sqs.Queue;
  feedbackQueue: sqs.Queue;
  /** The shared DLQ: the daily cleanup archives and deletes unreadable messages. */
  dlq: sqs.Queue;
  mcpProxyFunction: lambda.Function;
  encryptionKey: kms.Key;
  secretsKey: kms.Key;
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
        TEAMS_TABLE_NAME: `coderhelm-${props.stage}-teams`,
        RUNS_TABLE_NAME: `coderhelm-${props.stage}-runs`,
        ANALYTICS_TABLE_NAME: `coderhelm-${props.stage}-analytics`,
        USERS_TABLE_NAME: props.usersTable.tableName,
        PLANS_TABLE_NAME: props.plansTable.tableName,
        JIRA_CONFIG_TABLE_NAME: props.jiraConfigTable.tableName,
        REPOS_TABLE_NAME: props.reposTable.tableName,
        SETTINGS_TABLE_NAME: props.settingsTable.tableName,
        // Connected AWS accounts and what each can do (logs, metrics, Airflow).
        AWS_INSIGHTS_TABLE_NAME: `coderhelm-${props.stage}-aws-insights`,
        MCP_CONFIGS_TABLE_NAME: props.mcpConfigsTable.tableName,
        INFRA_TABLE_NAME: props.infraTable.tableName,
        BILLING_TABLE_NAME: props.billingTable.tableName, // Retained for CF migration
        TRACES_TABLE_NAME: props.tracesTable.tableName,
        CHECKPOINTS_TABLE_NAME: props.checkpointsTable.tableName,
        EVENTS_TABLE_NAME: props.eventsTable.tableName,
        TESTS_TABLE_NAME: props.testsTable.tableName,
        BUCKET_NAME: props.bucket.bucketName,
        SECRETS_NAME: `coderhelm/${props.stage}/secrets`,
        MODEL_ID: process.env.MODEL_ID || "claude-opus-5-5",
        LIGHT_MODEL_ID: process.env.LIGHT_MODEL_ID || "claude-sonnet-5-5",
        SES_FROM_ADDRESS: "noreply@coderhelm.com",
        SES_TEMPLATE_PREFIX: `coderhelm-${props.stage}`,
        // The Monday weekly summary sends nothing unless this is "true".
        WEEKLY_SUMMARY_ENABLED: WEEKLY_SUMMARY_ENABLED ? "true" : "false",
        // While "true", only teams with a rollout list get the summary.
        WEEKLY_SUMMARY_LISTED_TEAMS_ONLY: WEEKLY_SUMMARY_LISTED_TEAMS_ONLY ? "true" : "false",
        CI_FIX_QUEUE_URL: props.ciFixQueue.queueUrl,
        TICKET_QUEUE_URL: props.ticketQueue.queueUrl,
        FEEDBACK_QUEUE_URL: props.feedbackQueue.queueUrl,
        DLQ_URL: props.dlq.queueUrl,
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
    // Agents read a connected AWS account's abilities (metrics, Airflow) from
    // its record; they never change it.
    const awsInsightsTable = dynamodb.TableV2.fromTableName(this, "AwsInsightsTableRef", `coderhelm-${props.stage}-aws-insights`);
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
    // run lookups and tool calls, never seals or writes them.
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
    props.mcpConfigsTable.grantReadData(this.workerFunction);
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
          `arn:aws:secretsmanager:${this.region}:${this.account}:secret:coderhelm/${props.stage}/anthropic-admin-keys-??????`,
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

    // Grant worker permission to invoke the MCP proxy (owned by api-stack)
    props.mcpProxyFunction.grantInvoke(this.workerFunction);
    this.workerFunction.addEnvironment(
      "MCP_PROXY_FUNCTION_NAME",
      props.mcpProxyFunction.functionName
    );

    // --- Execution sandbox (CodeBuild) ---
    // Runs each repo's REAL build/lint/tests in an isolated container so the
    // agent iterates against ground truth instead of guessing at CI. The
    // sandbox runs untrusted customer build scripts, so its role is minimal by
    // design: read ONE ephemeral S3 bucket + write its own logs. No GitHub
    // token, no DynamoDB, no KMS. SSE-S3 (not the account CMK) on the bucket is
    // what keeps the CodeBuild role KMS-free.
    const sandboxLogGroup = new logs.LogGroup(this, "SandboxLogGroup", {
      logGroupName: `/aws/codebuild/${prefix}-sandbox`,
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const sandboxBucket = new s3.Bucket(this, "SandboxBucket", {
      bucketName: `${prefix}-sandbox`,
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
      // NO_SOURCE: the worker passes the tarball location + the derived check
      // command as env overrides. The buildspec fetches, extracts, runs the
      // checks, and emits a parseable verdict marker. The verdict is read from
      // the marker (not the build status), so "checks failed" is cleanly
      // distinguishable from "the sandbox itself broke".
      // Two modes in one buildspec:
      //  - CHECKS  (default): run the repo's build/type-check, emit a verdict
      //    marker read by the worker (not the build status).
      //  - CODEGEN (when $CODEGEN_CMD is set): run the repo's codegen, capture
      //    ONLY the files it changed (find -newer), and upload them to S3 so the
      //    worker can commit them back — making the sandbox bidirectional so the
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
                'aws s3 cp "s3://$SANDBOX_BUCKET/$SANDBOX_KEY" /tmp/src.tgz',
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
                '  if [ -s /tmp/changed.txt ] && [ -n "$CODEGEN_OUT_KEY" ]; then tar czf /tmp/out.tgz -T /tmp/changed.txt && aws s3 cp /tmp/out.tgz "s3://$SANDBOX_BUCKET/$CODEGEN_OUT_KEY" && echo "===CODERHELM_CODEGEN_UPLOADED==="; fi',
                "else",
                '  echo "===CODERHELM_CHECKS_START==="; node --version',
                '  if [ -n "$TERRAFORM_VERSION" ]; then terraform version 2>&1 | head -1; fi',
                '  (eval "$CHECKS_CMD"); CODE=$?',
                '  echo "===CODERHELM_CHECKS_END exit=$CODE==="',
                "fi",
              ].join("\n"),
            ],
          },
        },
      }),
    });

    // CodeBuild reads the source tarball AND writes captured codegen output —
    // both scoped to this ephemeral bucket only (no other creds).
    sandboxBucket.grantReadWrite(sandboxProject);

    // Worker: upload tarballs, drive builds, read their logs.
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
        path.join(__dirname, "../../lambda/stuck-run-cleanup")
      ),
      memorySize: 128,
      timeout: cdk.Duration.seconds(30),
      logGroup: cleanupLogGroup,
      environment: {
        RUNS_TABLE_NAME: `coderhelm-${props.stage}-runs`,
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
            "repo:CoderHelm/coderhelm-platform:ref:refs/heads/main",
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
  }
}
