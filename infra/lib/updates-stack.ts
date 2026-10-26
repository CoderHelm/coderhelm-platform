/**
 * Updates for a self-hosted install, run in its own account.
 *
 * - The updater Lambda (lambda/updater) checks GitHub for releases every
 *   hour, moves a domain added from Settings along (every 5 minutes:
 *   certificate, then the build that puts it on CloudFront), emails install admins about a new one (and posts to
 *   `updates.webhookUrl`), and starts the update build when auto-update
 *   allows it: on, in the maintenance window, majors only when allowed.
 *   Settings → System and `coderhelm-selfhost auto-update` change its
 *   settings; "Update now" and "Roll back" start the same build.
 * - The update build (CodeBuild) installs the target release's installer
 *   after checking its checksum against the release's signed SHA256SUMS
 *   (cosign, keyless: signed by this repository's release workflow), then
 *   runs `coderhelm-selfhost update`: health check, artifact checks, deploy,
 *   health check, and a rollback to the previous release if the new one is
 *   unhealthy. It reports the result back to the updater.
 *
 * The build's role is limited to what a CDK deploy and the health check
 * need: it assumes the CDK bootstrap roles (which do the deploying), reads
 * this install's stacks, functions, queues, secret and installer state, and
 * reports to the updater.
 */
import * as cdk from "aws-cdk-lib";
import * as codebuild from "aws-cdk-lib/aws-codebuild";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as events from "aws-cdk-lib/aws-events";
import * as targets from "aws-cdk-lib/aws-events-targets";
import * as iam from "aws-cdk-lib/aws-iam";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as logs from "aws-cdk-lib/aws-logs";
import { Construct } from "constructs";
import { ResolvedConfig } from "./config";
import { repoPath } from "./paths";
import { releaseStamp, stampEnvironment } from "./release";
import { CHECK_VERSION, CURL, installCosignCommands, RELEASE_BASE, RELEASE_REPO, VERIFY_SUMS } from "./release-verify";

/** The functions that read the install's secret once, at start. */
export function secretReaders(prefix: string): string[] {
  return [`${prefix}-gateway`, `${prefix}-streaming`, `${prefix}-worker`];
}

export { COSIGN_VERSION } from "./release-verify";

export interface UpdatesStackProps extends cdk.StackProps {
  cfg: ResolvedConfig;
  appUrl: string;
  settingsTable: dynamodb.ITableV2;
}

/** The update build's script (also checked by the tests). */
/**
 * The release check (read-only): before the updater applies a release, it
 * needs the models that release calls. This build checks SHA256SUMS's
 * signature exactly as the update build does, then reads models.json only
 * when the signed SHA256SUMS lists it and its checksum matches, and reports
 * it to the updater (models_verified). It installs and deploys nothing, and
 * its role can only report back.
 */
export function releaseCheckBuildSpec(cfg: ResolvedConfig): codebuild.BuildSpec {
  return codebuild.BuildSpec.fromObject({
    version: "0.2",
    env: { shell: "bash" },
    phases: {
      install: { "runtime-versions": { nodejs: 20 }, commands: ["set -euo pipefail", ...installCosignCommands()] },
      build: {
        commands: [
          "set -euo pipefail",
          CHECK_VERSION,
          "mkdir -p /tmp/ch && cd /tmp/ch",
          RELEASE_BASE,
          `${CURL} -o SHA256SUMS "$B/SHA256SUMS"`,
          `${CURL} -o SHA256SUMS.sigstore.json "$B/SHA256SUMS.sigstore.json"`,
          VERIFY_SUMS,
          // A signed release from before model manifests lists none.
          "if grep -q ' models.json$' SHA256SUMS; then " +
            `${CURL} -o models.json "$B/models.json"; ` +
            "grep ' models.json$' SHA256SUMS | sha256sum -c -; " +
            'node -e \'process.stdout.write(JSON.stringify({status:"verified",manifest:JSON.parse(require("fs").readFileSync("models.json","utf8"))}))\' > /tmp/result.json; ' +
            'else echo \'{"status":"verified","manifest":null}\' > /tmp/result.json; fi',
        ],
      },
      post_build: {
        commands: [
          // Anything short of a verified result reports a failure (nothing is trusted).
          'if [ "${CODEBUILD_BUILD_SUCCEEDING:-0}" != "1" ] || [ ! -s /tmp/result.json ]; then echo \'{"status":"failed","reason":"the release check failed: see its log"}\' > /tmp/result.json; fi',
          'node -e \'const r=require("/tmp/result.json");process.stdout.write(JSON.stringify({action:"models_verified",version:process.env.VERSION,status:r.status,reason:r.reason,manifest:r.manifest??null}))\' > /tmp/payload.json',
          `aws lambda invoke --function-name ${cfg.prefix}-updater --cli-binary-format raw-in-base64-out --payload file:///tmp/payload.json /tmp/out.json`,
        ],
      },
    },
  });
}

export function updateBuildSpec(cfg: ResolvedConfig): codebuild.BuildSpec {
  return codebuild.BuildSpec.fromObject({
    version: "0.2",
    env: { shell: "bash" },
    phases: {
      install: {
        "runtime-versions": { nodejs: 20 },
        commands: [
          "set -euo pipefail",
          // cosign, pinned by its SHA-256 (infra/lib/release-verify.ts).
          ...installCosignCommands(),
        ],
      },
      build: {
        commands: [
          "set -euo pipefail",
          CHECK_VERSION,
          "mkdir -p /tmp/ch && cd /tmp/ch",
          RELEASE_BASE,
          `${CURL} -o SHA256SUMS "$B/SHA256SUMS"`,
          `${CURL} -o SHA256SUMS.sigstore.json "$B/SHA256SUMS.sigstore.json"`,
          // Signed by exactly this repository's release.yml, on this tag, in Rekor.
          VERIFY_SUMS,
          `${CURL} -o coderhelm-selfhost.tgz "$B/coderhelm-selfhost.tgz"`,
          "grep ' coderhelm-selfhost.tgz$' SHA256SUMS | sha256sum -c -",
          // The installer then checks every other file of the release the same way,
          // and refuses an older release unless it is a rollback to one this install ran.
          "npm install -g --ignore-scripts ./coderhelm-selfhost.tgz",
          `coderhelm-selfhost pull --stage ${cfg.stage} --region "$AWS_REGION"${cfg.raw.resourcePrefix ? ` --resource-prefix ${cfg.raw.resourcePrefix}` : ""}`,
          'EXTRA=""; if [ "$MODE" = "rollback" ]; then EXTRA="--rollback"; fi',
          // A domain change (MODE=domain) redeploys the release that runs,
          // with the new address in the config (installer/lib/domain.mjs).
          'if [ "$MODE" = "domain" ]; then ' +
            'coderhelm-selfhost domain apply --yes --require-signature --version "$VERSION" --action "${DOMAIN_ACTION:-attach}" --hostname "${DOMAIN_HOSTNAME:-}" --certificate-arn "${DOMAIN_CERT:-}" --zone "${DOMAIN_ZONE:-}" --zone-id "${DOMAIN_ZONE_ID:-}" --result-file /tmp/result.json; ' +
            'else coderhelm-selfhost update --to "$VERSION" --yes --require-signature --auto-rollback --actor "$ACTOR" --result-file /tmp/result.json $EXTRA; fi',
        ],
      },
      post_build: {
        commands: [
          // Tell the updater how it went (also when the build failed early).
          'if [ ! -s /tmp/result.json ]; then echo "{\\"status\\":\\"failed\\",\\"reason\\":\\"the update build stopped before deploying: see its log\\"}" > /tmp/result.json; fi',
          'node -e \'const r=require("/tmp/result.json");process.stdout.write(JSON.stringify({action:"finished",history_id:process.env.HISTORY_ID,status:r.status,reason:r.reason,version:process.env.VERSION}))\' > /tmp/payload.json',
          `aws lambda invoke --function-name ${cfg.prefix}-updater --cli-binary-format raw-in-base64-out --payload file:///tmp/payload.json /tmp/out.json`,
        ],
      },
    },
  });
}

export class UpdatesStack extends cdk.Stack {
  public readonly updaterFunction: lambda.Function;
  public readonly project: codebuild.Project;
  public readonly checkProject: codebuild.Project;

  constructor(scope: Construct, id: string, props: UpdatesStackProps) {
    super(scope, id, props);
    const cfg = props.cfg;
    const prefix = cfg.prefix;
    const stamp = releaseStamp(cfg);

    // --- The update build ---
    const buildRole = new iam.Role(this, "UpdateBuildRole", {
      roleName: `${prefix}-update-build`,
      description: "CoderHelm update build: deploys a release through the CDK bootstrap roles, checks health",
      assumedBy: new iam.ServicePrincipal("codebuild.amazonaws.com"),
    });
    buildRole.addToPolicy(new iam.PolicyStatement({
      sid: "DeployThroughCdkBootstrapRoles",
      actions: ["sts:AssumeRole"],
      resources: [`arn:aws:iam::${this.account}:role/cdk-*`],
    }));
    buildRole.addToPolicy(new iam.PolicyStatement({
      sid: "ReadInstallState",
      actions: ["ssm:GetParameter", "ssm:PutParameter"],
      resources: [`arn:aws:ssm:${this.region}:${this.account}:parameter/${prefix}/installer/*`],
    }));
    buildRole.addToPolicy(new iam.PolicyStatement({
      sid: "CdkBootstrapVersion",
      actions: ["ssm:GetParameter"],
      resources: [`arn:aws:ssm:*:${this.account}:parameter/cdk-bootstrap/*`],
    }));
    buildRole.addToPolicy(new iam.PolicyStatement({
      sid: "HealthCheck",
      actions: [
        "cloudformation:ListStacks",
        "cloudformation:DescribeStacks",
        "sts:GetCallerIdentity",
        "ses:GetAccount",
      ],
      resources: ["*"],
    }));
    buildRole.addToPolicy(new iam.PolicyStatement({
      sid: "HealthCheckResources",
      actions: ["lambda:GetFunctionConfiguration", "sqs:GetQueueUrl", "sqs:GetQueueAttributes", "secretsmanager:GetSecretValue"],
      resources: [
        `arn:aws:lambda:${this.region}:${this.account}:function:${prefix}-*`,
        `arn:aws:sqs:${this.region}:${this.account}:${prefix}-*`,
        `arn:aws:secretsmanager:${this.region}:${this.account}:secret:${cfg.secretName}-??????`,
      ],
    }));
    buildRole.addToPolicy(new iam.PolicyStatement({
      sid: "ReportResult",
      actions: ["lambda:InvokeFunction"],
      resources: [`arn:aws:lambda:${this.region}:${this.account}:function:${prefix}-updater`],
    }));

    const buildLogs = new logs.LogGroup(this, "UpdateBuildLogs", {
      logGroupName: `/aws/codebuild/${prefix}-update`,
      retention: logs.RetentionDays.THREE_MONTHS,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });
    this.project = new codebuild.Project(this, "UpdateBuild", {
      projectName: `${prefix}-update`,
      description: "Updates CoderHelm to a release (started by the updater, Update now or Roll back)",
      role: buildRole,
      timeout: cdk.Duration.minutes(90),
      concurrentBuildLimit: 1,
      environment: {
        buildImage: codebuild.LinuxBuildImage.STANDARD_7_0,
        computeType: codebuild.ComputeType.SMALL,
        environmentVariables: {
          VERSION: { value: "" },
          ACTOR: { value: "auto" },
          MODE: { value: "update" },
          HISTORY_ID: { value: "" },
          DOMAIN_ACTION: { value: "" },
          DOMAIN_HOSTNAME: { value: "" },
          DOMAIN_CERT: { value: "" },
          DOMAIN_ZONE: { value: "" },
          DOMAIN_ZONE_ID: { value: "" },
        },
      },
      logging: { cloudWatch: { logGroup: buildLogs } },
      buildSpec: updateBuildSpec(cfg),
    });

    // --- The release check (read-only; the model check reads its result) ---
    const checkRole = new iam.Role(this, "ReleaseCheckRole", {
      roleName: `${prefix}-release-check`,
      description: "CoderHelm release check: verifies a release's signature and reads its model list; can only report to the updater",
      assumedBy: new iam.ServicePrincipal("codebuild.amazonaws.com"),
    });
    checkRole.addToPolicy(new iam.PolicyStatement({
      sid: "ReportResult",
      actions: ["lambda:InvokeFunction"],
      resources: [`arn:aws:lambda:${this.region}:${this.account}:function:${prefix}-updater`],
    }));
    const checkLogs = new logs.LogGroup(this, "ReleaseCheckLogs", {
      logGroupName: `/aws/codebuild/${prefix}-release-check`,
      retention: logs.RetentionDays.ONE_MONTH,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });
    this.checkProject = new codebuild.Project(this, "ReleaseCheck", {
      projectName: `${prefix}-release-check`,
      description: "Checks a CoderHelm release's signature and reads the models it calls (no install, no deploy)",
      role: checkRole,
      timeout: cdk.Duration.minutes(15),
      concurrentBuildLimit: 1,
      environment: {
        buildImage: codebuild.LinuxBuildImage.STANDARD_7_0,
        computeType: codebuild.ComputeType.SMALL,
        environmentVariables: { VERSION: { value: "" } },
      },
      logging: { cloudWatch: { logGroup: checkLogs } },
      buildSpec: releaseCheckBuildSpec(cfg),
    });

    // --- The updater ---
    const updaterLogs = new logs.LogGroup(this, "UpdaterLogs", {
      logGroupName: `/aws/lambda/${prefix}-updater`,
      retention: logs.RetentionDays.THREE_MONTHS,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });
    this.updaterFunction = new lambda.Function(this, "Updater", {
      functionName: `${prefix}-updater`,
      description: "CoderHelm updates: release checks, notices, auto-update, history",
      runtime: lambda.Runtime.PYTHON_3_12,
      architecture: lambda.Architecture.ARM_64,
      handler: "handler.handler",
      code: lambda.Code.fromAsset(repoPath("lambda/updater"), { exclude: ["__pycache__"] }),
      memorySize: 256,
      // The model check waits on the worker's test calls (retried when Bedrock is busy).
      timeout: cdk.Duration.minutes(3),
      logGroup: updaterLogs,
      environment: {
        SETTINGS_TABLE_NAME: props.settingsTable.tableName,
        UPDATE_PROJECT_NAME: this.project.projectName,
        UPDATE_DEFAULTS: JSON.stringify(cfg.updates.defaults),
        ADMIN_EMAILS: cfg.admins.join(","),
        SES_FROM_ADDRESS: cfg.email.from,
        APP_URL: props.appUrl,
        RELEASE_REPO,
        // Restarted after the gateway stores new keys (refresh_secrets).
        REFRESH_FUNCTIONS: secretReaders(prefix).join(","),
        // Your own domain (docs/self-host/domains.md).
        DISTRIBUTION_DOMAIN_PARAMETER: cfg.distributionDomainParameter,
        RESOURCE_PREFIX: prefix,
        // The model check before an update: the worker makes the test calls
        // for every model the next release calls, plus the config's own.
        WORKER_FUNCTION_NAME: `${prefix}-worker`,
        RELEASE_CHECK_PROJECT_NAME: `${prefix}-release-check`,
        CONFIGURED_MODELS: [...new Set([cfg.models.primary, cfg.models.light])].join(","),
        ...(cfg.updates.webhookUrl ? { UPDATE_WEBHOOK_URL: cfg.updates.webhookUrl } : {}),
        ...stampEnvironment(stamp),
      },
    });
    props.settingsTable.grantReadWriteData(this.updaterFunction);
    this.updaterFunction.addToRolePolicy(new iam.PolicyStatement({
      actions: ["codebuild:StartBuild"],
      resources: [this.project.projectArn, this.checkProject.projectArn],
    }));
    this.updaterFunction.addToRolePolicy(new iam.PolicyStatement({
      actions: ["ses:SendEmail"],
      resources: ["*"],
      conditions: { StringEquals: { "ses:FromAddress": cfg.email.from } },
    }));

    // The model check: the worker makes the test calls with its own role (or
    // a team's key). The updater itself never calls a model, and nothing in
    // the install may subscribe in AWS Marketplace or accept model terms.
    this.updaterFunction.addToRolePolicy(new iam.PolicyStatement({
      sid: "ModelCheckThroughWorker",
      actions: ["lambda:InvokeFunction"],
      resources: [`arn:aws:lambda:${this.region}:${this.account}:function:${prefix}-worker`],
    }));

    // refresh_secrets: stamp SECRETS_REVISION on the functions that read
    // the secret at start (nothing else about them can change this way).
    this.updaterFunction.addToRolePolicy(new iam.PolicyStatement({
      sid: "RestartSecretReaders",
      actions: ["lambda:GetFunctionConfiguration", "lambda:UpdateFunctionConfiguration"],
      resources: secretReaders(prefix).map((f) => `arn:aws:lambda:${this.region}:${this.account}:function:${f}`),
    }));

    // Your own domain: request its certificate in us-east-1 (CloudFront's
    // region), add the validation record when the zone is in Route 53 here,
    // and read the CloudFront hostname for the record people add elsewhere.
    this.updaterFunction.addToRolePolicy(new iam.PolicyStatement({
      sid: "DomainCertificate",
      actions: ["acm:DescribeCertificate", "acm:AddTagsToCertificate"],
      resources: [`arn:aws:acm:us-east-1:${this.account}:certificate/*`],
    }));
    this.updaterFunction.addToRolePolicy(new iam.PolicyStatement({
      sid: "DomainCertificateRequest",
      actions: ["acm:RequestCertificate"],
      resources: ["*"],
      conditions: { StringEquals: { "aws:RequestedRegion": "us-east-1" } },
    }));
    this.updaterFunction.addToRolePolicy(new iam.PolicyStatement({
      sid: "DomainZoneLookup",
      actions: ["route53:ListHostedZones", "route53:GetHostedZone"],
      resources: ["*"],
    }));
    this.updaterFunction.addToRolePolicy(new iam.PolicyStatement({
      sid: "DomainValidationRecord",
      actions: ["route53:ChangeResourceRecordSets"],
      resources: ["arn:aws:route53:::hostedzone/*"],
      conditions: {
        // Only the CNAME ACM checks (its name always starts with an underscore).
        "ForAllValues:StringEquals": { "route53:ChangeResourceRecordSetsRecordTypes": ["CNAME"] },
        "ForAllValues:StringLike": { "route53:ChangeResourceRecordSetsNormalizedRecordNames": ["_*"] },
      },
    }));
    this.updaterFunction.addToRolePolicy(new iam.PolicyStatement({
      sid: "DomainTarget",
      actions: ["ssm:GetParameter"],
      resources: [`arn:aws:ssm:${this.region}:${this.account}:parameter${cfg.distributionDomainParameter}`],
    }));

    new events.Rule(this, "DomainCheck", {
      ruleName: `${prefix}-domain-check`,
      description: "Every 5 minutes: a domain waiting for its certificate? Attach it once issued",
      schedule: events.Schedule.rate(cdk.Duration.minutes(5)),
      targets: [new targets.LambdaFunction(this.updaterFunction, { event: events.RuleTargetInput.fromObject({ action: "domain_status" }) })],
    });

    new events.Rule(this, "UpdateCheck", {
      ruleName: `${prefix}-update-check`,
      description: "Hourly: new CoderHelm release? Notify, or apply in the maintenance window",
      schedule: events.Schedule.rate(cdk.Duration.hours(1)),
      targets: [new targets.LambdaFunction(this.updaterFunction, { event: events.RuleTargetInput.fromObject({ action: "tick" }) })],
    });

    new cdk.CfnOutput(this, "UpdaterFunctionName", { value: this.updaterFunction.functionName });
    new cdk.CfnOutput(this, "UpdateProjectName", { value: this.project.projectName });
  }
}
