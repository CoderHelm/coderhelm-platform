/**
 * Updates for a self-hosted install, run in its own account.
 *
 * - The updater Lambda (lambda/updater) checks GitHub for releases every
 *   hour, emails install admins about a new one (and posts to
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

/** The cosign release the update build installs (checked against its checksums). */
export const COSIGN_VERSION = "v2.4.1";

export interface UpdatesStackProps extends cdk.StackProps {
  cfg: ResolvedConfig;
  appUrl: string;
  settingsTable: dynamodb.ITableV2;
}

/** The update build's script (also checked by the tests). */
export function updateBuildSpec(cfg: ResolvedConfig): codebuild.BuildSpec {
  const repo = "CoderHelm/coderhelm-platform";
  const identity = `^https://github.com/${repo}/.github/workflows/release.yml@refs/tags/v`;
  return codebuild.BuildSpec.fromObject({
    version: "0.2",
    env: { shell: "bash" },
    phases: {
      install: {
        "runtime-versions": { nodejs: 20 },
        commands: [
          "set -euo pipefail",
          // cosign, checked against its release's checksums.
          `curl -fsSLo /tmp/cosign https://github.com/sigstore/cosign/releases/download/${COSIGN_VERSION}/cosign-linux-amd64`,
          `curl -fsSLo /tmp/cosign_checksums.txt https://github.com/sigstore/cosign/releases/download/${COSIGN_VERSION}/cosign_checksums.txt`,
          "(cd /tmp && grep ' cosign-linux-amd64$' cosign_checksums.txt | sed 's/cosign-linux-amd64$/cosign/' | sha256sum -c -)",
          "install -m 0755 /tmp/cosign /usr/local/bin/cosign",
        ],
      },
      build: {
        commands: [
          "set -euo pipefail",
          "mkdir -p /tmp/ch && cd /tmp/ch",
          `B="https://github.com/${repo}/releases/download/$VERSION"`,
          'curl -fsSLo SHA256SUMS "$B/SHA256SUMS"',
          'curl -fsSLo SHA256SUMS.sigstore.json "$B/SHA256SUMS.sigstore.json"',
          // The checksums were signed by the release workflow for this repo.
          `cosign verify-blob --bundle SHA256SUMS.sigstore.json --certificate-identity-regexp '${identity}' --certificate-oidc-issuer https://token.actions.githubusercontent.com SHA256SUMS`,
          'curl -fsSLo coderhelm-selfhost.tgz "$B/coderhelm-selfhost.tgz"',
          "grep ' coderhelm-selfhost.tgz$' SHA256SUMS | sha256sum -c -",
          "npm install -g ./coderhelm-selfhost.tgz",
          `coderhelm-selfhost pull --stage ${cfg.stage} --region "$AWS_REGION"${cfg.raw.resourcePrefix ? ` --resource-prefix ${cfg.raw.resourcePrefix}` : ""}`,
          'EXTRA=""; if [ "$MODE" = "rollback" ]; then EXTRA="--rollback"; fi',
          'coderhelm-selfhost update --to "$VERSION" --yes --require-signature --auto-rollback --actor "$ACTOR" --result-file /tmp/result.json $EXTRA',
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
        },
      },
      logging: { cloudWatch: { logGroup: buildLogs } },
      buildSpec: updateBuildSpec(cfg),
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
      timeout: cdk.Duration.seconds(60),
      logGroup: updaterLogs,
      environment: {
        SETTINGS_TABLE_NAME: props.settingsTable.tableName,
        UPDATE_PROJECT_NAME: this.project.projectName,
        UPDATE_DEFAULTS: JSON.stringify(cfg.updates.defaults),
        ADMIN_EMAILS: cfg.admins.join(","),
        SES_FROM_ADDRESS: cfg.email.from,
        APP_URL: props.appUrl,
        RELEASE_REPO: "CoderHelm/coderhelm-platform",
        ...(cfg.updates.webhookUrl ? { UPDATE_WEBHOOK_URL: cfg.updates.webhookUrl } : {}),
        ...stampEnvironment(stamp),
      },
    });
    props.settingsTable.grantReadWriteData(this.updaterFunction);
    this.updaterFunction.addToRolePolicy(new iam.PolicyStatement({
      actions: ["codebuild:StartBuild"],
      resources: [this.project.projectArn],
    }));
    this.updaterFunction.addToRolePolicy(new iam.PolicyStatement({
      actions: ["ses:SendEmail"],
      resources: ["*"],
      conditions: { StringEquals: { "ses:FromAddress": cfg.email.from } },
    }));

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
