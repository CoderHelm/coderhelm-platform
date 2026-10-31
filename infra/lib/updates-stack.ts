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
import * as ssm from "aws-cdk-lib/aws-ssm";
import * as crypto from "crypto";
import { Construct } from "constructs";
import { ResolvedConfig } from "./config";
import { repoPath } from "./paths";
import { hybridUrlEnvironment } from "./service-env";
import { releaseStamp, stampEnvironment } from "./release";
import { WINDOW_MINUTES, WINDOW_POLICY_NAME, windowPolicyParts } from "./access";
import { CHECK_VERSION, CURL, installCosignCommands, RELEASE_BASE, RELEASE_REPO, VERIFY_SUMS, verifyTagCommands } from "./release-verify";

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
          // The tag: on main ($COMMIT), signed by a pinned maintainer key or attested by auto-release.yml on main.
          ...verifyTagCommands(),
          VERIFY_SUMS,
          // A native release's parent template: its sha256 as the signed
          // SHA256SUMS lists it (the updater checks the downloaded template
          // against it before it proposes the stack update).
          "TEMPLATE_SHA256=$(awk '$2==\"coderhelm.json\"{print $1}' SHA256SUMS); export TEMPLATE_SHA256",
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
          'node -e \'const r=require("/tmp/result.json");process.stdout.write(JSON.stringify({action:"models_verified",version:process.env.VERSION,status:r.status,reason:r.reason,manifest:r.manifest??null,template_sha256:r.status==="verified"&&/^[0-9a-f]{64}$/.test(process.env.TEMPLATE_SHA256||"")?process.env.TEMPLATE_SHA256:null}))\' > /tmp/payload.json',
          `aws lambda invoke --function-name ${cfg.prefix}-updater --cli-binary-format raw-in-base64-out --payload file:///tmp/payload.json /tmp/out.json`,
        ],
      },
    },
  });
}

/**
 * A hardened install's update build asks the updater for its deploy window
 * (lambda/updater/handler.py, open_window): the updater checks this is the
 * build it started, with this buildspec, then lets this build's own session
 * deploy until it ends. The build waits until IAM shows the window.
 */
export function openWindowCommands(opener: string, assetsBucket: string): string[] {
  return [
    'WINDOW_USER_ID=$(aws sts get-caller-identity --query UserId --output text)',
    `printf '{"action":"open_window","build_id":"%s","user_id":"%s","history_id":"%s"}' "$CODEBUILD_BUILD_ID" "$WINDOW_USER_ID" "\${HISTORY_ID:-}" > /tmp/window.json`,
    `aws lambda invoke --function-name ${opener} --cli-binary-format raw-in-base64-out --payload file:///tmp/window.json /tmp/window-out.json > /dev/null`,
    `node -e 'const r=require("/tmp/window-out.json");if(!r.ok){console.error("The deploy window was refused: "+(r.error||r.errorMessage||JSON.stringify(r)));process.exit(1)}console.log("Deploy window open until "+r.until)'`,
    `for i in $(seq 1 30); do aws s3api head-bucket --bucket ${assetsBucket} > /dev/null 2>&1 && break; [ "$i" = 30 ] && { echo "The deploy window did not take effect in 2 minutes"; exit 1; }; sleep 4; done`,
  ];
}

export function closeWindowCommand(role: string): string {
  return `aws iam delete-role-policy --role-name ${role} --policy-name ${WINDOW_POLICY_NAME} > /dev/null 2>&1 || true`;
}

/** The buildspec as JSON with sorted keys: what the updater hashes (canonical_json in handler.py). */
export function canonicalJson(v: unknown): string {
  if (Array.isArray(v)) return `[${v.map(canonicalJson).join(",")}]`;
  if (v && typeof v === "object") {
    return `{${Object.keys(v as object).sort().map((k) => `${JSON.stringify(k)}:${canonicalJson((v as Record<string, unknown>)[k])}`).join(",")}}`;
  }
  return JSON.stringify(v);
}

export function buildSpecSha256(spec: object): string {
  return crypto.createHash("sha256").update(canonicalJson(spec), "utf8").digest("hex");
}

/** The variables the updater may set on an update build (start_build, domain_build_env). */
export const UPDATE_BUILD_VARIABLES = ["VERSION", "ACTOR", "MODE", "HISTORY_ID", "DOMAIN_ACTION", "DOMAIN_HOSTNAME", "DOMAIN_CERT", "DOMAIN_ZONE", "DOMAIN_ZONE_ID"];

export function updateBuildSpec(cfg: ResolvedConfig): codebuild.BuildSpec {
  return codebuild.BuildSpec.fromObject(updateBuildSpecObject(cfg));
}

/** The update build's script as a plain object (what CodeBuild stores, and what the updater checks). */
export function updateBuildSpecObject(cfg: ResolvedConfig): Record<string, unknown> {
  return {
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
          // The tag: on main ($COMMIT), signed by a pinned maintainer key or attested by auto-release.yml on main.
          ...verifyTagCommands(),
          // Signed by exactly this repository's release.yml, on this tag and commit, in Rekor.
          VERIFY_SUMS,
          `${CURL} -o coderhelm-selfhost.tgz "$B/coderhelm-selfhost.tgz"`,
          "grep ' coderhelm-selfhost.tgz$' SHA256SUMS | sha256sum -c -",
          // The installer then checks every other file of the release the same way,
          // and refuses an older release unless it is a rollback to one this install ran.
          "npm install -g --ignore-scripts ./coderhelm-selfhost.tgz",
          // A hardened install: deploy rights only now, for this build.
          ...(cfg.access ? openWindowCommands(`${cfg.prefix}-updater`, cfg.access.assetsBucket) : []),
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
          // The window closes with the build (the updater closes it too).
          ...(cfg.access ? [closeWindowCommand(`${cfg.prefix}-update-build`)] : []),
          // Tell the updater how it went (also when the build failed early).
          'if [ ! -s /tmp/result.json ]; then echo "{\\"status\\":\\"failed\\",\\"reason\\":\\"the update build stopped before deploying: see its log\\"}" > /tmp/result.json; fi',
          'node -e \'const r=require("/tmp/result.json");process.stdout.write(JSON.stringify({action:"finished",history_id:process.env.HISTORY_ID,status:r.status,reason:r.reason,version:process.env.VERSION}))\' > /tmp/payload.json',
          `aws lambda invoke --function-name ${cfg.prefix}-updater --cli-binary-format raw-in-base64-out --payload file:///tmp/payload.json /tmp/out.json`,
        ],
      },
    },
  };
}

export class UpdatesStack extends cdk.Stack {
  public readonly updaterFunction: lambda.Function;
  /** The update build (absent in a native release: updates are stack updates). */
  public readonly project?: codebuild.Project;
  public readonly checkProject: codebuild.Project;

  constructor(scope: Construct, id: string, props: UpdatesStackProps) {
    super(scope, id, props);
    const cfg = props.cfg;
    const native = !!cfg.native;
    const prefix = cfg.prefix;
    const stamp = releaseStamp(cfg);

    // --- The update build (not in a native release: updates are stack updates) ---
    let buildRole: iam.Role | undefined;
    if (!native) {
    buildRole = new iam.Role(this, "UpdateBuildRole", {
      roleName: `${prefix}-update-build`,
      description: "CoderHelm update build: deploys a release through the CDK bootstrap roles, checks health",
      assumedBy: new iam.ServicePrincipal("codebuild.amazonaws.com"),
    });
    if (cfg.access) {
      // Hardened: between runs this role can only ask for its window and
      // report; the window (written by the updater, bound to one build)
      // holds everything else (infra/lib/access.ts, windowPolicy).
      buildRole.addToPolicy(new iam.PolicyStatement({
        sid: "AskForWindowAndReport",
        actions: ["lambda:InvokeFunction"],
        resources: [`arn:aws:lambda:${this.region}:${this.account}:function:${prefix}-updater`],
      }));
      buildRole.addToPolicy(new iam.PolicyStatement({
        sid: "CloseOwnWindow",
        actions: ["iam:DeleteRolePolicy"],
        resources: [`arn:aws:iam::${this.account}:role/${prefix}-update-build`],
      }));
    } else {
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
      // The installer's model check before it deploys (installer/lib/commands.mjs,
      // updateModelCheck): the worker makes the test calls; the build only asks,
      // and reads which team's key passed the last switch to Anthropic keys.
      buildRole.addToPolicy(new iam.PolicyStatement({
        sid: "InstallerModelCheck",
        actions: ["lambda:InvokeFunction"],
        resources: [`arn:aws:lambda:${this.region}:${this.account}:function:${prefix}-worker`],
      }));
      buildRole.addToPolicy(new iam.PolicyStatement({
        sid: "ModelCheckSetting",
        actions: ["dynamodb:GetItem"],
        resources: [props.settingsTable.tableArn],
        conditions: { "ForAllValues:StringEquals": { "dynamodb:LeadingKeys": ["SYSTEM"] } },
      }));
      buildRole.addToPolicy(new iam.PolicyStatement({
        sid: "ReportResult",
        actions: ["lambda:InvokeFunction"],
        resources: [`arn:aws:lambda:${this.region}:${this.account}:function:${prefix}-updater`],
      }));
    }

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
    }

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
        ...(this.project ? { UPDATE_PROJECT_NAME: this.project.projectName } : {}),
        // A native release: the launch parameters' JSON (the parent builds it).
        UPDATE_DEFAULTS: native ? cdk.Fn.ref("UpdateDefaultsJson") : JSON.stringify(cfg.updates.defaults),
        ADMIN_EMAILS: cfg.admins.join(","),
        SES_FROM_ADDRESS: cfg.email.from,
        // The sender name and subject prefix, as the services use them.
        EMAIL_FROM_NAME: cfg.email.fromName,
        ORGANIZATION_NAME: cfg.email.organization,
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
        // How update notices are emailed: Settings, Emails, else this.
        EMAIL_TRANSPORT_DEFAULT: native ? cdk.Fn.ref("EmailTransport") : cfg.email.transport,
        // The install's hybrid registration, read only to email through
        // CoderHelm's mail relay when an admin chose it.
        SECRETS_NAME: cfg.secretName,
        // Where the hosted side is: "off" when the config turned hybrid
        // off, a non-default URL, or nothing (the default).
        ...hybridUrlEnvironment(cfg),
        ...(cfg.updates.webhookUrl ? { UPDATE_WEBHOOK_URL: cfg.updates.webhookUrl } : {}),
        ...(cfg.access && !native
          ? {
              // The deploy window this updater opens (open_window): the role,
              // the buildspec it must see, and where the policy is.
              DEPLOY_WINDOW: JSON.stringify({
                role: `${prefix}-update-build`,
                buildspec: buildSpecSha256(updateBuildSpecObject(cfg)),
                image: codebuild.LinuxBuildImage.STANDARD_7_0.imageId,
                variables: UPDATE_BUILD_VARIABLES,
                param: `/${prefix}/access/window-policy`,
                parts: windowPolicyParts(cfg.access).length,
                minutes: WINDOW_MINUTES,
              }),
            }
          : {}),
        ...stampEnvironment(stamp),
      },
    });
    props.settingsTable.grantReadWriteData(this.updaterFunction);
    this.updaterFunction.addToRolePolicy(new iam.PolicyStatement({
      actions: ["codebuild:StartBuild"],
      resources: [...(this.project ? [this.project.projectArn] : []), this.checkProject.projectArn],
    }));
    this.updaterFunction.addToRolePolicy(new iam.PolicyStatement({
      sid: "ReadHybridRegistrationForMail",
      actions: ["secretsmanager:GetSecretValue"],
      resources: [`arn:aws:secretsmanager:${this.region}:${this.account}:secret:${cfg.secretName}-??????`],
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

    if (cfg.access && !native && buildRole && this.project) {
      // The window's statements, in parts that fit standard parameters.
      const parts = windowPolicyParts(cfg.access).map((value, i) => new ssm.StringParameter(this, `WindowPolicy${i + 1}`, {
        parameterName: `/${prefix}/access/window-policy/${i + 1}`,
        description: "CoderHelm: the deploy window an update build gets while it runs (infra/lib/access.ts)",
        stringValue: value,
      }));
      this.updaterFunction.addToRolePolicy(new iam.PolicyStatement({
        sid: "OpenAndCloseDeployWindow",
        actions: ["iam:PutRolePolicy", "iam:DeleteRolePolicy"],
        resources: [buildRole!.roleArn],
        conditions: { StringEquals: { "iam:PermissionsBoundary": cfg.access.boundaryArn } },
      }));
      this.updaterFunction.addToRolePolicy(new iam.PolicyStatement({
        sid: "ReadBuildRole",
        actions: ["iam:GetRole", "iam:GetRolePolicy"],
        resources: [buildRole!.roleArn],
      }));
      this.updaterFunction.addToRolePolicy(new iam.PolicyStatement({
        sid: "CheckTheBuild",
        actions: ["codebuild:BatchGetBuilds"],
        resources: [this.project!.projectArn],
      }));
      for (const part of parts) part.grantRead(this.updaterFunction);
    }

    if (native) nativeUpdates(this, cfg, this.updaterFunction);

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
    if (this.project) new cdk.CfnOutput(this, "UpdateProjectName", { value: this.project.projectName });
  }
}

/** Where native releases publish their parent template (write-once, by version). */
export const NATIVE_TEMPLATE_BASE = "https://coderhelm-public.s3.amazonaws.com/launch";
/** The parent template's file name, in SHA256SUMS and under launch/<version>/. */
export const NATIVE_TEMPLATE_FILE = "coderhelm.json";

/**
 * A native release's updates (docs DESIGN, "3. Update"): the updater proposes
 * a stack update to a verified release as a change set on the parent stack
 * (`coderhelm-<stage>`) and, only with auto-update on, executes it through the
 * parent's DeployRole. It may pass that role only to CloudFormation, and
 * CloudFormation accepts the change set only for this stack, with that role,
 * from a published release template.
 */
function nativeUpdates(scope: Construct, cfg: ResolvedConfig, fn: lambda.Function): void {
  const stack = cdk.Stack.of(scope);
  // The parent stack, by its own name (the parent passes AWS::StackName).
  const rootName = cdk.Fn.ref("RootStackName");
  const root = `arn:${stack.partition}:cloudformation:${stack.region}:${stack.account}:stack/${rootName}/*`;
  const parts = `arn:${stack.partition}:cloudformation:${stack.region}:${stack.account}:stack/${rootName}-*/*`;
  const deployRole = cfg.access!.deployRoleArn.replace(/^arn:aws:/, `arn:${stack.partition}:`);
  fn.addEnvironment("NATIVE_STACK", rootName);
  fn.addEnvironment("NATIVE_TEMPLATE_BASE", NATIVE_TEMPLATE_BASE);
  fn.addEnvironment("NATIVE_TEMPLATE_FILE", NATIVE_TEMPLATE_FILE);
  // "" while auto-update is off (the role exists only with AutoUpdate=on).
  fn.addEnvironment("DEPLOY_ROLE_ARN", cdk.Fn.ref("DeployRoleArn"));
  // The parent's helper stages the release's child templates before a
  // change set (lambda/native-helper, "stage").
  const helper = `${cfg.prefix}-install-helper`;
  fn.addEnvironment("NATIVE_HELPER_FUNCTION", helper);
  fn.addToRolePolicy(new iam.PolicyStatement({
    sid: "StageReleaseFiles",
    actions: ["lambda:InvokeFunction"],
    resources: [`arn:${stack.partition}:lambda:${stack.region}:${stack.account}:function:${helper}`],
  }));
  // CloudFormation reads the parts' templates with the caller's access while
  // it builds the nested change sets (only the staged releases, read-only).
  fn.addToRolePolicy(new iam.PolicyStatement({
    sid: "ReadStagedTemplates",
    actions: ["s3:GetObject"],
    resources: [`arn:${stack.partition}:s3:::${cfg.prefix}-release-${stack.account}/releases/*`],
  }));
  fn.addToRolePolicy(new iam.PolicyStatement({
    sid: "ProposeReleaseUpdate",
    actions: ["cloudformation:CreateChangeSet"],
    resources: [root],
    conditions: {
      StringLike: { "cloudformation:TemplateUrl": [`${NATIVE_TEMPLATE_BASE}/v*.*.*/${NATIVE_TEMPLATE_FILE}`] },
      // With auto-update on, only through the stack's own deploy role.
      StringEqualsIfExists: { "cloudformation:RoleArn": deployRole },
    },
  }));
  fn.addToRolePolicy(new iam.PolicyStatement({
    sid: "ReviewAndApplyReleaseUpdate",
    actions: [
      "cloudformation:DescribeChangeSet", "cloudformation:ListChangeSets", "cloudformation:ExecuteChangeSet",
      "cloudformation:DeleteChangeSet", "cloudformation:DescribeStacks", "cloudformation:DescribeStackEvents",
      // What CloudFormation actually fetched, compared with the verified template before anything runs.
      "cloudformation:GetTemplate",
    ],
    // The parts' nested change sets too (each data change is checked before auto-update applies it).
    resources: [root, parts],
  }));
  fn.addToRolePolicy(new iam.PolicyStatement({
    sid: "AutoUpdateThroughDeployRole",
    actions: ["iam:PassRole"],
    resources: [deployRole],
    conditions: { StringEquals: { "iam:PassedToService": "cloudformation.amazonaws.com" } },
  }));
  new events.Rule(scope, "NativeUpdatePoll", {
    ruleName: `${cfg.prefix}-update-poll`,
    description: "Every 5 minutes: follow a proposed or running stack update",
    schedule: events.Schedule.rate(cdk.Duration.minutes(5)),
    targets: [new targets.LambdaFunction(fn, { event: events.RuleTargetInput.fromObject({ action: "native_poll" }) })],
  });
}
