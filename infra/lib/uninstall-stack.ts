/**
 * The uninstall stack (infra/cfn/coderhelm-selfhost-uninstall.yaml), written
 * whole from code so it can never drift from what the tests check:
 *
 *   npm run build && node dist/lib/uninstall-stack.js > ../infra/cfn/coderhelm-selfhost-uninstall.yaml
 *
 * (scripts/uninstall-stack.sh). infra/test/uninstall-stack.test.ts checks
 * the file is exactly this output.
 *
 * How it runs (docs/self-host/uninstall.md):
 *
 * 1. Someone who may create IAM roles launches it from the uninstall guide,
 *    typing the install's name twice (the stack's Rules refuse a mismatch).
 * 2. The starter starts the uninstall build. The build checks the release
 *    exactly as the installer does, then asks the window function for its
 *    window: the function checks it is this stack's build with this
 *    template's buildspec, finds the install's pieces in this region and
 *    writes the window (infra/lib/uninstall-access.ts), bound to the
 *    build's session and an end time.
 * 3. The build runs `coderhelm-selfhost uninstall --guided`, which keeps a
 *    progress file the guide reads, and ends with a read-only check of what
 *    is left.
 * 4. The build answers the stack's wait condition with a failure, always:
 *    CloudFormation rolls the stack back with the credentials of the person
 *    who launched it, which removes the uninstaller itself (an IAM role
 *    can't delete itself). On a finished uninstall, the starter removes the
 *    build's log group and the progress bucket last; after a failure they
 *    stay so the reason is readable.
 */
import * as crypto from "crypto";
import * as fs from "fs";
import { repoPath } from "./paths";
import { CHECK_VERSION, CURL, installCosignCommands, RELEASE_BASE, RELEASE_REPO, VERIFY_SUMS, verifyTagCommands } from "./release-verify";
import { LAUNCH_ID, sub } from "./launch-access";
import { UNINSTALL_WINDOW_POLICY_NAME, uninstallBoundaryPolicy, uninstallNames, uninstallWindowParts, UninstallNames } from "./uninstall-access";

export const UNINSTALL_IMAGE = "aws/codebuild/standard:7.0";
export const UNINSTALL_VARIABLES = ["VERSION", "STAGE", "MODE", "STATUS_BUCKET", "INSTALL_STATUS_ID", "UNINSTALL_ID", "BUILD_ROLE", "WINDOW_FUNCTION", "WAIT_URL"];
/** Each window part fits a standard parameter (4 KB) with the longest names substituted. */
export const UNINSTALL_WINDOW_PART_MAX = 3000;

/** The names, as CloudFormation substitutions (the stack's own short id is ${Id}). */
export function stackNames(): UninstallNames {
  return uninstallNames({ partition: "${AWS::Partition}", account: "${AWS::AccountId}", region: "${AWS::Region}", stage: "${Stage}", uid: "${Id}", statusId: "${StatusId}" });
}

const norm = (x: string) => x.trim().split(/\r?\n/).map((l) => l.replace(/\s+$/, "")).join("\n");

/** The final answer the build gives the stack: always FAILURE, so the stack rolls itself back. */
const WAIT_ANSWER =
  `node -e 'let f={};try{f=require("/tmp/coderhelm-final.json")}catch{}const done=process.env.CODEBUILD_BUILD_SUCCEEDING==="1"&&f.phase==="done";` +
  `const why=(done?"Done: CoderHelm is uninstalled. This stack now rolls itself back, which removes the uninstaller too (an AWS role cannot delete itself).":` +
  `("The uninstall stopped: "+(f.message||"see the log")+(f.fix?" "+f.fix:"")+" Nothing outside this install was touched. Launch the uninstaller again: it picks up where it stopped. Log: CloudWatch /aws/codebuild/"+String(process.env.CODEBUILD_BUILD_ID||"").split(":")[0])).slice(0,4000);` +
  `process.stdout.write(JSON.stringify({Status:"FAILURE",Reason:why,UniqueId:"uninstall",Data:""}))' > /tmp/wait.json`;

export function buildspec(): string {
  // Every command a literal block: nothing in it is read as YAML.
  const ind = (lines: string[], n: number) => lines.map((l) => `${" ".repeat(n)}- |\n${l.split("\n").map((x) => " ".repeat(n + 2) + x).join("\n")}`).join("\n");
  const failNoWindow =
    `node -e 'const r=require("/tmp/window-out.json");if(r.ok){console.log("Uninstall window open until "+r.until);process.exit(0)}` +
    `const m="AWS refused the uninstaller its window: "+(r.error||r.errorMessage||"no answer");console.error(m);` +
    `require("fs").writeFileSync("/tmp/coderhelm-final.json",JSON.stringify({schema:1,kind:"uninstall",phase:"failed",message:m,fix:"Nothing was deleted. Launch the uninstaller again; if it happens again, tell us with this message.",stage:process.env.STAGE,region:process.env.AWS_REGION,updatedAt:new Date().toISOString()}));process.exit(1)'`;
  const build = [
    "set -euo pipefail",
    `REPO=${RELEASE_REPO}`,
    `if [ "$VERSION" = "latest" ]; then\n  VERSION=$(${CURL} "https://api.github.com/repos/$REPO/releases/latest" | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{const r=JSON.parse(s);if(r.draft||r.prerelease)process.exit(1);console.log(r.tag_name)})')\n  export VERSION\nfi`,
    CHECK_VERSION,
    `echo "Uninstalling with CoderHelm $VERSION"`,
    "mkdir -p /tmp/ch && cd /tmp/ch",
    RELEASE_BASE,
    `for f in SHA256SUMS SHA256SUMS.sigstore.json coderhelm-selfhost.tgz; do ${CURL} -o "$f" "$B/$f"; done`,
    ...verifyTagCommands(),
    VERIFY_SUMS,
    "grep ' coderhelm-selfhost.tgz$' SHA256SUMS | sha256sum -c -",
    "npm install -g --ignore-scripts --no-audit --no-fund ./coderhelm-selfhost.tgz > /dev/null",
    // The window: the window function checks this is this template's build, then lets its session delete what is this install's.
    "WINDOW_USER_ID=$(aws sts get-caller-identity --query UserId --output text)",
    `printf '{"build_id":"%s","user_id":"%s"}' "$CODEBUILD_BUILD_ID" "$WINDOW_USER_ID" > /tmp/window.json`,
    `aws lambda invoke --function-name "$WINDOW_FUNCTION" --cli-binary-format raw-in-base64-out --payload file:///tmp/window.json /tmp/window-out.json > /dev/null`,
    failNoWindow,
    `for i in $(seq 1 30); do aws resourcegroupstaggingapi get-tag-values --key coderhelm:install > /dev/null 2>&1 && break; [ "$i" = 30 ] && { echo "The window did not take effect in 2 minutes"; exit 1; }; sleep 4; done`,
    "coderhelm-selfhost uninstall --guided",
  ];
  const post = [
    // The window ends with the build.
    `aws iam delete-role-policy --role-name "$BUILD_ROLE" --policy-name ${UNINSTALL_WINDOW_POLICY_NAME} > /dev/null 2>&1 || true`,
    `if [ ! -s /tmp/coderhelm-final.json ] && [ -n "$STATUS_BUCKET" ]; then\n  printf '{"schema":1,"kind":"uninstall","phase":"failed","message":"The uninstaller stopped before it could start: the release could not be downloaded or its signature did not check out.","fix":"Nothing was deleted. Launch the uninstaller again.","stage":"%s","region":"%s","updatedAt":"%s"}' "$STAGE" "$AWS_REGION" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > /tmp/coderhelm-final.json\n  aws s3 cp /tmp/coderhelm-final.json "s3://$STATUS_BUCKET/status.json" --content-type application/json --cache-control no-store --only-show-errors || true\nfi`,
    `if [ -n "$WAIT_URL" ]; then\n  ${WAIT_ANSWER}\n  ${CURL.replace("-fsSL", "-fsS")} -X PUT -H 'Content-Type:' --data-binary @/tmp/wait.json "$WAIT_URL"\nfi`,
  ];
  return [
    "version: 0.2",
    "env:",
    "  shell: bash",
    "phases:",
    "  install:",
    "    runtime-versions:",
    "      nodejs: 20",
    "    commands:",
    ind(["set -euo pipefail", ...installCosignCommands()], 6),
    "  build:",
    "    commands:",
    ind(build, 6),
    "  post_build:",
    "    commands:",
    ind(post, 6),
    "",
  ].join("\n");
}

export function buildspecSha256(spec = buildspec()): string {
  return crypto.createHash("sha256").update(norm(spec)).digest("hex");
}

const py = (name: string) => fs.readFileSync(repoPath("infra", "cfn", "uninstall", name), "utf8");
const lambdaTrust = { Version: "2012-10-17", Statement: [{ Effect: "Allow", Principal: { Service: "lambda.amazonaws.com" }, Action: "sts:AssumeRole" }] };
const logArn = (group: string) => `arn:\${AWS::Partition}:logs:\${AWS::Region}:\${AWS::AccountId}:log-group:${group}`;

export function uninstallTemplate(): Record<string, unknown> {
  const n = stackNames();
  const parts = uninstallWindowParts(n, UNINSTALL_WINDOW_PART_MAX);
  const P = "arn:${AWS::Partition}";
  const fnArn = (f: string) => `${P}:lambda:\${AWS::Region}:\${AWS::AccountId}:function:${f}`;
  const roleArn = (r: string) => `${P}:iam::\${AWS::AccountId}:role/${r}`;
  const projectArn = `${P}:codebuild:\${AWS::Region}:\${AWS::AccountId}:project/${n.project}`;
  const statusBucket = `${P}:s3:::coderhelm-setup-\${StatusId}`;
  const ownLogs = (g: string) => ({ Sid: "OwnLog", Effect: "Allow", Action: ["logs:CreateLogStream", "logs:PutLogEvents"], Resource: [logArn(g), `${logArn(g)}:*`] });
  const resources: Record<string, unknown> = {
    StatusBucket: {
      Type: "AWS::S3::Bucket",
      Condition: "HasStatus",
      // The starter removes it after a finished uninstall; after a failure it keeps the reason readable.
      DeletionPolicy: "Retain",
      UpdateReplacePolicy: "Retain",
      Properties: {
        BucketName: sub("coderhelm-setup-${StatusId}"),
        OwnershipControls: { Rules: [{ ObjectOwnership: "BucketOwnerEnforced" }] },
        PublicAccessBlockConfiguration: { BlockPublicAcls: true, IgnorePublicAcls: true, BlockPublicPolicy: false, RestrictPublicBuckets: false },
        CorsConfiguration: { CorsRules: [{ AllowedMethods: ["GET", "HEAD"], AllowedOrigins: ["*"], MaxAge: 300 }] },
        LifecycleConfiguration: { Rules: [{ Id: "expire-progress", Status: "Enabled", ExpirationInDays: 14 }] },
      },
    },
    Boundary: {
      Type: "AWS::IAM::ManagedPolicy",
      Properties: {
        ManagedPolicyName: sub(n.boundaryName),
        Description: "CoderHelm uninstall: the most the uninstaller's roles can do (docs/self-host/uninstall.md)",
        PolicyDocument: sub(uninstallBoundaryPolicy(n)),
      },
    },
    BuildLogs: {
      Type: "AWS::Logs::LogGroup",
      // Kept after a failed uninstall, removed by the starter after a finished one.
      DeletionPolicy: "Retain",
      UpdateReplacePolicy: "Retain",
      Properties: { LogGroupName: sub(n.buildLogGroup), RetentionInDays: 30 },
    },
    WindowLogs: { Type: "AWS::Logs::LogGroup", DeletionPolicy: "Delete", Properties: { LogGroupName: sub(n.windowLogGroup), RetentionInDays: 7 } },
    StarterLogs: { Type: "AWS::Logs::LogGroup", DeletionPolicy: "Delete", Properties: { LogGroupName: sub(n.starterLogGroup), RetentionInDays: 7 } },
    // Idle, the build's role can write its log and progress, ask for its window and close it. Nothing else.
    BuildRole: {
      Type: "AWS::IAM::Role",
      Properties: {
        RoleName: sub(n.buildRole),
        Description: "CoderHelm uninstall build. Deletes only while its window is open (docs/self-host/uninstall.md).",
        PermissionsBoundary: { Ref: "Boundary" },
        AssumeRolePolicyDocument: { Version: "2012-10-17", Statement: [{ Effect: "Allow", Principal: { Service: "codebuild.amazonaws.com" }, Action: "sts:AssumeRole" }] },
        Policies: [{
          PolicyName: "idle",
          PolicyDocument: sub({
            Version: "2012-10-17",
            Statement: [
              ownLogs(n.buildLogGroup),
              { Sid: "Progress", Effect: "Allow", Action: "s3:PutObject", Resource: `${statusBucket}/status.json` },
              { Sid: "AskForWindow", Effect: "Allow", Action: "lambda:InvokeFunction", Resource: fnArn(n.windowFunction) },
              { Sid: "CloseOwnWindow", Effect: "Allow", Action: "iam:DeleteRolePolicy", Resource: roleArn(n.buildRole) },
            ],
          }),
        }],
      },
    },
    Project: {
      Type: "AWS::CodeBuild::Project",
      Properties: {
        Name: sub(n.project),
        Description: "Removes CoderHelm from this account with a signed release (coderhelm-selfhost uninstall --guided).",
        ServiceRole: { "Fn::GetAtt": ["BuildRole", "Arn"] },
        TimeoutInMinutes: 90,
        ConcurrentBuildLimit: 1,
        Artifacts: { Type: "NO_ARTIFACTS" },
        LogsConfig: { CloudWatchLogs: { Status: "ENABLED", GroupName: { Ref: "BuildLogs" } } },
        Environment: {
          Type: "LINUX_CONTAINER",
          ComputeType: "BUILD_GENERAL1_SMALL",
          Image: UNINSTALL_IMAGE,
          EnvironmentVariables: [
            { Name: "VERSION", Value: { Ref: "Version" } },
            { Name: "STAGE", Value: { Ref: "Stage" } },
            { Name: "MODE", Value: { Ref: "Mode" } },
            { Name: "STATUS_BUCKET", Value: { "Fn::If": ["HasStatus", { Ref: "StatusBucket" }, ""] } },
            { Name: "INSTALL_STATUS_ID", Value: { Ref: "InstallStatusId" } },
            { Name: "UNINSTALL_ID", Value: LAUNCH_ID },
            { Name: "BUILD_ROLE", Value: { Ref: "BuildRole" } },
            { Name: "WINDOW_FUNCTION", Value: sub(n.windowFunction) },
            { Name: "WAIT_URL", Value: "" },
          ],
        },
        Source: { Type: "NO_SOURCE", BuildSpec: buildspec() },
      },
    },
    WindowRole: {
      Type: "AWS::IAM::Role",
      Properties: {
        RoleName: sub(n.windowRole),
        PermissionsBoundary: { Ref: "Boundary" },
        AssumeRolePolicyDocument: lambdaTrust,
        Policies: [{
          PolicyName: "OpenWindow",
          PolicyDocument: sub({
            Version: "2012-10-17",
            Statement: [
              ownLogs(n.windowLogGroup),
              { Sid: "CheckTheBuild", Effect: "Allow", Action: "codebuild:BatchGetBuilds", Resource: projectArn },
              { Sid: "WriteTheWindow", Effect: "Allow", Action: ["iam:GetRole", "iam:PutRolePolicy"], Resource: roleArn(n.buildRole) },
              { Sid: "WindowStatements", Effect: "Allow", Action: "ssm:GetParameter", Resource: `${P}:ssm:\${AWS::Region}:\${AWS::AccountId}:parameter${n.paramPath}/*` },
              // Read-only: what it finds decides only which of this install's names go in the window.
              { Sid: "FindTheInstall", Effect: "Allow", Action: ["cloudformation:ListStacks", "cloudformation:DescribeStacks", "s3:ListAllMyBuckets", "tag:GetTagValues", "secretsmanager:ListSecrets"], Resource: "*" },
              { Sid: "AssetsBucketRegion", Effect: "Allow", Action: "s3:GetBucketLocation", Resource: `${P}:s3:::coderhelm-\${Stage}-assets-*` },
              { Sid: "InstallProgress", Effect: "Allow", Action: "s3:GetObject", Resource: `${P}:s3:::coderhelm-setup-\${InstallStatusId}/status.json` },
            ],
          }),
        }],
      },
    },
    WindowFunction: {
      Type: "AWS::Lambda::Function",
      Properties: {
        FunctionName: sub(n.windowFunction),
        Description: "Gives a verified CoderHelm uninstall build its window (docs/self-host/uninstall.md).",
        Runtime: "python3.12",
        Handler: "index.handler",
        Timeout: 60,
        Role: { "Fn::GetAtt": ["WindowRole", "Arn"] },
        LoggingConfig: { LogGroup: { Ref: "WindowLogs" } },
        Environment: {
          Variables: {
            PROJECT: sub(n.project),
            ROLE: sub(n.buildRole),
            IMAGE: UNINSTALL_IMAGE,
            PARAM: sub(n.paramPath),
            PARTS: String(parts.length),
            // sha256 of the build's BuildSpec, each line's trailing spaces and the ends trimmed.
            BUILDSPEC_SHA256: buildspecSha256(),
            VARIABLES: UNINSTALL_VARIABLES.join(","),
            STAGE: { Ref: "Stage" },
            MODE: { Ref: "Mode" },
            ACCOUNT: { Ref: "AWS::AccountId" },
            REGION: { Ref: "AWS::Region" },
            OWN_STATUS: { Ref: "StatusId" },
            INSTALL_STATUS: { Ref: "InstallStatusId" },
          },
        },
        Code: { ZipFile: py("window.py") },
      },
    },
    StarterRole: {
      Type: "AWS::IAM::Role",
      Properties: {
        RoleName: sub(n.starterRole),
        PermissionsBoundary: { Ref: "Boundary" },
        AssumeRolePolicyDocument: lambdaTrust,
        Policies: [{
          PolicyName: "StartAndCleanUp",
          PolicyDocument: sub({
            Version: "2012-10-17",
            Statement: [
              ownLogs(n.starterLogGroup),
              { Sid: "StartTheBuild", Effect: "Allow", Action: ["codebuild:StartBuild", "codebuild:ListBuildsForProject", "codebuild:BatchGetBuilds"], Resource: projectArn },
              { Sid: "CloseAWindowLeftOpen", Effect: "Allow", Action: "iam:DeleteRolePolicy", Resource: roleArn(n.buildRole) },
              {
                Sid: "ProgressFile",
                Effect: "Allow",
                Action: ["s3:PutBucketPolicy", "s3:PutObject", "s3:GetObject", "s3:ListBucket", "s3:ListBucketVersions", "s3:DeleteObject", "s3:DeleteObjectVersion", "s3:DeleteBucket"],
                Resource: [statusBucket, `${statusBucket}/*`],
              },
              { Sid: "BuildLogLast", Effect: "Allow", Action: "logs:DeleteLogGroup", Resource: [logArn(n.buildLogGroup), `${logArn(n.buildLogGroup)}:*`] },
            ],
          }),
        }],
      },
    },
    StarterFunction: {
      Type: "AWS::Lambda::Function",
      Properties: {
        FunctionName: sub(n.starterFunction),
        Description: "Starts the CoderHelm uninstall build once; removes its log and progress file after a finished uninstall.",
        Runtime: "python3.12",
        Handler: "index.handler",
        Timeout: 600,
        Role: { "Fn::GetAtt": ["StarterRole", "Arn"] },
        LoggingConfig: { LogGroup: { Ref: "StarterLogs" } },
        Code: { ZipFile: py("starter.py") },
      },
    },
    DoneHandle: { Type: "AWS::CloudFormation::WaitConditionHandle" },
  };
  parts.forEach((value, i) => {
    resources[`WindowPolicy${i + 1}`] = {
      Type: "AWS::SSM::Parameter",
      Properties: {
        Name: sub(`${n.paramPath}/${i + 1}`),
        Description: "CoderHelm: the uninstall window's statements (infra/lib/uninstall-access.ts)",
        Type: "String",
        Value: sub(value),
      },
    };
  });
  resources.StartUninstall = {
    Type: "Custom::StartUninstaller",
    DependsOn: ["WindowFunction", ...parts.map((_, i) => `WindowPolicy${i + 1}`)],
    Properties: {
      ServiceToken: { "Fn::GetAtt": ["StarterFunction", "Arn"] },
      Project: { Ref: "Project" },
      BuildRole: { Ref: "BuildRole" },
      WaitUrl: { Ref: "DoneHandle" },
      Bucket: { "Fn::If": ["HasStatus", { Ref: "StatusBucket" }, ""] },
      LogGroup: { Ref: "BuildLogs" },
      Mode: { Ref: "Mode" },
      Stage: { Ref: "Stage" },
      Region: { Ref: "AWS::Region" },
      Partition: { Ref: "AWS::Partition" },
    },
  };
  resources.Done = {
    Type: "AWS::CloudFormation::WaitCondition",
    DependsOn: "StartUninstall",
    Properties: { Handle: { Ref: "DoneHandle" }, Timeout: "5400", Count: 1 },
  };

  const NAME = "^[a-z][a-z0-9-]{0,19}$";
  return {
    AWSTemplateFormatVersion: "2010-09-09",
    Description:
      "Removes one CoderHelm install from this AWS account (10 to 40 minutes). It creates an uninstall build (CodeBuild) whose role can't delete " +
      "anything until a check gives this one verified build a window that ends with it, limited to resources named or tagged for the install you " +
      "name (coderhelm-<name>-*) and denied for every other install. When it ends, the stack rolls itself back, which removes the uninstaller " +
      "with your own credentials. docs.coderhelm.com/self-host/uninstall",
    Metadata: {
      "cfn-lint": { config: { ignore_checks: ["E6101"] } },
      "AWS::CloudFormation::Interface": {
        ParameterGroups: [
          { Label: { default: "Which install" }, Parameters: ["Stage", "ConfirmStage"] },
          { Label: { default: "What to remove" }, Parameters: ["Mode"] },
          { Label: { default: "Filled in by the uninstall guide (leave as they are)" }, Parameters: ["Version", "StatusId", "InstallStatusId"] },
        ],
        ParameterLabels: {
          Stage: { default: "Install name" },
          ConfirmStage: { default: "Type the install name again" },
          Mode: { default: "Remove" },
          Version: { default: "CoderHelm release" },
          StatusId: { default: "Progress ID" },
          InstallStatusId: { default: "The install's progress ID" },
        },
      },
    },
    Parameters: {
      Stage: { Type: "String", AllowedPattern: NAME, ConstraintDescription: "the install name: lowercase letters, digits and hyphens", Description: "The install to remove. Its resources start with coderhelm-<name>." },
      ConfirmStage: { Type: "String", AllowedPattern: NAME, ConstraintDescription: "the install name again", Description: "The same name again, to confirm. The stack is refused if they differ." },
      Mode: {
        Type: "String",
        Default: "keep",
        AllowedValues: ["keep", "everything"],
        Description: "keep: remove CoderHelm and keep its tables, data bucket, KMS keys, user pool and secret. everything: delete those too (the KMS keys after AWS's 7-day minimum; nothing can be recovered).",
      },
      Version: { Type: "String", Default: "latest", AllowedPattern: "^(latest|v\\d+\\.\\d+\\.\\d+(-[0-9A-Za-z.]+)?)$", Description: "The CoderHelm release whose uninstaller runs, e.g. v0.9.0." },
      StatusId: { Type: "String", Default: "", AllowedPattern: "^$|^[a-z0-9]{16}$", Description: "Lets the uninstall guide show progress (a small public file with the phase and what was removed). Empty for none." },
      InstallStatusId: { Type: "String", Default: "", AllowedPattern: "^$|^[a-z0-9]{16}$", Description: "The install guide's progress ID, so its bucket goes too (when the Launch Stack is already gone). Empty to find it from the Launch Stack." },
    },
    Rules: {
      TheNameTwice: {
        Assertions: [{ Assert: { "Fn::Equals": [{ Ref: "ConfirmStage" }, { Ref: "Stage" }] }, AssertDescription: "Type the install name exactly as in Install name to confirm." }],
      },
    },
    Conditions: { HasStatus: { "Fn::Not": [{ "Fn::Equals": [{ Ref: "StatusId" }, ""] }] } },
    Resources: resources,
    Outputs: {
      UninstallGuide: {
        Condition: "HasStatus",
        Description: "Back to the uninstall guide, which shows the progress and what is left.",
        Value: { "Fn::Sub": "https://docs.coderhelm.com/self-host/uninstall/?id=${StatusId}&region=${AWS::Region}&stage=${Stage}" },
      },
      UninstallerLogs: {
        Description: "The uninstall build's log (kept after a failed uninstall).",
        Value: { "Fn::Sub": ["https://${AWS::Region}.console.aws.amazon.com/cloudwatch/home?region=${AWS::Region}#logsV2:log-groups/log-group/$252Faws$252Fcodebuild$252Fcoderhelm-${Stage}-uninstall-${Id}", { Id: LAUNCH_ID }] },
      },
    },
  };
}

export function uninstallTemplateText(): string {
  return [
    "# Generated by infra/lib/uninstall-stack.ts (scripts/uninstall-stack.sh): do not edit by hand.",
    "# JSON is YAML. infra/test/uninstall-stack.test.ts checks this file is exactly that output.",
    JSON.stringify(uninstallTemplate(), null, 2),
    "",
  ].join("\n");
}

if (require.main === module) process.stdout.write(uninstallTemplateText());
