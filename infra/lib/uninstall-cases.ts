/**
 * The uninstall window's scope as calls: what an uninstall build of
 * coderhelm-prod may delete, and what it may never touch (another install
 * whose name starts with prod's, the uninstaller's own pieces, the CDK
 * bootstrap, IAM users, other roles, untagged or other installs' keys and
 * pools, other services). infra/test/uninstall-sim.test.ts checks today's
 * policies give each verdict with test/iam-eval.ts, and AWS's IAM policy
 * simulator gave it (test/fixtures/uninstall-sim.json, from
 * scripts/deploy-role-sim.py --uninstall).
 */
import { PolicyDocument } from "./access";
import { expandWindow, uninstallBoundaryPolicy, uninstallNames, uninstallWindowStatements, UninstallNames } from "./uninstall-access";

export const CASE_ACCOUNT = "111122223333";
export const CASE_REGION = "us-east-1";
export const CASE_LAUNCH_ID = "1a2b3c4d";
export const CASE_UNINSTALL_ID = "5e6f7a8b";
export const CASE_OWN_STATUS = "uninstallprogres";
export const CASE_INSTALL_STATUS = "abcdefghijklmnop";
export const CASE_USER_ID = "AROAEXAMPLEBUILDROLE1:AWSCodeBuild-0e5b7b1e-1b8a-4d2f-9a51-3c2d6e7f8a9b";
export const CASE_NOW = "2026-10-30T12:00:00Z";
export const CASE_EXPIRES = "2026-10-30T13:40:00Z";

export type Mode = "keep" | "everything";
export interface Case {
  why: string;
  action: string;
  resource: string;
  context?: Record<string, string | string[]>;
  /** Allowed in these modes; denied otherwise. */
  allowed: Mode[];
}

export function caseNames(): UninstallNames {
  return uninstallNames({ account: CASE_ACCOUNT, region: CASE_REGION, stage: "prod", uid: CASE_UNINSTALL_ID, statusId: CASE_OWN_STATUS });
}

/** The build role's idle policy (as the stack writes it). */
export function idlePolicy(n: UninstallNames): PolicyDocument {
  const log = `arn:${n.partition}:logs:${n.region}:${n.account}:log-group:${n.buildLogGroup}`;
  return {
    Version: "2012-10-17",
    Statement: [
      { Sid: "OwnLog", Effect: "Allow", Action: ["logs:CreateLogStream", "logs:PutLogEvents"], Resource: [log, `${log}:*`] },
      { Sid: "Progress", Effect: "Allow", Action: "s3:PutObject", Resource: `arn:${n.partition}:s3:::coderhelm-setup-${n.statusId}/status.json` },
      { Sid: "AskForWindow", Effect: "Allow", Action: "lambda:InvokeFunction", Resource: `arn:${n.partition}:lambda:${n.region}:${n.account}:function:${n.windowFunction}` },
      { Sid: "CloseOwnWindow", Effect: "Allow", Action: "iam:DeleteRolePolicy", Resource: `arn:${n.partition}:iam::${n.account}:role/${n.buildRole}` },
    ],
  };
}

export function casePolicies(mode: Mode) {
  const n = caseNames();
  const window = expandWindow(uninstallWindowStatements(n), {
    userId: CASE_USER_ID, expires: CASE_EXPIRES, ids: [CASE_LAUNCH_ID], launch: ["coderhelm-prod"], setup: [CASE_INSTALL_STATUS], siblings: ["coderhelm-prod-eu"],
  }, mode);
  return { idle: idlePolicy(n), window, boundary: uninstallBoundaryPolicy(n) };
}

export function uninstallCases(): Case[] {
  const A = CASE_ACCOUNT;
  const at = (svc: string, res: string, region = CASE_REGION) => `arn:aws:${svc}:${region}:${A}:${res}`;
  const iam = (res: string) => `arn:aws:iam::${A}:${res}`;
  const tag = (v: string) => ({ "aws:ResourceTag/coderhelm:install": v });
  const both: Mode[] = ["keep", "everything"];
  const all: Mode[] = ["everything"];
  const no: Mode[] = [];
  const deploy = iam(`role/coderhelm-prod-deploy-${CASE_LAUNCH_ID}`);
  const boundary = iam(`policy/coderhelm-prod-boundary-${CASE_LAUNCH_ID}`);
  return [
    // The install's stacks, through its deploy role.
    { why: "an install stack, tagged, through the deploy role", action: "cloudformation:DeleteStack", resource: at("cloudformation", "stack/coderhelm-prod-api/0a1b"), context: { ...tag("coderhelm-prod"), "cloudformation:RoleArn": deploy }, allowed: both },
    { why: "an install stack without the deploy role", action: "cloudformation:DeleteStack", resource: at("cloudformation", "stack/coderhelm-prod-api/0a1b"), context: tag("coderhelm-prod"), allowed: no },
    { why: "an install stack through another role", action: "cloudformation:DeleteStack", resource: at("cloudformation", "stack/coderhelm-prod-api/0a1b"), context: { ...tag("coderhelm-prod"), "cloudformation:RoleArn": iam("role/Admin") }, allowed: no },
    { why: "a stack of the name tagged for another install", action: "cloudformation:DeleteStack", resource: at("cloudformation", "stack/coderhelm-prod-web/0a1b"), context: { ...tag("coderhelm-prod-eu"), "cloudformation:RoleArn": deploy }, allowed: no },
    { why: "the longer install's stack, even tagged for this one", action: "cloudformation:DeleteStack", resource: at("cloudformation", "stack/coderhelm-prod-eu-api/0a1b"), context: { ...tag("coderhelm-prod"), "cloudformation:RoleArn": deploy }, allowed: no },
    { why: "the Launch Stack, by name, with the build's own credentials", action: "cloudformation:DeleteStack", resource: at("cloudformation", "stack/coderhelm-prod/1a2b3c4d-0000"), allowed: both },
    { why: "the uninstaller's own stack", action: "cloudformation:DeleteStack", resource: at("cloudformation", "stack/coderhelm-prod-uninstall/5e6f"), allowed: no },
    { why: "the CDK bootstrap stack", action: "cloudformation:DeleteStack", resource: at("cloudformation", "stack/CDKToolkit/0a1b"), allowed: no },
    { why: "someone else's stack", action: "cloudformation:DeleteStack", resource: at("cloudformation", "stack/my-app/0a1b"), allowed: no },
    { why: "the deploy role, passed to CloudFormation", action: "iam:PassRole", resource: deploy, context: { "iam:PassedToService": "cloudformation.amazonaws.com" }, allowed: both },
    { why: "the deploy role, passed to Lambda", action: "iam:PassRole", resource: deploy, context: { "iam:PassedToService": "lambda.amazonaws.com" }, allowed: no },
    { why: "an admin role, passed to CloudFormation", action: "iam:PassRole", resource: iam("role/Admin"), context: { "iam:PassedToService": "cloudformation.amazonaws.com" }, allowed: no },
    // The Launch Stack's pieces, by its id.
    { why: "the Launch Stack's starter function", action: "lambda:DeleteFunction", resource: at("lambda", `function:coderhelm-prod-starter-${CASE_LAUNCH_ID}`), allowed: both },
    { why: "an install function without the Launch Stack id", action: "lambda:DeleteFunction", resource: at("lambda", "function:coderhelm-prod-gateway-0f1e2d3c"), allowed: no },
    { why: "the uninstaller's window function", action: "lambda:DeleteFunction", resource: at("lambda", `function:coderhelm-prod-uninstall-window-${CASE_UNINSTALL_ID}`), allowed: no },
    { why: "the uninstaller's window function changed", action: "lambda:UpdateFunctionCode", resource: at("lambda", `function:coderhelm-prod-uninstall-window-${CASE_UNINSTALL_ID}`), allowed: no },
    { why: "someone else's function", action: "lambda:DeleteFunction", resource: at("lambda", "function:my-app"), allowed: no },
    { why: "the Launch Stack's installer project", action: "codebuild:DeleteProject", resource: at("codebuild", "project/coderhelm-prod-installer"), allowed: both },
    { why: "the uninstaller's own project", action: "codebuild:DeleteProject", resource: at("codebuild", `project/coderhelm-prod-uninstall-${CASE_UNINSTALL_ID}`), allowed: no },
    { why: "the install's sandbox project (its stack deletes it)", action: "codebuild:DeleteProject", resource: at("codebuild", "project/coderhelm-prod-sandbox"), allowed: no },
    // Roles and the boundary.
    { why: "the deploy role", action: "iam:DeleteRole", resource: deploy, allowed: both },
    { why: "the Launch Stack's installer role", action: "iam:DeleteRole", resource: iam(`role/coderhelm-prod-installer-${CASE_LAUNCH_ID}`), allowed: both },
    { why: "a leftover role tagged for the install", action: "iam:DeleteRole", resource: iam("role/coderhelm-prod-provider-0f1e2d3c"), context: { "iam:ResourceTag/coderhelm:install": "coderhelm-prod" }, allowed: both },
    { why: "an untagged role of the name", action: "iam:DeleteRole", resource: iam("role/coderhelm-prod-someones"), allowed: no },
    { why: "the longer install's deploy role", action: "iam:DeleteRole", resource: iam("role/coderhelm-prod-eu-deploy-77777777"), context: { "iam:ResourceTag/coderhelm:install": "coderhelm-prod-eu" }, allowed: no },
    { why: "the uninstaller's build role", action: "iam:DeleteRole", resource: iam(`role/coderhelm-prod-uninstall-build-${CASE_UNINSTALL_ID}`), allowed: no },
    { why: "an admin role", action: "iam:DeleteRole", resource: iam("role/Admin"), allowed: no },
    { why: "the CDK bootstrap's role", action: "iam:DeleteRole", resource: iam(`role/cdk-hnb659fds-cfn-exec-role-${A}-us-east-1`), allowed: no },
    { why: "a role's policy, the role carrying the install's boundary", action: "iam:DeleteRolePolicy", resource: iam("role/coderhelm-prod-provider-0f1e2d3c"), context: { "iam:PermissionsBoundary": boundary }, allowed: both },
    { why: "a role's policy, the role carrying another boundary", action: "iam:DeleteRolePolicy", resource: iam("role/coderhelm-prod-provider-0f1e2d3c"), context: { "iam:PermissionsBoundary": iam("policy/coderhelm-prod-eu-boundary-77777777") }, allowed: no },
    { why: "a role's policy, the role carrying no boundary", action: "iam:DetachRolePolicy", resource: iam("role/coderhelm-prod-someones"), allowed: no },
    { why: "the uninstaller's window role's policy", action: "iam:DeleteRolePolicy", resource: iam(`role/coderhelm-prod-uninstall-window-${CASE_UNINSTALL_ID}`), context: { "iam:PermissionsBoundary": iam(`policy/coderhelm-prod-uninstall-boundary-${CASE_UNINSTALL_ID}`) }, allowed: no },
    { why: "its own window, closed when the build ends", action: "iam:DeleteRolePolicy", resource: iam(`role/coderhelm-prod-uninstall-build-${CASE_UNINSTALL_ID}`), context: { "iam:PermissionsBoundary": iam(`policy/coderhelm-prod-uninstall-boundary-${CASE_UNINSTALL_ID}`) }, allowed: both },
    { why: "more rights for itself", action: "iam:PutRolePolicy", resource: iam(`role/coderhelm-prod-uninstall-build-${CASE_UNINSTALL_ID}`), context: { "iam:PermissionsBoundary": iam(`policy/coderhelm-prod-uninstall-boundary-${CASE_UNINSTALL_ID}`) }, allowed: no },
    { why: "an admin policy on an install role", action: "iam:AttachRolePolicy", resource: iam("role/coderhelm-prod-provider-0f1e2d3c"), context: { "iam:PermissionsBoundary": boundary, "iam:PolicyARN": "arn:aws:iam::aws:policy/AdministratorAccess" }, allowed: no },
    { why: "the install's permissions boundary", action: "iam:DeletePolicy", resource: boundary, allowed: both },
    { why: "the uninstaller's boundary", action: "iam:DeletePolicy", resource: iam(`policy/coderhelm-prod-uninstall-boundary-${CASE_UNINSTALL_ID}`), allowed: no },
    { why: "the uninstaller's boundary changed", action: "iam:CreatePolicyVersion", resource: iam(`policy/coderhelm-prod-uninstall-boundary-${CASE_UNINSTALL_ID}`), allowed: no },
    { why: "the longer install's boundary", action: "iam:DeletePolicy", resource: iam("policy/coderhelm-prod-eu-boundary-77777777"), allowed: no },
    { why: "an IAM user", action: "iam:CreateUser", resource: iam("user/coderhelm-prod-x"), allowed: no },
    { why: "an access key", action: "iam:CreateAccessKey", resource: iam("user/admin"), allowed: no },
    { why: "an admin role, assumed", action: "sts:AssumeRole", resource: iam("role/Admin"), context: { "aws:ResourceAccount": A }, allowed: no },
    // Buckets, by exact name.
    { why: "the Launch Stack's assets bucket", action: "s3:DeleteBucket", resource: `arn:aws:s3:::coderhelm-prod-assets-${CASE_LAUNCH_ID}-${A}`, allowed: both },
    { why: "the install's progress bucket", action: "s3:DeleteBucket", resource: `arn:aws:s3:::coderhelm-setup-${CASE_INSTALL_STATUS}`, allowed: both },
    { why: "the uninstaller's own progress bucket", action: "s3:DeleteBucket", resource: `arn:aws:s3:::coderhelm-setup-${CASE_OWN_STATUS}`, allowed: no },
    { why: "the uninstaller's progress file, written", action: "s3:PutObject", resource: `arn:aws:s3:::coderhelm-setup-${CASE_OWN_STATUS}/status.json`, allowed: both },
    { why: "another progress bucket", action: "s3:DeleteBucket", resource: "arn:aws:s3:::coderhelm-setup-zzzzzzzzzzzzzzzz", allowed: no },
    { why: "the data bucket", action: "s3:DeleteBucket", resource: `arn:aws:s3:::coderhelm-prod-data-${A}`, allowed: all },
    { why: "the data bucket's old versions", action: "s3:DeleteObjectVersion", resource: `arn:aws:s3:::coderhelm-prod-data-${A}/kb/doc.pdf`, allowed: all },
    { why: "the longer install's data bucket", action: "s3:DeleteBucket", resource: `arn:aws:s3:::coderhelm-prod-eu-data-${A}`, allowed: no },
    { why: "the CDK bootstrap's bucket", action: "s3:DeleteBucket", resource: `arn:aws:s3:::cdk-hnb659fds-assets-${A}-us-east-1`, allowed: no },
    { why: "someone else's bucket", action: "s3:DeleteObject", resource: "arn:aws:s3:::my-app-bucket/file", allowed: no },
    // Data: delete everything only, by the install's tag.
    { why: "an install table, tagged", action: "dynamodb:DeleteTable", resource: at("dynamodb", "table/coderhelm-prod-runs"), context: tag("coderhelm-prod"), allowed: all },
    { why: "the main table, tagged", action: "dynamodb:DeleteTable", resource: at("dynamodb", "table/coderhelm-prod"), context: tag("coderhelm-prod"), allowed: all },
    { why: "a table of the name tagged for another install", action: "dynamodb:DeleteTable", resource: at("dynamodb", "table/coderhelm-prod-runs"), context: tag("someone-else"), allowed: no },
    { why: "the longer install's table", action: "dynamodb:DeleteTable", resource: at("dynamodb", "table/coderhelm-prod-eu-runs"), context: tag("coderhelm-prod"), allowed: no },
    { why: "a table in another region", action: "dynamodb:DeleteTable", resource: at("dynamodb", "table/coderhelm-prod-runs", "eu-west-1"), context: tag("coderhelm-prod"), allowed: no },
    { why: "someone else's table", action: "dynamodb:DeleteTable", resource: at("dynamodb", "table/orders"), allowed: no },
    { why: "an install table's backup", action: "dynamodb:DeleteBackup", resource: at("dynamodb", "table/coderhelm-prod-runs/backup/01700000000000-abcdef12"), allowed: all },
    { why: "the longer install's backup", action: "dynamodb:DeleteBackup", resource: at("dynamodb", "table/coderhelm-prod-eu-runs/backup/01700000000000-abcdef12"), allowed: no },
    { why: "an install KMS key, tagged, scheduled", action: "kms:ScheduleKeyDeletion", resource: at("kms", "key/1234abcd-12ab-34cd-56ef-1234567890ab"), context: tag("coderhelm-prod"), allowed: all },
    { why: "an untagged KMS key", action: "kms:ScheduleKeyDeletion", resource: at("kms", "key/1234abcd-12ab-34cd-56ef-1234567890ab"), allowed: no },
    { why: "another install's KMS key", action: "kms:ScheduleKeyDeletion", resource: at("kms", "key/1234abcd-12ab-34cd-56ef-1234567890ab"), context: tag("coderhelm-prod-eu"), allowed: no },
    { why: "a KMS key disabled", action: "kms:DisableKey", resource: at("kms", "key/1234abcd-12ab-34cd-56ef-1234567890ab"), context: tag("coderhelm-prod"), allowed: no },
    { why: "an install key alias", action: "kms:DeleteAlias", resource: at("kms", "alias/coderhelm-prod-dynamo"), allowed: all },
    { why: "the longer install's key alias", action: "kms:DeleteAlias", resource: at("kms", "alias/coderhelm-prod-eu-dynamo"), allowed: no },
    { why: "an install user pool, tagged", action: "cognito-idp:DeleteUserPool", resource: at("cognito-idp", "userpool/us-east-1_AbCdEf123"), context: tag("coderhelm-prod"), allowed: all },
    { why: "an untagged user pool", action: "cognito-idp:DeleteUserPool", resource: at("cognito-idp", "userpool/us-east-1_AbCdEf123"), allowed: no },
    { why: "the install's secret", action: "secretsmanager:DeleteSecret", resource: at("secretsmanager", "secret:coderhelm/prod/secrets-AbCdEf"), allowed: all },
    { why: "the longer install's secret", action: "secretsmanager:DeleteSecret", resource: at("secretsmanager", "secret:coderhelm/prod-eu/secrets-AbCdEf"), allowed: no },
    { why: "the install's registration, read for hybrid", action: "secretsmanager:GetSecretValue", resource: at("secretsmanager", "secret:coderhelm/prod/secrets-AbCdEf"), allowed: both },
    { why: "someone else's secret, read", action: "secretsmanager:GetSecretValue", resource: at("secretsmanager", "secret:my-app/db-AbCdEf"), allowed: no },
    { why: "the registration's key, through Secrets Manager", action: "kms:Decrypt", resource: at("kms", "key/1234abcd-12ab-34cd-56ef-1234567890ab"), context: { ...tag("coderhelm-prod"), "kms:ViaService": "secretsmanager.us-east-1.amazonaws.com" }, allowed: both },
    { why: "an install key used directly", action: "kms:Decrypt", resource: at("kms", "key/1234abcd-12ab-34cd-56ef-1234567890ab"), context: tag("coderhelm-prod"), allowed: no },
    // Logs, parameters, the updater's certificate.
    { why: "an install log group", action: "logs:DeleteLogGroup", resource: at("logs", "log-group:/aws/lambda/coderhelm-prod-gateway"), allowed: both },
    { why: "the installer's log group", action: "logs:DeleteLogGroup", resource: at("logs", "log-group:/aws/codebuild/coderhelm-prod-installer"), allowed: both },
    { why: "the longer install's log group", action: "logs:DeleteLogGroup", resource: at("logs", "log-group:/aws/lambda/coderhelm-prod-eu-gateway"), allowed: no },
    { why: "the uninstaller's own log group", action: "logs:DeleteLogGroup", resource: at("logs", `log-group:/aws/codebuild/coderhelm-prod-uninstall-${CASE_UNINSTALL_ID}`), allowed: no },
    { why: "someone else's log group", action: "logs:DeleteLogGroup", resource: at("logs", "log-group:/aws/lambda/my-app"), allowed: no },
    { why: "an install parameter", action: "ssm:DeleteParameter", resource: at("ssm", "parameter/coderhelm-prod/public-url"), allowed: both },
    { why: "the uninstaller's window statements", action: "ssm:DeleteParameter", resource: at("ssm", `parameter/coderhelm-prod/uninstall-${CASE_UNINSTALL_ID}/window-policy/1`), allowed: no },
    { why: "the uninstaller's window statements changed", action: "ssm:PutParameter", resource: at("ssm", `parameter/coderhelm-prod/uninstall-${CASE_UNINSTALL_ID}/window-policy/1`), allowed: no },
    { why: "the longer install's parameter", action: "ssm:DeleteParameter", resource: at("ssm", "parameter/coderhelm-prod-eu/public-url"), allowed: no },
    { why: "the updater's certificate", action: "acm:DeleteCertificate", resource: `arn:aws:acm:us-east-1:${A}:certificate/f1e2d3c4-b5a6-4789-a012-345678901234`, context: { "aws:ResourceTag/coderhelm": "coderhelm-prod" }, allowed: both },
    { why: "someone else's certificate", action: "acm:DeleteCertificate", resource: `arn:aws:acm:us-east-1:${A}:certificate/f1e2d3c4-b5a6-4789-a012-345678901234`, allowed: no },
    // Nothing else in the account.
    { why: "a CloudTrail trail", action: "cloudtrail:DeleteTrail", resource: at("cloudtrail", "trail/org"), allowed: no },
    { why: "Organizations", action: "organizations:LeaveOrganization", resource: "*", allowed: no },
    { why: "an EC2 instance", action: "ec2:TerminateInstances", resource: at("ec2", "instance/i-0123456789abcdef0"), allowed: no },
    { why: "an RDS database", action: "rds:DeleteDBInstance", resource: at("rds", "db:orders"), allowed: no },
    { why: "a Route 53 zone's records", action: "route53:ChangeResourceRecordSets", resource: "arn:aws:route53:::hostedzone/Z0123456789", allowed: no },
  ];
}

/** The same calls by another session of the role, and after the window's end: all refused except the idle rights. */
export function sessionCases(): Case[] {
  return uninstallCases().filter((c) => c.allowed.length).slice(0, 12);
}
