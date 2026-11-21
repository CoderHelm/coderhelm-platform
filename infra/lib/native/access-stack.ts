/**
 * A native release's access stack template, `access.json` (published next to
 * coderhelm.json and pinned by its sha256 in the parent's ReleaseAssets).
 *
 * Why it exists: the parent stack runs every update as its deploy role, and
 * the boundary forbids that role to change itself or a boundary. An AWS
 * admin who executes a parent change set in the console doesn't help: the
 * stack's service role is the deploy role, so CloudFormation still runs it
 * as that role (it failed exactly so on a live install). So the parent never
 * changes its Boundary, DeployBoundary or DeployRole (pinned, parent.ts),
 * and today's documents for them reach an install here instead:
 *
 *   coderhelm-<name>-access, created by an AWS admin from Settings, System
 *   (one Launch link, a quick-create of this template), with the admin's own
 *   permissions: the stack has no service role. No CoderHelm role may
 *   create, change or delete a stack of that name (TheAccessStackStays).
 *
 * A newer release's permissions reach an install that already has the stack
 * by an update of it with the newer access.json (Settings, System: Open the
 * stack, Update, Replace existing template, the URL): never a delete and a
 * new one. Every release keeps the stack's logical ids, types and fixed
 * names (ACCESS_SHAPE), so an update changes each resource in place, and the
 * custom resource's Version is the release, so every update with a newer
 * template runs the writer again.
 *
 * The same stack keeps CoderHelmLogReader in the install's own account
 * current (lambda/native-reader/handler.py, the reader update): every team
 * read there is made as that role, and the install's own roles may never
 * change IAM. After each update of the install's stack (CloudFormation's
 * stack status event), once a day, and when the admin creates or updates
 * this stack, its function creates or updates one stack,
 * CoderHelmLogReaderUpdate-release, from the running release's published
 * update template, with read-only groups only. Its role may do exactly that
 * (READER_TEMPLATE_URL, aws:CalledVia) and nothing else, and like the
 * writer it is named outside the install's prefix.
 *
 * Its custom resource (lambda/native-access/handler.py) writes each document
 * that differs: a new default version of each boundary, the deploy role's
 * inline policy. Its function and role are named outside the install's
 * prefix, so no CoderHelm role can invoke, pass or change them
 * (NoIamOutsideTheInstall, OnlyTheInstallsOwn).
 */
import * as fs from "fs";
import { BOUNDARY_OPTION_READS } from "../access";
import { repoPath } from "../paths";
import { Json } from "./package";
import { DEPLOY_BOUNDARY_ARN, deployRolePolicy, nativeBoundaryDoc, nativeNames, parameters, sub, subInstall } from "./parent";

/** The admin's stack: the name every native boundary already reserves for it (access.ts accessStackName). */
export const ACCESS_STACK_NAME = "coderhelm-${Stage}-access";
/** Its writer's function and role: outside coderhelm-<name>-*, so no install role reaches them. */
export const ACCESS_WRITER_NAME = "CoderHelmAccess-${Stage}";

/** The reader update's function, role and rules: outside coderhelm-<name>-*, like the writer. */
export const READER_UPDATE_NAME = "CoderHelmReader-${Stage}";
/** The role it updates (infra/cfn/coderhelm-log-reader.yaml, RoleName). */
export const LOG_READER_ROLE = "CoderHelmLogReader";
/** The one stack it creates and updates: its inline policy on that role is named after it. */
export const READER_UPDATE_STACK = "CoderHelmLogReaderUpdate-release";
/** The only templates it may create that stack from: a release's published update template. */
export const READER_TEMPLATE_URL = "https://coderhelm-public.s3.amazonaws.com/launch/v*/coderhelm-log-reader-update.yaml";

/** The launch parameters the documents use; the updater's Launch link fills them from the parent stack. */
export const ACCESS_PARAMETERS = ["Stage", "AdminEmail", "EmailFrom", "StatusId"];

/**
 * What stays the same in every release's access.json, so an admin's update of
 * the stack with a newer one changes each resource in place (a new logical id,
 * a new type or a new fixed name would replace it, and a replaced writer role
 * or function is a delete and create AWS may refuse while the old one exists).
 */
export const ACCESS_SHAPE = {
  WriterRole: { Type: "AWS::IAM::Role", RoleName: ACCESS_WRITER_NAME },
  Writer: { Type: "AWS::Lambda::Function", FunctionName: ACCESS_WRITER_NAME },
  Access: { Type: "Custom::Access" },
  ReaderRole: { Type: "AWS::IAM::Role", RoleName: READER_UPDATE_NAME },
  Reader: { Type: "AWS::Lambda::Function", FunctionName: READER_UPDATE_NAME },
  ReaderAfterUpdate: { Type: "AWS::Events::Rule" },
  ReaderDaily: { Type: "AWS::Events::Rule" },
  ReaderFromUpdate: { Type: "AWS::Lambda::Permission" },
  ReaderFromDaily: { Type: "AWS::Lambda::Permission" },
  ReaderNow: { Type: "Custom::ReaderUpdate" },
} as const;

/**
 * The reader update's own permissions: read the install's stack (its release
 * and switches), find CoderHelmLogReader, and create, update or remove the
 * one stack CoderHelmLogReaderUpdate-release, only from a release's update
 * template; that stack's inline policy on the role is written only through
 * CloudFormation. No other IAM call, no other stack, no other role.
 */
export function readerPolicy(): Json {
  const stack = `arn:\${AWS::Partition}:cloudformation:\${AWS::Region}:\${AWS::AccountId}:stack/${READER_UPDATE_STACK}/*`;
  const role = `arn:\${AWS::Partition}:iam::\${AWS::AccountId}:role/${LOG_READER_ROLE}`;
  return sub({
    Version: "2012-10-17",
    Statement: [
      { Sid: "InstallStack", Effect: "Allow", Action: "cloudformation:DescribeStacks", Resource: "arn:${AWS::Partition}:cloudformation:${AWS::Region}:${AWS::AccountId}:stack/coderhelm-${Stage}/*" },
      { Sid: "FindTheReader", Effect: "Allow", Action: "iam:GetRole", Resource: role },
      // A template's parameter names, so only the ones it declares are given.
      { Sid: "TemplateParameters", Effect: "Allow", Action: "cloudformation:GetTemplateSummary", Resource: "*" },
      {
        Sid: "ReleaseTemplateOnly",
        Effect: "Allow",
        Action: ["cloudformation:CreateStack", "cloudformation:UpdateStack"],
        Resource: stack,
        Condition: { StringLike: { "cloudformation:TemplateUrl": READER_TEMPLATE_URL } },
      },
      // A first create that rolled back can't be updated: it is removed and made again.
      { Sid: "ReaderStack", Effect: "Allow", Action: ["cloudformation:DescribeStacks", "cloudformation:DeleteStack"], Resource: stack },
      {
        Sid: "ReaderPolicyThroughCloudFormation",
        Effect: "Allow",
        Action: ["iam:GetRolePolicy", "iam:PutRolePolicy", "iam:DeleteRolePolicy"],
        Resource: role,
        Condition: { "ForAnyValue:StringEquals": { "aws:CalledVia": ["cloudformation.amazonaws.com"] } },
      },
      {
        Sid: "OwnLog",
        Effect: "Allow",
        Action: ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
        Resource: `arn:\${AWS::Partition}:logs:\${AWS::Region}:\${AWS::AccountId}:log-group:/aws/lambda/${READER_UPDATE_NAME}:*`,
      },
    ],
  });
}

/** The reader update's resources (the file's comment: the reader update). */
export function readerResources(version: string, code: string): Json {
  const target = [{ Id: "reader", Arn: { "Fn::GetAtt": ["Reader", "Arn"] } }];
  const allow = (rule: string) => ({
    Type: "AWS::Lambda::Permission",
    Properties: { FunctionName: { Ref: "Reader" }, Action: "lambda:InvokeFunction", Principal: "events.amazonaws.com", SourceArn: { "Fn::GetAtt": [rule, "Arn"] } },
  });
  return {
    ReaderRole: {
      Type: "AWS::IAM::Role",
      Properties: {
        RoleName: sub(READER_UPDATE_NAME),
        Description: "CoderHelm: brings each release's read-only permissions to CoderHelmLogReader in this account (made by an AWS admin)",
        AssumeRolePolicyDocument: { Version: "2012-10-17", Statement: [{ Effect: "Allow", Principal: { Service: "lambda.amazonaws.com" }, Action: "sts:AssumeRole" }] },
        Policies: [{ PolicyName: "reader-update", PolicyDocument: readerPolicy() }],
      },
    },
    Reader: {
      Type: "AWS::Lambda::Function",
      Properties: {
        FunctionName: sub(READER_UPDATE_NAME),
        Description: "CoderHelm: the access stack's reader update (lambda/native-reader)",
        Runtime: "python3.12",
        Handler: "index.handler",
        Timeout: 300,
        Role: { "Fn::GetAtt": ["ReaderRole", "Arn"] },
        Environment: { Variables: { ROOT_STACK: sub("coderhelm-${Stage}") } },
        Code: { ZipFile: code },
      },
    },
    // After each update of the install's stack: a version update or a switch of the AWS card.
    ReaderAfterUpdate: {
      Type: "AWS::Events::Rule",
      Properties: {
        Name: sub(`${READER_UPDATE_NAME}-updated`),
        Description: "CoderHelm: the install's stack finished an update",
        EventPattern: {
          source: ["aws.cloudformation"],
          "detail-type": ["CloudFormation Stack Status Change"],
          detail: {
            "stack-id": [{ prefix: sub("arn:${AWS::Partition}:cloudformation:${AWS::Region}:${AWS::AccountId}:stack/coderhelm-${Stage}/") }],
            "status-details": { status: ["UPDATE_COMPLETE"] },
          },
        },
        Targets: target,
      },
    },
    // A reader made after the last update, or an event that never came.
    ReaderDaily: {
      Type: "AWS::Events::Rule",
      Properties: { Name: sub(`${READER_UPDATE_NAME}-daily`), Description: "CoderHelm: the reader update, once a day", ScheduleExpression: "rate(1 day)", Targets: target },
    },
    ReaderFromUpdate: allow("ReaderAfterUpdate"),
    ReaderFromDaily: allow("ReaderDaily"),
    // At once, when the admin creates or updates this stack (its Version changes with every release).
    ReaderNow: { Type: "Custom::ReaderUpdate", Properties: { ServiceToken: { "Fn::GetAtt": ["Reader", "Arn"] }, Version: version } },
  };
}

export function writerPolicy(): Json {
  const n = nativeNames();
  return sub({
    Version: "2012-10-17",
    Statement: [
      {
        Sid: "Boundaries",
        Effect: "Allow",
        Action: ["iam:GetPolicy", "iam:GetPolicyVersion", "iam:ListPolicyVersions", "iam:CreatePolicyVersion", "iam:DeletePolicyVersion", "iam:SetDefaultPolicyVersion"],
        Resource: [n.boundaryArn, DEPLOY_BOUNDARY_ARN],
      },
      { Sid: "DeployRolePolicy", Effect: "Allow", Action: ["iam:GetRolePolicy", "iam:PutRolePolicy"], Resource: n.deployRoleArn },
      {
        Sid: "OwnLog",
        Effect: "Allow",
        Action: ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
        Resource: `arn:\${AWS::Partition}:logs:\${AWS::Region}:\${AWS::AccountId}:log-group:/aws/lambda/${ACCESS_WRITER_NAME}:*`,
      },
    ],
  });
}

export function buildAccess(version: string, writerCode?: string, readerCode?: string): Json {
  const n = nativeNames();
  const all = parameters();
  const code = writerCode ?? fs.readFileSync(repoPath("lambda/native-access/handler.py"), "utf8");
  const reader = readerCode ?? fs.readFileSync(repoPath("lambda/native-reader/handler.py"), "utf8");
  const boundary = nativeBoundaryDoc();
  return {
    AWSTemplateFormatVersion: "2010-09-09",
    Description:
      `CoderHelm ${version}: the permissions of the install's own deploy role and its boundary, applied with your permissions ` +
      "(an install's updates can't change them). It writes a new version of coderhelm-<name>-boundary-stack and " +
      "coderhelm-<name>-deploy-boundary-stack and the deploy role's policy, only where they differ, and after each update it brings the " +
      "release's read-only permissions to CoderHelmLogReader in this account (the stack CoderHelmLogReaderUpdate-release). Deleting this stack changes nothing. " +
      "docs.coderhelm.com/self-host/security",
    // reads: the boundary's reads only one switch of the AWS card uses (access.ts),
    // so the updater asks an admin for them only when that switch is on.
    Metadata: { CoderHelm: { version, kind: "access", reads: BOUNDARY_OPTION_READS } },
    Parameters: Object.fromEntries(ACCESS_PARAMETERS.map((p) => [p, all[p]])),
    Conditions: { HasEmailFrom: { "Fn::Not": [{ "Fn::Equals": [{ Ref: "EmailFrom" }, ""] }] } },
    Resources: {
      WriterRole: {
        Type: "AWS::IAM::Role",
        Properties: {
          RoleName: sub(ACCESS_WRITER_NAME),
          Description: "CoderHelm: writes the install's boundary and deploy role policy from the access stack (made by an AWS admin)",
          AssumeRolePolicyDocument: { Version: "2012-10-17", Statement: [{ Effect: "Allow", Principal: { Service: "lambda.amazonaws.com" }, Action: "sts:AssumeRole" }] },
          Policies: [{ PolicyName: "write-access", PolicyDocument: writerPolicy() }],
        },
      },
      Writer: {
        Type: "AWS::Lambda::Function",
        Properties: {
          FunctionName: sub(ACCESS_WRITER_NAME),
          Description: "CoderHelm: the access stack's writer (lambda/native-access)",
          Runtime: "python3.12",
          Handler: "index.handler",
          Timeout: 120,
          Role: { "Fn::GetAtt": ["WriterRole", "Arn"] },
          Code: { ZipFile: code },
        },
      },
      Access: {
        Type: "Custom::Access",
        Properties: {
          ServiceToken: { "Fn::GetAtt": ["Writer", "Arn"] },
          // The release: an update of the stack with a newer access.json always
          // changes it, so CloudFormation sends the writer an Update every time.
          Version: version,
          Boundary: { Arn: sub(n.boundaryArn), Document: boundary },
          DeployBoundary: { Arn: sub(DEPLOY_BOUNDARY_ARN), Document: boundary },
          DeployRole: { RoleName: sub(n.deployRoleName), PolicyName: "deploy", Document: subInstall(deployRolePolicy(n)) },
        },
      },
      ...readerResources(version, reader),
    },
    Outputs: {
      Version: { Description: "The CoderHelm release whose permissions this stack applied", Value: version },
      Written: { Description: "What it wrote (none: already as this release has them)", Value: { "Fn::GetAtt": ["Access", "Written"] } },
    },
  };
}

