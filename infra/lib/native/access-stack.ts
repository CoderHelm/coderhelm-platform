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
 * Its custom resource (lambda/native-access/handler.py) writes each document
 * that differs: a new default version of each boundary, the deploy role's
 * inline policy. Its function and role are named outside the install's
 * prefix, so no CoderHelm role can invoke, pass or change them
 * (NoIamOutsideTheInstall, OnlyTheInstallsOwn).
 */
import * as fs from "fs";
import { repoPath } from "../paths";
import { Json } from "./package";
import { DEPLOY_BOUNDARY_ARN, deployRolePolicy, nativeBoundaryDoc, nativeNames, parameters, sub, subInstall } from "./parent";

/** The admin's stack: the name every native boundary already reserves for it (access.ts accessStackName). */
export const ACCESS_STACK_NAME = "coderhelm-${Stage}-access";
/** Its writer's function and role: outside coderhelm-<name>-*, so no install role reaches them. */
export const ACCESS_WRITER_NAME = "CoderHelmAccess-${Stage}";

/** The launch parameters the documents use; the updater's Launch link fills them from the parent stack. */
export const ACCESS_PARAMETERS = ["Stage", "AdminEmail", "EmailFrom", "StatusId"];

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

export function buildAccess(version: string, writerCode?: string): Json {
  const n = nativeNames();
  const all = parameters();
  const code = writerCode ?? fs.readFileSync(repoPath("lambda/native-access/handler.py"), "utf8");
  const boundary = nativeBoundaryDoc();
  return {
    AWSTemplateFormatVersion: "2010-09-09",
    Description:
      `CoderHelm ${version}: the permissions of the install's own deploy role and its boundary, applied with your permissions ` +
      "(an install's updates can't change them). It writes a new version of coderhelm-<name>-boundary-stack and " +
      "coderhelm-<name>-deploy-boundary-stack and the deploy role's policy, only where they differ. Deleting this stack changes nothing. " +
      "docs.coderhelm.com/self-host/security",
    Metadata: { CoderHelm: { version, kind: "access" } },
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
          Version: version,
          Boundary: { Arn: sub(n.boundaryArn), Document: boundary },
          DeployBoundary: { Arn: sub(DEPLOY_BOUNDARY_ARN), Document: boundary },
          DeployRole: { RoleName: sub(n.deployRoleName), PolicyName: "deploy", Document: subInstall(deployRolePolicy(n)) },
        },
      },
    },
    Outputs: {
      Version: { Description: "The CoderHelm release whose permissions this stack applied", Value: version },
      Written: { Description: "What it wrote (none: already as this release has them)", Value: { "Fn::GetAtt": ["Access", "Written"] } },
    },
  };
}

