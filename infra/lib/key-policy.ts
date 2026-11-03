// Key policies for CoderHelm's own deployment (the hosted side of hybrid).
//
// The hosted account also runs other products, so the default key policy
// ("the account may do anything with this key", leaving every decision to
// IAM) would let any role there that is ever given kms:* use CoderHelm's keys.
// Instead each key's policy says itself who may do what:
//
// - Managing the key (policy, rotation, deletion, tags): people through the
//   SSO administrator role, and CloudFormation through CDK's execution role.
//   Never encrypting, decrypting or signing with it.
// - Using it: only roles of this deployment (named `<prefix>-*`, which covers
//   every role CDK names after the stacks), the execution role (DynamoDB and
//   S3 create their grants through it), and the SSO administrator role (the
//   operator's tools read the tables and the bucket). Each role still needs
//   its own IAM grant: the key policy narrows, it never grants alone.
//
// A self-hosted install keeps CDK's default policy: the account is its
// owner's, and the install's permissions boundary already confines its roles.

import * as cdk from "aws-cdk-lib";
import * as iam from "aws-cdk-lib/aws-iam";
import { ResolvedConfig } from "./config";

/** The AWS IAM Identity Center administrator role, by its reserved path. */
export const SSO_ADMIN_ROLE = "role/aws-reserved/sso.amazonaws.com/*AWSReservedSSO_AdministratorAccess_*";
/** CDK's CloudFormation execution role (default bootstrap qualifier or any). */
export const CFN_EXEC_ROLE = "role/cdk-*-cfn-exec-role-*";

/** Key management, without any use of the key. */
export const KEY_ADMIN_ACTIONS = [
  "kms:Create*",
  "kms:Describe*",
  "kms:Enable*",
  "kms:List*",
  "kms:Put*",
  "kms:Update*",
  "kms:Revoke*",
  "kms:Disable*",
  "kms:Get*",
  "kms:Delete*",
  "kms:TagResource",
  "kms:UntagResource",
  "kms:ScheduleKeyDeletion",
  "kms:CancelKeyDeletion",
  "kms:RotateKeyOnDemand",
];

/** Using a symmetric key, as AWS services and the Lambdas do. */
export const SYMMETRIC_USE_ACTIONS = [
  "kms:Encrypt",
  "kms:Decrypt",
  "kms:ReEncrypt*",
  "kms:GenerateDataKey*",
  "kms:DescribeKey",
  "kms:CreateGrant",
  "kms:ListGrants",
  "kms:RevokeGrant",
];

/** Using a signing key. */
export const SIGNING_USE_ACTIONS = ["kms:Sign", "kms:Verify", "kms:GetPublicKey", "kms:DescribeKey"];

/** Changes to the signing key only a person may make (never CloudFormation). */
export const SIGNING_LOCKED_ACTIONS = ["kms:PutKeyPolicy", "kms:ScheduleKeyDeletion", "kms:DisableKey", "kms:CreateGrant"];

/** True for CoderHelm's own deployment, whose key policies are narrowed. */
export function narrowsKeyPolicies(cfg: ResolvedConfig): boolean {
  return cfg.hybrid !== null && cfg.hybrid !== undefined;
}

function roleArn(stack: cdk.Stack, rolePath: string): string {
  return `arn:${stack.partition}:iam::${stack.account}:${rolePath}`;
}

/**
 * The key policy for a key of CoderHelm's own deployment, or undefined (keep
 * CDK's default) for a self-hosted install.
 */
export function hostedKeyPolicy(
  stack: cdk.Stack,
  cfg: ResolvedConfig,
  kind: "symmetric" | "signing",
): iam.PolicyDocument | undefined {
  if (!narrowsKeyPolicies(cfg)) return undefined;
  const admins = [roleArn(stack, SSO_ADMIN_ROLE), roleArn(stack, CFN_EXEC_ROLE)];
  const users = [roleArn(stack, `role/${cfg.prefix}-*`), roleArn(stack, CFN_EXEC_ROLE), roleArn(stack, SSO_ADMIN_ROLE)];
  const account = new iam.AccountRootPrincipal();
  const statements = [
    new iam.PolicyStatement({
      sid: "ManageKeyAdminsAndCloudFormationOnly",
      principals: [account],
      actions: KEY_ADMIN_ACTIONS,
      resources: ["*"],
      conditions: { ArnLike: { "aws:PrincipalArn": admins } },
    }),
    new iam.PolicyStatement({
      sid: "UseKeyThisDeploymentOnly",
      principals: [account],
      actions: kind === "signing" ? SIGNING_USE_ACTIONS : SYMMETRIC_USE_ACTIONS,
      resources: ["*"],
      conditions: { ArnLike: { "aws:PrincipalArn": users } },
    }),
  ];
  if (kind === "signing") {
    statements.push(
      new iam.PolicyStatement({
        sid: "SigningKeyChangesByPeopleOnly",
        effect: iam.Effect.DENY,
        principals: [new iam.AnyPrincipal()],
        actions: SIGNING_LOCKED_ACTIONS,
        resources: ["*"],
        conditions: { ArnNotLike: { "aws:PrincipalArn": [roleArn(stack, SSO_ADMIN_ROLE)] } },
      }),
    );
  }
  return new iam.PolicyDocument({ statements });
}

/** Deletion waits the full 30 days on every key (AWS's maximum). */
export const KEY_PENDING_WINDOW = cdk.Duration.days(30);
