/**
 * What a hardened self-hosted install may do in its AWS account, written
 * once and used by both owners of it: the CDK app (`AccessStack`, for an
 * install an admin hardened with `coderhelm-selfhost access harden`) and
 * the Launch Stack (infra/cfn/coderhelm-selfhost-launch.yaml, for a new
 * install; infra/test/launch-stack.test.ts keeps the template equal to
 * these functions).
 *
 * Three pieces (docs/self-host/security.md, "What the installer can do in
 * your account"):
 *
 * - The permissions boundary (`<prefix>-boundary`): the most any role
 *   CoderHelm creates can ever do, whatever its own policy says. It allows
 *   the services CoderHelm uses and denies IAM users and access keys,
 *   Organizations, account settings, CloudTrail and GuardDuty, IAM outside
 *   the install's prefix, a role without this boundary, assuming other
 *   roles in this account, and any change to the boundary itself.
 * - The deploy role (`<prefix>-deploy`): CloudFormation's role for the
 *   install's stacks. Only CloudFormation may assume it; it may create roles
 *   only with the boundary, pass only the install's roles, and change only
 *   resources named for the install (or tagged for it where AWS names them
 *   by id).
 * - The deploy window (`deploy-window`, an inline policy): what an install
 *   or update build may do while it runs. Nothing else in the account holds
 *   it: the updater (or the Launch Stack's starter) writes it onto the build
 *   role only after checking the build is one it started with the expected
 *   buildspec, binds it to that build's own session (aws:userid) and an end
 *   time, and removes it when the build reports back. Between runs the
 *   build roles can only report to the updater.
 *
 * Values may be CloudFormation substitutions (`${Stage}`): the template
 * passes them, and every string here is then used inside Fn::Sub.
 */

/** Tag on every taggable resource of a hardened install (KMS, Cognito and CloudFront are scoped by it). */
export const INSTALL_TAG = "coderhelm:install";
/** The inline policy the deploy window adds to a build role. */
export const WINDOW_POLICY_NAME = "deploy-window";
/** Placeholders the window opener fills in (lambda/updater/handler.py, the Launch Stack's starter). */
export const WINDOW_USER_ID = "__CODERHELM_USER_ID__";
export const WINDOW_EXPIRES = "__CODERHELM_EXPIRES__";
/** How long a window lasts at most: the build's timeout (90 minutes) and a margin. */
export const WINDOW_MINUTES = 100;

export interface AccessNames {
  partition: string;
  account: string;
  region: string;
  prefix: string;
  boundaryName: string;
  boundaryArn: string;
  deployRoleName: string;
  deployRoleArn: string;
  assetsBucket: string;
  secretName: string;
  /** The access stack (owned by an admin; updates never change it). */
  accessStackName: string;
}

export function partitionOf(region: string): string {
  if (region.startsWith("us-gov-")) return "aws-us-gov";
  if (region.startsWith("cn-")) return "aws-cn";
  return "aws";
}

/**
 * The names of a hardened install's access pieces. `id` is the Launch
 * Stack's own short id (each launch makes its own set, kept when the stack
 * is deleted or rolled back, so a retry never collides); empty for an
 * install hardened from the CLI (`AccessStack`).
 */
export function accessNames(p: { partition?: string; account: string; region: string; prefix: string; suffix: string; secretName: string; id?: string }): AccessNames {
  const partition = p.partition ?? partitionOf(p.region);
  const tail = p.id ? `-${p.id}` : "";
  const boundaryName = `${p.prefix}-boundary${tail}`;
  const deployRoleName = `${p.prefix}-deploy${tail}`;
  return {
    partition,
    account: p.account,
    region: p.region,
    prefix: p.prefix,
    boundaryName,
    boundaryArn: `arn:${partition}:iam::${p.account}:policy/${boundaryName}`,
    deployRoleName,
    deployRoleArn: `arn:${partition}:iam::${p.account}:role/${deployRoleName}`,
    assetsBucket: `${p.prefix}-assets${tail}${p.suffix}`,
    secretName: p.secretName,
    accessStackName: `${p.prefix}-access`,
  };
}

export interface Statement {
  Sid: string;
  Effect: "Allow" | "Deny";
  Action?: string | string[];
  NotAction?: string | string[];
  Resource?: string | string[];
  NotResource?: string | string[];
  Condition?: Record<string, Record<string, string | string[]>>;
}

export interface PolicyDocument {
  Version: "2012-10-17";
  Statement: Statement[];
}

const doc = (Statement: Statement[]): PolicyDocument => ({ Version: "2012-10-17", Statement });

function arns(n: AccessNames) {
  const iam = `arn:${n.partition}:iam::${n.account}`;
  const at = (svc: string) => `arn:${n.partition}:${svc}:${n.region}:${n.account}`;
  return {
    role: `${iam}:role/${n.prefix}-*`,
    policy: `${iam}:policy/${n.prefix}-*`,
    anyRoleHere: `${iam}:role/*`,
    at,
    stacks: `${at("cloudformation")}:stack/${n.prefix}-*/*`,
    accessStack: `${at("cloudformation")}:stack/${n.accessStackName}/*`,
  };
}

/** Role and policy writes: only with the boundary on the role. */
export const ROLE_WRITES = [
  "iam:CreateRole",
  "iam:PutRolePolicy",
  "iam:AttachRolePolicy",
  "iam:DetachRolePolicy",
  "iam:DeleteRolePolicy",
  "iam:PutRolePermissionsBoundary",
];

const ROLE_MANAGE = [
  "iam:DeleteRole",
  "iam:GetRole",
  "iam:GetRolePolicy",
  "iam:ListRolePolicies",
  "iam:ListAttachedRolePolicies",
  "iam:ListRoleTags",
  "iam:UpdateRole",
  "iam:UpdateRoleDescription",
  "iam:UpdateAssumeRolePolicy",
  "iam:TagRole",
  "iam:UntagRole",
];

const POLICY_MANAGE = [
  "iam:CreatePolicy",
  "iam:DeletePolicy",
  "iam:CreatePolicyVersion",
  "iam:DeletePolicyVersion",
  "iam:GetPolicy",
  "iam:GetPolicyVersion",
  "iam:ListPolicyVersions",
  "iam:ListEntitiesForPolicy",
  "iam:TagPolicy",
  "iam:UntagPolicy",
];

/**
 * The services the boundary allows: what the install's functions, builds
 * and its deploy use (infra/test/access.test.ts checks every action in
 * every synthesized policy is in here).
 */
export const BOUNDARY_SERVICES = [
  "acm:*",
  "apigateway:*",
  "bedrock:InvokeModel",
  "bedrock:InvokeModelWithResponseStream",
  "bedrock:Get*",
  "bedrock:List*",
  "cloudformation:*",
  "cloudfront:*",
  "cloudwatch:*",
  "codebuild:*",
  "cognito-idp:*",
  "dynamodb:*",
  "events:*",
  "kms:*",
  "lambda:*",
  "logs:*",
  "route53:*",
  "s3:*",
  "scheduler:*",
  "secretsmanager:*",
  "ses:*",
  "sns:*",
  "sqs:*",
  "ssm:*",
  "sts:AssumeRole",
  "sts:GetCallerIdentity",
];

/** The guardrails every CoderHelm policy repeats (boundary, deploy role, window). */
export function guardrails(n: AccessNames): Statement[] {
  const a = arns(n);
  return [
    {
      Sid: "NoIamUsersOrKeys",
      Effect: "Deny",
      Action: [
        "iam:CreateUser", "iam:CreateAccessKey", "iam:UpdateAccessKey", "iam:CreateLoginProfile", "iam:UpdateLoginProfile",
        "iam:AttachUserPolicy", "iam:PutUserPolicy", "iam:AddUserToGroup", "iam:CreateGroup", "iam:PutGroupPolicy",
        "iam:AttachGroupPolicy", "iam:CreateServiceSpecificCredential", "iam:UploadSSHPublicKey",
        "iam:CreateOpenIDConnectProvider", "iam:CreateSAMLProvider", "iam:UpdateAccountPasswordPolicy", "iam:CreateAccountAlias",
      ],
      Resource: "*",
    },
    {
      Sid: "NoAccountGuardrailChanges",
      Effect: "Deny",
      Action: ["organizations:*", "account:*", "cloudtrail:*", "guardduty:*"],
      Resource: "*",
    },
    {
      // Nothing about IAM outside the install's own roles and policies.
      Sid: "NoIamOutsideTheInstall",
      Effect: "Deny",
      Action: "iam:*",
      NotResource: [a.role, a.policy],
    },
    {
      Sid: "RolesOnlyWithTheBoundary",
      Effect: "Deny",
      Action: ROLE_WRITES,
      Resource: "*",
      Condition: { StringNotEquals: { "iam:PermissionsBoundary": n.boundaryArn } },
    },
    {
      // No role's boundary comes off, and the boundary itself can't be edited.
      Sid: "TheBoundaryStays",
      Effect: "Deny",
      Action: ["iam:DeleteRolePermissionsBoundary", "iam:CreatePolicyVersion", "iam:DeletePolicy", "iam:DeletePolicyVersion", "iam:SetDefaultPolicyVersion", "iam:TagPolicy", "iam:UntagPolicy"],
      Resource: [n.boundaryArn, a.anyRoleHere],
    },
    {
      // The deploy role is the admin's: no release changes it.
      Sid: "TheDeployRoleStays",
      Effect: "Deny",
      Action: [...ROLE_WRITES, "iam:DeleteRole", "iam:UpdateRole", "iam:UpdateAssumeRolePolicy", "iam:TagRole", "iam:UntagRole"],
      Resource: n.deployRoleArn,
    },
    {
      // The access stack (boundary and deploy role) changes only by an admin.
      Sid: "TheAccessStackStays",
      Effect: "Deny",
      Action: ["cloudformation:CreateChangeSet", "cloudformation:ExecuteChangeSet", "cloudformation:UpdateStack", "cloudformation:DeleteStack", "cloudformation:CreateStack", "cloudformation:SetStackPolicy"],
      Resource: a.accessStack,
    },
    {
      // Other roles in this account (the CDK bootstrap's, yours) can't be assumed.
      Sid: "NoOtherRolesHere",
      Effect: "Deny",
      Action: ["sts:AssumeRole", "sts:AssumeRoleWithWebIdentity", "sts:AssumeRoleWithSAML", "sts:TagSession", "sts:SetSourceIdentity"],
      Resource: "*",
      Condition: { StringEquals: { "aws:ResourceAccount": n.account } },
    },
  ];
}

/** The permissions boundary: the most any CoderHelm role can do. */
export function boundaryPolicy(n: AccessNames): PolicyDocument {
  const a = arns(n);
  return doc([
    { Sid: "CoderHelmServices", Effect: "Allow", Action: BOUNDARY_SERVICES, Resource: "*" },
    { Sid: "InstallRoleWrites", Effect: "Allow", Action: ROLE_WRITES, Resource: a.role, Condition: { StringEquals: { "iam:PermissionsBoundary": n.boundaryArn } } },
    { Sid: "InstallRoles", Effect: "Allow", Action: [...ROLE_MANAGE, "iam:PassRole"], Resource: a.role },
    { Sid: "InstallPolicies", Effect: "Allow", Action: POLICY_MANAGE, Resource: a.policy },
    ...guardrails(n),
  ]);
}

/** CloudFormation's role for the install's stacks (the deploy role's own policy). */
export function deployPolicy(n: AccessNames): PolicyDocument {
  const a = arns(n);
  const p = n.prefix;
  const tagged = { StringEquals: { [`aws:ResourceTag/${INSTALL_TAG}`]: p } };
  return doc([
    { Sid: "Roles", Effect: "Allow", Action: ROLE_WRITES, Resource: a.role, Condition: { StringEquals: { "iam:PermissionsBoundary": n.boundaryArn } } },
    { Sid: "RolesManage", Effect: "Allow", Action: ROLE_MANAGE, Resource: a.role },
    { Sid: "Policies", Effect: "Allow", Action: POLICY_MANAGE, Resource: a.policy },
    { Sid: "PassInstallRoles", Effect: "Allow", Action: "iam:PassRole", Resource: a.role },
    {
      Sid: "Functions",
      Effect: "Allow",
      Action: "lambda:*",
      Resource: [`${a.at("lambda")}:function:${p}-*`, `${a.at("lambda")}:layer:${p}-*`],
    },
    {
      Sid: "FunctionEvents",
      Effect: "Allow",
      Action: ["lambda:CreateEventSourceMapping", "lambda:UpdateEventSourceMapping", "lambda:DeleteEventSourceMapping"],
      Resource: "*",
      Condition: { ArnLike: { "lambda:FunctionArn": `${a.at("lambda")}:function:${p}-*` } },
    },
    // An event source mapping's ARN is a random id, so it can't be scoped by
    // name; CloudFormation tags each one as it creates it.
    { Sid: "FunctionEventTags", Effect: "Allow", Action: ["lambda:TagResource", "lambda:UntagResource", "lambda:ListTags"], Resource: `${a.at("lambda")}:event-source-mapping:*` },
    { Sid: "Tables", Effect: "Allow", Action: "dynamodb:*", Resource: [`${a.at("dynamodb")}:table/${p}`, `${a.at("dynamodb")}:table/${p}/*`, `${a.at("dynamodb")}:table/${p}-*`] },
    { Sid: "Buckets", Effect: "Allow", Action: "s3:*", Resource: [`arn:${n.partition}:s3:::${p}-*`] },
    { Sid: "Queues", Effect: "Allow", Action: "sqs:*", Resource: `${a.at("sqs")}:${p}-*` },
    { Sid: "Topics", Effect: "Allow", Action: "sns:*", Resource: `${a.at("sns")}:${p}-*` },
    {
      Sid: "Logs",
      Effect: "Allow",
      Action: "logs:*",
      Resource: [`${a.at("logs")}:log-group:/aws/lambda/${p}-*`, `${a.at("logs")}:log-group:/aws/codebuild/${p}-*`],
    },
    {
      Sid: "Schedules",
      Effect: "Allow",
      Action: ["events:*", "scheduler:*"],
      Resource: [`${a.at("events")}:rule/${p}-*`, `${a.at("scheduler")}:schedule-group/${p}-*`, `${a.at("scheduler")}:schedule/${p}-*/*`],
    },
    {
      Sid: "Alarms",
      Effect: "Allow",
      Action: "cloudwatch:*",
      Resource: [`${a.at("cloudwatch")}:alarm:${p}-*`, `arn:${n.partition}:cloudwatch::${n.account}:dashboard/${p}`, `arn:${n.partition}:cloudwatch::${n.account}:dashboard/${p}-*`],
    },
    { Sid: "Builds", Effect: "Allow", Action: "codebuild:*", Resource: [`${a.at("codebuild")}:project/${p}-*`, `${a.at("codebuild")}:report-group/${p}-*`] },
    { Sid: "Parameters", Effect: "Allow", Action: "ssm:*", Resource: `${a.at("ssm")}:parameter/${p}/*` },
    {
      Sid: "Email",
      Effect: "Allow",
      Action: "ses:*",
      Resource: [`${a.at("ses")}:configuration-set/${p}-*`, `${a.at("ses")}:template/${p}-*`, `${a.at("ses")}:identity/*`],
    },
    // Named by id: created with the install's tag (AWS checks TagResource as
    // it creates them), changed only while they carry it. A key is never
    // deleted or disabled by a deploy (CoderHelm keeps its keys).
    { Sid: "KeysCreate", Effect: "Allow", Action: ["kms:CreateKey", "kms:TagResource"], Resource: "*", Condition: { StringEquals: { [`aws:RequestTag/${INSTALL_TAG}`]: p } } },
    { Sid: "Keys", Effect: "Allow", Action: "kms:*", Resource: `${a.at("kms")}:key/*`, Condition: tagged },
    { Sid: "KeyAliases", Effect: "Allow", Action: ["kms:CreateAlias", "kms:DeleteAlias", "kms:UpdateAlias"], Resource: `${a.at("kms")}:alias/${p}-*` },
    { Sid: "UserPoolsCreate", Effect: "Allow", Action: "cognito-idp:CreateUserPool", Resource: "*" },
    { Sid: "UserPoolsTag", Effect: "Allow", Action: "cognito-idp:TagResource", Resource: "*", Condition: { StringEquals: { [`aws:RequestTag/${INSTALL_TAG}`]: p } } },
    { Sid: "UserPools", Effect: "Allow", Action: "cognito-idp:*", Resource: `${a.at("cognito-idp")}:userpool/*`, Condition: tagged },
    { Sid: "DistributionsCreate", Effect: "Allow", Action: "cloudfront:CreateDistribution", Resource: "*" },
    { Sid: "DistributionsTag", Effect: "Allow", Action: "cloudfront:TagResource", Resource: "*", Condition: { StringEquals: { [`aws:RequestTag/${INSTALL_TAG}`]: p } } },
    { Sid: "Distributions", Effect: "Allow", Action: "cloudfront:*", Resource: `arn:${n.partition}:cloudfront::${n.account}:distribution/*`, Condition: tagged },
    { Sid: "EdgeFunctions", Effect: "Allow", Action: "cloudfront:*", Resource: `arn:${n.partition}:cloudfront::${n.account}:function/${p}-*` },
    {
      // Not taggable or named by id only (docs/self-host/security.md lists them).
      Sid: "EdgeSettings",
      Effect: "Allow",
      Action: [
        "cloudfront:CreateOriginAccessControl", "cloudfront:UpdateOriginAccessControl", "cloudfront:DeleteOriginAccessControl", "cloudfront:GetOriginAccessControl",
        "cloudfront:CreateResponseHeadersPolicy", "cloudfront:UpdateResponseHeadersPolicy", "cloudfront:DeleteResponseHeadersPolicy", "cloudfront:GetResponseHeadersPolicy",
      ],
      Resource: "*",
    },
    {
      Sid: "HttpApi",
      Effect: "Allow",
      // Tagging is its own action: CloudFormation tags the API, its stage and
      // a custom domain as it creates them.
      Action: ["apigateway:GET", "apigateway:POST", "apigateway:PUT", "apigateway:PATCH", "apigateway:DELETE", "apigateway:TagResource", "apigateway:UntagResource"],
      Resource: [
        `arn:${n.partition}:apigateway:${n.region}::/apis`, `arn:${n.partition}:apigateway:${n.region}::/apis/*`, `arn:${n.partition}:apigateway:${n.region}::/tags/*`,
        `arn:${n.partition}:apigateway:${n.region}::/domainnames`, `arn:${n.partition}:apigateway:${n.region}::/domainnames/*`,
      ],
    },
    { Sid: "CertificatesRequest", Effect: "Allow", Action: ["acm:RequestCertificate", "acm:AddTagsToCertificate"], Resource: "*", Condition: { StringEquals: { [`aws:RequestTag/${INSTALL_TAG}`]: p } } },
    { Sid: "Certificates", Effect: "Allow", Action: "acm:*", Resource: `arn:${n.partition}:acm:*:${n.account}:certificate/*`, Condition: tagged },
    { Sid: "DnsRecords", Effect: "Allow", Action: ["route53:ChangeResourceRecordSets", "route53:GetChange"], Resource: [`arn:${n.partition}:route53:::hostedzone/*`, `arn:${n.partition}:route53:::change/*`] },
    // The deploy role carries the boundary too, whose guardrails deny the rest.
    { Sid: "NoIamUsersOrKeys", Effect: "Deny", Action: ["iam:CreateUser", "iam:CreateAccessKey", "iam:CreateLoginProfile"], Resource: "*" },
    { Sid: "NoKeyDestruction", Effect: "Deny", Action: ["kms:ScheduleKeyDeletion", "kms:DisableKey", "kms:DeleteImportedKeyMaterial"], Resource: "*" },
    {
      Sid: "Reads",
      Effect: "Allow",
      Action: [
        "cloudformation:Describe*", "cloudformation:Get*", "cloudformation:List*",
        "cloudfront:Get*", "cloudfront:List*", "cloudwatch:Describe*", "cloudwatch:Get*", "cloudwatch:List*",
        "cognito-idp:Describe*", "cognito-idp:List*", "dynamodb:Describe*", "dynamodb:List*", "events:Describe*", "events:List*",
        "kms:List*", "lambda:Get*", "lambda:List*", "logs:Describe*", "logs:List*", "route53:Get*", "route53:List*",
        "ses:Get*", "ses:List*", "sqs:List*", "sns:List*", "ssm:Describe*", "acm:Describe*", "acm:List*", "codebuild:List*",
        "scheduler:List*", "sts:GetCallerIdentity",
      ],
      Resource: "*",
    },
  ]);
}

/**
 * The deploy window: what one build may do while it runs, bound to its own
 * session (aws:userid) and an end time. The opener fills in both.
 */
export function windowPolicy(n: AccessNames): PolicyDocument {
  return doc(windowStatements(n));
}

/**
 * The window policy in parts that each fit a standard SSM parameter (4 KB):
 * the updater reads them in order and joins their statements.
 */
export function windowPolicyParts(n: AccessNames, max = 3800): string[] {
  const parts: Statement[][] = [[]];
  for (const st of windowPolicy(n).Statement) {
    const cur = parts[parts.length - 1];
    if (cur.length && JSON.stringify([...cur, st]).length > max) parts.push([st]);
    else cur.push(st);
  }
  return parts.map((p) => JSON.stringify(p));
}

function windowStatements(n: AccessNames): Statement[] {
  const a = arns(n);
  const p = n.prefix;
  const id = { "aws:userid": WINDOW_USER_ID };
  const until = { DateLessThan: { "aws:CurrentTime": WINDOW_EXPIRES } };
  const s = (x: Statement, extra: Record<string, string | string[]> = {}): Statement => ({
    ...x,
    Condition: { StringEquals: { ...id, ...extra }, ...until, ...(x.Condition ?? {}) },
  });
  return [
    s(
      {
        Sid: "WindowStacksThroughDeployRole",
        Effect: "Allow",
        Action: ["cloudformation:CreateChangeSet", "cloudformation:CreateStack", "cloudformation:UpdateStack", "cloudformation:DeleteStack"],
        Resource: a.stacks,
      },
      { "cloudformation:RoleArn": n.deployRoleArn },
    ),
    s({
      Sid: "WindowStacks",
      Effect: "Allow",
      Action: [
        "cloudformation:ExecuteChangeSet", "cloudformation:DeleteChangeSet", "cloudformation:DescribeChangeSet", "cloudformation:ListChangeSets",
        "cloudformation:DescribeStacks", "cloudformation:DescribeStackEvents", "cloudformation:DescribeStackResources", "cloudformation:ListStackResources",
        "cloudformation:GetTemplate", "cloudformation:CancelUpdateStack", "cloudformation:ContinueUpdateRollback", "cloudformation:RollbackStack",
        "cloudformation:TagResource", "cloudformation:UntagResource",
      ],
      Resource: a.stacks,
    }),
    s({ Sid: "WindowStackReads", Effect: "Allow", Action: ["cloudformation:ListStacks", "cloudformation:DescribeStacks", "cloudformation:GetTemplateSummary", "cloudformation:ValidateTemplate"], Resource: "*" }),
    s({ Sid: "WindowPassDeployRole", Effect: "Allow", Action: "iam:PassRole", Resource: n.deployRoleArn }, { "iam:PassedToService": "cloudformation.amazonaws.com" }),
    s({
      Sid: "WindowAssets",
      Effect: "Allow",
      Action: ["s3:GetObject*", "s3:PutObject*", "s3:ListBucket", "s3:GetBucket*", "s3:GetEncryptionConfiguration", "s3:AbortMultipartUpload"],
      Resource: [`arn:${n.partition}:s3:::${n.assetsBucket}`, `arn:${n.partition}:s3:::${n.assetsBucket}/*`],
    }),
    s({ Sid: "WindowInstallState", Effect: "Allow", Action: ["ssm:GetParameter", "ssm:PutParameter"], Resource: `${a.at("ssm")}:parameter/${p}/installer/*` }),
    s({
      Sid: "WindowSecret",
      Effect: "Allow",
      Action: ["secretsmanager:DescribeSecret", "secretsmanager:GetSecretValue", "secretsmanager:PutSecretValue", "secretsmanager:CreateSecret"],
      Resource: `${a.at("secretsmanager")}:secret:${n.secretName}-*`,
    }),
    s({
      Sid: "WindowHealthAndChecks",
      Effect: "Allow",
      Action: ["lambda:GetFunctionConfiguration", "lambda:InvokeFunction", "sqs:GetQueueUrl", "sqs:GetQueueAttributes"],
      Resource: [`${a.at("lambda")}:function:${p}-*`, `${a.at("sqs")}:${p}-*`],
    }),
    s({
      Sid: "WindowModelCheckSetting",
      Effect: "Allow",
      Action: "dynamodb:GetItem",
      Resource: `${a.at("dynamodb")}:table/${p}-settings`,
      Condition: { "ForAllValues:StringEquals": { "dynamodb:LeadingKeys": ["SYSTEM"] } },
    }),
    s({
      Sid: "WindowLookups",
      Effect: "Allow",
      Action: ["ses:GetAccount", "route53:ListHostedZones", "route53:ListHostedZonesByName", "route53:GetHostedZone"],
      Resource: "*",
    }),
    // The build role carries the boundary, whose guardrails deny the rest.
    { Sid: "WindowNoIamUsersOrKeys", Effect: "Deny", Action: ["iam:CreateUser", "iam:CreateAccessKey", "iam:CreateLoginProfile"], Resource: "*" },
  ];
}
