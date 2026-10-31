/**
 * What a self-hosted install may do in its AWS account, written once and
 * used by every owner of it: a native release's parent stack
 * (infra/lib/native/parent.ts: the boundary and the optional auto-update
 * role, `-stack` names), the CDK app's `AccessStack` (an install an admin
 * hardened with `coderhelm-selfhost access harden`), and installs from an
 * earlier Launch Stack until they migrate.
 *
 * Three pieces (docs/self-host/security.md, "What an install can do in your
 * account"):
 *
 * - The permissions boundary: the most any role CoderHelm creates can ever
 *   do, whatever its own policy says. It allows the services CoderHelm uses,
 *   limits changes to the install's own functions, stacks, buckets, secrets
 *   and parameters, and denies IAM users and access keys, Organizations,
 *   account settings, CloudTrail and GuardDuty, IAM outside the install's
 *   prefix, a role without this boundary, instance commands, assuming other
 *   roles in this account (all but CoderHelm's two read roles, which only
 *   the gateway and worker may assume), and any change to the boundary.
 * - The deploy role: CloudFormation's role for the install's stacks (on a
 *   native install, only with auto-update on). Only CloudFormation may
 *   assume it; it may create roles only with the boundary, pass only the
 *   install's roles, and change only resources named for the install (or
 *   tagged for it where AWS names them by id).
 * - The deploy window (`deploy-window`): installs from before native
 *   releases only. What their update build may do while it runs, written
 *   by the updater onto the build role after checking the build, bound to
 *   its session and an end time, removed when the build reports back.
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
  /** The SES identity the install sends from: the only identity it may change (any when unset). */
  emailIdentity?: string;
  /** The Route 53 zone of the install's own domain: the only zone where it may change records other than certificate checks (any when unset). */
  zoneId?: string;
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
    ownReadRoles: OWN_READ_ROLES.map((r) => `${iam}:role/${r}`),
  };
}

/**
 * The two read-only roles CoderHelm itself has teams create, by their exact
 * names: the log reader (infra/cfn/coderhelm-log-reader.yaml, `RoleName:
 * CoderHelmLogReader`) and the data-source role (the setup script in
 * services/common/src/live/aws.rs, `ROLE=CoderhelmReadOnly`). An install in
 * the same account as the data it reads takes these on in its own account;
 * the boundary allows exactly that and nothing else there.
 */
export const OWN_READ_ROLES = ["CoderHelmLogReader", "CoderhelmReadOnly"];
/**
 * The roles that take on those roles: the gateway's and the worker's, which
 * carry this tag (infra/lib/api-stack.ts, infra/lib/worker-stack.ts). Only
 * the deploy role can set or remove it (`RuntimeTagOnlyByDeploy`).
 */
export const RUNTIME_ROLE_TAG = "coderhelm:role";
export const RUNTIME_ROLES = ["gateway", "worker"];
/** STS actions on a role in this account: all denied, except sts:AssumeRole on the read roles above. */
const ROLE_SESSION_ACTIONS = ["sts:AssumeRole", "sts:AssumeRoleWithWebIdentity", "sts:AssumeRoleWithSAML", "sts:TagSession", "sts:SetSourceIdentity"];

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
 * What the boundary allows on the install's own resources, by name: every
 * action of these services on the names `ownResources` lists (S3 has its
 * own statement: an object key is part of its ARN).
 */
export const OWN_SERVICES = [
  "apigateway:*", "cloudformation:*", "cloudfront:*", "cloudwatch:*", "codebuild:*", "dynamodb:*", "events:*", "kms:*", "lambda:*", "logs:*",
  "scheduler:*", "secretsmanager:*", "ses:*", "sns:*", "sqs:*", "ssm:*",
];

/** AWS's account that publishes the Lambda Web Adapter layer the gateway uses. */
const LAMBDA_ADAPTER_ACCOUNT = "753240598075";

/** Services whose resources AWS names by id: allowed only on those carrying the install's tag. */
export const TAGGED_SERVICES = ["acm:*", "cloudfront:*", "cognito-idp:*", "kms:*"];

/**
 * What the boundary allows on any resource: calls that take no resource
 * (creating something new, lookups), reads of names and settings (never of
 * data: no item, object, parameter value, secret value or function code),
 * sending email, using a key the caller's own policy names, and models.
 * infra/test/access.test.ts checks every action every synthesized role is
 * given is allowed on the resources it names.
 */
export const ANYWHERE = [
  "bedrock:InvokeModel*", "sts:GetCallerIdentity", "kms:Decrypt", "kms:Encrypt", "kms:GenerateDataKey*", "kms:DescribeKey", "kms:List*",
  "ses:Send*", "ses:Get*", "ses:Describe*", "ses:*Template",
  "cloudformation:Describe*", "cloudformation:List*", "cloudformation:GetTemplateSummary", "cloudformation:ValidateTemplate",
  "cloudfront:CreateDistribution*", "cloudfront:CreateFunction", "cloudfront:*OriginAccessControl", "cloudfront:*ResponseHeadersPolicy",
  "cognito-idp:CreateUserPool", "cognito-idp:DescribeUserPoolDomain", "dynamodb:Describe*", "logs:Describe*",
  "secretsmanager:GetRandomPassword", "lambda:CreateEventSourceMapping", "route53:Get*", "route53:List*",
  "acm:RequestCertificate", "acm:AddTagsToCertificate", "acm:Describe*", "acm:List*",
];

/** Every action the boundary allows somewhere (tests and docs read it). */
export const BOUNDARY_SERVICES = [...OWN_SERVICES, "s3:*", ...TAGGED_SERVICES, ...ANYWHERE, "acm:DeleteCertificate", "route53:ChangeResourceRecordSets", "sts:AssumeRole"];

/**
 * The install's own resources by name, any service. After `coderhelm-<name>`
 * every pattern needs a hyphen, a slash or the end, and a name has no hyphen,
 * so none also covers another install's (coderhelm-prod2-*).
 */
export function ownResources(n: AccessNames): string[] {
  const p = n.prefix;
  return [
    // stack/, table/, rule/, schedule/, project/, report-group/, dashboard/, configuration-set/, template/, alias/, function/ (CloudFront), log-group:/aws/lambda/.
    `arn:*:*:*:*:*/${p}-*`,
    // stack/<install>/<id>, table/<install> and its indexes, parameter/<install>/, dashboard/<install>.
    `arn:*:*:*:*:*/${p}`, `arn:*:*:*:*:*/${p}/*`,
    // function:, layer:, alarm:.
    `arn:*:*:*:*:*:${p}-*`,
    // Queues and topics.
    `arn:*:*:*:*:${p}-*`,
    "arn:*:lambda:*:*:event-source-mapping:*", `arn:*:lambda:*:${LAMBDA_ADAPTER_ACCOUNT}:layer:*`, "arn:*:cloudformation:*:aws:transform/*",
    `arn:*:secretsmanager:*:*:secret:${n.secretName.replace(/\/secrets$/, "")}/*`,
    `arn:*:ses:*:*:identity/${n.emailIdentity ?? "*"}`,
    "arn:*:apigateway:*::/apis*", "arn:*:apigateway:*::/domainnames*", "arn:*:apigateway:*::/tags*",
  ];
}

/**
 * What the boundary refuses although its allows would cover it: the
 * boundaries and the deploy role (named like the install's own roles), the
 * access stack, the install's tag on a key, user pool or distribution that
 * isn't already the install's, and roles in this account other than
 * CoderHelm's two read roles. Everything the allows don't name (IAM users
 * and keys, Organizations, CloudTrail, GuardDuty, instance commands, other
 * resources in the account) is refused because nothing allows it.
 */
export function guardrails(n: AccessNames, otherBoundaries: string[] = []): Statement[] {
  const a = arns(n);
  // A native install's two boundaries (its own and the auto-update role's copy) as one pattern, to stay
  // within IAM's 6,144 characters: coderhelm-<name>-boundary-stack and coderhelm-<name>-deploy-boundary-stack.
  const boundaries = otherBoundaries.length ? n.boundaryArn.replace(/-boundary(-[a-z0-9]+)$/, "-*boundary$1") : n.boundaryArn;
  for (const o of otherBoundaries) if (!new RegExp(`^${boundaries.replace(/[.$^{}()|[\]\\+?]/g, "\\$&").replace(/\*/g, ".*")}$`).test(o)) throw new Error(`${o} is not covered by ${boundaries}`);
  return [
    {
      // No role's boundary comes off, the boundary (and a native install's
      // auto-update role's own copy) can't be edited or deleted, and no role
      // can create a policy under either name. The first launch, turning
      // auto-update on and a delete run with an admin's own permissions.
      Sid: "TheBoundaryStays",
      Effect: "Deny",
      // CreatePolicy*: CreatePolicy, CreatePolicyVersion; DeletePolicy*: DeletePolicy, DeletePolicyVersion;
      // SetDefault*: SetDefaultPolicyVersion; *agPolicy: TagPolicy, UntagPolicy.
      Action: ["iam:DeleteRolePermissionsBoundary", "iam:CreatePolicy*", "iam:DeletePolicy*", "iam:SetDefault*", "iam:*agPolicy"],
      Resource: [boundaries, a.anyRoleHere],
    },
    {
      // The deploy role is the admin's: no release changes it.
      Sid: "TheDeployRoleStays",
      Effect: "Deny",
      // Create*, Put*, Attach*, Detach*, Delete*, Update*: every change; *agRole: TagRole, UntagRole.
      Action: ["iam:Create*", "iam:Put*", "iam:Attach*", "iam:Detach*", "iam:Delete*", "iam:Update*", "iam:*agRole"],
      Resource: n.deployRoleArn,
    },
    ...(otherBoundaries.length
      ? []
      : [{
          // The access stack (boundary and deploy role) changes only by an admin (a native install has none).
          Sid: "TheAccessStackStays",
          Effect: "Deny" as const,
          Action: ["cloudformation:CreateChangeSet", "cloudformation:ExecuteChangeSet", "cloudformation:UpdateStack", "cloudformation:DeleteStack", "cloudformation:CreateStack", "cloudformation:SetStackPolicy"],
          Resource: a.accessStack,
        }]),
    {
      // A key, user pool or distribution comes under the boundary only by
      // the install's tag: no role may put that tag on (or take it off) one
      // that isn't already the install's.
      Sid: "OnlyTheInstallsTagged",
      Effect: "Deny",
      // *agResource: TagResource, UntagResource.
      Action: ["kms:*agResource", "cognito-idp:*agResource", "cloudfront:*agResource"],
      Resource: ["arn:*:kms:*:*:key/*", "arn:*:cognito-idp:*:*:userpool/*", "arn:*:cloudfront::*:distribution/*"],
      Condition: { StringNotEquals: { [`aws:ResourceTag/${INSTALL_TAG}`]: n.prefix } },
    },
    // CoderHelm never exports a certificate's private key.
    { Sid: "NoCertificateExport", Effect: "Deny", Action: "acm:ExportCertificate", Resource: "*" },
    {
      // Roles in this account (the CDK bootstrap's, the deploy role, the
      // install's own, yours) can't be assumed. The one exception is
      // CoderHelm's two read roles, by exact name.
      Sid: "NoOtherRolesHere",
      Effect: "Deny",
      Action: "sts:AssumeRole",
      NotResource: a.ownReadRoles,
      Condition: { StringEquals: { "aws:ResourceAccount": n.account } },
    },
    {
      // And only by the gateway's and the worker's roles (their tag; a role
      // without it is refused). Each read role's trust policy still requires
      // the team's external id.
      Sid: "ReadRolesHereOnlyGatewayAndWorker",
      Effect: "Deny",
      Action: "sts:AssumeRole",
      Resource: a.ownReadRoles,
      Condition: { StringNotEquals: { [`aws:PrincipalTag/${RUNTIME_ROLE_TAG}`]: RUNTIME_ROLES } },
    },
    {
      // That tag is set and removed only by CloudFormation through the
      // deploy role, so no other role can give itself the exception.
      Sid: "RuntimeTagOnlyByDeploy",
      Effect: "Deny",
      Action: ["iam:CreateRole", "iam:TagRole", "iam:UntagRole"],
      Resource: "*",
      Condition: { "ForAnyValue:StringEqualsIgnoreCase": { "aws:TagKeys": [RUNTIME_ROLE_TAG] }, ArnNotEquals: { "aws:PrincipalArn": n.deployRoleArn } },
    },
  ];
}

/**
 * The permissions boundary: the most any CoderHelm role can do.
 * `otherBoundaries`: a native install's auto-update role carries its own
 * copy (kept on delete); neither copy can be edited by an install role.
 */
export function boundaryPolicy(n: AccessNames, otherBoundaries: string[] = []): PolicyDocument {
  const a = arns(n);
  const p = n.prefix;
  return doc([
    { Sid: "InstallResources", Effect: "Allow", Action: OWN_SERVICES, Resource: ownResources(n) },
    { Sid: "InstallBuckets", Effect: "Allow", Action: "s3:*", Resource: [`arn:*:s3:::${p}-*`, "arn:*:s3:::coderhelm-setup-*"] },
    { Sid: "InstallTagged", Effect: "Allow", Action: TAGGED_SERVICES, Resource: "*", Condition: { StringEquals: { [`aws:ResourceTag/${INSTALL_TAG}`]: p } } },
    // Created with the install's tag (AWS checks the tag as it creates them).
    { Sid: "CreateTagged", Effect: "Allow", Action: ["kms:CreateKey", "kms:TagResource", "cognito-idp:TagResource", "cloudfront:TagResource"], Resource: "*", Condition: { StringEquals: { [`aws:RequestTag/${INSTALL_TAG}`]: p } } },
    // The certificates the updater requested for the install's own domain (tagged coderhelm=<install>).
    { Sid: "Certificates", Effect: "Allow", Action: "acm:DeleteCertificate", Resource: "*", Condition: { StringEquals: { "aws:ResourceTag/coderhelm": p } } },
    { Sid: "Anywhere", Effect: "Allow", Action: ANYWHERE, Resource: "*" },
    // DNS: the install's own zone; elsewhere only the CNAME a certificate check uses (its name starts with an underscore).
    { Sid: "InstallZone", Effect: "Allow", Action: "route53:ChangeResourceRecordSets", Resource: `arn:*:route53:::hostedzone/${n.zoneId ?? "*"}` },
    {
      Sid: "CertificateRecords",
      Effect: "Allow",
      Action: "route53:ChangeResourceRecordSets",
      Resource: "*",
      Condition: { "ForAllValues:StringLike": { "route53:ChangeResourceRecordSetsNormalizedRecordNames": ["_*"] }, "ForAllValues:StringEquals": { "route53:ChangeResourceRecordSetsRecordTypes": ["CNAME"] } },
    },
    // CoderHelm's two read roles, in any account (the guardrails narrow them in this one).
    { Sid: "ReadRoles", Effect: "Allow", Action: "sts:AssumeRole", Resource: [`arn:*:iam::*:role/${OWN_READ_ROLES[0]}`, `arn:*:iam::*:role/${OWN_READ_ROLES[1]}*`] },
    { Sid: "InstallRoleWrites", Effect: "Allow", Action: ROLE_WRITES, Resource: a.role, Condition: { StringEquals: { "iam:PermissionsBoundary": n.boundaryArn } } },
    // The same calls as ROLE_MANAGE and POLICY_MANAGE, shorter (the boundary must fit IAM's 6,144 characters).
    { Sid: "InstallRoles", Effect: "Allow", Action: ["iam:Get*", "iam:List*", "iam:DeleteRole", "iam:Update*", "iam:*agRole", "iam:PassRole"], Resource: a.role },
    { Sid: "InstallPolicies", Effect: "Allow", Action: ["iam:CreatePolicy*", "iam:DeletePolicy*", "iam:Get*", "iam:List*", "iam:*agPolicy"], Resource: a.policy },
    ...guardrails(n, otherBoundaries),
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
      Resource: [`${a.at("ses")}:configuration-set/${p}-*`, `${a.at("ses")}:template/${p}-*`, `${a.at("ses")}:identity/${n.emailIdentity ?? "*"}`],
    },
    // CloudFormation's email template handler may use SES's first API, whose
    // template calls take no resource ARN (AWS checks them against "*").
    { Sid: "EmailTemplatesV1", Effect: "Allow", Action: ["ses:CreateTemplate", "ses:UpdateTemplate", "ses:DeleteTemplate"], Resource: "*" },
    // Named by id: created with the install's tag (AWS checks TagResource as
    // it creates them), changed only while they carry it. A key is never
    // deleted or disabled by a deploy (CoderHelm keeps its keys).
    { Sid: "KeysCreate", Effect: "Allow", Action: ["kms:CreateKey", "kms:TagResource"], Resource: "*", Condition: { StringEquals: { [`aws:RequestTag/${INSTALL_TAG}`]: p } } },
    { Sid: "Keys", Effect: "Allow", Action: "kms:*", Resource: `${a.at("kms")}:key/*`, Condition: tagged },
    { Sid: "KeyAliases", Effect: "Allow", Action: ["kms:CreateAlias", "kms:DeleteAlias", "kms:UpdateAlias"], Resource: `${a.at("kms")}:alias/${p}-*` },
    { Sid: "UserPoolsCreate", Effect: "Allow", Action: "cognito-idp:CreateUserPool", Resource: "*" },
    { Sid: "UserPoolsTag", Effect: "Allow", Action: "cognito-idp:TagResource", Resource: "*", Condition: { StringEquals: { [`aws:RequestTag/${INSTALL_TAG}`]: p } } },
    { Sid: "UserPools", Effect: "Allow", Action: "cognito-idp:*", Resource: `${a.at("cognito-idp")}:userpool/*`, Condition: tagged },
    // CreateDistributionWithTags is the API CloudFormation calls; AWS's service
    // reference authorizes it as CreateDistribution and TagResource, and it is
    // named too in case AWS checks it by its own name (as API Gateway does TagResource).
    { Sid: "DistributionsCreate", Effect: "Allow", Action: ["cloudfront:CreateDistribution", "cloudfront:CreateDistributionWithTags"], Resource: "*" },
    { Sid: "DistributionsTag", Effect: "Allow", Action: "cloudfront:TagResource", Resource: "*", Condition: { StringEquals: { [`aws:RequestTag/${INSTALL_TAG}`]: p } } },
    { Sid: "Distributions", Effect: "Allow", Action: "cloudfront:*", Resource: `arn:${n.partition}:cloudfront::${n.account}:distribution/*`, Condition: tagged },
    // CreateFunction takes no resource ARN (AWS checks it against "*"); every
    // later call on the function is scoped to its name.
    { Sid: "EdgeFunctionsCreate", Effect: "Allow", Action: "cloudfront:CreateFunction", Resource: "*" },
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
    // Records only in the install's own domain's zone (a native install's DomainZoneId).
    { Sid: "DnsRecords", Effect: "Allow", Action: ["route53:ChangeResourceRecordSets", "route53:GetChange"], Resource: [`arn:${n.partition}:route53:::hostedzone/${n.zoneId ?? "*"}`, `arn:${n.partition}:route53:::change/*`] },
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
        "ses:Get*", "ses:List*", "ses:DescribeConfigurationSet", "sqs:List*", "sns:List*", "ssm:Describe*", "acm:Describe*", "acm:List*", "codebuild:List*",
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
