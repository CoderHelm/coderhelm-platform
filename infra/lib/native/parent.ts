/**
 * The parent template of a native release, `coderhelm.json` (docs: DESIGN).
 *
 * One stack per install, `coderhelm-<stage>`: the permissions boundary, the
 * install's own assets bucket, the secret, the custom resources' helper
 * (inline code, lambda/native-helper/handler.py), the parts as nested
 * stacks, and the optional auto-update role. Launch Stack creates it, a
 * stack update to the next release's coderhelm.json updates it, Delete
 * stack removes it.
 */
import { accessNames, AccessNames, boundaryPolicy, deployPolicy, INSTALL_TAG, PolicyDocument } from "../access";
import { Asset, Child, Json, KEEP_CONDITION } from "./package";

/** A string with substitutions becomes Fn::Sub; objects and arrays recursively. */
export function sub(v: unknown): any {
  if (typeof v === "string") return v.includes("${") ? { "Fn::Sub": v } : v;
  if (Array.isArray(v)) return v.map(sub);
  if (v && typeof v === "object") return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, sub(x)]));
  return v;
}

export const RELEASE_BUCKET_URL = "https://coderhelm-public.s3.amazonaws.com/launch";

/** The install's names, as CloudFormation substitutions (no launch id: one stack per install). */
export function nativeNames(): AccessNames {
  return { ...accessNames({
    partition: "${AWS::Partition}",
    account: "${AWS::AccountId}",
    region: "${AWS::Region}",
    prefix: "coderhelm-${Stage}",
    suffix: "-${AWS::AccountId}",
    secretName: "coderhelm/${Stage}/secrets",
    // "-stack": never the name of an install hardened from the CLI (its
    // coderhelm-<name>-access stack owns coderhelm-<name>-boundary).
    id: NATIVE_ID,
  }), emailIdentity: "${EmailIdentity}", zoneId: "${DomainZoneId}" };
}

/**
 * `sub`, with ${EmailIdentity} (not a parameter: the address the install
 * sends from, EmailFrom or else AdminEmail) given as a Fn::Sub variable.
 */
export function subInstall(v: unknown): any {
  const out = sub(v);
  const walk = (x: any): any => {
    if (Array.isArray(x)) return x.map(walk);
    if (x && typeof x === "object") {
      if (typeof x["Fn::Sub"] === "string" && x["Fn::Sub"].includes("${EmailIdentity}")) return { "Fn::Sub": [x["Fn::Sub"], { EmailIdentity: installValue("EmailFromResolved") }] };
      return Object.fromEntries(Object.entries(x).map(([k, y]) => [k, walk(y)]));
    }
    return x;
  };
  return walk(out);
}

/** The suffix of a native install's boundary and auto-update role names. */
export const NATIVE_ID = "stack";

/** The auto-update role's own copy of the boundary (kept with the role on delete). */
export const DEPLOY_BOUNDARY_NAME = "coderhelm-${Stage}-deploy-boundary-stack";
export const DEPLOY_BOUNDARY_ARN = `arn:\${AWS::Partition}:iam::\${AWS::AccountId}:policy/${DEPLOY_BOUNDARY_NAME}`;

/** Where the install keeps the release's files: parameters and pseudo parameters only. */
export const ASSETS_BUCKET_NAME = "coderhelm-${Stage}-release-${AWS::AccountId}";

/**
 * A child template's URL. Only parameters and pseudo parameters, never a
 * resource: CloudFormation resolves it while it builds a change set with
 * the nested stacks included (the updater stages the files first).
 */
export function childTemplateUrl(version: string, name: string): string {
  return `https://${ASSETS_BUCKET_NAME}.s3.\${AWS::Region}.amazonaws.com/${childKey(version, name).dest}`;
}

/**
 * The exact log group names the parts declare (every function's and build
 * project's). Final deletes these on a stack delete whatever their tags:
 * Lambda (or CodeBuild) recreates a group when the last logs arrive after
 * CloudFormation deleted it.
 */
export function declaredLogGroups(children: Child[]): string[] {
  const out = new Set<string>();
  const ours = /^\/aws\/(lambda|codebuild)\/coderhelm-\$\{Stage\}-[A-Za-z0-9_.-]+$/;
  for (const c of children) {
    for (const [id, r] of Object.entries((c.template.Resources ?? {}) as Record<string, Json>)) {
      if (r.Type !== "AWS::Logs::LogGroup") continue;
      const name = flatSub(r.Properties?.LogGroupName);
      if (name !== null && ours.test(name)) out.add(name);
      else if (r.Metadata?.["coderhelm:native-log"] || name !== null) {
        throw new Error(`${c.name}.${id}: a declared log group whose name isn't /aws/lambda/coderhelm-\${Stage}-<name> (${JSON.stringify(r.Properties?.LogGroupName)})`);
      }
    }
  }
  return [...out].sort();
}

/** A name made of strings and `Fn::Sub` strings (no other intrinsics) as one Sub string. */
function flatSub(v: Json): string | null {
  if (typeof v === "string") return v;
  if (v && typeof v === "object" && typeof v["Fn::Sub"] === "string") return v["Fn::Sub"];
  if (v && typeof v === "object" && Array.isArray(v["Fn::Join"]) && v["Fn::Join"][0] === "") {
    const parts = (v["Fn::Join"][1] as Json[]).map(flatSub);
    return parts.some((x) => x === null) ? null : parts.join("");
  }
  return null;
}

/** Regions and their Bedrock cross-region inference prefix ("auto"). */
export const BEDROCK_PREFIX: Record<string, string> = {
  "us-east-1": "us.", "us-east-2": "us.", "us-west-1": "us.", "us-west-2": "us.", "ca-central-1": "us.", "ca-west-1": "us.",
  "eu-west-1": "eu.", "eu-west-2": "eu.", "eu-west-3": "eu.", "eu-central-1": "eu.", "eu-central-2": "eu.", "eu-north-1": "eu.",
  "eu-south-1": "eu.", "eu-south-2": "eu.", "ap-northeast-1": "apac.", "ap-northeast-2": "apac.", "ap-northeast-3": "apac.",
  "ap-southeast-1": "apac.", "ap-southeast-2": "apac.", "ap-southeast-3": "apac.", "ap-southeast-4": "apac.", "ap-south-1": "apac.",
  "ap-south-2": "apac.", "ap-east-1": "apac.", "sa-east-1": "global.", "me-central-1": "global.", "me-south-1": "global.",
  "af-south-1": "global.", "il-central-1": "global.", "mx-central-1": "us.",
};

const HOST = "^$|^([a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?\\.)+[a-z]{2,63}$";
const EMAIL = "^[^\\s@]+@[^\\s@]+\\.[^\\s@]+$";

export function parameters(): Json {
  return {
    AdminEmail: { Type: "String", AllowedPattern: EMAIL, MaxLength: 128, ConstraintDescription: "an email address", Description: "You become the first admin. Sign up with this address when the stack is complete." },
    Stage: { Type: "String", Default: "prod", AllowedPattern: "^[a-z][a-z0-9]{0,19}$", ConstraintDescription: "lowercase letters and digits, starting with a letter, at most 20 (no hyphens: coderhelm-prod-* must never also name another install)", Description: "A short name for this install. Resource names start with coderhelm-<name>. Name the stack coderhelm-<name> too." },
    ModelProvider: { Type: "String", Default: "bedrock", AllowedValues: ["bedrock", "anthropic"], Description: "bedrock (Claude on Amazon Bedrock in this account, no API key) or anthropic (each team adds its own key). Change it later in Settings, System." },
    BedrockInferencePrefix: { Type: "String", Default: "auto", AllowedValues: ["auto", "us.", "eu.", "apac.", "global.", "none"], Description: "Bedrock only. auto picks the cross-region inference profile for this region; none uses in-region model ids." },
    EmailTransport: { Type: "String", Default: "ses", AllowedValues: ["ses", "coderhelm"], Description: "ses: this account's Amazon SES. coderhelm: CoderHelm's mail service (hybrid; CoderHelm sees each recipient's address)." },
    EmailFrom: { Type: "String", Default: "", MaxLength: 128, AllowedPattern: `^$|${EMAIL}`, Description: "The address CoderHelm sends email from. Empty uses your email (AWS emails it a link to confirm)." },
    Hostname: { Type: "String", Default: "", AllowedPattern: HOST, Description: "Your own domain, e.g. coderhelm.example.com. Empty for a free CloudFront address. CoderHelm requests the certificate; Settings, System shows any DNS records to add and proposes the stack update that moves CoderHelm to it." },
    SignupDomain: { Type: "String", Default: "", AllowedPattern: HOST, Description: "Empty: only people you invite. Or an email domain whose people may join." },
    RequireTwoFactor: { Type: "String", Default: "off", AllowedValues: ["off", "on"], Description: "on makes everyone set up an authenticator app, you included." },
    AutoUpdate: { Type: "String", Default: "off", AllowedValues: ["off", "on"], Description: "off (the default): CoderHelm prepares each new release as a change set for you to review and execute. on: adds a CloudFormation service role (coderhelm-<name>-deploy) that applies releases in the maintenance window (docs/self-host/security.md)." },
    UpdateDay: { Type: "String", Default: "sun", AllowedValues: ["daily", "mon", "tue", "wed", "thu", "fri", "sat", "sun"], Description: "Auto-update's maintenance window: the day." },
    UpdateHour: { Type: "String", Default: "3", AllowedValues: Array.from({ length: 24 }, (_, i) => String(i)), Description: "The window's hour (0 to 23)." },
    UpdateTimezone: { Type: "String", Default: "UTC", AllowedPattern: "^(UTC|[A-Za-z_]+(/[A-Za-z0-9_+-]+)+)$", Description: "The window's time zone, e.g. Europe/Berlin." },
    KeepDataOnDelete: { Type: "String", Default: "true", AllowedValues: ["true", "false"], Description: "true: deleting this stack keeps the tables, data buckets, keys, user pool and secret. false: deleting it removes them too (keys after AWS's 7-day minimum)." },
    SetupToken: { Type: "String", Default: "", NoEcho: true, AllowedPattern: "^$|^[A-Za-z0-9]{32,64}$", Description: "A one-time key from the install guide that lets it hand your new GitHub App's keys to this install. Empty to create the GitHub App later." },
    StatusId: { Type: "String", Default: "", AllowedPattern: "^$|^[a-z0-9]{16}$", Description: "Lets the install guide show progress (a small public status file with the phase and your URL). Empty for none." },
    DomainCertificateArn: { Type: "String", Default: "", Description: "Set by CoderHelm's stack update once your domain's certificate is issued. Leave empty." },
    DomainZoneId: { Type: "String", Default: "", Description: "Set by CoderHelm's stack update when your domain's zone is in Route 53 here. Leave empty." },
  };
}

function conditions(): Json {
  const not = (p: string, v: string) => ({ "Fn::Not": [{ "Fn::Equals": [{ Ref: p }, v] }] });
  return {
    [KEEP_CONDITION]: { "Fn::Equals": [{ Ref: "KeepDataOnDelete" }, "true"] },
    HasEmailFrom: not("EmailFrom", ""),
    AutoPrefix: { "Fn::Equals": [{ Ref: "BedrockInferencePrefix" }, "auto"] },
    NoPrefix: { "Fn::Equals": [{ Ref: "BedrockInferencePrefix" }, "none"] },
    AutoUpdateOn: { "Fn::Equals": [{ Ref: "AutoUpdate" }, "on"] },
    HasHostname: not("Hostname", ""),
    HasDomainCert: { "Fn::And": [not("Hostname", ""), not("DomainCertificateArn", "")] },
    HasDomainZone: not("DomainZoneId", ""),
    HasStatus: not("StatusId", ""),
    HasSetupToken: not("SetupToken", ""),
  };
}

/** How the parent resolves each value a child may ask for. */
export function installValue(name: string): Json {
  switch (name) {
    case "AdminEmail": return { Ref: "AdminEmail" };
    case "EmailFromResolved": return { "Fn::If": ["HasEmailFrom", { Ref: "EmailFrom" }, { Ref: "AdminEmail" }] };
    case "BedrockPrefixResolved":
      return { "Fn::If": ["AutoPrefix", { "Fn::FindInMap": ["BedrockPrefix", { Ref: "AWS::Region" }, "Prefix"] }, { "Fn::If": ["NoPrefix", "", { Ref: "BedrockInferencePrefix" }] }] };
    case "EmailTransport": return { Ref: "EmailTransport" };
    case "ModelProvider": return { Ref: "ModelProvider" };
    case "UpdateDefaultsJson":
      return { "Fn::Sub": ['{"auto":${Auto},"channel":"stable","day":"${UpdateDay}","hour":${UpdateHour},"timezone":"${UpdateTimezone}","majors":"ask"}', { Auto: { "Fn::If": ["AutoUpdateOn", "true", "false"] } }] };
    case "RootStackName": return { Ref: "AWS::StackName" };
    case "DeployRoleArn": return { "Fn::If": ["AutoUpdateOn", { "Fn::GetAtt": ["DeployRole", "Arn"] }, ""] };
    // The web part's own parameters (infra/lib/web-stack.ts, nativeDomain).
    case "DomainName": return { "Fn::If": ["HasDomainCert", { Ref: "Hostname" }, ""] };
    case "DomainCertificateArn": return { "Fn::If": ["HasDomainCert", { Ref: "DomainCertificateArn" }, ""] };
    case "DomainZoneId": return { "Fn::If": ["HasDomainCert", { Ref: "DomainZoneId" }, ""] };
    default: throw new Error(`no parent value for ${name}`);
  }
}

/** What the helper function may do (docs/self-host/security.md, "The install helper"). */
/** The admin's "delete everything when uninstalled" switch: the updater writes it, the helper reads it on delete. */
export const UNINSTALL_SWITCH = "/coderhelm-${Stage}/uninstall/delete-data";

export function helperPolicy(): PolicyDocument {
  const p = "coderhelm-${Stage}";
  const at = (svc: string) => `arn:\${AWS::Partition}:${svc}:\${AWS::Region}:\${AWS::AccountId}`;
  return {
    Version: "2012-10-17",
    Statement: [
      { Sid: "OwnLog", Effect: "Allow", Action: ["logs:CreateLogStream", "logs:PutLogEvents"], Resource: `${at("logs")}:log-group:/aws/lambda/${p}-install-helper:*` },
      { Sid: "ReleaseFiles", Effect: "Allow", Action: ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket", "s3:ListBucketVersions", "s3:DeleteObjectVersion"], Resource: [`arn:\${AWS::Partition}:s3:::${p}-release-\${AWS::AccountId}`, `arn:\${AWS::Partition}:s3:::${p}-release-\${AWS::AccountId}/*`] },
      { Sid: "EmptyDataBucketsOnDelete", Effect: "Allow", Action: ["s3:ListBucket", "s3:ListBucketVersions", "s3:DeleteObject", "s3:DeleteObjectVersion"], Resource: [`arn:\${AWS::Partition}:s3:::${p}-*`, `arn:\${AWS::Partition}:s3:::${p}-*/*`] },
      { Sid: "StatusFile", Effect: "Allow", Action: ["s3:PutObject", "s3:PutBucketPolicy", "s3:ListBucket", "s3:ListBucketVersions", "s3:DeleteObject", "s3:DeleteObjectVersion"], Resource: ["arn:${AWS::Partition}:s3:::coderhelm-setup-${StatusId}", "arn:${AWS::Partition}:s3:::coderhelm-setup-${StatusId}/*"] },
      // DeleteSecret: only on a stack delete with KeepDataOnDelete=false (Final).
      { Sid: "Secret", Effect: "Allow", Action: ["secretsmanager:GetSecretValue", "secretsmanager:PutSecretValue", "secretsmanager:DeleteSecret"], Resource: `${at("secretsmanager")}:secret:coderhelm/\${Stage}/secrets-??????` },
      { Sid: "AllowlistAndTablesOnDelete", Effect: "Allow", Action: ["dynamodb:BatchWriteItem", "dynamodb:DescribeTable", "dynamodb:DeleteTable", "dynamodb:UpdateTable"], Resource: [`${at("dynamodb")}:table/${p}`, `${at("dynamodb")}:table/${p}-*`] },
      { Sid: "TableKey", Effect: "Allow", Action: ["kms:Encrypt", "kms:Decrypt", "kms:GenerateDataKey*", "kms:DescribeKey"], Resource: "*", Condition: { StringEquals: { [`aws:ResourceTag/${INSTALL_TAG}`]: p } } },
      { Sid: "Domain", Effect: "Allow", Action: "lambda:InvokeFunction", Resource: `${at("lambda")}:function:${p}-updater` },
      { Sid: "RootStack", Effect: "Allow", Action: ["cloudformation:DescribeStacks", "cloudformation:DescribeStackEvents"], Resource: "arn:${AWS::Partition}:cloudformation:${AWS::Region}:${AWS::AccountId}:stack/${AWS::StackName}/*" },
      { Sid: "LogGroupsOnDelete", Effect: "Allow", Action: ["logs:DescribeLogGroups"], Resource: "*" },
      // Only log groups tagged for this install (every one the stack declares is).
      { Sid: "InstallLogGroupsOnDelete", Effect: "Allow", Action: ["logs:DeleteLogGroup", "logs:ListTagsForResource"], Resource: [`${at("logs")}:log-group:/aws/lambda/${p}-*`, `${at("logs")}:log-group:/aws/codebuild/${p}-*`] },
      { Sid: "FindCertificates", Effect: "Allow", Action: ["acm:ListCertificates", "acm:ListTagsForCertificate", "acm:DescribeCertificate"], Resource: "*", Condition: { StringEquals: { "aws:RequestedRegion": "us-east-1" } } },
      // Only the certificates the updater requested for this install (its tag).
      { Sid: "CertificatesOnDelete", Effect: "Allow", Action: "acm:DeleteCertificate", Resource: "*", Condition: { StringEquals: { "aws:RequestedRegion": "us-east-1", "aws:ResourceTag/coderhelm": p } } },
      { Sid: "FindZones", Effect: "Allow", Action: "route53:ListHostedZones", Resource: "*" },
      // The admin's "delete everything when uninstalled" switch (Settings, System, Uninstall), read on a
      // stack delete and removed with the stack.
      { Sid: "UninstallSwitch", Effect: "Allow", Action: ["ssm:GetParameter", "ssm:DeleteParameter"], Resource: `${at("ssm")}:parameter/${p}/uninstall/delete-data` },
      // With the switch on and KeepDataOnDelete=true the stack retains its data: on a stack delete the
      // helper deletes the install's data bucket, its user pool and its keys (7 days) itself.
      { Sid: "DataBucketOnSwitch", Effect: "Allow", Action: "s3:DeleteBucket", Resource: `arn:\${AWS::Partition}:s3:::${p}-data-\${AWS::AccountId}` },
      { Sid: "UserPoolOnSwitch", Effect: "Allow", Action: ["cognito-idp:DescribeUserPool", "cognito-idp:UpdateUserPool", "cognito-idp:DeleteUserPool", "cognito-idp:DeleteUserPoolDomain"], Resource: `${at("cognito-idp")}:userpool/*`, Condition: { StringEquals: { [`aws:ResourceTag/${INSTALL_TAG}`]: p } } },
      { Sid: "FindKeyAliases", Effect: "Allow", Action: "kms:ListAliases", Resource: "*" },
      { Sid: "KeyAliasesOnSwitch", Effect: "Allow", Action: "kms:DeleteAlias", Resource: `${at("kms")}:alias/${p}-*` },
      { Sid: "KeysOnSwitch", Effect: "Allow", Action: ["kms:ScheduleKeyDeletion", "kms:DeleteAlias"], Resource: `${at("kms")}:key/*`, Condition: { StringEquals: { [`aws:ResourceTag/${INSTALL_TAG}`]: p } } },
      // Only deleting the CNAME a certificate check uses (its name starts with an underscore).
      { Sid: "ValidationRecordsOnDelete", Effect: "Allow", Action: "route53:ChangeResourceRecordSets", Resource: "arn:${AWS::Partition}:route53:::hostedzone/*", Condition: { "ForAllValues:StringLike": { "route53:ChangeResourceRecordSetsNormalizedRecordNames": ["_*"] }, "ForAllValues:StringEquals": { "route53:ChangeResourceRecordSetsActions": ["DELETE"], "route53:ChangeResourceRecordSetsRecordTypes": ["CNAME"] } } },
    ],
  };
}

/**
 * The DNS statement only when the install has a zone: with no custom domain
 * DomainZoneId is empty and the statement would name `hostedzone/` (never
 * widened to every zone instead).
 */
export function withZoneOnly(policy: any): any {
  return {
    ...policy,
    Statement: policy.Statement.map((s: any) => (s.Sid === "DnsRecords" ? { "Fn::If": ["HasDomainZone", s, { Ref: "AWS::NoValue" }] } : s)),
  };
}

/** The auto-update role's policy: the deploy policy, the parent stack, the release files. */
export function deployRolePolicy(n: AccessNames): PolicyDocument {
  const base = deployPolicy(n);
  return {
    Version: "2012-10-17",
    Statement: [
      ...base.Statement,
      // The parent by the name the install must have (coderhelm-<name>), never the stack's own name: a
      // root stack named "coderhelm" would otherwise cover every install's stacks.
      { Sid: "ParentStack", Effect: "Allow", Action: ["cloudformation:*"], Resource: ["arn:${AWS::Partition}:cloudformation:${AWS::Region}:${AWS::AccountId}:stack/coderhelm-${Stage}/*", "arn:${AWS::Partition}:cloudformation:${AWS::Region}:${AWS::AccountId}:stack/coderhelm-${Stage}-*/*"] },
      { Sid: "ReleaseFiles", Effect: "Allow", Action: ["s3:GetObject"], Resource: "arn:${AWS::Partition}:s3:::coderhelm-${Stage}-release-${AWS::AccountId}/*" },
      // The install's secret: read and update its settings, never delete it (its value comes from the helper and the gateway).
      { Sid: "InstallSecret", Effect: "Allow", Action: ["secretsmanager:DescribeSecret", "secretsmanager:GetSecretValue", "secretsmanager:UpdateSecret", "secretsmanager:TagResource", "secretsmanager:UntagResource"], Resource: "arn:${AWS::Partition}:secretsmanager:${AWS::Region}:${AWS::AccountId}:secret:coderhelm/${Stage}/secrets-??????" },
      { Sid: "SecretPassword", Effect: "Allow", Action: "secretsmanager:GetRandomPassword", Resource: "*" },
      // The install guide's progress bucket.
      { Sid: "ProgressBucket", Effect: "Allow", Action: "s3:*", Resource: ["arn:${AWS::Partition}:s3:::coderhelm-setup-${StatusId}", "arn:${AWS::Partition}:s3:::coderhelm-setup-${StatusId}/*"] },
    ],
  };
}

const s3Secure = {
  OwnershipControls: { Rules: [{ ObjectOwnership: "BucketOwnerEnforced" }] },
  PublicAccessBlockConfiguration: { BlockPublicAcls: true, BlockPublicPolicy: true, IgnorePublicAcls: true, RestrictPublicBuckets: true },
  BucketEncryption: { ServerSideEncryptionConfiguration: [{ ServerSideEncryptionByDefault: { SSEAlgorithm: "AES256" } }] },
};

export interface ParentInput {
  version: string;
  children: Child[];
  assets: Asset[];
  /** Child templates: name -> sha256 of the published file. */
  childSha: Record<string, string>;
  helperCode: string;
}

export function childKey(version: string, name: string): { key: string; dest: string } {
  return { key: `stacks/${name}.json`, dest: `releases/${version}/stacks/${name}.json` };
}

export function buildParent(input: ParentInput): Json {
  const n = nativeNames();
  const { version, children } = input;
  const names = new Set(children.map((c) => c.name));
  const files = [
    ...children.map((c) => ({ Key: childKey(version, c.name).key, Dest: childKey(version, c.name).dest, Sha256: input.childSha[c.name] })),
    ...input.assets.map((a) => ({ Key: a.key, Dest: a.dest, Sha256: a.sha256 })),
  ];
  const root = { RootStackId: { Ref: "AWS::StackId" } };
  const helper = { ServiceToken: { "Fn::GetAtt": ["Helper", "Arn"] }, ...root };
  const R: Json = {};

  // No AWS::LanguageExtensions here: CloudFormation stores a template with
  // that transform already resolved, so a later "use the current template"
  // update would silently keep every old parameter value (KeepDataOnDelete
  // included). Every policy in this template is a literal.
  const boundaryDoc = subInstall(boundaryPolicy(n, [DEPLOY_BOUNDARY_ARN]));
  R.Boundary = {
    Type: "AWS::IAM::ManagedPolicy",
    // Only roles the stack deletes carry it (the kept auto-update role has its own copy).
    DeletionPolicy: "Delete",
    UpdateReplacePolicy: "Retain",
    Properties: {
      ManagedPolicyName: sub(n.boundaryName),
      Description: "CoderHelm: the most any CoderHelm role can do (docs/self-host/security.md)",
      PolicyDocument: boundaryDoc,
    },
  };
  // The same boundary for the auto-update role. That role is kept on delete,
  // and IAM refuses to delete a policy a role still carries: kept with it,
  // and both listed by the leftover check.
  R.DeployBoundary = {
    Type: "AWS::IAM::ManagedPolicy",
    Condition: "AutoUpdateOn",
    DeletionPolicy: "Retain",
    UpdateReplacePolicy: "Retain",
    Metadata: { "cfn-lint": { config: { ignore_checks: ["W3011"] } } },
    Properties: {
      ManagedPolicyName: sub(DEPLOY_BOUNDARY_NAME),
      Description: "CoderHelm: the auto-update role's boundary, the same as every CoderHelm role's (docs/self-host/security.md)",
      PolicyDocument: boundaryDoc,
    },
  };
  R.AssetsBucket = {
    Type: "AWS::S3::Bucket",
    Properties: { BucketName: sub(ASSETS_BUCKET_NAME), ...s3Secure, Tags: [{ Key: INSTALL_TAG, Value: sub("coderhelm-${Stage}") }] },
  };
  // Only the helper may write or delete the release files (it checks each one).
  R.AssetsBucketPolicy = {
    Type: "AWS::S3::BucketPolicy",
    Properties: {
      Bucket: { Ref: "AssetsBucket" },
      PolicyDocument: {
        Version: "2012-10-17",
        Statement: [{
          Sid: "OnlyTheHelperWritesReleaseFiles", Effect: "Deny", Principal: "*",
          Action: ["s3:PutObject", "s3:DeleteObject", "s3:DeleteObjectVersion", "s3:PutObjectAcl"],
          Resource: { "Fn::Sub": "${AssetsBucket.Arn}/*" },
          Condition: { ArnNotEquals: { "aws:PrincipalArn": { "Fn::GetAtt": ["HelperRole", "Arn"] } } },
        }],
      },
    },
  };
  R.InstallSecret = {
    Type: "AWS::SecretsManager::Secret",
    // Kept by CloudFormation; with KeepDataOnDelete=false, Final deletes it
    // after everything that reads it on delete (HybridLeave, SetupKey).
    DeletionPolicy: "RetainExceptOnCreate",
    UpdateReplacePolicy: "Retain",
    Properties: {
      Name: sub(n.secretName),
      Description: "CoderHelm: GitHub App keys and the session signing key",
      // Never regenerated: the template is fixed, so an update never changes it.
      GenerateSecretString: {
        SecretStringTemplate: JSON.stringify({ github_app_id: "", github_private_key: "", github_webhook_secret: "", github_client_id: "", github_client_secret: "" }),
        GenerateStringKey: "jwt_secret",
        PasswordLength: 64,
        ExcludePunctuation: true,
        ExcludeUppercase: true,
        ExcludeCharacters: "ghijklmnopqrstuvwxyz",
      },
      Tags: [{ Key: INSTALL_TAG, Value: sub("coderhelm-${Stage}") }],
    },
  };
  R.HelperLogs = {
    Type: "AWS::Logs::LogGroup",
    Properties: { LogGroupName: sub("/aws/lambda/coderhelm-${Stage}-install-helper"), RetentionInDays: 30 },
  };
  R.HelperRole = {
    Type: "AWS::IAM::Role",
    Properties: {
      RoleName: sub("coderhelm-${Stage}-install-helper"),
      Description: "CoderHelm: the install's custom resources (copies the release, the setup key, cleanup on delete)",
      PermissionsBoundary: { Ref: "Boundary" },
      AssumeRolePolicyDocument: { Version: "2012-10-17", Statement: [{ Effect: "Allow", Principal: { Service: "lambda.amazonaws.com" }, Action: "sts:AssumeRole" }] },
      Policies: [{ PolicyName: "helper", PolicyDocument: sub(helperPolicy()) }],
      Tags: [{ Key: INSTALL_TAG, Value: sub("coderhelm-${Stage}") }],
    },
  };
  R.Helper = {
    Type: "AWS::Lambda::Function",
    Properties: {
      FunctionName: sub("coderhelm-${Stage}-install-helper"),
      Description: "CoderHelm: the install's custom resources (lambda/native-helper)",
      Runtime: "python3.12",
      Handler: "index.handler",
      Timeout: 900,
      MemorySize: 1024,
      EphemeralStorage: { Size: 2048 },
      Role: { "Fn::GetAtt": ["HelperRole", "Arn"] },
      LoggingConfig: { LogGroup: { Ref: "HelperLogs" } },
      Code: { ZipFile: input.helperCode },
      // stage: the only bucket it copies a release's child templates to.
      Environment: { Variables: { ASSETS_BUCKET: { Ref: "AssetsBucket" } } },
      Tags: [{ Key: INSTALL_TAG, Value: sub("coderhelm-${Stage}") }],
    },
  };
  R.ReleaseAssets = {
    Type: "Custom::ReleaseAssets",
    Properties: { ...helper, Bucket: { Ref: "AssetsBucket" }, BaseUrl: `${RELEASE_BUCKET_URL}/${version}`, Version: version, Files: files },
  };
  R.StatusBucket = {
    Type: "AWS::S3::Bucket",
    Condition: "HasStatus",
    Properties: {
      BucketName: sub("coderhelm-setup-${StatusId}"),
      OwnershipControls: { Rules: [{ ObjectOwnership: "BucketOwnerEnforced" }] },
      PublicAccessBlockConfiguration: { BlockPublicAcls: true, IgnorePublicAcls: true, BlockPublicPolicy: false, RestrictPublicBuckets: false },
      CorsConfiguration: { CorsRules: [{ AllowedMethods: ["GET", "HEAD"], AllowedOrigins: ["*"], MaxAge: 300 }] },
      LifecycleConfiguration: { Rules: [{ Id: "expire-progress", Status: "Enabled", ExpirationInDays: 30 }] },
    },
  };
  const statusProps = {
    StatusBucket: { "Fn::If": ["HasStatus", { Ref: "StatusBucket" }, ""] },
    Version: version,
    Stage: { Ref: "Stage" },
    Region: { Ref: "AWS::Region" },
  };
  const statusOn = { ...statusProps, StatusBucket: { Ref: "StatusBucket" } };
  R.StatusStart = { Type: "Custom::Status", Condition: "HasStatus", Properties: { ...helper, ...statusOn, Phase: "start" } };
  // Deleted after every part (they depend on it): certificates, and logs Lambda made by itself.
  R.Final = {
    Type: "Custom::Final",
    Properties: {
      ...helper,
      Prefix: sub("coderhelm-${Stage}"),
      KeepData: { Ref: "KeepDataOnDelete" },
      StatusBucket: statusProps.StatusBucket,
      LogGroupPrefixes: [sub("/aws/lambda/coderhelm-${Stage}-"), sub("/aws/codebuild/coderhelm-${Stage}-")],
      LogGroupNames: declaredLogGroups(children).map((x) => sub(x)),
      SecretId: { Ref: "InstallSecret" },
      DeleteSwitch: sub(UNINSTALL_SWITCH),
      KeyAliasPrefix: sub("alias/coderhelm-${Stage}-"),
    },
  };
  R.DeployRole = {
    Type: "AWS::IAM::Role",
    Condition: "AutoUpdateOn",
    // A role can't delete itself, and CloudFormation keeps using a stack's
    // role for later operations: kept on delete (the leftover check lists it).
    DeletionPolicy: "Retain",
    UpdateReplacePolicy: "Retain",
    Metadata: { "cfn-lint": { config: { ignore_checks: ["W3037", "W3011"] } } },
    Properties: {
      RoleName: sub(n.deployRoleName),
      Description: "CoderHelm: applies a reviewed release's stack update when auto-update is on (docs/self-host/security.md)",
      AssumeRolePolicyDocument: {
        Version: "2012-10-17",
        Statement: [{
          Effect: "Allow", Principal: { Service: "cloudformation.amazonaws.com" }, Action: "sts:AssumeRole",
          Condition: {
            StringEquals: { "aws:SourceAccount": { Ref: "AWS::AccountId" } },
            StringLike: { "aws:SourceArn": [{ "Fn::Sub": "arn:${AWS::Partition}:cloudformation:${AWS::Region}:${AWS::AccountId}:stack/coderhelm-${Stage}/*" }, { "Fn::Sub": "arn:${AWS::Partition}:cloudformation:${AWS::Region}:${AWS::AccountId}:stack/coderhelm-${Stage}-*/*" }] },
          },
        }],
      },
      PermissionsBoundary: { Ref: "DeployBoundary" },
      Policies: [{ PolicyName: "deploy", PolicyDocument: withZoneOnly(subInstall(deployRolePolicy(n))) }],
      Tags: [{ Key: INSTALL_TAG, Value: sub("coderhelm-${Stage}") }],
    },
  };

  const dataPolicy = { "Fn::If": [KEEP_CONDITION, "RetainExceptOnCreate", "Delete"] };
  const replacePolicy = { "Fn::If": [KEEP_CONDITION, "Retain", "Delete"] };
  for (const c of children) {
    const params: Json = {};
    const declared = Object.keys(c.template.Parameters ?? {});
    for (const p of declared) {
      if (p === "Stage") params.Stage = { Ref: "Stage" };
      else if (p === "AssetsBucket") params.AssetsBucket = { Ref: "AssetsBucket" };
      else if (p === "DataDeletionPolicy") params.DataDeletionPolicy = dataPolicy;
      else if (p === "DataReplacePolicy") params.DataReplacePolicy = replacePolicy;
      else if (c.imports[p]) {
        const src = c.imports[p];
        if (!names.has(src.part)) throw new Error(`${c.name} imports from unknown part ${src.part}`);
        params[p] = { "Fn::GetAtt": [src.part, `Outputs.${src.output}`] };
      } else params[p] = installValue(p);
    }
    const deps = new Set<string>(["ReleaseAssets", "Boundary", "Final", "InstallSecret"]);
    // Parts it reads outputs from already wait for them (GetAtt).
    const fed = new Set(Object.values(c.imports).map((i) => i.part));
    for (const d of c.dependsOn) if (names.has(d) && !fed.has(d)) deps.add(d);
    R[c.name] = {
      Type: "AWS::CloudFormation::Stack",
      DependsOn: [...deps].sort(),
      Properties: {
        TemplateURL: { "Fn::Sub": childTemplateUrl(version, c.name) },
        Parameters: params,
        Tags: [{ Key: INSTALL_TAG, Value: sub("coderhelm-${Stage}") }],
      },
    };
  }

  const list = (flag: "hasTables" | "hasBuckets", out: string) => {
    const parts = children.filter((c) => c[flag]).map((c) => ({ "Fn::GetAtt": [c.name, `Outputs.${out}`] }));
    return parts.length ? { "Fn::Split": [",", { "Fn::Join": [",", parts] }] } : [];
  };
  // Deleted before the parts (it references their data): empties buckets
  // and deletes tables itself on a delete with KeepDataOnDelete=false.
  R.Sweep = {
    Type: "Custom::Sweep",
    Properties: {
      ...helper,
      KeepData: { Ref: "KeepDataOnDelete" },
      DeleteSwitch: sub(UNINSTALL_SWITCH),
      Tables: list("hasTables", "NativeDataTables"),
      Buckets: list("hasBuckets", "NativeDataBuckets"),
      ...(names.has("Api") ? { UserPoolId: { "Fn::GetAtt": ["Api", "Outputs.UserPoolId"] } } : {}),
    },
  };
  if (names.has("Database")) {
    R.Allowlist = {
      Type: "Custom::Allowlist",
      Properties: { ...helper, Table: { "Fn::GetAtt": ["Database", "Outputs.SettingsTableName"] }, Admin: { Ref: "AdminEmail" }, Domain: { Ref: "SignupDomain" }, TwoFactor: { Ref: "RequireTwoFactor" } },
    };
  }
  // Both read the secret on delete: deleted before Final (which may delete the secret).
  R.SetupKey = { Type: "Custom::SetupKey", Condition: "HasSetupToken", DependsOn: ["Final"], Properties: { ...helper, SecretId: { Ref: "InstallSecret" }, Token: { Ref: "SetupToken" } } };
  R.HybridLeave = { Type: "Custom::HybridLeave", DependsOn: ["Final"], Properties: { ...helper, SecretId: { Ref: "InstallSecret" } } };
  if (names.has("Updates")) {
    R.Domain = {
      Type: "Custom::Domain",
      Condition: "HasHostname",
      DependsOn: [...children.map((c) => c.name)].sort(),
      Properties: { ...helper, Hostname: { Ref: "Hostname" }, UpdaterFunction: sub("coderhelm-${Stage}-updater") },
    };
  }
  if (names.has("Web")) {
    R.StatusDone = {
      Type: "Custom::Status",
      Condition: "HasStatus",
      DependsOn: [...children.map((c) => c.name)].filter((x) => x !== "Web").sort(),
      Properties: { ...helper, ...statusOn, Phase: "done", Url: { "Fn::GetAtt": ["Web", "Outputs.PublicUrl"] } },
    };
  }

  const outputs: Json = {
    Version: { Description: "The CoderHelm release this stack runs", Value: version },
    KeepDataOnDelete: { Description: "Whether deleting this stack keeps the data", Value: { Ref: "KeepDataOnDelete" } },
  };
  if (names.has("Web")) {
    outputs.CoderHelm = { Description: "Your CoderHelm. Sign up there with your email.", Value: { "Fn::GetAtt": ["Web", "Outputs.PublicUrl"] } };
    outputs.SignUpUrl = { Description: "The first admin signs up here", Value: { "Fn::GetAtt": ["Web", "Outputs.SignUpUrl"] } };
  }
  outputs.InstallGuide = { Condition: "HasStatus", Description: "Back to the install guide", Value: { "Fn::Sub": "https://docs.coderhelm.com/self-host/install/?id=${StatusId}&region=${AWS::Region}&stage=${Stage}" } };
  if (R.Domain) outputs.Domain = { Condition: "HasHostname", Description: "Your domain's state (Settings, System has the details)", Value: { "Fn::GetAtt": ["Domain", "Message"] } };

  return {
    AWSTemplateFormatVersion: "2010-09-09",
    Description:
      `CoderHelm ${version} (about 20 minutes). One stack holds the whole install: a permissions boundary every CoderHelm role carries, ` +
      "a private bucket with this release's files (each checked against the sha256 in this signed template), the install's secret, " +
      "and the parts as nested stacks. No build job and no standing deploy role (unless you turn auto-update on). " +
      "Update: a stack update to the next release's template. Uninstall: delete this stack (KeepDataOnDelete decides about the data). " +
      "docs.coderhelm.com/self-host/security",
    Metadata: {
      "AWS::CloudFormation::Interface": {
        ParameterGroups: [
          { Label: { default: "You" }, Parameters: ["AdminEmail"] },
          { Label: { default: "Install" }, Parameters: ["Stage", "ModelProvider", "KeepDataOnDelete"] },
          { Label: { default: "Optional: your own domain" }, Parameters: ["Hostname"] },
          { Label: { default: "Who may join" }, Parameters: ["SignupDomain", "RequireTwoFactor"] },
          { Label: { default: "Optional" }, Parameters: ["EmailTransport", "EmailFrom", "BedrockInferencePrefix"] },
          { Label: { default: "Optional: updates" }, Parameters: ["AutoUpdate", "UpdateDay", "UpdateHour", "UpdateTimezone"] },
          { Label: { default: "Filled in by the install guide or by CoderHelm (leave as they are)" }, Parameters: ["StatusId", "SetupToken", "DomainCertificateArn", "DomainZoneId"] },
        ],
      },
      CoderHelm: { version, files: files.length },
    },
    Parameters: parameters(),
    Mappings: { BedrockPrefix: Object.fromEntries(Object.entries(BEDROCK_PREFIX).map(([r, p]) => [r, { Prefix: p }])) },
    Conditions: conditions(),
    Resources: R,
    Outputs: outputs,
  };
}
