/**
 * A hardened install's stacks (config `access`, infra/lib/access.ts):
 *
 * - every stack deploys with the CLI's own credentials and the install's
 *   deploy role (`cdk deploy --role-arn`), its assets in the install's own
 *   bucket: no CDK bootstrap and none of its roles;
 * - every IAM role carries the install's permissions boundary;
 * - every role, function, layer and managed policy is named for the
 *   install (`<prefix>-...`), so the deploy role and the boundary can scope
 *   them by name;
 * - every taggable resource carries `coderhelm:install = <prefix>`, which
 *   scopes the few AWS names by id (KMS keys, user pools, distributions).
 *
 * Without `access` none of this applies and the stacks synthesize exactly
 * as before (infra/test/access.test.ts compares them).
 */
import * as crypto from "crypto";
import * as cdk from "aws-cdk-lib";
import * as iam from "aws-cdk-lib/aws-iam";
import * as s3 from "aws-cdk-lib/aws-s3";
import { Construct, IConstruct } from "constructs";
import { boundaryPolicy, deployPolicy, INSTALL_TAG } from "./access";
import { ResolvedConfig } from "./config";

/** The stack properties every stack of the app gets: its environment, and for a hardened install its synthesizer. */
export function stackBase(cfg: ResolvedConfig): { env: cdk.Environment; synthesizer?: cdk.IStackSynthesizer } {
  const env = { account: cfg.account, region: cfg.region };
  if (!cfg.access) return { env };
  return {
    env,
    synthesizer: new cdk.CliCredentialsStackSynthesizer({
      fileAssetsBucketName: cfg.access.assetsBucket,
      bucketPrefix: "",
    }),
  };
}

/** What a resource type's name property is, for the types the deploy role scopes by name. */
const NAME_PROPERTY: Record<string, { prop: string; max: number }> = {
  "AWS::IAM::Role": { prop: "RoleName", max: 64 },
  "AWS::IAM::ManagedPolicy": { prop: "ManagedPolicyName", max: 128 },
  "AWS::Lambda::Function": { prop: "FunctionName", max: 64 },
  "AWS::Lambda::LayerVersion": { prop: "LayerName", max: 64 },
};

/** `<prefix>-<readable part>-<hash of the construct path>`, within AWS's length. */
export function installName(prefix: string, stackName: string, path: string, max: number): string {
  const hash = crypto.createHash("sha256").update(`${stackName}/${path}`).digest("hex").slice(0, 8);
  const tail = path.split("/").filter((x) => x && x !== "Resource" && x !== "Default").pop() ?? "r";
  const readable = tail.replace(/[^A-Za-z0-9]/g, "").slice(0, Math.max(0, max - prefix.length - hash.length - 2));
  return readable ? `${prefix}-${readable}-${hash}` : `${prefix}-${hash}`;
}

function declaredName(node: cdk.CfnResource, prop: string): unknown {
  const props = (node as unknown as { _cfnProperties?: Record<string, unknown> })._cfnProperties ?? {};
  const camel = prop.charAt(0).toLowerCase() + prop.slice(1);
  return props[prop] ?? (node as unknown as Record<string, unknown>)[camel];
}

class NameForInstall implements cdk.IAspect {
  constructor(private readonly prefix: string) {}
  visit(node: IConstruct): void {
    if (!cdk.CfnResource.isCfnResource(node)) return;
    const spec = NAME_PROPERTY[node.cfnResourceType];
    if (!spec) return;
    const stack = cdk.Stack.of(node);
    // L2 constructs pass a lazy physical name that is often empty.
    const declared = declaredName(node, spec.prop);
    if (declared !== undefined && stack.resolve(declared) !== undefined) return;
    node.addPropertyOverride(spec.prop, installName(this.prefix, stack.stackName, node.node.path, spec.max));
  }
}

/** The boundary, the names and the tag on every stack of a hardened install. */
export function harden(scope: Construct, cfg: ResolvedConfig): void {
  if (!cfg.access) return;
  const boundary = iam.ManagedPolicy.fromManagedPolicyArn(scope, "InstallBoundary", cfg.access.boundaryArn);
  iam.PermissionsBoundary.of(scope).apply(boundary);
  cdk.Aspects.of(scope).add(new NameForInstall(cfg.prefix), { priority: cdk.AspectPriority.MUTATING });
  cdk.Tags.of(scope).add(INSTALL_TAG, cfg.prefix);
}

/**
 * The boundary, the deploy role and the assets bucket of an install an
 * admin hardened from the CLI (`coderhelm-selfhost access harden`).
 * Deployed only with the admin's own credentials: updates never change it
 * (the boundary and the deploy role's policy deny it).
 */
export class AccessStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: { cfg: ResolvedConfig }) {
    const cfg = props.cfg;
    const a = cfg.access!;
    super(scope, id, {
      env: { account: cfg.account, region: cfg.region },
      // No assets and no bootstrap: the template goes to CloudFormation inline.
      synthesizer: new cdk.BootstraplessSynthesizer(),
      description: `CoderHelm ${cfg.stage}: the permissions boundary, deploy role and assets bucket (changed only by an admin)`,
    });
    const boundary = new iam.CfnManagedPolicy(this, "Boundary", {
      managedPolicyName: a.boundaryName,
      description: "CoderHelm: the most any CoderHelm role can do (docs/self-host/security.md)",
      policyDocument: boundaryPolicy(a),
    });
    boundary.applyRemovalPolicy(cdk.RemovalPolicy.RETAIN);
    const role = new iam.CfnRole(this, "DeployRole", {
      roleName: a.deployRoleName,
      description: "CoderHelm: CloudFormation's role for this install's stacks",
      assumeRolePolicyDocument: {
        Version: "2012-10-17",
        Statement: [{ Effect: "Allow", Principal: { Service: "cloudformation.amazonaws.com" }, Action: "sts:AssumeRole" }],
      },
      permissionsBoundary: a.boundaryArn,
      policies: [{ policyName: "deploy", policyDocument: deployPolicy(a) }],
    });
    role.addDependency(boundary);
    role.applyRemovalPolicy(cdk.RemovalPolicy.RETAIN);
    const bucket = new s3.CfnBucket(this, "Assets", {
      bucketName: a.assetsBucket,
      publicAccessBlockConfiguration: { blockPublicAcls: true, blockPublicPolicy: true, ignorePublicAcls: true, restrictPublicBuckets: true },
      bucketEncryption: { serverSideEncryptionConfiguration: [{ serverSideEncryptionByDefault: { sseAlgorithm: "AES256" } }] },
      ownershipControls: { rules: [{ objectOwnership: "BucketOwnerEnforced" }] },
      lifecycleConfiguration: { rules: [{ id: "old-assets", status: "Enabled", noncurrentVersionExpiration: { noncurrentDays: 30 }, abortIncompleteMultipartUpload: { daysAfterInitiation: 1 } }] },
      tags: [{ key: INSTALL_TAG, value: cfg.prefix }],
    });
    bucket.applyRemovalPolicy(cdk.RemovalPolicy.RETAIN);
  }
}
