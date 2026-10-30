/**
 * The Launch Stack's access resources (infra/cfn/coderhelm-selfhost-launch.yaml):
 * the permissions boundary, the deploy role, the assets bucket and the
 * deploy window's policy, from the same functions the CDK app uses
 * (infra/lib/access.ts). infra/test/launch-stack.test.ts checks the template
 * holds exactly these; after changing access.ts, regenerate the block with
 *
 *   npm run build && node dist/lib/launch-access.js
 *
 * and paste it over the template's "Generated from infra/lib/access.ts" block.
 *
 * Each launch names its pieces with its own short id (the first part of the
 * stack's id), and keeps them when the stack is deleted or rolled back:
 * CoderHelm keeps running and updating after the Launch Stack is gone, and
 * a second launch after a failed one never collides with the first's.
 */
import { accessNames, AccessNames, boundaryPolicy, deployPolicy, INSTALL_TAG, windowPolicyParts } from "./access";

/** The launch's short id: the first 8 characters of the stack's UUID. */
export const LAUNCH_ID = {
  "Fn::Select": [0, { "Fn::Split": ["-", { "Fn::Select": [2, { "Fn::Split": ["/", { Ref: "AWS::StackId" }] }] }] }],
};

/** The names, as CloudFormation substitutions. */
export function launchNames(): AccessNames {
  return accessNames({
    partition: "${AWS::Partition}",
    account: "${AWS::AccountId}",
    region: "${AWS::Region}",
    prefix: "coderhelm-${Stage}",
    suffix: "-${AWS::AccountId}",
    secretName: "coderhelm/${Stage}/secrets",
    id: "${Id}",
  });
}

/** A string with substitutions becomes Fn::Sub (with the launch id); objects and arrays recursively. */
export function sub(v: unknown): unknown {
  if (typeof v === "string") {
    if (!v.includes("${")) return v;
    return v.includes("${Id}") ? { "Fn::Sub": [v, { Id: LAUNCH_ID }] } : { "Fn::Sub": v };
  }
  if (Array.isArray(v)) return v.map(sub);
  if (v && typeof v === "object") return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, sub(x)]));
  return v;
}

/** Window policy parts sized so they fit 4 KB once the longest names are substituted. */
export const LAUNCH_WINDOW_PART_MAX = 3200;

export function launchAccessResources(): Record<string, unknown> {
  const n = launchNames();
  const keep = { DeletionPolicy: "Retain", UpdateReplacePolicy: "Retain" };
  const out: Record<string, unknown> = {
    Boundary: {
      Type: "AWS::IAM::ManagedPolicy",
      ...keep,
      Properties: {
        ManagedPolicyName: sub(n.boundaryName),
        Description: "CoderHelm: the most any CoderHelm role can do (docs/self-host/security.md)",
        PolicyDocument: sub(boundaryPolicy(n)),
      },
    },
    DeployRole: {
      Type: "AWS::IAM::Role",
      ...keep,
      // W3037: cfn-lint's action list lacks apigateway:TagResource and
      // UntagResource, which AWS checks when CloudFormation tags an HTTP API
      // or its stage (a real install was refused without them).
      Metadata: { "cfn-lint": { config: { ignore_checks: ["W3037"] } } },
      Properties: {
        RoleName: sub(n.deployRoleName),
        Description: "CoderHelm: CloudFormation's role for this install's stacks",
        AssumeRolePolicyDocument: {
          Version: "2012-10-17",
          Statement: [{ Effect: "Allow", Principal: { Service: "cloudformation.amazonaws.com" }, Action: "sts:AssumeRole" }],
        },
        PermissionsBoundary: { Ref: "Boundary" },
        Policies: [{ PolicyName: "deploy", PolicyDocument: sub(deployPolicy(n)) }],
      },
    },
    AssetsBucket: {
      Type: "AWS::S3::Bucket",
      ...keep,
      Properties: {
        BucketName: sub(n.assetsBucket),
        OwnershipControls: { Rules: [{ ObjectOwnership: "BucketOwnerEnforced" }] },
        PublicAccessBlockConfiguration: { BlockPublicAcls: true, BlockPublicPolicy: true, IgnorePublicAcls: true, RestrictPublicBuckets: true },
        BucketEncryption: { ServerSideEncryptionConfiguration: [{ ServerSideEncryptionByDefault: { SSEAlgorithm: "AES256" } }] },
        Tags: [{ Key: INSTALL_TAG, Value: sub("coderhelm-${Stage}") }],
      },
    },
  };
  windowPolicyParts(n, LAUNCH_WINDOW_PART_MAX).forEach((value, i) => {
    out[`WindowPolicy${i + 1}`] = {
      Type: "AWS::SSM::Parameter",
      Properties: {
        Name: sub(`/coderhelm-\${Stage}/launch-\${Id}/window-policy/${i + 1}`),
        Description: "CoderHelm: the deploy window the installer gets while it runs (infra/lib/access.ts)",
        Type: "String",
        Value: sub(value),
      },
    };
  });
  return out;
}

/** The template block (JSON is YAML; each resource on its own lines). */
export function launchAccessYaml(): string {
  const lines = ["  # ── Generated from infra/lib/access.ts (infra/lib/launch-access.ts): do not edit by hand ──"];
  for (const [id, r] of Object.entries(launchAccessResources())) {
    lines.push(`  ${id}:`);
    for (const [k, v] of Object.entries(r as Record<string, unknown>)) {
      const text = typeof v === "string" ? v : JSON.stringify(v, null, 2).split("\n").join("\n      ");
      lines.push(`    ${k}: ${text}`);
    }
  }
  lines.push("  # ── End of the generated block ──");
  return lines.join("\n");
}

if (require.main === module) process.stdout.write(launchAccessYaml() + "\n");
