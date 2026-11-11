/**
 * A self-hosted install reading its own AWS account (logs, metrics, Airflow,
 * ECS and the rest): its gateway and worker read with their own roles, and
 * this stack gives those roles the read-only permissions of the release.
 * Every version update brings them along, so nobody launches an extra stack
 * to grant the install what its own release needs.
 *
 * The permissions are the update template's (infra/cfn/coderhelm-log-reader-update.yaml),
 * read from that file: the same one the services check a role against
 * (services/common/src/log_reader.rs), so the two can't drift apart.
 *
 * Connecting the account still proves the team's own CoderHelmLogReader role
 * with its private ID (routes/log_analyzer.rs); only reading uses the
 * install's role (services/common/src/live/aws.rs, install_target).
 *
 * Bedrock knowledge bases connected in place (services/common/src/knowledge/remote.rs):
 * the install is the customer's alone, so its role may search every knowledge
 * base in its own account (the template's KnowledgeBases "all"), and list them
 * for the picker. Admins still choose which ones a team connects. Retrieve also
 * needs the permissions boundary to allow bedrock:Retrieve, which reaches an
 * install only through the access stack an AWS admin applies (native/access-stack.ts).
 *
 * Copying a team's chosen knowledge bases into Bedrock (services/common/src/knowledge/bedrock_sync.rs)
 * never uses the install's own role to write a knowledge base: the template's
 * SyncKnowledgeBases is empty here, so this policy and the permissions boundary stay as
 * they were. A self-hosted install copies into an S3 folder its own stack lets it write
 * (infra/cfn/coderhelm-bedrock-export.yaml, a bucket named with the install's prefix),
 * and that stack's own schedule starts the data source's syncs.
 *
 * Infrastructure vulnerabilities (services/worker/src/passes/infra_inventory.rs),
 * Amazon Inspector findings and AWS Health events: each follows a stack
 * parameter of the same name as the role template's (InfrastructureVulnerabilities,
 * on by default; InspectorFindings and HealthEvents, off by default). A native
 * install's parent declares them (native/parent.ts) and an admin switches them
 * from the AWS card: the updater makes a change set that changes only those
 * parameters on the running release (lambda/updater, set_options), so the
 * policy statements come and go without a version update. Like
 * bedrock:Retrieve, the boundary allows these reads only through the access
 * stack an AWS admin applies.
 *
 * Knowledge imports from S3 (services/common/src/knowledge/sources/s3.rs) never
 * use the install's own role: the template's ImportBuckets is empty here, so
 * the role gains no read on the account's buckets. An import reads a bucket
 * with the team's CoderHelmLogReader role in that account, whose stack names
 * every bucket an admin chose.
 */
import * as cdk from "aws-cdk-lib";
import * as iam from "aws-cdk-lib/aws-iam";
import { CfnInclude } from "aws-cdk-lib/cloudformation-include";
import { Construct } from "constructs";
import { repoPath } from "./paths";

/** The environment variable naming the function's own role (live/aws.rs, OWN_ROLE_ENV). */
export const OWN_ROLE_ENV = "INSTALL_OWN_ROLE_ARN";
const UPDATE_TEMPLATE = "infra/cfn/coderhelm-log-reader-update.yaml";
const UPDATE_POLICY_ID = "CoderHelmReadOnlyUpdate";
const ROLE_NAME_MARK = "__CODERHELM_ROLE__";
/** The install's own role searches every knowledge base in its own account. */
const OWN_KNOWLEDGE_BASES = "all";
/** The template's conditions for that value. A condition not listed here fails the build. */
const OWN_CONDITIONS: Record<string, boolean> = {
  ReadsKnowledgeBases: true,
  ReadsEveryKnowledgeBase: true,
  ReadsImportBuckets: false,
  SyncsKnowledgeBases: false,
  SyncsEveryKnowledgeBase: false,
};

/**
 * The optional reads: the role template's condition, the stack parameter
 * that switches it (the same name as the template's), its default, and the
 * condition the install's own stack declares for it. Kept as conditions in
 * the policy, so changing a parameter adds or removes exactly that statement.
 */
export const OWN_OPTIONS = [
  // Describe and list calls only. On by default, as before it was a switch.
  { condition: "ReadsInfrastructure", parameter: "InfrastructureVulnerabilities", default: "on", own: "OwnReadsInfrastructure" },
  // Billed by AWS (Inspector itself; CoderHelm only reads its findings).
  { condition: "ReadsInspector", parameter: "InspectorFindings", default: "off", own: "OwnReadsInspector" },
  // The Health API needs AWS Business Support or higher.
  { condition: "ReadsHealth", parameter: "HealthEvents", default: "off", own: "OwnReadsHealth" },
] as const;

/** The parameter names, in the parent and in each part that reads the install's own account. */
export const OWN_OPTION_PARAMETERS: string[] = OWN_OPTIONS.map((o) => o.parameter);

type Statement = { Sid?: string; Effect: string; Action: string | string[]; Resource: unknown };

/**
 * A policy's statements with every statement-level `Fn::If` opened up into
 * the statements it may add (a switch's statement counts as given): what a
 * check of "every action any role may get" must read.
 */
export function plainStatements(statements: unknown): any[] {
  const list = Array.isArray(statements) ? statements : [statements];
  return list.flatMap((s: any) => {
    if (s && typeof s === "object" && Array.isArray(s["Fn::If"])) {
      const [, yes, no] = s["Fn::If"];
      return [...plainStatements(yes), ...plainStatements(no)];
    }
    if (!s || (typeof s === "object" && s.Ref === "AWS::NoValue")) return [];
    return [s];
  });
}

/**
 * `v` with every `Fn::If` decided by `conditions` and every `AWS::NoValue`
 * dropped, as CloudFormation would for those values (CfnInclude keeps them,
 * and the policy is written into another stack that has no such conditions).
 */
export function decideConditions(v: unknown, conditions: Record<string, boolean>, kept: Record<string, string> = {}): unknown {
  const noValue = (x: unknown) => !!x && typeof x === "object" && (x as any).Ref === "AWS::NoValue";
  if (Array.isArray(v)) return v.map((x) => decideConditions(x, conditions, kept)).filter((x) => !noValue(x));
  if (v && typeof v === "object") {
    const o = v as Record<string, unknown>;
    if (Object.keys(o).length === 1 && Array.isArray(o["Fn::If"])) {
      const [name, yes, no] = o["Fn::If"] as [string, unknown, unknown];
      // A switch the install's stack keeps: the same Fn::If, on its own condition.
      if (name in kept) return { "Fn::If": [kept[name], decideConditions(yes, conditions, kept), noValue(no) ? no : decideConditions(no, conditions, kept)] };
      if (!(name in conditions)) throw new Error(`${UPDATE_TEMPLATE}: condition ${name} isn't decided for the install's own role (own-account-read.ts)`);
      return decideConditions(conditions[name] ? yes : no, conditions, kept);
    }
    return Object.fromEntries(Object.entries(o).map(([k, x]) => [k, decideConditions(x, conditions, kept)]).filter(([, x]) => !noValue(x)));
  }
  return v;
}

/**
 * The update template's statements for the install's own role: its knowledge
 * bases are every one in its account, and each optional read stays an
 * `Fn::If` on the install stack's own condition (OWN_OPTIONS).
 */
export function updateTemplateStatements(): Statement[] {
  const scratch = new cdk.Stack(new cdk.App(), "Scratch");
  const inc = new CfnInclude(scratch, "T", { templateFile: repoPath(UPDATE_TEMPLATE), parameters: { RoleName: ROLE_NAME_MARK, KnowledgeBases: OWN_KNOWLEDGE_BASES, ImportBuckets: "", SyncKnowledgeBases: "", InfrastructureVulnerabilities: "on", InspectorFindings: "on", HealthEvents: "on" } });
  const p = inc.getResource(UPDATE_POLICY_ID) as iam.CfnPolicy;
  const kept = Object.fromEntries(OWN_OPTIONS.map((o) => [o.condition, o.own]));
  return decideConditions(scratch.resolve(p.policyDocument).Statement, OWN_CONDITIONS, kept) as Statement[];
}

/**
 * The stack's switches for the optional reads (once per stack): a parameter
 * per read with its default, and the condition the policy's statements use.
 */
export function ownOptionConditions(stack: cdk.Stack): void {
  for (const o of OWN_OPTIONS) {
    if (stack.node.tryFindChild(o.parameter)) continue;
    const p = new cdk.CfnParameter(stack, o.parameter, {
      type: "String",
      default: o.default,
      allowedValues: ["off", "on"],
      description: `CoderHelm: ${o.parameter} for this install's own account (Settings, Integrations, AWS)`,
    });
    new cdk.CfnCondition(stack, o.own, { expression: cdk.Fn.conditionEquals(p.valueAsString, "on") });
  }
}

/**
 * The statements for `roleArn`: the template's, with the one statement that
 * names the role (the permission check on itself) pointed at `roleArn`.
 */
export function ownAccountStatements(roleArn: string): Statement[] {
  return updateTemplateStatements().map((s) =>
    s.Resource !== undefined && JSON.stringify(s.Resource).includes(ROLE_NAME_MARK) ? { ...s, Resource: roleArn } : s,
  );
}

/** Gives `fn`'s role this release's read-only permissions in its own account, and tells it so. */
export function grantOwnAccountRead(scope: Construct, id: string, fn: cdk.aws_lambda.Function): void {
  const role = fn.role!;
  ownOptionConditions(cdk.Stack.of(scope));
  // The description is frozen as released (test/fixtures/released-managed-policies.json).
  // Changing it replaces the policy, the replacement keeps the generated name,
  // and every install's update fails with "already exists". What the policy
  // covers now (Bedrock knowledge bases too) is in the statements, not here.
  new iam.CfnManagedPolicy(scope, id, {
    description: "CoderHelm: reads this install's own AWS account (logs, metrics, Airflow, ECS), read-only; kept current by each version update",
    roles: [role.roleName],
    policyDocument: { Version: "2012-10-17", Statement: ownAccountStatements(role.roleArn) },
  });
  fn.addEnvironment(OWN_ROLE_ENV, role.roleArn);
}
