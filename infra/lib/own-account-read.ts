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

type Statement = { Sid?: string; Effect: string; Action: string | string[]; Resource: unknown };

/** The update template's statements, as the template has them. */
export function updateTemplateStatements(): Statement[] {
  const scratch = new cdk.Stack(new cdk.App(), "Scratch");
  const inc = new CfnInclude(scratch, "T", { templateFile: repoPath(UPDATE_TEMPLATE), parameters: { RoleName: ROLE_NAME_MARK } });
  const p = inc.getResource(UPDATE_POLICY_ID) as iam.CfnPolicy;
  return scratch.resolve(p.policyDocument).Statement as Statement[];
}

/**
 * The statements for `roleArn`: the template's, with the one statement that
 * names the role (the permission check on itself) pointed at `roleArn`.
 */
export function ownAccountStatements(roleArn: string): Statement[] {
  return updateTemplateStatements().map((s) =>
    JSON.stringify(s.Resource).includes(ROLE_NAME_MARK) ? { ...s, Resource: roleArn } : s,
  );
}

/** Gives `fn`'s role this release's read-only permissions in its own account, and tells it so. */
export function grantOwnAccountRead(scope: Construct, id: string, fn: cdk.aws_lambda.Function): void {
  const role = fn.role!;
  new iam.CfnManagedPolicy(scope, id, {
    description: "CoderHelm: reads this install's own AWS account (logs, metrics, Airflow, ECS), read-only; kept current by each version update",
    roles: [role.roleName],
    policyDocument: { Version: "2012-10-17", Statement: ownAccountStatements(role.roleArn) },
  });
  fn.addEnvironment(OWN_ROLE_ENV, role.roleArn);
}
