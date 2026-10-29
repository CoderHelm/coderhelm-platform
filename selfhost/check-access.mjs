#!/usr/bin/env node
// Checks, read-only, that a hardened CoderHelm install holds no broad standing
// rights in its AWS account (docs/self-host/security.md). It only reads: IAM
// roles and policies, CloudFormation stacks, one SSM parameter, and IAM's
// policy simulator (iam:SimulatePrincipalPolicy evaluates policies; it changes
// nothing and starts nothing).
//
//   AWS_PROFILE=<admin profile> node selfhost/check-access.mjs --stage prod --region us-east-1
//
// Prints a PASS/FAIL table and exits 1 when anything fails. Needs the AWS CLI
// v2 and read access to IAM, CloudFormation and SSM.
import { execFileSync } from "node:child_process";

const args = Object.fromEntries(process.argv.slice(2).join(" ").split(/\s*--/).filter(Boolean).map((a) => a.trim().split(/\s+/)));
const stage = String(args.stage ?? "prod");
const region = String(args.region ?? process.env.AWS_REGION ?? process.env.AWS_DEFAULT_REGION ?? "");
if (!/^[a-z][a-z0-9-]{0,19}$/.test(stage) || !region) {
  console.error("Usage: node selfhost/check-access.mjs --stage <install name> --region <region>");
  process.exit(2);
}
const prefix = `coderhelm-${stage}`;

function aws(...a) {
  try {
    const out = execFileSync("aws", [...a, "--output", "json", "--region", region], { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] });
    return out.trim() ? JSON.parse(out) : {};
  } catch (e) {
    const err = new Error(String(e.stderr || e.message).trim().split("\n").pop());
    err.code = /NoSuchEntity/.test(String(e.stderr)) ? "NoSuchEntity" : "Error";
    throw err;
  }
}

const results = [];
const check = (name, ok, detail = "") => results.push({ name, ok, detail });

function pages(fn) {
  const out = [];
  let marker;
  do {
    const r = fn(marker);
    out.push(r);
    marker = r.Marker ?? r.NextToken;
  } while (marker);
  return out;
}

const decode = (d) => (typeof d === "string" ? JSON.parse(decodeURIComponent(d)) : d);
const statements = (doc) => [decode(doc).Statement].flat();
const wildcard = (doc) => statements(doc).some((s) => s.Effect === "Allow" && [s.Action].flat().some((x) => x === "*" || x === "*:*") && [s.Resource].flat().includes("*"));

// ── The install ──────────────────────────────────────────────────────────

const identity = aws("sts", "get-caller-identity");
const account = identity.Account;
let config = null;
try {
  config = JSON.parse(aws("ssm", "get-parameter", "--name", `/${prefix}/installer/config`).Parameter.Value);
} catch (e) {
  check("the install exists", false, `no /${prefix}/installer/config in ${region}: ${e.message}`);
}
const access = config?.access;
check("the install is hardened (config access)", access?.mode === "hardened", access ? `${access.owner}${access.id ? ` ${access.id}` : ""}` : "not hardened: it deploys through the CDK bootstrap roles");
const tail = access?.owner === "launch" && access.id ? `-${access.id}` : "";
const boundaryArn = `arn:aws:iam::${account}:policy/${prefix}-boundary${tail}`;
const deployRole = `${prefix}-deploy${tail}`;
const deployRoleArn = `arn:aws:iam::${account}:role/${deployRole}`;

// ── Every CoderHelm role ─────────────────────────────────────────────────

const roles = pages((m) => aws("iam", "list-roles", ...(m ? ["--starting-token", m] : []))).flatMap((p) => p.Roles ?? []).filter((r) => r.RoleName.startsWith(`${prefix}-`));
check("CoderHelm roles found", roles.length > 0, `${roles.length} roles named ${prefix}-*`);
const noBoundary = [];
const admin = [];
const star = [];
for (const r of roles) {
  const full = aws("iam", "get-role", "--role-name", r.RoleName).Role;
  if (full.PermissionsBoundary?.PermissionsBoundaryArn !== boundaryArn) noBoundary.push(`${r.RoleName} (${full.PermissionsBoundary?.PermissionsBoundaryArn ?? "none"})`);
  const attached = aws("iam", "list-attached-role-policies", "--role-name", r.RoleName).AttachedPolicies ?? [];
  for (const p of attached) {
    if (/AdministratorAccess|PowerUserAccess|IAMFullAccess/.test(p.PolicyArn)) admin.push(`${r.RoleName}: ${p.PolicyArn}`);
    if (!p.PolicyArn.startsWith("arn:aws:iam::aws:")) {
      const pol = aws("iam", "get-policy", "--policy-arn", p.PolicyArn).Policy;
      const v = aws("iam", "get-policy-version", "--policy-arn", p.PolicyArn, "--version-id", pol.DefaultVersionId).PolicyVersion;
      if (wildcard(v.Document)) star.push(`${r.RoleName}: ${p.PolicyArn}`);
    }
  }
  for (const name of aws("iam", "list-role-policies", "--role-name", r.RoleName).PolicyNames ?? []) {
    const doc = aws("iam", "get-role-policy", "--role-name", r.RoleName, "--policy-name", name).PolicyDocument;
    if (wildcard(doc)) star.push(`${r.RoleName}: inline ${name}`);
  }
}
check("every CoderHelm role carries the boundary", noBoundary.length === 0, noBoundary.join(", ") || boundaryArn);
check("no role has AdministratorAccess (or PowerUser, IAMFullAccess)", admin.length === 0, admin.join(", "));
check("no policy allows *:* on *", star.length === 0, star.join(", "));

// ── The deploy role and the CDK bootstrap ────────────────────────────────

try {
  const d = aws("iam", "get-role", "--role-name", deployRole).Role;
  const trust = statements(d.AssumeRolePolicyDocument);
  const onlyCfn = trust.length === 1 && trust[0].Effect === "Allow" && JSON.stringify(trust[0].Principal) === JSON.stringify({ Service: "cloudformation.amazonaws.com" }) && [trust[0].Action].flat().join() === "sts:AssumeRole";
  check("the deploy role's trust allows CloudFormation only", onlyCfn, JSON.stringify(trust));
} catch (e) {
  check("the deploy role's trust allows CloudFormation only", false, `${deployRole}: ${e.message}`);
}
const stacks = pages((m) => aws("cloudformation", "describe-stacks", ...(m ? ["--next-token", m] : []))).flatMap((p) => p.Stacks ?? []).filter((s) => s.StackName.startsWith(`${prefix}-`) && s.StackName !== `${prefix}-access`);
const otherRole = stacks.filter((s) => s.RoleARN !== deployRoleArn).map((s) => `${s.StackName} (${s.RoleARN ?? "none"})`);
check("every CoderHelm stack deploys through the deploy role", stacks.length > 0 && otherRole.length === 0, otherRole.join(", ") || `${stacks.length} stacks`);
const cdkRoles = pages((m) => aws("iam", "list-roles", ...(m ? ["--starting-token", m] : []))).flatMap((p) => p.Roles ?? []).filter((r) => /^cdk-[a-z0-9]+-(deploy|cfn-exec|file-publishing|image-publishing|lookup)-role-/.test(r.RoleName));
const cdkUsed = stacks.filter((s) => /role\/cdk-/.test(s.RoleARN ?? "")).map((s) => s.StackName);
check("the CDK bootstrap roles are not used by CoderHelm", cdkUsed.length === 0, cdkRoles.length ? `${cdkRoles.length} cdk-* roles exist in the account (not CoderHelm's); used by: ${cdkUsed.join(", ") || "no CoderHelm stack"}` : "no cdk-* roles in the account");

// ── Idle build roles: no deploy rights, no window ────────────────────────

const builds = roles.filter((r) => r.RoleName === `${prefix}-update-build` || r.RoleName.startsWith(`${prefix}-installer-`));
check("build roles found", builds.length > 0, builds.map((r) => r.RoleName).join(", "));
const deployActions = [
  ["cloudformation:CreateChangeSet", `arn:aws:cloudformation:${region}:${account}:stack/${prefix}-api/*`],
  ["cloudformation:UpdateStack", `arn:aws:cloudformation:${region}:${account}:stack/${prefix}-api/*`],
  ["iam:PassRole", deployRoleArn],
  ["iam:CreateRole", `arn:aws:iam::${account}:role/${prefix}-check`],
  ["iam:PutRolePolicy", `arn:aws:iam::${account}:role/${prefix}-update-build`],
  ["iam:CreateUser", `arn:aws:iam::${account}:user/${prefix}-check`],
  ["sts:AssumeRole", `arn:aws:iam::${account}:role/cdk-hnb659fds-deploy-role-${account}-${region}`],
  ["s3:PutObject", `arn:aws:s3:::${prefix}-assets${tail}-${account}/check`],
  ["secretsmanager:GetSecretValue", `arn:aws:secretsmanager:${region}:${account}:secret:coderhelm/${stage}/secrets-AbCdEf`],
  ["lambda:UpdateFunctionCode", `arn:aws:lambda:${region}:${account}:function:${prefix}-gateway`],
];
for (const r of builds) {
  const allowedNow = [];
  for (const [action, resource] of deployActions) {
    const sim = aws("iam", "simulate-principal-policy", "--policy-source-arn", r.Arn, "--action-names", action, "--resource-arns", resource,
      "--context-entries", `ContextKeyName=iam:PermissionsBoundary,ContextKeyValues=${boundaryArn},ContextKeyType=string`,
      `ContextKeyName=cloudformation:RoleArn,ContextKeyValues=${deployRoleArn},ContextKeyType=string`,
      `ContextKeyName=aws:ResourceAccount,ContextKeyValues=${account},ContextKeyType=string`);
    for (const e of sim.EvaluationResults ?? []) if (e.EvalDecision === "allowed") allowedNow.push(action);
  }
  check(`${r.RoleName}: no deploy rights while idle (policy simulator)`, allowedNow.length === 0, allowedNow.join(", ") || `${deployActions.length} deploy actions denied`);
  let window = false;
  try {
    aws("iam", "get-role-policy", "--role-name", r.RoleName, "--policy-name", "deploy-window");
    window = true;
  } catch (e) {
    if (e.code !== "NoSuchEntity") throw e;
  }
  const running = stacks.some((s) => /_IN_PROGRESS$/.test(s.StackStatus));
  check(`${r.RoleName}: no deploy window while idle`, !window, window ? (running ? "a window is open and a stack is changing: an update is running, check again after it" : "a window is open with nothing running") : "none");
}

// ── The deploy role itself (simulator) ───────────────────────────────────

const deny = [
  ["iam:CreateUser", `arn:aws:iam::${account}:user/check`, []],
  ["iam:CreateRole", `arn:aws:iam::${account}:role/${prefix}-check`, []],
  ["iam:CreateRole", `arn:aws:iam::${account}:role/not-coderhelm`, [`ContextKeyName=iam:PermissionsBoundary,ContextKeyValues=${boundaryArn},ContextKeyType=string`]],
  ["iam:DeleteRolePermissionsBoundary", `arn:aws:iam::${account}:role/${prefix}-update-build`, []],
  ["iam:CreatePolicyVersion", boundaryArn, []],
  ["iam:PassRole", `arn:aws:iam::${account}:role/OrganizationAccountAccessRole`, []],
  ["organizations:LeaveOrganization", "*", []],
  ["cloudtrail:StopLogging", `arn:aws:cloudtrail:${region}:${account}:trail/check`, []],
  ["kms:ScheduleKeyDeletion", `arn:aws:kms:${region}:${account}:key/00000000-0000-0000-0000-000000000000`, []],
];
const allow = [
  ["iam:CreateRole", `arn:aws:iam::${account}:role/${prefix}-check`, [`ContextKeyName=iam:PermissionsBoundary,ContextKeyValues=${boundaryArn},ContextKeyType=string`]],
  ["lambda:UpdateFunctionCode", `arn:aws:lambda:${region}:${account}:function:${prefix}-gateway`, []],
];
const sim = (action, resource, ctx) =>
  aws("iam", "simulate-principal-policy", "--policy-source-arn", deployRoleArn, "--action-names", action, "--resource-arns", resource, ...(ctx.length ? ["--context-entries", ...ctx] : [])).EvaluationResults?.[0]?.EvalDecision;
try {
  const wrong = deny.filter(([a, r, c]) => sim(a, r, c) === "allowed").map(([a, r]) => `${a} on ${r}`);
  check("the deploy role is refused IAM users, roles without the boundary, the boundary, other roles, Organizations, CloudTrail, key deletion", wrong.length === 0, wrong.join(", ") || `${deny.length} denied`);
  const missing = allow.filter(([a, r, c]) => sim(a, r, c) !== "allowed").map(([a, r]) => `${a} on ${r}`);
  check("the deploy role can still deploy CoderHelm", missing.length === 0, missing.join(", ") || `${allow.length} allowed`);
} catch (e) {
  check("the deploy role (simulator)", false, e.message);
}

// ── The table ────────────────────────────────────────────────────────────

const width = Math.max(...results.map((r) => r.name.length));
console.log(`CoderHelm ${stage} in ${account} / ${region}\n`);
for (const r of results) console.log(`${r.ok ? "PASS" : "FAIL"}  ${r.name.padEnd(width)}  ${r.detail}`);
const failed = results.filter((r) => !r.ok).length;
console.log(`\n${results.length - failed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
