#!/usr/bin/env node
// Checks, read-only, that a hardened CoderHelm install holds no broad standing
// rights in its AWS account (docs/self-host/security.md). It only reads: IAM
// roles and policies, CloudFormation stacks, CodeBuild projects and builds,
// one SSM parameter, and IAM's policy simulator
// (iam:SimulatePrincipalPolicy and iam:SimulateCustomPolicy evaluate
// policies; they change nothing and start nothing).
//
//   AWS_PROFILE=<admin profile> node selfhost/check-access.mjs --stage prod --region us-east-1
//
// Prints a PASS/FAIL table and exits 1 when anything fails. Needs the AWS CLI
// v2 and read access to IAM, CloudFormation, CodeBuild and SSM.
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
// A native install is one stack, coderhelm-<stage> (infra/lib/native): its
// boundary and its optional auto-update role carry no launch id, and it has
// no installer or update build at all.
let parent = null;
try {
  parent = aws("cloudformation", "describe-stacks", "--stack-name", prefix).Stacks?.[0] ?? null;
} catch {
  parent = null;
}
const nativeParams = Object.fromEntries((parent?.Parameters ?? []).map((p) => [p.ParameterKey, p.ParameterValue]));
const native = "KeepDataOnDelete" in nativeParams;
const autoUpdate = native && nativeParams.AutoUpdate === "on";
let config = null;
let access = null;
if (native) {
  check("the install is one CloudFormation stack", true, `${prefix}: ${parent.StackStatus}, auto-update ${autoUpdate ? "on" : "off"}`);
} else {
  try {
    config = JSON.parse(aws("ssm", "get-parameter", "--name", `/${prefix}/installer/config`).Parameter.Value);
  } catch (e) {
    check("the install exists", false, `no stack ${prefix} and no /${prefix}/installer/config in ${region}: ${e.message}`);
  }
  access = config?.access;
  check("the install is hardened (config access)", access?.mode === "hardened", access ? `${access.owner}${access.id ? ` ${access.id}` : ""}` : "not hardened: it deploys through the CDK bootstrap roles");
}
// A native install's names end in -stack (infra/lib/native/parent.ts, NATIVE_ID).
const tail = native ? "-stack" : access?.owner === "launch" && access.id ? `-${access.id}` : "";
const boundaryArn = `arn:aws:iam::${account}:policy/${prefix}-boundary${tail}`;
const deployRole = `${prefix}-deploy${tail}`;
const deployRoleArn = `arn:aws:iam::${account}:role/${deployRole}`;

// ── Every CoderHelm role ─────────────────────────────────────────────────

const roles = pages((m) => aws("iam", "list-roles", ...(m ? ["--starting-token", m] : []))).flatMap((p) => p.Roles ?? []).filter((r) => r.RoleName.startsWith(`${prefix}-`));
check("CoderHelm roles found", roles.length > 0, `${roles.length} roles named ${prefix}-*`);
const noBoundary = [];
const admin = [];
const star = [];
// The tag that lets a role take on CoderHelm's read roles in this account
// (infra/lib/access.ts RUNTIME_ROLE_TAG): only the gateway's and worker's.
const RUNTIME_TAG = "coderhelm:role";
const runtimeTagged = new Map();
for (const r of roles) {
  const full = aws("iam", "get-role", "--role-name", r.RoleName).Role;
  const tag = (full.Tags ?? []).find((t) => t.Key.toLowerCase() === RUNTIME_TAG);
  if (tag) runtimeTagged.set(r.RoleName, tag.Value);
  // A native install's auto-update role carries its own copy (kept with it on delete).
  const want = native && r.RoleName === deployRole ? `arn:aws:iam::${account}:policy/${prefix}-deploy-boundary-stack` : boundaryArn;
  if (full.PermissionsBoundary?.PermissionsBoundaryArn !== want) noBoundary.push(`${r.RoleName} (${full.PermissionsBoundary?.PermissionsBoundaryArn ?? "none"})`);
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

let deployRoleExists = true;
if (native && !autoUpdate) {
  try {
    aws("iam", "get-role", "--role-name", deployRole);
    check("no standing deploy role (auto-update is off)", false, `${deployRole} exists: it is left from when auto-update was on (coderhelm-selfhost leftovers lists it)`);
  } catch (e) {
    check("no standing deploy role (auto-update is off)", e.code === "NoSuchEntity", e.code === "NoSuchEntity" ? "none" : e.message);
  }
  deployRoleExists = false;
} else try {
  const d = aws("iam", "get-role", "--role-name", deployRole).Role;
  const trust = statements(d.AssumeRolePolicyDocument);
  const onlyCfn = trust.length === 1 && trust[0].Effect === "Allow" && JSON.stringify(trust[0].Principal) === JSON.stringify({ Service: "cloudformation.amazonaws.com" }) && [trust[0].Action].flat().join() === "sts:AssumeRole";
  check("the deploy role's trust allows CloudFormation only", onlyCfn, JSON.stringify(trust));
} catch (e) {
  check("the deploy role's trust allows CloudFormation only", false, `${deployRole}: ${e.message}`);
}
const stacks = pages((m) => aws("cloudformation", "describe-stacks", ...(m ? ["--next-token", m] : []))).flatMap((p) => p.Stacks ?? []).filter((s) => s.StackName.startsWith(`${prefix}-`) && s.StackName !== `${prefix}-access`);
if (native) {
  // With auto-update off a stack runs with the admin's own permissions (no role); with it on, only the deploy role.
  const other = [parent, ...stacks].filter((s) => s.RoleARN && s.RoleARN !== deployRoleArn).map((s) => `${s.StackName} (${s.RoleARN})`);
  check("no CoderHelm stack runs with any other role", other.length === 0, other.join(", ") || `${stacks.length + 1} stacks`);
} else {
  const otherRole = stacks.filter((s) => s.RoleARN !== deployRoleArn).map((s) => `${s.StackName} (${s.RoleARN ?? "none"})`);
  check("every CoderHelm stack deploys through the deploy role", stacks.length > 0 && otherRole.length === 0, otherRole.join(", ") || `${stacks.length} stacks`);
}
const cdkRoles = pages((m) => aws("iam", "list-roles", ...(m ? ["--starting-token", m] : []))).flatMap((p) => p.Roles ?? []).filter((r) => /^cdk-[a-z0-9]+-(deploy|cfn-exec|file-publishing|image-publishing|lookup)-role-/.test(r.RoleName));
const cdkUsed = stacks.filter((s) => /role\/cdk-/.test(s.RoleARN ?? "")).map((s) => s.StackName);
check("the CDK bootstrap roles are not used by CoderHelm", cdkUsed.length === 0, cdkRoles.length ? `${cdkRoles.length} cdk-* roles exist in the account (not CoderHelm's); used by: ${cdkUsed.join(", ") || "no CoderHelm stack"}` : "no cdk-* roles in the account");

// ── Idle build roles: no deploy rights, no window ────────────────────────

const builds = roles.filter((r) => r.RoleName === `${prefix}-update-build` || r.RoleName.startsWith(`${prefix}-installer-`));
if (native) check("no installer or update build (a native install updates as a stack update)", builds.length === 0, builds.map((r) => r.RoleName).join(", ") || "none");
else check("build roles found", builds.length > 0, builds.map((r) => r.RoleName).join(", "));

// The builds running as each build role right now. A build holds its window
// from its first minute, while it downloads, checks and synthesizes the
// release, before any stack changes: so "running" is the build, not a stack.
// The projects are found by the role they run as (the installer's project
// is named after the Launch Stack, which can have any name).
function runningBuilds() {
  const byRole = new Map();
  let names = [];
  try {
    names = aws("codebuild", "list-projects").projects ?? [];
  } catch (e) {
    return { error: e.message, byRole };
  }
  for (let i = 0; i < names.length; i += 100) {
    for (const p of aws("codebuild", "batch-get-projects", "--names", ...names.slice(i, i + 100)).projects ?? []) {
      const role = String(p.serviceRole ?? "").split("/").pop();
      if (!builds.some((b) => b.RoleName === role)) continue;
      // Newest first; one build runs at a time, so the latest few are enough.
      const ids = aws("codebuild", "list-builds-for-project", "--project-name", p.name, "--sort-order", "DESCENDING", "--max-items", "10").ids ?? [];
      const running = ids.length ? (aws("codebuild", "batch-get-builds", "--ids", ...ids).builds ?? []).filter((b) => b.buildStatus === "IN_PROGRESS").map((b) => b.id) : [];
      byRole.set(role, [...(byRole.get(role) ?? []), ...running]);
    }
  }
  return { error: null, byRole };
}
const running = runningBuilds();
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
  const live = running.byRole.get(r.RoleName) ?? [];
  if (!window) check(`${r.RoleName}: no deploy window while idle`, true, "none");
  else if (live.length) check(`${r.RoleName}: no deploy window while idle`, true, `window open for running build ${live.join(", ")}`);
  else {
    const changing = stacks.some((s) => /_IN_PROGRESS$/.test(s.StackStatus));
    const why = running.error ? `a window is open; its builds could not be listed (${running.error})` : changing ? "a window is open and a stack is changing, with no build running as this role" : "a window is open with nothing running";
    check(`${r.RoleName}: no deploy window while idle`, false, why);
  }
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
if (deployRoleExists) try {
  const wrong = deny.filter(([a, r, c]) => sim(a, r, c) === "allowed").map(([a, r]) => `${a} on ${r}`);
  check("the deploy role is refused IAM users, roles without the boundary, the boundary, other roles, Organizations, CloudTrail, key deletion", wrong.length === 0, wrong.join(", ") || `${deny.length} denied`);
  const missing = allow.filter(([a, r, c]) => sim(a, r, c) !== "allowed").map(([a, r]) => `${a} on ${r}`);
  check("the deploy role can still deploy CoderHelm", missing.length === 0, missing.join(", ") || `${allow.length} allowed`);
} catch (e) {
  check("the deploy role (simulator)", false, e.message);
}

// ── Roles in this account: only CoderHelm's two read roles ───────────────
//
// An install in the same account as the data it reads takes on the log
// reader (CoderHelmLogReader) and the data-source role (CoderhelmReadOnly)
// here. The boundary allows exactly that: sts:AssumeRole on those two names,
// by the gateway's and the worker's roles. Anything broader fails.

const READ_ROLES = ["CoderHelmLogReader", "CoderhelmReadOnly"];
const partition = region.startsWith("us-gov-") ? "aws-us-gov" : region.startsWith("cn-") ? "aws-cn" : "aws";
const roleArn = (name) => `arn:${partition}:iam::${account}:role/${name}`;
const ctxEntry = (k, v) => `ContextKeyName=${k},ContextKeyValues=${v},ContextKeyType=string`;
try {
  const pol = aws("iam", "get-policy", "--policy-arn", boundaryArn).Policy;
  const boundaryDoc = decode(aws("iam", "get-policy-version", "--policy-arn", boundaryArn, "--version-id", pol.DefaultVersionId).PolicyVersion.Document);
  // Every sts action allowed by the role's own policy: only the boundary decides.
  const anySts = JSON.stringify({ Version: "2012-10-17", Statement: [{ Effect: "Allow", Action: "sts:*", Resource: "*" }] });
  const decide = (action, resource, tag) =>
    aws("iam", "simulate-custom-policy", "--policy-input-list", anySts, "--permissions-boundary-policy-input-list", JSON.stringify(boundaryDoc), "--action-names", action, "--resource-arns", resource,
      "--context-entries", ctxEntry("aws:ResourceAccount", account), ...(tag ? [ctxEntry(`aws:PrincipalTag/${RUNTIME_TAG}`, tag)] : [])).EvaluationResults?.[0]?.EvalDecision;
  // Who may set the tag: only the deploy role (an identity policy allowing the tag writes, so the boundary decides).
  const anyTag = JSON.stringify({ Version: "2012-10-17", Statement: [{ Effect: "Allow", Action: ["iam:CreateRole", "iam:TagRole", "iam:UntagRole"], Resource: "*" }] });
  const tagDecide = (action, key, principal) =>
    aws("iam", "simulate-custom-policy", "--policy-input-list", anyTag, "--permissions-boundary-policy-input-list", JSON.stringify(boundaryDoc), "--action-names", action, "--resource-arns", roleArn(`${prefix}-check`),
      "--context-entries", `ContextKeyName=aws:TagKeys,ContextKeyValues=${key},ContextKeyType=stringList`, ctxEntry("aws:PrincipalArn", principal), ctxEntry("iam:PermissionsBoundary", boundaryArn)).EvaluationResults?.[0]?.EvalDecision;
  const broader = [];
  const others = ["CoderhelmReadOnly-2", "CoderHelmLogReaderAdmin", "team/CoderhelmReadOnly", "coderhelmreadonly", `cdk-hnb659fds-deploy-role-${account}-${region}`, deployRole, `${prefix}-update-build`, "OrganizationAccountAccessRole"];
  for (const tag of ["gateway", "worker"]) {
    for (const r of others) for (const a of ["sts:AssumeRole", "sts:TagSession"]) if (decide(a, roleArn(r), tag) === "allowed") broader.push(`${tag}: ${a} on ${r}`);
    for (const r of READ_ROLES) for (const a of ["sts:TagSession", "sts:SetSourceIdentity", "sts:AssumeRoleWithWebIdentity"]) if (decide(a, roleArn(r), tag) === "allowed") broader.push(`${tag}: ${a} on ${r}`);
  }
  for (const r of READ_ROLES) for (const tag of [null, "log-analyzer"]) if (decide("sts:AssumeRole", roleArn(r), tag) === "allowed") broader.push(`${tag ? `role tagged ${tag}` : "an untagged role"}: sts:AssumeRole on ${r}`);
  const hasException = READ_ROLES.some((r) => ["gateway", "worker"].some((tag) => decide("sts:AssumeRole", roleArn(r), tag) === "allowed"));
  if (hasException) {
    for (const a of ["iam:TagRole", "iam:UntagRole", "iam:CreateRole"]) {
      for (const key of [RUNTIME_TAG, "Coderhelm:Role"]) if (tagDecide(a, key, roleArn(`${prefix}-update-build`)) === "allowed") broader.push(`a role other than the deploy role: ${a} with tag ${key}`);
    }
  }
  const exception = ["gateway", "worker"].flatMap((tag) => READ_ROLES.filter((r) => decide("sts:AssumeRole", roleArn(r), tag) === "allowed").map((r) => `${tag}: ${r}`));
  const what = exception.length === 4 ? "expected exception: the gateway and worker may assume CoderHelmLogReader and CoderhelmReadOnly here" : exception.length ? `partial exception: ${exception.join(", ")}` : "no exception (a boundary from before it: same-account AWS data sources can't be read)";
  check("the boundary: no other role in this account can be assumed (only CoderHelm's two read roles, by the gateway and worker)", broader.length === 0, broader.join(", ") || what);
} catch (e) {
  check("the boundary: no other role in this account can be assumed (only CoderHelm's two read roles, by the gateway and worker)", false, e.message);
}

// Only the gateway's and the worker's roles carry the tag, and they can
// take on the read roles (the policy simulator, with the role's own tag).
// The functions' roles, found in their stacks (GatewayServiceRole..., WorkerServiceRole...).
const runtime = {};
for (const [f, stack, id] of [["gateway", "api", "GatewayServiceRole"], ["worker", "worker", "WorkerServiceRole"]]) {
  try {
    const res = aws("cloudformation", "describe-stack-resources", "--stack-name", `${prefix}-${stack}`).StackResources ?? [];
    runtime[f] = res.find((r) => r.ResourceType === "AWS::IAM::Role" && r.LogicalResourceId.startsWith(id))?.PhysicalResourceId ?? null;
  } catch (e) {
    runtime[f] = null;
  }
}
const wrongTag = [...runtimeTagged].filter(([name, v]) => !(runtime[v] === name)).map(([name, v]) => `${name} (${RUNTIME_TAG}=${v})`);
check(`only the gateway's and worker's roles carry ${RUNTIME_TAG}`, wrongTag.length === 0, wrongTag.join(", ") || [...runtimeTagged].map(([name, v]) => `${v}: ${name}`).join(", ") || "no role carries it yet (a release from before it)");
try {
  const notes = [];
  const broader = [];
  for (const [f, name] of Object.entries(runtime)) {
    if (!name) continue;
    const tag = runtimeTagged.get(name);
    const sim = (r) => aws("iam", "simulate-principal-policy", "--policy-source-arn", roleArn(name), "--action-names", "sts:AssumeRole", "--resource-arns", roleArn(r),
      "--context-entries", ctxEntry("aws:ResourceAccount", account), ...(tag ? [ctxEntry(`aws:PrincipalTag/${RUNTIME_TAG}`, tag)] : [])).EvaluationResults?.[0]?.EvalDecision;
    notes.push(`${f}: ${READ_ROLES.map((r) => `${r} ${sim(r)}`).join(", ")}`);
    for (const r of [`cdk-hnb659fds-deploy-role-${account}-${region}`, deployRole, "CoderhelmReadOnly-2"]) if (sim(r) === "allowed") broader.push(`${f}: sts:AssumeRole on ${r}`);
  }
  check("the gateway and worker roles: only the read roles in this account (policy simulator)", broader.length === 0, broader.join(", ") || notes.join("; "));
} catch (e) {
  check("the gateway and worker roles: only the read roles in this account (policy simulator)", false, e.message);
}

// CoderHelm's read roles in this account, when they exist: the boundary
// lets the gateway and worker take them on, so they must only read. Their
// trust needs an external id and names only this account or the install's
// roles; no managed policies; nothing that assumes or passes roles, writes
// IAM, or allows *; and the simulator refuses them the install's own secret
// and the deploy role.
{
  const bad = [];
  const seen = [];
  for (const name of READ_ROLES) {
    let role;
    try {
      role = aws("iam", "get-role", "--role-name", name).Role;
    } catch (e) {
      if (e.code === "NoSuchEntity") continue;
      throw e;
    }
    seen.push(name);
    for (const t of statements(role.AssumeRolePolicyDocument).filter((x) => x.Effect === "Allow")) {
      const ext = Object.entries(t.Condition ?? {}).some(([op, kv]) => /^StringEquals$/i.test(op) && Object.keys(kv).some((k) => k.toLowerCase() === "sts:externalid"));
      const who = [t.Principal?.AWS ?? []].flat();
      const okWho = who.length > 0 && who.every((p) => p === `arn:${partition}:iam::${account}:root` || p === account || String(p).startsWith(`arn:${partition}:iam::${account}:role/${prefix}-`));
      if (!ext) bad.push(`${name}: its trust has no sts:ExternalId condition`);
      if (!okWho || t.Principal?.Service || t.Principal?.Federated || t.Principal === "*") bad.push(`${name}: its trust names ${JSON.stringify(t.Principal)}`);
    }
    for (const p of aws("iam", "list-attached-role-policies", "--role-name", name).AttachedPolicies ?? []) bad.push(`${name}: managed policy ${p.PolicyArn}`);
    for (const pn of aws("iam", "list-role-policies", "--role-name", name).PolicyNames ?? []) {
      const doc = aws("iam", "get-role-policy", "--role-name", name, "--policy-name", pn).PolicyDocument;
      for (const st of statements(doc).filter((x) => x.Effect === "Allow")) {
        const acts = [st.Action ?? []].flat().map((x) => String(x).toLowerCase());
        if (st.NotAction || acts.some((x) => x === "*" || x.endsWith(":*") && !/^(logs|cloudwatch):\*$/.test(x) || x.startsWith("sts:assumerole") || x === "iam:passrole" || /^iam:(create|put|attach|update|delete|add|tag|untag|set|pass)/.test(x))) {
          bad.push(`${name}: inline ${pn} allows ${st.NotAction ? `NotAction ${JSON.stringify(st.NotAction)}` : acts.join(",")}`);
        }
      }
    }
    for (const [action, resource] of [
      ["sts:AssumeRole", deployRoleArn],
      ["iam:PassRole", deployRoleArn],
      ["secretsmanager:GetSecretValue", `arn:${partition}:secretsmanager:${region}:${account}:secret:coderhelm/${stage}/secrets-AbCdEf`],
      ["iam:CreateRole", roleArn("check")],
    ]) {
      const d = aws("iam", "simulate-principal-policy", "--policy-source-arn", role.Arn, "--action-names", action, "--resource-arns", resource).EvaluationResults?.[0]?.EvalDecision;
      if (d === "allowed") bad.push(`${name}: ${action} on ${resource} (policy simulator)`);
    }
  }
  check("CoderHelm's read roles in this account only read (trust needs the external id)", bad.length === 0, bad.join("; ") || (seen.length ? `${seen.join(", ")}: external id required, read-only` : "none in this account"));
}

// ── The table ────────────────────────────────────────────────────────────

const width = Math.max(...results.map((r) => r.name.length));
console.log(`CoderHelm ${stage} in ${account} / ${region}\n`);
for (const r of results) console.log(`${r.ok ? "PASS" : "FAIL"}  ${r.name.padEnd(width)}  ${r.detail}`);
const failed = results.filter((r) => !r.ok).length;
console.log(`\n${results.length - failed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
