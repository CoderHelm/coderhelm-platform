// Every check the installer runs: before each step of the guided install
// moves on, and all of them in `doctor`. A check never throws: it returns a
// result that says, when something is wrong, three things in plain words:
//
//   what   what is wrong, in one sentence
//   why    why it matters, in one sentence
//   fix    how to fix it: a command to copy, a link, or both
//
// The AWS calls go through the Runner (tests answer them with a fake), the
// HTTP calls through ctx.fetchImpl, DNS through ctx.dns. No check creates
// anything; a few offer an action (such as running `cdk bootstrap`) that the
// guided install asks about first.
import crypto from "node:crypto";
import dnsPromises from "node:dns/promises";
import { PERMISSIONS, EVENTS } from "./github-app.mjs";
import { readSecret } from "./secrets.mjs";
import { derived } from "./util.mjs";

export const PASS = "pass";
export const WARN = "warn";
export const FAIL = "fail";
export const SKIP = "skip";

export const CLI = "npx -y github:CoderHelm/coderhelm-platform";

/** A passing result. */
export function pass(id, title, detail = "") {
  return { id, title, status: PASS, detail };
}

/** A result that isn't a pass: what, why and the fix. */
export function problem(status, id, title, { what, why, fix, action } = {}) {
  return { id, title, status, what, why, fix: fix ?? {}, ...(action ? { action } : {}) };
}

const aws = (ctx, args, region, answer) =>
  ctx.runner.json("aws", [...args, ...(region ? ["--region", region] : []), "--output", "json"], {
    allowFail: true,
    dryRunAnswer: answer === undefined ? "null" : JSON.stringify(answer),
  });

/** The last error line of an AWS CLI call that failed, for a plain detail. */
async function awsError(ctx, args, region) {
  const r = await ctx.runner.run("aws", [...args, ...(region ? ["--region", region] : []), "--output", "json"], {
    capture: true,
    allowFail: true,
  }).catch((e) => ({ code: 1, stdout: "", stderr: e.message }));
  if (r.code === 0) {
    try {
      return { ok: JSON.parse(r.stdout || "null") };
    } catch {
      return { ok: null };
    }
  }
  return { error: (r.stderr || "").trim().split("\n").filter(Boolean).pop() ?? `exit ${r.code}` };
}

// ── This machine ──────────────────────────────────────────────────────────

export function checkNode(version = process.versions.node) {
  const major = Number(String(version).split(".")[0]);
  if (major >= 20) return pass("node", "Node.js 20 or newer", `Node.js ${version}`);
  return problem(FAIL, "node", "Node.js 20 or newer", {
    what: `This machine has Node.js ${version}.`,
    why: "The installer and the AWS CDK need Node.js 20 or newer.",
    fix: { text: "Install the current LTS release, then run this again.", url: "https://nodejs.org/en/download" },
  });
}

export async function checkAwsCli(ctx) {
  const r = await ctx.runner.run("aws", ["--version"], { capture: true, allowFail: true, dryRunAnswer: "aws-cli/2.17.0" }).catch(() => null);
  const out = r ? `${r.stdout}${r.stderr}`.trim() : "";
  const url = "https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html";
  if (!r || r.code !== 0) {
    return problem(FAIL, "aws_cli", "AWS CLI v2", {
      what: "The AWS CLI isn't installed (no `aws` command).",
      why: "The installer signs in to AWS and deploys with it.",
      fix: { text: "Install the AWS CLI v2, open a new terminal, then check again.", url },
    });
  }
  if (!/aws-cli\/2\./.test(out)) {
    return problem(FAIL, "aws_cli", "AWS CLI v2", {
      what: `The AWS CLI is version 1 (${out.split(" ")[0]}).`,
      why: "Version 1 lacks the sign-in and commands the installer uses.",
      fix: { text: "Install the AWS CLI v2 (it replaces v1), then check again.", url },
    });
  }
  return pass("aws_cli", "AWS CLI v2", out.split(" ")[0]);
}

export async function checkTar(ctx) {
  const r = await ctx.runner.run("tar", ["--version"], { capture: true, allowFail: true, dryRunAnswer: "tar" }).catch(() => null);
  if (r && r.code === 0) return pass("tar", "tar");
  return problem(FAIL, "tar", "tar", {
    what: "The `tar` command isn't available.",
    why: "Releases are unpacked with it.",
    fix: { text: "Install tar with your package manager (it comes with macOS and Linux)." },
  });
}

/** Signed in to AWS; returns { result, identity }. */
export async function checkSignIn(ctx, { profile } = {}) {
  const r = await awsError(ctx, ["sts", "get-caller-identity"]);
  const identity = r.ok && r.ok.Account ? r.ok : null;
  if (ctx.runner.dryRun) {
    return { result: pass("aws_signin", "Signed in to AWS", "dry run"), identity: { Account: "000000000000", Arn: "arn:aws:iam::000000000000:user/dry-run" } };
  }
  if (identity) return { result: pass("aws_signin", "Signed in to AWS", `${identity.Arn}`), identity };
  const expired = /expired|token.*(invalid|refresh)|sso/i.test(r.error ?? "");
  return {
    identity: null,
    result: problem(FAIL, "aws_signin", "Signed in to AWS", {
      what: expired ? "Your AWS sign-in has expired." : "No AWS credentials were found on this machine.",
      why: "Every step after this one reads or changes your AWS account.",
      fix: expired || profile
        ? { text: "Sign in again, then check again.", command: `aws sso login${profile ? ` --profile ${profile}` : ""}` }
        : {
            text: "Set up a profile (SSO is the usual way in a company account), then check again.",
            command: "aws configure sso",
            url: "https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-sso.html",
          },
    }),
  };
}

export function checkAccountMatch(identity, account) {
  if (!identity || !account) return null;
  if (identity.Account === account) return pass("aws_account", "The right AWS account", account);
  return problem(FAIL, "aws_account", "The right AWS account", {
    what: `You're signed in to account ${identity.Account}, but the install goes to ${account}.`,
    why: "Deploying with the wrong credentials would put CoderHelm in the wrong account, or fail.",
    fix: { text: `Switch to a profile for ${account} (AWS_PROFILE=<name>), or change "account" in the config.`, command: "aws configure list-profiles" },
  });
}

// ── Permissions ───────────────────────────────────────────────────────────

/** What a first install does, as IAM actions (the CDK deploy roles do the rest). */
export const NEEDED_ACTIONS = [
  "cloudformation:CreateStack",
  "cloudformation:DescribeStacks",
  "cloudformation:CreateChangeSet",
  "cloudformation:ExecuteChangeSet",
  "s3:CreateBucket",
  "s3:PutObject",
  "iam:CreateRole",
  "iam:PassRole",
  "iam:AttachRolePolicy",
  "ssm:GetParameter",
  "ssm:PutParameter",
  "secretsmanager:CreateSecret",
  "secretsmanager:PutSecretValue",
  "secretsmanager:GetSecretValue",
  "ecr:CreateRepository",
  "route53:ListHostedZones",
  "ses:GetAccount",
  "sts:AssumeRole",
];

/**
 * The ARN to simulate: an assumed role's session ARN becomes its role ARN
 * (with the role's path, which IAM needs; SSO roles have one).
 */
export async function principalArn(ctx, arn) {
  const m = String(arn).match(/^arn:(aws[a-z-]*):sts::(\d{12}):assumed-role\/([^/]+)\//);
  if (!m) return arn;
  const role = await aws(ctx, ["iam", "get-role", "--role-name", m[3]]);
  return role?.Role?.Arn ?? `arn:${m[1]}:iam::${m[2]}:role/${m[3]}`;
}

export async function checkPermissions(ctx, identity, actions = NEEDED_ACTIONS) {
  const title = "AWS permissions for the install";
  if (!identity) return null;
  if (ctx.runner.dryRun) return pass("aws_permissions", title, "dry run");
  if (/:root$/.test(identity.Arn)) {
    return problem(WARN, "aws_permissions", title, {
      what: "You're signed in as the account's root user.",
      why: "It works, but AWS recommends an IAM role or SSO user with AdministratorAccess.",
      fix: { text: "Create an SSO user or IAM role with AdministratorAccess and sign in with it.", url: "https://console.aws.amazon.com/iam/home#/roles" },
    });
  }
  const source = await principalArn(ctx, identity.Arn);
  const r = await awsError(ctx, ["iam", "simulate-principal-policy", "--policy-source-arn", source, "--action-names", ...actions]);
  if (r.error) {
    return problem(WARN, "aws_permissions", title, {
      what: "Your permissions couldn't be checked ahead of time (iam:SimulatePrincipalPolicy isn't allowed).",
      why: "A missing permission then shows up during the deploy instead of now.",
      fix: { text: "Carry on if your role has AdministratorAccess; otherwise ask your AWS admin to allow iam:SimulatePrincipalPolicy so this check can run." },
    });
  }
  const denied = (r.ok?.EvaluationResults ?? []).filter((e) => e.EvalDecision !== "allowed").map((e) => e.EvalActionName);
  if (!denied.length) return pass("aws_permissions", title, `${actions.length} actions allowed for ${source.split("/").pop()}`);
  return problem(FAIL, "aws_permissions", title, {
    what: `Your AWS identity isn't allowed ${denied.length} thing${denied.length === 1 ? "" : "s"} the install does: ${denied.join(", ")}.`,
    why: "The deploy stops halfway at the first of them.",
    fix: {
      text: "Ask your AWS admin to allow them, or sign in with a role that has AdministratorAccess, then check again.",
      url: "https://console.aws.amazon.com/iam/home#/roles",
    },
  });
}

// ── Region ────────────────────────────────────────────────────────────────

/** Services the install uses, by their names in AWS's public region list. */
export const NEEDED_SERVICES = [
  "lambda", "dynamodb", "sqs", "apigateway", "cognito-idp", "kms", "secretsmanager",
  "events", "cloudformation", "codebuild", "ses", "cloudfront", "ssm",
];

export async function checkRegion(ctx, region, { bedrock = false } = {}) {
  const title = `Region ${region} has every service`;
  if (ctx.runner.dryRun) return pass("region", title, "dry run");
  const wanted = bedrock ? [...NEEDED_SERVICES, "bedrock"] : NEEDED_SERVICES;
  const out = await aws(ctx, ["ssm", "get-parameters-by-path", "--path", `/aws/service/global-infrastructure/regions/${region}/services`], region);
  const have = new Set((out?.Parameters ?? []).map((p) => String(p.Value ?? p.Name.split("/").pop())));
  if (!have.size) {
    const known = await aws(ctx, ["ec2", "describe-regions", "--all-regions", "--query", "Regions[].RegionName"], "us-east-1");
    if (Array.isArray(known) && known.length && !known.includes(region)) {
      return problem(FAIL, "region", title, {
        what: `${region} isn't an AWS region.`,
        why: "Everything is created in this region.",
        fix: { text: "Pick a region such as us-east-1, eu-west-1 or ap-southeast-2." },
      });
    }
    return problem(WARN, "region", title, {
      what: `The services in ${region} couldn't be listed.`,
      why: "A region without one of them fails during the deploy.",
      fix: { text: "us-east-1, us-west-2, eu-west-1, eu-central-1 and ap-southeast-2 have everything.", url: "https://aws.amazon.com/about-aws/global-infrastructure/regional-product-services/" },
    });
  }
  // CloudFront is global; it isn't listed per region.
  const missing = wanted.filter((s) => s !== "cloudfront" && !have.has(s));
  if (!missing.length) return pass("region", title);
  return problem(FAIL, "region", title, {
    what: `${region} lacks ${missing.join(", ")}.`,
    why: "The install needs every one of them in the same region.",
    fix: { text: "Choose another region (type back), such as us-east-1, us-west-2 or eu-west-1.", url: "https://aws.amazon.com/about-aws/global-infrastructure/regional-product-services/" },
  });
}

// ── Bedrock ───────────────────────────────────────────────────────────────

export const BEDROCK_MODELS = [
  ["anthropic.claude-opus-5-5", "Claude Opus 5.5"],
  ["anthropic.claude-sonnet-5-5", "Claude Sonnet 5.5"],
];

export function bedrockConsole(region) {
  return `https://console.aws.amazon.com/bedrock/home?region=${region}#/modelaccess`;
}

export async function checkBedrock(ctx, region) {
  const title = "Claude models enabled in Bedrock";
  if (ctx.runner.dryRun) return pass("bedrock", title, "dry run");
  const missing = [];
  for (const [id, name] of BEDROCK_MODELS) {
    const a = await aws(ctx, ["bedrock", "get-foundation-model-availability", "--model-id", id], region);
    const ok = !!a && a.authorizationStatus === "AUTHORIZED" && (a.agreementAvailability?.status ?? "AVAILABLE") === "AVAILABLE"
      && (a.entitlementAvailability ?? "AVAILABLE") === "AVAILABLE" && (a.regionAvailability ?? "AVAILABLE") === "AVAILABLE";
    if (!ok) missing.push(`${name} (${id})`);
  }
  if (!missing.length) return pass("bedrock", title, BEDROCK_MODELS.map((m) => m[1]).join(", "));
  return problem(FAIL, "bedrock", title, {
    what: `Not enabled in ${region}: ${missing.join(", ")}.`,
    why: "Every run calls these models; none can start until Bedrock allows them.",
    fix: { text: "Open Model access, choose Modify model access, tick these models, submit, then check again (it usually takes a minute).", url: bedrockConsole(region) },
  });
}

// ── CDK bootstrap ─────────────────────────────────────────────────────────

/** `missing`: how a missing bootstrap counts (the guided install's deploy runs it, so there it is a warning). */
export async function checkBootstrap(ctx, account, region, { missing = FAIL } = {}) {
  const title = `CDK set up in ${region}`;
  const command = `npx -y cdk bootstrap aws://${account}/${region}`;
  if (ctx.runner.dryRun) return pass(`cdk_${region}`, title, "dry run");
  const s = await aws(ctx, ["cloudformation", "describe-stacks", "--stack-name", "CDKToolkit"], region);
  const status = s?.Stacks?.[0]?.StackStatus;
  if (status && /_COMPLETE$/.test(status) && !/ROLLBACK|DELETE/.test(status)) return pass(`cdk_${region}`, title, `CDKToolkit ${status}`);
  return problem(status ? FAIL : missing, `cdk_${region}`, title, {
    what: status ? `The CDKToolkit stack in ${region} is ${status}.` : `CDK isn't bootstrapped in ${account} / ${region} yet.`,
    why: "The deploy uploads CoderHelm's code through the bucket and roles the bootstrap creates.",
    fix: status && /ROLLBACK|FAILED/.test(status)
      ? { text: "Delete the CDKToolkit stack in the CloudFormation console, then bootstrap again.", command, url: `https://console.aws.amazon.com/cloudformation/home?region=${region}` }
      : { text: missing === FAIL ? "Run the bootstrap once for this account and region." : "The deploy runs it for you, or run it now.", command },
    action: { label: "Run the bootstrap now", command: ["npx", ["--yes", "cdk", "bootstrap", `aws://${account}/${region}`]] },
  });
}

// ── Domain and DNS ────────────────────────────────────────────────────────

const norm = (s) => String(s).toLowerCase().replace(/\.$/, "");

export async function checkHostedZone(ctx, zone) {
  const title = `Route 53 zone ${zone}`;
  if (ctx.runner.dryRun) return { result: pass("dns_zone", title, "dry run"), zoneId: null };
  const out = await aws(ctx, ["route53", "list-hosted-zones-by-name", "--dns-name", zone, "--max-items", "5"]);
  const z = (out?.HostedZones ?? []).find((h) => norm(h.Name) === norm(zone) && !h.Config?.PrivateZone);
  if (z) return { result: pass("dns_zone", title, String(z.Id ?? "").replace("/hostedzone/", "")), zoneId: z.Id ?? null };
  return {
    zoneId: null,
    result: problem(FAIL, "dns_zone", title, {
      what: `There's no public Route 53 zone named ${zone} in this account.`,
      why: "The certificate and the DNS record for your address are created in that zone.",
      fix: {
        text: `Create the zone, then point ${zone}'s name servers at it (the next check shows them). Or leave the hostname empty to use CloudFront's address.`,
        command: `aws route53 create-hosted-zone --name ${zone} --caller-reference coderhelm-${Date.now()}`,
      },
    }),
  };
}

/** The zone's name servers against what the public DNS answers. */
export async function checkDelegation(ctx, zone, zoneId) {
  const title = `${zone} points at Route 53`;
  if (!zoneId || ctx.runner.dryRun) return null;
  const z = await aws(ctx, ["route53", "get-hosted-zone", "--id", zoneId]);
  const expected = (z?.DelegationSet?.NameServers ?? []).map(norm).sort();
  if (!expected.length) return null;
  let current = [];
  try {
    current = (await (ctx.dns ?? dnsPromises).resolveNs(zone)).map(norm).sort();
  } catch {
    current = [];
  }
  const same = current.length === expected.length && current.every((n, i) => n === expected[i]);
  if (same) return pass("dns_delegation", title, expected.join(", "));
  return problem(FAIL, "dns_delegation", title, {
    what: current.length
      ? `${zone} uses other name servers: ${current.join(", ")}.`
      : `${zone} has no name servers in public DNS yet.`,
    why: "Until the name servers match, the certificate can't validate and nobody can reach your address.",
    fix: {
      text: `At your domain registrar (or the parent zone), set the NS records for ${zone} to exactly these, then check again (changes can take up to an hour):`,
      command: expected.map((n) => `${zone}. NS ${n}.`).join("\n"),
    },
  });
}

/** An ACM certificate (us-east-1, for CloudFront) that is issued. */
export async function checkCertificate(ctx, arn, hostname) {
  const title = "TLS certificate issued";
  if (ctx.runner.dryRun) return pass("certificate", title, "dry run");
  const region = String(arn).split(":")[3];
  if (region !== "us-east-1") {
    return problem(FAIL, "certificate", title, {
      what: `The certificate is in ${region}; CloudFront only uses certificates in us-east-1.`,
      why: "The deploy can't attach it to your address.",
      fix: { text: `Request a certificate for ${hostname} in us-east-1 and use its ARN.`, url: "https://us-east-1.console.aws.amazon.com/acm/home?region=us-east-1#/certificates/request" },
    });
  }
  const c = (await aws(ctx, ["acm", "describe-certificate", "--certificate-arn", arn], "us-east-1"))?.Certificate;
  if (!c) {
    return problem(FAIL, "certificate", title, {
      what: "That certificate ARN wasn't found in this account.",
      why: "The deploy attaches it to your address.",
      fix: { text: "Copy the ARN again from the ACM console (us-east-1).", url: "https://us-east-1.console.aws.amazon.com/acm/home?region=us-east-1#/certificates/list" },
    });
  }
  const names = [c.DomainName, ...(c.SubjectAlternativeNames ?? [])].map(norm);
  const covers = names.some((n) => n === norm(hostname) || (n.startsWith("*.") && norm(hostname).endsWith(n.slice(1)) && norm(hostname).split(".").length === n.split(".").length));
  if (!covers) {
    return problem(FAIL, "certificate", title, {
      what: `The certificate covers ${names.join(", ")}, not ${hostname}.`,
      why: "Browsers refuse a certificate for another name.",
      fix: { text: `Request a certificate for ${hostname} in us-east-1 and use its ARN.`, url: "https://us-east-1.console.aws.amazon.com/acm/home?region=us-east-1#/certificates/request" },
    });
  }
  if (c.Status === "ISSUED") return pass("certificate", title, `${c.DomainName}, issued`);
  const records = (c.DomainValidationOptions ?? [])
    .map((o) => o.ResourceRecord)
    .filter(Boolean)
    .map((r) => `${r.Name} ${r.Type} ${r.Value}`);
  return problem(c.Status === "PENDING_VALIDATION" ? WARN : FAIL, "certificate", title, {
    what: c.Status === "PENDING_VALIDATION" ? "The certificate is waiting for DNS validation." : `The certificate is ${c.Status}.`,
    why: "CloudFront can't serve your address over HTTPS until it is issued.",
    fix: records.length
      ? { text: "Add these DNS records where your domain's DNS is hosted; ACM issues the certificate within minutes:", command: records.join("\n") }
      : { text: "Open the certificate in ACM for the records to add.", url: `https://us-east-1.console.aws.amazon.com/acm/home?region=us-east-1#/certificates/${arn.split("/").pop()}` },
  });
}

// ── Email ─────────────────────────────────────────────────────────────────

export async function checkSesIdentity(ctx, from, region, { zone } = {}) {
  const title = "Email sender verified";
  const domain = String(from).split("@")[1] ?? from;
  if (ctx.runner.dryRun) return pass("ses_identity", title, "dry run");
  const id = await aws(ctx, ["sesv2", "get-email-identity", "--email-identity", domain], region);
  const console = `https://console.aws.amazon.com/ses/home?region=${region}#/identities/${domain}`;
  if (!id) {
    if (zone && norm(zone) === norm(domain)) return pass("ses_identity", title, `${domain} is your Route 53 zone: the deploy adds its DKIM records`);
    return problem(WARN, "ses_identity", title, {
      what: `${domain} isn't set up in Amazon SES yet.`,
      why: "SES refuses to send from a domain until its DKIM records are in DNS.",
      fix: { text: `The deploy creates the identity; then add the three DKIM records SES shows for ${domain} (doctor prints them).`, url: console },
    });
  }
  const dkim = id.DkimAttributes ?? {};
  if (id.VerifiedForSendingStatus && (dkim.Status ?? "SUCCESS") === "SUCCESS") return pass("ses_identity", title, `${domain}, DKIM verified`);
  const records = (dkim.Tokens ?? []).map((t) => `${t}._domainkey.${domain} CNAME ${t}.dkim.amazonses.com`);
  return problem(id.VerifiedForSendingStatus ? WARN : FAIL, "ses_identity", title, {
    what: id.VerifiedForSendingStatus ? `${domain} is verified but DKIM is ${String(dkim.Status).toLowerCase()}.` : `${domain} isn't verified yet (DKIM ${String(dkim.Status ?? "not started").toLowerCase()}).`,
    why: "Mail without DKIM lands in spam, and SES won't send at all from an unverified domain.",
    fix: records.length
      ? { text: `Add these DNS records for ${domain}; SES checks them within an hour:`, command: records.join("\n"), url: console }
      : { text: `Open ${domain} in SES and add the DKIM records it shows.`, url: console },
  });
}

export async function checkSesSandbox(ctx, region) {
  const title = "Email can reach anyone";
  if (ctx.runner.dryRun) return pass("ses_sandbox", title, "dry run");
  const a = await aws(ctx, ["sesv2", "get-account"], region);
  if (a?.ProductionAccessEnabled) return pass("ses_sandbox", title, "SES production access");
  return problem(WARN, "ses_sandbox", title, {
    what: a ? "This account is in the SES sandbox: email only reaches verified addresses, 200 a day." : "The SES account couldn't be read.",
    why: "Invites and run results won't reach teammates until production access is granted.",
    fix: { text: "Request production access (Account dashboard, Request production access); AWS usually answers within a day. You can install meanwhile.", url: `https://console.aws.amazon.com/ses/home?region=${region}#/account` },
  });
}

// ── Model provider ────────────────────────────────────────────────────────

export const TEST_MODEL = "claude-sonnet-5-5";

/** A real one-token call with the key. */
export async function checkAnthropicKey(ctx, key) {
  const title = "Anthropic API key works";
  if (!key) return null;
  let r;
  try {
    r = await (ctx.fetchImpl ?? fetch)("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: { "x-api-key": key, "anthropic-version": "2023-06-01", "content-type": "application/json" },
      body: JSON.stringify({ model: TEST_MODEL, max_tokens: 1, messages: [{ role: "user", content: "Reply with OK." }] }),
      signal: AbortSignal.timeout(15000),
    });
  } catch {
    return problem(WARN, "anthropic_key", title, {
      what: "Anthropic couldn't be reached to test the key.",
      why: "CoderHelm calls Claude with your team's key on every run.",
      fix: { text: "Check your network, then check again; or add the key later in Settings, AI models.", url: "https://status.anthropic.com" },
    });
  }
  if (r.status === 200) return pass("anthropic_key", title, `${TEST_MODEL} answered`);
  const keys = "https://console.anthropic.com/settings/keys";
  if (r.status === 401) {
    return problem(FAIL, "anthropic_key", title, {
      what: "Anthropic refused the key (401).",
      why: "Runs would fail with the same key.",
      fix: { text: "Copy the key again, or create a new one, then check again.", url: keys },
    });
  }
  if (r.status === 403 || r.status === 404) {
    return problem(FAIL, "anthropic_key", title, {
      what: `The key works but can't use ${TEST_MODEL} (${r.status}).`,
      why: "CoderHelm's runs use the Claude 5.5 models.",
      fix: { text: "Check the key's workspace and model access in the Anthropic console.", url: keys },
    });
  }
  if (r.status === 400 && /credit|billing/i.test(await r.text().catch(() => ""))) {
    return problem(FAIL, "anthropic_key", title, {
      what: "The key's organization has no credit left.",
      why: "Every call fails until it has credit.",
      fix: { text: "Add credit in the Anthropic console, then check again.", url: "https://console.anthropic.com/settings/billing" },
    });
  }
  return problem(WARN, "anthropic_key", title, {
    what: `The test call answered ${r.status}.`,
    why: "This is usually a busy moment at Anthropic, not your key.",
    fix: { text: "Check again in a minute.", url: "https://status.anthropic.com" },
  });
}

/** A real one-token call through Bedrock, from this machine's credentials. */
export async function checkBedrockCall(ctx, region, prefix = "us.") {
  const title = "Claude answers through Bedrock";
  if (ctx.runner.dryRun) return pass("model_call", title, "dry run");
  const modelId = `${prefix ?? ""}${BEDROCK_MODELS[1][0]}`;
  const r = await awsError(ctx, [
    "bedrock-runtime", "converse", "--model-id", modelId,
    "--messages", JSON.stringify([{ role: "user", content: [{ text: "Reply with OK." }] }]),
    "--inference-config", JSON.stringify({ maxTokens: 1 }),
  ], region);
  if (r.ok) return pass("model_call", title, `${modelId} answered`);
  return problem(FAIL, "model_call", title, {
    what: `Bedrock refused a test call to ${modelId}: ${r.error}`,
    why: "Every run makes the same call.",
    fix: /access|denied|not authorized/i.test(r.error)
      ? { text: `Enable model access for ${BEDROCK_MODELS[1][1]} and ${BEDROCK_MODELS[0][1]} in ${region}, then check again.`, url: bedrockConsole(region) }
      : { text: `Check the inference profile prefix ("${prefix}") in models.bedrockInferencePrefix matches the region, then check again.`, url: bedrockConsole(region) },
  });
}

// ── GitHub App ────────────────────────────────────────────────────────────

/** An app JWT (RS256) from the stored id and private key. */
export function appJwt(appId, pem, now = Math.floor(Date.now() / 1000)) {
  const b64 = (o) => Buffer.from(JSON.stringify(o)).toString("base64url");
  const body = `${b64({ alg: "RS256", typ: "JWT" })}.${b64({ iat: now - 60, exp: now + 540, iss: String(appId) })}`;
  const sig = crypto.createSign("RSA-SHA256").update(body).sign(pem).toString("base64url");
  return `${body}.${sig}`;
}

const LEVEL = { none: 0, read: 1, write: 2, admin: 3 };
const PERMISSION_NAMES = {
  contents: "Contents", pull_requests: "Pull requests", issues: "Issues", checks: "Checks", actions: "Actions",
  statuses: "Commit statuses", workflows: "Workflows", metadata: "Metadata", email_addresses: "Email addresses",
};

export function appSettingsUrl(app) {
  const org = app?.owner?.type === "Organization" ? app.owner.login : null;
  return org ? `https://github.com/organizations/${org}/settings/apps/${app.slug}` : `https://github.com/settings/apps/${app?.slug}`;
}

/** The permissions and events that are missing or too weak. */
export function permissionGaps(app) {
  const have = app?.permissions ?? {};
  const perms = Object.entries(PERMISSIONS)
    .filter(([k, need]) => (LEVEL[have[k] ?? "none"] ?? 0) < LEVEL[need])
    .map(([k, need]) => `${PERMISSION_NAMES[k] ?? k}: ${need} (${have[k] ? `now ${have[k]}` : "missing"})`);
  const events = EVENTS.filter((e) => !(app?.events ?? []).includes(e));
  return { perms, events };
}

async function gh(ctx, jwt, p) {
  try {
    const r = await (ctx.fetchImpl ?? fetch)(`https://api.github.com${p}`, {
      headers: { authorization: `Bearer ${jwt}`, accept: "application/vnd.github+json", "user-agent": "coderhelm-selfhost" },
      signal: AbortSignal.timeout(10000),
    });
    return { status: r.status, body: await r.json().catch(() => null) };
  } catch {
    return { status: 0, body: null };
  }
}

/**
 * Every GitHub App check from the stored secret: the keys work, the
 * permissions and events, the webhook URL and its latest deliveries (a 401
 * there means the webhook secret doesn't match), the installations, and
 * the OAuth client id and secret.
 */
export async function checkGithubApp(ctx, secret, publicUrl, hybrid = null) {
  const out = [];
  // Hybrid with CoderHelm's GitHub App: the install has no app of its own
  // to check (its absence isn't a failure).
  if (hybrid?.github_mode === "hosted") {
    out.push(pass("github_keys", "GitHub App created", "CoderHelm's GitHub App (hybrid)"));
    return out;
  }
  const hasKeys = secret?.github_app_id && String(secret.github_private_key ?? "").includes("PRIVATE KEY");
  if (!hasKeys) {
    out.push(problem(FAIL, "github_keys", "GitHub App created", {
      what: "No GitHub App is stored for this install.",
      why: "Without it CoderHelm can't read code, open pull requests or hear about issues.",
      fix: { text: "Create it (one click on github.com, from a machine with a browser).", command: `${CLI} github-app` },
    }));
    return out;
  }
  let jwt;
  try {
    jwt = appJwt(secret.github_app_id, secret.github_private_key);
  } catch {
    out.push(problem(FAIL, "github_keys", "GitHub App created", {
      what: "The stored private key can't be read.",
      why: "CoderHelm signs every call to GitHub with it.",
      fix: { text: "Generate a new private key on the app's page, then store the app again.", command: `${CLI} github-app --force` },
    }));
    return out;
  }
  out.push(pass("github_keys", "GitHub App created", `app ${secret.github_app_id}`));
  const app = await gh(ctx, jwt, "/app");
  if (app.status !== 200) {
    out.push(app.status === 0
      ? problem(WARN, "github_app", "GitHub accepts the app's keys", {
          what: "GitHub couldn't be reached.", why: "The app checks need GitHub.",
          fix: { text: "Check your network, then check again.", url: "https://www.githubstatus.com" },
        })
      : problem(FAIL, "github_app", "GitHub accepts the app's keys", {
          what: `GitHub refused the stored app id ${secret.github_app_id} and private key (${app.status}).`,
          why: "Every call CoderHelm makes as the app will fail.",
          fix: { text: "Store the right app id with a new private key, or create a new app.", command: `${CLI} github-app --force` },
        }));
    return out;
  }
  out.push(pass("github_app", "GitHub accepts the app's keys", `${app.body.slug ?? app.body.name}`));
  const settings = appSettingsUrl(app.body);

  const { perms, events } = permissionGaps(app.body);
  if (perms.length || events.length) {
    out.push(problem(FAIL, "github_permissions", "GitHub App permissions and events", {
      what: [perms.length && `Permissions to change: ${perms.join(", ")}.`, events.length && `Events to subscribe to: ${events.join(", ")}.`].filter(Boolean).join(" "),
      why: "Without them CoderHelm can't push some changes or misses the events that start work.",
      fix: { text: "Set them on the app's Permissions and events page, save, then accept the new permissions on each installation.", url: `${settings}/permissions` },
    }));
  } else out.push(pass("github_permissions", "GitHub App permissions and events", "all granted"));

  const [hook, deliveries, installs] = await Promise.all([
    gh(ctx, jwt, "/app/hook/config"),
    gh(ctx, jwt, "/app/hook/deliveries?per_page=10"),
    gh(ctx, jwt, "/app/installations?per_page=100"),
  ]);
  const expected = publicUrl && !publicUrl.includes("<") ? `${publicUrl.replace(/\/+$/, "")}/webhooks/github` : null;
  const got = String(hook.body?.url ?? "").replace(/\/+$/, "");
  if (expected && got !== expected) {
    out.push(problem(FAIL, "github_webhook", "GitHub webhook reaches this install", {
      what: `The app's webhook URL is ${got || "empty"}, not ${expected}.`,
      why: "GitHub sends issue, pull request and CI events there, so CoderHelm never hears about them.",
      fix: { text: `Set Webhook URL to ${expected} in the app's General settings and save.`, url: settings },
    }));
  } else {
    const last = Array.isArray(deliveries.body) ? deliveries.body[0] : null;
    if (!last) {
      out.push(problem(WARN, "github_webhook", "GitHub webhook reaches this install", {
        what: "GitHub hasn't delivered an event yet.",
        why: "The first one arrives when the app is installed or an issue changes.",
        fix: { text: "Install the app on a repository, then check again.", url: `${settings}/advanced` },
      }));
    } else if (last.status_code >= 200 && last.status_code < 300) {
      out.push(pass("github_webhook", "GitHub webhook reaches this install", `last event ${last.event}, ${last.delivered_at}`));
    } else if (last.status_code === 401) {
      out.push(problem(FAIL, "github_webhook", "GitHub webhook reaches this install", {
        what: "The install refused GitHub's last event: the webhook secret doesn't match.",
        why: "Every event is refused until both sides have the same secret.",
        fix: { text: "Store the app again so both sides get the same secret, then redeliver from the app's Advanced page.", command: `${CLI} github-app --force`, url: `${settings}/advanced` },
      }));
    } else {
      out.push(problem(FAIL, "github_webhook", "GitHub webhook reaches this install", {
        what: last.status_code ? `The install answered GitHub's last event (${last.event}) with ${last.status_code}.` : "GitHub couldn't connect to the install for the last event.",
        why: "Events that fail are lost unless redelivered.",
        fix: { text: "Open the delivery on the app's Advanced page for the answer, fix it, then redeliver.", url: `${settings}/advanced` },
      }));
    }
  }

  const n = Array.isArray(installs.body) ? installs.body.length : null;
  if (n === 0) {
    out.push(problem(FAIL, "github_installations", "GitHub App installed", {
      what: "The app isn't installed on any account or organization.",
      why: "CoderHelm only sees repositories the app is installed on.",
      fix: { text: "Install it and choose the repositories.", url: `${app.body.html_url}/installations/new` },
    }));
  } else if (n) out.push(pass("github_installations", "GitHub App installed", `${n} installation${n === 1 ? "" : "s"}`));

  // The OAuth client: GitHub says "bad_verification_code" for a good id and
  // secret with a made-up code, and "incorrect_client_credentials" otherwise.
  if (secret.github_client_id && secret.github_client_secret) {
    try {
      const r = await (ctx.fetchImpl ?? fetch)("https://github.com/login/oauth/access_token", {
        method: "POST",
        headers: { accept: "application/json", "content-type": "application/json", "user-agent": "coderhelm-selfhost" },
        body: JSON.stringify({ client_id: secret.github_client_id, client_secret: secret.github_client_secret, code: "coderhelm-check" }),
        signal: AbortSignal.timeout(10000),
      });
      const j = await r.json().catch(() => ({}));
      if (j.error === "incorrect_client_credentials") {
        out.push(problem(FAIL, "github_oauth", "GitHub sign-in client", {
          what: "GitHub refused the stored OAuth client id and secret.",
          why: "Connecting a GitHub account in CoderHelm fails until they match.",
          fix: { text: "Generate a new client secret on the app's page and store the app again.", command: `${CLI} github-app --force`, url: settings },
        }));
      } else out.push(pass("github_oauth", "GitHub sign-in client", "client id and secret accepted"));
    } catch {
      // Network trouble here is already reported by the checks above.
    }
  }
  return out;
}

// ── Chat webhooks ─────────────────────────────────────────────────────────

/** A Slack or Teams incoming webhook answers (an empty post is refused as a bad payload, not as unknown). */
export async function checkWebhook(ctx, url) {
  if (!url) return null;
  const kind = /hooks\.slack\.com/.test(url) ? "Slack" : "Teams";
  const title = `${kind} webhook for update notices`;
  let status = 0;
  try {
    status = (await (ctx.fetchImpl ?? fetch)(url, { method: "POST", headers: { "content-type": "application/json" }, body: "{}", signal: AbortSignal.timeout(8000) })).status;
  } catch {
    status = 0;
  }
  if ((status >= 200 && status < 300) || status === 400) return pass("chat_webhook", title, "the webhook answers");
  return problem(FAIL, "chat_webhook", title, {
    what: status ? `The webhook answered ${status}: it was removed or the URL is wrong.` : "The webhook URL couldn't be reached.",
    why: "Update notices would only go by email.",
    fix: kind === "Slack"
      ? { text: "Create an incoming webhook for the channel and paste its URL.", url: "https://api.slack.com/messaging/webhooks" }
      : { text: "Create a Workflows (Power Automate) webhook for the channel and paste its URL.", url: "https://support.microsoft.com/en-us/office/create-incoming-webhooks-with-workflows-for-microsoft-teams-8ae491c7-0394-4861-ba59-055e33f75498" },
  });
}

// ── An existing install's health ──────────────────────────────────────────

export async function checkStacks(ctx, cfg) {
  const d = derived(cfg);
  const title = "CloudFormation stacks";
  const s = await aws(ctx, ["cloudformation", "list-stacks", "--stack-status-filter",
    "CREATE_COMPLETE", "UPDATE_COMPLETE", "UPDATE_ROLLBACK_COMPLETE", "UPDATE_IN_PROGRESS", "UPDATE_ROLLBACK_FAILED", "CREATE_FAILED", "ROLLBACK_COMPLETE"], cfg.region, { StackSummaries: [] });
  if (ctx.runner.dryRun) return pass("stacks", title, "dry run");
  const ours = (s?.StackSummaries ?? []).filter((x) => x.StackName.startsWith(`${d.prefix}-`));
  const bad = ours.filter((x) => !/_COMPLETE$/.test(x.StackStatus) || /ROLLBACK/.test(x.StackStatus));
  const console = `https://console.aws.amazon.com/cloudformation/home?region=${cfg.region}#/stacks`;
  if (!ours.length) {
    return problem(FAIL, "stacks", title, {
      what: `No stacks named ${d.prefix}-* in ${cfg.region}.`,
      why: "CoderHelm isn't deployed here (or the config names another install).",
      fix: { text: "Deploy it, or check stage and region in the config.", command: `${CLI} deploy` },
    });
  }
  if (bad.length) {
    return problem(FAIL, "stacks", title, {
      what: `Not healthy: ${bad.map((x) => `${x.StackName} (${x.StackStatus})`).join(", ")}.`,
      why: "A stack stuck in rollback keeps the install from updating.",
      fix: { text: "Open the stack's Events tab for the first failure, fix it, then deploy again.", command: `${CLI} deploy`, url: console },
    });
  }
  return pass("stacks", title, `${ours.length} stacks complete`);
}

export async function checkFunctions(ctx, cfg) {
  const d = derived(cfg);
  const out = [];
  for (const fn of ["gateway", "worker", "streaming"]) {
    const c = await aws(ctx, ["lambda", "get-function-configuration", "--function-name", `${d.prefix}-${fn}`], cfg.region, { State: "Active", LastUpdateStatus: "Successful" });
    const ok = c?.State === "Active" && c?.LastUpdateStatus !== "Failed";
    out.push(ok
      ? pass(`fn_${fn}`, `Function ${fn}`, `${c.State}`)
      : problem(FAIL, `fn_${fn}`, `Function ${fn}`, {
          what: c ? `${d.prefix}-${fn} is ${c.State}/${c.LastUpdateStatus}${c.StateReason ? `: ${c.StateReason}` : ""}.` : `${d.prefix}-${fn} wasn't found.`,
          why: fn === "worker" ? "Runs can't start without it." : "The dashboard and webhooks go through it.",
          fix: { text: "Deploy again; it replaces the function.", command: `${CLI} deploy`, url: `https://console.aws.amazon.com/lambda/home?region=${cfg.region}#/functions/${d.prefix}-${fn}` },
        }));
  }
  return out;
}

export async function checkDlq(ctx, cfg) {
  const d = derived(cfg);
  const q = await aws(ctx, ["sqs", "get-queue-url", "--queue-name", `${d.prefix}-dlq`], cfg.region, { QueueUrl: "dry-run" });
  if (!q?.QueueUrl) return null;
  const a = await aws(ctx, ["sqs", "get-queue-attributes", "--queue-url", q.QueueUrl, "--attribute-names", "ApproximateNumberOfMessages"], cfg.region, { Attributes: { ApproximateNumberOfMessages: "0" } });
  const n = Number(a?.Attributes?.ApproximateNumberOfMessages ?? 0);
  if (!n) return pass("dlq", "No failed events waiting", "dead-letter queue empty");
  return problem(WARN, "dlq", "No failed events waiting", {
    what: `${n} event${n === 1 ? "" : "s"} failed and wait in the dead-letter queue.`,
    why: "Each is work CoderHelm didn't do, such as a ticket that never started.",
    fix: { text: "Open the failed events page in the dashboard to retry or clear each one.", url: cfg.domain?.hostname ? `https://${cfg.domain.hostname}/admin/queue/` : undefined },
  });
}

/** The install's hybrid settings (Settings, System, Hybrid), or null. */
export async function readHybrid(ctx, cfg) {
  const d = derived(cfg);
  const key = JSON.stringify({ pk: { S: "SYSTEM" }, sk: { S: "HYBRID" } });
  const r = await aws(ctx, ["dynamodb", "get-item", "--table-name", `${d.prefix}-settings`, "--key", key], cfg.region, {});
  try {
    return r?.Item?.data?.S ? JSON.parse(r.Item.data.S) : null;
  } catch {
    return null;
  }
}

export async function checkSecretStored(ctx, cfg) {
  const d = derived(cfg);
  const s = await readSecret(ctx.runner, d.secretName, cfg.region);
  if (s?.jwt_secret || ctx.runner.dryRun) return { result: pass("secret", "Install secret", d.secretName), secret: s };
  return {
    secret: s,
    result: problem(FAIL, "secret", "Install secret", {
      what: `The secret ${d.secretName} is missing or unreadable.`,
      why: "Sign-in and the GitHub App read their keys from it.",
      fix: { text: "Deploy again; it recreates the secret (then store the GitHub App again).", command: `${CLI} deploy` },
    }),
  };
}

/** The dashboard and the API answer at the public address (DNS and TLS included). */
export async function checkUrls(ctx, url, cfg = {}) {
  if (!url || url.includes("<") || ctx.runner.dryRun) return [];
  const f = ctx.fetchImpl ?? fetch;
  const host = (() => {
    try {
      return new URL(url).hostname;
    } catch {
      return url;
    }
  })();
  const out = [];
  let page;
  try {
    page = await f(`${url}/`, { signal: AbortSignal.timeout(10000) });
  } catch (e) {
    const code = e?.cause?.code ?? e?.code ?? "";
    const dnsFail = /ENOTFOUND|EAI_AGAIN/.test(code);
    const tlsFail = /CERT|SSL|TLS/i.test(code);
    out.push(problem(FAIL, "url", "Web address answers", {
      what: dnsFail ? `${host} doesn't resolve in public DNS.` : tlsFail ? `${host} answered with a certificate browsers refuse (${code}).` : `${url} didn't answer.`,
      why: "People and GitHub reach the install at this address.",
      fix: dnsFail
        ? { text: `Add the DNS record for ${host}: a CNAME (or Route 53 alias) to the CloudFront address the deploy printed, then check again.`, command: `dig +short ${host}` }
        : tlsFail
          ? { text: "Check the certificate in ACM (us-east-1) is issued and covers this name.", url: "https://us-east-1.console.aws.amazon.com/acm/home?region=us-east-1#/certificates/list" }
          : { text: "Check the CloudFront distribution is deployed, then check again.", url: "https://console.aws.amazon.com/cloudfront/v4/home#/distributions" },
    }));
    return out;
  }
  out.push(page.status === 200
    ? pass("url", "Web address answers", `HTTP 200 at ${url}`)
    : problem(FAIL, "url", "Web address answers", {
        what: `${url} answered ${page.status}.`,
        why: "The dashboard isn't reachable.",
        fix: { text: "Deploy again; the dashboard files are uploaded with it.", command: `${CLI} deploy` },
      }));
  const api = await f(`${url}/api/me`, { signal: AbortSignal.timeout(10000) }).catch(() => null);
  out.push(api && (api.status === 401 || api.status === 200)
    ? pass("api", "API answers", `HTTP ${api.status}`)
    : problem(FAIL, "api", "API answers", {
        what: api ? `${url}/api answered ${api.status}.` : `${url}/api didn't answer.`,
        why: "The dashboard and webhooks go through it.",
        fix: { text: "Check the gateway function's logs for errors, then deploy again.", command: `aws logs tail /aws/lambda/${cfg.stage ? derived(cfg).prefix : "coderhelm-prod"}-gateway --since 15m${cfg.region ? ` --region ${cfg.region}` : ""}` },
      }));
  return out;
}

// ── Showing results ───────────────────────────────────────────────────────

const MARK = { pass: "PASS", warn: "WARN", fail: "FAIL", skip: "SKIP" };

/** The lines for one result: a mark and the title; for a problem, what, why and fix. */
export function formatResult(r) {
  const lines = [`  ${MARK[r.status].padEnd(4)}  ${r.title}${r.status === PASS && r.detail ? `  (${r.detail})` : ""}`];
  if (r.status === PASS || r.status === SKIP && !r.what) return lines;
  const pad = "        ";
  if (r.what) lines.push(`${pad}What's wrong: ${r.what}`);
  if (r.why) lines.push(`${pad}Why it matters: ${r.why}`);
  const f = r.fix ?? {};
  if (f.text) lines.push(`${pad}How to fix: ${f.text}`);
  if (f.command) for (const c of String(f.command).split("\n")) lines.push(`${pad}  ${c}`);
  if (f.url) lines.push(`${pad}  ${f.url}`);
  return lines;
}

export function formatResults(results) {
  return results.filter(Boolean).flatMap(formatResult).join("\n");
}

export function tally(results) {
  const r = results.filter(Boolean);
  const n = (s) => r.filter((x) => x.status === s).length;
  return { pass: n(PASS), warn: n(WARN), fail: n(FAIL), skip: n(SKIP), ok: n(FAIL) === 0 };
}
