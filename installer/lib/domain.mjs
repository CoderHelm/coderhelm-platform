// Your own domain: `coderhelm-selfhost domain apply`.
//
// The updater (lambda/updater) requests the certificate and waits for it.
// Once ACM has issued it, the update build runs this command with the
// release that already runs: it writes the new address into the config and
// deploys. CloudFront then answers on the hostname with that certificate,
// and with a Route 53 zone in this account the A and AAAA records are made
// too. `--action remove` goes back to CloudFront's own address.
//
// It also works from your machine, with a certificate you made yourself:
//
//   coderhelm-selfhost domain apply --hostname coderhelm.example.com \
//     --certificate-arn arn:aws:acm:us-east-1:123456789012:certificate/...
//
// docs/self-host/domains.md has the whole flow.
import path from "node:path";
import { deploy, loadConfig, loadState, configPath } from "./commands.mjs";
import { writeJson } from "./util.mjs";

const HOST_RE = /^(?=.{4,253}$)([a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}$/;
const CERT_RE = /^arn:aws[a-z-]*:acm:us-east-1:\d{12}:certificate\/[0-9a-f-]+$/;
const ZONE_ID_RE = /^Z[A-Z0-9]{1,32}$/;

/**
 * The config with the domain applied (or removed). Throws one plain
 * sentence when the values can't work, before anything is deployed.
 */
export function withDomain(cfg, { action = "attach", hostname, certificateArn, zone, zoneId } = {}) {
  if (action === "remove") {
    if (cfg.integrations?.googleSignIn) {
      throw new Error("Google sign-in needs a domain: turn integrations.googleSignIn off first, then go back to the AWS address.");
    }
    return { ...cfg, domain: null };
  }
  if (action !== "attach") throw new Error(`--action is attach or remove, not ${action}.`);
  const host = String(hostname ?? "").trim().toLowerCase().replace(/\.$/, "");
  const problems = [];
  if (!HOST_RE.test(host)) problems.push("--hostname must be a hostname such as coderhelm.example.com");
  if (!CERT_RE.test(String(certificateArn ?? ""))) problems.push("--certificate-arn must be an ACM certificate in us-east-1 (CloudFront only uses those)");
  const z = String(zone ?? "").trim().toLowerCase().replace(/\.$/, "");
  if (z && host && host !== z && !host.endsWith(`.${z}`)) problems.push(`${host} is not inside the zone ${z}`);
  if (zoneId && !ZONE_ID_RE.test(String(zoneId))) problems.push("--zone-id must be a Route 53 zone id such as Z0123456789ABC");
  if (problems.length) throw new Error(`The domain can't be applied: ${problems.join("; ")}.`);
  const domain = { hostname: host, certificateArn };
  if (z) {
    domain.hostedZone = z;
    if (zoneId) domain.hostedZoneId = zoneId;
  }
  return { ...cfg, domain };
}

/** What changes, in words. */
export function describeChange(before, after) {
  const was = before?.hostname ? `https://${before.hostname}` : "the AWS address";
  const now = after?.hostname ? `https://${after.hostname}` : "the AWS address";
  return `${was} to ${now}`;
}

export async function domainApply(ctx) {
  const { flags, runner, log } = ctx;
  const result = { status: "failed", reason: "" };
  const done = (status, reason) => {
    Object.assign(result, { status, reason });
    if (flags.resultFile && !runner.dryRun) writeJson(path.resolve(flags.resultFile), result);
    return result;
  };
  try {
    const cfg = loadConfig(flags);
    const next = withDomain(cfg, {
      action: flags.action === true ? "attach" : flags.action || "attach",
      hostname: flags.hostname,
      certificateArn: flags.certificateArn,
      zone: flags.zone === true ? "" : flags.zone,
      zoneId: flags.zoneId === true ? "" : flags.zoneId,
    });
    const change = describeChange(cfg.domain, next.domain);
    log(`Changing the address from ${change}`);
    if (!runner.dryRun) writeJson(configPath(flags), next);
    const version = flags.version ?? loadState(flags).version;
    try {
      // The same release, the new config; no bootstrap (the certificate is
      // already in us-east-1, so no stack goes there) and no GitHub App step.
      await deploy({ ...ctx, flags: { ...flags, version, skipBootstrap: true, skipGithubApp: true, quiet: true } });
    } catch (e) {
      if (!runner.dryRun) writeJson(configPath(flags), cfg);
      throw e;
    }
    const where = next.domain ? `https://${next.domain.hostname}` : "its AWS address";
    log(`\nCoderHelm answers on ${where}.`);
    if (next.domain && !next.domain.hostedZone) {
      log(`Point ${next.domain.hostname} at CloudFront with the CNAME record Settings, then System shows (or the deploy's DnsRecord output).`);
    }
    log("Update the GitHub App's URLs to the new address (Settings, then Setup shows what to change).");
    return done("succeeded", `now on ${where}`);
  } catch (e) {
    done("failed", String(e.message ?? e).split("\n")[0].slice(0, 400));
    throw e;
  }
}

export async function domain(ctx) {
  const sub = ctx.flags._[1];
  if (sub === "apply") return domainApply(ctx);
  throw new Error('Usage: coderhelm-selfhost domain apply --hostname <host> --certificate-arn <arn> [--zone <zone> --zone-id <id>] | --action remove\nTo add a domain with checks and the records to add, use Settings, then System in CoderHelm.');
}
