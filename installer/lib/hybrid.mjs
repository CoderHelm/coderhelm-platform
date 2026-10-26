// Hybrid installs: what the installer does with CoderHelm's hosted side.
//
// On uninstall, an install registered with CoderHelm (Settings, System,
// Hybrid) releases its {name}.coderhelm.com address, unlinks CoderHelm's
// GitHub App and revokes its sign-ins, signed with its registration secret
// exactly as the services sign (services/common/src/hybrid/sign.rs).
import crypto from "node:crypto";

export const DEFAULT_HOSTED_URL = "https://api.coderhelm.com";

/** `v1=<hex>` over `"{ts}.{METHOD}.{path}.{sha256(body) hex}"`. */
export function signRequest(secret, ts, method, path, body = "") {
  const hash = crypto.createHash("sha256").update(body).digest("hex");
  const msg = `${ts}.${method.toUpperCase()}.${path}.${hash}`;
  return `v1=${crypto.createHmac("sha256", secret).update(msg).digest("hex")}`;
}

/** The registration in the install's secret, when it has one. */
export function registration(secret) {
  const id = secret?.hybrid_install_id;
  const key = secret?.hybrid_install_secret;
  return typeof id === "string" && /^ins_[a-z0-9]{24}$/.test(id) && typeof key === "string" && key ? { id, secret: key } : null;
}

/**
 * Leaves hybrid on uninstall. Never fails the uninstall: the hosted side
 * also releases names whose install stops answering.
 */
export async function leaveHybrid(ctx, secret, hostedUrl = DEFAULT_HOSTED_URL) {
  const reg = registration(secret);
  if (!reg) return false;
  const path = "/hybrid/installs";
  const ts = Math.floor(Date.now() / 1000);
  if (ctx.runner?.dryRun) {
    ctx.log(`  [dry run] release CoderHelm address and links for ${reg.id}`);
    return true;
  }
  try {
    const r = await (ctx.fetchImpl ?? fetch)(`${hostedUrl}${path}`, {
      method: "DELETE",
      headers: {
        "x-coderhelm-install": reg.id,
        "x-coderhelm-timestamp": String(ts),
        "x-coderhelm-signature": signRequest(reg.secret, ts, "DELETE", path),
        "user-agent": "coderhelm-selfhost",
      },
      signal: AbortSignal.timeout(8000),
    });
    ctx.log(r.ok ? "  Released the CoderHelm address and links." : `  CoderHelm answered ${r.status}; release the address in Settings, System, Hybrid before uninstalling next time.`);
    return r.ok;
  } catch {
    ctx.log("  Couldn't reach CoderHelm to release the address; it is released when the install stops answering.");
    return false;
  }
}
