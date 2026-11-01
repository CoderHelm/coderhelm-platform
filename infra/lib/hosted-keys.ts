/**
 * CoderHelm's public signing keys, pinned in every self-hosted release.
 *
 * The hosted side signs everything it sends a hybrid install (relayed events
 * and the answers to the install's own calls) with an Ed25519 key in AWS KMS
 * (lib/hybrid.ts, common::hybrid::hosted_sig). Each install checks those
 * signatures with the keys listed here, which reach it inside its release:
 * the release is signed, so the trust comes from the release signature,
 * never from the network.
 *
 * Each entry is the raw 32-byte public key, base64url without padding (the
 * last 32 bytes of what `aws kms get-public-key` returns). Rotation: add the
 * next key here and release before the hosted side signs with it; remove the
 * old one in a later release to retire it. While this list is empty, installs
 * check the relay key alone, as before; once it holds a key, installs on that
 * release refuse anything CoderHelm didn't sign.
 */
export const HOSTED_SIGNING_KEYS: string[] = [];

/** The keys as the install's HYBRID_HOSTED_KEYS value, each one checked. */
export function hostedKeysEnv(keys: string[] = HOSTED_SIGNING_KEYS): string {
  for (const k of keys) {
    const raw = Buffer.from(k, "base64url");
    if (raw.length !== 32 || raw.toString("base64url") !== k) {
      throw new Error(`hosted-keys.ts: ${k} isn't a 32-byte Ed25519 public key in base64url`);
    }
  }
  return keys.join(",");
}
