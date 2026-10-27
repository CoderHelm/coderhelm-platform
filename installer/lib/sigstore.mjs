// Reads the signing certificate out of a cosign bundle (SHA256SUMS.sigstore.json)
// and checks who it was issued to, before cosign checks the signature itself.
//
// A Sigstore keyless certificate (Fulcio) names the GitHub Actions workflow
// that signed: its subjectAltName is the workflow's URL at the ref it ran on,
// and extensions under 1.3.6.1.4.1.57264.1 carry the OIDC issuer, the
// repository and the ref. Checking these here as well as with cosign's own
// flags means a release signed by another repository, another workflow, a
// branch or another tag is refused with a plain reason, and that a cosign
// left out of the picture (the GitHub CLI path) still never accepts a
// bundle made for something else.
import crypto from "node:crypto";

/** Fulcio's certificate extensions (https://github.com/sigstore/fulcio/blob/main/docs/oid-info.md). */
export const OID = {
  issuerV1: "1.3.6.1.4.1.57264.1.1",
  githubWorkflowSha: "1.3.6.1.4.1.57264.1.3",
  issuer: "1.3.6.1.4.1.57264.1.8",
  buildSignerUri: "1.3.6.1.4.1.57264.1.9",
  buildSignerDigest: "1.3.6.1.4.1.57264.1.10",
  sourceRepositoryUri: "1.3.6.1.4.1.57264.1.12",
  sourceRepositoryDigest: "1.3.6.1.4.1.57264.1.13",
  sourceRepositoryRef: "1.3.6.1.4.1.57264.1.14",
};

/** DER bytes of an OBJECT IDENTIFIER's value (without tag and length). */
export function encodeOid(oid) {
  const parts = oid.split(".").map(Number);
  const out = [40 * parts[0] + parts[1]];
  for (const n of parts.slice(2)) {
    const bytes = [n & 0x7f];
    for (let v = n >>> 7; v > 0; v >>>= 7) bytes.unshift((v & 0x7f) | 0x80);
    out.push(...bytes);
  }
  return Buffer.from(out);
}

/** Reads a DER length at `i`; returns [length, index after it]. */
function readLength(buf, i) {
  const first = buf[i];
  if (first < 0x80) return [first, i + 1];
  const n = first & 0x7f;
  if (n === 0 || n > 4 || i + 1 + n > buf.length) throw new Error("bad DER length");
  let len = 0;
  for (let k = 0; k < n; k++) len = len * 256 + buf[i + 1 + k];
  return [len, i + 1 + n];
}

/**
 * The value of a certificate extension, as text, or null when the
 * certificate has none. The extension is `SEQUENCE { OID, [BOOLEAN], OCTET
 * STRING }`; Fulcio's newer extensions hold a DER UTF8String inside the
 * OCTET STRING, the first one (issuer v1) holds the raw text.
 */
export function extensionValue(der, oid) {
  const needle = Buffer.concat([Buffer.from([0x06, encodeOid(oid).length]), encodeOid(oid)]);
  const at = der.indexOf(needle);
  if (at < 0) return null;
  let i = at + needle.length;
  if (der[i] === 0x01) {
    const [len, next] = readLength(der, i + 1);
    i = next + len;
  }
  if (der[i] !== 0x04) return null;
  const [len, start] = readLength(der, i + 1);
  const inner = der.subarray(start, start + len);
  if (inner[0] === 0x0c) {
    const [l2, s2] = readLength(inner, 1);
    return inner.subarray(s2, s2 + l2).toString("utf8");
  }
  return inner.toString("utf8");
}

/** The certificate (DER) and whether a transparency log entry is in a cosign bundle (old or new format). */
export function readBundle(text) {
  let b;
  try {
    b = JSON.parse(text);
  } catch {
    throw new Error("the signature bundle is not valid JSON");
  }
  let der = null;
  // cosign 2.x `sign-blob --bundle`: base64 of the PEM certificate.
  if (typeof b?.cert === "string") {
    const pem = Buffer.from(b.cert, "base64").toString("utf8");
    der = new crypto.X509Certificate(pem.includes("BEGIN CERTIFICATE") ? pem : Buffer.from(b.cert, "base64")).raw;
  }
  // Sigstore bundle v0.2/v0.3 (cosign --new-bundle-format, cosign 3).
  const vm = b?.verificationMaterial;
  const raw = vm?.certificate?.rawBytes ?? vm?.x509CertificateChain?.certificates?.[0]?.rawBytes;
  if (!der && typeof raw === "string") der = new crypto.X509Certificate(Buffer.from(raw, "base64")).raw;
  if (!der) throw new Error("the signature bundle has no signing certificate");
  const tlog = Boolean(b?.rekorBundle?.SignedEntryTimestamp || b?.rekorBundle?.Payload?.logIndex !== undefined)
    || (Array.isArray(vm?.tlogEntries) && vm.tlogEntries.some((e) => e?.logIndex !== undefined && (e?.inclusionPromise || e?.inclusionProof)));
  return { der, tlog };
}

/** Who the certificate names: its URI subjectAltName and the Fulcio extensions. */
export function certificateClaims(der) {
  const x = new crypto.X509Certificate(der);
  const uris = String(x.subjectAltName ?? "")
    .split(/,\s*/)
    .filter((s) => s.startsWith("URI:"))
    .map((s) => s.slice(4));
  return {
    identities: uris,
    issuer: extensionValue(der, OID.issuer) ?? extensionValue(der, OID.issuerV1),
    repository: extensionValue(der, OID.sourceRepositoryUri),
    ref: extensionValue(der, OID.sourceRepositoryRef),
    buildSigner: extensionValue(der, OID.buildSignerUri),
    buildSignerDigest: extensionValue(der, OID.buildSignerDigest),
    sourceDigest: extensionValue(der, OID.sourceRepositoryDigest),
    workflowSha: extensionValue(der, OID.githubWorkflowSha),
  };
}

/**
 * Throws unless the bundle's certificate was issued to exactly `identity`
 * by `issuer`, for `repositoryUri` at `ref`, built from exactly `commit`
 * (the commit the release's signed tag points to: the certificate's source
 * repository digest must be it, and its build signer digest and workflow
 * SHA too when present), and the bundle carries a transparency log (Rekor)
 * entry. This is a pre-check: the signature and the certificate chain are
 * checked by cosign (or the GitHub CLI), which are also given the commit.
 */
export function checkBundleIdentity(text, { identity, issuer, repositoryUri, ref, commit }) {
  if (!/^[0-9a-f]{40}$/.test(String(commit))) throw new Error("no source commit to check the certificate against");
  const { der, tlog } = readBundle(text);
  const c = certificateClaims(der);
  const problems = [];
  if (c.identities.length !== 1 || c.identities[0] !== identity) problems.push(`it was signed by ${c.identities.join(", ") || "an unnamed signer"}, not ${identity}`);
  if (c.issuer !== issuer) problems.push(`its certificate was issued for ${c.issuer ?? "an unknown issuer"}, not ${issuer}`);
  if (c.repository !== null && c.repository !== repositoryUri) problems.push(`it was built from ${c.repository}, not ${repositoryUri}`);
  if (c.ref !== null && c.ref !== ref) problems.push(`it was built from ${c.ref}, not ${ref}`);
  if (c.buildSigner !== null && c.buildSigner !== identity) problems.push(`its build signer is ${c.buildSigner}, not ${identity}`);
  if (c.sourceDigest !== commit) problems.push(`it was built from commit ${c.sourceDigest ?? "(none named)"}, not ${commit} (the commit the signed tag points to)`);
  if (c.buildSignerDigest !== null && c.buildSignerDigest !== commit) problems.push(`its workflow file came from commit ${c.buildSignerDigest}, not ${commit}`);
  if (c.workflowSha !== null && c.workflowSha !== commit) problems.push(`its workflow ran on commit ${c.workflowSha}, not ${commit}`);
  if (!tlog) problems.push("it has no transparency log (Rekor) entry");
  if (problems.length) throw new Error(problems.join("; "));
  return c;
}
