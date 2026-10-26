#!/bin/sh
# Regenerates signers.json: self-signed stand-ins for Sigstore (Fulcio)
# certificates, each naming a different signer, for the signature tests
# (test/release-security.test.mjs). They carry the same subjectAltName and
# Fulcio extensions as a real GitHub Actions certificate; the signature
# itself is cosign's job and is stubbed in the tests.
#
#   sh test/fixtures/make-signers.sh > test/fixtures/signers.json
set -eu
dir=$(mktemp -d)
trap 'rm -rf "$dir"' EXIT
GH=https://github.com
WF=.github/workflows
ISS=https://token.actions.githubusercontent.com
cert() { # name repo workflow-file ref issuer
  openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -keyout "$dir/k" -subj "/O=sigstore.dev" -days 3650 \
    -addext "subjectAltName=URI:$GH/$2/$WF/$3@$4" \
    -addext "1.3.6.1.4.1.57264.1.8=ASN1:UTF8String:$5" \
    -addext "1.3.6.1.4.1.57264.1.9=ASN1:UTF8String:$GH/$2/$WF/$3@$4" \
    -addext "1.3.6.1.4.1.57264.1.12=ASN1:UTF8String:$GH/$2" \
    -addext "1.3.6.1.4.1.57264.1.14=ASN1:UTF8String:$4" \
    -out "$dir/$1.pem" 2>/dev/null
  printf '  "%s": %s' "$1" "$(node -p 'JSON.stringify(require("fs").readFileSync(process.argv[1], "utf8"))' "$dir/$1.pem")"
}
R=CoderHelm/coderhelm-platform
echo "{"
cert good "$R" release.yml refs/tags/v1.2.3 "$ISS"; echo ","
cert otherRepo evil/coderhelm-platform release.yml refs/tags/v1.2.3 "$ISS"; echo ","
cert otherWorkflow "$R" ci.yml refs/tags/v1.2.3 "$ISS"; echo ","
cert branch "$R" release.yml refs/heads/main "$ISS"; echo ","
cert otherTag "$R" release.yml refs/tags/v1.0.0 "$ISS"; echo ","
cert otherIssuer "$R" release.yml refs/tags/v1.2.3 https://accounts.google.com
echo
echo "}"
