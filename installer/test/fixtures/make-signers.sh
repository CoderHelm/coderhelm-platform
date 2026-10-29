#!/bin/sh
# Regenerates signers.json: self-signed stand-ins for Sigstore (Fulcio)
# certificates, each naming a different signer, for the signature tests
# (test/release-security.test.mjs). They carry the same subjectAltName and
# Fulcio extensions as a real GitHub Actions certificate (all built from
# commit 1111...1111 unless named otherwise); the signature
# itself is cosign's job and is stubbed in the tests.
#
#   sh test/fixtures/make-signers.sh > test/fixtures/signers.json
set -eu
dir=$(mktemp -d)
trap 'rm -rf "$dir"' EXIT
GH=https://github.com
WF=.github/workflows
ISS=https://token.actions.githubusercontent.com
C=1111111111111111111111111111111111111111
cert() { # name repo workflow-file ref issuer [commit|none]
  c=${6:-$C}
  digest=""
  if [ "$c" != none ]; then
    # 1.3 (GitHub workflow SHA) holds the raw text, like Fulcio's first extensions;
    # 1.10 and 1.13 (build signer and source repository digests) a UTF8String.
    digest="-addext 1.3.6.1.4.1.57264.1.3=DER:$(printf %s "$c" | od -An -v -tx1 | tr -d ' \n')
      -addext 1.3.6.1.4.1.57264.1.10=ASN1:UTF8String:$c
      -addext 1.3.6.1.4.1.57264.1.13=ASN1:UTF8String:$c"
  fi
  # shellcheck disable=SC2086 # $digest is several arguments
  openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -keyout "$dir/k" -subj "/O=sigstore.dev" -days 3650 \
    -addext "subjectAltName=URI:$GH/$2/$WF/$3@$4" \
    -addext "1.3.6.1.4.1.57264.1.8=ASN1:UTF8String:$5" \
    -addext "1.3.6.1.4.1.57264.1.9=ASN1:UTF8String:$GH/$2/$WF/$3@$4" \
    -addext "1.3.6.1.4.1.57264.1.12=ASN1:UTF8String:$GH/$2" \
    -addext "1.3.6.1.4.1.57264.1.14=ASN1:UTF8String:$4" \
    $digest \
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
cert otherIssuer "$R" release.yml refs/tags/v1.2.3 https://accounts.google.com; echo ","
# Right workflow and tag, but built from another commit (a release.yml changed under a new tag).
cert otherCommit "$R" release.yml refs/tags/v1.2.3 "$ISS" 2222222222222222222222222222222222222222; echo ","
cert noCommit "$R" release.yml refs/tags/v1.2.3 "$ISS" none; echo ","
# An automatic release's tag attestation (lib/source-tag.mjs): auto-release.yml
# on main, run on the tag's commit; and the ways it can be wrong.
cert auto "$R" auto-release.yml refs/heads/main "$ISS"; echo ","
cert autoOtherWorkflow "$R" deploy.yml refs/heads/main "$ISS"; echo ","
cert autoOtherRef "$R" auto-release.yml refs/heads/feature "$ISS"; echo ","
cert autoTagRef "$R" auto-release.yml refs/tags/v1.2.3 "$ISS"; echo ","
cert autoOtherCommit "$R" auto-release.yml refs/heads/main "$ISS" 2222222222222222222222222222222222222222; echo ","
cert autoOtherRepo evil/coderhelm-platform auto-release.yml refs/heads/main "$ISS"
echo
echo "}"
