#!/usr/bin/env bash
# Builds the Rust Lambdas (gateway, worker) the one way the deploy and the
# release both use, into <out>/gateway.zip and <out>/worker.zip. The toolchain
# is services/rust-toolchain.toml; the caller installs it, zig and
# cargo-zigbuild 0.24.0. This file is part of the Rust inputs digest
# (scripts/rust-inputs.mjs): changing how the Lambdas are built makes the next
# deploy and release build again.
#
#   bash scripts/rust-build.sh <out>
set -euo pipefail

out=$(mkdir -p "${1:?usage: rust-build.sh <out>}" && cd "$1" && pwd)
root=$(cd "$(dirname "$0")/.." && pwd)

v=$(cargo zigbuild --version 2>/dev/null | awk '{print $2}')
[ "$v" = "0.24.0" ] || { echo "cargo-zigbuild 0.24.0 is needed (found ${v:-none})" >&2; exit 1; }

(cd "$root/services" && cargo zigbuild --locked --workspace --release --target aarch64-unknown-linux-musl)

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
for b in gateway worker; do
  cp "$root/services/target/aarch64-unknown-linux-musl/release/$b" "$tmp/bootstrap"
  rm -f "$out/$b.zip"
  (cd "$tmp" && zip -q -X "$out/$b.zip" bootstrap)
  rm "$tmp/bootstrap"
done
