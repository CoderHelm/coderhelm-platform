#!/usr/bin/env python3
"""Does this pull request change anything the Rust jobs build, lint or test?

CI's "Rust (clippy)" and "Rust (tests)" jobs run on every pull request (the
required "Rust (workspace)" check waits on both) but skip their steps when
the answer is no, so a pull request that only moves a pin
(selfhost/versions.json) finishes them in seconds instead of most of an hour.

The list is wider than the crates because the workspace reads more than its
own sources:
- include_str!/include_bytes! embed files from lambda/, infra/cfn/,
  infra/lib/, installer/lib/ and docs/api/;
- worker/build.rs hashes services/worker/src and services/common/src;
- tests read or scan files across the repository: model_call_sites.rs walks
  every .js/.mjs/.cjs/.ts/.tsx file, model_ids.rs and run_status.rs read
  lambda/ and infra/lib/, and tests/no_hosted_urls.rs scans services, lambda,
  infra/lib, infra/bin, installer/lib, installer/bin and selfhost.

Anything uncertain (no diff, an unexpected checkout) counts as a change, so
the jobs run in full rather than pass without running.

Usage: rust_affected.py [FILE ...]   (no files: read names from stdin)
Prints "true" or "false".
"""

import sys

# Every file under these runs the Rust jobs.
DIRS = (
    "services/",
    "lambda/",
    "docs/api/",
    "infra/cfn/",
    "infra/lib/",
    "infra/bin/",
    "installer/lib/",
    "installer/bin/",
)

# The files that decide how this workflow runs them.
FILES = (
    ".github/workflows/ci.yml",
    ".github/scripts/rust_affected.py",
)

# Cargo, toolchain and formatter settings, wherever they sit.
NAMES = (
    "Cargo.toml",
    "Cargo.lock",
    "rustfmt.toml",
    ".rustfmt.toml",
    "clippy.toml",
    ".clippy.toml",
)

# Rust sources anywhere, and the JS/TS model_call_sites.rs scans repo-wide.
EXTS = (".rs", ".js", ".mjs", ".cjs", ".ts", ".tsx")

# What tests/no_hosted_urls.rs scans under selfhost/ (versions.json is not).
SELFHOST_EXTS = (".rs", ".ts", ".mjs", ".js", ".py", ".sh", ".yaml", ".yml", ".html")


def affects_rust(path):
    path = path.strip()
    name = path.rsplit("/", 1)[-1]
    return (
        path.startswith(DIRS)
        or path in FILES
        or name in NAMES
        or name.startswith("rust-toolchain")
        or path.startswith(".cargo/")
        or "/.cargo/" in path
        or path.endswith(EXTS)
        or (path.startswith("selfhost/") and path.endswith(SELFHOST_EXTS))
    )


def main(argv):
    paths = argv[1:] or sys.stdin.read().splitlines()
    paths = [p for p in paths if p.strip()]
    # No changed files means the diff did not work: run everything.
    run = not paths or any(affects_rust(p) for p in paths)
    print("true" if run else "false")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
