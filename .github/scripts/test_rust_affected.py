import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(__file__))

from rust_affected import affects_rust  # noqa: E402


class AffectsRust(unittest.TestCase):
    def test_pin_and_docs_only_skip(self):
        for p in [
            "selfhost/versions.json",
            "README.md",
            "docs/incidents.md",
            "coderhelm-jira/manifest.yml",
            ".github/workflows/deploy.yml",
        ]:
            self.assertFalse(affects_rust(p), p)

    def test_rust_inputs_run(self):
        for p in [
            "services/worker/src/main.rs",
            "services/common/contract/messages.json",
            "services/Cargo.lock",
            "services/rust-toolchain.toml",
            "rustfmt.toml",
            ".cargo/config.toml",
            "services/.cargo/config.toml",
            ".github/workflows/ci.yml",
            ".github/scripts/rust_affected.py",
            "lambda/log-analyzer/model_prices.json",
            "infra/cfn/coderhelm-log-reader.yaml",
            "docs/api/openapi.json",
            "installer/lib/github-app.mjs",
            "coderhelm-jira/src/index.js",
            "selfhost/install.sh",
            "selfhost/launch/check.mjs",
        ]:
            self.assertTrue(affects_rust(p), p)


if __name__ == "__main__":
    unittest.main()
