"""Tests for text_guard.py, on synthetic strings only."""

import contextlib
import io
import os
import pathlib
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import text_guard as g  # noqa: E402

def hits(text):
    return g.scan_lines(g.numbered(text), g.rules_for())


def rules(text):
    return {rule for _, rule in hits(text)}


class Rules(unittest.TestCase):
    def test_clean_text_passes(self):
        text = (
            "Runs retry a failed check once.\n\n"
            "The worker reads the plan, then writes the patch; SHA-256 and UTF-8 stay as they were.\n"
            "See CVE-2024-12345 and RFC-7231. Example: arn:aws:iam::123456789012:role/x."
        )
        self.assertEqual(hits(text), [])

    def test_aws_account_ids(self):
        self.assertIn("AWS account id", rules("arn:aws:lambda:us-east-1:418273645091:function:w"))
        self.assertIn("AWS account id", rules("pushed to 418273645091.dkr.ecr.us-east-1.amazonaws.com"))
        self.assertIn("AWS account id", rules("the install's account 4182-7364-5091"))
        self.assertEqual(rules("account 111122223333"), set())
        self.assertIn("AWS account id", rules("AWS_ACCOUNT_ID=418273645091"))
        self.assertEqual(rules("arn:aws:iam::111122223333:root, account id 123456789012"), set())
        self.assertEqual(rules("processed 418273645091 bytes"), {"12-digit number"})
        self.assertEqual(rules("ids 4182-7364-5091 and 111122223333"), {"12-digit number"})
        self.assertEqual(rules("ALAS2023-2025-1234, v1.418273645091, 4182736450912"), set())

    def test_team_ids(self):
        self.assertEqual(rules("pk TEAM#9f3a21"), {"TEAM# id"})
        self.assertEqual(rules("keys look like TEAM#<team_id>"), set())
        self.assertEqual(rules('"team_id":"TEAM#7c1f0e2a"'), set())
        self.assertEqual(rules("TEAM#7c1f0e2ab"), {"TEAM# id"})

    def test_ulid_run_ids(self):
        self.assertEqual(rules("run 01JD8ZQ4M3T7KXW2B9RVNHC6PA failed"), {"ULID run id"})
        self.assertEqual(rules("run 01jd8zq4m3t7kxw2b9rvnhc6pa"), {"ULID run id"})
        self.assertEqual(rules('"run_id": "01JB8X6F4M2C9QZ3T7V5W1K0HA"'), set())
        self.assertEqual(rules("a 26-digit number 12345678901234567890123456"), set())
        self.assertEqual(rules("too long 01JD8ZQ4M3T7KXW2B9RVNHC6PAX"), set())

    def test_jira_keys_and_allowlist(self):
        self.assertEqual(rules("Closes QXZ-4821"), {"Jira-style key"})
        self.assertEqual(rules("branch coderhelm/QXZ-48"), {"Jira-style key"})
        self.assertEqual(rules("SHA-256, AES-256, AVX-512, ISO-8601, ENG-142, WEB-42"), set())
        self.assertEqual(rules("CVE-2025-31337 and GHSA-abcd-1234"), set())
        self.assertEqual(rules("qxz-4821 lower case"), set())

    def test_email_addresses(self):
        self.assertEqual(rules("from pat.lee@zorbmail.io"), {"email address"})
        self.assertEqual(rules("ops@example.com, 1+bot@users.noreply.github.com, git@github.com"), set())
        self.assertEqual(rules("uses: actions/checkout@v4.4.0 and @types/node"), set())

    def test_line_numbers(self):
        self.assertEqual(hits("ok\nok\nmail a@zorbmail.io\nQXZ-11"), [(3, "email address"), (4, "Jira-style key")])


class NoList(unittest.TestCase):
    def test_no_list_of_names_hashed_or_plain(self):
        src = (pathlib.Path(__file__).resolve().parent / "text_guard.py").read_text()
        for gone in ("GUARD_DENYLIST", "sha256", "hashlib", "--hash-terms"):
            self.assertNotIn(gone, src)
        repo = pathlib.Path(__file__).resolve().parents[1]
        self.assertFalse((repo / "text-guard.sha256").exists())


class Diffs(unittest.TestCase):
    DIFF = (
        "diff --git a/docs/a.md b/docs/a.md\n"
        "--- a/docs/a.md\n"
        "+++ b/docs/a.md\n"
        "@@ -3,0 +4,2 @@ heading\n"
        "+first\n"
        "+second\n"
        "@@ -10 +12 @@\n"
        "-gone\n"
        "+third\n"
        "diff --git a/docs/old.md b/docs/old.md\n"
        "--- a/docs/old.md\n"
        "+++ /dev/null\n"
        "@@ -1 +0,0 @@\n"
        "-removed\n"
    )

    def test_added_lines_carry_new_line_numbers(self):
        self.assertEqual(g.added_lines(self.DIFF), {"docs/a.md": [(4, "first"), (5, "second"), (12, "third")]})


class Output(unittest.TestCase):
    def run_main(self, text):
        with tempfile.NamedTemporaryFile("w", suffix=".md", delete=False) as f:
            f.write(text)
        out = io.StringIO()
        try:
            with contextlib.redirect_stdout(out):
                code = g.main(["text_guard.py", "--text", f.name])
        finally:
            os.unlink(f.name)
        return code, out.getvalue()

    def test_failure_names_line_and_rule_never_the_text(self):
        code, out = self.run_main("intro\nShipped, see QXZ-4821 and pat@zorbmail.io\n")
        self.assertEqual(code, 1)
        report = out.splitlines()
        self.assertTrue(any("line 2: Jira-style key" in line for line in report))
        self.assertTrue(any("line 2: email address" in line for line in report))
        joined = "\n".join(report).lower()
        for secret in ("qxz-4821", "zorbmail"):
            self.assertNotIn(secret, joined)

    def test_clean_text_passes(self):
        code, out = self.run_main("Retries a failed check once.\n")
        self.assertEqual(code, 0)
        self.assertIn("no customer details", out)


if __name__ == "__main__":
    unittest.main()
