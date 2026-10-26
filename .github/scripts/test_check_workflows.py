"""Tests for check_workflows.py, and the repository's own workflows against it.

Run: python3 -m unittest discover -s .github/scripts
"""

import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import check_workflows as cw  # noqa: E402

SHA = "11d5960a326750d5838078e36cf38b85af677262"


def wf(steps, on="push:\n  branches: [main]", top_permissions=True):
    perms = "permissions:\n  contents: read\n" if top_permissions else ""
    body = "\n".join(f"      {s}" for s in steps)
    return f"name: t\non:\n  {on.replace(chr(10), chr(10) + '  ')}\n{perms}jobs:\n  a:\n    runs-on: ubuntu-latest\n    steps:\n{body}\n"


class Pins(unittest.TestCase):
    def test_sha_with_version_comment_passes(self):
        self.assertEqual(cw.check_text("w", wf([f"- uses: actions/checkout@{SHA} # v4.4.0"])), [])

    def test_tags_branches_and_short_shas_fail(self):
        for ref in ("actions/checkout@v4", "dtolnay/rust-toolchain@stable", "actions/checkout@11d5960", "actions/checkout@main"):
            p = cw.check_text("w", wf([f"- uses: {ref}"]))
            self.assertTrue(any("full commit SHA" in x for x in p), ref)

    def test_a_sha_needs_its_version_in_a_comment(self):
        p = cw.check_text("w", wf([f"- uses: actions/checkout@{SHA}"]))
        self.assertTrue(any("version it stands for" in x for x in p))

    def test_local_and_digest_pinned_docker_pass(self):
        self.assertEqual(cw.check_text("w", wf(["- uses: ./.github/actions/x", "- uses: docker://alpine@sha256:" + "a" * 64])), [])
        self.assertTrue(cw.check_text("w", wf(["- uses: docker://alpine:3"])))


class Triggers(unittest.TestCase):
    def test_pull_request_target_and_workflow_run_fail(self):
        for on in ("pull_request_target:\n  branches: [main]", "workflow_run:\n  workflows: [CI]"):
            p = cw.check_text("w", wf([f"- uses: actions/checkout@{SHA} # v4.4.0"], on=on))
            self.assertTrue(any("hands this repository's secrets" in x for x in p), on)


class Scripts(unittest.TestCase):
    def test_untrusted_values_in_run_fail(self):
        for expr in ("${{ github.event.pull_request.title }}", "${{ github.head_ref }}", "${{ inputs.tag }}", "${{ github.event.inputs.tag || github.ref_name }}"):
            p = cw.check_text("w", wf([f'- run: echo "{expr}"']))
            self.assertTrue(any("pass it through env" in x for x in p), expr)

    def test_env_is_fine(self):
        steps = ["- env:", "    T: ${{ github.event.pull_request.title }}", '  run: echo "$T"']
        self.assertEqual(cw.check_text("w", wf(steps)), [])

    def test_permissions_are_required(self):
        p = cw.check_text("w", wf([f"- uses: actions/checkout@{SHA} # v4.4.0"], top_permissions=False))
        self.assertTrue(any("has no permissions" in x for x in p))


class ThisRepository(unittest.TestCase):
    def test_every_workflow_here_passes(self):
        root = pathlib.Path(__file__).resolve().parents[1] / "workflows"
        files, problems = cw.check_dir(root)
        self.assertGreater(len(files), 0)
        self.assertEqual(problems, [])


if __name__ == "__main__":
    unittest.main()
