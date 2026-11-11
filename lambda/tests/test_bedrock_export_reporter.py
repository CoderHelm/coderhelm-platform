"""The Bedrock export stack's reporter (infra/cfn/coderhelm-bedrock-export.yaml,
Reporter): the inline function that tells the install each step, with the
one-time token, and answers CloudFormation.

Run: python3 -m unittest discover -s lambda/tests
"""

import io
import json
import pathlib
import sys
import types
import unittest
import urllib.error

TEMPLATE = pathlib.Path(__file__).resolve().parents[2] / "infra" / "cfn" / "coderhelm-bedrock-export.yaml"


def inline_code() -> str:
    lines = TEMPLATE.read_text().splitlines()
    start = next(i for i, l in enumerate(lines) if l.strip() == "ZipFile: |") + 1
    indent = len(lines[start]) - len(lines[start].lstrip())
    out = []
    for l in lines[start:]:
        if l.strip() and len(l) - len(l.lstrip()) < indent:
            break
        out.append(l[indent:])
    return "\n".join(out)


class FakeCfn:
    def __init__(self, status="CREATE_IN_PROGRESS", events=None):
        self.status, self.events = status, events or []

    def describe_stacks(self, StackName):
        return {"Stacks": [{"StackStatus": self.status}]}

    def describe_stack_events(self, StackName):
        return {"StackEvents": self.events}


def load(cfn=None, refuse=None, unreachable=False):
    """The function, with boto3 and the network replaced: returns it and the
    list of calls it made (url, method, body)."""
    calls = []
    fake_boto3 = types.ModuleType("boto3")
    fake_boto3.client = lambda name: cfn or FakeCfn()
    sys.modules["boto3"] = fake_boto3
    mod = types.ModuleType("reporter")
    exec(compile(inline_code(), "reporter", "exec"), mod.__dict__)

    class Answer(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    def urlopen(req, timeout=0):
        body = json.loads(req.data)
        calls.append((req.full_url, req.get_method(), body))
        if req.full_url.startswith("https://ch.example.org"):
            if unreachable:
                raise urllib.error.URLError("timed out")
            if refuse:
                raise urllib.error.HTTPError(req.full_url, 409, "no", {}, io.BytesIO(json.dumps({"error": refuse}).encode()))
        return Answer(b"{}")

    mod.urllib.request.urlopen = urlopen
    return mod, calls


STACK = "arn:aws:cloudformation:us-east-1:111122223333:stack/coderhelm-prod-bedrock-export/abc"
URL = "https://ch.example.org/api/knowledge/bedrock-setup/report"


def event(kind="Create", step="started", **props):
    p = {"ServiceToken": "x", "Step": step, "Url": URL, "Token": "chbs_" + "a" * 43, "Account": "111122223333"}
    p.update(props)
    return {
        "RequestType": kind,
        "ResponseURL": "https://cloudformation-custom-resource-response.s3.amazonaws.com/r",
        "StackId": STACK,
        "RequestId": "r1",
        "LogicalResourceId": "Report",
        "ResourceProperties": p,
    }


def answer(calls):
    """CloudFormation's answer: the last call, a PUT to the response URL."""
    url, method, body = calls[-1]
    assert method == "PUT" and "cloudformation" in url, calls
    return body


class Reporter(unittest.TestCase):
    def test_the_inline_code_fits_cloudformations_limit(self):
        self.assertLess(len(inline_code().encode()), 4096)

    def test_a_step_is_reported_with_the_token_then_answered(self):
        f, calls = load()
        f.handler(event(step="ready", Outputs={"BucketName": "b"}), None)
        url, method, body = calls[0]
        self.assertEqual((url, method), (URL, "POST"))
        self.assertEqual(body["step"], "ready")
        self.assertEqual(body["stack_id"], STACK)
        self.assertTrue(body["token"].startswith("chbs_"))
        self.assertEqual(body["outputs"], {"BucketName": "b"})
        self.assertEqual(answer(calls)["Status"], "SUCCESS")

    def test_another_account_stops_the_stack_before_anything_is_reported(self):
        f, calls = load()
        f.handler(event(Account="999988887777"), None)
        self.assertEqual(len(calls), 1)
        a = answer(calls)
        self.assertEqual(a["Status"], "FAILED")
        self.assertIn("999988887777", a["Reason"])

    def test_a_refusal_fails_the_stack_in_coderhelms_words(self):
        f, calls = load(refuse="This setup link has expired. Start again in Coderhelm.")
        f.handler(event(), None)
        a = answer(calls)
        self.assertEqual(a["Status"], "FAILED")
        self.assertIn("This setup link has expired", a["Reason"])

    def test_an_unreachable_install_fails_the_stack_and_says_where(self):
        f, calls = load(unreachable=True)
        f.handler(event(), None)
        a = answer(calls)
        self.assertEqual(a["Status"], "FAILED")
        self.assertIn("couldn't reach Coderhelm at " + URL, a["Reason"])

    def test_without_the_token_it_reports_nothing(self):
        f, calls = load()
        f.handler(event(Url="", Token=""), None)
        self.assertEqual(len(calls), 1)
        self.assertEqual(answer(calls)["Status"], "SUCCESS")

    def test_a_rollback_reports_the_first_failure(self):
        events = [
            {"LogicalResourceId": "KnowledgeBase", "ResourceStatus": "CREATE_FAILED", "ResourceStatusReason": "Resource creation cancelled"},
            {"LogicalResourceId": "VectorBucket", "ResourceStatus": "CREATE_FAILED", "ResourceStatusReason": "S3 Vectors isn't available in this Region"},
        ]
        f, calls = load(cfn=FakeCfn("ROLLBACK_IN_PROGRESS", events))
        f.handler(event(kind="Delete"), None)
        url, method, body = calls[0]
        self.assertEqual((url, body["step"]), (URL, "failed"))
        self.assertIn("VectorBucket: S3 Vectors isn't available", body["reason"])
        self.assertEqual(answer(calls)["Status"], "SUCCESS")

    def test_deleting_the_stack_later_reports_nothing_and_never_blocks(self):
        f, calls = load(cfn=FakeCfn("DELETE_IN_PROGRESS"))
        f.handler(event(kind="Delete"), None)
        self.assertEqual(len(calls), 1)
        self.assertEqual(answer(calls)["Status"], "SUCCESS")
        # Even when the install refuses (the token is long spent).
        f, calls = load(cfn=FakeCfn("ROLLBACK_IN_PROGRESS"), refuse="spent")
        f.handler(event(kind="Delete"), None)
        self.assertEqual(answer(calls)["Status"], "SUCCESS")


if __name__ == "__main__":
    unittest.main()
