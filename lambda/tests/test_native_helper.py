"""The native install's custom resources: verified copies, a delete that only
cleans on a real stack delete, tables deleted with retries and waits (the
GlobalTable failures seen in a real uninstall), and reasons that say what
happened.

Run: python3 -m unittest discover -s lambda/tests
"""

import hashlib
import importlib.util
import io
import json
import pathlib
import sys
import unittest

PATH = pathlib.Path(__file__).resolve().parents[1] / "native-helper" / "handler.py"
spec = importlib.util.spec_from_file_location("native_helper", PATH)
h = importlib.util.module_from_spec(spec)
sys.modules["native_helper"] = h
spec.loader.exec_module(h)

BASE = "https://coderhelm-public.s3.amazonaws.com/launch/v1.2.0"


class Resp(io.BytesIO):
    status = 200

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class FakeS3:
    def __init__(self, objects=None, versions=None):
        self.objects = dict(objects or {})
        self.versions = list(versions or [])
        self.deleted = []

    def head_object(self, Bucket, Key, ChecksumMode=None):
        if Key not in self.objects:
            raise Exception("404")
        # S3's own checksum: of the content, whatever the uploader claims.
        return {"ChecksumSHA256": h.b64(self.objects[Key]["sha"]) if ChecksumMode == "ENABLED" else None}

    def put_object(self, Bucket, Key, Body, ChecksumSHA256=None, ContentType=None, CacheControl=None, Metadata=None):
        data = Body.read() if hasattr(Body, "read") else Body
        real = hashlib.sha256(data).hexdigest()
        if ChecksumSHA256 is not None and ChecksumSHA256 != h.b64(real):
            raise Exception("BadDigest: the checksum doesn't match the content")
        self.objects[Key] = {"sha": real, "data": data}

    def list_objects_v2(self, **kw):
        return {"Contents": [{"Key": k} for k in self.objects], "IsTruncated": False}

    def delete_object(self, Bucket, Key):
        self.objects.pop(Key, None)
        self.deleted.append(Key)

    def list_object_versions(self, **kw):
        if self.versions == "gone":
            raise Exception("NoSuchBucket: The specified bucket does not exist")
        v, self.versions = self.versions, []
        return {"Versions": v, "IsTruncated": False}

    def delete_objects(self, Bucket, Delete):
        self.deleted += [o["Key"] for o in Delete["Objects"]]
        return {}


class FakeCfn:
    def __init__(self, status):
        self.status = status
        self.events = []

    def describe_stacks(self, StackName):
        return {"Stacks": [{"StackStatus": self.status}]}

    def describe_stack_events(self, StackName):
        return {"StackEvents": self.events}


class FakeDdb:
    """A table that fails twice the way the GlobalTable handler did, then goes."""

    def __init__(self, tables):
        self.tables = {n: dict(status="ACTIVE", protect=p, fail=f) for n, (p, f) in tables.items()}
        self.calls = []

    def describe_table(self, TableName):
        t = self.tables.get(TableName)
        if t is None:
            raise type("ResourceNotFoundException", (Exception,), {})("not found")
        if t["status"] == "DELETING":
            t["ticks"] = t.get("ticks", 0) + 1
            if t["ticks"] >= 2:
                del self.tables[TableName]
            return {"Table": {"TableStatus": "DELETING"}}
        return {"Table": {"TableStatus": t["status"], "DeletionProtectionEnabled": t["protect"]}}

    def update_table(self, TableName, DeletionProtectionEnabled):
        self.tables[TableName]["protect"] = DeletionProtectionEnabled
        self.calls.append(("protect-off", TableName))

    def delete_table(self, TableName):
        t = self.tables[TableName]
        if t["fail"] > 0:
            t["fail"] -= 1
            raise type("ResourceInUseException", (Exception,), {})("Table is being updated (replica settling)")
        t["status"] = "DELETING"
        self.calls.append(("delete", TableName))


class Clock:
    def __init__(self):
        self.t = 0.0

    def now(self):
        return self.t

    def sleep(self, s):
        self.t += s


class ReleaseAssets(unittest.TestCase):
    def test_copies_only_what_matches_the_signed_hash(self):
        good, bad = b"good zip", b"tampered"
        files = [{"Key": "assets/a.zip", "Sha256": hashlib.sha256(good).hexdigest()},
                 {"Key": "assets/b.zip", "Sha256": hashlib.sha256(b"original").hexdigest()}]
        body = {f"{BASE}/assets/a.zip": good, f"{BASE}/assets/b.zip": bad}
        s3 = FakeS3()
        with self.assertRaisesRegex(RuntimeError, "assets/b.zip: .*refused"):
            h.copy_assets({"Bucket": "x", "BaseUrl": BASE, "Files": files}, s3=s3,
                          opener=lambda req, timeout=None: Resp(body[req.full_url]))
        self.assertIn("assets/a.zip", s3.objects)
        self.assertNotIn("assets/b.zip", s3.objects)

    def test_a_planted_file_with_a_claimed_hash_is_replaced(self):
        good = b"the release's zip"
        sha = hashlib.sha256(good).hexdigest()
        # Someone put other bytes there, claiming the hash in metadata: S3's own checksum says otherwise.
        s3 = FakeS3({"assets/a.zip": {"sha": hashlib.sha256(b"planted").hexdigest(), "data": b"planted"}})
        out = h.copy_assets({"Bucket": "x", "BaseUrl": BASE, "Files": [{"Key": "assets/a.zip", "Sha256": sha}]}, s3=s3,
                            opener=lambda req, timeout=None: Resp(good))
        self.assertEqual(out["Copied"], "1")
        self.assertEqual(s3.objects["assets/a.zip"]["data"], good)

    def test_answers_only_cloudformations_response_url(self):
        sent = []
        ev = {"ResponseURL": "https://evil.example.com/x", "StackId": "s", "RequestId": "r", "LogicalResourceId": "L"}
        self.assertIsNone(h.respond(ev, "SUCCESS"))
        ev["ResponseURL"] = "https://cloudformation-custom-resource-response-useast1.s3.amazonaws.com/a?sig=1"
        h.respond(ev, "SUCCESS", put=lambda url, body: sent.append(url))
        self.assertEqual(len(sent), 1)

    def test_log_groups_only_with_this_installs_tag(self):
        class Logs:
            deleted = []

            def get_paginator(self, name):
                class P:
                    def paginate(self, logGroupNamePrefix):
                        return [{"logGroups": [{"logGroupName": "/aws/lambda/coderhelm-prod-gateway", "arn": "arn:aws:logs:r:1:log-group:/aws/lambda/coderhelm-prod-gateway:*"},
                                               {"logGroupName": "/aws/lambda/coderhelm-prod-other", "arn": "arn:aws:logs:r:1:log-group:/aws/lambda/coderhelm-prod-other:*"}]}]
                return P()

            def list_tags_for_resource(self, resourceArn):
                return {"tags": {"coderhelm:install": "coderhelm-prod" if resourceArn.endswith("gateway") else "coderhelm-prodx"}}

            def delete_log_group(self, logGroupName):
                Logs.deleted.append(logGroupName)
        gone, _ = h.delete_log_groups(["/aws/lambda/coderhelm-prod-"], tag="coderhelm-prod", logs=Logs())
        self.assertEqual(gone, ["/aws/lambda/coderhelm-prod-gateway"])

    def test_status_file_redacts_arns_accounts_and_emails(self):
        self.assertEqual(h.redact("Api: arn:aws:iam::111122223333:role/x refused for a@b.co in 111122223333"), "Api: <arn> refused for <email> in <account>")

    def test_skips_files_already_verified(self):
        sha = hashlib.sha256(b"z").hexdigest()
        s3 = FakeS3({"assets/a.zip": {"sha": sha}})
        out = h.copy_assets({"Bucket": "x", "BaseUrl": BASE, "Files": [{"Key": "assets/a.zip", "Sha256": sha}]}, s3=s3,
                            opener=lambda *a, **k: self.fail("downloaded again"))
        self.assertEqual(out["Copied"], "0")

    def test_only_release_locations(self):
        for base in ("https://evil.example.com/launch/v1.0.0", "http://coderhelm-public.s3.amazonaws.com/launch/v1.0.0"):
            with self.assertRaises(ValueError):
                h.release_url(base, "assets/a.zip")
        with self.assertRaises(ValueError):
            h.release_url(BASE, "../other")

    def test_prune_keeps_this_and_the_previous_version(self):
        s3 = FakeS3({"new": {"sha": "1"}, "prev": {"sha": "2"}, "ancient": {"sha": "3"}})
        h.prune_assets({"Bucket": "x", "Files": [{"Key": "new"}]}, {"Files": [{"Key": "prev"}]}, s3=s3)
        self.assertEqual(sorted(s3.objects), ["new", "prev"])


class OnlyOnARealDelete(unittest.TestCase):
    def setUp(self):
        self.saved = dict(h._clients)

    def tearDown(self):
        h._clients.clear()
        h._clients.update(self.saved)

    def ev(self, kind, props, rt="Delete"):
        return {"ResourceType": kind, "RequestType": rt, "LogicalResourceId": "X", "ResourceProperties": dict(props, RootStackId="root")}

    def test_update_that_removes_sweep_never_empties_anything(self):
        h._clients["cloudformation@None"] = FakeCfn("UPDATE_COMPLETE_CLEANUP_IN_PROGRESS")
        s3 = FakeS3(versions=[{"Key": "k", "VersionId": "1"}])
        h._clients["s3@None"] = s3
        status, reason, _ = h.handle(self.ev("Custom::Sweep", {"KeepData": "false", "Buckets": ["data"], "Tables": ["t"]}))
        self.assertEqual(status, "SUCCESS")
        self.assertEqual(s3.deleted, [])

    def test_delete_with_keep_data_keeps_it(self):
        h._clients["cloudformation@None"] = FakeCfn("DELETE_IN_PROGRESS")
        s3 = FakeS3(versions=[{"Key": "k", "VersionId": "1"}])
        h._clients["s3@None"] = s3
        status, reason, _ = h.handle(self.ev("Custom::Sweep", {"KeepData": "true", "Buckets": ["data"], "Tables": ["t"]}))
        self.assertEqual((status, reason), ("SUCCESS", "data kept"))
        self.assertEqual(s3.deleted, [])

    def test_delete_without_keep_data_empties_and_deletes_tables(self):
        h._clients["cloudformation@None"] = FakeCfn("DELETE_IN_PROGRESS")
        s3 = FakeS3(versions=[{"Key": "k", "VersionId": "1"}])
        h._clients["s3@None"] = s3
        ddb = FakeDdb({"coderhelm-prod-traces": (True, 2), "coderhelm-prod-checkpoints": (False, 1)})
        h._clients["dynamodb@None"] = ddb
        clock = Clock()
        orig = h.delete_tables
        h.delete_tables = lambda names, deadline=None: orig(names, ddb, deadline=clock.now() + 600, sleep=clock.sleep, now=clock.now)
        try:
            status, reason, _ = h.handle(self.ev("Custom::Sweep", {"KeepData": "false", "Buckets": ["data", "gone"], "Tables": list(ddb.tables)}))
        finally:
            h.delete_tables = orig
        self.assertEqual(status, "SUCCESS", reason)
        self.assertEqual(ddb.tables, {})
        self.assertIn(("protect-off", "coderhelm-prod-traces"), ddb.calls)
        self.assertIn("k", s3.deleted)

    def test_a_rolled_back_first_launch_cleans_even_with_keep_data(self):
        h._clients["cloudformation@None"] = FakeCfn("ROLLBACK_IN_PROGRESS")
        s3 = FakeS3(versions=[{"Key": "k", "VersionId": "1"}])
        h._clients["s3@None"] = s3
        h._clients["dynamodb@None"] = FakeDdb({})
        status, _, _ = h.handle(self.ev("Custom::Sweep", {"KeepData": "true", "Buckets": ["data"], "Tables": []}))
        self.assertEqual(status, "SUCCESS")
        self.assertIn("k", s3.deleted)


class Tables(unittest.TestCase):
    def test_retries_in_use_and_waits_until_gone(self):
        ddb = FakeDdb({"a": (False, 3), "b": (True, 0)})
        clock = Clock()
        left = h.delete_tables(["a", "b"], ddb, deadline=600, sleep=clock.sleep, now=clock.now)
        self.assertEqual(left, {})

    def test_reports_each_table_left_with_awss_reason(self):
        ddb = FakeDdb({"stuck": (False, 10_000)})
        clock = Clock()
        left = h.delete_tables(["stuck"], ddb, deadline=120, sleep=clock.sleep, now=clock.now)
        self.assertIn("ResourceInUseException", left["stuck"])
        self.assertIn("replica settling", left["stuck"])

    def test_sweep_failure_names_the_table(self):
        h._clients["cloudformation@None"] = FakeCfn("DELETE_IN_PROGRESS")
        h._clients["s3@None"] = FakeS3()
        h._clients["dynamodb@None"] = FakeDdb({"stuck": (False, 10_000)})
        orig = h.delete_tables
        clock = Clock()
        h.delete_tables = lambda names, deadline=None: orig(names, h._clients["dynamodb@None"], deadline=60, sleep=clock.sleep, now=clock.now)
        try:
            status, reason, _ = h.handle({"ResourceType": "Custom::Sweep", "RequestType": "Delete", "LogicalResourceId": "S",
                                          "ResourceProperties": {"RootStackId": "r", "KeepData": "false", "Tables": ["stuck"]}})
        finally:
            h.delete_tables = orig
            h._clients.clear()
        self.assertEqual(status, "FAILED")
        self.assertIn("table stuck: ResourceInUseException", reason)


class SecretAndAllowlist(unittest.TestCase):
    class SM:
        def __init__(self, value):
            self.value = value
            self.put = None

        def get_secret_value(self, SecretId):
            return {"SecretString": json.dumps(self.value)}

        def put_secret_value(self, SecretId, SecretString):
            self.put = json.loads(SecretString)

    def test_setup_key_only_without_a_github_app(self):
        sm = self.SM({"jwt_secret": "j"})
        self.assertEqual(h.setup_key({"SecretId": "s", "Token": "a" * 32}, sm)["Stored"], "true")
        self.assertEqual(sm.put, {"jwt_secret": "j", "setup_token": "a" * 32})
        sm = self.SM({"github_app_id": "1", "github_private_key": "k"})
        self.assertEqual(h.setup_key({"SecretId": "s", "Token": "a" * 32}, sm)["Stored"], "false")
        self.assertIsNone(sm.put)
        with self.assertRaises(ValueError):
            h.setup_key({"SecretId": "s", "Token": "short"}, self.SM({}))

    def test_allowlist(self):
        self.assertEqual(h.allowlist_items({"Admin": "Me@Example.com", "Domain": "", "TwoFactor": "off"}),
                         [{"sk": "EMAIL#me@example.com", "kind": "admin"}])
        self.assertEqual(len(h.allowlist_items({"Admin": "a@b.co", "Domain": "b.co", "TwoFactor": "on"})), 3)

    def test_hybrid_leave_signs_and_never_fails(self):
        sm = self.SM({"hybrid_install_id": "ins_" + "a" * 24, "hybrid_install_secret": "k"})
        seen = {}

        def opener(req, timeout=None):
            seen["sig"] = req.headers["X-coderhelm-signature"]
            raise OSError("unreachable")
        msg = h.hybrid_leave({"SecretId": "s"}, sm, opener)
        self.assertTrue(seen["sig"].startswith("v1="))
        self.assertIn("could not be reached", msg)
        self.assertIn("not registered", h.hybrid_leave({"SecretId": "s"}, self.SM({})))


class Status(unittest.TestCase):
    def test_failed_create_reports_cloudformations_reason(self):
        cfn = FakeCfn("ROLLBACK_IN_PROGRESS")
        cfn.events = [
            {"LogicalResourceId": "Web", "ResourceStatus": "CREATE_FAILED", "ResourceStatusReason": "Resource creation cancelled"},
            {"LogicalResourceId": "Api", "ResourceStatus": "CREATE_FAILED", "ResourceStatusReason": "Embedded stack failed: Role is invalid"},
        ]
        h._clients["cloudformation@None"] = cfn
        s3 = FakeS3()
        h._clients["s3@None"] = s3
        try:
            status, reason, _ = h.handle({"ResourceType": "Custom::Status", "RequestType": "Delete", "LogicalResourceId": "S",
                                          "ResourceProperties": {"Phase": "start", "RootStackId": "r", "StatusBucket": "b"}})
        finally:
            h._clients.clear()
        doc = json.loads(s3.objects["status.json"]["data"])
        self.assertEqual(doc["phase"], "failed")
        self.assertIn("Api: Embedded stack failed: Role is invalid", doc["message"])
        self.assertNotIn("download", doc["message"])


if __name__ == "__main__":
    unittest.main()
