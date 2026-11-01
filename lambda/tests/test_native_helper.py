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
    def __init__(self, status, params=None):
        self.status = status
        self.events = []
        self.params = params

    def describe_stacks(self, StackName):
        stack = {"StackStatus": self.status}
        if self.params is not None:
            stack["Parameters"] = [{"ParameterKey": k, "ParameterValue": v} for k, v in self.params.items()]
        return {"Stacks": [stack]}

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


class FakeLogs:
    def __init__(self, groups):
        self.groups = set(groups)
        self.deleted = []

    def delete_log_group(self, logGroupName):
        if logGroupName not in self.groups:
            raise type("ResourceNotFoundException", (Exception,), {})("gone")
        self.groups.discard(logGroupName)
        self.deleted.append(logGroupName)

    def get_paginator(self, name):
        groups = self.groups

        class P:
            def paginate(self, logGroupNamePrefix):
                return [{"logGroups": [{"logGroupName": g, "arn": f"arn:{g}:*"} for g in sorted(groups) if g.startswith(logGroupNamePrefix)]}]
        return P()

    def list_tags_for_resource(self, resourceArn):
        return {"tags": {}}  # what Lambda makes by itself has no tags


class FakeSecrets:
    def __init__(self, exists=True):
        self.exists = exists
        self.deleted = []

    def delete_secret(self, SecretId, ForceDeleteWithoutRecovery):
        if not self.exists:
            raise type("ResourceNotFoundException", (Exception,), {})("gone")
        self.deleted.append((SecretId, ForceDeleteWithoutRecovery))
        self.exists = False


class FakeAcm:
    def get_paginator(self, name):
        class P:
            def paginate(self):
                return [{"CertificateSummaryList": []}]
        return P()


class Final(unittest.TestCase):
    """Deleted after every part: the log groups Lambda recreated (the live
    test left two), and with KeepDataOnDelete=false the install's secret."""

    RECREATED = ["/aws/lambda/coderhelm-prod-Handler-cd8e80af", "/aws/lambda/coderhelm-prod-LogRetentionaae0aa3c5b4d-1532c9b8"]

    def setUp(self):
        self.saved = dict(h._clients)
        self.logs = FakeLogs(self.RECREATED + ["/aws/lambda/coderhelm-prodx-worker", "/aws/lambda/other-app"])
        self.sm = FakeSecrets()
        h._clients["logs@None"] = self.logs
        h._clients["secretsmanager@None"] = self.sm
        h._clients["acm@us-east-1"] = FakeAcm()
        h._clients["route53@None"] = object()

    def tearDown(self):
        h._clients.clear()
        h._clients.update(self.saved)

    def final(self, keep, status="DELETE_IN_PROGRESS", names=None):
        h._clients["cloudformation@None"] = FakeCfn(status)
        props = {"Prefix": "coderhelm-prod", "KeepData": keep, "RootStackId": "root", "SecretId": "arn:secret:coderhelm/prod/secrets-abc123",
                 "LogGroupPrefixes": ["/aws/lambda/coderhelm-prod-"],
                 "LogGroupNames": names if names is not None else self.RECREATED + ["/aws/lambda/coderhelm-prod-never-recreated"]}
        return h.handle({"ResourceType": "Custom::Final", "RequestType": "Delete", "LogicalResourceId": "Final", "ResourceProperties": props})

    def test_recreated_log_groups_go_by_their_exact_names_even_when_keeping_data(self):
        status, reason, _ = self.final("true")
        self.assertEqual(status, "SUCCESS", reason)
        self.assertEqual(sorted(self.logs.deleted), sorted(self.RECREATED))
        # Never another install's or another app's, and the secret stays.
        self.assertIn("/aws/lambda/coderhelm-prodx-worker", self.logs.groups)
        self.assertIn("/aws/lambda/other-app", self.logs.groups)
        self.assertEqual(self.sm.deleted, [])

    def test_a_name_outside_this_install_is_refused(self):
        status, reason, _ = self.final("true", names=["/aws/lambda/other-app"])
        self.assertEqual(status, "FAILED")
        self.assertIn("not this install's", reason)
        self.assertIn("/aws/lambda/other-app", self.logs.groups)

    def test_without_keep_data_the_secret_goes_too_without_a_recovery_window(self):
        status, reason, _ = self.final("false")
        self.assertEqual(status, "SUCCESS", reason)
        self.assertEqual(self.sm.deleted, [("arn:secret:coderhelm/prod/secrets-abc123", True)])
        self.assertIn("the install's secret", reason)

    def test_a_secret_already_gone_is_fine(self):
        self.sm.exists = False
        status, reason, _ = self.final("false")
        self.assertEqual(status, "SUCCESS", reason)

    def test_an_update_never_deletes_anything(self):
        status, reason, _ = self.final("false", status="UPDATE_COMPLETE_CLEANUP_IN_PROGRESS")
        self.assertEqual((status, reason), ("SUCCESS", "nothing to do"))
        self.assertEqual(self.logs.deleted, [])
        self.assertEqual(self.sm.deleted, [])


class Stage(unittest.TestCase):
    """The updater's step before a change set with the nested stacks: the
    release's child templates into the install's bucket, from its verified
    coderhelm.json, each checked."""

    V = "v1.2.0"
    BASE = f"https://coderhelm-public.s3.amazonaws.com/launch/{V}"

    def release(self, base=None, version=None):
        child = b'{"Resources":{}}'
        files = [
            {"Key": "stacks/Database.json", "Dest": f"releases/{self.V}/stacks/Database.json", "Sha256": hashlib.sha256(child).hexdigest()},
            {"Key": "assets/big.zip", "Dest": f"releases/{self.V}/assets/big.zip", "Sha256": "0" * 64},
        ]
        template = json.dumps({"Resources": {"ReleaseAssets": {"Type": "Custom::ReleaseAssets", "Properties": {
            "BaseUrl": base or self.BASE, "Version": version or self.V, "Files": files}}}}).encode()
        served = {f"{self.BASE}/coderhelm.json": template, f"{self.BASE}/stacks/Database.json": child}
        self.fetched = []

        def opener(req, timeout=None):
            self.fetched.append(req.full_url)
            return Resp(served[req.full_url])
        return template, opener

    def test_stages_only_the_child_templates_each_checked(self):
        template, opener = self.release()
        s3 = FakeS3()
        r = h.stage_release({"version": self.V, "template_sha256": hashlib.sha256(template).hexdigest()}, "coderhelm-prod-release-1", s3=s3, opener=opener)
        self.assertEqual(r, {"ok": True, "version": self.V, "staged": 1, "copied": 1})
        self.assertEqual(list(s3.objects), [f"releases/{self.V}/stacks/Database.json"])
        self.assertNotIn(f"{self.BASE}/assets/big.zip", self.fetched)  # the code waits for the update itself

    def test_a_template_that_is_not_the_verified_one_is_refused(self):
        template, opener = self.release()
        s3 = FakeS3()
        with self.assertRaisesRegex(ValueError, "refused"):
            h.stage_release({"version": self.V, "template_sha256": "1" * 64}, "b", s3=s3, opener=opener)
        self.assertEqual(s3.objects, {})

    def test_a_template_listing_another_releases_files_is_refused(self):
        template, opener = self.release(version="v1.1.0")
        with self.assertRaisesRegex(ValueError, "another release"):
            h.stage_release({"version": self.V, "template_sha256": hashlib.sha256(template).hexdigest()}, "b", s3=FakeS3(), opener=opener)

    def test_bad_input_is_refused_before_anything_is_fetched(self):
        for ev in ({"version": "../x", "template_sha256": "a" * 64}, {"version": self.V, "template_sha256": "nope"}):
            with self.assertRaises(ValueError):
                h.stage_release(ev, "b", s3=FakeS3(), opener=lambda *a, **k: self.fail("fetched"))

    def test_called_directly_it_answers_the_caller_not_cloudformation(self):
        saved = h.stage_release
        h.stage_release = lambda event, bucket: {"ok": True, "bucket": bucket}
        try:
            import os
            os.environ["ASSETS_BUCKET"] = "coderhelm-prod-release-1"
            self.assertEqual(h.handler({"action": "stage"}, None), {"ok": True, "bucket": "coderhelm-prod-release-1"})
            h.stage_release = lambda event, bucket: (_ for _ in ()).throw(ValueError("refused"))
            self.assertEqual(h.handler({"action": "stage"}, None), {"ok": False, "error": "ValueError: refused"})
        finally:
            h.stage_release = saved
            os.environ.pop("ASSETS_BUCKET", None)


class OnlyWhatTheStackSays(unittest.TestCase):
    """Delete work follows the root stack's own KeepDataOnDelete, never only
    the event's, and an event without CloudFormation's answer URL does nothing."""

    def setUp(self):
        self.saved = dict(h._clients)

    def tearDown(self):
        h._clients.clear()
        h._clients.update(self.saved)

    def test_an_event_saying_false_while_the_stack_says_true_keeps_everything(self):
        h._clients["cloudformation@None"] = FakeCfn("DELETE_IN_PROGRESS", {"KeepDataOnDelete": "true"})
        sm = FakeSecrets()
        s3 = FakeS3(versions=[{"Key": "k", "VersionId": "1"}])
        h._clients.update({"secretsmanager@None": sm, "s3@None": s3, "logs@None": FakeLogs([]), "acm@us-east-1": FakeAcm(), "route53@None": object()})
        ev = lambda kind, props: {"ResourceType": kind, "RequestType": "Delete", "LogicalResourceId": "X", "ResourceProperties": dict(props, RootStackId="root")}
        status, reason, _ = h.handle(ev("Custom::Sweep", {"KeepData": "false", "Buckets": ["data"], "Tables": []}))
        self.assertEqual((status, reason), ("SUCCESS", "data kept"))
        status, _, _ = h.handle(ev("Custom::Final", {"KeepData": "false", "Prefix": "coderhelm-prod", "SecretId": "s", "LogGroupNames": []}))
        self.assertEqual(status, "SUCCESS")
        self.assertEqual(sm.deleted, [])
        self.assertEqual(s3.deleted, [])

    def test_the_stack_saying_false_is_what_deletes(self):
        h._clients["cloudformation@None"] = FakeCfn("DELETE_IN_PROGRESS", {"KeepDataOnDelete": "false"})
        sm = FakeSecrets()
        h._clients.update({"secretsmanager@None": sm, "logs@None": FakeLogs([]), "acm@us-east-1": FakeAcm(), "route53@None": object()})
        h.handle({"ResourceType": "Custom::Final", "RequestType": "Delete", "LogicalResourceId": "Final",
                  "ResourceProperties": {"RootStackId": "root", "KeepData": "true", "Prefix": "coderhelm-prod", "SecretId": "s", "LogGroupNames": []}})
        self.assertEqual(sm.deleted, [("s", True)])

    def test_an_event_without_cloudformations_answer_url_does_nothing(self):
        called = []
        saved = h.handle
        h.handle = lambda *a, **k: called.append(1) or ("SUCCESS", "", {})
        try:
            self.assertIsNone(h.handler({"ResourceType": "Custom::Final", "RequestType": "Delete", "LogicalResourceId": "Final",
                                         "ResponseURL": "https://attacker.example.com/x", "ResourceProperties": {}}, None))
        finally:
            h.handle = saved
        self.assertEqual(called, [])

    def test_stage_refuses_an_odd_part_template_name(self):
        v = "v1.2.0"
        base = f"https://coderhelm-public.s3.amazonaws.com/launch/{v}"
        bad = json.dumps({"Resources": {"ReleaseAssets": {"Properties": {"BaseUrl": base, "Version": v, "Files": [
            {"Key": "stacks/Database.json", "Dest": f"releases/{v}/stacks/../../other.json", "Sha256": "0" * 64}]}}}}).encode()
        opener = lambda req, timeout=None: Resp(bad)
        with self.assertRaisesRegex(ValueError, "odd part template"):
            h.stage_release({"version": v, "template_sha256": hashlib.sha256(bad).hexdigest()}, "b", s3=FakeS3(), opener=opener)


class FakeSsm:
    def __init__(self, value=None, error=None):
        self.value = value
        self.error = error
        self.deleted = []

    def get_parameter(self, Name):
        if self.error:
            raise type("AccessDeniedException", (Exception,), {})(self.error)
        if self.value is None:
            raise type("ParameterNotFound", (Exception,), {})("not found")
        return {"Parameter": {"Value": self.value}}

    def delete_parameter(self, Name):
        self.deleted.append(Name)
        self.value = None


class FakeCognito:
    def __init__(self):
        self.calls = []
        self.pools = {"us-east-1_prod": {"DeletionProtection": "ACTIVE", "Domain": "coderhelm-prod-auth"}}

    def describe_user_pool(self, UserPoolId):
        if UserPoolId not in self.pools:
            raise type("ResourceNotFoundException", (Exception,), {})("gone")
        return {"UserPool": self.pools[UserPoolId]}

    def update_user_pool(self, UserPoolId, DeletionProtection):
        self.calls.append(("protect", UserPoolId, DeletionProtection))

    def delete_user_pool_domain(self, Domain, UserPoolId):
        self.calls.append(("domain", Domain))

    def delete_user_pool(self, UserPoolId):
        self.calls.append(("delete", UserPoolId))
        del self.pools[UserPoolId]


class FakeS3Delete(FakeS3):
    def __init__(self, **kw):
        super().__init__(**kw)
        self.buckets_deleted = []

    def delete_bucket(self, Bucket):
        self.buckets_deleted.append(Bucket)


class FakeKms:
    def __init__(self):
        self.aliases = [{"AliasName": "alias/coderhelm-prod-dynamo", "TargetKeyId": "k1"}, {"AliasName": "alias/coderhelm-prod-s3", "TargetKeyId": "k2"},
                        {"AliasName": "alias/coderhelm-prodx-dynamo", "TargetKeyId": "k9"}, {"AliasName": "alias/my-app", "TargetKeyId": "k8"}]
        self.scheduled = []
        self.alias_deleted = []

    def get_paginator(self, name):
        aliases = self.aliases

        class P:
            def paginate(self):
                return [{"Aliases": list(aliases)}]
        return P()

    def delete_alias(self, AliasName):
        self.alias_deleted.append(AliasName)

    def schedule_key_deletion(self, KeyId, PendingWindowInDays):
        self.scheduled.append((KeyId, PendingWindowInDays))


class UninstallSwitch(unittest.TestCase):
    """The admin's "delete everything when uninstalled" switch (Settings,
    System, Uninstall): read from SSM on a real stack delete, it deletes the
    data even while the stack says KeepDataOnDelete=true (which retains it),
    and never on an update."""

    SWITCH = "/coderhelm-prod/uninstall/delete-data"

    def setUp(self):
        self.saved = dict(h._clients)
        self.s3 = FakeS3Delete(versions=[{"Key": "k", "VersionId": "1"}])
        self.cognito = FakeCognito()
        self.kms = FakeKms()
        self.sm = FakeSecrets()
        self.logs = FakeLogs(["/aws/lambda/coderhelm-prod-gateway"])
        self.ddb = FakeDdb({"coderhelm-prod-runs": (True, 0)})
        h._clients.update({"s3@None": self.s3, "cognito-idp@None": self.cognito, "kms@None": self.kms, "secretsmanager@None": self.sm,
                           "logs@None": self.logs, "dynamodb@None": self.ddb, "acm@us-east-1": FakeAcm(), "route53@None": object()})
        clock = Clock()
        self.orig = h.delete_tables
        h.delete_tables = lambda names, deadline=None: self.orig(names, self.ddb, deadline=clock.now() + 600, sleep=clock.sleep, now=clock.now)

    def tearDown(self):
        h.delete_tables = self.orig
        h._clients.clear()
        h._clients.update(self.saved)

    def run_both(self, ssm, status="DELETE_IN_PROGRESS", keep="true"):
        h._clients["ssm@None"] = ssm
        h._clients["cloudformation@None"] = FakeCfn(status, {"KeepDataOnDelete": keep})
        ev = lambda kind, props: {"ResourceType": kind, "RequestType": "Delete", "LogicalResourceId": "X", "ResourceProperties": dict(props, RootStackId="root", DeleteSwitch=self.SWITCH)}
        sweep = h.handle(ev("Custom::Sweep", {"KeepData": keep, "Buckets": ["coderhelm-prod-data-1"], "Tables": ["coderhelm-prod-runs"], "UserPoolId": "us-east-1_prod"}))
        final = h.handle(ev("Custom::Final", {"KeepData": keep, "Prefix": "coderhelm-prod", "SecretId": "s", "LogGroupNames": [],
                                              "LogGroupPrefixes": ["/aws/lambda/coderhelm-prod-"], "KeyAliasPrefix": "alias/coderhelm-prod-"}))
        return sweep, final

    def test_switch_on_while_the_stack_keeps_data_deletes_everything(self):
        ssm = FakeSsm("delete")
        (s_status, s_reason, _), (f_status, f_reason, _) = self.run_both(ssm)
        self.assertEqual(s_status, "SUCCESS", s_reason)
        self.assertEqual(f_status, "SUCCESS", f_reason)
        self.assertIn("k", self.s3.deleted)
        self.assertEqual(self.s3.buckets_deleted, ["coderhelm-prod-data-1"])
        self.assertEqual(self.ddb.tables, {})
        self.assertEqual(self.cognito.calls, [("protect", "us-east-1_prod", "INACTIVE"), ("domain", "coderhelm-prod-auth"), ("delete", "us-east-1_prod")])
        self.assertEqual(sorted(self.kms.scheduled), [("k1", 7), ("k2", 7)], "only this install's keys, after AWS's 7 days")
        self.assertEqual(sorted(self.kms.alias_deleted), ["alias/coderhelm-prod-dynamo", "alias/coderhelm-prod-s3"])
        self.assertEqual(self.sm.deleted, [("s", True)])
        # Tagged log groups follow the same switch (FakeLogs' groups carry no tag, so none here).
        self.assertEqual(ssm.deleted, [self.SWITCH], "the switch goes with the stack")

    def test_switch_off_keeps_everything(self):
        ssm = FakeSsm(None)
        (s_status, s_reason, _), (f_status, _, _) = self.run_both(ssm)
        self.assertEqual((s_status, s_reason), ("SUCCESS", "data kept"))
        self.assertEqual(f_status, "SUCCESS")
        self.assertEqual((self.s3.deleted, self.s3.buckets_deleted, self.cognito.calls, self.kms.scheduled, self.sm.deleted), ([], [], [], [], []))
        self.assertIn("coderhelm-prod-runs", self.ddb.tables)

    def test_an_unreadable_switch_keeps_the_data(self):
        ssm = FakeSsm("delete", error="AccessDenied")
        (s_status, s_reason, _), _ = self.run_both(ssm)
        self.assertEqual((s_status, s_reason), ("SUCCESS", "data kept"))
        self.assertEqual((self.s3.deleted, self.cognito.calls, self.kms.scheduled), ([], [], []))

    def test_any_other_switch_value_keeps_the_data(self):
        (s_status, s_reason, _), _ = self.run_both(FakeSsm("yes please"))
        self.assertEqual((s_status, s_reason), ("SUCCESS", "data kept"))

    def test_keep_data_false_lets_the_stack_delete_its_own_buckets_pool_and_keys(self):
        # CloudFormation deletes those itself (DeletionPolicy Delete): the helper doesn't.
        (s_status, _, _), (f_status, _, _) = self.run_both(FakeSsm(None), keep="false")
        self.assertEqual((s_status, f_status), ("SUCCESS", "SUCCESS"))
        self.assertIn("k", self.s3.deleted)
        self.assertEqual((self.s3.buckets_deleted, self.cognito.calls, self.kms.scheduled), ([], [], []))
        self.assertEqual(self.sm.deleted, [("s", True)])

    def test_an_update_never_reads_or_acts_on_the_switch(self):
        ssm = FakeSsm("delete")
        (s_status, s_reason, _), (f_status, f_reason, _) = self.run_both(ssm, status="UPDATE_COMPLETE_CLEANUP_IN_PROGRESS")
        self.assertEqual((s_status, s_reason, f_reason), ("SUCCESS", "nothing to do", "nothing to do"))
        self.assertEqual((self.s3.deleted, self.cognito.calls, self.kms.scheduled, ssm.deleted), ([], [], [], []))

    def test_keys_only_behind_an_installs_aliases(self):
        done, problems = h.schedule_keys("alias/", kms=self.kms)
        self.assertEqual(done, [])
        self.assertTrue(problems)
