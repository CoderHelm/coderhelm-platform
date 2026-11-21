"""The access stack's reader update (lambda/native-reader): after each
release update, CoderHelmLogReader in the install's own account gets that
release's read-only permissions, from the release's own update template,
with no person doing anything.

Run: python3 -m unittest discover -s lambda/tests
"""

import importlib.util
import pathlib
import unittest

HANDLER = pathlib.Path(__file__).resolve().parents[1] / "native-reader" / "handler.py"
spec = importlib.util.spec_from_file_location("native_reader", HANDLER)
reader = importlib.util.module_from_spec(spec)
spec.loader.exec_module(reader)

TEMPLATE = pathlib.Path(__file__).resolve().parents[2] / "infra" / "cfn" / "coderhelm-log-reader-update.yaml"


def declared():
    """The parameters the update template in this repo declares (top-level keys under Parameters:)."""
    out, inside = [], False
    for line in TEMPLATE.read_text().splitlines():
        if line.startswith("Parameters:"):
            inside = True
            continue
        if inside and line and not line.startswith(" "):
            break
        if inside and line.startswith("  ") and not line.startswith("   ") and line.rstrip().endswith(":"):
            out.append(line.strip()[:-1])
    return out


class Missing(Exception):
    pass


class FakeIam:
    def __init__(self, role=True):
        self.role = role

    def get_role(self, RoleName):
        assert RoleName == "CoderHelmLogReader"
        if not self.role:
            raise Missing("An error occurred (NoSuchEntity): The role with name CoderHelmLogReader cannot be found.")
        return {"Role": {"RoleName": RoleName}}


class Waiter:
    def wait(self, **kw):
        pass


class FakeCfn:
    def __init__(self, root_status="UPDATE_COMPLETE", version="v0.9.4", reader_status=None, params=None, no_updates=False):
        self.root = {"StackName": "coderhelm-acme", "StackStatus": root_status,
                     "Outputs": [{"OutputKey": "Version", "OutputValue": version}],
                     "Parameters": params if params is not None else [
                         {"ParameterKey": "InfrastructureVulnerabilities", "ParameterValue": "on"},
                         {"ParameterKey": "InspectorFindings", "ParameterValue": "on"},
                         {"ParameterKey": "HealthEvents", "ParameterValue": "off"}]}
        self.reader_status = reader_status
        self.no_updates = no_updates
        self.calls = []

    def describe_stacks(self, StackName):
        if StackName == "coderhelm-acme":
            return {"Stacks": [self.root]}
        assert StackName == reader.STACK
        if self.reader_status is None:
            raise Missing(f"An error occurred (ValidationError): Stack with id {StackName} does not exist")
        return {"Stacks": [{"StackName": StackName, "StackStatus": self.reader_status}]}

    def get_template_summary(self, TemplateURL):
        self.calls.append(("summary", TemplateURL))
        return {"Parameters": [{"ParameterKey": k} for k in declared()]}

    def create_stack(self, **kw):
        self.calls.append(("create", kw))

    def update_stack(self, **kw):
        self.calls.append(("update", kw))
        if self.no_updates:
            raise Exception("An error occurred (ValidationError): No updates are to be performed.")

    def delete_stack(self, StackName):
        self.calls.append(("delete", StackName))

    def get_waiter(self, name):
        assert name == "stack_delete_complete"
        return Waiter()


def written(cfn):
    return [c for c in cfn.calls if c[0] in ("create", "update")]


class ReaderUpdateTests(unittest.TestCase):
    def test_after_a_release_update_the_reader_gets_that_releases_update_template(self):
        cfn = FakeCfn()
        r = reader.keep(cfn, FakeIam(), "coderhelm-acme")
        self.assertIn("creating", r)
        [(kind, kw)] = written(cfn)
        self.assertEqual(kind, "create")
        self.assertEqual(kw["StackName"], "CoderHelmLogReaderUpdate-release")
        # The release's own published update template, the one every Update link opens.
        self.assertEqual(kw["TemplateURL"], "https://coderhelm-public.s3.amazonaws.com/launch/v0.9.4/coderhelm-log-reader-update.yaml")
        self.assertEqual(kw["Capabilities"], ["CAPABILITY_IAM"])
        p = {x["ParameterKey"]: x["ParameterValue"] for x in kw["Parameters"]}
        self.assertEqual(p["RoleName"], "CoderHelmLogReader")
        # The AWS card's switches as the install's stack has them.
        self.assertEqual((p["InfrastructureVulnerabilities"], p["InspectorFindings"], p["HealthEvents"]), ("on", "on", "off"))

    def test_only_read_only_groups_are_ever_switched_on(self):
        cfn = FakeCfn()
        reader.keep(cfn, FakeIam(), "coderhelm-acme")
        p = {x["ParameterKey"]: x["ParameterValue"] for x in written(cfn)[0][1]["Parameters"]}
        # Every parameter the template declares is given, and only those.
        self.assertEqual(sorted(p), sorted(declared()))
        for off in ("ApplyMaintenance", "WafLogs"):
            self.assertEqual(p[off], "off", off)
        for empty in ("KnowledgeBases", "ImportBuckets", "SyncKnowledgeBases", "WafLogResults", "WafLogKey", "DatabaseClusters", "Warehouses"):
            self.assertEqual(p[empty], "", empty)

    def test_a_template_without_a_parameter_never_gets_it(self):
        cfn = FakeCfn()
        cfn.get_template_summary = lambda TemplateURL: {"Parameters": [{"ParameterKey": "RoleName"}, {"ParameterKey": "InfrastructureVulnerabilities"}]}
        reader.keep(cfn, FakeIam(), "coderhelm-acme")
        self.assertEqual([x["ParameterKey"] for x in written(cfn)[0][1]["Parameters"]], ["InfrastructureVulnerabilities", "RoleName"])

    def test_the_next_release_updates_the_same_stack(self):
        cfn = FakeCfn(version="v0.9.5", reader_status="UPDATE_COMPLETE")
        self.assertIn("updating", reader.keep(cfn, FakeIam(), "coderhelm-acme"))
        [(kind, kw)] = written(cfn)
        self.assertEqual(kind, "update")
        self.assertTrue(kw["TemplateURL"].endswith("/launch/v0.9.5/coderhelm-log-reader-update.yaml"))

    def test_already_current_is_not_a_failure(self):
        cfn = FakeCfn(reader_status="CREATE_COMPLETE", no_updates=True)
        self.assertIn("already has v0.9.4's reads", reader.keep(cfn, FakeIam(), "coderhelm-acme"))

    def test_no_reader_role_changes_nothing(self):
        cfn = FakeCfn()
        self.assertIn("nothing to update", reader.keep(cfn, FakeIam(role=False), "coderhelm-acme"))
        self.assertEqual(cfn.calls, [])

    def test_a_busy_install_or_reader_waits_for_the_next_run(self):
        cfn = FakeCfn(root_status="UPDATE_IN_PROGRESS")
        self.assertIn("busy", reader.keep(cfn, FakeIam(), "coderhelm-acme"))
        self.assertEqual(written(cfn), [])
        cfn = FakeCfn(reader_status="UPDATE_IN_PROGRESS")
        self.assertIn("busy", reader.keep(cfn, FakeIam(), "coderhelm-acme"))
        self.assertEqual(written(cfn), [])

    def test_a_failed_first_create_is_removed_and_made_again(self):
        cfn = FakeCfn(reader_status="ROLLBACK_COMPLETE")
        reader.keep(cfn, FakeIam(), "coderhelm-acme")
        self.assertEqual([c[0] for c in cfn.calls if c[0] != "summary"], ["delete", "create"])

    def test_only_a_release_version_names_a_template(self):
        cfn = FakeCfn(version="main")
        self.assertIn("names no release", reader.keep(cfn, FakeIam(), "coderhelm-acme"))
        for bad in ("../x", "v1.2", "", None):
            with self.assertRaises(ValueError):
                reader.template_url(bad)

    def test_switches_the_stack_lacks_take_their_defaults(self):
        cfn = FakeCfn(params=[])
        reader.keep(cfn, FakeIam(), "coderhelm-acme")
        p = {x["ParameterKey"]: x["ParameterValue"] for x in written(cfn)[0][1]["Parameters"]}
        self.assertEqual((p["InfrastructureVulnerabilities"], p["InspectorFindings"], p["HealthEvents"]), ("on", "off", "off"))


class HandlerTests(unittest.TestCase):
    def test_the_event_is_never_read_and_a_failure_never_raises(self):
        class Broken(FakeCfn):
            def describe_stacks(self, StackName):
                raise RuntimeError("throttled")
        r = reader.handler({"detail": {"anything": "at all"}}, None, cfn=Broken(), iam=FakeIam(), root="coderhelm-acme")
        self.assertIn("next run tries again", r["result"])

    def test_as_a_custom_resource_it_always_answers_success(self):
        sent = []
        orig = reader.respond
        reader.respond = lambda event, status, reason: sent.append(status)
        try:
            ev = {"RequestType": "Create", "ResponseURL": "https://x", "StackId": "s", "RequestId": "r", "LogicalResourceId": "ReaderNow"}
            reader.handler(ev, None, cfn=FakeCfn(root_status="UPDATE_IN_PROGRESS"), iam=FakeIam(), root="coderhelm-acme")
            reader.handler(dict(ev, RequestType="Delete"), None, cfn=FakeCfn(), iam=FakeIam(), root="coderhelm-acme")
        finally:
            reader.respond = orig
        self.assertEqual(sent, ["SUCCESS", "SUCCESS"])


if __name__ == "__main__":
    unittest.main()
