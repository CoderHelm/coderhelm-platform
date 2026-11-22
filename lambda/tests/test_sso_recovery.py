"""The self-hosted recovery script for "Require single sign-on"
(scripts/sso-recovery.py), against a stand-in DynamoDB client.

Run: python3 -m unittest discover -s lambda/tests
"""

import datetime
import importlib.util
import pathlib
import unittest

SCRIPT = pathlib.Path(__file__).resolve().parents[2] / "scripts" / "sso-recovery.py"


def load():
    spec = importlib.util.spec_from_file_location("sso_recovery", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeDynamo:
    """Items by (pk, sk); a transaction applies only when every condition holds."""

    def __init__(self, org=None):
        self.items = {}
        self.transactions = []
        if org is not None:
            self.items[("ORG", "META")] = org

    def get_item(self, TableName, Key, ConsistentRead):
        assert ConsistentRead
        item = self.items.get((Key["pk"]["S"], Key["sk"]["S"]))
        return {"Item": dict(item)} if item else {}

    def transact_write_items(self, TransactItems):
        self.transactions.append(TransactItems)
        for t in TransactItems:
            if "Update" in t:
                u = t["Update"]
                key = (u["Key"]["pk"]["S"], u["Key"]["sk"]["S"])
                assert u["ConditionExpression"] == "attribute_exists(require_sso)"
                if "require_sso" not in self.items.get(key, {}):
                    raise RuntimeError("TransactionCanceledException")
        for t in TransactItems:
            if "Update" in t:
                u = t["Update"]
                key = (u["Key"]["pk"]["S"], u["Key"]["sk"]["S"])
                item = self.items[key]
                for a in ("require_sso", "require_sso_since"):
                    item.pop(a, None)
                v = u["ExpressionAttributeValues"]
                item["sso_recovery_notice"] = v[":n"]
                item["updated_at"] = v[":at"]
                item["updated_by"] = v[":by"]
            if "Put" in t:
                i = t["Put"]["Item"]
                self.items[(i["pk"]["S"], i["sk"]["S"])] = i


def org(on=True):
    item = {"pk": {"S": "ORG"}, "sk": {"S": "META"}, "team_id": {"S": "TEAM#org"}}
    if on:
        item["require_sso"] = {"S": "okta"}
        item["require_sso_since"] = {"N": "1000"}
    return item


class Recovery(unittest.TestCase):
    def setUp(self):
        self.m = load()
        self.lines = []

    def run_it(self, db, apply=True, note="Okta is down; turning it off to sign in"):
        return self.m.run(db, "settings", "Ops person", note, apply, out=self.lines.append,
                          t=datetime.datetime(2026, 11, 21, 12, 0, tzinfo=datetime.timezone.utc))

    def test_turns_the_rule_off_and_writes_the_audit_line(self):
        db = FakeDynamo(org())
        self.assertEqual(self.run_it(db), 0)
        meta = db.items[("ORG", "META")]
        self.assertNotIn("require_sso", meta)
        self.assertNotIn("require_sso_since", meta)
        self.assertTrue(meta["sso_recovery_notice"]["S"].startswith("okta|Ops person|"))
        audit = [i for (pk, sk), i in db.items.items() if sk.startswith("ROLE_AUDIT#")]
        self.assertEqual(len(audit), 1)
        self.assertEqual(audit[0]["pk"]["S"], "TEAM#org")
        self.assertEqual(audit[0]["action"]["S"], "sign_in_rules")
        self.assertIn("Ops person", audit[0]["detail"]["S"])
        self.assertIn("Okta is down", audit[0]["detail"]["S"])
        # One transaction: the rule and its audit line land together.
        self.assertEqual(len(db.transactions), 1)

    def test_a_dry_run_changes_nothing(self):
        db = FakeDynamo(org())
        self.assertEqual(self.run_it(db, apply=False), 0)
        self.assertEqual(db.transactions, [])
        self.assertIn("require_sso", db.items[("ORG", "META")])

    def test_off_already_is_nothing_to_do(self):
        db = FakeDynamo(org(on=False))
        self.assertEqual(self.run_it(db), 0)
        self.assertEqual(db.transactions, [])

    def test_no_note_no_change(self):
        db = FakeDynamo(org())
        self.assertEqual(self.run_it(db, note="x"), 2)
        self.assertEqual(db.transactions, [])

    def test_no_organization_record(self):
        self.assertEqual(self.run_it(FakeDynamo()), 1)

    def test_the_names_match_the_gateway(self):
        # The gateway reads these names (routes/require_sso.rs, sign_in.rs).
        src = (SCRIPT.parents[1] / "services" / "gateway" / "src" / "routes" / "require_sso.rs").read_text()
        self.assertIn('pub const RECOVERY_NOTICE: &str = "sso_recovery_notice";', src)
        self.assertIn('pub const RULE_ATTR: &str = "require_sso";', src)
        self.assertIn('pub const SINCE_ATTR: &str = "require_sso_since";', src)
        roles = (SCRIPT.parents[1] / "services" / "gateway" / "src" / "routes" / "roles.rs").read_text()
        self.assertIn('pub const AUDIT_PREFIX: &str = "ROLE_AUDIT#";', roles)


if __name__ == "__main__":
    unittest.main()
