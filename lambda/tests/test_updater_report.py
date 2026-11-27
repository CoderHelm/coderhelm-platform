"""The release report: with the `report` setting on, the hourly check tells
CoderHelm how the release whose post-update watch ended did here (version,
the watch's end, counts of finished runs and Chat answers, whether this
install uses Chat), and nothing else. Stable promotion waits for it.

Run: python3 -m unittest discover -s lambda/tests
"""

import unittest

import test_updater_native as base

u = base.u
NOW = base.NOW


class Dynamo(base.FakeDynamo):
    def query(self, TableName, KeyConditionExpression, ExpressionAttributeValues, **kw):
        if "begins_with" in KeyConditionExpression:
            return super().query(TableName, KeyConditionExpression, ExpressionAttributeValues,
                                 kw.get("ScanIndexForward", True), kw.get("Limit", 100))
        p = ExpressionAttributeValues[":p"]["S"]
        return {"Items": [i for k, i in sorted(self.items.items()) if k[0] == p]}


def use(store, version, runs=0, answers=0):
    item = {"pk": {"S": u.RELEASE_USE_PK}, "sk": {"S": "VERSION#" + version}}
    if runs:
        item["runs"] = {"N": str(runs)}
    if answers:
        item["answers"] = {"N": str(answers)}
    store.c.put_item(TableName="t", Item=item)


class Report(unittest.TestCase):
    def setUp(self):
        self.store = u.Store(Dynamo(), "t")
        self.sent = []
        self._creds = u.relay_credentials
        u.relay_credentials = lambda env: {"id": "inst_1", "secret": "s"}
        self.on = dict(u.DEFAULT_SETTINGS, report=True)

    def tearDown(self):
        u.relay_credentials = self._creds

    def send(self, env, creds, path, body):
        self.sent.append((path, body))

    def report(self, settings=None):
        return u.release_report(self.store, {}, NOW, settings or self.on, send=self.send)

    def test_off_by_default_and_a_setting_like_any_other(self):
        self.assertIs(u.DEFAULT_SETTINGS["report"], False)
        self.assertIs(u.validate_settings({"report": True})["report"], True)
        with self.assertRaises(ValueError):
            u.validate_settings({"report": "yes"})
        self.store.put(u.WATCH, {"version": "v1.1.0-beta.20300101", "status": "healthy"})
        self.assertIsNone(self.report(u.DEFAULT_SETTINGS))
        self.assertEqual(self.sent, [])

    def test_a_healthy_watch_reports_counts_only_and_again_when_they_change(self):
        v = "v1.1.0-beta.20300101"
        self.store.put(u.WATCH, {"version": v, "status": "healthy", "from_version": "v1.0.0", "probe": {"detail": "x"}})
        use(self.store, "v1.0.0", runs=9, answers=4)
        use(self.store, v, runs=2)
        body = self.report()
        self.assertEqual(body, {"version": v, "watch": "healthy", "runs": 2, "answers": 0, "chat": True})
        self.assertEqual(self.sent, [(u.REPORT_PATH, body)])
        self.assertIsNone(self.report(), "nothing new: nothing sent")
        use(self.store, v, runs=2, answers=1)
        self.assertEqual(self.report()["answers"], 1)
        self.assertEqual(len(self.sent), 2)

    def test_a_rollback_is_reported_and_an_open_watch_is_not(self):
        v = "v1.1.0-beta.20300101"
        for status in ("watching", "rolling_back", "ended", "rollback_failed"):
            self.store.put(u.WATCH, {"version": v, "status": status})
            self.assertIsNone(self.report(), status)
        self.store.put(u.WATCH, {"version": v, "status": "rolled_back"})
        self.assertEqual(self.report(), {"version": v, "watch": "rolled_back", "runs": 0, "answers": 0, "chat": False})

    def test_an_unregistered_install_sends_nothing_and_never_fails(self):
        self.store.put(u.WATCH, {"version": "v1.1.0", "status": "healthy"})
        u.relay_credentials = lambda env: None
        self.assertIsNone(self.report())

        def boom(*a):
            raise OSError("no route")
        u.relay_credentials = lambda env: {"id": "i", "secret": "s"}
        self.assertIsNone(u.release_report(self.store, {}, NOW, self.on, send=boom))
        self.assertIsNone(self.store.get(u.REPORTED), "not marked sent: the next check tries again")

    def test_the_hourly_check_sends_it(self):
        with open(u.__file__) as f:
            src = f.read()
        tick = src[src.index('if action == "tick":'):src.index('raise ValueError(f"unknown action: {action}")')]
        self.assertIn("release_report(store, env, now, settings)", tick)


if __name__ == "__main__":
    unittest.main()
