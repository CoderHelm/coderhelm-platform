"""The beta channel installs at any hour. A single maintenance hour a week
(or a day) let at most one beta a day install while several ship; with day
"any" on the beta channel the hourly check installs each new beta, at least
gap_hours after the last install. The stable channel keeps its window.

Run: python3 -m unittest discover -s lambda/tests
"""

import unittest
from datetime import timedelta

import test_updater as t
import test_updater_native as base

u = t.u
BETA = [t.rel("v1.1.0-beta.20261105", pre=True)]
ANY = dict(t.ON, channel="beta", day="any")


class Settings(unittest.TestCase):
    def test_any_hour_is_a_beta_setting(self):
        s = u.validate_settings({"auto": True, "channel": "beta", "day": "any"})
        self.assertEqual((s["day"], s["gap_hours"]), ("any", u.DEFAULT_SETTINGS["gap_hours"]))
        self.assertEqual(u.validate_settings({"channel": "beta", "day": "any", "gap_hours": 6})["gap_hours"], 6)
        with self.assertRaisesRegex(ValueError, "beta channel"):
            u.validate_settings({"channel": "stable", "day": "any"})
        for bad in (0, 169, "2", True):
            with self.assertRaises(ValueError, msg=bad):
                u.validate_settings({"channel": "beta", "day": "any", "gap_hours": bad})

    def test_moving_to_stable_brings_its_window_back(self):
        s = u.validate_settings({"channel": "stable"}, dict(u.DEFAULT_SETTINGS, channel="beta", day="any"))
        self.assertEqual((s["channel"], s["day"]), ("stable", u.DEFAULT_SETTINGS["day"]))

    def test_the_gateway_passes_the_setting_through(self):
        # services/gateway/src/routes/system.rs settings_from lists the keys it takes.
        text = (t.PATH.parents[2] / "services" / "gateway" / "src" / "routes" / "system.rs").read_text()
        for key in u.DEFAULT_SETTINGS:
            self.assertIn(f'"{key}"', text, key)


class Window(unittest.TestCase):
    def test_every_hour_of_every_day(self):
        for h in range(0, 24 * 7, 5):
            at = t.MONDAY + timedelta(hours=h)
            self.assertTrue(u.in_window(at, ANY), at)
            self.assertEqual(u.decide(at, ANY, "v1.0.0", BETA, {})["action"], "update", at)

    def test_stable_keeps_its_hour(self):
        stable = dict(t.ON, channel="stable", day="any")  # only reachable by a hand-edited record
        self.assertFalse(u.in_window(t.MONDAY, stable))
        self.assertFalse(u.in_window(t.MONDAY, t.ON))
        self.assertTrue(u.in_window(t.SUNDAY_3AM, t.ON))

    def test_a_minimum_gap_after_the_last_install(self):
        last = t.MONDAY - timedelta(minutes=30)
        d = u.decide(t.MONDAY, ANY, "v1.0.0", BETA, {}, last_install=last)
        self.assertEqual(d["action"], "notify")
        self.assertIn("less than 2 hours ago", d["reason"])
        later = last + timedelta(hours=2)
        self.assertEqual(u.decide(later, ANY, "v1.0.0", BETA, {}, last_install=last)["action"], "update")

    def test_what_a_person_decides_still_waits(self):
        major = [t.rel("v2.0.0-beta.20261105", pre=True)]
        self.assertEqual(u.decide(t.MONDAY, ANY, "v1.4.0", major, {})["reason"], "a major release waits for an admin")
        self.assertEqual(u.decide(t.MONDAY, dict(ANY, auto=False), "v1.0.0", BETA, {})["reason"], "auto-update is off")


class Tick(unittest.TestCase):
    """The hourly tick starts a beta at an hour no weekly window covers."""

    def setUp(self):
        self.store = u.Store(t.FakeDynamo(), "t")
        self.calls = []
        self._begin, self._mc = u.begin, u.model_check
        u.begin = lambda *a, **k: self.calls.append((a, k))
        u.model_check = lambda *a, **k: {"version": a[3], "results": []}
        self.env = dict(base.ENV, CODERHELM_VERSION="v1.0.0")

    def tearDown(self):
        u.begin, u.model_check = self._begin, self._mc

    def tick(self, at):
        return u.handle({"action": "tick"}, self.env, self.store, at, fetch=lambda repo: BETA)

    def test_a_new_beta_installs_within_the_hour(self):
        self.store.put("AUTO_UPDATE", {"auto": True, "channel": "beta", "day": "any"})
        at = t.MONDAY.replace(hour=14)
        d = self.tick(at)
        self.assertEqual(d["action"], "update", d)
        (args, kw), = self.calls
        self.assertEqual(args[3], "v1.1.0-beta.20261105")
        self.assertTrue(kw.get("scheduled"))

    def test_the_last_install_sets_the_gap(self):
        self.store.put("AUTO_UPDATE", {"auto": True, "channel": "beta", "day": "any", "gap_hours": 3})
        at = t.MONDAY.replace(hour=14)
        self.store.put("UPDATE#2026-10-26T13:00:00Z#aaaaaaaa", {
            "mode": "update", "version": "v1.0.0", "status": "succeeded",
            "started_at": (at - timedelta(hours=1)).isoformat(), "executed_at": (at - timedelta(hours=1)).isoformat()})
        self.assertEqual(self.tick(at)["action"], "notify")
        self.assertEqual(self.calls, [])


if __name__ == "__main__":
    unittest.main()
