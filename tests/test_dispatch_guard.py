"""Dispatch skip guard (monitor.skip_reason, via _next_dispatchable).

Run:  python3 -m unittest discover -s tests -v     (from the skill root)

Stdlib only (the monitor targets macOS system python 3.9). gh is faked through
monitor._gh, and BASE_ROOT points at a temp dir, so nothing here reads or writes
a live run's queue/state under /tmp/dual-author.
"""
import copy
import importlib.util
import json
import os
import shutil
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
_spec = importlib.util.spec_from_file_location(
    "monitor", os.path.join(ROOT, "scripts", "monitor.py"))
monitor = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(monitor)

REPO = "acme/widgets"


class FakeGh:
    """Stands in for monitor._gh. `issues` maps number -> issue dict (labels,
    state, pr) served by `gh api repos/R/issues/N`; `fail` makes that call fail
    like an unauthenticated / rate-limited gh. Blocker lookups report none."""

    def __init__(self, issues=None, fail=False):
        self.issues = issues or {}
        self.fail = fail
        self.calls = []

    def __call__(self, *args, timeout=None):
        self.calls.append(args)
        if args[0] == "api" and args[1].endswith("/dependencies/blocked_by"):
            return True, "[]"
        if args[0] == "api" and args[1].startswith(f"repos/{REPO}/issues/"):
            if self.fail:
                return False, ""
            n = args[1].rsplit("/", 1)[1]
            d = self.issues.get(n, {})
            return True, json.dumps({"labels": d.get("labels", []),
                                     "state": d.get("state", "OPEN").lower(),
                                     "pr": d.get("pr", False)})
        if args[:2] == ("issue", "view"):
            return True, ""  # empty body: no "blocked by #N"
        return False, ""

    def meta_calls(self, n):
        return [c for c in self.calls if c[:2] == ("api", f"repos/{REPO}/issues/{n}")]


class DispatchGuardTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="dual-author-guard-")
        self._saved = (monitor.BASE_ROOT, monitor._NS, monitor._REPO, monitor._CFG,
                       monitor._gh)
        monitor.BASE_ROOT = self.tmp
        monitor._NS = "guard-test"
        monitor._REPO = REPO
        monitor._CFG = copy.deepcopy(monitor.DEFAULTS)
        for c in (monitor._META_CACHE, monitor._META_WARNED, monitor._GATE_CACHE,
                  monitor._STATE_CACHE, monitor._GATE_WARNED):
            c.clear()
        self.assertTrue(monitor.queue_path().startswith(self.tmp))
        os.makedirs(monitor.base(), exist_ok=True)
        self.state = {}

    def tearDown(self):
        (monitor.BASE_ROOT, monitor._NS, monitor._REPO, monitor._CFG,
         monitor._gh) = self._saved
        shutil.rmtree(self.tmp, ignore_errors=True)

    # helpers
    def gh(self, **kw):
        fake = FakeGh(**kw)
        monitor._gh = fake
        return fake

    def queue(self, *entries):
        with open(monitor.queue_path(), "w") as f:
            f.write("".join(f"{e}\n" for e in entries))
        return list(entries)

    def queued(self):
        with open(monitor.queue_path()) as f:
            return [ln.strip() for ln in f if ln.strip()]

    def feed(self):
        return [(e["issue"], e["text"]) for e in self.state.get("_events", [])]

    def pick(self):
        q = monitor.read_queue([])
        return monitor._next_dispatchable(self.state, q)

    # cases the owner asked for
    def test_epic_label_is_popped(self):
        self.gh(issues={"101": {"labels": ["epic", "area:x"]}})
        self.queue("101", "102")
        self.assertEqual(self.pick(), "102")
        self.assertEqual(self.queued(), ["102"])
        self.assertIn(("101", "skipped: labelled epic (not for dual-author)"),
                      self.feed())

    def test_owner_step_label_is_popped(self):
        self.gh(issues={"101": {"labels": ["owner-step"]}})
        self.queue("#101", "102")
        self.assertEqual(self.pick(), "102")
        self.assertEqual(self.queued(), ["102"])
        self.assertIn(("101", "skipped: labelled owner-step (not for dual-author)"),
                      self.feed())

    def test_closed_issue_is_popped(self):
        self.gh(issues={"101": {"state": "CLOSED"}})
        self.queue("101", "102")
        self.assertEqual(self.pick(), "102")
        self.assertEqual(self.queued(), ["102"])
        self.assertIn(("101", "skipped: issue is closed (nothing to dispatch)"),
                      self.feed())

    def test_gh_failure_dispatches_and_says_so_once(self):
        fake = self.gh(fail=True)
        self.queue("101")
        self.assertEqual(self.pick(), "101")
        self.assertEqual(self.queued(), ["101"])
        warn = ("101", "⚠ label check unavailable (gh): dispatching unguarded")
        self.assertEqual(self.feed().count(warn), 1)
        monitor._META_CACHE.clear()  # force a second live (failing) lookup
        self.assertEqual(self.pick(), "101")
        self.assertEqual(len(fake.meta_calls("101")), 2)
        self.assertEqual(self.feed().count(warn), 1)

    def test_no_labels_dispatches(self):
        self.gh(issues={"101": {"labels": []}})
        self.queue("101", "102")
        self.assertEqual(self.pick(), "101")
        self.assertEqual(self.queued(), ["101", "102"])  # popped only on dispatch
        self.assertEqual(self.feed(), [])

    # edges
    def test_pull_request_is_popped(self):
        self.gh(issues={"101": {"pr": True}})
        self.queue("101", "102")
        self.assertEqual(self.pick(), "102")
        self.assertEqual(self.queued(), ["102"])
        self.assertIn(("101", "skipped: is a pull request, not an issue "
                              "(not for dual-author)"), self.feed())

    def test_unrelated_labels_dispatch(self):
        self.gh(issues={"101": {"labels": ["bug", "epic-adjacent"]}})
        self.queue("101")
        self.assertEqual(self.pick(), "101")
        self.assertEqual(self.queued(), ["101"])

    def test_label_match_ignores_case(self):
        self.gh(issues={"101": {"labels": ["Epic"]}})
        self.queue("101")
        self.assertIsNone(self.pick())
        self.assertEqual(self.queued(), [])

    def test_every_entry_skipped_returns_none(self):
        self.gh(issues={"101": {"labels": ["epic"]}, "102": {"state": "CLOSED"}})
        self.queue("101", "102")
        self.assertIsNone(self.pick())
        self.assertEqual(self.queued(), [])

    def test_guard_runs_without_respect_dependencies(self):
        monitor._CFG["dispatch"]["respect_dependencies"] = False
        self.gh(issues={"101": {"labels": ["epic"]}})
        self.queue("101", "102")
        self.assertEqual(self.pick(), "102")
        self.assertEqual(self.queued(), ["102"])

    def test_skip_labels_config_is_honoured(self):
        monitor._CFG["dispatch"]["skip_labels"] = ["wontfix"]
        self.gh(issues={"101": {"labels": ["epic"]}, "102": {"labels": ["wontfix"]}})
        self.queue("102", "101")
        self.assertEqual(self.pick(), "101")
        self.assertEqual(self.queued(), ["101"])

    def test_gh_failure_holds_when_fail_closed(self):
        monitor._CFG["dispatch"]["dependency_fail_closed"] = True
        self.gh(fail=True)
        self.queue("101")
        self.assertIsNone(self.pick())
        self.assertEqual(self.queued(), ["101"])
        self.assertNotIn("101", monitor._META_CACHE)  # retried live next tick
        self.assertIn(("101", "⛔ label check unavailable (gh): holding (fail-closed)"),
                      self.feed())

    def test_lookup_is_cached(self):
        fake = self.gh(issues={"101": {"labels": []}})
        self.queue("101")
        self.pick()
        self.pick()
        self.assertEqual(len(fake.meta_calls("101")), 1)

    def test_shipped_config_skips_epic_and_owner_step(self):
        with open(os.path.join(ROOT, "config.toml")) as f:
            conf = monitor._toml_load(f.read())
        self.assertEqual(conf["dispatch"]["skip_labels"], ["epic", "owner-step"])
        self.assertEqual(monitor.DEFAULTS["dispatch"]["skip_labels"],
                         ["epic", "owner-step"])


if __name__ == "__main__":
    unittest.main()
