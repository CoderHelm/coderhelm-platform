"""CI gate for CoderHelm's "Customer details guard" check run.

CoderHelm sets that check run on every pull request of a public repository
it is installed on, from its own records (services/common/src/customer_guard).
This gate makes the repository's CI follow it:

- the guard's newest run on the PR's head failed: the gate fails ("the guard
  reported a customer detail");
- it passed, or was skipped (a pull request from a fork): the gate passes;
- there is no finished guard run after waiting up to five minutes (the guard
  did not run, or its runs can't be read): the gate says so and falls back
  to the rules CI can run itself without any secret or list, the public
  text guard (.github/scripts/text_guard.py: account ids, TEAM# ids, run
  ids, ticket keys, email addresses). A missing guard never blocks forever,
  and never passes silently either.

Only the guard's own app counts (GUARD_APP, `coderhelm`): a check of the
same name from anything else, a workflow job included, is ignored, so a
present guard can't be overridden.

An edit to the PR's text after CI finished sets the guard again; the
"Customer details guard rerun" workflow then runs this job again.

It prints only why it passed or failed, and the place and rule of a
fallback hit; never a matched text.

    python3 .github/scripts/customer_guard_gate.py   # in Actions, on pull_request
"""

import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

CHECK = "Customer details guard"
APP = os.environ.get("GUARD_APP", "coderhelm")
WAIT_SECS = int(os.environ.get("GUARD_WAIT_SECS", "300"))
POLL_SECS = 15


class NotFound(Exception):
    pass


def api(path):
    """GET one GitHub API path; NotFound on a 404, any other failure raises."""
    base = os.environ.get("GITHUB_API_URL", "https://api.github.com")
    req = urllib.request.Request(base + path)
    req.add_header("Accept", "application/vnd.github+json")
    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            raise NotFound() from None
        raise


def latest(runs, app=APP):
    """The guard's newest check run from its own app, or None."""
    own = [r for r in runs if r.get("name") == CHECK and (r.get("app") or {}).get("slug") == app]
    if not own:
        return None
    return max(own, key=lambda r: (r.get("started_at") or "", r.get("id") or 0))


def verdict(run):
    """'pass', 'fail', or None while there is no finished run."""
    if run is None or run.get("status") != "completed":
        return None
    return "pass" if run.get("conclusion") in ("success", "neutral", "skipped") else "fail"


def event():
    with open(os.environ["GITHUB_EVENT_PATH"], encoding="utf-8") as f:
        return json.load(f)


def head_sha(ev=None):
    sha = os.environ.get("HEAD_SHA", "")
    if sha:
        return sha
    return (ev or event())["pull_request"]["head"]["sha"]


# Outcomes: (passed, why).
PRESENT_PASS = (True, "pass: the Customer details guard passed on this commit")
PRESENT_SKIP = (True, "pass: the Customer details guard skipped this pull request (not a member's branch of this repository)")
PRESENT_FAIL = (False, "fail: the Customer details guard reported a customer detail; its check run says where")


def wait_for_guard(repo, sha, get=api, sleep=time.sleep, now=time.monotonic, wait=WAIT_SECS):
    """The guard's outcome, or None when no finished guard run was found in
    time (missing, still running, or unreadable)."""
    name = urllib.parse.quote(CHECK)
    deadline = now() + wait
    while True:
        try:
            runs = get(f"/repos/{repo}/commits/{sha}/check-runs?check_name={name}&filter=all&per_page=100")
            run = latest(runs.get("check_runs") or [])
            v = verdict(run)
            if v == "fail":
                return PRESENT_FAIL
            if v == "pass":
                return PRESENT_SKIP if run.get("conclusion") == "neutral" else PRESENT_PASS
        except Exception:
            pass
        if now() >= deadline:
            return None
        sleep(POLL_SECS)


def fallback(ev, scan=None):
    """(passed, why, lines): the public text guard's rules over the PR's
    text, run here because the guard's own check never came."""
    if scan is None:
        import text_guard
        def scan(e):
            return text_guard.report(text_guard.sources_from_pr(e), text_guard.rules_for())
    lines = scan(ev)
    head = "no Customer details guard check found after 5 min; ran CI's own rules (ids, keys, emails) instead"
    if lines:
        return False, f"fail: {head}: {len(lines)} line(s) carry an id, key or email", lines
    return True, f"pass: {head}: none found", []


def run(repo, sha, ev, get=api, sleep=time.sleep, now=time.monotonic, wait=WAIT_SECS, scan=None):
    """(passed, why, lines) for the guard on `sha`."""
    found = wait_for_guard(repo, sha, get=get, sleep=sleep, now=now, wait=wait)
    if found is not None:
        return found[0], found[1], []
    try:
        return fallback(ev, scan)
    except Exception:
        return False, "fail: no Customer details guard check found after 5 min, and CI's own rules could not read the pull request", []


def main():
    try:
        ev = event()
        passed, why, lines = run(os.environ["GITHUB_REPOSITORY"], head_sha(ev), ev)
    except Exception:
        passed, why, lines = False, "fail: the gate could not read this pull request", []
    for line in lines:
        print(line)
    print(why)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
