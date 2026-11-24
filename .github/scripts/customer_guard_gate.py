"""CI gate for CoderHelm's "Customer details guard" check run.

CoderHelm sets that check run on every pull request of a public repository
it is installed on, from its own records (services/common/src/customer_guard).
This gate makes the repository's CI wait for it: it fails when the guard's
newest run on the PR's head failed, or when there is none, finished, after
waiting up to five minutes. A skipped run (a pull request from a fork) passes.

Only the guard's own app counts (GUARD_APP, `coderhelm`): a check of the
same name from anything else, a workflow job included, is ignored.

An edit to the PR's text after CI finished sets the guard again; the
"Customer details guard rerun" workflow then runs this job again, so the
required CI follows the text.

Until the guard is deployed (the last successful deploy of the platform
does not yet contain it), the gate passes: nothing could set the check.

It prints only "pass" or "fail".

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
PLATFORM = "CoderHelm/coderhelm-platform"
GUARD_SOURCE = "services/worker/src/passes/customer_guard.rs"
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


def guard_live(get=api):
    """Is the guard deployed: the last successful deploy's commit has it. A
    read that fails counts as deployed, so a failure never waves a PR on."""
    try:
        runs = get(f"/repos/{PLATFORM}/actions/workflows/deploy.yml/runs?branch=main&status=success&per_page=1")
        sha = (runs.get("workflow_runs") or [{}])[0].get("head_sha")
        if not sha:
            return True
        get(f"/repos/{PLATFORM}/contents/{GUARD_SOURCE}?ref={sha}")
        return True
    except NotFound:
        return False
    except Exception:
        return True


def head_sha():
    sha = os.environ.get("HEAD_SHA", "")
    if sha:
        return sha
    with open(os.environ["GITHUB_EVENT_PATH"], encoding="utf-8") as f:
        return json.load(f)["pull_request"]["head"]["sha"]


def run(repo, sha, get=api, sleep=time.sleep, now=time.monotonic, wait=WAIT_SECS):
    """'pass' or 'fail' for the guard on `sha`."""
    if not guard_live(get):
        return "pass"
    name = urllib.parse.quote(CHECK)
    deadline = now() + wait
    while True:
        try:
            runs = get(f"/repos/{repo}/commits/{sha}/check-runs?check_name={name}&filter=all&per_page=100")
            v = verdict(latest(runs.get("check_runs") or []))
            if v:
                return v
        except Exception:
            pass
        if now() >= deadline:
            return "fail"
        sleep(POLL_SECS)


def main():
    try:
        result = run(os.environ["GITHUB_REPOSITORY"], head_sha())
    except Exception:
        result = "fail"
    print(result)
    return 0 if result == "pass" else 1


if __name__ == "__main__":
    sys.exit(main())
