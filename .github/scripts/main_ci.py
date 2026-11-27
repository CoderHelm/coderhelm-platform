"""Did CI pass on this commit of main?

CI (.github/workflows/ci.yml) runs on every push to main, on the commit that
landed: the merged result, not the pull request's head. Two pull requests
can each pass on their own and still break main together, so the hosted
deploy (deploy.yml) and the self-host releases (auto-release.yml) ship a
commit of main only when CI's push run on exactly that commit succeeded.

The verdict is CI's newest push run on main for the commit (a re-run of it
counts: a re-run keeps the run and updates its conclusion):

- green    the run succeeded;
- red      the run finished any other way (failed, cancelled, timed out);
- pending  no finished run after waiting up to --wait seconds, or GitHub
           could not be read. Never green.

Prints the verdict and exits 0 only on green.

    python3 .github/scripts/main_ci.py COMMIT [--wait SECONDS]   # in Actions
"""

import json
import os
import sys
import time
import urllib.request

WORKFLOW = ".github/workflows/ci.yml"
POLL_SECS = 30


def api(path):
    """GET one GitHub API path as JSON; any failure raises."""
    base = os.environ.get("GITHUB_API_URL", "https://api.github.com")
    req = urllib.request.Request(base + path)
    req.add_header("Accept", "application/vnd.github+json")
    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


def latest(runs, sha):
    """CI's newest push run on main for `sha`, or None."""
    own = [
        r for r in runs
        if r.get("path") == WORKFLOW
        and r.get("head_sha") == sha
        and r.get("event") == "push"
        and r.get("head_branch") == "main"
    ]
    if not own:
        return None
    return max(own, key=lambda r: (r.get("run_number") or 0, r.get("id") or 0))


def verdict(run):
    """'green', 'red', or None while there is no finished run."""
    if run is None or run.get("status") != "completed":
        return None
    return "green" if run.get("conclusion") == "success" else "red"


def run(repo, sha, get=api, sleep=time.sleep, now=time.monotonic, wait=0):
    """'green', 'red' or 'pending' for CI on `sha`, waiting up to `wait` seconds."""
    deadline = now() + wait
    path = f"/repos/{repo}/actions/workflows/ci.yml/runs?branch=main&event=push&head_sha={sha}&per_page=20"
    while True:
        try:
            v = verdict(latest(get(path).get("workflow_runs") or [], sha))
            if v:
                return v
        except Exception as e:
            print(f"could not read CI's runs: {e}", file=sys.stderr)
        if now() >= deadline:
            return "pending"
        sleep(POLL_SECS)


def main(argv):
    args = argv[1:]
    wait = 0
    if len(args) == 3 and args[1] == "--wait":
        wait = int(args[2])
        args = args[:1]
    if len(args) != 1 or len(args[0]) != 40:
        print("usage: main_ci.py COMMIT [--wait SECONDS] (COMMIT: a full SHA)", file=sys.stderr)
        return 2
    sha = args[0]
    result = run(os.environ["GITHUB_REPOSITORY"], sha, wait=wait)
    print(result)
    if result == "red":
        print(f"::error::CI failed on {sha}, the merged result on main: it is not shipped. "
              "A fix merged to main ships once CI passes on it.")
    elif result == "pending":
        print(f"::error::CI has not passed on {sha} on main yet: it is not shipped.")
    return 0 if result == "green" else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
