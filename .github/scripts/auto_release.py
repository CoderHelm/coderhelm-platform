#!/usr/bin/env python3
"""Plans the next automatic self-host release (.github/workflows/auto-release.yml).

    python3 .github/scripts/auto_release.py plan --repo OWNER/NAME --commit SHA \\
        --notes-out notes.md [--github-output "$GITHUB_OUTPUT"]

Run in a full clone of main (all tags fetched). It decides, for the deployed
commit SHA on main:

  skip      when SHA already has a release tag, when a newer release than the
            last one SHA contains exists (a later commit was already released),
            or when nothing that ships in a self-host release changed since the
            last release (SHIPPED: the paths the release bundle is built from);
  version   the last stable release vX.Y.Z on SHA's history, bumped:
              minor  when a pull request merged since then is a feature: the
                     "enhancement" or "feature" label, or a conventional
                     "feat:" / "feat(scope):" title;
              patch  otherwise;
            and never below installer/package.json's version at SHA, which is
            how a maintainer asks for a minor or major release in a reviewed
            pull request (the first self-host release, v0.7.0, comes from it);
  notes     the merged pull requests since the last release, as Markdown.

Only tags on main count: a v* tag anywhere else is never a release (installs
refuse it), so it neither sets the base nor blocks the plan; the version
skips past its name instead. The tag it names never exists yet: a tag is
never re-cut or moved.

Outputs (GITHUB_OUTPUT): skip=true|false, reason, tag, previous.
Only the standard library is used; git and gh must be on PATH.
"""

import argparse
import json
import re
import subprocess
import sys

STABLE = re.compile(r"^v(\d+)\.(\d+)\.(\d+)$")
FEATURE_TITLE = re.compile(r"^feat(\([^)]*\))?!?:", re.IGNORECASE)
FEATURE_LABELS = {"enhancement", "feature"}

# What a self-host release is built from (.github/workflows/release.yml, build
# job): the Rust Lambdas (services/), the MCP proxy and the updater (lambda/),
# the CDK app and the CloudFormation templates (infra/), the installer
# (installer/), install.sh, the dashboard pin and
# migrations (selfhost/), the dashboard pin check, and the release workflow.
SHIPPED = (
    "services/",
    "lambda/",
    "infra/",
    "installer/",
    "selfhost/",
    "scripts/check-dashboard-ref.mjs",
    ".github/workflows/release.yml",
)


def parse(tag):
    """(major, minor, patch) of a stable release tag, or None."""
    m = STABLE.match(tag or "")
    return tuple(int(x) for x in m.groups()) if m else None


def fmt(v):
    return "v%d.%d.%d" % v


def ships(paths):
    """The changed paths that are part of a self-host release."""
    return [p for p in paths if any(p == s or (s.endswith("/") and p.startswith(s)) for s in SHIPPED)]


def is_feature(pr):
    labels = {str(n).lower() for n in pr.get("labels") or []}
    return bool(labels & FEATURE_LABELS) or bool(FEATURE_TITLE.match(pr.get("title") or ""))


def next_version(last, prs, floor=None):
    """The version after `last` (a tuple, or None for no release yet) for these pull requests."""
    base = last or (0, 0, 0)
    if any(is_feature(p) for p in prs):
        bumped = (base[0], base[1] + 1, 0)
    else:
        bumped = (base[0], base[1], base[2] + 1)
    if floor and floor > bumped:
        return floor
    return bumped


MAX_LISTED = 100


def notes(repo, tag, previous, prs):
    """Markdown release notes: the merged pull requests, features first (the newest MAX_LISTED)."""
    lines = ["## What changed", ""]
    shown = sorted(prs, key=lambda p: p.get("number") or 0)[-MAX_LISTED:]
    feats = [p for p in shown if is_feature(p)]
    rest = [p for p in shown if not is_feature(p)]
    if not prs:
        lines.append("- Maintenance changes (no pull request found for the commits).")
    for group, title in ((feats, "Features"), (rest, "Fixes and changes")):
        if not group:
            continue
        if feats:
            lines += ["### " + title, ""]
        for p in group:
            # One line each; a title can't break the list or start a heading.
            t = " ".join(str(p.get("title") or "").split())
            lines.append("- %s (#%d)" % (t, p["number"]) if p.get("number") else "- %s" % t)
        lines.append("")
    if len(prs) > len(shown):
        lines += ["And %d earlier pull request(s)." % (len(prs) - len(shown)), ""]
    if previous:
        lines.append("**Full list of changes:** https://github.com/%s/compare/%s...%s" % (repo, previous, tag))
    return "\n".join(lines).rstrip() + "\n"


def plan(commit, tags, tags_at_commit, is_ancestor, changed_paths, merged_prs, floor=None, on_main=None):
    """The decision for `commit`: {skip, reason, tag, previous, prs}.

    tags            every tag name in the repository;
    tags_at_commit  the tag names pointing at `commit`;
    is_ancestor     f(tag) -> True when the tag's commit is in `commit`'s history;
    changed_paths   f(previous_tag_or_None) -> paths changed since it;
    merged_prs      f(previous_tag_or_None) -> pull requests merged since it;
    floor           installer/package.json's version as a tuple, or None;
    on_main         f(tag) -> True when the tag's commit is on main (default: is_ancestor).
    """
    on_main = on_main or is_ancestor
    released = list(tags_at_commit)
    if released:
        return {"skip": True, "reason": "%s is already released as %s" % (commit[:12], ", ".join(sorted(released)))}
    stable = sorted((parse(t), t) for t in tags if parse(t) and on_main(t))
    mine = [(v, t) for v, t in stable if is_ancestor(t)]
    previous = mine[-1][1] if mine else None
    if stable and (not mine or stable[-1][0] > mine[-1][0]):
        return {"skip": True, "reason": "%s is newer than this commit's history: a later commit is already released" % stable[-1][1]}
    shipped = ships(changed_paths(previous))
    if not shipped:
        return {"skip": True, "reason": "nothing that ships in a self-host release changed since %s" % (previous or "the start")}
    prs = merged_prs(previous)
    version = next_version(parse(previous) if previous else None, prs, floor)
    # A name taken by a tag that isn't a release (not on main) is skipped, never reused.
    while fmt(version) in tags:
        version = (version[0], version[1], version[2] + 1)
    tag = fmt(version)
    return {"skip": False, "reason": "%d shipped file(s) changed since %s" % (len(shipped), previous or "the start"),
            "tag": tag, "previous": previous, "prs": prs}


# ---------------------------------------------------------------------------
# git and GitHub
# ---------------------------------------------------------------------------


def _run(*args):
    return subprocess.run(args, check=True, capture_output=True, text=True).stdout


def _merged_prs(repo, since):
    """Pull requests merged into main (on or after the date `since`, YYYY-MM-DD, when given), with their merge commits."""
    search = "merged:>=%s" % since if since else ""
    try:
        out = _run("gh", "pr", "list", "-R", repo, "--state", "merged", "--base", "main", "--limit", "1000",
                   "--search", search, "--json", "number,title,labels,mergeCommit")
    except subprocess.CalledProcessError:
        return []
    return [{"number": p["number"], "title": p.get("title") or "", "labels": [l.get("name") for l in p.get("labels") or []],
             "commit": (p.get("mergeCommit") or {}).get("oid")} for p in json.loads(out or "[]")]


def git_plan(repo, commit):
    tags = [t for t in _run("git", "tag", "-l", "v*").split() if t]
    at = [t for t in _run("git", "tag", "--points-at", commit, "-l", "v*").split() if t]

    def is_ancestor(tag, of=commit):
        return subprocess.run(["git", "merge-base", "--is-ancestor", tag + "^{commit}", of], capture_output=True).returncode == 0

    main = "refs/remotes/origin/main" if subprocess.run(["git", "rev-parse", "-q", "--verify", "refs/remotes/origin/main"], capture_output=True).returncode == 0 else commit

    def rng(previous):
        return "%s..%s" % (previous, commit) if previous else commit

    def changed(previous):
        if not previous:
            return _run("git", "ls-tree", "-r", "--name-only", commit).split("\n")
        return [p for p in _run("git", "diff", "--name-only", previous, commit).split("\n") if p]

    def prs(previous):
        # The pull requests whose merge commit is in this release's range of main.
        shas = set(_run("git", "log", "--first-parent", "--format=%H", rng(previous)).split())
        since = _run("git", "log", "-1", "--format=%cs", previous + "^{commit}").strip() if previous else None
        found = [p for p in _merged_prs(repo, since) if p["commit"] in shas]
        return sorted(found, key=lambda p: p["number"])

    floor = None
    try:
        floor = parse("v" + json.loads(_run("git", "show", "%s:installer/package.json" % commit)).get("version", ""))
    except (subprocess.CalledProcessError, ValueError):
        pass
    return plan(commit, tags, at, is_ancestor, changed, prs, floor, on_main=lambda t: is_ancestor(t, main))


def main(argv):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("plan")
    p.add_argument("--repo", required=True)
    p.add_argument("--commit", required=True)
    p.add_argument("--notes-out", required=True)
    p.add_argument("--github-output")
    a = ap.parse_args(argv)
    if not re.fullmatch(r"[0-9a-f]{40}", a.commit):
        raise SystemExit("--commit must be a full commit SHA")
    if not re.fullmatch(r"[A-Za-z0-9-]+/[A-Za-z0-9._-]+", a.repo):
        raise SystemExit("--repo must be OWNER/NAME")
    r = git_plan(a.repo, a.commit)
    out = {"skip": "true" if r["skip"] else "false", "reason": r["reason"], "tag": r.get("tag", ""), "previous": r.get("previous") or ""}
    if not r["skip"]:
        assert STABLE.match(out["tag"])
        with open(a.notes_out, "w", encoding="utf-8") as f:
            f.write(notes(a.repo, r["tag"], r.get("previous"), r["prs"]))
    print(json.dumps(out))
    if a.github_output:
        with open(a.github_output, "a", encoding="utf-8") as f:
            for k, v in out.items():
                f.write("%s=%s\n" % (k, " ".join(str(v).split())))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
