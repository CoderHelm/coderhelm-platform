#!/usr/bin/env python3
"""Plans the automatic self-host releases (.github/workflows/auto-release.yml).

    python3 .github/scripts/auto_release.py plan --repo OWNER/NAME --mode MODE \\
        [--commit SHA] [--version vX.Y.Z] --notes-dir DIR [--github-output "$GITHUB_OUTPUT"]

Run in a full clone of main (all tags fetched). Two update channels
(docs/self-host/updates.md, "Channels"):

  beta     a nightly prerelease vX.Y.Z-beta.YYYYMMDD of the deployed commit
           SHA on main. Its tag also carries a signed statement for the
           stable tag vX.Y.Z on the same commit, made in the same run, so the
           beta can be promoted later without signing anything then.
  stable   vX.Y.Z: a promoted beta (same commit, same version number without
           the suffix), or cut straight from the deployed commit SHA (an
           urgent fix: deploy.yml run by hand with "stable release").

Modes:

  beta      the beta for SHA, or a skip: SHA already has a release tag, a
            later commit already has one, or nothing that ships in a
            self-host release changed since the last release of either kind
            (SHIPPED: the paths the release bundle is built from);
  stable    the stable release for SHA, or a skip (the same reasons, counted
            from the last stable release);
  promote   the newest beta that is ready (below), as a stable release; with
            --version (a beta tag, or the stable vX.Y.Z it would become), that
            one beta, ready or not by age.

A beta is ready to promote when its GitHub release has been published for
at least PROMOTE_AFTER_HOURS, no open issue labelled RELEASE_BLOCKER names it
(or the stable version it would become), its stable version doesn't exist
yet and sorts above every stable release, and its tag carries the stable
statement. Only the open issues count as reports of a failed update or
rollback: installs send nothing back to CoderHelm about their updates.

The version (the stable number, X.Y.Z): the highest of the last stable
release and the last beta's X.Y.Z on SHA's history, bumped:
  minor  when a pull request merged since the last release of either kind is
         a feature: the "enhancement" or "feature" label, or a conventional
         "feat:" / "feat(scope):" title;
  patch  otherwise;
and never below installer/package.json's version at SHA, which is how a
maintainer asks for a minor or major release in a reviewed pull request (the
first self-host release, v0.7.0, comes from it). So every beta has its own
X.Y.Z: a beta that never becomes stable is skipped, and a promoted one always
sorts below every later beta, as its code is older.

Only tags on main count: a v* tag anywhere else is never a release (installs
refuse it), so it neither sets the base nor blocks the plan; the version
skips past its name instead. The tag it names never exists yet: a tag is
never re-cut or moved.

Outputs (GITHUB_OUTPUT): releases (a JSON list of {kind, channel, tag,
commit, previous, stable_tag, beta}), count, summary. Notes for each release
go to DIR/<tag>.md. Only the standard library is used; git and gh must be on
PATH.
"""

import argparse
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timedelta, timezone

STABLE = re.compile(r"^v(\d+)\.(\d+)\.(\d+)$")
BETA = re.compile(r"^v(\d+)\.(\d+)\.(\d+)-beta\.(\d{8})$")
FEATURE_TITLE = re.compile(r"^feat(\([^)]*\))?!?:", re.IGNORECASE)
FEATURE_LABELS = {"enhancement", "feature"}
PROMOTE_AFTER_HOURS = 24
RELEASE_BLOCKER = "release-blocker"
# The block in a beta's tag message holding the signed statement for its
# stable tag (the same format as the release attestation installs read).
STABLE_BEGIN = "-----BEGIN CODERHELM STABLE ATTESTATION-----"
STABLE_END = "-----END CODERHELM STABLE ATTESTATION-----"

# What a self-host release is built from (.github/workflows/release.yml, build
# job): the Rust Lambdas (services/), the MCP proxy and the updater (lambda/),
# the CDK app and the CloudFormation templates (infra/), the installer
# (installer/), install.sh, the dashboard pin and
# migrations (selfhost/), the dashboard pin check, the Rust build and its
# inputs digest and reuse check (scripts/rust-build.sh, scripts/rust-inputs.mjs,
# .github/scripts/rust-build-reuse.sh), and the release workflow.
SHIPPED = (
    "services/",
    "lambda/",
    "infra/",
    "installer/",
    "selfhost/",
    "scripts/check-dashboard-ref.mjs",
    "scripts/rust-build.sh",
    "scripts/rust-inputs.mjs",
    ".github/scripts/rust-build-reuse.sh",
    ".github/workflows/release.yml",
)


def parse(tag):
    """(major, minor, patch) of a stable release tag, or None."""
    m = STABLE.match(tag or "")
    return tuple(int(x) for x in m.groups()) if m else None


def parse_beta(tag):
    """((major, minor, patch), "YYYYMMDD") of a beta tag, or None."""
    m = BETA.match(tag or "")
    return (tuple(int(x) for x in m.groups()[:3]), m.group(4)) if m else None


def target(tag):
    """The stable X.Y.Z a release tag stands for (a beta's, without its suffix), or None."""
    b = parse_beta(tag)
    return parse(tag) or (b[0] if b else None)


def order(tag):
    """Sort key of a release tag: by X.Y.Z, a beta before its stable release, betas by date."""
    b = parse_beta(tag)
    return (b[0], 0, b[1]) if b else (parse(tag), 1, "")


def is_release(tag):
    return bool(parse(tag) or parse_beta(tag))


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


def plan(commit, tags, tags_at_commit, is_ancestor, changed_paths, merged_prs, floor=None, on_main=None,
         channel="stable", date=None):
    """The decision for `commit` on a channel: {skip, reason, tag, previous, prs, stable_tag}.

    tags            every tag name in the repository;
    tags_at_commit  the tag names pointing at `commit`;
    is_ancestor     f(tag) -> True when the tag's commit is in `commit`'s history;
    changed_paths   f(previous_tag_or_None) -> paths changed since it;
    merged_prs      f(previous_tag_or_None) -> pull requests merged since it;
    floor           installer/package.json's version as a tuple, or None;
    on_main         f(tag) -> True when the tag's commit is on main (default: is_ancestor);
    channel         "stable" (vX.Y.Z) or "beta" (vX.Y.Z-beta.<date>);
    date            the beta's date, YYYYMMDD (UTC).
    """
    on_main = on_main or is_ancestor
    beta = channel == "beta"
    released = [t for t in tags_at_commit if (is_release(t) if beta else parse(t))]
    if released:
        return {"skip": True, "reason": "%s is already released as %s" % (commit[:12], ", ".join(sorted(released)))}
    releases = sorted((t for t in tags if is_release(t) and on_main(t)), key=order)
    mine = [t for t in releases if is_ancestor(t)]
    if releases and (not mine or order(releases[-1]) > order(mine[-1])):
        return {"skip": True, "reason": "%s is newer than this commit's history: a later commit is already released" % releases[-1]}
    last_any = mine[-1] if mine else None
    stables = [t for t in mine if parse(t)]
    last_stable = stables[-1] if stables else None
    # A beta counts what changed since the last release of either kind; a
    # stable release since the last stable one (a beta of the same code
    # doesn't make it pointless).
    since = last_any if beta else last_stable
    shipped = ships(changed_paths(since))
    if not shipped:
        return {"skip": True, "reason": "nothing that ships in a self-host release changed since %s" % (since or "the start")}
    prs = merged_prs(since)
    bump = prs if since == last_any else merged_prs(last_any)
    bases = [target(t) for t in mine]
    version = next_version(max(bases) if bases else None, bump, floor)
    if beta and not re.fullmatch(r"\d{8}", str(date or "")):
        raise ValueError("a beta needs its date, YYYYMMDD")

    def name(v):
        return "%s-beta.%s" % (fmt(v), date) if beta else fmt(v)
    # A name taken by a tag that isn't a release (not on main) is skipped, never
    # reused; so is a stable number taken by any tag, as a beta's stable tag.
    while name(version) in tags or fmt(version) in tags:
        version = (version[0], version[1], version[2] + 1)
    return {"skip": False, "reason": "%d shipped file(s) changed since %s" % (len(shipped), since or "the start"),
            "tag": name(version), "previous": since, "prs": prs, "stable_tag": fmt(version)}


def plan_promote(betas, tags, stables, now, blockers, version=None, after_hours=PROMOTE_AFTER_HOURS):
    """The beta to promote to stable: {skip, reason, tag, beta, commit, error}.

    betas     [{tag, commit, published_at (datetime or None), draft, attested}],
              the beta tags on main;
    tags      every tag name in the repository;
    stables   the stable release tags on main;
    now       the time (UTC);
    blockers  [{number, text}]: the open issues labelled RELEASE_BLOCKER, or
              None when they couldn't be read (then nothing is promoted);
    version   a beta tag, or the stable vX.Y.Z it would become: that beta,
              promoted now whatever its age (an error when it can't be).
    """
    names = set(tags)
    top = max((parse(t) for t in stables if parse(t)), default=None)

    def blocked_by(b):
        words = {b["tag"], fmt(target(b["tag"]))}
        for i in blockers or []:
            found = set(re.findall(r"v\d+\.\d+\.\d+(?:-beta\.\d{8})?", i.get("text") or ""))
            if found & words:
                return i
        return None

    def why_not(b, manual):
        t = target(b["tag"])
        if fmt(t) in names:
            return "%s exists already" % fmt(t)
        if top and t <= top:
            return "%s is out, newer than %s" % (fmt(top), fmt(t))
        if not b.get("attested"):
            return "its tag carries no signed statement for %s (cut before betas were promoted)" % fmt(t)
        if b.get("draft") or not b.get("published_at"):
            return "its GitHub release isn't published"
        if blockers is None:
            return "the open %s issues couldn't be read" % RELEASE_BLOCKER
        i = blocked_by(b)
        if i:
            return "issue #%s (%s) reports a problem with it" % (i.get("number"), RELEASE_BLOCKER)
        age = now - b["published_at"]
        if not manual and age < timedelta(hours=after_hours):
            return "out for %dh of the %dh before promotion" % (age.total_seconds() // 3600, after_hours)
        return None

    ordered = sorted(betas, key=lambda b: order(b["tag"]), reverse=True)
    if version:
        pick = [b for b in ordered if version in (b["tag"], fmt(target(b["tag"])))]
        if not pick:
            return {"skip": True, "error": True, "reason": "%s is not a beta on main" % version}
        reason = why_not(pick[0], manual=True)
        if reason:
            return {"skip": True, "error": True, "reason": "%s can't be promoted: %s" % (pick[0]["tag"], reason)}
        ordered = pick
    passed = []
    for b in ordered:
        reason = why_not(b, manual=bool(version))
        if not reason:
            return {"skip": False, "reason": "%s promoted%s" % (b["tag"], ("; " + "; ".join(passed)) if passed else ""),
                    "tag": fmt(target(b["tag"])), "beta": b["tag"], "commit": b["commit"]}
        passed.append("%s: %s" % (b["tag"], reason))
        if fmt(target(b["tag"])) in names or (top and target(b["tag"]) <= top):
            break  # every older beta is behind the newest stable release too
    return {"skip": True, "reason": "no beta to promote" + ((": " + "; ".join(passed)) if passed else " (none on main)")}


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


def _main_ref(fallback):
    ok = subprocess.run(["git", "rev-parse", "-q", "--verify", "refs/remotes/origin/main"], capture_output=True).returncode == 0
    return "refs/remotes/origin/main" if ok else fallback


def _is_ancestor(tag, of):
    return subprocess.run(["git", "merge-base", "--is-ancestor", tag + "^{commit}", of], capture_output=True).returncode == 0


def _prs_between(repo, previous, commit):
    """The pull requests whose merge commit is in previous..commit of main."""
    rng = "%s..%s" % (previous, commit) if previous else commit
    shas = set(_run("git", "log", "--first-parent", "--format=%H", rng).split())
    since = _run("git", "log", "-1", "--format=%cs", previous + "^{commit}").strip() if previous else None
    found = [p for p in _merged_prs(repo, since) if p["commit"] in shas]
    return sorted(found, key=lambda p: p["number"])


def git_plan(repo, commit, channel="stable", date=None):
    tags = [t for t in _run("git", "tag", "-l", "v*").split() if t]
    at = [t for t in _run("git", "tag", "--points-at", commit, "-l", "v*").split() if t]
    main = _main_ref(commit)

    def changed(previous):
        if not previous:
            return _run("git", "ls-tree", "-r", "--name-only", commit).split("\n")
        return [p for p in _run("git", "diff", "--name-only", previous, commit).split("\n") if p]

    floor = None
    try:
        floor = parse("v" + json.loads(_run("git", "show", "%s:installer/package.json" % commit)).get("version", ""))
    except (subprocess.CalledProcessError, ValueError):
        pass
    return plan(commit, tags, at, lambda t: _is_ancestor(t, commit), changed, lambda p: _prs_between(repo, p, commit),
                floor, on_main=lambda t: _is_ancestor(t, main), channel=channel, date=date)


def _when(s):
    try:
        return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc) if s else None
    except ValueError:
        return None


def _blockers(repo):
    try:
        out = _run("gh", "issue", "list", "-R", repo, "--label", RELEASE_BLOCKER, "--state", "open", "--limit", "200",
                   "--json", "number,title,body")
    except subprocess.CalledProcessError:
        return None
    return [{"number": i["number"], "text": "%s\n%s" % (i.get("title") or "", i.get("body") or "")} for i in json.loads(out or "[]")]


def _release(repo, tag):
    try:
        return json.loads(_run("gh", "api", "repos/%s/releases/tags/%s" % (repo, tag)))
    except (subprocess.CalledProcessError, ValueError):
        return None


def git_promote(repo, version=None, now=None):
    tags = [t for t in _run("git", "tag", "-l", "v*").split() if t]
    main = _main_ref("HEAD")
    betas = []
    for t in tags:
        if not parse_beta(t) or not _is_ancestor(t, main):
            continue
        commit = _run("git", "rev-parse", t + "^{commit}").strip()
        text = _run("git", "cat-file", "-p", "refs/tags/" + t) if _run("git", "cat-file", "-t", "refs/tags/" + t).strip() == "tag" else ""
        r = _release(repo, t) or {}
        betas.append({"tag": t, "commit": commit, "published_at": _when(r.get("published_at")), "draft": bool(r.get("draft", True)),
                      "attested": STABLE_BEGIN in text.split("\n") and STABLE_END in text.split("\n")})
    stable_on_main = [t for t in tags if parse(t) and _is_ancestor(t, main)]
    r = plan_promote(betas, tags, stable_on_main, now or datetime.now(timezone.utc), _blockers(repo), version)
    if not r["skip"]:
        # The notes: everything since the stable release before it on that commit's history.
        prior = sorted((t for t in stable_on_main if _is_ancestor(t, r["commit"])), key=parse)
        r["previous"] = prior[-1] if prior else None
        r["prs"] = _prs_between(repo, r["previous"], r["commit"])
    return r


def main(argv):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("plan")
    p.add_argument("--repo", required=True)
    p.add_argument("--mode", choices=("stable", "beta", "promote"), default="stable")
    p.add_argument("--commit", help="the deployed commit (stable, beta)")
    p.add_argument("--version", help="promote: a beta tag, or the stable version it would become")
    p.add_argument("--notes-dir", required=True)
    p.add_argument("--github-output")
    a = ap.parse_args(argv)
    if a.commit and not re.fullmatch(r"[0-9a-f]{40}", a.commit):
        raise SystemExit("--commit must be a full commit SHA")
    if a.mode in ("stable", "beta") and not a.commit:
        raise SystemExit("--commit is needed for %s" % a.mode)
    if not re.fullmatch(r"[A-Za-z0-9-]+/[A-Za-z0-9._-]+", a.repo):
        raise SystemExit("--repo must be OWNER/NAME")
    if a.version and not (parse(a.version) or parse_beta(a.version)):
        raise SystemExit("--version must be vX.Y.Z or vX.Y.Z-beta.YYYYMMDD")
    os.makedirs(a.notes_dir, exist_ok=True)
    now = datetime.now(timezone.utc)
    out, lines, failed = [], [], False

    def keep(kind, channel, r, tag, commit):
        with open(os.path.join(a.notes_dir, tag + ".md"), "w", encoding="utf-8") as f:
            f.write(notes(a.repo, tag, r.get("previous"), r.get("prs") or []))
        out.append({"kind": kind, "channel": channel, "tag": tag, "commit": commit, "previous": r.get("previous") or "",
                    "stable_tag": r.get("stable_tag") if kind == "beta" else "", "beta": r.get("beta") or ""})

    if a.mode == "promote":
        r = git_promote(a.repo, a.version, now)
        lines.append("promote: " + r["reason"])
        failed = failed or bool(r.get("error"))
        if not r["skip"]:
            assert STABLE.match(r["tag"]) and BETA.match(r["beta"])
            keep("promote", "stable", r, r["tag"], r["commit"])
    else:
        channel = a.mode
        r = git_plan(a.repo, a.commit, channel, now.strftime("%Y%m%d"))
        lines.append("%s: %s" % (channel, r["reason"]))
        if not r["skip"]:
            assert (STABLE if channel == "stable" else BETA).match(r["tag"])
            keep(channel, channel, r, r["tag"], a.commit)
    summary = "; ".join(lines)
    print(json.dumps({"releases": out, "summary": summary}, indent=1))
    if a.github_output:
        with open(a.github_output, "a", encoding="utf-8") as f:
            f.write("releases=%s\n" % json.dumps(out, separators=(",", ":")))
            f.write("count=%d\n" % len(out))
            f.write("summary=%s\n" % " ".join(summary.split()))
    if failed:
        print("::error::" + summary, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
