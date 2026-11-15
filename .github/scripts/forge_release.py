#!/usr/bin/env python3
"""Helpers for the Jira app (Forge) release workflow.

Commands:

  diff      Compare the manifest that was last released with the one about to
            be released, and say whether the new version needs a site admin to
            accept it. Forge makes a "major" version (one an admin must accept
            in Jira) when an app asks for more access: a scope change, new
            egress, a new dynamic web trigger, new remotes or providers, or
            licensing turned on. Everything else is a "minor" version that
            Forge rolls out to every site by itself.

  summary   Print the run-summary text for a result saved by `diff --json`.

  pending   Read `forge install list --json` and print the installations that
            are behind (one "site<TAB>product<TAB>major" line each), so the
            workflow can upgrade them. The output holds site addresses: the
            workflow writes it to a file and never prints it, because the run
            logs are public.

  latest    Read `forge version list --json` and the manifest, and print the
            pointer CoderHelm reads to know the newest production version
            (launch/jira-app.json): {"version", "appId", "commit", "publishedAt"}.

  behind    Read the installations still behind after the upgrade step (one
            "major<TAB>product" line each, no site addresses) and print the run
            summary's list and the warning, each with what that version lacks
            (the feature table CoderHelm itself shows,
            services/common/src/jira_app_features.json).

Only the standard library and PyYAML are used.
"""

import argparse
import json
import re
import sys

import yaml


# ---------------------------------------------------------------------------
# Manifest reading
# ---------------------------------------------------------------------------


def load_manifest(path):
    """Parse a manifest file. A missing or empty path means "no manifest"."""
    if not path:
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except FileNotFoundError:
        return {}


def _entry_key(entry):
    """One egress or content permission entry as a comparable string.

    Entries are plain strings, or objects such as
    {address: ..., category: ..., inScopeEUD: ...} or {remote: key}. The
    category and inScopeEUD are part of the key, so changing them counts as a
    new entry (Forge treats both as a permission change).
    """
    if isinstance(entry, dict):
        target = entry.get("address") or (
            f"remote:{entry['remote']}" if "remote" in entry else json.dumps(entry, sort_keys=True)
        )
        extras = []
        if entry.get("category"):
            extras.append(f"category={entry['category']}")
        if entry.get("inScopeEUD"):
            extras.append("inScopeEUD")
        return f"{target} ({', '.join(extras)})" if extras else str(target)
    return str(entry)


def _flatten(node, prefix):
    """Walk a nested permissions block into a set of "path: entry" strings."""
    out = set()
    if isinstance(node, dict):
        for k, v in node.items():
            out |= _flatten(v, f"{prefix}.{k}" if prefix else str(k))
    elif isinstance(node, list):
        for item in node:
            out.add(f"{prefix}: {_entry_key(item)}")
    elif node is not None:
        out.add(f"{prefix}: {node}")
    return out


def access_of(manifest):
    """Everything in a manifest that decides what the app may reach."""
    perms = manifest.get("permissions") or {}
    modules = manifest.get("modules") or {}
    app = manifest.get("app") or {}

    dynamic_webtriggers = set()
    for wt in modules.get("webtrigger") or []:
        # Web triggers are dynamic unless `response.type: static` is set.
        rtype = ((wt.get("response") or {}).get("type") or "dynamic").lower()
        if rtype == "dynamic":
            dynamic_webtriggers.add(wt.get("key", "?"))

    module_keys = set()
    for mtype, items in modules.items():
        for item in items or []:
            module_keys.add(f"{mtype}:{item.get('key', '?')}" if isinstance(item, dict) else f"{mtype}:?")

    return {
        "scopes": set(perms.get("scopes") or []),
        "egress": _flatten(perms.get("external") or {}, "external"),
        "content": _flatten(perms.get("content") or {}, "content"),
        "dynamic_webtriggers": dynamic_webtriggers,
        "remotes": {json.dumps(r, sort_keys=True) for r in manifest.get("remotes") or []},
        "providers": _flatten(manifest.get("providers") or {}, "providers"),
        "licensing": bool((app.get("licensing") or {}).get("enabled")),
        "modules": module_keys,
    }


# ---------------------------------------------------------------------------
# Diff
# ---------------------------------------------------------------------------


def diff(old, new):
    """Compare two parsed manifests.

    Returns a dict with what was added/removed and `needs_admin`: True when
    the new version is one a site admin must accept in Jira.
    """
    a, b = access_of(old), access_of(new)
    result = {"first_release": not old}
    for key in ("scopes", "egress", "content", "dynamic_webtriggers", "remotes", "providers", "modules"):
        result[f"{key}_added"] = sorted(b[key] - a[key])
        result[f"{key}_removed"] = sorted(a[key] - b[key])
    result["licensing_enabled"] = b["licensing"] and not a["licensing"]

    reasons = []
    if result["scopes_added"]:
        reasons.append("new scopes: " + ", ".join(result["scopes_added"]))
    if result["scopes_removed"]:
        # Forge makes a major version for a removed scope too.
        reasons.append("removed scopes: " + ", ".join(result["scopes_removed"]))
    if result["egress_added"]:
        reasons.append("new egress: " + "; ".join(result["egress_added"]))
    if result["content_added"]:
        reasons.append("new content permissions: " + "; ".join(result["content_added"]))
    if result["dynamic_webtriggers_added"]:
        reasons.append("new dynamic web triggers: " + ", ".join(result["dynamic_webtriggers_added"]))
    if result["remotes_added"] or result["remotes_removed"]:
        reasons.append("remotes changed")
    if result["providers_added"] or result["providers_removed"]:
        reasons.append("providers changed")
    if result["licensing_enabled"]:
        reasons.append("licensing turned on")

    # With no earlier manifest to compare with we cannot tell, so we say an
    # admin may be needed rather than promise that none is.
    if result["first_release"]:
        reasons.append("no earlier release to compare with")

    result["reasons"] = reasons
    result["needs_admin"] = bool(reasons)
    return result


def summary_markdown(result):
    """The run summary text. Holds manifest facts only, never site names."""
    lines = ["## Jira app permissions", ""]
    if result["needs_admin"]:
        lines += [
            "**A site admin must accept this update in Jira.** The new version asks for:",
            "",
            *[f"- {r}" for r in result["reasons"]],
            "",
            "Forge rolls out code-only changes by itself, but not new access. On each site,",
            "an admin opens **Jira settings (gear) → Apps → Manage apps**, finds **Coderhelm**",
            "and clicks **Update**. Until then the site keeps running the previous major version.",
            "The workflow also runs `forge install --upgrade` itself; that succeeds only on",
            "sites where the Forge token's account is a site admin.",
        ]
    else:
        lines.append("No new access. Every site gets this version automatically; no one needs to do anything.")
    if result["modules_added"] or result["modules_removed"]:
        lines += ["", "Modules changed (no admin step on their own):"]
        lines += [f"- added `{m}`" for m in result["modules_added"]]
        lines += [f"- removed `{m}`" for m in result["modules_removed"]]
    narrowed = result["egress_removed"] + result["content_removed"]
    if narrowed:
        lines += ["", "Access removed:"] + [f"- {n}" for n in narrowed]
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Installations
# ---------------------------------------------------------------------------

_BEHIND = re.compile(r"out-?of-?date|outdated", re.IGNORECASE)


def major_of(text):
    """The major version at the start of `forge install list`'s "5 (Out-of-date)"."""
    m = re.match(r"\s*v?(\d+)", str(text or ""))
    return int(m.group(1)) if m else None


def pending_installs(installs, environment="production"):
    """(site, product, major) for each installation in `environment` that is behind."""
    out = []
    for inst in installs or []:
        if str(inst.get("environment", "")).lower() != environment:
            continue
        state = f"{inst.get('majorVersion', '')} {inst.get('status', '')}"
        if not _BEHIND.search(state):
            continue
        site, product = inst.get("site"), str(inst.get("product", "")).lower()
        if site and product:
            major = major_of(inst.get("majorVersion"))
            out.append((site, product, "" if major is None else str(major)))
    return out


# ---------------------------------------------------------------------------
# Versions
# ---------------------------------------------------------------------------


def parse_version(text):
    """(major, minor, patch) from "6.10.0" or "v6.10"; None otherwise."""
    m = re.fullmatch(r"\s*v?(\d+)(?:\.(\d+))?(?:\.(\d+))?\s*", str(text or ""))
    return tuple(int(g or 0) for g in m.groups()) if m else None


def latest_version(versions):
    """The highest version in `forge version list --json` (one entry per major)."""
    found = [parse_version(v.get("version")) for v in versions or [] if isinstance(v, dict)]
    found = [v for v in found if v]
    return ".".join(map(str, max(found))) if found else None


def latest_pointer(versions, manifest, commit="", published_at=""):
    """The pointer CoderHelm reads; None when no version is known."""
    version = latest_version(versions)
    if not version:
        return None
    app_id = str(((manifest or {}).get("app") or {}).get("id") or "")
    return {"version": version, "appId": app_id, "commit": commit, "publishedAt": published_at}


def load_features(path):
    """[(since, feature)] from the feature table, oldest first."""
    with open(path, encoding="utf-8") as f:
        rows = json.load(f)
    out = [(parse_version(r.get("since")), str(r.get("feature", ""))) for r in rows]
    return sorted([r for r in out if r[0] and r[1]])


def missing_on_major(features, major):
    """What a site on major version `major` lacks (None: unknown, so all of it)."""
    return [name for since, name in features if major is None or since[0] > major]


def behind_report(lines, latest, features):
    """(summary markdown, warning text) for the installations still behind.

    `lines` are "major<TAB>product": sites are numbered, never named, because
    the run summary and log are public.
    """
    rows = []
    for line in lines:
        if not line.strip():
            continue
        parts = line.rstrip("\n").split("\t")
        major = int(parts[0]) if parts[0].strip().isdigit() else None
        product = parts[1].strip() if len(parts) > 1 else ""
        rows.append((major, product))
    if not rows:
        return "", ""
    latest_text = f"latest {latest}" if latest else "latest unknown"
    md = ["", "## Sites still behind", ""]
    md.append("Each one runs an older Coderhelm app until its Jira admin accepts the update:")
    md.append("Jira Settings, Apps, Manage apps, Coderhelm, Update. CoderHelm shows the same on the team's Integrations page.")
    md.append("")
    lacking = []
    for i, (major, product) in enumerate(rows, 1):
        version = f"major version {major}" if major is not None else "an unknown version"
        missing = missing_on_major(features, major)
        lacks = f" Missing: {', '.join(missing)}." if missing else ""
        md.append(f"- Site {i} ({product or 'jira'}): {version} ({latest_text}).{lacks}")
        lacking += [m for m in missing if m not in lacking]
    what = f" They lack: {', '.join(lacking)}." if lacking else ""
    warning = (
        f"{len(rows)} installation(s) still run an older Coderhelm app ({latest_text}).{what}"
        " A Jira admin on each site must accept the update: Jira Settings, Apps, Manage apps, Coderhelm, Update."
    )
    return "\n".join(md) + "\n", warning


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    d = sub.add_parser("diff", help="compare the last released manifest with the new one")
    d.add_argument("--old", default="", help="last released manifest (empty: none)")
    d.add_argument("--new", required=True, help="manifest about to be released")
    d.add_argument("--json", dest="json_out", help="write the full result as JSON here")
    d.add_argument("--summary", help="append the markdown summary here")

    sm = sub.add_parser("summary", help="print the markdown summary for a saved diff result")
    sm.add_argument("--json", dest="json_in", required=True, help="result written by `diff --json`")

    pe = sub.add_parser("pending", help="list installations that are behind")
    pe.add_argument("--installs", required=True, help="output of `forge install list --json`")
    pe.add_argument("--environment", default="production")

    la = sub.add_parser("latest", help="print the latest-version pointer")
    la.add_argument("--versions", required=True, help="output of `forge version list --json`")
    la.add_argument("--manifest", required=True)
    la.add_argument("--commit", default="")
    la.add_argument("--published-at", default="")

    be = sub.add_parser("behind", help="summary and warning for installations still behind")
    be.add_argument("--failed", required=True, help='"major<TAB>product" lines')
    be.add_argument("--latest", default="")
    be.add_argument("--features", required=True, help="the feature table (JSON)")
    be.add_argument("--summary", help="append the markdown here")

    args = p.parse_args(argv)

    if args.cmd == "diff":
        result = diff(load_manifest(args.old), load_manifest(args.new))
        if args.json_out:
            with open(args.json_out, "w", encoding="utf-8") as f:
                json.dump(result, f, indent=2)
        if args.summary:
            with open(args.summary, "a", encoding="utf-8") as f:
                f.write(summary_markdown(result))
        print("needs_admin=" + ("true" if result["needs_admin"] else "false"))
        for r in result["reasons"]:
            print("  - " + r)
        return 0

    if args.cmd == "summary":
        with open(args.json_in, encoding="utf-8") as f:
            print(summary_markdown(json.load(f)), end="")
        return 0

    if args.cmd == "latest":
        with open(args.versions, encoding="utf-8") as f:
            versions = json.load(f)
        pointer = latest_pointer(versions, load_manifest(args.manifest), args.commit, args.published_at)
        if not pointer:
            print("no production version found", file=sys.stderr)
            return 1
        print(json.dumps(pointer))
        return 0

    if args.cmd == "behind":
        with open(args.failed, encoding="utf-8") as f:
            lines = f.readlines()
        md, warning = behind_report(lines, args.latest, load_features(args.features))
        if args.summary and md:
            with open(args.summary, "a", encoding="utf-8") as f:
                f.write(md)
        print(warning)
        return 0

    with open(args.installs, encoding="utf-8") as f:
        installs = json.load(f)
    for site, product, major in pending_installs(installs, args.environment):
        print(f"{site}\t{product}\t{major}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
