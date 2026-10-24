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
            are behind (one "site<TAB>product" line each), so the workflow can
            upgrade them. The output holds site addresses: the workflow writes
            it to a file and never prints it, because the run logs are public.

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


def pending_installs(installs, environment="production"):
    """(site, product) for each installation in `environment` that is behind."""
    out = []
    for inst in installs or []:
        if str(inst.get("environment", "")).lower() != environment:
            continue
        state = f"{inst.get('majorVersion', '')} {inst.get('status', '')}"
        if not _BEHIND.search(state):
            continue
        site, product = inst.get("site"), str(inst.get("product", "")).lower()
        if site and product:
            out.append((site, product))
    return out


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

    with open(args.installs, encoding="utf-8") as f:
        installs = json.load(f)
    for site, product in pending_installs(installs, args.environment):
        print(f"{site}\t{product}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
