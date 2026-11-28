"""The permission changes of a release, for its release notes.

Every role, policy and resource policy of the native templates (the parent,
access.json and each part) is compared with the previous release's, both
resolved the way the install's updater compares them before an update
(lambda/updater/handler.py, resource_on and resource_difference): policy
documents as IAM means them, order ignored, so only a real difference in
actions, resources or conditions is listed. Here no install's values are
known, so each parameter stays its own name on both sides and both branches
of a condition are kept.

  iam_diff.py previous TAG < releases.json
      The release a TAG install moves from: the newest published release
      below TAG (any for a beta, a stable one for a stable TAG). releases.json
      is `gh release list --json tagName,isPrerelease,isDraft`.
  iam_diff.py diff OLD_DIR NEW_DIR [FROM_TAG]
      Markdown for the release notes. Each DIR holds coderhelm.json,
      access.json and stacks/*.json, as the release's native files do.
"""

import importlib.util
import json
import pathlib
import sys

HANDLER = pathlib.Path(__file__).resolve().parents[2] / "lambda" / "updater" / "handler.py"
_spec = importlib.util.spec_from_file_location("iam_diff_updater", HANDLER)
u = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(u)

PSEUDO = ("AWS::AccountId", "AWS::Region", "AWS::Partition", "AWS::URLSuffix", "AWS::StackName", "AWS::StackId",
          "AWS::NotificationARNs")


def previous(tag, releases):
    """The tag of the release an install of `tag` moves from, or None."""
    beta = bool(u.parse_version(tag) and u.parse_version(tag)[3])
    best = None
    for r in releases or []:
        t = r.get("tagName") or ""
        p = u.parse_version(t)
        if not p or r.get("isDraft") or t == tag:
            continue
        if not beta and (p[3] or r.get("isPrerelease")):
            continue
        if u.compare(t, tag) >= 0:
            continue
        if best is None or u.compare(t, best) > 0:
            best = t
    return best


def templates(root):
    root = pathlib.Path(root)
    out = {}
    for rel in ["coderhelm.json", "access.json"] + sorted(f"stacks/{p.name}" for p in (root / "stacks").glob("*.json")):
        f = root / rel
        if f.is_file():
            out[rel] = json.loads(f.read_text())
    return out


def side(t):
    values = {name: "${" + name + "}" for name in (t.get("Parameters") or {})}
    values.update({p: "${" + p + "}" for p in PSEUDO})
    return {"t": t, "values": values, "symbolic": True}


def is_permission(res):
    t = res.get("Type") or ""
    if t.startswith("AWS::IAM::") or t in u.POLICY_TYPES:
        return True
    return any(k in (res.get("Properties") or {}) for k in u.POLICY_PROPERTIES)


def diff(old_dir, new_dir):
    """[(file, resource, type, what changes)]."""
    old, new = templates(old_dir), templates(new_dir)
    out = []
    for rel in sorted(set(old) | set(new)):
        a, b = old.get(rel) or {}, new.get(rel) or {}
        ra, rb = a.get("Resources") or {}, b.get("Resources") or {}
        sa, sb = side(a), side(b)
        for lid in sorted(set(ra) | set(rb)):
            if not (is_permission(ra.get(lid) or {}) or is_permission(rb.get(lid) or {})):
                continue
            kind = (rb.get(lid) or ra.get(lid) or {}).get("Type") or ""
            if lid not in ra:
                out.append((rel, lid, kind, "added"))
                continue
            if lid not in rb:
                out.append((rel, lid, kind, "removed"))
                continue
            try:
                what = u.resource_difference(u.resource_on(sa, lid), u.resource_on(sb, lid))
            except u.Unknown as e:
                what = f"couldn't be compared ({e})"
            if what:
                out.append((rel, lid, kind, what))
        for name in sorted(set(a.get("Conditions") or {}) | set(b.get("Conditions") or {})):
            ca, cb = (a.get("Conditions") or {}).get(name), (b.get("Conditions") or {}).get(name)
            if json.dumps(ca, sort_keys=True) != json.dumps(cb, sort_keys=True):
                out.append((rel, name, "Condition", "added" if ca is None else "removed" if cb is None else "changes"))
    return out


def markdown(changes, from_tag):
    since = f"since {from_tag}" if from_tag else "since the previous release"
    if not changes:
        return (f"## Permission changes {since}\n\nNone: every role, policy and resource policy grants exactly what "
                f"{from_tag or 'the previous release'}'s does (compared as the updater compares them).\n")
    lines = [f"## Permission changes {since}", "",
             "An update with these waits for an admin's OK; nothing else about permissions changes.", ""]
    for rel, lid, kind, what in changes:
        lines.append(f"- `{rel}` {lid} ({kind}): {what}")
    return "\n".join(lines) + "\n"


def main(argv):
    if len(argv) >= 2 and argv[0] == "previous":
        found = previous(argv[1], json.load(sys.stdin))
        if found:
            print(found)
        return 0
    if len(argv) >= 3 and argv[0] == "diff":
        print(markdown(diff(argv[1], argv[2]), argv[3] if len(argv) > 3 else ""), end="")
        return 0
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
