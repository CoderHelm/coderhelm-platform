"""Public text guard: a pull request's words never carry a customer install's details.

Scans the PR title, the PR body, every commit message in the PR, and the
lines the PR adds under docs/, and fails (exit 1) on:

- a name or hostname from GUARD_DENYLIST (one regex per line, matched
  ignoring case; blank lines and lines starting with # are skipped);
- an AWS account id in an ARN, an ECR host, or after "account";
- a TEAM# id;
- a 26-character ULID (a run id);
- a Jira-style key, apart from the allowlist below;
- an email address, apart from example and no-reply domains.

It prints where (the source and the line number) and which rule, never the
matched text, and masks every denylist entry in the log first.

    python3 .github/scripts/text_guard.py            # in Actions, on pull_request
    python3 .github/scripts/text_guard.py --text F    # one file, for a local check
"""

import json
import os
import re
import subprocess
import sys

# AWS's documentation placeholders.
PLACEHOLDER_ACCOUNTS = {"123456789012", "111122223333", "000000000000", "012345678901", "999999999999"}

# Standards and versions written like a Jira key, and the synthetic keys the
# API reference uses in its examples.
JIRA_PREFIX_ALLOW = {
    "AES", "AVX", "CVE", "CWE", "ECMA", "GHSA", "HTTP", "IEEE", "ISO", "PEP",
    "RFC", "RSA", "SHA", "TLS", "UTF",
}
JIRA_KEY_ALLOW = {"ENG-142", "WEB-42", "PROJ-123", "ABC-123", "KEY-123"}

# The synthetic ids the API reference and the webhook docs use in examples.
# A new example id goes here, never a real one.
TEAM_ALLOW = {"TEAM#7c1f0e2a", "TEAM#4817263"}
ULID_ALLOW = {
    "01JB7Z2R4T6V8X0Z2B4D6F8H0K", "01JB8V0K3M5P7R9T1W3Y5A7C9E", "01JB8W2Q7N4D6R8S0T2V4X6Z8A",
    "01JB8X6F4M2C9QZ3T7V5W1K0HA", "01JB8Z3N5Q7S9V1X3Z5B7D9F1H", "01JBENDPT4A7B0C3D6E9F2G5H8",
    "01JBENDPT4K2M8N3P5Q7R9S1T3", "01JBEVNT8M1N4P7Q0R3S6T9V2W",
}

EMAIL_DOMAIN_ALLOW = re.compile(
    r"(^|\.)(example\.(com|org|net)|[a-z0-9-]+\.example|noreply\.github\.com|github\.com)$"
)

ACCOUNT_ID = r"(?<!\d)(\d{4}-?\d{4}-?\d{4})(?!\d)"
RE_ARN = re.compile(r"arn:aws[a-z-]*:[a-z0-9-]*:[a-z0-9-]*:(\d{12}):", re.IGNORECASE)
RE_ECR = re.compile(r"(?<!\d)(\d{12})\.dkr\.ecr\.", re.IGNORECASE)
RE_ACCOUNT = re.compile(r"(?:account|acct)(?:[ _-]?(?:id|ids|number|no))?[^\n\d]{0,30}?" + ACCOUNT_ID, re.IGNORECASE)
RE_TEAM = re.compile(r"TEAM#[0-9A-Za-z][0-9A-Za-z_-]*")
RE_ULID = re.compile(r"(?<![0-9A-Za-z])[0-7][0-9A-HJKMNP-TV-Z]{25}(?![0-9A-Za-z])", re.IGNORECASE)
RE_JIRA = re.compile(r"(?<![A-Za-z0-9-])([A-Z]{2,6})-(\d{2,6})(?![0-9])")
RE_EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@((?:[A-Za-z0-9-]+\.)+[A-Za-z]{2,})(?![A-Za-z0-9-])")


def load_denylist(raw):
    """(patterns, problems): compiled entries, and the entries that don't compile, by line."""
    patterns, problems = [], []
    for n, line in enumerate((raw or "").splitlines(), 1):
        entry = line.strip()
        if not entry or entry.startswith("#"):
            continue
        try:
            patterns.append(re.compile(entry, re.IGNORECASE))
        except re.error:
            problems.append(f"GUARD_DENYLIST line {n} is not a valid regex")
    return patterns, problems


def mask_commands(raw):
    """The ::add-mask:: lines that hide every denylist entry from the log."""
    return [f"::add-mask::{line.strip()}" for line in (raw or "").splitlines()
            if line.strip() and not line.strip().startswith("#")]


def _account(line):
    for rx in (RE_ARN, RE_ECR, RE_ACCOUNT):
        for m in rx.finditer(line):
            if m.group(1).replace("-", "") not in PLACEHOLDER_ACCOUNTS:
                return True
    return False


def _team(line):
    return any(m.group(0) not in TEAM_ALLOW for m in RE_TEAM.finditer(line))


def _ulid(line):
    return any(re.search(r"[A-Za-z]", m.group(0)) and m.group(0).upper() not in ULID_ALLOW
               for m in RE_ULID.finditer(line))


def _jira(line):
    for m in RE_JIRA.finditer(line):
        if m.group(1) not in JIRA_PREFIX_ALLOW and m.group(0) not in JIRA_KEY_ALLOW:
            return True
    return False


def _email(line):
    return any(not EMAIL_DOMAIN_ALLOW.search(m.group(1).lower()) for m in RE_EMAIL.finditer(line))


def rules_for(denylist):
    rules = [("denylisted name or host", lambda line: any(p.search(line) for p in denylist))]
    rules += [
        ("AWS account id", _account),
        ("TEAM# id", _team),
        ("ULID run id", _ulid),
        ("Jira-style key", _jira),
        ("email address", _email),
    ]
    return rules


def scan_lines(lines, rules):
    """[(line number, rule name)] for every (numbered) line that breaks a rule."""
    hits = []
    for n, line in lines:
        for name, test in rules:
            if test(line):
                hits.append((n, name))
    return hits


def numbered(text):
    return list(enumerate((text or "").splitlines(), 1))


def added_lines(diff):
    """{path: [(new line number, text)]} for the + lines of a unified diff."""
    out, path, n = {}, None, 0
    for line in diff.splitlines():
        if line.startswith("+++ "):
            target = line[4:]
            path = target[2:] if target.startswith("b/") else None
            continue
        if line.startswith("@@"):
            m = re.match(r"@@ -\d+(?:,\d+)? \+(\d+)(?:,\d+)? @@", line)
            n = int(m.group(1)) if m else 0
            continue
        if path is None or line.startswith("--- "):
            continue
        if line.startswith("+"):
            out.setdefault(path, []).append((n, line[1:]))
            n += 1
        elif line.startswith(" "):
            n += 1
    return out


def _git(*args):
    return subprocess.run(["git", *args], check=True, capture_output=True, text=True).stdout


def sources_from_pr(event):
    """[(label, file or None, numbered lines)] for the PR's title, body, commits and docs/ additions."""
    pr = event["pull_request"]
    base, head = pr["base"]["sha"], pr["head"]["sha"]
    sources = [("PR title", None, numbered(pr.get("title"))), ("PR body", None, numbered(pr.get("body")))]
    for sha in _git("rev-list", "--reverse", f"{base}..{head}").split():
        sources.append((f"commit {sha[:12]} message", None, numbered(_git("log", "-1", "--format=%B", sha))))
    diff = _git("diff", "-U0", "--no-color", "--no-ext-diff", f"{base}...{head}", "--", "docs/")
    for path, lines in sorted(added_lines(diff).items()):
        sources.append((f"{path} (added line)", path, lines))
    return sources


def report(sources, rules):
    """The failure lines, one per hit, naming the place and the rule only."""
    out = []
    for label, path, lines in sources:
        for n, rule in scan_lines(lines, rules):
            where = f"file={path},line={n}," if path else ""
            out.append(f"::error {where}title=Public text guard::{label} line {n}: {rule}")
    return out


def main(argv):
    raw = os.environ.get("GUARD_DENYLIST", "")
    for cmd in mask_commands(raw):
        print(cmd)
    denylist, problems = load_denylist(raw)
    for p in problems:
        print(f"::error title=Public text guard::{p}")
    if not raw.strip():
        print("::warning title=Public text guard::GUARD_DENYLIST is not available here; names and hosts are not checked")
    rules = rules_for(denylist)

    if len(argv) == 3 and argv[1] == "--text":
        with open(argv[2], encoding="utf-8") as f:
            sources = [(argv[2], None, numbered(f.read()))]
    else:
        with open(os.environ["GITHUB_EVENT_PATH"], encoding="utf-8") as f:
            sources = sources_from_pr(json.load(f))

    failures = report(sources, rules)
    for line in failures:
        print(line)
    checked = sum(len(lines) for _, _, lines in sources)
    if failures or problems:
        print(f"{len(failures)} line(s) carry details from an install; PR text describes the behaviour and the code change only")
        return 1
    print(f"{checked} lines in {len(sources)} source(s): no customer details")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
