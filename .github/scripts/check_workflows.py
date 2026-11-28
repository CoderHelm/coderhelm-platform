"""Supply-chain rules for every GitHub Actions workflow in this repository.

Fails (exit 1) when a workflow:

- uses an action by tag or branch instead of a full commit SHA, or without
  the version it stands for in a comment (`uses: owner/repo@<40 hex> # v1.2.3`).
  Local actions (`./...`) and `docker://image@sha256:...` are fine;
- runs on `pull_request_target` or `workflow_run` (both run with this
  repository's secrets on behalf of code or events a stranger controls);
- puts untrusted values (`github.event.*`, `github.head_ref`, `inputs.*`) straight into
  a `run:` script, where they become shell code; pass them through `env:`;
- has no top-level or per-job `permissions:` (the token's default can write);
- gives a job an `if:` with no status function (`success()`, `failure()`,
  `always()`, `cancelled()`) while a job it needs, directly or further up,
  has an `if:` of its own. GitHub then adds `success()`, which is false
  whenever that upstream job was skipped, so the job never runs on the paths
  where it was skipped. Write `!cancelled() && needs.x.result == 'success' && ...`
  to run after a skipped ancestor, or `success() && ...` to mean the skip.

    python3 .github/scripts/check_workflows.py [.github/workflows]
"""

import pathlib
import re
import sys

import yaml

SHA_PIN = re.compile(r"^[^@\s]+@[0-9a-f]{40}$")
DOCKER_PIN = re.compile(r"^docker://[^@\s]+@sha256:[0-9a-f]{64}$")
VERSION_COMMENT = re.compile(r"#\s*\S+")
UNTRUSTED = re.compile(r"\$\{\{[^}]*\b(github\.event\.|github\.head_ref|inputs\.)[^}]*\}\}")
FORBIDDEN_TRIGGERS = {"pull_request_target", "workflow_run"}
STATUS_FN = re.compile(r"\b(success|failure|always|cancelled)\(\)")


def _triggers(doc):
    # PyYAML reads the key `on` as True.
    on = doc.get("on", doc.get(True))
    if isinstance(on, str):
        return {on}
    if isinstance(on, list):
        return set(on)
    if isinstance(on, dict):
        return set(on)
    return set()


def check_text(name, text):
    """The problems in one workflow file, as 'file:line: message' strings."""
    problems = []
    lines = text.split("\n")
    for i, line in enumerate(lines, 1):
        m = re.match(r"^\s*(?:-\s+)?uses:\s*([^\s#]+)\s*(#.*)?$", line)
        if not m:
            continue
        ref, comment = m.group(1).strip("'\""), m.group(2) or ""
        if ref.startswith("./"):
            continue
        if ref.startswith("docker://"):
            if not DOCKER_PIN.match(ref):
                problems.append(f"{name}:{i}: {ref} must be pinned by digest (docker://image@sha256:...)")
            continue
        if not SHA_PIN.match(ref):
            problems.append(f"{name}:{i}: {ref} must be pinned by full commit SHA (owner/repo@<40 hex> # vX.Y.Z)")
        elif not VERSION_COMMENT.match(comment):
            problems.append(f"{name}:{i}: {ref} needs the version it stands for in a comment (# vX.Y.Z)")

    try:
        doc = yaml.safe_load(text) or {}
    except yaml.YAMLError as e:
        return problems + [f"{name}: not valid YAML: {e}"]
    for t in sorted(_triggers(doc) & FORBIDDEN_TRIGGERS):
        problems.append(f"{name}: runs on {t}, which hands this repository's secrets to code or events others control")

    jobs = doc.get("jobs") or {}
    if "permissions" not in doc:
        for job_id, job in jobs.items():
            if isinstance(job, dict) and "permissions" not in job:
                problems.append(f"{name}: job {job_id} has no permissions (set them at the top or on the job)")

    problems += _skipped_upstream(name, jobs)

    for job_id, job in jobs.items():
        for step in (job or {}).get("steps") or []:
            run = step.get("run") if isinstance(step, dict) else None
            if isinstance(run, str):
                for m in UNTRUSTED.finditer(run):
                    problems.append(f"{name}: job {job_id}: {m.group(0)} is used inside run:; pass it through env: instead")
    return problems


def _needs(job):
    n = (job or {}).get("needs") or []
    return [n] if isinstance(n, str) else list(n)


def _skipped_upstream(name, jobs):
    """Jobs whose implicit success() is false whenever a conditional ancestor is skipped."""
    problems = []
    for job_id, job in jobs.items():
        if not isinstance(job, dict) or "if" not in job:
            continue
        cond = str(job["if"])
        if STATUS_FN.search(cond):
            continue
        seen, todo, conditional = set(), _needs(job), []
        while todo:
            up = todo.pop()
            if up in seen or up not in jobs:
                continue
            seen.add(up)
            if isinstance(jobs[up], dict) and "if" in jobs[up]:
                conditional.append(up)
            todo += _needs(jobs[up])
        if conditional:
            problems.append(
                f"{name}: job {job_id}: its if: has no status function, so it is skipped whenever "
                f"{', '.join(sorted(conditional))} is skipped; start it with !cancelled() && (and check needs.*.result) "
                f"or success() && if that skip is meant"
            )
    return problems


def check_dir(path):
    problems = []
    files = sorted(pathlib.Path(path).glob("*.y*ml"))
    for f in files:
        problems += check_text(str(f), f.read_text())
    return files, problems


def main(argv):
    path = argv[1] if len(argv) > 1 else ".github/workflows"
    files, problems = check_dir(path)
    for p in problems:
        print(p)
    if problems:
        print(f"{len(problems)} problem(s) in {len(files)} workflow(s)")
        return 1
    print(f"{len(files)} workflows: every action pinned by SHA, no untrusted triggers or inputs in scripts, no job lost to a skipped ancestor")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
