# Contributing

PR text (title, description and commit messages) describes the behaviour and the code change only, never details from a customer install; the Public text guard check enforces it.

## Merging

A pull request's CI tests its head against `main` as it was when CI ran. Two pull requests can each pass on their own and still break `main` together, for example when one adds a required field to a shared struct and the other's new code builds that struct without it. So:

- **Merge through the merge queue.** CI runs on `merge_group` (`.github/workflows/ci.yml`): with the queue turned on for `main` (Settings, Rules, a ruleset on `main` with **Require merge queue**, and **Rust (workspace)** and the other CI checks as required checks), **Merge when ready** adds the pull request to the queue, GitHub tests it on top of `main` and of every pull request ahead of it, and it lands only when that combined commit passes. Squash is the queue's merge method.
- **Never merge a pull request that is behind `main` on its own green checks.** Outside the queue, press **Update branch** first and wait for CI on the new head. `gh pr merge --admin` skips both the queue and the "branch must be up to date" rule, so check that the branch is current before using it (`gh pr view <number> --json mergeStateStatus` must not say `BEHIND`, and `git merge-base --is-ancestor origin/main HEAD` must succeed on the branch).
- **`main` is tested after every merge.** CI also runs on every push to `main`, on the merged result. When that run is red, the hosted deploy refuses the commit and the self-host releases skip it (`.github/scripts/main_ci.py`): fix `main` first, and the fix ships once CI passes on it.

CoderHelm's own merges (the armed auto-merge on any repository it merges in) do the same on their own: when the base has moved past a pull request's head, CoderHelm merges the base into the branch, waits for CI on that commit, and merges only when it is green (`docs/incidents/mz_merged_result_passes_before_merge.md`).
