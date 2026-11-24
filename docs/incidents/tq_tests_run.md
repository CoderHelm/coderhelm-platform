# tq. `tq_tests_run`

**What happened:** Across a batch of recent runs, many had new or changed tests to run with and without the change. Not one of them ran both ways:

- **Several runs: the install failed in the sandbox.** All of them were in one pnpm workspace. The test files sat in a workspace member (`apps/web`) whose lockfile is the workspace root's, and the sandbox installed the member on its own: `cd 'apps/web' && pnpm install --ignore-workspace --frozen-lockfile`. That folder has no lockfile, so pnpm stopped with `ERR_PNPM_NO_LOCKFILE` before any test ran, on every run. The run's CI install (cached per manifest blob) was never used for these runs, and the failure didn't say why. One of the runs also listed a Playwright spec as a unit test.
- **Several runs: no step had the time.** The check asked once for a step of its own. The fresh step ran every other check first (the proof, the link checks, the new-feature rules, the live probes, the scope ledger), so it reached the sandbox runs with less than their 760 seconds left, and its one request was already spent. One run resumed for the runs and only reached them minutes later.
- **A few runs: CI's test command wasn't read.** A data pipeline's CI runs a `python "$t"` loop over its tests. Its fixture JSON files were also listed as tests, and one file without a command stopped every file from running.
- **A few runs: no test to run.** One change was only tests; the other changed no test file.

**The guarantee:**
- **Install the way CI does.** The sandbox runs use CI's job for the changed files: its install lines in order, its Node and pnpm versions, through the cached environment. Without one, a workspace member installs from the workspace root, falling back to the member and what it depends on. A failed install says why, with its error lines.
- **Unit tests only.** An end-to-end spec and test data (fixtures, snapshots) are never run as unit tests.
- **A budget of their own.** When a step is short, the runs are owed to a fresh step. That step runs them first, before any other check, and records the result for the head. A run asks for such a step up to `MAX_PROOF_STEPS` times; only past that are the tests recorded as not run.
- **Read the test command.** Each file runs with CI's test step narrowed to it. One function narrows pytest, unittest, Vitest, Jest, `go test` and `cargo test`, and package scripts and make targets are read through to their runner. When CI's step can't be narrowed, the file's own package's test script runs it. A file with no command doesn't stop the others.
- **Counted.** Whether the tests ran, and why not, is recorded on the run's checks (`tests_ran`), written to its progress notes, and shown in the pull request.

**Where it's enforced:**
- `common::e2e::test_package::workspace_member` and `install_cmd`;
- `common::test_env` (`narrow`, `resolve_scripts`, `package_test_command`, `INSTALL_FAILED`);
- `habits_check::bench_plan`, `install_failure`, `proof_time` and `run_owed`;
- the ledger's `proof_runs` and `proof_owed`;
- `habits_check::TestsRan`.
