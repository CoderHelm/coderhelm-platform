# vw. `vw_runs_share_the_hour`

**What happened:** Three failures, all in how a run accounts for itself.

- **Worked time added up overlaps.** Two messages of one run can overlap, such as a CI resume next to a review event, or a wait for GitHub sent on while a step is running. When the second message started, it found the first one's "working" marker and charged it as a killed step. Then both messages' times were added in full. A run could reach the worked-time ceiling long before it had worked that long, and runs showed more worked time than they had existed.
- **A burst of runs hit GitHub's hourly budget together.** When many tickets started close together, nothing checked new runs against the installation's shared budget. Every run spent requests until only the people's share was left, and then they all waited for the reset at once. Run pages said "Waiting for GitHub's hourly limit" again and again.
- **A server error while building a revert was treated as final.** A merge's deploy failed and CoderHelm started the revert pull request. GitHub answered one of the calls with a 5xx, the revert was reported as impossible, and nothing tried again.

**The guarantee:**

- A run's worked time is the union of its invocations, not their sum. A step counts as killed only when its marker is older than any step can run. A killed step is charged once for all of its time, and the kill is recorded on the run (`ceil_killed`). A marker from an invocation that is still running is never treated as a kill.
- A run starts or resumes its next GitHub-heavy step, a ticket step or a CI round, only when that step's expected spend fits the installation's live budget. That budget is GitHub's own `remaining` and `reset` minus the people's share, after the steps already running and the runs ahead in line. The expected spend is learned from the steps that already ran. If the step doesn't fit, it waits its turn in arrival order. Its run page says when it will start, at most once an hour.
- Every GitHub call on the revert path is retried while the worker's shared classifier (`common::run_recovery::classify`) says the error will pass, such as a 5xx, a 429, a secondary limit or a dropped connection. If the error persists, the deploy watch looks again later and builds the revert then. The write calls are safe to repeat. A branch is reset only when it has no open pull request. If GitHub opened a pull request before its answer failed, the retry finds that pull request before asking for another.

**Where:**

- `worker/src/passes/run_ceiling.rs`: `Clock`, `mark_entered`, `mark_left` and `update_clock`.
- `common/src/github_budget.rs`: `admission`.
- `worker/src/passes/github_admission.rs`, called from `worker/src/main.rs`. Step costs are learned in `clients::github_budget::flush`.
- `worker/src/passes/deploy_revert.rs`: `step`, `transient` and `Outcome::Retry`.
- `worker/src/passes/health_check.rs`: `revert_looks_again`.

**Test:** `worker/src/passes/incidents/vw_runs_share_the_hour.rs`

- Two overlapping invocations count their shared time once, in either order of ending, where the old code counted both and a killed-step charge.
- A busy run's worked time stays within its lifetime, where the old sum went past it.
- A burst of runs on one installation goes through the real per-request decision. With admission, no request meets the limit, every run finishes, and each run's page notes the wait at most once per hour. Without admission, the same burst meets the limit.
- A server error on each call of the revert path is retried, and the revert opens.
