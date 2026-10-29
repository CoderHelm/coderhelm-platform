# Incident regression suite

Every run failure that reached a person is a case in
`services/worker/src/passes/incidents/`, so it can't come back without a
failing test. CI runs the suite as its own job, **Incident regressions**
(`cargo test -p worker incidents::`), on every pull request.

Each case replays the failure on one invented monorepo:

- `apps/studio`: the CMS studio, its schema, and a `typegen` script. The
  script writes `apps/studio/schema.json`, and writes the site's types
  through a `../web/...` path.
- `apps/web`: the site.
- `e2e/`: end-to-end specs, with a map of which spec tests which page.

The case runs the pipeline's own decision code. Only the edges are faked:

- GitHub: branches, compares and pull requests, in the shapes GitHub returns.
- The model: recorded judge replies, read by the real parser.
- The sandbox: a real tar.gz archive and a CodeBuild-shaped log, read by the
  real `codegen_outcome`.
- DynamoDB: run records as the table holds them.

Each case checks three things: the run's final status, the pull request and
notes the run leaves, and that the old failure message never appears. When a
decision is made inside the pipeline itself, the case also checks that the
pipeline still calls it there.

## Cases

| Case | What happened | The guarantee |
|---|---|---|
| a. `a_whole_branch` | The plan check said 5 of 7 tasks were done. The last step wrote nothing, and the run ended "I ran but produced no file changes". | A step that writes nothing is judged on the whole branch. Work on the branch goes on to the checks and the pull request. With nothing on the branch, the run names the planned tasks that weren't built and never asks generic questions. |
| b. `b_open_pr_retry` | A retry started over on a branch whose earlier run was still fixing CI. | A retry continues the earlier run's open pull request and opens no other. While the earlier run still works on the branch, the retry stops before writing anything and says why. That covers a run that is running, waiting on CI, holding a writer slot, or has a round under way or parked. |
| c. `c_codegen_egress` | The sandbox's network allowlist refused the CMS host. The run blamed the change: "its generated files were not regenerated". | Builds that run a generator may reach the hosts it needs. A host that is still refused is a tooling failure that names the host. |
| d. `d_echoed_markers` | The echoed buildspec holds the upload markers, so every build read as "could not be uploaded". | A marker counts only on a line of its own. The exit code is the last one printed. A failed generator's reason quotes what the generator printed, not the echoed buildspec. |
| e. `e_large_generated_output` | A types file over 5 MB, written through `../`, was dropped silently. The run said "regenerated 1 file(s)" and opened no pull request. | Every changed file is committed, or named as a tooling failure. Paths have one spelling. A generated task its generator settled counts as done in the plan check and the proof check. |
| f. `f_steps` | The generator check ran out of time and the run failed "not enough time left". | A gate that lacks time continues in a fresh step at that gate, as a new queue message. A run that keeps making progress continues past 20 steps. Two steps in a row without progress stop it, and it says where it was stuck. |
| g. `g_large_plan` | A 40-file ticket was capped and narrowed. | A plan holds every file the ticket needs. Nothing caps or narrows it, and every later round is told every task. |
| h. `h_pr_updates` | A PR updated over several runs went stale. Its description was out of date, a ticket comment was posted twice, a question was asked on every update, an e2e test skipped itself when content was missing, and a note said "could not look up" content that was readable. | Each one is caught by its own check. The description is rewritten when a round lands. A notice is posted once per ticket, and so is a question. A spec that skips on missing content fails the checklist. A note that didn't look, when it could have, goes back to the planner. |
| i. `i_finished_work_pr` | A run with one planned task left ended with no pull request. Later the run opened a draft but left the work to a person. | Finished work always ends with a pull request. Planned work that is still open puts a draft PR up with a "Not finished" section. CoderHelm then keeps working those tasks in rounds on the PR and marks it ready when the section is empty. It stops only after two rounds in a row without progress. It then names the tasks and why, and sets the run to `needs_input`, never `completed`. |

## Adding a case

When a fix lands for a new failure:

1. Add `services/worker/src/passes/incidents/<letter>_<name>.rs`. Start it with
   what happened and the guarantee, in plain words, with no customer names.
2. Build the scenario on the fixture monorepo (`fixture.rs`). Add any new edge
   to the fixture as a fake, never as a mock of the pipeline's own code.
3. Check the final status, what the run leaves, and that the old message never
   appears (`Ending::never_says`).
4. Add a row to the table above.
