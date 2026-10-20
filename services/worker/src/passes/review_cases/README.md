# Reviewer eval cases

Each JSON file here is a real review history, anonymized, replayed as a
regression case by `passes/review_eval.rs`. Every reviewer incident becomes a
case, so the same class of mistake can't come back silently.

## The two layers

- **Deterministic (every PR, in CI, no model).** `cargo test -p worker review_eval`
  drives the real decision code with the model outputs recorded in the case:
  `decide_verdict`, the enforcement rules, the one verdict function
  (`review_verdict::decide`), the failure and requeue policy, job dedup keys,
  the stuck-review sweep, and the reply-after-review rule. It checks the
  case's `expect` and three invariants on every case:
  1. no formal verdict is posted for a review that didn't finish;
  2. no reply that says nothing blocks is posted while CoderHelm's own
     request for changes stands;
  3. the current head ends with a formal outcome, and a redelivered job
     never posts twice;
  4. the merge gate never merges a draft, a PR whose CoderHelm approval was
     dismissed or never given, or one whose CI is running or failing.
- **Live (nightly, model in the loop).**
  `cargo test -p worker review_eval -- --ignored`. The model classifies
  every recorded comment (`ask_kind`) and reads every recorded reply
  (`conclusion`), and each answer must fall in the case's class.
  `.github/workflows/reviewer-evals.yml` runs it nightly and opens an issue
  when it fails. It holds no model key: GitHub's OIDC token becomes
  short-lived credentials for the `coderhelm-prod-reviewer-evals` role
  (`infra/lib/worker-stack.ts`), which only this repo's main branch can
  assume and which may only invoke the eval model on Bedrock
  (`EVAL_PROVIDER=bedrock`). Locally: `EVAL_PROVIDER=bedrock` with AWS
  credentials, or `ANTHROPIC_API_KEY` with your own key.

## Add a case from a real PR (one step)

```sh
AWS_PROFILE=<profile> python3 scripts/export_review_case.py \
  --team 'TEAM#...' --repo owner/name --pr 123 --names alice,bob \
  > services/worker/src/passes/review_cases/<what_went_wrong>.json
```

The script reads the PR's `REVIEW#` records and writes a skeleton. The repo
becomes `acme/app`, commits become `h1`, `h2` and so on, and the names you
list are scrubbed. Then:

1. Fill in each `TODO`: the findings each review emitted, each comment's
   text and `ask_kind`, and each reply's `conclusion`.
2. Rewrite any code or text from the customer into a generic equivalent.
   Keep only the minimal hunk the case needs.
3. Set `expect`:
   - `final_formal`: the head and event the PR must end on.
   - `formal_events` (optional): the exact list of formal reviews.
   - `first_approval_has_notes` (optional): the approval carries its
     non-blocking notes.
   - `gate` (optional, with `auto_merge` set on the case): the merge gate's
     final `state`, whether it `merged`, and whether the PR is a `draft`.
4. Run `cargo test -p worker review_eval`. The case should fail on the
   behavior you're fixing and pass once the fix is in.

## Steps

| step | meaning |
|---|---|
| `{"push": "h2"}` | A new head. |
| `{"review": {"head", "model_output", "deliveries"?}}` | A review finished with this output (`ReviewOutput` JSON). `deliveries: 2` replays an SQS redelivery. |
| `{"review_fails": {"head", "times"}}` | The review failed `times` times. |
| `{"legacy_error_review": {"head", "error"}}` | The old system posted a failure as "changes requested". |
| `{"comment": {"id", "text", "ask_kind", "reply"?, "conclusion"?, "review"?}}` | A person addressed CoderHelm. `review` is the formal review that runs if the ask or the reply needs one. |
| `{"sweep": {"minutes_later", "review"?}}` | The stuck-review sweep runs. `review` is what it gets if it picks the PR. |

### Merge-gate steps

A case with `"auto_merge": {"require_human": true}` runs the real gate
decision (`await_merge::decide_gate`) whenever production would: CoderHelm's
APPROVE verdict arms it, a person's approval arms it, and the gateway's
re-arm rules (`common::merge_gate::wakes_on_review` / `wakes_on_ci`) decide
which later events wake it.

| step | meaning |
|---|---|
| `{"approve": {"by"}}` | A person approves the head. |
| `{"request_changes": {"by"}}` | A person requests changes. |
| `{"dismiss": {"who"}}` | A review is dismissed (`"coderhelm"` or a login). |
| `{"ci": {"name", "status", "conclusion"?, "suite_completed"?}}` | A check run on the head changes; `suite_completed` sends GitHub's `check_suite completed`. |
| `{"convert_to_draft": {"by"}}` | A person converts the PR back to draft. |
| `{"ci_passed_mark_ready": {}}` | CI passed on CoderHelm's branch and the pipeline tries to mark the PR ready. |
| `{"gate_is": {"state"?, "merged"?, "draft"?}}` | Asserts the gate mid-case. |

## Run cases (`runs/`)

What a ticket run does around its review, replayed by
`review_eval_replays_every_run_case` (deterministic, every PR) and
`review_eval_live_ticket_comments` (live, nightly: the model reads each
recorded ticket comment and must give the case's `intent`). `kind` is one of:

| kind | inputs | `expect` |
|---|---|---|
| `rerun` | `history` (earlier `runs`: status, PR and how it ended, lesson, content hash; `notes` people wrote), `current_hash`, `plan` (the planner's `fix_location.json`, or `null`) | `new_pr`, `route` (`code`, `outside_only`, `code_and_outside`, `ask`) |
| `jira_comment` | `run_status`, `action` (`run`, `feedback`, `none`), `comment`, `intent` | `act` (`dispatch`, `elsewhere`, `nothing`), `starts_run` |
| `own_pr_review` | `model_output` (the PR reviewer's `ReviewOutput` on the run's own PR) | `verdict`, `closes_own_pr` |

Invariants checked on every case: a ticket whose CoderHelm PR was closed
unmerged with an outside-code lesson never gets another PR while the ticket
is unchanged; a plan with no decision on where the fix belongs never codes; an
informational or status-only ticket comment never starts a run; a run never
completes while its PR reviewer's verdict blocks.
