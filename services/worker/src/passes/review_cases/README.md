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
     never posts twice.
- **Live (nightly, model in the loop).**
  `cargo test -p worker review_eval -- --ignored` with `ANTHROPIC_API_KEY`
  set. The model classifies every recorded comment (`ask_kind`) and reads
  every recorded reply (`conclusion`), and each answer must fall in the
  case's class. `.github/workflows/reviewer-evals.yml` runs it nightly and
  opens an issue when it fails.

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
