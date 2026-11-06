# Replay suite

Incidents where CoderHelm stopped, asked, blocked or looped when it should
have gone on, replayed through the real decision code and graded on the
decision. CI runs it with the incident suite (`cargo test -p worker
replay::`); a prompt, model or effort change that breaks a case fails CI.

## Add an incident

Add one file to `cases/`, named `NN_what_happened.json`. Use made-up names
only (no company, customer, repository, ticket or person names).

```json
{
  "name": "one line: what must hold",
  "about": "what happened, and why the decision below is right",
  "case": "stop_judge",
  "input": { "picture": { "kind": "ask", "step": "plan", "reason": "...", "reversible": true } },
  "replies": [[ {"verdict": "proceed", "assumption": "..."} ], [ ... ], [ ... ]],
  "expect": { "verdict": "proceed", "never_ask": true }
}
```

- `case` picks the decision path (see `mod.rs`): `stop_judge`,
  `ticket_route`, `registration`, `run_loop`, `review_settled`,
  `blocker_comment`, `deploy_count`. Each reads its own `input` and
  `expect`; the existing cases show both.
- `replies`: where the stop judge is called, the model's recorded answers,
  one list per trial, in call order (a rule-breaking first answer followed by
  the re-asked one is two entries). Three trials (`k` = 3), and every trial
  must pass (pass^k). Cases with no model call leave `replies` out.
- Record real answers with `REPLAY_RECORD=1 ANTHROPIC_API_KEY=... cargo test
  -p worker replay::replay_record -- --ignored` and review the diff.
- Run the real model on every case without writing anything with
  `REPLAY_LIVE=1 ANTHROPIC_API_KEY=... cargo test -p worker
  replay::replay_live -- --ignored` (optional, not in CI).

## From the outcome log

A decision people confirmed wrong (a question answered with the default, a
review dismissed or merged unchanged) exports as a case from Analytics
("Unnecessary stops" and "Nuisance blocks", export) or from
`common::decision_log::fixture`. It arrives as a `stop_judge` case with the
picture and the answers that were wrong, so it fails until the decision is
right. Scrub names, fix the decision path, record three trials, commit.

## Model and effort

Judgment steps run on the strongest configured model at high effort
(`ModelProvider::judgment_model_id`, `common::stop_judge::JUDGMENT_EFFORT`),
and `passes::stop_judge`'s tests list every one. A downgrade to a cheaper
model or a lower effort clears this suite in live mode first.
