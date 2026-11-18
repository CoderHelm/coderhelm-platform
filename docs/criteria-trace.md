# Criteria traced to code, tests and proof

For each run, every acceptance criterion is linked to the code changed for
it, the tests that cover it, and what proves it on the pull request's head.
The link is a record on the run, a table in the description, and open items
for whatever is missing.

Code: `services/common/src/trace.rs` (pure). The description builds the
trace (`services/worker/src/passes/description.rs`), and the run API serves
it.

## The criteria

The criteria are the ticket's acceptance list, else its Expected lines,
else the plan's `## Criteria map` (`common::e2e_proof::criteria_or_map`),
the same list the two-way proof uses. Each row records whether it came from
the `ticket` or the `plan`.

A criteria map line can name the code it is built in:

```
- <criterion> | page: <path> | e2e: <spec> › <title> | asserts: <what a person sees> | code: <files or functions>
```

## Each row

| Column | How it is read |
| --- | --- |
| Code | The diff's non-test files that the map line's `code:` names, by path or by a definition in their patch. Without `code:`, it is the files whose path, added definitions or added lines share at least two of the criterion's words (or one in the path or definitions). A ticket with one criterion owns the whole change. Each file carries the definitions it adds or changes (`fn`, `function`, `class`, `def`, `func`, and so on). |
| Tests | The map line's end-to-end test, plus the diff's test files whose path or test titles share the criterion's words. A spec that the map gives to another criterion is never taken by words alone. |
| Proof | The criterion's two-way proof on this head (staging and the preview, with the server HTML check from `e2e_proof`). Without one, its tests' runs with and without the change (the tests and scope check). Otherwise not run yet, or no test. |

A proof is one of `proven`, `not_the_ticket` (passes without the change),
`failing`, `missing`, `unverified` or `not_run`.

## Gaps

A criterion is an open gap when no change in the diff is traced to it, no
test on the head covers it, or its proof doesn't hold. Each gap is listed
in the description's open items with what is missing.

The gaps are also work for the coder (`common::trace::coder_items`), fed
into the same loop as the run's other open gaps:

- Each build of the description records them on the run's checks ledger
  (`run_guard::Ledger::trace`, for the head it was built for). There they
  are open items like any check's: they hold the draft (`open_findings`)
  and the run converges on them (`run_guard::Target`).
- When the two-way proof runs on a fresh head (`pr_watch`), the
  description is built first, then the proof's items and the trace's go
  back to the coder together, in one fix round
  (`resume::queue_own_review_round`).
- For a pull request with no preview, where no two-way proof runs, the
  description build sends the trace's items itself.
- A criterion whose two-way proof already sends an item (a test missing,
  failing on the preview, or passing on staging too) is sent once, by the
  proof. The trace adds an item for it only when no changed code is traced
  to it.

## Where it shows

- **Pull request description:** a compact table, `Acceptance criteria,
  traced`, in the "Full test evidence" fold. It is rebuilt from the facts of
  the head on every push, like the rest of the description.
- **Run record:** attribute `traceability` (JSON), written on every build of
  the description.
- **Run API:** `GET /api/runs/{run_id}` returns `traceability`:

```json
{
  "head": "c0ffee1…",
  "rows": [
    {
      "criterion": "Only one sport filter chip shows on /camps",
      "source": "ticket",
      "page": "/camps",
      "code": [{ "path": "src/utils/filters.ts", "symbols": ["dedupeSportsBySlug"] }],
      "tests": ["e2e/camps.spec.ts › shows one chip"],
      "proof": { "state": "proven", "by": "end to end on staging and the preview", "detail": "…" }
    }
  ]
}
```

The replay case is `services/worker/src/passes/incidents/dh_criteria_traced_to_proof.rs`.
