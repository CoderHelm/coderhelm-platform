# vv. `vv_run_endings_typed`

**What happened:** Three ways a run ended said something that wasn't so.

- A run's checks stayed red after its fix rounds. The pass knew why it stopped (which checks, on which head) and wrote it as a sentence. The error classifier, which is built for HTTP errors, read the sentence back, found no shape it knew, and the run waited on a person as if after an unknown error it had already tried again. Nothing had tried again. The base branch's checks were read only when the round started. The base went red on the same deploy check during the round, and the run never saw it.
- Planned-work rounds met the team's usage limit before their first model call. Each was counted as a round that made no progress, and enough of those stopped the work.
- A test read preview content that wasn't there. The content-gap check matched a list of phrases the failure didn't use, and it ended the CI loop only when the change carried its own step to add the content. The round changed no code and the run failed.

**The guarantee:**

- Every run status write goes through one gate, `common::run_ending`. A run ends only as done (with its pull request, its merge, the reason no change was needed, the pull request that already does the work, or where its answer is), waiting on someone named (a person or a permission, with what they do and the evidence), or stopped (with a typed reason and what was tried). The gate keeps the ending on the run as `run_ending`. An ending without its evidence is written as `ending_unproven`, never as claimed. A test walks every status write in every service and refuses one that doesn't come through the gate.
- A stop whose reason the pass knows is typed (`run_fail::Cause::Stopped`) and is never read by the error classifier. The classifier reads only errors a pass raised. "Tried again" is written only when the recovery ledger holds a retry.
- Red CI at the ending names its checks, its head and the base, and the base is read again at the ending. Every failing check red on the base too finishes the run (`ci_end` = `red_on_base`). Otherwise the run stops `failed` with the checks named and what was tried. It is not resumed as a pause.
- A round the usage limit refused never counts as a stall. The run waits on the limit, the same ending every run at the limit takes. A round that didn't finish its work runs again with the stalls it had, under the same runaway ceiling.
- Every CI fix round ends its account with its verdict (`Cause: code`, `Cause: content: ...` or `Cause: outside_change: ...`). A round that changed no code and found the test's content missing ends the loop with that content named as a step before merge, whether or not the change has a step for it. The failure's own words are only the fallback when a round wrote no verdict.
