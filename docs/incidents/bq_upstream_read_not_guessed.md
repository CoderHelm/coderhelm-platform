# bq. `bq_upstream_read_not_guessed`

**What happened:** A card said "1 new log finding: Seen again: Request failed with 503 in AUTH GUARD" for two lines of an error the API logs most days, with "the evidence does not show which upstream service returned the 503", an AWS account id line and a generic link; nothing had read the guard's code, which names the service it calls, and each analysis sent every line of a burst to the model.

**The guarantee:** A sighting is judged against its own week first (`common::log_triage`, the analyzer's same cases): at its usual rate it is counted with no card and no fix, rising is act, new or back after a quiet week is said once. A failed call's code is read: the card names the upstream (host and setting), its timeout and retries, the lines, tasks and the week's rate, with the decision on the first line and no account id. A burst is one entry before any model call, known errors are counted without the model, and an hourly budget caps analyses.
