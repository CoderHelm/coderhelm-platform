# wb. `wb_beta_tag_after_skipped_evidence`

**What happened:** Promotion gained a job that reads release reports from live installs, and it runs only when promoting. The planning job ran after it either way, but the job that attests and tags each release had a condition with no status function. GitHub adds `success()` to such a condition, and `success()` is false when anything upstream was skipped, so every beta was planned but never tagged, and its publish failed waiting for the tag.

**The guarantee:** The tag job runs whenever planning succeeded with something to release, whether or not the reports were read. The workflow check that runs in CI refuses any job whose `if:` has no status function while a job it needs, directly or further up, has a condition of its own; a job that is meant to skip with its ancestor says so with `success() &&`.
