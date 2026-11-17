# ba. `ba_verdict_on_old_commit`

**What happened:** A run finished as Done with its pull request ready while its own reviewer stood at "Changes requested" on a commit older than the head, and the run page said a person was needed and that merging was the team's call. The last fix was pushed, and the wake-up that found CI green with no event to read completed the run through an ending that skips the review step, so the new head was never reviewed.

**The guarantee:** Every completion and the sweep look at the finished run's pull request (`pr_watch`): no reviewer verdict on the head asks for the review of that commit, once per head; changes requested on the head get their fix round, and when the run's rounds are over the findings are deferred on the pull request with the reason. The run page names the one true reason the pull request is where it is (`common::merge_standing`) and names a person only for the team's own rule, with where it is set.
