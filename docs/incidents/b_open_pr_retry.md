# b. `b_open_pr_retry`

**What happened:** A retry started over on a branch whose earlier run was still fixing CI.

**The guarantee:** A retry continues the earlier run's open pull request and opens no other. While the earlier run still works on the branch, the retry stops before writing anything and says why. That covers a run that is running, waiting on CI, holding a writer slot, or has a round under way or parked.
