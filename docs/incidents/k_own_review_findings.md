# k. `k_own_review_findings`

**What happened:** CoderHelm's own review of its own PR posted three findings as notes and APPROVE. Three seconds later the PR was marked ready and the merge armed, with none of the three fixed and no thread resolved.

**The guarantee:** On its own PR, CoderHelm fixes every finding of its own review that isn't strictly optional (an inferred preference, or an optional improvement to correct code) before the PR is marked ready or the merge armed. The review requests changes and lists them at the top of the description. The run takes them in review-fix rounds, and the next review resolves the threads the new commit settled. It goes on while rounds make progress. Two rounds without progress stop it: the PR stays a draft with the findings listed, and the run is `needs_input`. A verdict is never APPROVE while a finding holds the merge.
