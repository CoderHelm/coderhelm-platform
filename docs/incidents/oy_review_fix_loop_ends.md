# oy. `oy_review_fix_loop_ends`

**What happened:** A data-pipeline run's own review raised something new each round: a model finding in round 2, "A test out of proportion to the change" in round 3, and in round 4 "A parallel structure next to an existing one" that named as new a function the main branch had had for months (the branch only added a parameter to it). Each held the merge, so the run went review, fix, review every ten minutes for nine fix rounds, past its spend ceiling. The review on the pull request stayed on an old head the whole time, and a person had to ask for a re-review.

**The guarantee:** The parallel-structure check reads both sides against the base, so a definition the base already has is never new. After the first review, the smallest-change checks that a reason may keep give notes, and a finding on lines unchanged since the previous review is a note. Late notes join a round that has work that holds, and never start one, so the run ends once only notes are left. Every head the run pushes gets the PR reviewer's review posted, news or not.

**Where it's enforced:** `minimal_change::parallel_structures`; `review_policy::hold_late_blockers` (`ADVISORY_LATE_NOTE`) and `review_rules::advisory_check`; the work list in `review_pr`; `review_pr::own_head_is_new`; the run's review step in `resume` (`review_for_run` on every head).
