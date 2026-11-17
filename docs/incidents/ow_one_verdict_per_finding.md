# ow. `ow_one_verdict_per_finding`

**What happened:** CoderHelm's own pull request fixed a sport filter that listed one sport twice. A later review listed findings under "Holding the merge" that the same review said no longer applied: the earlier dropdown thread, whose code the fix rounds had replaced, and two findings the review itself kept as notes because they came late. On CoderHelm's own pull request each late note was also chained into its open work, which is named as holding the merge, so each of those findings had two verdicts in one review.

**The guarantee:** Every finding has one state before the review is written: blocking, a note, settled, or no longer applying. A thread that is resolved, settled by this review or about code no longer in the change, a finding raised late, and an earlier hold the review now calls a note never hold the merge. "Holding the merge" lists only what still blocks at the head with its thread open. The finished body is checked before it is posted: no finding listed as holding is also called a note, settled or no longer applying.

**Where it's enforced:** `passes::review_one_verdict` (`settle`, `check`, `enforce`), called by `review_pr` before the verdict is made and before the review is posted; `review_pr::closed_threads`; the `gone` thread status (`review_agent::GONE`, `review_rules::settles`).
