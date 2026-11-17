# oz. `oz_reviewer_converges`

**What happened:** The fix on the pull request of `ow` and `ox` was right by its third commit. CI and the preview's end-to-end run were green, and a person's product answers were built. The review still requested changes for six more rounds on two generic findings. "A parallel structure next to an existing one" never named the function it duplicated. "A test out of proportion to the change" moved from one test file to the next as each was trimmed, and landed on a test whose cases the review had asked to restore. CoderHelm's own fix rounds deferred both as making no progress, and the review kept blocking. A person had answered that the path pages should cover both rows; the run reverted that work as out of scope, and a later review called it missing.

**The guarantee:** On every review, before the verdict:
- a blocking finding names its file, its line, and what it conflicts with or asks for;
- a generic finding doesn't move to another file;
- a finding never asks the opposite of an earlier request;
- a finding CoderHelm's own rounds deferred holds no more unless new code gives it new evidence;
- a person's answer decides scope;
- once each ticket criterion is proven both ways with CI green, whatever is left that isn't a shown defect is a follow-up note on an approval.

**Where it's enforced:** `passes::review_discipline` (`judge` and its rules, `criteria_met` reading the run's two-way proofs), called by `review_pr` after the severity pass; `review_rules::anchor_line`; `review_pr::settle_own_findings` (a deferred finding's thread no longer holds).
