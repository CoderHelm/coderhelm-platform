# bc. `bc_stays_through_merge_and_deploy`

**What happened:** Pull requests were left as they were after their runs finished: one conflicted after its base moved, one went red a day later on a commit status no event carries, and one merged by a person had its production deploy fail with nothing watching.

**The guarantee:** A finished run's pull request is looked at on completion and by the sweep: a conflict starts the conflict round once per base commit, red checks not red on the base go to the fix loop once per head, and a merge by anyone gets its merge commit's deploy watched once. A deploy of CoderHelm's own change that fails on checks passing before the merge gets a revert pull request with the failing jobs as evidence, ready for the team's approval and never merged by CoderHelm; files changed again since are never overwritten.
