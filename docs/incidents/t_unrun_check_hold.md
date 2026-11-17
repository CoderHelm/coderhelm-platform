# t. `t_unrun_check_hold`

**What happened:** A check's gate couldn't install its package's dependencies (`pnpm: command not found` in CoderHelm's sandbox). The tooling failure went under Open items as a check the change didn't pass, and the pull request stayed a draft after CI passed.

**The guarantee:** A check CoderHelm's own tools couldn't run becomes an Open item CI settles, never a hold. A line written the old way stops holding once CI is green on the head and leaves the description when the pull request is marked ready. A check the change really failed still holds. The decision reads the repository's CI whenever such a line is in the description, not only when CoderHelm's own feature checks have findings: without that read, CI counted as not green and the line kept holding on every sweep.
