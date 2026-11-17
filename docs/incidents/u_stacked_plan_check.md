# u. `u_stacked_plan_check`

**What happened:** A run stacked on an earlier part's open pull request reset to the default branch in every step, rebuilt and force-pushed its branch each time, and measured its plan against the other branch: 13 files that branch already changed read as not done, the coder found nothing to write, and the guard stopped the run after seven continuations.

**The guarantee:** A later step keeps the base an earlier step stacked it on; a planned file the stacked-on branch already changes is done there, and the plan check's note says so; a fall in the done count is reported on the run page, and its recovery counts as progress a bounded number of times.
