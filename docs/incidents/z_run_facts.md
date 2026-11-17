# z. `z_run_facts`

**What happened:** A run's plan check found 3 of 3 tasks done; a later fix round wrote 1 file and its task tracker, built from the plan's unchecked copy, saved 1 of 3 over the plan check's checklist. The same runs showed no changed files: `files_modified` was written only by a completion step nothing called.

**The guarantee:** A round's coder starts from the checklist as saved and only adds ticks; only the plan check unchecks a task. `files_modified` is the branch's work against its base, without the plan files, written when the pull request opens and after every round that records the run's status, capped at 500 paths.
