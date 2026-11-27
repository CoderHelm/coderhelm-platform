# p. `p_finished_work_loop`

**What happened:** A small ticket finished its tasks, then spent hours and tens of dollars with no pull request: each 15-minute step re-ran the checks before the pull request from nothing and started fresh fix rounds.

**The guarantee:** The draft pull request opens before the proof check, the rules and the review, which then can't block it. A check runs once on a head and keeps its rounds across steps. A run with no real progress (tasks, the pull request, CI green, fewer findings) ends its steps one way: a draft with Open items. Once its tasks are done that draft is shipped, not handed back: CI's errors and comments still start rounds on it (see `da_finished_run_ships`).
