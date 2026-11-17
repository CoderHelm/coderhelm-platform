# p. `p_finished_work_loop`

**What happened:** A small ticket finished its 11 tasks, then spent 2.5 hours and $41.96 with no pull request: each 15-minute step re-ran the checks before the pull request from nothing and started fresh fix rounds.

**The guarantee:** The draft pull request opens before the proof check, the rules and the review, which then can't block it. A check runs once on a head and keeps its rounds across steps. A run with no real progress (tasks, the pull request, CI green, fewer findings) ends one way: a draft with Open items, waiting on a person.
