# ax. `ax_github_budget`

**What happened:** An installation used up GitHub's hourly request budget: sweeps on timers spent it beside the runs. A review fix round that had worked for two hours met the limit, the next step logged `Resume failed: RateLimited`, the run read "the last fix round didn't finish" and its review's findings were deferred to a person; three sweeps failed on the same limit, one warning per item.

**The guarantee:** A rate limit is a wait, everywhere: the resume is sent for the reset as the same run and round (no attempt spent, nothing unfinished, nothing deferred) and the run page says "Waiting for GitHub's hourly limit; continues by itself at <time>". Once the limit is met nothing is sent until the reset. Every request is counted by caller; sweeps spend only above the share kept for runs and within their own hourly caps, and say a skip once. A round a limit ended is run again by the sweep once the budget allows.
