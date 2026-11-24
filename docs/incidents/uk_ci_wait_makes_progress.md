# uk. `uk_ci_wait_makes_progress`

**What happened:** A run finished its work and waited for its pull request's checks. One workflow passed; another never left `queued`. Every two minutes the run asked GitHub again and armed another check. Webhook resumes in between armed a second chain, so it checked about once a minute. After eight hours and almost 500 checks nothing had changed, and nothing said which check it was waiting on. The 24-hour cap measured from the run's last write, which every check refreshed, so it never came.

**The guarantee:** The wait keeps the pending set and when it last changed. An hour with no progress re-runs the stuck checks' workflows once. A second hour stops the wait, names the stuck checks with how long they waited, and finishes the run with its pull request open for the ready decision. A run has one wait chain: a delayed check is armed only when none is due.
