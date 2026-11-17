# bu. `bu_held_draft_after_release`

**What happened:** A release fixed how a held draft is finished, and the run it was for never got there. Its hold had been decided 22 minutes before the release and the draft sweep re-decides a hold only once it is an hour old: the first sweep after the release came six seconds early and only peeked. The next skipped the installation for GitHub's budget, logging "it looks again in about 1 min", though nothing ran until the next scheduled sweep. The pull request watch spent its hourly cap on merged pull requests and looked at the held draft in none of three sweeps. A person reset the run 67 minutes after the release.

**The guarantee:** A hold carries the version of the code that decided it, and the sweep decides a hold other code wrote on its first pass after a release, decisions before peeks. A sweep GitHub's budget skipped runs again once the budget refills (one follow-up per gap, claimed). The pull request watch looks at held drafts first.
