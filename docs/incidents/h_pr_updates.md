# h. `h_pr_updates`

**What happened:** A PR updated over several runs went stale. Its description was out of date, a ticket comment was posted twice, a question was asked on every update, an e2e test skipped itself when content was missing, and a note said "could not look up" content that was readable.

**The guarantee:** Each one is caught by its own check. The description is rewritten when a round lands. A notice is posted once per ticket, and so is a question. A spec that skips on missing content fails the checklist. A note that didn't look, when it could have, goes back to the planner.
