# dp. `dp_merged_is_deployed`

**What happened:** In a repository that deploys to production only when a version tag is pushed, merged pull requests were never released: one CoderHelm merged, and others people merged after the last tag. A person cut the next tag by hand. Three things stopped it:

- The pull request's release steps ticked while waiting for the merge. A tick that met GitHub's budget held returned the error and sent no next tick, so the chain ended before the merge. At the merge the release was unchanged, and the follow assumed its chain was still running.
- The repository's batched release tag fired after CoderHelm's merge. Its tries met the same held budget, counted it as failures, and recorded the release as blocked.
- Merges by people never opened a release window. Only CoderHelm's own merge did.

**The guarantee:**
- A push to the default branch joins the repository's release window, whoever merged, when the team releases merged changes (`auto_tag`). One tag covers everything merged so far. Merges during a release go in the next one.
- The tag is cut on the head only when its checks are green. While they run, the release waits (up to 3 hours). A failed check stops it and is shown on the Releases page, where "Release now" cuts it. A release freeze (agents paused) holds it until it ends.
- A held or spent GitHub budget is a wait until the reset, never a failed try. An unreadable tag list is an error, never "no tags".
- A release step tick that fails is tried again, and a merge arms an unchanged release again.
- The tag deploy of a team that releases merged changes waits for that release with no approval card of its own. It follows the deploy run of the tag that first contains the merge (on that tag's commit), and says once on the run and on its ticket "Deployed to production in `vX.Y.Z` at HH:MM UTC", or that the deploy failed. A deploy on merge is followed and said the same way.
- Changes merged while nothing was listening are caught up: after each CoderHelm release, and hourly, every repository that releases merged changes and already releases by tag gets a release window when its default-branch head is in no release tag and no window waits. That window follows the same rules.
