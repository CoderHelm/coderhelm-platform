# wc. `wc_release_tags_only_from_main`

**What happened:** A repository whose release tags deploy production had CoderHelm's release on. Pull requests that merged into a feature branch were released like merges into main: the auto merge tagged the merge commit, and the release steps and the batched release tagged the head of the branch the pull request went into. Each tag deployed that feature branch to production over what main held. Pushes were already read only from the default branch; the merge paths never asked which branch they merged into.

**The guarantee:** Every release tag CoderHelm cuts goes through one function, which tags a commit only once GitHub says the default branch holds it; if GitHub can't say, nothing is tagged. A merge into another branch arms no release, and the pull request and its release steps say it was not released and why. A batched release always tags the head of the default branch, whichever branch its window was opened from.
