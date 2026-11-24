# um. `um_own_reads_never_handed`

**What happened:** Runs handed people reads CoderHelm does itself. One asked a person to read the preview's end-to-end result because its app lacked Actions write, though reading a run needs no write. Another stopped with a list of requirements "that need access CoderHelm doesn't have"; the first was to confirm the preview's end-to-end run passed. A third deferred a review finding to a person: check the browser's DevTools on the preview for a network request.

**The guarantee:** Reading an end-to-end or CI result (the workflow run, its log, report or artifacts), or what a page or the preview shows or requests, is CoderHelm's own work (`github_history`, `check_url`, `probe_page`). The person gate keeps it as CoderHelm's whatever permission the writer blamed. The plan check makes such a step `by: coderhelm`, and a review round that says only a person can read it is not a person's claim.
