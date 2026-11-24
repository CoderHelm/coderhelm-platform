# um. `um_own_reads_never_handed`

**What happened:** Runs handed people reads CoderHelm does itself. One asked a person to "read the Deploy Preview e2e result" because its app lacked Actions write, though reading a run needs no write. Another stopped with six requirements "that need access CoderHelm doesn't have"; the first was to confirm the preview's e2e ran and passed. A third deferred a review finding to a person: "check DevTools on the preview for the analytics request when the tab is hidden".

**The guarantee:** Reading an end-to-end or CI result (the workflow run, its log, report or artifacts), or what a page or the preview shows or requests, is CoderHelm's own work (`github_history`, `check_url`, `probe_page`). The person gate keeps it as CoderHelm's whatever permission the writer blamed. The plan check makes such a step `by: coderhelm`, and a review round that says only a person can read it is not a person's claim.
