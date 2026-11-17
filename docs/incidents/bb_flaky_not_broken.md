# bb. `bb_flaky_not_broken`

**What happened:** An end-to-end workflow failed on a timeout in a spec the change never touched, as it had on unrelated pull requests that week. The run spent fix rounds editing the spec's timeout and a retry helper. Where the GitHub App may not write Actions, the re-run tried first was refused and the same fix round followed.

**The guarantee:** Before a CI fix round the failure is judged (`common::flaky_checks`): a failure naming a changed file is the change's; a check with a record of failing and then passing on one commit of other pull requests, naming no changed file, is re-run once and no code changes for it, and the pull request says which check, the evidence and what was done. A refused re-run is said with the permission by name and still changes no code. The record is per repository and learns only from a commit that failed and then passed unchanged.
