# bd. `bd_skill_runs_on_coderhelm`

**What happened:** A team's report skill was judged to need tools: its login check was a gap, its output folder "nothing can be written outside the repository", its bundled script "Coderhelm cannot run shell commands or scripts", and the files the script writes could not be read. The check's list of what Coderhelm can do left out the sandbox and a run's files, and its rules called every script impossible.

**The guarantee:** Every reader of text written for a person's machine applies one rule and one capability catalogue (`common::host_steps`). Laptop setup is not needed on Coderhelm: left out silently and never counted against the item. A kept script runs in the sandbox with a read-only token for the repositories it reads, taken out of everything that comes back; its files are kept as the run's outputs, read back and linked. The check reads 7 of 7 steps run, and a re-check names each step that changed.
