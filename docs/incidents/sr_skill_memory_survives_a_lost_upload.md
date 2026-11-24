# sr. `sr_skill_memory_survives_a_lost_upload`

**What happened:** A scheduled skill keeps its memory (last week's snapshot) in its output folder, which goes into the sandbox with each script and comes back after it. A script whose upload failed came back with no files; taken as the whole folder, that dropped the restored snapshot, so the run kept no memory and the next week ran as a first run with no week-over-week comparison.

**The guarantee:** An answer replaces the output folder only when the script ran, nothing was dropped, and it did not come back empty where there were files. Anything else is merged into the folder. The memory folder rides inside the skill's folder at `.coderhelm-out/`, so every script finds it at `$CODERHELM_OUT`.
