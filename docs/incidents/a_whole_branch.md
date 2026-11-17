# a. `a_whole_branch`

**What happened:** The plan check said 5 of 7 tasks were done. The last step wrote nothing, and the run ended "I ran but produced no file changes".

**The guarantee:** A step that writes nothing is judged on the whole branch. Work on the branch goes on to the checks and the pull request. With nothing on the branch, the run names the planned tasks that weren't built and never asks generic questions.
