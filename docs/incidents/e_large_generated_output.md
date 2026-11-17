# e. `e_large_generated_output`

**What happened:** A types file over 5 MB, written through `../`, was dropped silently. The run said "regenerated 1 file(s)" and opened no pull request.

**The guarantee:** Every changed file is committed, or named as a tooling failure. Paths have one spelling. A generated task its generator settled counts as done in the plan check and the proof check.
