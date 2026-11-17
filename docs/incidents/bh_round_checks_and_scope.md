# bh. `bh_round_checks_and_scope`

**What happened:** A fix round edited 15 files and ended with its checks "not run: not enough time left": it kept editing until no sandbox build fit, and nothing ran the checks after it. Two edits were refused as outside the round's files, one a test of a file the round changed, with no file named on the run page; live rows read "Edited · 1 change" with no file and a list of reads with an empty name.

**The guarantee:** When the branch has unchecked edits and the step fits one more build but not two, the coder is told to run the checks now; a pass that ends with edits no check ran on owes its checks and the settle stage runs them before the run finishes. The tests of a file the round changes and the module file that exports it are the round's own files, recorded with why; every refusal names its file. The live log's summary of a call names its file or query as `key: value`.
