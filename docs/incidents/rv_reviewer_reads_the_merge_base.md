# rv. `rv_reviewer_reads_the_merge_base`

**What happened:** The reviewer got four things wrong on a pull request for a booking vendor's portal page and on a CI script's.

- **The base merge:** after the base branch was merged into the pull request, the re-review read the diff from the commit it had approved. That diff carried a drawer component another pull request had already merged, and the review called it an unrelated change. Against the merge base the pull request changed a handful of files, and the drawer wasn't one of them.
- **The passing run:** the preview's end-to-end run exercised the vendor's test ids against the live portal and passed. The review still blocked on "selectors no probe observed", because it only read probe records committed in the repository.
- **The red check:** the end-to-end check was red on the pull request and on the base branch. The review was told only the check's name, and it wrote that an unrelated fuel-pricing test was "broken on main". The failing test was the slip-selection test in the spec the pull request changed. It failed because the vendor had opened next season's booking.
- **The missing test:** on the CI script's pull request the review rightly asked for a test, and the run handed it back. A stub of the cloud command on PATH and a fixture of its answer were all the test needed.

**The guarantee:**

- **What a pull request changes:** its merge-base diff. A re-review covers only the files whose own change moved since the reviewed commit. A finding that calls a file outside that diff unrelated or out of scope is dropped.
- **A passing run is a record:** a passing end-to-end run on the head, whose report shows the test ids, counts as a probe record of them.
- **The reviewer probes:** for ids nothing observed, the reviewer runs the probe itself.
  - Ids the page has clear the gap.
  - Ids the page lacks are a required finding that quotes the probe.
  - A probe that couldn't run is a note with the reason, never a block.
- **Red checks, test by test:** a red end-to-end check is read test by test, from each failing test's own error. Tests in specs the change touches come first. A check is never the base branch's by its name.
- **Blame and reverts:** a sentence that blames the base branch for a test, or calls it flaky or pre-existing, is held unless that test fails on the base branch's head too. If it does and its spec is one the change touches, the fix belongs on this branch, and the sentence says so. Asking to revert part of the change is held unless the test passes on the base branch's head.
- **Tests are the coder's work:** on CoderHelm's own pull request, a request for a test always goes to the coder, whatever made it a note. A request about a script comes with how to test it: stub commands on PATH, fixtures, and running the script itself.
