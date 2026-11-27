# rp. `rp_reproduce_before_planning`

**What happened:** A data-pipeline ticket named a few tours that were filed under the wrong activity. The pipeline already had an overrides table for exactly this case, with entries for those tours. Its lookup matched exact names, though, and the booking vendor appends a "-Half Day" or "-Full Day" suffix, so the entries never matched: the lookup returned nothing for each of the ticket's names on the base.

The run's notes said so, then planned a keyword step beside the table. The diff was hundreds of lines across the pipeline: a moved extractor, keyword matching, new logging and a fixture. It also moved other programs to another lookup, which its own reviewer flagged as out of scope, and it asked two questions on the pull request that one page load answered.

The new test was never run on the base. The repository's CI runs each test file in a `python "$t"` loop, which CoderHelm couldn't narrow to one file, so the test was recorded as not run and the description said nothing about it. The right change stripped the suffix in the lookup and added two entries (a small diff, mostly a test of the exact stored names), and that test fails on the base.

An audit of recent runs found the same shape often:
- most bug tickets that wrote code ran nothing before their first edit;
- several diffs of hundreds of lines answered a ticket that named a few records;
- none of the runs whose tests were checked had a test shown to fail on the base.

**The guarantee:**
- **Reproduce first.** A bug plan has a `## Reproduce` section with:
  - the ticket's exact inputs and their stored variants, with where they came from;
  - a command this run ran on the base with `reproduce_on_base`, with its output quoted;
  - the line where the existing code goes wrong.

  A run that tried and couldn't reproduce writes `Unverified:` with why.
- **Fix the mechanism found.** The fix goes in the mechanism that the wrong line is in. A new resolution path beside it goes back to the planner.
- **Look questions up first.** A question that a page load or a query answers is looked up before it is decided with a default.
- **Size the diff to the ticket.** A diff that rewrites a code path for a few named records goes back to be narrowed, unless a `Wider than the ticket:` line gives the reason.
- **Fail on the base.** Each new test runs on the base and on the head, the way CI runs it, even when CI runs it in a loop. A bug fix with no test shown to fail on the base says `Unverified:` in its description.

**Where it's enforced:**
- `common::repro_first`;
- `passes::reproduce` (the tool, and `plan_gaps` in the plan check);
- `minimal_change::whole_path`;
- `common::test_env::TestEnv::file_command`;
- `habits_check::file_cmd` and `UNVERIFIED_FIX`.
