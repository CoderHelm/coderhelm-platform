# The coder's loop

How CoderHelm turns a ticket into a change: explore, measure, plan the smallest
change, implement, review it against the ticket, prove it. Each step has a check
in code, so the loop holds whatever the model writes.

## Why

Two pull requests show what went wrong.

- **A data-pipeline ticket** asked to link four camps to a second sport. The plan
  added a second map and helpers beside the existing overrides map, and made an
  insert-only link sync replace and delete links. The build added warnings, three
  counters and a log level change in shared code, plus a 432-line test with fake
  hooks and an invented payload. The right change was four map entries, an insert
  that does nothing on conflict, and one small test.
- **A web ticket's** end-to-end tests for a vendor's embedded portal were rewritten
  three times, each time on a guess about the vendor's page. When a criterion met
  the vendor's behaviour, the test was changed instead of the page.

The causes were in the loop, not only in prompt wording:

- The planner was told to use "2-4 tool calls" on a simple ticket and not to read
  whole files. It never saw the map it should have extended.
- The data rules told every change that touched a pipeline to add counters and a
  warning for each skip, and a fix round enforced it.
- Nothing compared the diff with the ticket. The scope checks compared files with
  the plan, and the plan was already too wide.
- Nothing stopped the coder writing before it searched, or adding a twin of a
  structure that was already there.
- Outside pages were never measured, and nothing caught a test bent to a vendor
  against a criterion.

## The loop

1. **Explore.** Before the planner runs, the worker searches the repository for the
   ticket's own names (backticked code, identifiers, quoted names, capitalised
   names) and gives the planner the hits (`plan_shape::existing_for_ticket`).
   The planner is told to read the structure it extends in full, along with its
   writers and readers.

2. **Measure.** The plan has a `## Measure first` section listing every outside
   system the change or its tests rest on, and how each was measured: `probe_page`
   for a page (from #734), a read-only lookup for data, or `None`. A plan for a
   ticket about an embed, vendor, portal, webhook or partner that measured nothing
   is sent back.

3. **Plan the smallest change.** design.md has five sections:
   - `## Reuse`
   - `## Must not change`
   - `## Measure first`
   - `## Smallest change`
   - `## Proof`

   `plan_shape::plan_gaps` sends the plan back when a section is missing. It also
   sends it back when the plan:
   - adds a name parallel to one it reuses or one the search found;
   - deletes, overwrites, upserts or prunes when the ticket asks for none of that;
   - changes logging or adds reporting the ticket doesn't ask for;
   - would change a test to match an outside system while the ticket's acceptance
     criteria say what the product must do.

   The Reuse section also says, for each helper that writes data, whether it
   only inserts or replaces and deletes. The worker reads each reused helper's
   body (`minimal_change::write_semantics`: a `DELETE`, an `ON CONFLICT DO
   UPDATE`, a delete-then-insert, a relation `.set(`). A plan that reuses a
   replacing helper on a path that only inserts today is sent back unless the
   ticket asks to replace. The camps run's plan named the right map, then
   reused the link helper that deletes every link it isn't given.

   Whatever is still open after the one send-back stays on the decision.

4. **Implement.** In the coder's tool loop (`explore_guard`):
   - The first write of a build is refused until a search has run and the write
     names what it reuses (`reuses`).
   - An edit that adds a top-level map, helper or config beside a similar existing
     one is refused unless it says why (`why_new`).

   Both are kept as evidence lines. The coder's prompt carries the same rules
   (`SMALLEST_CHANGE_RULES`). The data rules ask for reporting on a skip only when
   the ticket asks for it.

5. **Review against the ticket.** After the build, the habits check reads the diff
   against the base and the ticket (`minimal_change::check`). It flags:
   - parallel structures;
   - deleting or overwriting semantics where the base only added;
   - log level changes;
   - new counters and log calls nobody asked for;
   - tests out of proportion to the code;
   - large fake payloads with no probe record.

   Each goes to the fix round as "make it smaller". A `Wider than the ticket:
   <path>: <why>` line keeps an item, except deletes and invented payloads: only
   the ticket can ask for those. The reviewer runs the same check. Deletes, an
   unobserved payload, and a test assertion flipped to an outside page's
   behaviour against an acceptance criterion are required findings
   (`review_rules::SMALLEST_RULE`).

6. **Prove.**
   The first round works out how the repository's CI installs and
   runs its tests (`common::test_env`: the test job's working folder, Python
   and Node versions, install steps and test steps). It records this on the
   run (`test_setup`). Every sandbox build of every later round, and the proof
   step's runs, restore that install from a cache named by the install steps
   and the manifests' blob ids. Only the first build installs and saves it.
   The coder is told the environment is ready and how the tests run. One run
   reinstalled its dependencies eighteen times, about 25 minutes of builds.
   The CI job chosen is the one whose tests run for the plan's files
   (`test_env::from_workflows_for`). A workflow filtered to other paths, or a
   job in a folder none of the files is in, is used only when nothing fits
   better. A web change in a monorepo was once given a lambda's `npm ci` and
   `npm test` (`incidents::cm_test_env_for_the_change`).

   Each new or changed test runs in the sandbox with and without the
   change (`habits_check`). A test the runner finds nothing in goes back to the
   coder, and the coder quotes the run's output in its Evidence.
   These runs install through CI's job for the test files. Without one, a
   workspace member installs from the workspace root. They have a budget
   of their own: a short step owes them to a fresh step, which runs them
   before any other check. How many ran, and why the rest didn't, is
   counted on the run and in the pull request (`incidents::tq_tests_run`). End-to-end code
   against another company's page uses only what a probe observed (#734).

   A proof fix round that answers a gap by looking (a query, a page, a check
   run) is judged again with what it found, even when it wrote no code
   (`proof::round_answered`, `incidents::cn_proof_answer_judged`).

7. **Commit and push.**
   Each gate runs where the commit is made, not as a line in the prompt
   (`common::push_gate`, `passes/push_checks.rs`).

   - **The checks come from the repository.** `run_checks` adds the scripts
     of each package that owns a touched file: its test script, its type
     check (`check-types`, `typecheck`) and its lint. When a non-test file
     changed, the package's whole unit suite runs, because a shared helper
     breaks callers outside the diff. Only a change to nothing but test
     files runs just those files. This step replaces the test line taken
     from the first workflow that had one. On one pull request that line was
     another package's `npm test --passWithNoTests`: it reported "2 of 2
     checks passed" while `vitest` on the touched files exited 1.
   - **Checks before every push** (`push_checks::before_push`, in the CI fix
     round before `round::land`). If the checks ran on the fix's last edit
     and passed, the fix is pushed and the commit records what ran. If they
     failed, or never ran on the last edit, the fix is not pushed: the next
     attempt continues on the same edits with the failing output. If the
     sandbox couldn't run them (the install failed, or there is no sandbox),
     the fix is pushed with an `Unverified:` line saying why and what ran.
   - **The message matches the diff** (`round::land`,
     `push_gate::true_message`). A message is rewritten from the diff when
     it says revert, remove or drop while the diff adds test lines or more
     than it removes, when it says add while the diff only removes, or when
     it names a file the commit doesn't touch.
   - **A revert takes its tests with it** (`push_gate::removed_symbols`,
     `still_expected`). A commit that removes a definition a test still
     names is held until that test is reverted or updated in the same
     commit.
   - **Two fixes per failing test** (`push_gate::FixLedger`, kept on the run
     as `ci_test_fixes`). The failing tests are read from the failure report
     or the log (vitest, jest, Playwright, pytest, cargo, go), or, when they
     can't be, the failing checks are used. After two pushed fixes a test that
     is still red stops the rounds: the pull request and the run name the
     test and its error, and nothing more is pushed. The branch is never
     rolled back to an older green head, and a planned task is never taken
     out to turn CI green. Only a dependency run's newest set of bumps comes
     out (`resume::take_out_failing_bumps`). Five of 53 runs had rolled back,
     two of them with tests left behind that expected the reverted code.
   - **No log is no reason to stop** (`push_gate::logs_unread_brief`,
     `hold_no_logs`). When CI's job log can't be read, the fix brief gives
     the owning package's commands so the fixer reproduces the failure in the
     sandbox. Any sentence that blames the missing log is replaced.
   - **Fixtures use looked-up ids, and tests are sized to the behaviour**
     (`minimal_change::made_up_ids`, `repeated_cases`, in the habits check).
     Ids like `sport-a` or `id-1` are sent back when the run's lookups
     returned real ids. Three or more added cases that differ only in their
     values are sent back as one case per behaviour. The size check (test
     lines against code lines) runs too; it fires on the roughly 415 test
     lines one run added for 50 lines of code.

8. **Rounds agree.**
   Planned work that a review commit on the pull request took out is not
   built again by a planned-work round, and the run says which commit took it
   out. When an acceptance criterion of the ticket asks for it, the ticket
   wins: it is built, and the reply to the review says why
   (`round_agree`, `incidents::co_rounds_agree`).

9. **Scope is decided once.**
   The plan writes `## Out of scope` in design.md, one `- <item>: <why>` line
   each, naming the code that would show it (backticked names, paths, routes).
   A product question the ticket doesn't settle is decided there with its
   default; it is never asked in the pull request. The plan goes back when the
   section is missing, a question is left open, an item has no reason, an
   acceptance criterion asks for an item it leaves out, a task names an
   out-of-scope item, or a task is open-ended ("switch any other caller")
   (`scope_ledger::plan_gaps`).
   When the plan is generated, before any code, the run keeps a scope ledger
   (`common::scope_ledger`, run attribute `scope_ledger`): the ticket's
   acceptance criteria as read by `stop_judge::acceptance` (the same list the
   criteria-to-test mapping reads) are in scope, the plan's out-of-scope lines
   are out. A plan revision never rewrites it. Every later round is held to it:
   - the tests and scope check reads the diff, tests included, and sends back
     each out-of-scope item it brings in (tests are exempt from "inside the
     plan", never from the ledger);
   - a planned-work round never builds an item the ledger leaves out, next to
     `round_agree`'s rule for what a review took out;
   - a round that must build one writes `Scope change: <item>: <why>`; it is
     accepted only when the why cites an acceptance criterion or a person's
     comment on the pull request, and the ledger records who moved it in.
   (`incidents::sl_scope_decided_once`)

   What people say decides scope as they say it (`Ledger::take_people`).
   When the ledger is seeded, and on each round's comments (the habits check
   and the planned-work rounds), an out-of-scope entry that a person's answer
   or comment asks for moves into scope and records who asked. No
   `Scope change:` line is needed. From then on, a diff that takes that work
   out is a breach that goes back to be put back. A comment that keeps the
   item out moves nothing. One run replied "Understood" to a reviewer's answer
   and then reverted the work the answer asked for
   (`incidents::sn_person_answer_moves_scope`).

10. **The description describes the head.**
   On every push the description is rebuilt from the head's facts
   (`description::build`; Problem, Changes, Risk, Verification). The written
   text is held to the head and the ledger (`description_facts::hold_scope`):
   - sections about earlier attempts, pushes and waits go; the revert and
     "CI is failing" lines stay for `ci_lines`, which drops them once untrue;
   - a sentence saying something is out of scope, untouched or put back while
     the diff changes it goes (`scope_ledger::contradictions`), and a change
     to a file not in the diff goes (`pr_claims`);
   - open product questions become out-of-scope defaults;
   - the written notes fit a length scaled to the changed lines of code, tests
     and generated files left out (150 to 600 words), with the run page for
     the rest.
   Changes ends with one Out of scope line rendered from the ledger. An item
   the diff brings back is never shown as out of scope; it is listed as not
   verified until it is taken out or moved in. `pr_body::defects` fails a body
   that asks an open product question.
   (`incidents::sm_description_of_the_head`)

11. **A bug is reproduced before it is planned.**
   For a bug, the planner has `reproduce_on_base` (`passes::reproduce`). The
   tool runs one call or command in the sandbox on the base branch's head, in
   the run's test environment, and returns its exit code and output. Each run
   is kept.

   design.md has a `## Reproduce` section (`common::repro_first::reproduce_gaps`)
   with these fields:
   - `Inputs:` lists the ticket's ids and names, with every stored variant a
     read-only lookup found, and says where they came from.
   - `Ran on base:` is a command this run ran with the tool.
   - `Output:` quotes what that command printed.
   - `Wrong line:` names the `path:line` where the existing code goes wrong.

   A run that tried and can't reproduce writes `Unverified:` with why. It is
   kept as unverified, never skipped silently.

   The fix goes in the mechanism the wrong line is in (`reuse_gaps`).
   `## Smallest change` names it, or says why it can't hold the fix. A plan
   that adds a new resolution path beside it (a keyword match, a new
   resolver) goes back.

   A question the plan decides with a default goes back unless it carries
   `Looked up: <what it showed>` or `No lookup settles it: <why>`. This
   applies when a page load or a query would answer it (`question_gaps`).

   After the build:
   - **Diff size.** A diff that changes more than 40 lines of code (map and
     list entries aside) for a ticket that names 1 to 12 records goes back to
     be narrowed (`minimal_change::whole_path`). A `Wider than the ticket:`
     line keeps it, with the reason.
   - **Fail on the base.** Each new test runs on the base and on the head the
     way CI runs it. That includes a CI loop such as `for t in tests/test_*.py;
     do python "$t"` (`TestEnv::file_command`, `habits_check::file_cmd`). A
     bug fix that no test was shown to fail on the base for says `Unverified:`
     in its description.

   (`incidents::rp_reproduce_before_planning`)

## What is deterministic

Everything above except the model's own writing: the searches, section checks,
word checks, definition parsing, diff checks and refusals are plain code with unit
tests. `incidents::bz_smallest_change` replays both pull requests through the plan
gate, the coder's guard, the habits check and the reviewer. The plans and diffs
as they were are sent back; the small ones pass.

## Sources

- Claude Code best practices: explore, then plan, then code; give the agent a way
  to verify; review against the spec. https://code.claude.com/docs/en/best-practices
- Claude prompting best practices: the minimum complexity the task needs; don't
  touch code you weren't asked to change; tests check correctness and don't define
  the solution.
  https://platform.claude.com/docs/en/build-with-claude/prompt-engineering/claude-prompting-best-practices
- Building effective agents: ground truth from the environment at each step; tools
  that are hard to misuse. https://www.anthropic.com/engineering/building-effective-agents
- SWE-agent: search tools, a bounded file viewer, and edits checked as they are
  made. https://arxiv.org/abs/2405.15793
- OpenHands: agents that browse and run code in a sandbox to see the real system.
  https://arxiv.org/abs/2407.16741
- Agentless: find the files, then the functions, then the lines, before editing;
  keep only reproduction tests that fail on the original code.
  https://arxiv.org/abs/2407.01489
- Aider: a repository map so the model reuses what exists; edits that send only
  the changed parts. https://aider.chat/docs/repomap.html,
  https://aider.chat/docs/more/edit-formats.html
- Google engineering practices: one self-contained change per CL; reviewers look
  for over-engineering and features nobody asked for.
  https://google.github.io/eng-practices/review/developer/small-cls.html,
  https://google.github.io/eng-practices/review/reviewer/looking-for.html
