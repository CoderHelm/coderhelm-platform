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

   Each new or changed test runs in the sandbox with and without the
   change (`habits_check`). A test the runner finds nothing in goes back to the
   coder, and the coder quotes the run's output in its Evidence. End-to-end code
   against another company's page uses only what a probe observed (#734).

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
