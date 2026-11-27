# bz. `bz_smallest_change`

**What happened:** Two pull requests did far more than their tickets, and guessed where they should have measured.

- **The tours ticket:** a data-pipeline ticket asked to link four tours to a second activity.
  - The plan named the right overrides map, then reused the link helper that deletes every link it isn't given, on a sync that only inserted. Links nobody asked about were deleted.
  - The build wrote a second map and helpers beside the existing one.
  - It added counters, warnings and a log level change in shared code, because the data rules asked for reporting on any skip.
  - It added a test hundreds of lines long with fake hooks and an invented payload.
  - Every round reinstalled the test dependencies, many minutes of builds in one run.
- **The portal ticket:** a web ticket's vendor end-to-end tests were written before anyone looked at the vendor's page, then rewritten on guesses. A criterion ("no double scrollbars") was met by changing the test to expect the vendor's behaviour instead of changing the page.

**The guarantee:**

- **Before planning:** the ticket's own names are searched and the hits go to the planner.
- **The plan:** it writes Reuse, Must not change, Measure first, Smallest change and Proof.
  - The Reuse section says what each reused helper does to stored rows. This is read from the helper's body.
  - The plan is sent back when it proposes:
    - a structure parallel to an existing one;
    - a replacing helper on a path that only inserts;
    - deletes, overwrites, logging or reporting the ticket doesn't ask for;
    - outside-page behaviour with nothing measured;
    - a test change where a criterion says the product must change.
- **The coder:** its first write follows a search and names what it reuses. An edit that adds a twin of an existing map or helper is refused unless it says why.
- **After the build:** a diff wider than its ticket goes back to be made smaller. Only the ticket can excuse deletes where the base only added, or a payload no probe observed.
- **The reviewer:** it finds the same. A test bent to an outside page against a criterion is a required finding.
- **The test environment:** CI's install is found once per run, and every later sandbox build restores the cached install instead of installing again.
