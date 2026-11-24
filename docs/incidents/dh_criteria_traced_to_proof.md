# dh. `dh_criteria_traced_to_proof`

**What happened:** A ticket had two acceptance criteria: one filter chip per dock on a slips page, and the page view tracked there. The pull request changed the chip code and added an end-to-end spec for the chip, and its two-way proof passed. Its description listed the files and the checks, but nothing tied either one to the criteria. The tracking criterion had changed code and no test, and neither the description, the run page nor the API said so.

**The guarantee:** Every build of the description traces each criterion (the ticket's, else the plan's criteria map) to three things, all in `common::trace`:

- the files and definitions changed for it (the map line's `code:`, else matched by its words);
- the tests that cover it;
- its proof on the head: the two-way end-to-end proof with the served-page check, else its tests' runs with and without the change.

The table sits in the description's checks and is rebuilt from the facts on every push. A criterion with no traced code, no test, or no proof that holds is listed as an open item. The trace is kept on the run (`traceability`) and served by `GET /api/runs/{run_id}`.
