# Incident regression suite

Every run failure that reached a person is a case in
`services/worker/src/passes/incidents/`, so it can't come back without a
failing test. CI runs the suite as its own step, **Incident regressions** in **Rust (tests)**
(`cargo nextest run --workspace -E 'test(/incidents::/)'`; locally,
`cargo test --workspace -- incidents::` runs the same tests), on every pull
request. A case still running after two minutes fails under its own name
(`services/.config/nextest.toml`).

Each case replays the failure on one invented monorepo:

- `apps/studio`: the CMS studio, its schema, and a `typegen` script. The
  script writes `apps/studio/schema.json`, and writes the site's types
  through a `../web/...` path.
- `apps/web`: the site.
- `e2e/`: end-to-end specs, with a map of which spec tests which page.

The case runs the pipeline's own decision code. Only the edges are faked:

- GitHub: branches, compares and pull requests, in the shapes GitHub returns.
- The model: recorded judge replies, read by the real parser.
- The sandbox: a real tar.gz archive and a CodeBuild-shaped log, read by the
  real `codegen_outcome`.
- DynamoDB: run records as the table holds them.

Each case checks three things: the run's final status, the pull request and
notes the run leaves, and that the old failure message never appears. When a
decision is made inside the pipeline itself, the case also checks that the
pipeline still calls it there.

## Cases
Each case has its own page in [`docs/incidents/`](incidents/), named after
its module (`docs/incidents/<case>.md`): what happened, and the guarantee it
protects. A new case is two new files, the test in
`services/worker/src/passes/incidents/` and its page here, and nothing
shared is edited: `services/worker/build.rs` finds every case file, and a
test fails when a case has no page or a page no case. To print all cases as
one table, run `python3 .github/scripts/incidents_table.py`.


## Adding a case

When a fix lands for a new failure:

1. Add `services/worker/src/passes/incidents/<letter>_<name>.rs`. Start it with
   what happened and the guarantee, in plain words, with no customer names.
2. Build the scenario on the fixture monorepo (`fixture.rs`). Add any new edge
   to the fixture as a fake, never as a mock of the pipeline's own code.
3. Check the final status, what the run leaves, and that the old message never
   appears (`Ending::never_says`).
4. Add its page, `docs/incidents/<letter>_<name>.md`, with the case as its
   title, then **What happened:** and **The guarantee:**. Nothing else is
   edited: no `mod` line, no table row.
