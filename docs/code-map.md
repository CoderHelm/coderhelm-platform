# The code map

What CoderHelm knows about each repository's code, kept current with its
default branch, so the planner and the coder start from it instead of
exploring the layout again on every run.

Code: `services/common/src/code_map.rs` (the rules, pure) and
`services/worker/src/passes/code_map.rs` (reading, writing, recording).

## What it holds

The map is kept on the repository's agent file record (settings table,
`sk = AGENTS#REPO#<owner/repo>`, attribute `code_map`), beside the agent
file. It is made of sections:

| Section | What it says | Read from |
| --- | --- | --- |
| `architecture` | What the repository is for, its main parts, how requests and jobs flow, where each part is deployed | README, agent guidance, infrastructure roots |
| `stack` | Languages, frameworks, runtimes and the versions the manifests pin | Top-level manifests and toolchain files |
| `build` | Install, build, type-check, lint, test (each suite) and run commands, and what CI runs | CI workflows, manifests, test and lint config |
| `conventions` | Style, lint and format rules, naming, error handling, logging, test layout, guidance and the pull request template | Agent guidance, tooling config, the template |
| `dependencies` | Which packages, services and infrastructure depend on which | Every package manifest up to two levels deep, compose files |
| `area:<folder>` | One per top-level folder (at most 12): what it holds, its packages and entry points by path, where each concern lives | The folder's listing, its manifests and entry points |

Each section is stamped with the commit it describes. The map is stamped
with the commit and the tree it was last brought to.

## Kept current, section by section

Before the planner starts, the map is brought to the run's base:

1. **Same commit, or same tree.** Every section is reused. No tree is read and
   no model is called.
2. **Otherwise** GitHub's comparison from the map's commit to the base gives
   the changed paths, and each path marks the sections it touches:
   - a top-level manifest touches `stack`, `build` and `dependencies`;
   - a package manifest deeper in the tree touches `stack`, `dependencies`
     and its area;
   - CI touches `build`;
   - lint and format config touch `conventions` and `build`;
   - guidance and the pull request template touch `conventions`;
   - the README touches `architecture`;
   - infrastructure (Terraform, CDK, CloudFormation, Kubernetes, compose)
     touches `architecture` and `dependencies`;
   - a file added, removed or renamed in an area, or an entry point
     (`main`, `index`, `lib`, `app`, `server`, `handler`, route tables)
     changed there, touches that area;
   - an area added or gone touches `architecture`.

   A plain edit inside a source file touches nothing: the map describes
   structure, not every line.
3. **A comparison that can't be read, or that GitHub cut** (300 files or
   more), rebuilds every section.

Only the touched sections are read again (their own files, at most ten each)
and written, all in one model call. The rest are kept. A section the reply
left out keeps its older stamp. The map tells the agents it may be out of
date, and the next run rebuilds it.

## Who reads it

- **The planner** gets the map in its system prompt, after the repository's
  instruction files.
- **The coder** gets the stored map in every round's system prompt. The coder
  reads the map without refreshing it, so its prompt stays the same across a
  run's rounds.

## What it saves, on the run

Two run attributes, served by `GET /api/runs/{run_id}` under `exploration`:

- `code_map_refresh` (`exploration.code_map`): the commit the map was
  brought to, why (`new`, `current`, `changed`, `unknown changes`), and the
  sections reused, rebuilt and missed.
- `exploration` (`exploration.coder`): the build's tool calls before its
  first write (`calls_before_first_write`), all its tool calls, and the
  map's commit when it started from one (`map_commit`). Comparing runs with
  and without a `map_commit` shows how much exploration the map saves.

The replay case is `services/worker/src/passes/incidents/dg_code_map_kept_current.rs`.
