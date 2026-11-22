# App map

The App map shows how a team's app fits together across all its repositories. It is made with [Floorplan](https://github.com/haydenschmitty/floorplan) (MIT): its extractor, builder and job definitions are vendored at `vendor/floorplan`, and its viewer is vendored in the dashboard repository at `dashboard/vendor/floorplan`. CoderHelm adds its own adapter around them and never edits the vendored files.

## How a map is made

- `worker/src/passes/app_map.rs` runs Floorplan the way its `workflow.json` and `docs/unattended.md` describe a run without an agent session. One step per `app_map` message:
  - `start`: reads each repository's default branch head. When no head moved since the last map, nothing is redone. Otherwise it packs every repository side by side, Floorplan's files from the last run and the vendored release, and runs `services/worker/floorplan/driver.mjs` in the run sandbox (CodeBuild, where Node already is): a scan for a first map, Floorplan's own `refresh.mjs` after that.
  - The worker then does the model job Floorplan's result asks for (`architecture`, `update_architecture` or `descriptions`), with the job's own instructions, references and schema from `workflow.json`, the read-only tools it names over the same snapshot, and `submit`, whose answer is checked against the schema (`common::app_map::check_answer`). Architecture uses the team's primary model, descriptions the light model; every call is metered under feature `app_map`.
  - `build`: writes the answers and builds, until the build passes and every file and endpoint is described (the workflow's own limits: 4 and 3 attempts).
- After a push to a default branch (a code graph update), a team that has a map gets an update queued two minutes later. Floorplan's refresh keeps every description whose file didn't change, so only new or changed files and endpoints are described again.
- The map (`floorplan/graph@1`) is kept in the bucket at `app-map/{team}/graph.json.gz`, Floorplan's own files at `state.tgz`, and the step's job at `job.json`; the build status is in the settings table (`AM#{team}`, `STATUS`).

## What CoderHelm adds (`common/src/app_map.rs`)

- Recent work and Health, put into the map's own `recent` and `health.issues` fields when the gateway serves it, so Floorplan's viewer shows them natively. CoderHelm's findings are marked `source: "coderhelm"` and link to their pages; Floorplan's own checks are marked `Floorplan check`, and one at the same file and line as a CoderHelm finding is dropped.
- The page's copy: read-only (Floorplan's exported mode), with the sandbox's folder left out.
- Lookups for the read API and Ask (`/api/app-map/part`, `file`, `screen`, `search`, and `POST /api/app-map/ask`, which runs Chat's agent with those lookups).

## Bumping Floorplan

1. `scripts/update-floorplan.sh floorplan--vX.Y.Z` here, and `dashboard/scripts/update-floorplan.sh floorplan--vX.Y.Z` in the dashboard repository (the same release). Each clones the tag, checks every copied file against the release's `CHECKSUMS.txt`, copies only the files used, keeps the licenses and records the tag in `VERSION`.
2. `node --test services/worker/floorplan/driver.test.mjs` runs the driver on the made-up harbor fixture with the new release. If the map's shape changed, it fails; regenerate the fixture with `node services/worker/floorplan/make-fixture.mjs` and fix the adapter until `cargo test -p common app_map` and `cargo test -p gateway app_map` pass.
3. In the dashboard: `npm run floorplan` and `npx playwright test e2e/app-map.spec.ts`.

CI runs the driver test on every pull request.
