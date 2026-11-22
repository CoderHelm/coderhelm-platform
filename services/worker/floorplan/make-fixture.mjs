// Writes fixtures/harbor-graph.json, the map the Rust adapter's tests read,
// from the vendored Floorplan release. Run it after scripts/update-floorplan.sh
// when driver.test.mjs says the map's shape changed:
//   node services/worker/floorplan/make-fixture.mjs
import fs from 'node:fs';
import path from 'node:path';
import { HERE, drive, harborAnswers, stable, workdir } from './harness.mjs';

const { graph } = drive(workdir(harborAnswers(), 'start'));
fs.writeFileSync(path.join(HERE, 'fixtures', 'harbor-graph.json'), JSON.stringify(stable(graph), null, 1) + '\n');
console.log('wrote fixtures/harbor-graph.json');
