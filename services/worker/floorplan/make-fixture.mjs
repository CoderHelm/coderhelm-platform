// Writes fixtures/harbor-graph.json and fixtures/marina-graph.json, the maps the Rust adapter's tests read,
// from the vendored Floorplan release. Run it after scripts/update-floorplan.sh
// when driver.test.mjs says the map's shape changed:
//   node services/worker/floorplan/make-fixture.mjs
import fs from 'node:fs';
import path from 'node:path';
import { HERE, drive, harborAnswers, marinaAnswers, stable, workdir } from './harness.mjs';

const { graph } = drive(workdir(harborAnswers(), 'start'));
fs.writeFileSync(path.join(HERE, 'fixtures', 'harbor-graph.json'), JSON.stringify(stable(graph), null, 1) + '\n');
console.log('wrote fixtures/harbor-graph.json');
const marina = drive(workdir(marinaAnswers(), 'start', {}, 'marina')).graph;
fs.writeFileSync(path.join(HERE, 'fixtures', 'marina-graph.json'), JSON.stringify(stable(marina), null, 1) + '\n');
console.log('wrote fixtures/marina-graph.json');
