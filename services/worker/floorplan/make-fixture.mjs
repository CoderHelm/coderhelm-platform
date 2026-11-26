// Writes fixtures/harbor-graph.json and fixtures/library-graph.json, the maps the Rust adapter's tests read,
// from the vendored Floorplan release. Run it after scripts/update-floorplan.sh
// when driver.test.mjs says the map's shape changed:
//   node services/worker/floorplan/make-fixture.mjs
import fs from 'node:fs';
import path from 'node:path';
import { HERE, drive, harborAnswers, libraryAnswers, stable, workdir } from './harness.mjs';

const { graph } = drive(workdir(harborAnswers(), 'start'));
fs.writeFileSync(path.join(HERE, 'fixtures', 'harbor-graph.json'), JSON.stringify(stable(graph), null, 1) + '\n');
console.log('wrote fixtures/harbor-graph.json');
const library = drive(workdir(libraryAnswers(), 'start', {}, 'library')).graph;
fs.writeFileSync(path.join(HERE, 'fixtures', 'library-graph.json'), JSON.stringify(stable(library), null, 1) + '\n');
console.log('wrote fixtures/library-graph.json');
