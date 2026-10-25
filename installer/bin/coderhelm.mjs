#!/usr/bin/env node
import { main } from "../lib/cli.mjs";

main(process.argv.slice(2)).then(
  (code) => process.exit(code ?? 0),
  (err) => {
    console.error(`\n${err.message || err}`);
    if (process.env.CODERHELM_DEBUG) console.error(err.stack);
    process.exit(1);
  },
);
