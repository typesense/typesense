#!/usr/bin/env bun

import { Filters } from "./constants";
import { TypesenseTestRunner } from "./index";

async function main() {
  const args = process.argv.slice(2);
  const runner = TypesenseTestRunner.getInstance();
  if (args.length === 1 && args[0] === "--no-secrets") {
    await runner.run([Filters.SECRETS]);
  } else {
    await runner.run([]);
  }
}

main();