#!/usr/bin/env bun

import { Filters } from "./constants";
import { TypesenseTestRunner } from "./index";

async function main() {
  const args = process.argv.slice(2);
  const runner = TypesenseTestRunner.getInstance();

  try {
    // Clean data directories first
    runner['manager'].cleanDataDirs();

    // Run only multi-node tests
    if (args.length === 1 && args[0] === "--no-secrets") {
      await runner['multiServerTests']([Filters.SECRETS]);
    } else {
      await runner['multiServerTests']([]);
    }

    // Shutdown
    await runner['manager'].shutdown();
    process.exit(0);
  } catch (err) {
    await runner['manager'].shutdown();
    console.error("Multi-node tests failed:", err);
    process.exit(1);
  }
}

main();
