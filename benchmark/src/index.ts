#!/usr/bin/env node
import { Command } from "commander";

import { getPackageInfo } from "@/utils/package-info";

process.on("SIGINT", () => process.exit(0));
process.on("SIGTERM", () => process.exit(0));

function main() {
  const packageInfo = getPackageInfo();
  const program = new Command()
    .name("Typesense CLI")
    .description(
      "Utilities for Typesense. Benchmark versions, build and install versions, run integration tests, etc.",
    )
    .version(
      packageInfo.version ?? "1.0.0",
      "-v, --version",
      "output the current version",
    )
    .helpCommand(true);

  // Check if no arguments were provided
  if (process.argv.length <= 2) {
    program.help();
  }

  program.parse(process.argv);
}

main();
