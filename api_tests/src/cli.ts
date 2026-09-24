#!/usr/bin/env bun

import { Filters } from "./constants";
import { TypesenseTestRunner } from "./index";

async function main() {
  const args = process.argv.slice(2);
  const runner = TypesenseTestRunner.getInstance();

  // Check for --no-secrets flag
  const noSecretsIndex = args.indexOf("--no-secrets");
  const filters: Filters[] = noSecretsIndex !== -1 ? [Filters.SECRETS] : [];

  // Check for file path argument (any argument that's not a flag and ends with .test.ts)
  const fileArg = args.find(arg => arg.endsWith(".test.ts") && !arg.startsWith("--"));
  const testFile = fileArg || null;

  await runner.run(filters, testFile);
}

main();