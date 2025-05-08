import { createEnv } from "@t3-oss/env-core";
import { expect, test } from "vitest";
import { z } from "zod";

test("environment validation fails when required variables are missing", () => {
  // Test with empty env
  expect(() => {
    createEnv({
      clientPrefix: "TYPESENSE_",
      client: {
        TYPESENSE_BINARY: z.string(),
        TYPESENSE_WORKING_DIRECTORY: z.string(),
        TYPESENSE_SNAPSHOT_PATH: z.string().optional(),
      },
      runtimeEnv: {},
    });
  }).toThrow();

  // Test with only one required variable
  expect(() => {
    createEnv({
      clientPrefix: "TYPESENSE_",
      client: {
        TYPESENSE_BINARY: z.string(),
        TYPESENSE_WORKING_DIRECTORY: z.string(),
        TYPESENSE_SNAPSHOT_PATH: z.string().optional(),
      },
      runtimeEnv: {
        TYPESENSE_BINARY: "/path/to/binary",
      },
    });
  }).toThrow();

  // Test with all required variables
  expect(() => {
    createEnv({
      clientPrefix: "TYPESENSE_",
      client: {
        TYPESENSE_BINARY: z.string(),
        TYPESENSE_WORKING_DIRECTORY: z.string(),
        TYPESENSE_SNAPSHOT_PATH: z.string().optional(),
      },
      runtimeEnv: {
        TYPESENSE_BINARY: "/path/to/binary",
        TYPESENSE_WORKING_DIRECTORY: "/path/to/dir",
      },
    });
  }).not.toThrow();

  // Test with all variables including optional
  expect(() => {
    createEnv({
      clientPrefix: "TYPESENSE_",
      client: {
        TYPESENSE_BINARY: z.string(),
        TYPESENSE_WORKING_DIRECTORY: z.string(),
        TYPESENSE_SNAPSHOT_PATH: z.string().optional(),
      },
      runtimeEnv: {
        TYPESENSE_BINARY: "/path/to/binary",
        TYPESENSE_WORKING_DIRECTORY: "/path/to/dir",
        TYPESENSE_SNAPSHOT_PATH: "/path/to/snapshot",
      },
    });
  }).not.toThrow();
});
