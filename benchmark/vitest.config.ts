import { defineConfig } from "vitest/config";

import "dotenv/config";

import path from "path";

export default defineConfig({
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  test: {
    environment: "node",
    hookTimeout: 180000,
    testTimeout: 180000,
    fileParallelism: false,
    env: {
      LOG_LEVEL: "4",
    },
    sequence: {
      shuffle: false,
      concurrent: false,
    },
    globalSetup: ["./test/setup.ts"],
  },
});
