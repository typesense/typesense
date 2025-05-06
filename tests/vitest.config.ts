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
    env: {
      LOG_LEVEL: "4",
    },
  },
});
