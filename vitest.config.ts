import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["test/**/*.test.ts"],
    globalSetup: ["test/helpers/global-setup.ts"],
    env: { LOG_LEVEL: "error" },
    testTimeout: 60_000,
    hookTimeout: 60_000,
    pool: "forks",
    poolOptions: { forks: { maxForks: 3, minForks: 1 } },
  },
});
