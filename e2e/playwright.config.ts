import { defineConfig } from "@playwright/test";

// Needs the stack from ./scripts/demo.sh (frontend on 5190).
export default defineConfig({
  testDir: ".",
  timeout: 90_000,
  expect: { timeout: 20_000 },
  workers: 1,
  reporter: [["list"]],
  use: { baseURL: process.env.E2E_BASE_URL ?? "http://127.0.0.1:5190", headless: true, viewport: { width: 1280, height: 900 } },
});
