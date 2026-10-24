import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    // The suites under src/__tests__ exercise pure functions (bot detection,
    // referrer classification, payload validation); they need no Workers
    // runtime emulation. Node 22 provides URL, Request/Response, TextEncoder
    // and crypto.subtle natively. The old "miniflare" environment came from
    // vitest-environment-miniflare, a deprecated Miniflare v2 package with no
    // maintained release and the sole source of several Dependabot alerts.
    environment: "node",
    globals: true,
    coverage: {
      provider: "v8",
      reporter: ["text", "json", "html"],
      exclude: ["node_modules/", "dist/", "**/*.test.ts"],
      thresholds: {
        statements: 60,
        branches: 60,
        functions: 60,
        lines: 60,
      },
    },
  },
});
