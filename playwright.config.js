import { createHash } from 'node:crypto';
import { defineConfig } from '@playwright/test';

// Per-worktree dev-server port. A fixed port collides across git worktrees:
// with `reuseExistingServer` on, a run in one worktree silently reuses another
// worktree's dev server and tests the wrong code. Hashing the worktree path
// gives each checkout its own stable port (range 20000-39999, clear of 5173
// and the OS ephemeral range), so worktrees can run e2e independently.
const portHash = createHash('sha1')
	.update(import.meta.dirname)
	.digest('hex');
const PORT = 20000 + (parseInt(portHash.slice(0, 8), 16) % 20000);

export default defineConfig({
	testDir: 'tests/e2e',
	testMatch: '**/*.e2e.{ts,js}',
	fullyParallel: true,
	// Each Playwright worker gets its own browser context — own OPFS, DuckDB-WASM
	// instance and localStorage — so tests parallelise without sharing state.
	// Capped so N concurrent WASM query engines don't thrash the CPU.
	workers: process.env.CI ? 2 : 4,
	use: {
		baseURL: `http://localhost:${PORT}`,
		trace: 'retain-on-failure'
	},
	webServer: {
		command: `vite dev --port ${PORT} --strictPort`,
		url: `http://localhost:${PORT}`,
		reuseExistingServer: !process.env.CI,
		timeout: 60_000,
		env: {
			VITE_DISABLE_WATCH: '1', // no HMR needed for e2e; avoids EMFILE on Linux CI
			ORIGIN: `http://localhost:${PORT}`,
			BETTER_AUTH_SECRET: 'dev-secret-32-chars-padding-okok',
			DATABASE_URL: 'local.db',
			AUTH_DISABLED: 'true', // e2e bypasses login
			PUBLIC_PROTOMAPS_API_KEY: process.env.PUBLIC_PROTOMAPS_API_KEY ?? 'b20f1204b39252e6'
		}
	}
});
