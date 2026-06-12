import { realpathSync } from 'node:fs';
import { dirname, join } from 'node:path';
import tailwindcss from '@tailwindcss/vite';
import { defineConfig } from 'vitest/config';
import { playwright } from '@vitest/browser-playwright';
import { sveltekit } from '@sveltejs/kit/vite';

// In a git worktree `node_modules` is symlinked to the main repo (see
// scripts/setup-worktree.sh). Vite resolves that symlink to its real path,
// which lies outside the worktree root and is otherwise blocked by
// `server.fs.allow`. Allowing the main repo root (the symlink target's parent)
// covers both the worktree and the shared node_modules; in the main repo it
// resolves to the project root itself, i.e. a no-op.
const mainRepoRoot = dirname(realpathSync(join(import.meta.dirname, 'node_modules')));

// Vite middleware to set cross-origin headers on every dev response.
// SvelteKit's hooks.server.js only runs for page/endpoint responses, not for
// Vite-served assets (static/, /@fs/, node_modules). Without these headers
// on `/`, the document isn't cross-origin isolated and `SharedArrayBuffer` is
// disabled — which breaks DuckDB-WASM's pthread workers and webR's fast
// channel. CORP=cross-origin on every asset (including same-origin assets,
// where it's harmless) keeps COEP=require-corp happy without us needing to
// audit each fetch path. In production, the static adapter relies on the
// hosting layer (or a worker) to apply the same headers — documented in
// README under "Deployment".
const crossOriginIsolationPlugin = {
	name: 'cross-origin-isolation-dev',
	configureServer(server) {
		server.middlewares.use((_req, res, next) => {
			res.setHeader('Cross-Origin-Opener-Policy', 'same-origin');
			res.setHeader('Cross-Origin-Embedder-Policy', 'require-corp');
			res.setHeader('Cross-Origin-Resource-Policy', 'cross-origin');
			next();
		});
	}
};

export default defineConfig({
	plugins: [crossOriginIsolationPlugin, tailwindcss(), sveltekit()],
	server: {
		fs: { allow: [mainRepoRoot] },
		// Disable the chokidar watcher when launched by the Playwright e2e web
		// server. On Linux CI, inotify-backed fs.watch opens one fd per file and
		// exceeds the container's ulimit -n -> EMFILE. e2e never edits files, so
		// the watcher is dead weight. Gated by env so normal `npm run dev` keeps HMR.
		watch: process.env.VITE_DISABLE_WATCH ? null : undefined
	},
	test: {
		expect: { requireAssertions: true },
		projects: [
			{
				extends: './vite.config.js',
				test: {
					name: 'client',
					browser: {
						enabled: true,
						provider: playwright(),
						instances: [{ browser: 'chromium', headless: true }]
					},
					include: ['src/**/*.svelte.{test,spec}.{js,ts}'],
					exclude: ['src/lib/server/**']
				}
			},

			{
				extends: './vite.config.js',
				test: {
					name: 'server',
					environment: 'node',
					include: ['src/**/*.{test,spec}.{js,ts}'],
					exclude: ['src/**/*.svelte.{test,spec}.{js,ts}']
				}
			}
		]
	}
});
