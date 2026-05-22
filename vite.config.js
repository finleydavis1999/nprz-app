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

export default defineConfig({
	plugins: [tailwindcss(), sveltekit()],
	server: {
		fs: { allow: [mainRepoRoot] }
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
