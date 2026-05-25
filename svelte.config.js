import adapter from '@sveltejs/adapter-static';

/** @type {import('@sveltejs/kit').Config} */
const config = {
	compilerOptions: {
		// Force runes mode for the project, except for libraries. Can be removed in svelte 6.
		runes: ({ filename }) => (filename.split(/[/\\]/).includes('node_modules') ? undefined : true)
	},
	kit: {
		adapter: adapter({ fallback: 'index.html' }),
		paths: { base: process.env.BASE_PATH ?? '' },
		// Auto-registration runs in dev too, where the SW does nothing useful
		// (no immutable build assets to cache) but its fetch handler interferes
		// with concurrent OPFS streaming under cross-origin isolation — Playwright
		// + headless Chromium hangs OPFS writes for the parquet pipeline. We
		// register manually in `+layout.svelte` for production only.
		serviceWorker: { register: false },
		typescript: {
			config: (config) => ({
				...config,
				include: [...config.include, '../drizzle.config.js']
			})
		}
	}
};

export default config;
