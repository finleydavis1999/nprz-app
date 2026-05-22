// Background-prefetch every parquet referenced by the manifest into OPFS, so
// switching dataset / scale doesn't block on a fresh download. Runs once per
// session after the page is idle (the first active query already kicked off
// its own fetch via `ensureRegistered` — we just fill in the rest).
//
// We call `getOrFetch` directly rather than `ensureRegistered` to avoid
// pre-registering 14 file handles in DuckDB the user may never query; the
// later `ensureRegistered` call is essentially free once OPFS has the file.
import { browser } from '$app/environment';
import { loadManifest } from './manifest.js';
import { initCache, getOrFetch } from './opfs-cache.js';
import { dataUrl } from './url.js';

let started = false;

export function schedulePrefetch() {
	// e2e disables the prefetch (set via Playwright addInitScript) so tests only
	// fetch the datasets they query — the full ~289 MB sweep otherwise starves
	// the active test query for bandwidth/OPFS.
	if (!browser || started || globalThis.__E2E_NO_PREFETCH__) return;
	started = true;
	const start = () => {
		prefetchAll().catch(() => {});
	};
	if (typeof requestIdleCallback === 'function') {
		requestIdleCallback(start, { timeout: 10_000 });
	} else {
		setTimeout(start, 3_000);
	}
}

async function prefetchAll() {
	const manifest = await loadManifest();
	const versionRoot = await initCache(manifest.version);
	for (const relPath of collectParquetPaths(manifest)) {
		try {
			await getOrFetch(versionRoot, relPath, dataUrl(relPath, manifest.version));
		} catch {
			// Best-effort — a failed prefetch shouldn't surface to the user.
		}
	}
}

function collectParquetPaths(/** @type {any} */ manifest) {
	/** @type {string[]} */
	const out = [];
	for (const section of ['datasets', 'flows']) {
		const entries = manifest[section] ?? {};
		for (const ds of Object.values(entries)) {
			const scales = /** @type {any} */ (ds).scales ?? {};
			for (const path of Object.values(scales)) {
				if (typeof path === 'string') out.push(path);
			}
		}
	}
	return out;
}
