// Session-singleton centroids cache.
//
// Centroids are static per-scale data shipped with the build (one JSON file
// per scale at /data/geo/centroids-<scale>.json, URL pinned through the
// manifest version hash). Every SIM / GWR fit used to call `fetch(url)` +
// `await res.json()` from scratch — costing a network round-trip + parse per
// fit, even though the data is identical for the whole session.
//
// This module caches the parsed object per scale, sharing an in-flight
// fetch promise so concurrent callers don't double-fetch. Same lazy lifetime
// as manifestState — load on first need, keep for the rest of the session.
//
// Usage:
//   import { centroidsState } from '$lib/state/centroids.svelte.js';
//   const centroids = await centroidsState.ensureLoaded(scale, url);
//   // centroids: Record<areaCode, [x, y]>  (RD meters)
//
// The URL is passed in because building it requires the manifest version
// hash (dataUrl(geo.centroidsRd, manifestState.data.version)) — keeping that
// computation in the caller means this module stays decoupled from the
// manifest singleton and trivially testable.

import { SvelteMap } from 'svelte/reactivity';

class CentroidsState {
	/** Parsed centroid objects per scale. Reactive only so consumers using
	 *  `centroidsState.data.get(scale)` in a derive can pick up the value
	 *  once loaded — in practice the awaited `ensureLoaded` path is enough.
	 *  @type {SvelteMap<string, Record<string, [number, number]>>} */
	data = new SvelteMap();
	/** In-flight fetches keyed by scale — concurrent callers share. Plain
	 *  Map (non-reactive) because we don't want renders waking up on
	 *  promise transitions. @type {Map<string, Promise<Record<string, [number, number]>>>} */
	#inflight = new Map();

	/**
	 * Lazy-load the centroids for a scale. First call fetches + parses;
	 * concurrent calls share the in-flight promise; subsequent calls after
	 * resolution return the cached object directly. Rejects only on a real
	 * fetch / parse failure — callers should bubble these up as fit errors.
	 *
	 * @param {string} scale
	 * @param {string} url
	 * @returns {Promise<Record<string, [number, number]>>}
	 */
	async ensureLoaded(scale, url) {
		const cached = this.data.get(scale);
		if (cached) return cached;
		const inflight = this.#inflight.get(scale);
		if (inflight) return inflight;
		const p = (async () => {
			const res = await fetch(url);
			if (!res.ok) throw new Error(`centroids: HTTP ${res.status}`);
			const parsed = /** @type {Record<string, [number, number]>} */ (await res.json());
			this.data.set(scale, parsed);
			return parsed;
		})();
		this.#inflight.set(scale, p);
		try {
			return await p;
		} finally {
			this.#inflight.delete(scale);
		}
	}

	/** Synchronous accessor — returns null if not yet loaded. Useful when a
	 *  caller wants to skip work if the data isn't there yet rather than
	 *  trigger a fetch. */
	get(scale) {
		return this.data.get(scale) ?? null;
	}
}

export const centroidsState = new CentroidsState();
