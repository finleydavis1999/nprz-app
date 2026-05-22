// Web Worker: builds the neighbour index and runs spatial-lag smoothing off the
// main thread so live decay/distance sliders stay at 60fps. Centroids and the
// neighbour index are cached in worker memory — the index is keyed by
// (scale, maxDist), so changing only the decay / kernel / mode re-runs just the
// cheap weighting pass.
import { buildNeighborIndex, smooth } from './spatial-lag.js';

/** @type {Map<string, Record<string, [number, number]>>} */
const centroidsByScale = new Map();
/** @type {Map<string, Map<string, Array<{ code: string, dist: number }>>>} */
const indexCache = new Map();

self.onmessage = async (e) => {
	const { reqId, scale, centroidsUrl, maxDist, kernel, decay, mode, includeSelf, values } = e.data;
	try {
		let cs = centroidsByScale.get(scale);
		if (!cs) {
			const res = await fetch(centroidsUrl);
			if (!res.ok) throw new Error(`centroids: HTTP ${res.status}`);
			cs = await res.json();
			centroidsByScale.set(scale, cs);
		}
		const key = `${scale}|${maxDist}`;
		let idx = indexCache.get(key);
		if (!idx) {
			idx = buildNeighborIndex(cs, maxDist);
			indexCache.set(key, idx);
		}
		const out = smooth(idx, new Map(Object.entries(values)), { kernel, decay, mode, includeSelf });
		self.postMessage({ reqId, result: Object.fromEntries(out) });
	} catch (err) {
		self.postMessage({ reqId, error: err?.message ?? String(err) });
	}
};
