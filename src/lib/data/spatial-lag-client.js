// Main-thread client for the spatial-lag Web Worker. Lazily creates the worker
// (so SSR never touches `Worker`), correlates responses by request id, and
// resolves each call with a Map<area_code, number>.
/** @type {Worker | null} */
let worker = null;
let nextReqId = 1;
/** @type {Map<number, { resolve: (v: Map<string, number>) => void, reject: (e: Error) => void }>} */
const pending = new Map();

function getWorker() {
	if (worker) return worker;
	worker = new Worker(new URL('./spatial-lag.worker.js', import.meta.url), { type: 'module' });
	worker.onmessage = (e) => {
		const { reqId, result, error } = e.data;
		const p = pending.get(reqId);
		if (!p) return;
		pending.delete(reqId);
		if (error) p.reject(new Error(error));
		else p.resolve(new Map(Object.entries(result)));
	};
	worker.onerror = (e) => {
		const msg = e.message ?? 'spatial-lag worker error';
		for (const p of pending.values()) p.reject(new Error(msg));
		pending.clear();
	};
	return worker;
}

// opts: { scale, centroidsUrl, maxDist, kernel, decay, mode, includeSelf, values }
//   `values` is a plain object { area_code: number }.
// Returns Promise<Map<string, number>>.
export function computeSmooth(opts) {
	const w = getWorker();
	const reqId = nextReqId++;
	return new Promise((resolve, reject) => {
		pending.set(reqId, { resolve, reject });
		w.postMessage({ reqId, ...opts });
	});
}
