// Spatial lag / gravitational smoothing — pure compute, no runes, no DOM.
// Imported by the spatial-lag Web Worker, and unit-tested directly.
//
// For each node i the smoothed value is a distance-decay-weighted aggregate of
// an input variable over nearby nodes j within `maxDist`:
//   mean: out_i = Σ w_ij·x_j / Σ w_ij   (row-standardised spatial lag)
//   sum:  out_i = Σ w_ij·x_j            (gravitational potential)
// Distances are planar Euclidean over RD (EPSG:28992) centroids in metres —
// true ground distance for the Netherlands, no haversine needed.

// Uniform-grid spatial index. Cell size = maxDist, so every node within
// `maxDist` of node i sits in i's cell or one of the 8 adjacent cells.
// O(N) to build, O(N·k) to query (k = mean neighbours within maxDist).
//
// centroids: Record<area_code, [easting, northing]>  (RD metres)
// returns:   Map<area_code, Array<{ code, dist }>>   (self excluded)
export function buildNeighborIndex(centroids, maxDist) {
	/** @type {Array<{ code: string, x: number, y: number }>} */
	const pts = [];
	for (const [code, xy] of Object.entries(centroids)) {
		pts.push({ code, x: xy[0], y: xy[1] });
	}
	const cell = maxDist;
	/** @type {Map<string, number[]>} */
	const grid = new Map();
	pts.forEach((p, i) => {
		const key = `${Math.floor(p.x / cell)},${Math.floor(p.y / cell)}`;
		let bucket = grid.get(key);
		if (!bucket) {
			bucket = [];
			grid.set(key, bucket);
		}
		bucket.push(i);
	});

	/** @type {Map<string, Array<{ code: string, dist: number }>>} */
	const index = new Map();
	const max2 = maxDist * maxDist;
	for (const p of pts) {
		const cx = Math.floor(p.x / cell);
		const cy = Math.floor(p.y / cell);
		const near = [];
		for (let dx = -1; dx <= 1; dx++) {
			for (let dy = -1; dy <= 1; dy++) {
				const bucket = grid.get(`${cx + dx},${cy + dy}`);
				if (!bucket) continue;
				for (const j of bucket) {
					const q = pts[j];
					if (q.code === p.code) continue;
					const ex = p.x - q.x;
					const ey = p.y - q.y;
					const d2 = ex * ex + ey * ey;
					if (d2 <= max2) near.push({ code: q.code, dist: Math.sqrt(d2) });
				}
			}
		}
		index.set(p.code, near);
	}
	return index;
}

// Distance-decay weight. `d` in metres; `p` is the decay parameter — a length
// scale (d0, metres) for exp/gauss, a positive exponent (β) for power. The d=0
// self term is added by `smooth()` with weight 1, so power at d=0 returns 0
// here rather than Infinity.
export function kernelWeight(kernel, d, p) {
	if (kernel === 'exp') return Math.exp(-d / p);
	if (kernel === 'gauss') {
		const r = d / p;
		return Math.exp(-r * r);
	}
	// power: d^(-β)
	return d > 0 ? Math.pow(d, -p) : 0;
}

// Smooth `values` over `neighborIndex`.
//   neighborIndex: from buildNeighborIndex
//   values:        Map<area_code, number>  (the input layer result)
//   opts:          { kernel, decay, mode: 'mean' | 'sum', includeSelf }
// Returns Map<area_code, number>. Nodes with no finite contributor are omitted,
// matching the layer calculator's skip-on-undefined convention.
export function smooth(neighborIndex, values, { kernel, decay, mode, includeSelf }) {
	const out = new Map();
	for (const [code, neighbors] of neighborIndex) {
		let num = 0;
		let den = 0;
		let any = false;
		if (includeSelf) {
			const xi = values.get(code);
			if (xi != null && Number.isFinite(xi)) {
				// self distance is 0 → weight 1 for every kernel
				num += xi;
				den += 1;
				any = true;
			}
		}
		for (const { code: j, dist } of neighbors) {
			const xj = values.get(j);
			if (xj == null || !Number.isFinite(xj)) continue;
			const w = kernelWeight(kernel, dist, decay);
			if (!Number.isFinite(w) || w === 0) continue;
			num += w * xj;
			den += w;
			any = true;
		}
		if (!any) continue;
		if (mode === 'mean') {
			if (den > 0) out.set(code, num / den);
		} else {
			out.set(code, num);
		}
	}
	return out;
}
