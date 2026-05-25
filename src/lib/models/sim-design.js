// Build a SIM (spatial interaction model) design matrix from a flow layer +
// origin-mass + destination-mass node layers + RD centroids.
//
// Phase 2 vertical-slice scope:
//  - Three constraint modes: 'none' (unconstrained gravity), 'production'
//    (origin balancing factors absorb mass_o → only distance + mass_d in the
//    continuous part), 'attraction' (mirror, dest absorbs). Doubly-constrained
//    is intentionally out of scope; documented in the plan.
//  - Two iteration modes via `expandToAllOD`:
//      false (default): observed flows only. Cheap, but the Poisson MLE is
//        biased low for β_distance (a zero-truncated Poisson effect).
//      true: cartesian product of (origins in massO × destinations in massD),
//        with y=0 for absent OD pairs. The textbook unbiased gravity estimate.
//        Costs O(N²) memory at full coverage — fine at gemeente (342²≈117K),
//        gets heavy at PC4 (~16M); UI should warn.
//  - Distance is always log-transformed (the deterrence function); masses get
//    user-selectable transforms (default log) for consistency with the NLM
//    transform machinery in design-matrix.js.
//
// Rows with missing or non-finite columns drop, same convention as
// `buildNodeDesignMatrix`. Self-loops (o === d) drop by default — they have
// distance 0, log(0) = -Infinity. The caller can flip `includeSelfLoops` if
// they've pre-handled the zero-distance case (e.g. by intra-zonal averaging).

import { decorateName } from './design-matrix.js';
import { kernelWeight } from '$lib/data/spatial-lag.js';

/** @typedef {'none' | 'log' | 'log1p' | 'sqrt'} Transform */
/** @typedef {{
 *   o: string[], d: string[],
 *   columns: Record<string, Float64Array>,
 *   edgeKeys: string[]
 * }} SimDesignMatrix */

function applyTransform(transform, v) {
	if (transform === 'log') return Math.log(v);
	if (transform === 'log1p') return Math.log1p(v);
	if (transform === 'sqrt') return Math.sqrt(v);
	return v;
}

/**
 * @param {object} args
 * @param {Map<string, number>} args.flows          OD flow counts; keys are
 *   "o|d" (matches the flowEdgeKey convention in layers.svelte.js).
 * @param {string} args.flowName                    Slug of the flow layer; used
 *   for the dependent column name (`y` placeholder if omitted).
 * @param {Map<string, number>} args.massO          Origin-mass values keyed by
 *   area_code.
 * @param {string} args.massOName                   Slug of the origin-mass layer.
 * @param {Transform} [args.massOTransform]         Default 'log'.
 * @param {Map<string, number>} args.massD          Destination-mass values.
 * @param {string} args.massDName                   Slug of the dest-mass layer.
 * @param {Transform} [args.massDTransform]         Default 'log'.
 * @param {Record<string, [number, number]>} args.centroids  RD centroids
 *   keyed by area_code. Distance is Euclidean / 1000 → km.
 * @param {boolean} [args.includeSelfLoops]         Drop o===d by default.
 * @param {boolean} [args.expandToAllOD]            When true, iterate
 *   `massO.keys() × massD.keys()` instead of `flows.keys()`, filling absent
 *   pairs with y=0. Unbiased β estimate; O(|massO|·|massD|) memory.
 * @param {'none' | 'production' | 'attraction'} [args.constraint]
 *   Controls which continuous mass column ends up in the design. Production
 *   absorbs mass_o into per-origin fixed effects (added in R via
 *   sparse.model.matrix), so we omit it from the columns dict; mirror for
 *   attraction.
 * @param {{ kernel: 'exp'|'gauss'|'power', decay: number } | null} [args.compDest]
 *   When set, add a `log1p(comp_dest)` column. Competing-destinations is a
 *   per-destination metric: cd_j = Σ_{k≠j} mass_k · f(d_jk) where f is the
 *   chosen kernel of the distance between destinations j and k. Broadcast
 *   per-OD by destination code.
 * @param {boolean} [args.radiation]
 *   When true, add a `log1p(radiation)` column. Radiation is per-OD:
 *   r_ij = cumulative mass of destinations within distance d_ij of origin i
 *   (excluding i and j). The classic Simini-Maritan deterrence term.
 * @returns {SimDesignMatrix}
 */
export function buildSimDesignMatrix({
	flows,
	flowName,
	massO,
	massOName,
	massOTransform = 'log',
	massD,
	massDName,
	massDTransform = 'log',
	centroids,
	includeSelfLoops = false,
	expandToAllOD = false,
	constraint = 'none',
	compDest = null,
	radiation = false
}) {
	if (!flows || (flows.size === 0 && !expandToAllOD)) {
		// In expand mode an empty flow map is meaningful: a pure "potential
		// trips" gravity fit where every OD pair gets y=0. Without expand it
		// just means the user gave us nothing to fit.
		throw new Error('Flow layer has no values');
	}
	if (!centroids) throw new Error('SIM needs RD centroids for the active scale');

	const yKey = flowName ?? 'y';
	// Distance is *always* logged in a gravity SIM — it's the deterrence term.
	const distKey = 'log(distance_km)';
	const oKey = decorateName(`${massOName ?? 'mass_o'}_o`, massOTransform);
	const dKey = decorateName(`${massDName ?? 'mass_d'}_d`, massDTransform);
	// Production absorbs origin mass into per-origin balancing factors built
	// on the R side; attraction does the same for destinations. Skip the
	// absorbed column from the design entirely so we don't ship redundant
	// numbers across the bridge or confuse the coefficient table.
	const includeOMass = constraint !== 'production';
	const includeDMass = constraint !== 'attraction';

	/** @type {string[]} */
	const o = [];
	/** @type {string[]} */
	const d = [];
	/** @type {string[]} */
	const edgeKeys = [];
	/** @type {number[]} */
	const yVals = [];
	/** @type {number[]} */
	const distVals = [];
	/** @type {number[]} */
	const oMassVals = [];
	/** @type {number[]} */
	const dMassVals = [];

	/** Try to add one OD pair to the design. `count` may be 0 for padded
	 *  pairs in expand mode. Returns silently when any filter (self-loop,
	 *  missing centroid, missing mass, log-of-zero distance, non-finite
	 *  transformed mass) drops the row. */
	function tryAdd(oi, di, count) {
		if (!Number.isFinite(count)) return;
		if (!includeSelfLoops && oi === di) return;

		const cO = centroids[oi];
		const cD = centroids[di];
		if (!cO || !cD) return;

		const dx = cO[0] - cD[0];
		const dy = cO[1] - cD[1];
		const distKm = Math.sqrt(dx * dx + dy * dy) / 1000;
		const logDist = Math.log(distKm);
		if (!Number.isFinite(logDist)) return;

		// We need the mass only when its side isn't constrained away. For the
		// absorbed side we still require the area exist in massO/D so the
		// per-area fixed effect is well-defined.
		const moRaw = massO.get(oi);
		const mdRaw = massD.get(di);
		if (moRaw == null || mdRaw == null) return;
		let mo = 0;
		let md = 0;
		if (includeOMass) {
			mo = applyTransform(massOTransform, moRaw);
			if (!Number.isFinite(mo)) return;
		}
		if (includeDMass) {
			md = applyTransform(massDTransform, mdRaw);
			if (!Number.isFinite(md)) return;
		}

		o.push(oi);
		d.push(di);
		edgeKeys.push(`${oi}|${di}`);
		yVals.push(count);
		distVals.push(logDist);
		oMassVals.push(mo);
		dMassVals.push(md);
	}

	if (expandToAllOD) {
		// Full cartesian: every origin with a mass × every destination with a
		// mass. Pairs absent from `flows` get y=0, restoring the unbiased
		// Poisson MLE. Iteration order is mass-key order; the SIM fit doesn't
		// depend on row order, so we don't sort.
		for (const oi of massO.keys()) {
			for (const di of massD.keys()) {
				const obs = flows.get(`${oi}|${di}`);
				tryAdd(oi, di, obs != null && Number.isFinite(obs) ? obs : 0);
			}
		}
	} else {
		for (const [edgeKey, count] of flows) {
			if (count == null) continue;
			const sep = edgeKey.indexOf('|');
			if (sep < 0) continue;
			tryAdd(edgeKey.slice(0, sep), edgeKey.slice(sep + 1), count);
		}
	}

	const N = yVals.length;
	if (N === 0) {
		throw new Error(
			'No OD pairs survived: check that mass layers cover the origins/destinations and that distances are positive'
		);
	}

	/** @type {Record<string, Float64Array>} */
	const columns = {
		[yKey]: Float64Array.from(yVals),
		[distKey]: Float64Array.from(distVals)
	};
	if (includeOMass) columns[oKey] = Float64Array.from(oMassVals);
	if (includeDMass) columns[dKey] = Float64Array.from(dMassVals);

	// Competing destinations: per-destination summary, broadcast per-OD by d.
	// One cd_j per unique destination — pre-compute once then index by row.
	if (compDest) {
		const cdByDest = new Map();
		for (let r = 0; r < N; r++) {
			const j = d[r];
			if (cdByDest.has(j)) continue;
			const cj = centroids[j];
			if (!cj) {
				cdByDest.set(j, NaN);
				continue;
			}
			let s = 0;
			for (const [k, mkRaw] of massD) {
				if (k === j) continue;
				if (mkRaw == null || !Number.isFinite(mkRaw)) continue;
				const ck = centroids[k];
				if (!ck) continue;
				const dx = cj[0] - ck[0];
				const dy = cj[1] - ck[1];
				const dkm = Math.sqrt(dx * dx + dy * dy) / 1000;
				const w = kernelWeight(compDest.kernel, dkm, compDest.decay);
				if (Number.isFinite(w)) s += w * mkRaw;
			}
			cdByDest.set(j, s);
		}
		const cdArr = new Float64Array(N);
		for (let r = 0; r < N; r++) cdArr[r] = Math.log1p(cdByDest.get(d[r]) ?? 0);
		columns['log1p(comp_dest)'] = cdArr;
	}

	// Radiation: per-OD cumulative mass within distance d_ij of origin i.
	// Per-origin pre-pass: sort destinations by distance from i, then the
	// cumulative-mass-up-to-rank-j is r_ij. Excludes i and j (j's mass not
	// counted in its own radiation value).
	if (radiation) {
		const radByOrigin = new Map();
		for (let r = 0; r < N; r++) {
			const i = o[r];
			if (radByOrigin.has(i)) continue;
			const ci = centroids[i];
			if (!ci) {
				radByOrigin.set(i, new Map());
				continue;
			}
			const sortedDests = [];
			for (const [k, mkRaw] of massD) {
				if (mkRaw == null || !Number.isFinite(mkRaw)) continue;
				const ck = centroids[k];
				if (!ck) continue;
				const dx = ci[0] - ck[0];
				const dy = ci[1] - ck[1];
				sortedDests.push({ k, dist: Math.sqrt(dx * dx + dy * dy), mass: mkRaw });
			}
			sortedDests.sort((a, b) => a.dist - b.dist);

			const cum = new Map();
			let running = 0;
			for (const { k, mass } of sortedDests) {
				cum.set(k, running); // value BEFORE adding this k's mass (excludes self)
				if (k !== i) running += mass; // never include origin's own mass
			}
			radByOrigin.set(i, cum);
		}
		const radArr = new Float64Array(N);
		for (let r = 0; r < N; r++) {
			const cum = radByOrigin.get(o[r]);
			const v = cum?.get(d[r]) ?? 0;
			radArr[r] = Math.log1p(v);
		}
		columns['log1p(radiation)'] = radArr;
	}

	return { o, d, edgeKeys, columns };
}
