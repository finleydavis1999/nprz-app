// Orchestrates a single model fit: design-matrix assembly → webR call →
// result distribution. Three families:
//   runNlm — node-level GLM (gaussian / poisson) on a node-domain design.
//   runSim — Poisson SIM (gravity / production-constrained / attraction-
//            constrained, ± zero-inflation) on an OD flow design.
//   runGwr — geographically weighted regression (one local fit per area)
//            on top of the NLM design.
//
// Pure functions — no Svelte runes, no DOM. The state singleton in
// layers.svelte.js calls these with already-resolved input maps and turns
// each `*FitResult` into child-layer results + a parent `fit`.
//
// R source files are imported as raw strings via Vite's `?raw` query (works
// with the .R extension out of the box; Vite treats it as a text asset).

import { buildNodeDesignMatrix, decorateName } from './design-matrix.js';
import { buildSimDesignMatrix } from './sim-design.js';
import { ensurePackagesFor } from './r-packages.js';
import { ensurePackages, getWebR, runR } from './webr-client.js';
import fitStatsSrc from './r/fit_stats.R?raw';
import nlmSrc from './r/nlm.R?raw';
import simSrc from './r/sim.R?raw';
import simZeroInflSrc from './r/sim_zeroinfl.R?raw';
import gwrSrc from './r/gwr.R?raw';

/** @typedef {'none' | 'log' | 'log1p' | 'sqrt'} Transform */

/** @typedef {{
 *   coefficients: { name: string[], est: Float64Array, se: Float64Array, z: Float64Array, p: Float64Array },
 *   fit: { rSquared: number, adjRSquared: number, rmse: number, aic: number, bic: number,
 *          meanResid: number, varResid: number },
 *   areaCodes: string[],
 *   fitted: Map<string, number>,
 *   residual: Map<string, number>
 * }} ModelFitResult */

// Source the R fitting functions exactly once per webR session. The R global
// env keeps the definitions; subsequent fit_nlm() / fit_sim() calls are free.
let rSourceLoaded = false;
async function ensureRSource() {
	if (rSourceLoaded) return;
	const webR = await getWebR();
	// Load fit_stats first (nlm.R + sim.R reference make_fit_list).
	await webR.evalRVoid(fitStatsSrc);
	await webR.evalRVoid(nlmSrc);
	// sim.R reuses %||% from nlm.R, so it must source after.
	await webR.evalRVoid(simSrc);
	// sim_zeroinfl.R also reuses make_fit_list; safe to source unconditionally.
	// pscl itself is only installed when zero-inflated is actually used (see
	// ensureZeroInflSupport below).
	await webR.evalRVoid(simZeroInflSrc);
	// GWR uses only base R (lm.wfit / glm.fit) + the %||% helper from nlm.R.
	await webR.evalRVoid(gwrSrc);
	rSourceLoaded = true;
}

// pscl is large + niche, so install lazily on first zero-inflated fit rather
// than as part of the standard SIM bootstrap. Failure surfaces in the fit's
// error message via the catch in computeModelLayer.
let pscalInstalled = false;
async function ensureZeroInflSupport() {
	if (pscalInstalled) return;
	await ensurePackages(['pscl']);
	pscalInstalled = true;
}

/**
 * Fit an NLM on the given dependent + covariate layer results. Per-variable
 * transforms (none/log/log1p/sqrt) are applied in the design-matrix step;
 * column names handed to R are decorated (e.g. `log(pop)`) so the coefficient
 * table reads naturally.
 *
 * @param {object} args
 * @param {string} args.dependentName
 * @param {Map<string, number>} args.dependentValues
 * @param {Transform} [args.dependentTransform]
 * @param {Array<{ name: string, values: Map<string, number>, transform?: Transform }>} args.covariates
 * @param {{ name: string, values: Map<string, number> } | null} [args.weights]
 *   Optional per-area observation weights (e.g. population for
 *   inverse-variance scaling). speedglm.wfit consumes them directly.
 * @param {{ name: string, values: Map<string, number>, transform?: Transform } | null} [args.offset]
 *   Optional offset — added to the linear predictor without a coefficient.
 *   Typical use: Poisson rate model with `log(population)` offset.
 * @param {'gaussian' | 'poisson'} args.family
 * @param {'identity' | 'log'} args.link
 * @returns {Promise<ModelFitResult>}
 */
export async function runNlm({
	dependentName,
	dependentValues,
	dependentTransform = 'none',
	covariates,
	weights = null,
	offset = null,
	family,
	link
}) {
	const dm = buildNodeDesignMatrix({
		dependentName,
		dependentValues,
		dependentTransform,
		covariates,
		weights,
		offset
	});
	const depKey = decorateName(dependentName, dependentTransform);
	const covNames = covariates.map((c) => decorateName(c.name, c.transform ?? 'none'));

	await ensurePackagesFor('nlm');
	await ensureRSource();

	// Pack covariate columns into a single matrix (N × p) for the R side.
	// We do it on the JS side because building the matrix from individually
	// bound vectors in R is just an extra hop with no payoff.
	const N = dm.areaCodes.length;
	const p = covNames.length;
	const X = new Float64Array(N * p);
	for (let j = 0; j < p; j++) {
		const col = dm.columns[covNames[j]];
		// R is column-major: column j occupies indices [j*N, (j+1)*N).
		X.set(col, j * N);
	}

	// Bind into a fresh R env. webR auto-converts **plain number arrays** into
	// R numeric vectors; passing a Float64Array directly is a trap — webR sees
	// the underlying ArrayBuffer and turns it into an R `raw` (or numeric of
	// length byteLength = N*8), which then fails at `matrix(..., nrow=N, ncol=p)`
	// with "dims do not match length of object". `Array.from` is the cheapest
	// path that crosses cleanly. (Manifests as: a fit that looks fine in the
	// dock layer-list but never populates modelFits — the exact bug shape we
	// were debugging.)
	const webR = await getWebR();
	// Build the env conditionally — webR's `env` binding chokes on certain
	// empty-array shapes (the `fromD3` Object.keys path errors on something
	// odd); sending the key only when there's a non-empty value sidesteps it
	// entirely. The R wrapper uses `exists()` to detect "missing" optional
	// vectors and rebinds them to NULL.
	const env = {
		y: Array.from(dm.columns[depKey]),
		X_flat: Array.from(X),
		N: N,
		p: p,
		family: family,
		link: link,
		// Plain string '' is webR-converted to an empty character vector; the R
		// side treats that as "no covariates" without us inventing a sentinel.
		col_names: covNames.length > 0 ? covNames : ''
	};
	if (dm.columns['(weights)']) env.weights_vec = Array.from(dm.columns['(weights)']);
	if (dm.columns['(offset)']) env.offset_vec = Array.from(dm.columns['(offset)']);

	let resultRObj;
	try {
		resultRObj = await runR(
			`
			X_mat <- matrix(X_flat, nrow = N, ncol = p)
			cn <- if (identical(col_names, "")) character(0) else as.character(col_names)
			w <- if (exists("weights_vec")) as.numeric(weights_vec) else NULL
			off <- if (exists("offset_vec")) as.numeric(offset_vec) else NULL
			fit_nlm(y = as.numeric(y), X = X_mat, family = family, link = link, col_names = cn, weights = w, offset = off)
			`,
			env
		);

		// .toJs() returns the list as { type: 'list', names: [...], values: [...] }.
		const result = /** @type {any} */ (await resultRObj.toJs({ depth: 3 }));
		const fields = listToObject(result);
		const coefs = listToObject(fields.coefficients);
		const fitObj = listToObject(fields.fit);
		const perObs = listToObject(fields.perObs);

		const fittedArr = toFloat64(perObs.fitted);
		const residualArr = toFloat64(perObs.residual);

		const fittedMap = new Map();
		const residualMap = new Map();
		for (let i = 0; i < N; i++) {
			fittedMap.set(dm.areaCodes[i], fittedArr[i]);
			residualMap.set(dm.areaCodes[i], residualArr[i]);
		}

		return {
			coefficients: {
				name: toStringArray(coefs.name),
				est: toFloat64(coefs.est),
				se: toFloat64(coefs.se),
				z: toFloat64(coefs.z),
				p: toFloat64(coefs.p)
			},
			fit: {
				rSquared: scalarNum(fitObj.rSquared),
				adjRSquared: scalarNum(fitObj.adjRSquared),
				rmse: scalarNum(fitObj.rmse),
				aic: scalarNum(fitObj.aic),
				bic: scalarNum(fitObj.bic),
				meanResid: scalarNum(fitObj.meanResid),
				varResid: scalarNum(fitObj.varResid)
			},
			areaCodes: dm.areaCodes,
			fitted: fittedMap,
			residual: residualMap
		};
	} finally {
		if (resultRObj) {
			try {
				await webR.destroy(resultRObj);
			} catch {
				// shelter cleanup is best-effort; webR also collects on next gc
			}
		}
	}
}

/** @typedef {{
 *   coefficients: { name: string[], est: Float64Array, se: Float64Array, z: Float64Array, p: Float64Array },
 *   fit: { rSquared: number, adjRSquared: number, rmse: number, aic: number, bic: number,
 *          meanResid: number, varResid: number, sorensen: number },
 *   edgeKeys: string[],
 *   fitted: Map<string, number>,
 *   residual: Map<string, number>
 * }} SimFitResult */

/**
 * Fit an unconstrained Poisson SIM on OD flow counts. The deterrence term
 * (`log(distance_km)`) and the two mass terms are pre-staged on the JS side
 * by sim-design.js, so the R half is just a Poisson GLM call.
 *
 * @param {object} args
 * @param {Map<string, number>} args.flows           OD flow counts keyed "o|d".
 * @param {string} args.flowName                     Slug for the dependent.
 * @param {Map<string, number>} args.massO
 * @param {string} args.massOName
 * @param {Transform} [args.massOTransform]
 * @param {Map<string, number>} args.massD
 * @param {string} args.massDName
 * @param {Transform} [args.massDTransform]
 * @param {Record<string, [number, number]>} args.centroids   RD centroids.
 * @param {boolean} [args.includeSelfLoops]
 * @param {boolean} [args.expandToAllOD]    See sim-design.js — fills zero-flow
 *   OD pairs to debias the Poisson distance-decay estimate.
 * @param {'none' | 'production' | 'attraction'} [args.constraint]
 *   Production absorbs origin into per-i fixed effects; attraction does the
 *   same for destinations. Sparse design is built on the R side.
 * @param {Map<string, number> | null} [args.offsetByOrigin]
 *   Optional per-origin offset values (already on the linear-predictor
 *   scale — caller applies the log if they want a log-exposure offset).
 *   Broadcast to per-OD: row (i,j) gets the value for i. NULL = no offset.
 * @param {boolean} [args.zeroInflated]   When true, fit via pscl::zeroinfl
 *   (two-process: logit zeros + Poisson counts). v0 ignores `constraint`
 *   when zero-inflated (UI forces 'none'); the R-side fitter takes a single
 *   shared X for both parts.
 * @returns {Promise<SimFitResult>}
 */
export async function runSim({
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
	offsetByOrigin = null,
	compDest = null,
	radiation = false,
	zeroInflated = false
}) {
	const dm = buildSimDesignMatrix({
		flows,
		flowName,
		massO,
		massOName,
		massOTransform,
		massD,
		massDName,
		massDTransform,
		centroids,
		includeSelfLoops,
		expandToAllOD,
		constraint,
		compDest,
		radiation
	});

	// Order of columns matters: the R side reshapes a flat column-major buffer
	// back into a matrix in this exact order. Same convention as runNlm.
	// Composition depends on the constraint — the absorbed-side mass column
	// is omitted to match what sim-design.js emitted.
	const yKey = flowName ?? 'y';
	const distKey = 'log(distance_km)';
	const oKey = decorateName(`${massOName ?? 'mass_o'}_o`, massOTransform);
	const dKey = decorateName(`${massDName ?? 'mass_d'}_d`, massDTransform);
	const covNames = [distKey];
	if (constraint !== 'production') covNames.push(oKey);
	if (constraint !== 'attraction') covNames.push(dKey);
	// Add the optional pre-computed SIM extras in the same order as the
	// columns dict so the flat buffer aligns. Both are log1p-transformed
	// in sim-design.js so they're already on the linear-predictor scale.
	if (compDest) covNames.push('log1p(comp_dest)');
	if (radiation) covNames.push('log1p(radiation)');

	await ensurePackagesFor('sim');
	await ensureRSource();

	const N = dm.edgeKeys.length;
	const p = covNames.length;
	const X = new Float64Array(N * p);
	for (let j = 0; j < p; j++) {
		X.set(dm.columns[covNames[j]], j * N);
	}

	const webR = await getWebR();
	// Build the per-OD offset by broadcasting from origin (if supplied).
	// Empty array = "no offset"; non-empty = aligned with dm.o.
	let offsetPerOD = [];
	if (offsetByOrigin) {
		offsetPerOD = new Array(dm.o.length);
		for (let i = 0; i < dm.o.length; i++) {
			const v = offsetByOrigin.get(dm.o[i]);
			offsetPerOD[i] = Number.isFinite(v) ? v : 0;
		}
	}
	const env = {
		// Same Float64Array → plain-array conversion as runNlm — webR's `evalR`
		// env binding reads typed-array buffers as raw bytes (length=byteLength),
		// which then breaks `matrix(..., nrow=N, ncol=p)`. See runNlm comment.
		y: Array.from(dm.columns[yKey]),
		X_flat: Array.from(X),
		N: N,
		p: p,
		col_names: covNames.length > 0 ? covNames : '',
		// Origin / destination area codes — always sent. R uses them when
		// constraint != 'none' to build `factor(o)` or `factor(d)` sparse
		// dummies. Plain string arrays cross into R as character vectors.
		o_codes: dm.o,
		d_codes: dm.d,
		constraint: constraint
	};
	// Optional offset — only bind when non-empty (see runNlm comment about
	// webR fromD3 chokes on certain empty-vector env shapes).
	if (offsetPerOD.length > 0) env.offset_vec = offsetPerOD;

	if (zeroInflated) {
		// Heavy + niche; pulled in on demand so non-ZI SIM fits don't pay
		// the install cost.
		await ensureZeroInflSupport();
	}

	let resultRObj;
	try {
		const rCode = zeroInflated
			? `
			cn <- if (identical(col_names, "")) character(0) else as.character(col_names)
			X_mat <- if (p == 0) matrix(numeric(0), nrow = N, ncol = 0)
			         else matrix(X_flat, nrow = N, ncol = p, dimnames = list(NULL, cn))
			off <- if (exists("offset_vec")) as.numeric(offset_vec) else NULL
			fit_sim_zeroinfl(y = as.numeric(y), X = X_mat, col_names = cn, offset = off)
			`
			: `
			cn <- if (identical(col_names, "")) character(0) else as.character(col_names)
			X_mat <- if (p == 0) matrix(numeric(0), nrow = N, ncol = 0)
			         else matrix(X_flat, nrow = N, ncol = p, dimnames = list(NULL, cn))
			off <- if (exists("offset_vec")) as.numeric(offset_vec) else NULL
			fit_sim(
				y = as.numeric(y), X = X_mat, col_names = cn,
				o = as.character(o_codes), d = as.character(d_codes),
				constraint = as.character(constraint), offset = off
			)
			`;
		resultRObj = await runR(rCode, env);
		const result = /** @type {any} */ (await resultRObj.toJs({ depth: 3 }));
		const fields = listToObject(result);
		const coefs = listToObject(fields.coefficients);
		const fitObj = listToObject(fields.fit);
		const perObs = listToObject(fields.perObs);

		const fittedArr = toFloat64(perObs.fitted);
		const residualArr = toFloat64(perObs.residual);

		const fittedMap = new Map();
		const residualMap = new Map();
		for (let i = 0; i < N; i++) {
			fittedMap.set(dm.edgeKeys[i], fittedArr[i]);
			residualMap.set(dm.edgeKeys[i], residualArr[i]);
		}

		return {
			coefficients: {
				name: toStringArray(coefs.name),
				est: toFloat64(coefs.est),
				se: toFloat64(coefs.se),
				z: toFloat64(coefs.z),
				p: toFloat64(coefs.p)
			},
			fit: {
				rSquared: scalarNum(fitObj.rSquared),
				adjRSquared: scalarNum(fitObj.adjRSquared),
				rmse: scalarNum(fitObj.rmse),
				aic: scalarNum(fitObj.aic),
				bic: scalarNum(fitObj.bic),
				meanResid: scalarNum(fitObj.meanResid),
				varResid: scalarNum(fitObj.varResid),
				sorensen: scalarNum(fitObj.sorensen)
			},
			edgeKeys: dm.edgeKeys,
			fitted: fittedMap,
			residual: residualMap
		};
	} finally {
		if (resultRObj) {
			try {
				await webR.destroy(resultRObj);
			} catch {
				// noop
			}
		}
	}
}

/** @typedef {{
 *   coefficients: { name: string[], est: Float64Array, se: Float64Array, z: Float64Array, p: Float64Array },
 *   fit: { rSquared: number, adjRSquared: number, rmse: number, aic: number, bic: number,
 *          meanResid: number, varResid: number },
 *   areaCodes: string[],
 *   fitted: Map<string, number>,
 *   residual: Map<string, number>,
 *   perNode: {
 *     betas: Record<string, Map<string, number>>,
 *     localR2: Map<string, number>,
 *     bwActual: Map<string, number>
 *   }
 * }} GwrFitResult */

/**
 * Geographically weighted regression. Fits one local weighted regression
 * per area; returns per-area coefficient vectors that the JS-state layer
 * spreads into one node-domain child layer per coefficient + local_r2 +
 * bw_actual.
 *
 * Distance matrix is built JS-side from RD centroids (Euclidean in km).
 * Shipped as a flat column-major Float64Array; R reshapes via matrix().
 *
 * @param {object} args
 * @param {string} args.dependentName
 * @param {Map<string, number>} args.dependentValues
 * @param {Transform} [args.dependentTransform]
 * @param {Array<{ name: string, values: Map<string, number>, transform?: Transform }>} args.covariates
 * @param {'gaussian' | 'poisson'} args.family
 * @param {'identity' | 'log'} args.link
 * @param {Record<string, [number, number]>} args.centroids   RD centroids.
 * @param {'fixed' | 'adaptive'} args.kernelType
 * @param {'bi-square' | 'gaussian'} args.kernelShape
 * @param {number | 'auto'} args.bandwidth   km for fixed; k for adaptive.
 *   When 'auto', a golden-section search on residual SS picks the bw.
 * @returns {Promise<GwrFitResult>}
 */
export async function runGwr({
	dependentName,
	dependentValues,
	dependentTransform = 'none',
	covariates,
	family,
	link,
	centroids,
	kernelType = 'fixed',
	kernelShape = 'bi-square',
	bandwidth = 'auto'
}) {
	const dm = buildNodeDesignMatrix({
		dependentName,
		dependentValues,
		dependentTransform,
		covariates
	});
	const depKey = decorateName(dependentName, dependentTransform);
	const covNames = covariates.map((c) => decorateName(c.name, c.transform ?? 'none'));

	await ensurePackagesFor('gwr');
	await ensureRSource();

	// Build the full N×N distance matrix (km) from RD centroids in the
	// canonical row order (dm.areaCodes is sorted). Column-major flat array
	// for direct matrix() reshape in R.
	const N = dm.areaCodes.length;
	const xs = new Float64Array(N);
	const ys = new Float64Array(N);
	for (let i = 0; i < N; i++) {
		const c = centroids[dm.areaCodes[i]];
		if (!c) throw new Error(`Centroid missing for area ${dm.areaCodes[i]}`);
		xs[i] = c[0];
		ys[i] = c[1];
	}
	const D = new Float64Array(N * N);
	for (let i = 0; i < N; i++) {
		for (let j = 0; j < N; j++) {
			const dx = xs[i] - xs[j];
			const dy = ys[i] - ys[j];
			D[j * N + i] = Math.sqrt(dx * dx + dy * dy) / 1000;
		}
	}

	const p = covNames.length;
	const X = new Float64Array(N * p);
	for (let j = 0; j < p; j++) X.set(dm.columns[covNames[j]], j * N);

	const webR = await getWebR();

	// Default bandwidth bracket when 'auto':
	//   fixed: 1km .. 0.5 × longest pairwise distance
	//   adaptive: 5 nearest neighbours .. N/2
	let bw;
	let bwArg = null;
	if (bandwidth === 'auto') {
		if (kernelType === 'fixed') {
			let maxD = 0;
			for (let i = 0; i < N * N; i++) if (D[i] > maxD) maxD = D[i];
			bwArg = { lo: 1, hi: Math.max(2, maxD * 0.5) };
		} else {
			bwArg = { lo: 5, hi: Math.max(10, Math.floor(N / 2)) };
		}
		bw = -1; // sentinel; will be replaced by the search result
	} else {
		bw = bandwidth;
	}

	const env = {
		y: Array.from(dm.columns[depKey]),
		X_flat: Array.from(X),
		D_flat: Array.from(D),
		N,
		p,
		col_names: covNames.length > 0 ? covNames : '',
		bw,
		bw_auto: bwArg ? 1 : 0,
		bw_lo: bwArg?.lo ?? 0,
		bw_hi: bwArg?.hi ?? 0,
		kernel_type: kernelType,
		kernel_shape: kernelShape,
		family,
		link
	};

	let resultRObj;
	try {
		resultRObj = await runR(
			`
			X_mat <- matrix(X_flat, nrow = N, ncol = p)
			cn <- if (identical(col_names, "")) character(0) else as.character(col_names)
			chosen_bw <- if (bw_auto == 1) gwr_bandwidth_aic(
				y = as.numeric(y), X = X_mat, col_names = cn, D_flat = as.numeric(D_flat),
				bw_lo = as.numeric(bw_lo), bw_hi = as.numeric(bw_hi),
				kernel_type = as.character(kernel_type), kernel_shape = as.character(kernel_shape),
				family = as.character(family), link = as.character(link)
			) else as.numeric(bw)
			fit_gwr(
				y = as.numeric(y), X = X_mat, col_names = cn, D_flat = as.numeric(D_flat),
				bw = chosen_bw,
				kernel_type = as.character(kernel_type), kernel_shape = as.character(kernel_shape),
				family = as.character(family), link = as.character(link)
			)
			`,
			env
		);

		const result = /** @type {any} */ (await resultRObj.toJs({ depth: 4 }));
		const fields = listToObject(result);
		const coefs = listToObject(fields.coefficients);
		const fitObj = listToObject(fields.fit);
		const perObs = listToObject(fields.perObs);
		const perNode = listToObject(fields.perNode);

		const fittedArr = toFloat64(perObs.fitted);
		const residualArr = toFloat64(perObs.residual);
		const localR2Arr = toFloat64(perNode.localR2);
		const bwActualArr = toFloat64(perNode.bwActual);

		const fittedMap = new Map();
		const residualMap = new Map();
		const localR2Map = new Map();
		const bwActualMap = new Map();
		for (let i = 0; i < N; i++) {
			fittedMap.set(dm.areaCodes[i], fittedArr[i]);
			residualMap.set(dm.areaCodes[i], residualArr[i]);
			localR2Map.set(dm.areaCodes[i], localR2Arr[i]);
			bwActualMap.set(dm.areaCodes[i], bwActualArr[i]);
		}

		// betas is a list of named numeric vectors, each length N, ordered by
		// the same coef-name order as `coefficients.name`.
		const betasList = listToObject(perNode.betas);
		const betas = {};
		for (const cname of Object.keys(betasList)) {
			const arr = toFloat64(betasList[cname]);
			const m = new Map();
			for (let i = 0; i < N; i++) m.set(dm.areaCodes[i], arr[i]);
			betas[cname] = m;
		}

		return {
			coefficients: {
				name: toStringArray(coefs.name),
				est: toFloat64(coefs.est),
				se: toFloat64(coefs.se),
				z: toFloat64(coefs.z),
				p: toFloat64(coefs.p)
			},
			fit: {
				rSquared: scalarNum(fitObj.rSquared),
				adjRSquared: scalarNum(fitObj.adjRSquared),
				rmse: scalarNum(fitObj.rmse),
				aic: scalarNum(fitObj.aic),
				bic: scalarNum(fitObj.bic),
				meanResid: scalarNum(fitObj.meanResid),
				varResid: scalarNum(fitObj.varResid)
			},
			areaCodes: dm.areaCodes,
			fitted: fittedMap,
			residual: residualMap,
			perNode: { betas, localR2: localR2Map, bwActual: bwActualMap }
		};
	} finally {
		if (resultRObj) {
			try {
				await webR.destroy(resultRObj);
			} catch {
				/* noop */
			}
		}
	}
}

// ── webR result conversion helpers ─────────────────────────────────────────
//
// webR's `.toJs()` returns lists as { type: 'list', names: [...], values: [...] }
// and atomic vectors as { type: 'logical' | 'integer' | 'double' | 'character',
// names: [...] | null, values: [...] }. These small helpers normalise that into
// plain JS values without us having to remember the wrapper shape everywhere.

function listToObject(rList) {
	if (!rList || !Array.isArray(rList.names) || !Array.isArray(rList.values)) {
		throw new Error('Expected an R list with names + values');
	}
	const out = {};
	for (let i = 0; i < rList.names.length; i++) out[rList.names[i]] = rList.values[i];
	return out;
}

function toFloat64(rVec) {
	if (rVec == null) return new Float64Array(0);
	const vals = rVec.values ?? rVec;
	if (vals == null) return new Float64Array(0);
	if (vals instanceof Float64Array) return vals;
	return Float64Array.from(vals);
}

function toStringArray(rVec) {
	const vals = rVec?.values ?? rVec;
	return Array.from(vals ?? []).map((v) => String(v));
}

function scalarNum(rVec) {
	const vals = rVec?.values ?? rVec;
	if (vals == null) return NaN;
	if (typeof vals === 'number') return vals;
	if (Array.isArray(vals) || vals instanceof Float64Array) return Number(vals[0]);
	return NaN;
}
