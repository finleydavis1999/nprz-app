// Build a node-domain model design matrix from saved layer results.
//
// Used by NLM and GWR (both run on the same node-level design). The SIM OD
// design lives next door in sim-design.js since the join shape (flow × node
// × node × distance) is different enough to deserve its own file.
//
// Every input layer's result is a `Map<areaCode, number>` already in memory.
// The design matrix is just the intersection of area codes across every
// input, zipped column-by-column into Float64Arrays — pure JS, no DuckDB
// needed at the node-domain scale we work at (≤ a few thousand rows).
//
// Output shape matches what r/nlm.R and r/gwr.R expect: `areaCodes` is the
// canonical row order; every column in `columns` is a Float64Array of
// identical length; `dependent` is one of those columns by convention
// (split out by the caller when handing off to webR).
//
// Per-variable transforms (none / log / log1p / sqrt) are applied BEFORE the
// finite-row filter so log(0) and log(negative) rows drop cleanly. Column
// keys in the output are decorated (e.g. `log(pop)`) so they flow straight
// through to R coefficient names — what the user sees in the form formula
// preview matches what they see in the coefficient table.

/** @typedef {'none' | 'log' | 'log1p' | 'sqrt'} Transform */
/** @typedef {{ areaCodes: string[], columns: Record<string, Float64Array> }} DesignMatrix */

/** Apply a transform to a single numeric value. `log(0)`, `log(<0)` and
 *  `sqrt(<0)` produce non-finite values that the row-filter then drops. */
function applyTransform(transform, v) {
	if (transform === 'log') return Math.log(v);
	if (transform === 'log1p') return Math.log1p(v);
	if (transform === 'sqrt') return Math.sqrt(v);
	return v;
}

/** Format a variable name to reflect its transform — the same string is used
 *  as the design-matrix column key, the R column name, and the formula
 *  preview, so they always agree. */
export function decorateName(name, transform) {
	if (!transform || transform === 'none') return name;
	return `${transform}(${name})`;
}

/**
 * Assemble a design matrix from a dependent layer + zero-or-more covariate
 * layers. Rows with a non-finite value in *any* column (after transforms) are
 * dropped — webR's glm.fit chokes on NaN/Inf, and silently dropping is the
 * same behaviour as base `lm()` / `glm()` under `na.action = na.omit`.
 *
 * Optional `weights` (per-area observation weights, e.g. population for
 * inverse-variance scaling) participate in the row intersection — a row
 * drops if its weight is missing or non-positive (zero / negative weights
 * break weighted least squares).
 *
 * @param {object} args
 * @param {string} args.dependentName        Column name for the dependent.
 * @param {Map<string, number>} args.dependentValues
 * @param {Transform} [args.dependentTransform]
 * @param {Array<{ name: string, values: Map<string, number>, transform?: Transform }>} args.covariates
 * @param {{ name: string, values: Map<string, number> } | null} [args.weights]
 *   Optional per-area weights. Returned as `columns['(weights)']` so the
 *   caller can split it back out before sending to R.
 * @param {{ name: string, values: Map<string, number>, transform?: Transform } | null} [args.offset]
 *   Optional offset — added directly to the linear predictor without
 *   estimating a coefficient. Standard use is `log(exposure)` for Poisson
 *   rate models (`deaths ~ ... + offset(log(population))`); we expose the
 *   transform here so the user picks it on the layer rather than wrapping
 *   it manually. Returned as `columns['(offset)']`.
 * @returns {DesignMatrix}
 */
export function buildNodeDesignMatrix({
	dependentName,
	dependentValues,
	dependentTransform = 'none',
	covariates,
	weights = null,
	offset = null
}) {
	if (!dependentValues || dependentValues.size === 0) {
		throw new Error('Dependent layer has no values');
	}
	const depKey = decorateName(dependentName, dependentTransform);
	const covEntries = covariates.map((c) => ({
		values: c.values,
		key: decorateName(c.name, c.transform ?? 'none'),
		transform: c.transform ?? 'none'
	}));

	// Intersect area codes across dependent + all covariates + optional
	// weights, applying transforms inline so non-finite results (log of 0/
	// negative, sqrt of negative) take a row out of the model just like NaN
	// inputs would. Non-positive weights also drop — WLS treats w<=0 as
	// "no information" and most solvers error on them.
	const codes = [];
	for (const code of dependentValues.keys()) {
		const yiRaw = dependentValues.get(code);
		if (yiRaw == null) continue;
		const yi = applyTransform(dependentTransform, yiRaw);
		if (!Number.isFinite(yi)) continue;
		let drop = false;
		for (const c of covEntries) {
			const xiRaw = c.values.get(code);
			if (xiRaw == null) {
				drop = true;
				break;
			}
			const xi = applyTransform(c.transform, xiRaw);
			if (!Number.isFinite(xi)) {
				drop = true;
				break;
			}
		}
		if (!drop && weights) {
			const wRaw = weights.values.get(code);
			if (wRaw == null || !Number.isFinite(wRaw) || wRaw <= 0) drop = true;
		}
		if (!drop && offset) {
			const oRaw = offset.values.get(code);
			if (oRaw == null) {
				drop = true;
			} else {
				const ov = applyTransform(offset.transform ?? 'none', oRaw);
				if (!Number.isFinite(ov)) drop = true;
			}
		}
		if (!drop) codes.push(code);
	}
	codes.sort();

	const N = codes.length;
	if (N === 0) {
		throw new Error(
			'No rows survived: dependent and covariates share no finite values (check log/sqrt transforms against zero/negative inputs)'
		);
	}

	/** @type {Record<string, Float64Array>} */
	const columns = {};
	columns[depKey] = new Float64Array(N);
	for (const c of covEntries) columns[c.key] = new Float64Array(N);
	if (weights) columns['(weights)'] = new Float64Array(N);
	if (offset) columns['(offset)'] = new Float64Array(N);

	const offsetTx = offset?.transform ?? 'none';
	for (let i = 0; i < N; i++) {
		const code = codes[i];
		columns[depKey][i] = applyTransform(
			dependentTransform,
			/** @type {number} */ (dependentValues.get(code))
		);
		for (const c of covEntries) {
			columns[c.key][i] = applyTransform(c.transform, /** @type {number} */ (c.values.get(code)));
		}
		if (weights) columns['(weights)'][i] = /** @type {number} */ (weights.values.get(code));
		if (offset) {
			columns['(offset)'][i] = applyTransform(
				offsetTx,
				/** @type {number} */ (offset.values.get(code))
			);
		}
	}

	return { areaCodes: codes, columns };
}
