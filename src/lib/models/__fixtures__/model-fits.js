// Shared test fixtures for the modeling layer.
//
// Two consumers:
//   - `model-runner.test.js` uses the R-shape builders (`dbl`, `chr`, `rList`,
//     plus `fakeNlmFit` / `fakeSimFit` / `fakeGwrFit`) to mock what webR's
//     `.toJs()` returns from `fit_nlm` / `fit_sim` / `fit_gwr`. The shapes
//     mirror exactly what the R side builds via `list(coefficients = list(...),
//     fit = list(...), perObs = list(...))`.
//
//   - `layers.svelte.test.js` uses `syntheticFit` to mock what `runNlm` /
//     `runGwr` return after parsing — a `ModelFitResult` with Maps keyed by
//     area code.
//
// Both files used to duplicate these helpers; lifting them here keeps the R
// shape contract in one place and shrinks the test files by ~80 lines.

// ── R-shape helpers ────────────────────────────────────────────────────────
// webR's `.toJs()` returns atomic vectors as
// `{ type: 'double' | 'character' | ..., names: null | string[], values: any[] }`.
// Named lists come back as `{ type: 'list', names: string[], values: any[] }`.
// These three helpers compose the same shape so test fixtures stay readable.

export function dbl(values) {
	return { type: 'double', names: null, values };
}
export function chr(values) {
	return { type: 'character', names: null, values };
}
export function rList(obj) {
	return { type: 'list', names: Object.keys(obj), values: Object.values(obj) };
}

/** Builder for a `mockRObjectReturning(...)` wrap — the runner expects the
 *  awaited shape to be `{ toJs: () => js }`. Imported as needed by the tests
 *  that mock runR. */
export function mockRObject(jsShape) {
	return { toJs: async () => jsShape };
}

// ── fit_nlm-shaped fixtures ───────────────────────────────────────────────
// Default: N=3 areas, p=2 covariates (intercept + x + z = 3 coefs).

export function fakeNlmFit({
	coefNames = ['(Intercept)', 'x', 'z'],
	est = [1.0, 2.0, -0.5],
	se = [0.1, 0.2, 0.05],
	z = [10, 10, -10],
	p = [0.0001, 0.0001, 0.0002],
	fit = {
		rSquared: 0.95,
		adjRSquared: 0.92,
		rmse: 0.5,
		aic: 42,
		bic: 45,
		meanResid: 0.01,
		varResid: 0.25
	},
	fitted = [10.5, 20.5, 30.5],
	residual = [-0.5, 0.5, -0.1]
} = {}) {
	return rList({
		coefficients: rList({
			name: chr(coefNames),
			est: dbl(est),
			se: dbl(se),
			z: dbl(z),
			p: dbl(p)
		}),
		fit: rList({
			rSquared: dbl([fit.rSquared]),
			adjRSquared: dbl([fit.adjRSquared]),
			rmse: dbl([fit.rmse]),
			aic: dbl([fit.aic]),
			bic: dbl([fit.bic]),
			meanResid: dbl([fit.meanResid]),
			varResid: dbl([fit.varResid])
		}),
		perObs: rList({
			fitted: dbl(fitted),
			residual: dbl(residual)
		})
	});
}

// ── fit_sim-shaped fixtures ───────────────────────────────────────────────
// SIM adds `sorensen` to the fit block. Default fixture: N=2 OD pairs.

export function fakeSimFit({
	coefNames = ['(Intercept)', 'log(distance_km)', 'log(pop_o)', 'log(jobs_d)'],
	est = [4.5, -1.2, 0.95, 0.88],
	se = [0.1, 0.05, 0.03, 0.04],
	z = [45, -24, 32, 22],
	p = [0, 0, 0, 0],
	fit = {
		rSquared: 0.87,
		adjRSquared: 0.85,
		rmse: 4.1,
		aic: 220,
		bic: 232,
		meanResid: 0.01,
		varResid: 16,
		sorensen: 0.92
	},
	fitted = [48, 38],
	residual = [2, 2]
} = {}) {
	return rList({
		coefficients: rList({
			name: chr(coefNames),
			est: dbl(est),
			se: dbl(se),
			z: dbl(z),
			p: dbl(p)
		}),
		fit: rList({
			rSquared: dbl([fit.rSquared]),
			adjRSquared: dbl([fit.adjRSquared]),
			rmse: dbl([fit.rmse]),
			aic: dbl([fit.aic]),
			bic: dbl([fit.bic]),
			meanResid: dbl([fit.meanResid]),
			varResid: dbl([fit.varResid]),
			sorensen: dbl([fit.sorensen])
		}),
		perObs: rList({
			fitted: dbl(fitted),
			residual: dbl(residual)
		})
	});
}

// ── fit_gwr-shaped fixtures ───────────────────────────────────────────────
// GWR adds a `perNode` block with one numeric vector per coefficient under
// `betas`, plus `localR2` and `bwActual`. Default fixture: N=3, p=1 covariate.

export function fakeGwrFit({
	coefNames = ['(Intercept)', 'x'],
	est = [1.2, 0.55],
	se = [0.1, 0.05],
	z = [12, 11],
	p = [0, 0],
	fit = {
		rSquared: 0.82,
		adjRSquared: 0.8,
		rmse: 1.1,
		aic: NaN,
		bic: NaN,
		meanResid: 0,
		varResid: 1.2
	},
	fitted = [10, 20, 30],
	residual = [-0.1, -0.2, -0.3],
	betas = { '(Intercept)': [1.1, 1.2, 1.3], x: [0.5, 0.55, 0.6] },
	localR2 = [0.81, 0.82, 0.83],
	bwActual = [12.5, 12.5, 12.5]
} = {}) {
	return rList({
		coefficients: rList({
			name: chr(coefNames),
			est: dbl(est),
			se: dbl(se),
			z: dbl(z),
			p: dbl(p)
		}),
		fit: rList({
			rSquared: dbl([fit.rSquared]),
			adjRSquared: dbl([fit.adjRSquared]),
			rmse: dbl([fit.rmse]),
			aic: dbl([fit.aic]),
			bic: dbl([fit.bic]),
			meanResid: dbl([fit.meanResid]),
			varResid: dbl([fit.varResid])
		}),
		perObs: rList({
			fitted: dbl(fitted),
			residual: dbl(residual)
		}),
		perNode: rList({
			betas: rList(
				Object.fromEntries(Object.entries(betas).map(([name, vals]) => [name, dbl(vals)]))
			),
			localR2: dbl(localR2),
			bwActual: dbl(bwActual)
		})
	});
}

// ── Parsed-result (JS-side) fixture ───────────────────────────────────────
// This is what `runNlm` / `runGwr` return AFTER parsing — used by
// `layers.svelte.test.js` to mock the runner. Mirrors the `ModelFitResult`
// shape that `computeModelLayer` distributes into child layers.

export function syntheticFit({
	areaCodes,
	coefs = { name: ['(Intercept)', 'x'], est: [1, 2] }
} = {}) {
	const fitted = new Map();
	const residual = new Map();
	for (let i = 0; i < areaCodes.length; i++) {
		fitted.set(areaCodes[i], 10 + i);
		residual.set(areaCodes[i], 0.1 * i);
	}
	return {
		coefficients: {
			name: coefs.name,
			est: Float64Array.from(coefs.est),
			se: Float64Array.from(coefs.est.map(() => 0.1)),
			z: Float64Array.from(coefs.est.map((e) => e / 0.1)),
			p: Float64Array.from(coefs.est.map(() => 0.001))
		},
		fit: {
			rSquared: 0.9,
			adjRSquared: 0.85,
			rmse: 1.2,
			aic: 100,
			bic: 110,
			meanResid: 0.05,
			varResid: 0.5
		},
		areaCodes,
		fitted,
		residual
	};
}
