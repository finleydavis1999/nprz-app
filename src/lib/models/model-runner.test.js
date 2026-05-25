// Unit tests for the runNlm / runSim / runGwr orchestration — specifically
// the JS-side that converts webR's RObject (returned via .toJs()) into the
// `*FitResult` shapes layers.svelte.js distributes into child layers, plus
// the env it ships into runR (which has to navigate the Float64Array →
// raw-bytes trap and several optional-field shapes).
//
// webR is mocked so this runs in node without booting R. Each test asserts
// either:
//   - the parsed result shape (`runFoo()` produces what computeModelLayer
//     expects), or
//   - the env shape sent into runR (regression guard for marshaling traps).
//
// The runR mock returns a synthetic R-list shape; the builders live in
// __fixtures__/model-fits.js so model-runner and layers tests share one
// source of truth for "what does fit_nlm()/fit_sim()/fit_gwr() look like
// after .toJs()".

import { describe, it, expect, vi, beforeEach } from 'vitest';
import {
	dbl,
	chr,
	rList,
	mockRObject,
	fakeNlmFit,
	fakeSimFit,
	fakeGwrFit
} from './__fixtures__/model-fits.js';

// Mock collaborators before importing the SUT. r-packages.js eagerly imports
// webr-client (for ensurePackages), so we mock that too — otherwise the test
// tries to import `webr` itself.
const destroyMock = vi.fn(async () => {});
const evalRVoidMock = vi.fn(async () => {});
const runRMock = vi.fn();
vi.mock('./webr-client.js', () => ({
	getWebR: vi.fn(async () => ({ destroy: destroyMock, evalRVoid: evalRVoidMock })),
	runR: (...args) => runRMock(...args),
	ensurePackages: vi.fn(async () => {})
}));
vi.mock('./r-packages.js', () => ({
	packagesFor: () => [],
	ensurePackagesFor: vi.fn(async () => {})
}));

// Imported AFTER mocks register so the SUT picks up the mocked modules.
const { runNlm, runSim, runGwr } = await import('./model-runner.js');

// Thin wrappers around the shared fixtures so existing assertions
// (`destroyMock` count) keep working — the runner expects `.toJs()` to be a
// callable, not just a property; vi.fn keeps the spying intact.
function mockRObjectReturning(jsShape) {
	const base = mockRObject(jsShape);
	return { toJs: vi.fn(base.toJs) };
}
const fakeFitForN3 = () => fakeNlmFit();

describe('runNlm', () => {
	beforeEach(() => {
		destroyMock.mockClear();
		runRMock.mockClear();
	});

	it('parses a typical fit_nlm() result into a ModelFitResult', async () => {
		runRMock.mockResolvedValue(mockRObjectReturning(fakeFitForN3()));

		const dep = new Map([
			['A', 10],
			['B', 20],
			['C', 30]
		]);
		const x = new Map([
			['A', 1],
			['B', 2],
			['C', 3]
		]);
		const z = new Map([
			['A', 100],
			['B', 200],
			['C', 300]
		]);

		const result = await runNlm({
			dependentName: 'y',
			dependentValues: dep,
			covariates: [
				{ name: 'x', values: x },
				{ name: 'z', values: z }
			],
			family: 'gaussian',
			link: 'identity'
		});

		// Coefficients table: names + four numeric columns.
		expect(result.coefficients.name).toEqual(['(Intercept)', 'x', 'z']);
		expect(Array.from(result.coefficients.est)).toEqual([1.0, 2.0, -0.5]);
		expect(result.coefficients.se).toBeInstanceOf(Float64Array);
		expect(result.coefficients.p).toBeInstanceOf(Float64Array);

		// Fit stats: all unwrapped to scalar numbers.
		expect(result.fit.rSquared).toBe(0.95);
		expect(result.fit.adjRSquared).toBe(0.92);
		expect(result.fit.rmse).toBe(0.5);
		expect(result.fit.aic).toBe(42);
		expect(result.fit.bic).toBe(45);

		// Per-observation maps keyed by area_code in the canonical order
		// design-matrix.js emits (sorted).
		expect(result.areaCodes).toEqual(['A', 'B', 'C']);
		expect(result.fitted.get('A')).toBe(10.5);
		expect(result.fitted.get('C')).toBe(30.5);
		expect(result.residual.get('B')).toBe(0.5);

		// Cleanup ran (webR shelter release is best-effort but must be called).
		expect(destroyMock).toHaveBeenCalledTimes(1);
	});

	it('passes the right env bindings into runR (flat column-major X buffer)', async () => {
		runRMock.mockResolvedValue(mockRObjectReturning(fakeFitForN3()));

		const dep = new Map([
			['A', 1],
			['B', 2]
		]);
		const x = new Map([
			['A', 10],
			['B', 20]
		]);

		await runNlm({
			dependentName: 'y',
			dependentValues: dep,
			covariates: [{ name: 'x', values: x }],
			family: 'poisson',
			link: 'log'
		});

		expect(runRMock).toHaveBeenCalledTimes(1);
		const [, env] = runRMock.mock.calls[0];
		// y / X_flat must be plain number[], NOT Float64Array. webR turns the
		// latter into a raw byte vector (length = byteLength) and the R-side
		// `matrix(..., nrow=N, ncol=p)` then errors with "dims do not match
		// length of object". This is a hard contract — keep both assertions.
		expect(Array.isArray(env.y)).toBe(true);
		expect(env.y).not.toBeInstanceOf(Float64Array);
		expect(env.y).toEqual([1, 2]);
		expect(Array.isArray(env.X_flat)).toBe(true);
		expect(env.X_flat).not.toBeInstanceOf(Float64Array);
		expect(env.X_flat).toEqual([10, 20]);
		expect(env.N).toBe(2);
		expect(env.p).toBe(1);
		expect(env.family).toBe('poisson');
		expect(env.link).toBe('log');
		expect(env.col_names).toEqual(['x']);
	});

	it('uses sentinel "" for col_names when there are no covariates', async () => {
		runRMock.mockResolvedValue(
			mockRObjectReturning(
				rList({
					coefficients: rList({
						name: chr(['(Intercept)']),
						est: dbl([5]),
						se: dbl([0.1]),
						z: dbl([50]),
						p: dbl([0.0001])
					}),
					fit: rList({
						rSquared: dbl([0]),
						adjRSquared: dbl([0]),
						rmse: dbl([0.1]),
						aic: dbl([10]),
						bic: dbl([12]),
						meanResid: dbl([0]),
						varResid: dbl([0.01])
					}),
					perObs: rList({
						fitted: dbl([5, 5, 5]),
						residual: dbl([0, 0, 0])
					})
				})
			)
		);

		await runNlm({
			dependentName: 'y',
			dependentValues: new Map([
				['A', 5],
				['B', 5],
				['C', 5]
			]),
			covariates: [],
			family: 'gaussian',
			link: 'identity'
		});

		const [, env] = runRMock.mock.calls[0];
		// "" sentinel — the R side maps this back to character(0).
		expect(env.col_names).toBe('');
		expect(env.X_flat.length).toBe(0);
		// Same Float64Array-trap regression guard for the no-covariate path.
		expect(Array.isArray(env.X_flat)).toBe(true);
		expect(env.p).toBe(0);
	});

	it('propagates a webR rejection so the caller sets an error', async () => {
		runRMock.mockRejectedValue(new Error('singular fit: design matrix is rank-deficient'));

		await expect(
			runNlm({
				dependentName: 'y',
				dependentValues: new Map([['A', 1]]),
				covariates: [{ name: 'x', values: new Map([['A', 1]]) }],
				family: 'gaussian',
				link: 'identity'
			})
		).rejects.toThrow(/singular fit/i);
	});

	it('throws clearly when toJs() returns an unexpected shape (catches the bug class that hid behind "hasn\'t been fit yet")', async () => {
		// webR returning a list with no `names` field is the regression we
		// want a loud error for. Without the listToObject guard, this would
		// have silently produced undefined fields and a NaN-filled fit object
		// that masquerades as success.
		runRMock.mockResolvedValue(mockRObjectReturning({ type: 'list', values: [], names: null }));

		await expect(
			runNlm({
				dependentName: 'y',
				dependentValues: new Map([['A', 1]]),
				covariates: [{ name: 'x', values: new Map([['A', 1]]) }],
				family: 'gaussian',
				link: 'identity'
			})
		).rejects.toThrow(/expected an r list/i);
	});
});

describe('runSim', () => {
	beforeEach(() => {
		destroyMock.mockClear();
		runRMock.mockClear();
	});

	const fakeSimFitForN2 = () => fakeSimFit();

	const centroids = {
		A: [0, 0],
		B: [3000, 4000] // 5 km from A
	};

	it('parses a fit_sim() result into a SimFitResult keyed by edgeKey', async () => {
		runRMock.mockResolvedValue(mockRObjectReturning(fakeSimFitForN2()));

		const result = await runSim({
			flows: new Map([
				['A|B', 50],
				['B|A', 40]
			]),
			flowName: 'trips',
			massO: new Map([
				['A', 100],
				['B', 200]
			]),
			massOName: 'pop',
			massD: new Map([
				['A', 1000],
				['B', 2000]
			]),
			massDName: 'jobs',
			centroids
		});

		expect(result.edgeKeys).toEqual(['A|B', 'B|A']);
		expect(result.fitted.get('A|B')).toBe(48);
		expect(result.residual.get('B|A')).toBe(2);
		expect(result.coefficients.name).toEqual([
			'(Intercept)',
			'log(distance_km)',
			'log(pop_o)',
			'log(jobs_d)'
		]);
		// Sørensen is SIM-specific — included in the fit stats.
		expect(result.fit.sorensen).toBe(0.92);
		expect(destroyMock).toHaveBeenCalledTimes(1);
	});

	it('hands plain arrays to runR (Float64Array trap regression guard, SIM path)', async () => {
		runRMock.mockResolvedValue(mockRObjectReturning(fakeSimFitForN2()));

		await runSim({
			flows: new Map([
				['A|B', 50],
				['B|A', 40]
			]),
			flowName: 'trips',
			massO: new Map([
				['A', 100],
				['B', 200]
			]),
			massOName: 'pop',
			massD: new Map([
				['A', 1000],
				['B', 2000]
			]),
			massDName: 'jobs',
			centroids
		});

		const [, env] = runRMock.mock.calls[0];
		expect(Array.isArray(env.y)).toBe(true);
		expect(env.y).not.toBeInstanceOf(Float64Array);
		expect(Array.isArray(env.X_flat)).toBe(true);
		expect(env.X_flat).not.toBeInstanceOf(Float64Array);
		expect(env.N).toBe(2);
		expect(env.p).toBe(3); // distance + massO + massD
		expect(env.col_names).toEqual(['log(distance_km)', 'log(pop_o)', 'log(jobs_d)']);
	});

	it('propagates a webR rejection', async () => {
		runRMock.mockRejectedValue(new Error('Poisson glm did not converge'));
		await expect(
			runSim({
				flows: new Map([['A|B', 50]]),
				flowName: 'y',
				massO: new Map([['A', 1]]),
				massOName: 'm',
				massD: new Map([['B', 1]]),
				massDName: 'm',
				centroids
			})
		).rejects.toThrow(/did not converge/i);
	});
});

describe('runGwr', () => {
	beforeEach(() => {
		destroyMock.mockClear();
		runRMock.mockClear();
	});

	// fit_gwr returns the same outer shape as fit_nlm plus a `perNode` slot
	// carrying betas (list of named numeric vectors keyed by coef name),
	// localR2 (numeric length N), bwActual (numeric length N). Shared fixture.
	const fakeGwrFitForN3 = () => fakeGwrFit();

	const centroidsForN3 = {
		A: [0, 0],
		B: [3000, 4000], // 5 km from A
		C: [6000, 8000] // 10 km from A
	};

	it('parses a fit_gwr() result and splits perNode betas into per-coef Maps keyed by area', async () => {
		runRMock.mockResolvedValue(mockRObjectReturning(fakeGwrFitForN3()));

		const dep = new Map([
			['A', 10],
			['B', 20],
			['C', 30]
		]);
		const x = new Map([
			['A', 1],
			['B', 2],
			['C', 3]
		]);
		const result = await runGwr({
			dependentName: 'y',
			dependentValues: dep,
			covariates: [{ name: 'x', values: x }],
			family: 'gaussian',
			link: 'identity',
			centroids: centroidsForN3,
			kernelType: 'fixed',
			kernelShape: 'bi-square',
			bandwidth: 10
		});

		expect(result.areaCodes).toEqual(['A', 'B', 'C']);
		expect(result.coefficients.name).toEqual(['(Intercept)', 'x']);
		// Per-coef beta maps — one per coefficient, area-keyed.
		expect(Object.keys(result.perNode.betas).sort()).toEqual(['(Intercept)', 'x']);
		expect(result.perNode.betas['(Intercept)'].get('A')).toBe(1.1);
		expect(result.perNode.betas['(Intercept)'].get('C')).toBe(1.3);
		expect(result.perNode.betas.x.get('B')).toBeCloseTo(0.55, 6);
		// localR2 + bwActual maps keyed by area in the canonical order.
		expect(result.perNode.localR2.get('A')).toBe(0.81);
		expect(result.perNode.bwActual.get('C')).toBe(12.5);

		expect(destroyMock).toHaveBeenCalledTimes(1);
	});

	it('builds a flat D_flat distance matrix and passes the GWR env to runR', async () => {
		runRMock.mockResolvedValue(mockRObjectReturning(fakeGwrFitForN3()));

		await runGwr({
			dependentName: 'y',
			dependentValues: new Map([
				['A', 1],
				['B', 2],
				['C', 3]
			]),
			covariates: [
				{
					name: 'x',
					values: new Map([
						['A', 1],
						['B', 2],
						['C', 3]
					])
				}
			],
			family: 'gaussian',
			link: 'identity',
			centroids: centroidsForN3,
			kernelType: 'adaptive',
			kernelShape: 'gaussian',
			bandwidth: 'auto'
		});

		const [, env] = runRMock.mock.calls[0];
		// D_flat is N² = 9 entries, column-major. Diagonal must be zero.
		expect(env.N).toBe(3);
		expect(env.p).toBe(1);
		expect(env.D_flat.length).toBe(9);
		expect(env.D_flat[0]).toBe(0); // (A,A)
		expect(env.D_flat[4]).toBe(0); // (B,B)
		expect(env.D_flat[8]).toBe(0); // (C,C)
		// (A,B) = 5km — column-major index = 1*3 + 0 = 3, OR row-major (0,1).
		// In our column-major layout entry (i,j) is at j*N+i.
		expect(env.D_flat[1 * 3 + 0]).toBeCloseTo(5, 6);
		// Float64Array trap guard — webR converts typed arrays to raw bytes.
		expect(Array.isArray(env.D_flat)).toBe(true);
		expect(env.D_flat).not.toBeInstanceOf(Float64Array);
		expect(env.kernel_type).toBe('adaptive');
		expect(env.kernel_shape).toBe('gaussian');
		// 'auto' → bw_auto=1; the search bracket gets set on the JS side.
		expect(env.bw_auto).toBe(1);
		expect(env.bw_lo).toBeGreaterThan(0);
		expect(env.bw_hi).toBeGreaterThan(env.bw_lo);
	});

	it('throws if a centroid is missing for an area', async () => {
		await expect(
			runGwr({
				dependentName: 'y',
				dependentValues: new Map([
					['A', 1],
					['Z', 2]
				]),
				covariates: [
					{
						name: 'x',
						values: new Map([
							['A', 1],
							['Z', 2]
						])
					}
				],
				family: 'gaussian',
				link: 'identity',
				centroids: { A: [0, 0] }, // Z is missing
				kernelType: 'fixed',
				kernelShape: 'bi-square',
				bandwidth: 5
			})
		).rejects.toThrow(/centroid missing for area Z/i);
	});

	it("'auto' bandwidth sets bw_auto=1 and populates the search bracket", async () => {
		runRMock.mockResolvedValue(mockRObjectReturning(fakeGwrFitForN3()));

		await runGwr({
			dependentName: 'y',
			dependentValues: new Map([
				['A', 1],
				['B', 2],
				['C', 3]
			]),
			covariates: [
				{
					name: 'x',
					values: new Map([
						['A', 1],
						['B', 2],
						['C', 3]
					])
				}
			],
			family: 'gaussian',
			link: 'identity',
			centroids: centroidsForN3,
			kernelType: 'fixed',
			kernelShape: 'bi-square',
			bandwidth: 'auto'
		});

		const [, env] = runRMock.mock.calls[0];
		// bw_auto=1 tells the R wrapper to run gwr_bandwidth_aic() instead of
		// passing bw straight through. bw_lo/bw_hi define the bracket; for a
		// 'fixed' kernel we pick [1, 0.5 * maxD] (km).
		expect(env.bw_auto).toBe(1);
		expect(env.bw).toBe(-1); // sentinel — replaced inside R
		expect(env.bw_lo).toBeGreaterThan(0);
		expect(env.bw_hi).toBeGreaterThan(env.bw_lo);
	});

	it("adaptive 'auto' uses an integer-k bracket, not a km bracket", async () => {
		runRMock.mockResolvedValue(mockRObjectReturning(fakeGwrFitForN3()));

		await runGwr({
			dependentName: 'y',
			dependentValues: new Map([
				['A', 1],
				['B', 2],
				['C', 3]
			]),
			covariates: [
				{
					name: 'x',
					values: new Map([
						['A', 1],
						['B', 2],
						['C', 3]
					])
				}
			],
			family: 'gaussian',
			link: 'identity',
			centroids: centroidsForN3,
			kernelType: 'adaptive',
			kernelShape: 'gaussian',
			bandwidth: 'auto'
		});

		const [, env] = runRMock.mock.calls[0];
		expect(env.kernel_type).toBe('adaptive');
		// k-nn bracket is [5, max(10, N/2)]; for N=3 we expect [5, 10].
		expect(env.bw_lo).toBe(5);
		expect(env.bw_hi).toBeGreaterThanOrEqual(10);
	});
});

// ── NLM weights + offset integration ──────────────────────────────────────
// design-matrix.test.js covers the column shapes; here we confirm the runner
// actually plumbs weights/offset through to the env that R sees.

describe('runNlm — weights + offset env shape', () => {
	beforeEach(() => {
		destroyMock.mockClear();
		runRMock.mockClear();
	});

	it('weights map produces a weights_vec env binding aligned with areaCodes', async () => {
		runRMock.mockResolvedValue(mockRObjectReturning(fakeFitForN3()));

		await runNlm({
			dependentName: 'y',
			dependentValues: new Map([
				['A', 1],
				['B', 2],
				['C', 3]
			]),
			covariates: [
				{
					name: 'x',
					values: new Map([
						['A', 10],
						['B', 20],
						['C', 30]
					])
				}
			],
			weights: {
				name: 'pop',
				values: new Map([
					['A', 100],
					['B', 200],
					['C', 300]
				])
			},
			family: 'gaussian',
			link: 'identity'
		});

		const [, env] = runRMock.mock.calls[0];
		expect(env.weights_vec).toEqual([100, 200, 300]);
		// Same Float64Array trap guard for the weights path.
		expect(Array.isArray(env.weights_vec)).toBe(true);
		expect(env.weights_vec).not.toBeInstanceOf(Float64Array);
	});

	it('offset with log transform pre-applies log() before sending env', async () => {
		runRMock.mockResolvedValue(mockRObjectReturning(fakeFitForN3()));

		await runNlm({
			dependentName: 'y',
			dependentValues: new Map([
				['A', 1],
				['B', 2],
				['C', 3]
			]),
			covariates: [
				{
					name: 'x',
					values: new Map([
						['A', 10],
						['B', 20],
						['C', 30]
					])
				}
			],
			offset: {
				name: 'pop',
				values: new Map([
					['A', Math.E],
					['B', Math.E * Math.E],
					['C', Math.E * Math.E * Math.E]
				]),
				transform: 'log'
			},
			family: 'poisson',
			link: 'log'
		});

		const [, env] = runRMock.mock.calls[0];
		// design-matrix.js applies the transform; offset_vec values should be
		// log(E), log(E²), log(E³) = 1, 2, 3.
		expect(env.offset_vec.length).toBe(3);
		expect(env.offset_vec[0]).toBeCloseTo(1, 6);
		expect(env.offset_vec[1]).toBeCloseTo(2, 6);
		expect(env.offset_vec[2]).toBeCloseTo(3, 6);
	});

	it('omits weights_vec and offset_vec env keys when neither is supplied', async () => {
		// webR's `env` binding chokes on certain empty-array shapes, so the
		// runner sends the key ONLY when the value is present. Pin that
		// behaviour — regression for the "weights = null still binds an empty
		// vector → fromD3 crashes" trap.
		runRMock.mockResolvedValue(mockRObjectReturning(fakeFitForN3()));

		await runNlm({
			dependentName: 'y',
			dependentValues: new Map([['A', 1]]),
			covariates: [{ name: 'x', values: new Map([['A', 1]]) }],
			family: 'gaussian',
			link: 'identity'
		});

		const [, env] = runRMock.mock.calls[0];
		expect('weights_vec' in env).toBe(false);
		expect('offset_vec' in env).toBe(false);
	});
});

// ── Constrained / zero-inflated SIM ───────────────────────────────────────

describe('runSim — constrained + zero-inflated paths', () => {
	beforeEach(() => {
		destroyMock.mockClear();
		runRMock.mockClear();
	});

	const centroids = {
		A: [0, 0],
		B: [3000, 4000]
	};

	function simInputs() {
		return {
			flows: new Map([
				['A|B', 50],
				['B|A', 40]
			]),
			flowName: 'trips',
			massO: new Map([
				['A', 100],
				['B', 200]
			]),
			massOName: 'pop',
			massD: new Map([
				['A', 1000],
				['B', 2000]
			]),
			massDName: 'jobs',
			centroids
		};
	}

	it("production constraint omits the origin mass column and tags constraint='production'", async () => {
		runRMock.mockResolvedValue(
			mockRObjectReturning(
				fakeSimFit({
					coefNames: ['log(distance_km)', 'log(jobs_d)', 'o.A', 'o.B'],
					est: [-1.2, 0.88, 4.5, 4.6],
					se: [0.05, 0.04, 0.2, 0.2],
					z: [-24, 22, 22, 23],
					p: [0, 0, 0, 0]
				})
			)
		);

		await runSim({
			...simInputs(),
			constraint: 'production'
		});

		const [, env] = runRMock.mock.calls[0];
		expect(env.constraint).toBe('production');
		// Origin-mass column is absorbed by factor(o); remaining covariates are
		// distance + destination mass.
		expect(env.col_names).toEqual(['log(distance_km)', 'log(jobs_d)']);
		expect(env.p).toBe(2);
		// Per-OD o / d codes are still sent — fit_sim builds factor(o) from them.
		expect(env.o_codes).toEqual(['A', 'B']);
		expect(env.d_codes).toEqual(['B', 'A']);
	});

	it('attraction constraint omits the destination mass column', async () => {
		runRMock.mockResolvedValue(
			mockRObjectReturning(
				fakeSimFit({
					coefNames: ['log(distance_km)', 'log(pop_o)', 'd.A', 'd.B'],
					est: [-1.2, 0.95, 4.5, 4.6],
					se: [0.05, 0.03, 0.2, 0.2],
					z: [-24, 32, 22, 23],
					p: [0, 0, 0, 0]
				})
			)
		);

		await runSim({
			...simInputs(),
			constraint: 'attraction'
		});

		const [, env] = runRMock.mock.calls[0];
		expect(env.constraint).toBe('attraction');
		expect(env.col_names).toEqual(['log(distance_km)', 'log(pop_o)']);
	});

	it('zeroInflated=true returns count.* + zero.* prefixed coefficient names', async () => {
		runRMock.mockResolvedValue(
			mockRObjectReturning(
				fakeSimFit({
					coefNames: [
						'count.(Intercept)',
						'count.log(distance_km)',
						'count.log(pop_o)',
						'count.log(jobs_d)',
						'zero.(Intercept)',
						'zero.log(distance_km)',
						'zero.log(pop_o)',
						'zero.log(jobs_d)'
					],
					est: [4.5, -1.2, 0.95, 0.88, -2.0, 0.5, -0.1, -0.2],
					se: [0.1, 0.05, 0.03, 0.04, 0.3, 0.1, 0.05, 0.05],
					z: [45, -24, 32, 22, -6.7, 5, -2, -4],
					p: [0, 0, 0, 0, 0, 0, 0.04, 0]
				})
			)
		);

		const result = await runSim({
			...simInputs(),
			zeroInflated: true
		});

		expect(result.coefficients.name.filter((n) => n.startsWith('count.'))).toHaveLength(4);
		expect(result.coefficients.name.filter((n) => n.startsWith('zero.'))).toHaveLength(4);
		// runSim doesn't transform names — they flow straight through from R.
	});
});
