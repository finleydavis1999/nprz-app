// Browser test for the model-fit half of the layers singleton.
//
// Regression coverage for the bug where ModelResults kept rendering "Model 1
// hasn't been fit yet" after a successful fit. The bug class we want to catch
// here: anything that prevents `modelFits` from picking up the fit result, or
// that drops the per-child `results` maps, or that fails to auto-activate the
// fitted child. We mock the model-runner so this doesn't boot webR — the
// actual webR-side correctness is verified separately (runtime via the dev
// server, and the JS-side parsing via model-runner.test.js).

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { syntheticFit } from '$lib/models/__fixtures__/model-fits.js';

// Mock the runner BEFORE importing layers — runner is imported eagerly at
// the top of layers.svelte.js. The mock returns a synthetic ModelFitResult
// that mirrors what `r/nlm.R` would build for a tiny fixture.
const runNlmMock = vi.fn();
const runSimMock = vi.fn();
const runGwrMock = vi.fn();
vi.mock('$lib/models/model-runner.js', () => ({
	runNlm: (...args) => runNlmMock(...args),
	runSim: (...args) => runSimMock(...args),
	runGwr: (...args) => runGwrMock(...args)
}));
// Stub the manifest centroids fetch — SIM's computeModelLayer fetches centroidsRd.
const originalFetch = globalThis.fetch;
globalThis.fetch = vi.fn(async (url) => {
	if (String(url).includes('centroids')) {
		return new Response(
			JSON.stringify({
				A: [0, 0],
				B: [3000, 4000]
			}),
			{ status: 200 }
		);
	}
	return originalFetch ? originalFetch(url) : new Response('', { status: 404 });
});

import { layers } from './layers.svelte.js';
import { selection } from './selection.svelte.js';
import { manifestState } from './manifest.svelte.js';

function resetLayers() {
	layers.items = [];
	layers.results.clear();
	layers.modelFits.clear();
	layers.loading.clear();
	layers.errors.clear();
	layers.activeId = null;
}

/** Minimal manifest stub so SIM can resolve the centroids URL. */
function seedManifest(scale) {
	manifestState.data = {
		version: 'test',
		geo: { [scale]: { centroidsRd: `centroids-${scale}.json` } }
	};
}

/** Seed two filter-like layers with results pre-populated, mimicking what the
 *  Node-data Save-layer button produces once its DuckDB query resolves. */
function seedDependentAndCovariate(scale) {
	const depId = 'dep_test';
	const covId = 'cov_test';
	layers.items = [
		...layers.items,
		{
			id: depId,
			name: 'jobs',
			slug: 'jobs',
			kind: 'filter',
			domain: 'node',
			scale,
			dataset: 'banen',
			year: 2018,
			filters: {}
		},
		{
			id: covId,
			name: 'pop',
			slug: 'pop',
			kind: 'filter',
			domain: 'node',
			scale,
			dataset: 'demographics',
			year: 2018,
			filters: {}
		}
	];
	layers.results.set(
		depId,
		new Map([
			['A', 100],
			['B', 200],
			['C', 300]
		])
	);
	layers.results.set(
		covId,
		new Map([
			['A', 1000],
			['B', 2000],
			['C', 3000]
		])
	);
	return { depId, covId };
}

/** Inject a `model` parent + fitted/residual children directly (bypassing
 *  saveModel) so tests of computeModelLayer aren't racing saveModel's
 *  fire-and-forget `then(recomputeCalcs)` chain. */
function injectModelEntry({ depId, covId, scale }) {
	const parentId = 'model_test_parent';
	const fittedId = 'model_test_fitted';
	const residualId = 'model_test_residual';
	layers.items = [
		...layers.items,
		{
			id: parentId,
			name: 'TestModel',
			slug: 'testmodel',
			kind: 'model',
			domain: 'node',
			scale,
			family: 'nlm',
			spec: {
				dependentId: depId,
				covariateIds: [covId],
				glm: { family: 'gaussian', link: 'identity' }
			},
			childIds: [fittedId, residualId]
		},
		{
			id: fittedId,
			name: 'TestModel — fitted',
			slug: 'testmodel_fitted',
			kind: 'model-output',
			domain: 'node',
			scale,
			parentId,
			channel: 'fitted'
		},
		{
			id: residualId,
			name: 'TestModel — residual',
			slug: 'testmodel_residual',
			kind: 'model-output',
			domain: 'node',
			scale,
			parentId,
			channel: 'residual'
		}
	];
	return { parentId, fittedId, residualId };
}

describe('layers — model fit lifecycle', () => {
	beforeEach(() => {
		resetLayers();
		runNlmMock.mockReset();
		runSimMock.mockReset();
		runGwrMock.mockReset();
	});

	it('saveModel creates a parent + fitted + residual children and triggers a fit', async () => {
		runNlmMock.mockResolvedValue(syntheticFit({ areaCodes: ['A', 'B', 'C'] }));
		const { depId, covId } = seedDependentAndCovariate(selection.scale);

		const { parentId, fitPromise } = layers.saveModel({
			name: 'My model',
			family: 'nlm',
			spec: {
				dependentId: depId,
				covariateIds: [covId],
				glm: { family: 'gaussian', link: 'identity' }
			}
		});

		expect(parentId).toBeTruthy();
		const parent = layers.items.find((i) => i.id === parentId);
		expect(parent?.kind).toBe('model');
		expect(parent?.childIds?.length).toBe(2);

		// fitPromise resolves when computeModelLayer + recomputeCalcs settle —
		// deterministic replacement for the old setTimeout-then-tick dance.
		await fitPromise;
		expect(runNlmMock).toHaveBeenCalledTimes(1);
		const [args] = runNlmMock.mock.calls[0];
		expect(args.dependentName).toBe('jobs');
		expect(args.covariates).toHaveLength(1);
		expect(args.covariates[0].name).toBe('pop');
	});

	it('computeModelLayer populates modelFits, child results, and auto-activates fitted', async () => {
		runNlmMock.mockResolvedValue(syntheticFit({ areaCodes: ['A', 'B', 'C'] }));
		const { depId, covId } = seedDependentAndCovariate(selection.scale);
		// Bypass saveModel so we're not racing its fire-and-forget recompute
		// chain — this test is specifically about computeModelLayer's behavior.
		const { parentId, fittedId, residualId } = injectModelEntry({
			depId,
			covId,
			scale: selection.scale
		});

		await layers.computeModelLayer(parentId);

		// The regression we care about: modelFits must contain an entry. If this
		// fails, ModelResults will keep rendering "hasn't been fit yet".
		expect(layers.modelFits.has(parentId)).toBe(true);
		const fit = layers.modelFits.get(parentId);
		expect(fit?.fit.rSquared).toBe(0.9);
		expect(fit?.coefficients.name).toEqual(['(Intercept)', 'x']);

		// Child results are populated and keyed by area_code.
		expect(layers.results.get(fittedId)?.get('A')).toBe(10);
		expect(layers.results.get(fittedId)?.get('C')).toBe(12);
		expect(layers.results.get(residualId)?.get('B')).toBeCloseTo(0.1, 6);

		// Auto-activation: the fitted child is now the active layer.
		expect(layers.activeId).toBe(fittedId);

		// Loading flag cleared; no error.
		expect(layers.loading.has(parentId)).toBe(false);
		expect(layers.errors.get(parentId)).toBeUndefined();
	});

	it('records an error and clears loading when runNlm rejects', async () => {
		runNlmMock.mockRejectedValue(new Error('rank-deficient design'));
		const { depId, covId } = seedDependentAndCovariate(selection.scale);
		const { parentId } = injectModelEntry({ depId, covId, scale: selection.scale });

		await layers.computeModelLayer(parentId);

		// The Model results panel reads errors first; the dock row meta shows `!`.
		// Both rely on this being populated.
		expect(layers.errors.get(parentId)).toMatch(/rank-deficient/);
		expect(layers.modelFits.has(parentId)).toBe(false);
		expect(layers.loading.has(parentId)).toBe(false);
	});

	it('does not auto-activate when the user has chosen an unrelated saved layer', async () => {
		runNlmMock.mockResolvedValue(syntheticFit({ areaCodes: ['A', 'B', 'C'] }));
		const { depId, covId } = seedDependentAndCovariate(selection.scale);
		// Pre-set active to one of the input filter layers — simulates the user
		// looking at the dependent's distribution while building the model.
		layers.activeId = depId;

		const { parentId } = injectModelEntry({ depId, covId, scale: selection.scale });
		await layers.computeModelLayer(parentId);

		// Fit succeeded but the user's pre-existing active selection wins.
		expect(layers.modelFits.has(parentId)).toBe(true);
		expect(layers.activeId).toBe(depId);
	});

	it('forwards per-variable transforms to runNlm', async () => {
		runNlmMock.mockResolvedValue(syntheticFit({ areaCodes: ['A', 'B', 'C'] }));
		const { depId, covId } = seedDependentAndCovariate(selection.scale);
		// Inject a model whose spec carries transforms.
		const parentId = 'tx_test_parent';
		const fittedId = 'tx_test_fitted';
		const residualId = 'tx_test_residual';
		layers.items = [
			...layers.items,
			{
				id: parentId,
				name: 'Tx',
				slug: 'tx',
				kind: 'model',
				domain: 'node',
				scale: selection.scale,
				family: 'nlm',
				spec: {
					dependentId: depId,
					dependentTransform: 'log',
					covariateIds: [covId],
					covariateTransforms: { [covId]: 'log' },
					glm: { family: 'gaussian', link: 'identity' }
				},
				childIds: [fittedId, residualId]
			},
			{
				id: fittedId,
				name: 'Tx — fitted',
				slug: 'tx_fitted',
				kind: 'model-output',
				domain: 'node',
				scale: selection.scale,
				parentId,
				channel: 'fitted'
			},
			{
				id: residualId,
				name: 'Tx — residual',
				slug: 'tx_residual',
				kind: 'model-output',
				domain: 'node',
				scale: selection.scale,
				parentId,
				channel: 'residual'
			}
		];

		await layers.computeModelLayer(parentId);

		expect(runNlmMock).toHaveBeenCalledTimes(1);
		const [args] = runNlmMock.mock.calls[0];
		expect(args.dependentTransform).toBe('log');
		expect(args.covariates[0].transform).toBe('log');
		expect(layers.modelFits.has(parentId)).toBe(true);
	});

	it('SIM saveModel + computeModelLayer creates flow-domain children with edge-keyed results', async () => {
		seedManifest(selection.scale);
		const flowsByEdge = new Map([
			['A|B', 50],
			['B|A', 40]
		]);
		const fittedByEdge = new Map([
			['A|B', 48],
			['B|A', 42]
		]);
		const residualByEdge = new Map([
			['A|B', 2],
			['B|A', -2]
		]);
		runSimMock.mockResolvedValue({
			coefficients: {
				name: ['(Intercept)', 'log(distance_km)', 'log(pop_o)', 'log(jobs_d)'],
				est: Float64Array.from([4.5, -1.2, 0.95, 0.88]),
				se: Float64Array.from([0.1, 0.05, 0.03, 0.04]),
				z: Float64Array.from([45, -24, 32, 22]),
				p: Float64Array.from([0, 0, 0, 0])
			},
			fit: {
				rSquared: 0.87,
				adjRSquared: 0.85,
				rmse: 4.1,
				aic: 220,
				bic: 232,
				meanResid: 0.01,
				varResid: 16,
				sorensen: 0.92
			},
			edgeKeys: ['A|B', 'B|A'],
			fitted: fittedByEdge,
			residual: residualByEdge
		});

		// Inject a flow filter layer + two node mass layers, all with results.
		const flowId = 'flow_test';
		const massOId = 'mass_o_test';
		const massDId = 'mass_d_test';
		layers.items = [
			{
				id: flowId,
				name: 'trips',
				slug: 'trips',
				kind: 'filter',
				domain: 'flow',
				scale: selection.scale,
				dataset: 'ovin',
				yearMin: 2018,
				yearMax: 2018,
				filters: {},
				includeSelfLoops: false
			},
			{
				id: massOId,
				name: 'pop',
				slug: 'pop',
				kind: 'filter',
				domain: 'node',
				scale: selection.scale,
				dataset: 'demographics',
				year: 2018,
				filters: {}
			},
			{
				id: massDId,
				name: 'jobs',
				slug: 'jobs',
				kind: 'filter',
				domain: 'node',
				scale: selection.scale,
				dataset: 'banen',
				year: 2018,
				filters: {}
			}
		];
		layers.results.set(flowId, flowsByEdge);
		layers.results.set(
			massOId,
			new Map([
				['A', 100],
				['B', 200]
			])
		);
		layers.results.set(
			massDId,
			new Map([
				['A', 1000],
				['B', 2000]
			])
		);

		const { parentId, fitPromise } = layers.saveModel({
			name: 'SIM A',
			family: 'sim',
			spec: {
				flowId,
				massOId,
				massDId,
				massOTransform: 'log',
				massDTransform: 'log'
			}
		});

		expect(parentId).toBeTruthy();
		const parent = layers.items.find((i) => i.id === parentId);
		expect(parent?.family).toBe('sim');
		// SIM parent + children are flow-domain (drives where they render).
		expect(parent?.domain).toBe('flow');
		const [fittedId, residualId] = parent.childIds;
		expect(layers.items.find((i) => i.id === fittedId)?.domain).toBe('flow');

		// Deterministic — fitPromise resolves when computeModelLayer +
		// recomputeCalcs both settle (no race between fire-and-forget chain
		// and explicit await).
		await fitPromise;

		// Surface any unexpected error before the modelFits assertion fails
		// — gives the test a useful message instead of "expected true got false".
		if (layers.errors.get(parentId)) {
			throw new Error(`SIM fit errored: ${layers.errors.get(parentId)}`);
		}

		expect(runSimMock).toHaveBeenCalled();
		const [args] = runSimMock.mock.calls[0];
		expect(args.flowName).toBe('trips');
		expect(args.massOName).toBe('pop');
		expect(args.massDName).toBe('jobs');
		expect(args.massOTransform).toBe('log');

		// Fit metadata + edge-keyed results populated.
		expect(layers.modelFits.has(parentId)).toBe(true);
		expect(layers.modelFits.get(parentId)?.fit.sorensen).toBe(0.92);
		expect(layers.results.get(fittedId)?.get('A|B')).toBe(48);
		expect(layers.results.get(residualId)?.get('B|A')).toBe(-2);
	});

	it('GWR mode creates per-coef + local_r2 + bw_actual children and distributes perNode maps', async () => {
		seedManifest(selection.scale);
		const { depId, covId } = seedDependentAndCovariate(selection.scale);
		const areaCodes = ['A', 'B', 'C'];
		const interceptM = new Map([
			['A', 1.1],
			['B', 1.2],
			['C', 1.3]
		]);
		const popM = new Map([
			['A', 0.5],
			['B', 0.55],
			['C', 0.6]
		]);
		const localR2M = new Map([
			['A', 0.81],
			['B', 0.82],
			['C', 0.83]
		]);
		const bwM = new Map([
			['A', 12.5],
			['B', 12.5],
			['C', 12.5]
		]);
		runGwrMock.mockResolvedValue({
			coefficients: {
				name: ['(Intercept)', 'pop'],
				est: Float64Array.from([1.2, 0.55]),
				se: Float64Array.from([0.1, 0.05]),
				z: Float64Array.from([12, 11]),
				p: Float64Array.from([0, 0])
			},
			fit: {
				rSquared: 0.82,
				adjRSquared: 0.8,
				rmse: 1.1,
				aic: NaN,
				bic: NaN,
				meanResid: 0,
				varResid: 1.2
			},
			areaCodes,
			fitted: new Map([
				['A', 100.1],
				['B', 200.5],
				['C', 300.7]
			]),
			residual: new Map([
				['A', -0.1],
				['B', -0.5],
				['C', -0.7]
			]),
			perNode: {
				betas: { '(Intercept)': interceptM, pop: popM },
				localR2: localR2M,
				bwActual: bwM
			}
		});

		const { parentId, fitPromise } = layers.saveModel({
			name: 'GWR test',
			family: 'nlm',
			spec: {
				dependentId: depId,
				covariateIds: [covId],
				glm: { family: 'gaussian', link: 'identity' },
				gwr: {
					enabled: true,
					kernelType: 'fixed',
					kernelShape: 'bi-square',
					bandwidth: 'auto'
				}
			}
		});
		expect(parentId).toBeTruthy();

		await fitPromise;
		if (layers.errors.get(parentId)) {
			throw new Error(`GWR fit errored: ${layers.errors.get(parentId)}`);
		}

		const parent = layers.items.find((i) => i.id === parentId);
		expect(parent?.kind).toBe('model');
		// Children: fitted + residual + 2 betas (intercept + pop) + local_r2 + bw_actual = 6
		expect(parent?.childIds?.length).toBe(6);

		const children = (parent?.childIds ?? []).map((cid) => layers.items.find((i) => i.id === cid));
		const byChannel = (ch) => children.filter((c) => c.channel === ch);
		expect(byChannel('fitted').length).toBe(1);
		expect(byChannel('residual').length).toBe(1);
		expect(byChannel('beta').length).toBe(2);
		expect(byChannel('local_r2').length).toBe(1);
		expect(byChannel('bw_actual').length).toBe(1);

		// Beta children carry coefName so computeModelLayer can route the right
		// perNode map to each one. Intercept slug → `<parent>_beta_intercept`.
		const intChild = byChannel('beta').find((c) => c.coefName === '(Intercept)');
		const popChild = byChannel('beta').find((c) => c.coefName === 'pop');
		expect(intChild?.slug).toBe('GWR_test_beta_intercept');
		expect(popChild?.slug).toBe('GWR_test_beta_pop');

		// runGwr was called with the GWR config from the spec.
		expect(runGwrMock).toHaveBeenCalled();
		const [args] = runGwrMock.mock.calls[0];
		expect(args.dependentName).toBe('jobs');
		expect(args.kernelType).toBe('fixed');
		expect(args.kernelShape).toBe('bi-square');
		expect(args.bandwidth).toBe('auto');
		expect(args.centroids).toBeDefined();

		// Per-coef + local_r2 + bw_actual results landed on the right children.
		expect(layers.results.get(intChild.id)?.get('A')).toBe(1.1);
		expect(layers.results.get(intChild.id)?.get('C')).toBe(1.3);
		expect(layers.results.get(popChild.id)?.get('B')).toBeCloseTo(0.55, 6);
		const lr2Child = byChannel('local_r2')[0];
		const bwChild = byChannel('bw_actual')[0];
		expect(layers.results.get(lr2Child.id)?.get('A')).toBe(0.81);
		expect(layers.results.get(bwChild.id)?.get('A')).toBe(12.5);

		// modelFits stores the global summary (mean coef across areas) just like
		// the NLM path — so ModelResults renders a coefficient table.
		expect(layers.modelFits.has(parentId)).toBe(true);
		expect(layers.modelFits.get(parentId)?.coefficients.name).toEqual(['(Intercept)', 'pop']);

		// Auto-activate: fitted child becomes active.
		const fittedChild = byChannel('fitted')[0];
		expect(layers.activeId).toBe(fittedChild.id);
	});

	it('immutable contract: a fitted model never re-fits, even after spec patch', async () => {
		// Under the immutable-snapshot contract, computeModelLayer is idempotent
		// once modelFits.has(id). Patching spec in-place doesn't trigger a
		// re-fit — that would be a NEW model the user has to explicitly build
		// via delete + recreate (or future "Re-fit" action).
		seedManifest(selection.scale);
		const { depId, covId } = seedDependentAndCovariate(selection.scale);
		runGwrMock.mockResolvedValue({
			coefficients: {
				name: ['(Intercept)', 'pop'],
				est: Float64Array.from([1, 0.5]),
				se: Float64Array.from([0.1, 0.05]),
				z: Float64Array.from([10, 10]),
				p: Float64Array.from([0, 0])
			},
			fit: {
				rSquared: 0.5,
				adjRSquared: 0.4,
				rmse: 1,
				aic: NaN,
				bic: NaN,
				meanResid: 0,
				varResid: 1
			},
			areaCodes: ['A', 'B', 'C'],
			fitted: new Map([
				['A', 1],
				['B', 2],
				['C', 3]
			]),
			residual: new Map([
				['A', 0],
				['B', 0],
				['C', 0]
			]),
			perNode: {
				betas: {
					'(Intercept)': new Map([
						['A', 1],
						['B', 1],
						['C', 1]
					]),
					pop: new Map([
						['A', 0.5],
						['B', 0.5],
						['C', 0.5]
					])
				},
				localR2: new Map([
					['A', 0.5],
					['B', 0.5],
					['C', 0.5]
				]),
				bwActual: new Map([
					['A', 10],
					['B', 10],
					['C', 10]
				])
			}
		});

		const { parentId, fitPromise } = layers.saveModel({
			name: 'GWR immut',
			family: 'nlm',
			spec: {
				dependentId: depId,
				covariateIds: [covId],
				glm: { family: 'gaussian', link: 'identity' },
				gwr: { enabled: true, kernelType: 'fixed', kernelShape: 'bi-square', bandwidth: 10 }
			}
		});
		await fitPromise;
		const calls0 = runGwrMock.mock.calls.length;
		expect(calls0).toBe(1);

		// Same-spec recompute → no-op (modelFits.has(id) short-circuits).
		await layers.computeModelLayer(parentId);
		expect(runGwrMock.mock.calls.length).toBe(calls0);

		// Patch the bandwidth in-place → still no re-fit. The previous version
		// would have invalidated the sig and triggered another runGwr call;
		// under the immutable contract, in-place spec edits are ignored.
		const idx = layers.items.findIndex((x) => x.id === parentId);
		layers.items = layers.items.map((x, k) =>
			k === idx
				? {
						...x,
						spec: { ...x.spec, gwr: { ...x.spec.gwr, bandwidth: 25 } }
					}
				: x
		);
		await layers.computeModelLayer(parentId);
		expect(runGwrMock.mock.calls.length).toBe(calls0);

		// Only way to re-fit: drop modelFits (simulating delete + recreate or
		// a future "Re-fit" action). Then the next computeModelLayer runs.
		layers.modelFits.delete(parentId);
		await layers.computeModelLayer(parentId);
		expect(runGwrMock.mock.calls.length).toBe(calls0 + 1);
		const [latestArgs] = runGwrMock.mock.calls.at(-1);
		expect(latestArgs.bandwidth).toBe(25); // picked up the spec patch
	});

	it('removing a model parent cascades child removal', async () => {
		runNlmMock.mockResolvedValue(syntheticFit({ areaCodes: ['A'] }));
		const { depId, covId } = seedDependentAndCovariate(selection.scale);
		const { parentId, fittedId, residualId } = injectModelEntry({
			depId,
			covId,
			scale: selection.scale
		});
		await layers.computeModelLayer(parentId);

		layers.remove(parentId);

		expect(layers.items.find((i) => i.id === parentId)).toBeUndefined();
		for (const cid of [fittedId, residualId]) {
			expect(layers.items.find((i) => i.id === cid)).toBeUndefined();
			expect(layers.results.has(cid)).toBe(false);
		}
		expect(layers.modelFits.has(parentId)).toBe(false);
		expect(layers.activeId).toBeNull();
	});

	// ── saveModel validation gates (added in the cleanup pass) ──────────
	it('saveModel rejects an NLM with a non-existent weights layer id', () => {
		const { depId, covId } = seedDependentAndCovariate(selection.scale);
		expect(() =>
			layers.saveModel({
				name: 'Bad weights',
				family: 'nlm',
				spec: {
					dependentId: depId,
					covariateIds: [covId],
					glm: { family: 'gaussian', link: 'identity' },
					weightsId: 'nonexistent_id'
				}
			})
		).toThrow(/Weights layer not found/i);
	});

	it('saveModel rejects an NLM with an offset that is on the wrong domain', () => {
		const { depId, covId } = seedDependentAndCovariate(selection.scale);
		// Inject a flow-domain layer; using it as an NLM offset is illegal.
		const badId = 'bad_flow';
		layers.items = [
			...layers.items,
			{
				id: badId,
				name: 'flow_bad',
				slug: 'flow_bad',
				kind: 'filter',
				domain: 'flow',
				scale: selection.scale,
				dataset: 'ovin',
				yearMin: 2018,
				yearMax: 2018,
				filters: {}
			}
		];
		expect(() =>
			layers.saveModel({
				name: 'Bad offset',
				family: 'nlm',
				spec: {
					dependentId: depId,
					covariateIds: [covId],
					glm: { family: 'gaussian', link: 'identity' },
					offsetId: badId
				}
			})
		).toThrow(/Offset layer .* must be node-domain/i);
	});

	it('saveModel rejects GWR with an invalid kernelShape', () => {
		const { depId, covId } = seedDependentAndCovariate(selection.scale);
		expect(() =>
			layers.saveModel({
				name: 'Bad GWR',
				family: 'nlm',
				spec: {
					dependentId: depId,
					covariateIds: [covId],
					glm: { family: 'gaussian', link: 'identity' },
					gwr: { enabled: true, kernelType: 'fixed', kernelShape: 'triangle', bandwidth: 10 }
				}
			})
		).toThrow(/kernelShape/i);
	});

	it('saveModel rejects GWR with bandwidth that is neither "auto" nor a positive number', () => {
		const { depId, covId } = seedDependentAndCovariate(selection.scale);
		expect(() =>
			layers.saveModel({
				name: 'Bad bw',
				family: 'nlm',
				spec: {
					dependentId: depId,
					covariateIds: [covId],
					glm: { family: 'gaussian', link: 'identity' },
					gwr: { enabled: true, kernelType: 'fixed', kernelShape: 'bi-square', bandwidth: -5 }
				}
			})
		).toThrow(/bandwidth/i);
	});

	it('saveModel rejects SIM compDest with a non-positive decay', () => {
		seedManifest(selection.scale);
		// Need a flow + two node layers in items[] for SIM validation to pass
		// the dependent / mass checks before reaching compDest.
		layers.items = [
			{
				id: 'f',
				name: 'flow',
				slug: 'flow',
				kind: 'filter',
				domain: 'flow',
				scale: selection.scale,
				dataset: 'ovin',
				yearMin: 2018,
				yearMax: 2018,
				filters: {},
				includeSelfLoops: false
			},
			{
				id: 'mO',
				name: 'pop',
				slug: 'pop',
				kind: 'filter',
				domain: 'node',
				scale: selection.scale,
				dataset: 'pop',
				year: 2018,
				filters: {}
			},
			{
				id: 'mD',
				name: 'jobs',
				slug: 'jobs',
				kind: 'filter',
				domain: 'node',
				scale: selection.scale,
				dataset: 'jobs',
				year: 2018,
				filters: {}
			}
		];
		expect(() =>
			layers.saveModel({
				name: 'Bad compDest',
				family: 'sim',
				spec: {
					flowId: 'f',
					massOId: 'mO',
					massDId: 'mD',
					compDest: { kernel: 'exp', decay: 0 }
				}
			})
		).toThrow(/compDest\.decay/i);
	});

	// ── A1/A2: immutability + first-fit-only auto-activate ────────────
	it('A1: clicking a residual child does not trigger another fit', async () => {
		runNlmMock.mockResolvedValue(syntheticFit({ areaCodes: ['A', 'B', 'C'] }));
		const { depId, covId } = seedDependentAndCovariate(selection.scale);
		const { parentId, fitPromise } = layers.saveModel({
			name: 'A1Test',
			family: 'nlm',
			spec: {
				dependentId: depId,
				covariateIds: [covId],
				glm: { family: 'gaussian', link: 'identity' }
			}
		});
		await fitPromise;
		const callsAfterFit = runNlmMock.mock.calls.length;
		expect(callsAfterFit).toBe(1);
		// Auto-activated fitted child.
		const parent = layers.items.find((i) => i.id === parentId);
		const residualChild = parent.childIds
			.map((cid) => layers.items.find((i) => i.id === cid))
			.find((c) => c.channel === 'residual');
		expect(residualChild).toBeTruthy();
		layers.setActive(residualChild.id);
		// Trigger any path that previously caused a re-fit (recomputeCalcs).
		await layers.recomputeCalcs();
		// activeId stays on residual; runNlm count unchanged.
		expect(layers.activeId).toBe(residualChild.id);
		expect(runNlmMock.mock.calls.length).toBe(callsAfterFit);
	});

	// ── A2: model outputs feeding another model don't cascade re-fits ──
	it('A2: model B that depends on model A does not refit when nothing changed', async () => {
		runNlmMock.mockResolvedValue(syntheticFit({ areaCodes: ['A', 'B', 'C'] }));
		const { depId, covId } = seedDependentAndCovariate(selection.scale);
		const { parentId: aId, fitPromise: aFit } = layers.saveModel({
			name: 'A',
			family: 'nlm',
			spec: {
				dependentId: depId,
				covariateIds: [covId],
				glm: { family: 'gaussian', link: 'identity' }
			}
		});
		await aFit;
		const aFitted = layers.items
			.find((i) => i.id === aId)
			.childIds.map((cid) => layers.items.find((i) => i.id === cid))
			.find((c) => c.channel === 'fitted');
		// Build B using A's fitted as the dependent.
		const { parentId: bId, fitPromise: bFit } = layers.saveModel({
			name: 'B',
			family: 'nlm',
			spec: {
				dependentId: aFitted.id,
				covariateIds: [covId],
				glm: { family: 'gaussian', link: 'identity' }
			}
		});
		await bFit;
		const callsAfter = runNlmMock.mock.calls.length;
		// Recompute both — under the immutable contract, neither fires runNlm.
		await layers.computeModelLayer(aId);
		await layers.computeModelLayer(bId);
		expect(runNlmMock.mock.calls.length).toBe(callsAfter);
	});

	// ── A3: fractional y triggers a fit note ─────────────────────────────
	it('A3: fractional flow counts emit a "weighted counts rounded" note', async () => {
		seedManifest(selection.scale);
		runSimMock.mockResolvedValue({
			coefficients: {
				name: ['(Intercept)', 'log(distance_km)', 'log(m_o)', 'log(m_d)'],
				est: Float64Array.from([1, -1, 1, 1]),
				se: Float64Array.from([0.1, 0.05, 0.05, 0.05]),
				z: Float64Array.from([10, -20, 20, 20]),
				p: Float64Array.from([0, 0, 0, 0])
			},
			fit: {
				rSquared: 0.5,
				adjRSquared: 0.4,
				rmse: 1,
				aic: 10,
				bic: 12,
				meanResid: 0,
				varResid: 1,
				sorensen: 0.5
			},
			edgeKeys: ['A|B', 'B|A'],
			fitted: new Map([
				['A|B', 1],
				['B|A', 2]
			]),
			residual: new Map([
				['A|B', 0],
				['B|A', 0]
			])
		});
		const flowId = 'flow_frac';
		const massOId = 'm_o';
		const massDId = 'm_d';
		layers.items = [
			{
				id: flowId,
				name: 'trips',
				slug: 'trips',
				kind: 'filter',
				domain: 'flow',
				scale: selection.scale,
				dataset: 'ovin',
				yearMin: 2018,
				yearMax: 2018,
				filters: {},
				includeSelfLoops: false
			},
			{
				id: massOId,
				name: 'pop',
				slug: 'pop',
				kind: 'filter',
				domain: 'node',
				scale: selection.scale,
				dataset: 'pop',
				year: 2018,
				filters: {}
			},
			{
				id: massDId,
				name: 'jobs',
				slug: 'jobs',
				kind: 'filter',
				domain: 'node',
				scale: selection.scale,
				dataset: 'jobs',
				year: 2018,
				filters: {}
			}
		];
		// One fractional flow value triggers the note.
		layers.results.set(
			flowId,
			new Map([
				['A|B', 1.282],
				['B|A', 2]
			])
		);
		layers.results.set(
			massOId,
			new Map([
				['A', 100],
				['B', 200]
			])
		);
		layers.results.set(
			massDId,
			new Map([
				['A', 1000],
				['B', 2000]
			])
		);
		const { parentId, fitPromise } = layers.saveModel({
			name: 'SIM frac',
			family: 'sim',
			spec: { flowId, massOId, massDId }
		});
		await fitPromise;
		const notes = layers.modelNotes.get(parentId);
		expect(notes).toBeTruthy();
		expect(notes.some((n) => /rounded/i.test(n))).toBe(true);
	});
});
