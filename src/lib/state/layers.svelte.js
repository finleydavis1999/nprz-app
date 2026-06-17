// Saved layers + calc layers, domain-aware.
//
// A filter layer is a snapshot of either the node-side `selection` (dataset/
// year/filters → choropleth values) or the flow-side `flow` state (dataset/
// year-range/filters → OD edge values). A calc layer is a math.js expression
// over other layer slugs of the same domain, optionally wrapping flow inputs
// with `inflow(...)` / `outflow(...)` / `net(...)` to produce node-domain
// output. A smooth layer is a spatially-lagged (distance-decay-weighted)
// version of a node-domain input layer, computed in a Web Worker from RD
// centroids. A model layer is a webR-fit GLM / spatial-interaction model; it
// owns a set of `model-output` child layers (fitted, residual, per-coefficient
// betas for GWR) each of which renders through the standard `displayed`
// facade.
//
// Persisted to localStorage under v2; v1 records (no domain) are migrated to
// node-domain on first load. Smooth-layer + model fields are additive — no
// schema bump.

import { SvelteMap, SvelteSet } from 'svelte/reactivity';
import { runChoropleth } from '$lib/data/query.js';
import { runFlows } from '$lib/data/flowQuery.js';
import {
	parseExpression,
	evaluateOverAreas,
	slugify,
	uniqueLayerName
} from '$lib/data/layer-calc.js';
import { aggregateFlow } from '$lib/data/flow-aggregations.js';
import { computeSmooth } from '$lib/data/spatial-lag-client.js';
import { dataUrl } from '$lib/data/url.js';
import { runNlm, runSim, runGwr } from '$lib/models/model-runner.js';
import { decorateName } from '$lib/models/design-matrix.js';
import { selection } from './selection.svelte.js';
import { flow } from './flow.svelte.js';
import { queryResult } from './query-result.svelte.js';
import { manifestState } from './manifest.svelte.js';
import { centroidsState } from './centroids.svelte.js';
import { studyArea } from './study-area.svelte.js';

const STORAGE_KEY_V1 = 'nprz.layers.v1';
const STORAGE_KEY = 'nprz.layers.v2';

function newId() {
	return `l_${Math.random().toString(36).slice(2, 10)}`;
}

function flowEdgeKey(o, d) {
	return `${o}|${d}`;
}

// Predict the GWR coefficient-name list from an NLM spec at saveModel time.
// We need this up-front so we can pre-create one `beta_*` child layer per
// coefficient before the fit runs (children carry stable IDs/slugs that
// survive across re-fits). The names must match exactly what r/gwr.R emits
// in `coefficients$name` / `perNode$betas` — that's `(Intercept)` followed
// by `decorateName(lag?(slug,...), transform)` per covariate, in spec order.
// Same construction as the runGwr covariate-name loop.
function predictGwrCoefNames(spec, byId) {
	const names = ['(Intercept)'];
	const tx = spec.covariateTransforms ?? {};
	const lags = spec.covariateLags ?? {};
	for (const cid of spec.covariateIds ?? []) {
		const c = byId.get(cid);
		if (!c) continue;
		let term = c.slug;
		const lag = lags[cid];
		if (lag) term = `lag(${term},${lag.kernel},${lag.decay})`;
		names.push(decorateName(term, tx[cid]));
	}
	return names;
}

// Filter a Map<areaCode, value> to keys present in the study area set.
// Used by computeModelLayer when spec.studyAreaScoped === true. Returns a
// new plain Map (preserves insertion order of the source). Empty result is
// the caller's responsibility — saveModel won't have validated the study
// area's content yet (the user might have toggled scoping on after a fit).
function filterByStudyArea(values, studyAreaIds) {
	// eslint-disable-next-line svelte/prefer-svelte-reactivity -- pure local filter; result is a snapshot passed to runX, not reactive state.
	const out = new Map();
	for (const [k, v] of values) {
		if (studyAreaIds.has(k)) out.set(k, v);
	}
	return out;
}

// Slugify a GWR coefficient name into a child-layer suffix. `(Intercept)` →
// `intercept`, `log(pop)` → `log_pop`, etc. Mirrors the cleanup slugify() in
// layer-calc.js but lowercases for visual consistency with the other
// auto-generated child slugs (`<parent>_fitted`, `_residual`).
function gwrCoefSuffix(coefName) {
	if (coefName === '(Intercept)') return 'intercept';
	return coefName
		.toLowerCase()
		.replace(/[^a-z0-9_]+/g, '_')
		.replace(/^_+|_+$/g, '');
}

class LayersState {
	/** @type {Array<{
	 *   id: string, name: string, slug: string,
	 *   kind: 'filter' | 'calc' | 'smooth' | 'model' | 'model-output',
	 *   domain: 'node' | 'flow',
	 *   scale: string,
	 *   dataset?: string, year?: number,
	 *   yearMin?: number, yearMax?: number,
	 *   ageMin?: number, ageMax?: number,
	 *   filters?: Record<string, number[]>,
	 *   includeSelfLoops?: boolean,
	 *   expression?: string,
	 *   inputId?: string, kernel?: 'exp' | 'gauss' | 'power',
	 *   decay?: number, maxDist?: number,
	 *   mode?: 'mean' | 'sum', includeSelf?: boolean,
	 *   family?: 'nlm' | 'sim',
	 *   spec?: any,
	 *   childIds?: string[],
	 *   parentId?: string,
	 *   channel?: 'fitted' | 'residual' | 'beta' | 'local_r2' | 'bw_actual',
	 *   coefName?: string
	 * }>} */
	items = $state([]);
	/** @type {SvelteMap<string, Map<string, number>>} */
	results = new SvelteMap();
	/** @type {SvelteSet<string>} */
	loading = new SvelteSet();
	/** @type {SvelteMap<string, string>} */
	errors = new SvelteMap();
	activeId = $state(/** @type {string | null} */ (null));
	/** Per-smooth-layer request token — discards results of superseded async
	 *  recomputes (e.g. from rapid slider drags). Non-reactive bookkeeping.
	 *  @type {Map<string, number>} */
	#smoothSeq = new Map();
	/** Per-model-layer request token — same purpose as #smoothSeq but for
	 *  webR fits. @type {Map<string, number>} */
	#modelSeq = new Map();
	/** Transient (non-persisted) fit metadata per model-parent id: coefficient
	 *  table + scalar fit stats. The map shape mirrors what r/nlm.R returns
	 *  minus per-observation arrays (those live in `results` keyed by child id).
	 *  @type {SvelteMap<string, {
	 *    coefficients: { name: string[], est: number[], se: number[], z: number[], p: number[] },
	 *    fit: { rSquared: number, adjRSquared: number, rmse: number, aic: number, bic: number,
	 *           meanResid: number, varResid: number }
	 *  }>} */
	modelFits = new SvelteMap();
	/** Transient non-error notices per model — currently used for "weighted
	 *  counts were rounded for Poisson fit" but designed to hold any
	 *  human-readable advisory string. Read by ModelResults to show a small
	 *  notice strip under the fit stats. @type {SvelteMap<string, string[]>} */
	modelNotes = new SvelteMap();
	/** Live status string per model id while a fit is in progress —
	 *  "Installing R packages…", "Building design matrix…", "Fitting model…",
	 *  "Distributing per-area results…". Cleared when loading flips off.
	 *  ModelResults + ModelDock both read it.
	 *  @type {SvelteMap<string, string>} */
	modelStatus = new SvelteMap();
	/** Set of model-parent ids we've already auto-activated the fitted child
	 *  for. Auto-activate is a first-fit-only convenience — page-reload
	 *  rehydration shouldn't override the user's persisted active selection,
	 *  and re-fits never happen in our immutable-snapshot contract anyway.
	 *  @type {Set<string>} */
	#autoActivated = new Set();

	load() {
		if (typeof localStorage === 'undefined') return;
		try {
			const rawV2 = localStorage.getItem(STORAGE_KEY);
			if (rawV2) {
				const parsed = JSON.parse(rawV2);
				if (Array.isArray(parsed?.items)) this.items = parsed.items;
				if (typeof parsed?.activeId === 'string' || parsed?.activeId === null) {
					this.activeId = parsed.activeId ?? null;
				}
				return;
			}
			const rawV1 = localStorage.getItem(STORAGE_KEY_V1);
			if (!rawV1) return;
			const parsed = JSON.parse(rawV1);
			if (Array.isArray(parsed?.items)) {
				// v1 had no flow layers, so everything migrates to domain='node'.
				this.items = parsed.items.map((i) => ({ ...i, domain: 'node' }));
			}
			if (typeof parsed?.activeId === 'string' || parsed?.activeId === null) {
				this.activeId = parsed.activeId ?? null;
			}
			this.persist();
		} catch {
			// corrupted storage just resets to empty
		}
	}

	persist() {
		if (typeof localStorage === 'undefined') return;
		try {
			localStorage.setItem(
				STORAGE_KEY,
				JSON.stringify({ items: this.items, activeId: this.activeId })
			);
		} catch {
			// quota / private mode — non-fatal
		}
	}

	slugTaken(slug, exceptId = null) {
		return this.items.some((i) => i.slug === slug && i.id !== exceptId);
	}

	/** Resolve a base display name to one whose slug is free (auto-suffixed). */
	uniqueName(base) {
		return uniqueLayerName(base, (s) => this.slugTaken(s));
	}

	/** Snapshot the current node-side `selection` into a filter layer. */
	saveCurrent(name) {
		const slug = slugify(name);
		if (!slug || this.slugTaken(slug)) return null;
		const id = newId();
		this.items = [
			...this.items,
			{
				id,
				name: name.trim(),
				slug,
				kind: 'filter',
				domain: 'node',
				scale: selection.scale,
				dataset: selection.dataset,
				year: selection.year,
				filters: structuredClone($state.snapshot(selection.filters))
			}
		];
		this.persist();
		this.refreshFilterLayer(id).then(() => this.recomputeCalcs());
		return id;
	}

	/** Snapshot the current flow-side `flow` state into a flow-domain filter layer. */
	saveCurrentFlow(name) {
		const slug = slugify(name);
		if (!slug || this.slugTaken(slug)) return null;
		const id = newId();
		this.items = [
			...this.items,
			{
				id,
				name: name.trim(),
				slug,
				kind: 'filter',
				domain: 'flow',
				scale: flow.scale,
				dataset: flow.dataset,
				yearMin: flow.yearMin,
				yearMax: flow.yearMax,
				ageMin: flow.ageMin,
				ageMax: flow.ageMax,
				filters: structuredClone($state.snapshot(flow.filters)),
				toggles: structuredClone($state.snapshot(flow.toggles)),
				includeSelfLoops: flow.includeSelfLoops
			}
		];
		this.persist();
		this.refreshFilterLayer(id).then(() => this.recomputeCalcs());
		return id;
	}

	saveCalc(name, expression, domain = 'node') {
		const slug = slugify(name);
		if (!slug || this.slugTaken(slug)) return null;
		// Validate parse, aggregator usage, and that referenced symbols exist on
		// the same scale + are domain-compatible.
		const { symbols, aggs } = parseExpression(expression);
		const sameScale = this.items.filter((i) => i.scale === selection.scale);
		const bySlug = new Map(sameScale.map((i) => [i.slug, i]));
		for (const { aggName, slug: aggSlug } of aggs) {
			const dep = bySlug.get(aggSlug);
			if (!dep) throw new Error(`Unknown layer: ${aggSlug}`);
			if (dep.domain !== 'flow') {
				throw new Error(`${aggName}() needs a flow layer; '${aggSlug}' is ${dep.domain}`);
			}
		}
		const synthSlugs = new Set(aggs.map((a) => a.synthSlug));
		for (const s of symbols) {
			if (synthSlugs.has(s)) continue;
			const dep = bySlug.get(s);
			if (!dep) throw new Error(`Unknown layer: ${s}`);
			if (dep.domain !== domain) {
				throw new Error(`'${s}' is ${dep.domain}-domain; can't use directly in a ${domain} calc`);
			}
		}
		const id = newId();
		this.items = [
			...this.items,
			{
				id,
				name: name.trim(),
				slug,
				kind: 'calc',
				domain,
				scale: selection.scale,
				expression
			}
		];
		this.persist();
		this.computeCalcLayer(id);
		return id;
	}

	/** Create a smooth layer: a spatially-lagged version of a node-domain input
	 *  layer. `decay` and `maxDist` are in kilometres; `decay` is instead a
	 *  positive exponent when `kernel` is 'power'. */
	saveSmooth(name, { inputId, kernel, decay, maxDist, mode, includeSelf }) {
		const slug = slugify(name);
		if (!slug) throw new Error('Name required');
		if (this.slugTaken(slug)) throw new Error('Name already in use');
		const input = this.items.find((i) => i.id === inputId);
		if (!input) throw new Error('Pick an input layer to smooth');
		if ((input.domain ?? 'node') !== 'node') throw new Error('Input must be a node layer');
		if (input.scale !== selection.scale) throw new Error('Input layer is on a different scale');
		const id = newId();
		this.items = [
			...this.items,
			{
				id,
				name: name.trim(),
				slug,
				kind: 'smooth',
				domain: 'node',
				scale: selection.scale,
				inputId,
				kernel,
				decay,
				maxDist,
				mode,
				includeSelf
			}
		];
		this.persist();
		this.computeSmoothLayer(id);
		return id;
	}

	remove(id) {
		const target = this.items.find((i) => i.id === id);
		// Cascade: removing a model parent removes its child output layers too.
		// (We don't allow orphaning children — they're meaningless without their
		// parent's spec to recompute from.)
		// eslint-disable-next-line svelte/prefer-svelte-reactivity -- local lookup, not a reactive store
		const cascadeIds = new Set([id]);
		if (target?.kind === 'model') {
			for (const cid of target.childIds ?? []) cascadeIds.add(cid);
		}
		this.items = this.items.filter((i) => !cascadeIds.has(i.id));
		for (const cid of cascadeIds) {
			this.results.delete(cid);
			this.loading.delete(cid);
			this.errors.delete(cid);
		}
		if (target?.kind === 'model') {
			this.modelFits.delete(id);
			this.modelNotes.delete(id);
			this.#autoActivated.delete(id);
			this.#modelSeq.delete(id);
		}
		if (this.activeId && cascadeIds.has(this.activeId)) this.activeId = null;
		this.persist();
		this.recomputeCalcs();
	}

	clearAll() {
		this.items = [];
		this.results.clear();
		this.loading.clear();
		this.errors.clear();
		// Model-related transient state — keep in sync with `remove()`'s
		// cascade. Without this, the dock would still show stale model fits
		// / notes / status text after the user nukes everything from the
		// SavedLayers "Remove all" button.
		this.modelFits.clear();
		this.modelNotes.clear();
		this.modelStatus.clear();
		this.#autoActivated.clear();
		this.#modelSeq.clear();
		this.activeId = null;
		this.persist();
	}

	setActive(id) {
		this.activeId = id;
		this.persist();
	}

	/** Recompute every derived (smooth + calc + model) layer at the current scale. */
	recomputeCalcs() {
		this.#recomputeDerived(0);
	}

	/** Recompute derived layers from index `fromIdx` onward, in array order.
	 *  Array (creation) order is a valid dependency order — a layer can only
	 *  reference inputs that already existed when it was created. Smooth and
	 *  model layers are `await`ed (async, Worker- or webR-backed) so a later
	 *  calc that references their outputs sees fresh results. Model-output
	 *  child entries are skipped here — they're populated as a side-effect of
	 *  the parent's recompute. */
	async #recomputeDerived(fromIdx = 0) {
		for (let k = fromIdx; k < this.items.length; k++) {
			const l = this.items[k];
			if (!l || l.scale !== selection.scale) continue;
			if (l.kind === 'smooth') await this.computeSmoothLayer(l.id);
			else if (l.kind === 'calc') this.computeCalcLayer(l.id);
			else if (l.kind === 'model') await this.computeModelLayer(l.id);
		}
	}

	async refreshFilterLayer(id) {
		const layer = this.items.find((i) => i.id === id);
		if (!layer || layer.kind !== 'filter') return;
		this.loading.add(id);
		this.errors.delete(id);
		try {
			if (layer.domain === 'flow') {
				const res = await runFlows({
					dataset: layer.dataset,
					scale: layer.scale,
					yearMin: layer.yearMin,
					yearMax: layer.yearMax,
					ageMin: layer.ageMin,
					ageMax: layer.ageMax,
					filters: layer.filters,
					toggles: layer.toggles,
					includeSelfLoops: layer.includeSelfLoops
				});
				const map = new SvelteMap();
				for (const f of res.flows) map.set(flowEdgeKey(f.o, f.d), f.value);
				this.results.set(id, map);
			} else {
				const data = await runChoropleth({
					dataset: layer.dataset,
					scale: layer.scale,
					year: layer.year,
					filters: layer.filters
				});
				this.results.set(id, data);
			}
		} catch (e) {
			this.errors.set(id, /** @type {Error} */ (e)?.message ?? String(e));
		} finally {
			this.loading.delete(id);
		}
	}

	computeCalcLayer(id) {
		const layer = this.items.find((i) => i.id === id);
		if (!layer || layer.kind !== 'calc') return;
		try {
			const { compiled, symbols, aggs } = parseExpression(layer.expression ?? '');
			const slugToLayer = new Map(
				this.items.filter((i) => i.scale === layer.scale).map((i) => [i.slug, i])
			);
			const inputs = new SvelteMap();

			// Pre-compute aggregator outputs.
			for (const { aggName, slug, synthSlug } of aggs) {
				const dep = slugToLayer.get(slug);
				if (!dep || dep.domain !== 'flow') {
					this.errors.set(id, `${aggName}() needs a flow layer; '${slug}' missing or wrong domain`);
					return;
				}
				const flowData = this.results.get(dep.id);
				if (!flowData) {
					this.errors.set(id, `missing flow input: ${slug}`);
					return;
				}
				inputs.set(synthSlug, aggregateFlow(flowData, aggName));
			}

			// Direct (non-aggregator) symbol references must match domain.
			for (const s of symbols) {
				if (inputs.has(s)) continue;
				const dep = slugToLayer.get(s);
				if (!dep) {
					this.errors.set(id, `missing input: ${s}`);
					return;
				}
				if (dep.domain !== layer.domain) {
					this.errors.set(id, `'${s}' is ${dep.domain}; calc is ${layer.domain}`);
					return;
				}
				const data = this.results.get(dep.id);
				if (!data) {
					this.errors.set(id, `missing input: ${s}`);
					return;
				}
				inputs.set(s, data);
			}

			const out = evaluateOverAreas(compiled, symbols, inputs);
			this.results.set(id, out);
			this.errors.delete(id);
		} catch (e) {
			this.errors.set(id, /** @type {Error} */ (e)?.message ?? String(e));
		}
	}

	/** Compute a smooth layer — a distance-decay-weighted aggregate of its input
	 *  layer's per-node values. Async: the work runs in the spatial-lag Worker.
	 *  A per-layer request token discards results of superseded calls. */
	async computeSmoothLayer(id) {
		const layer = this.items.find((i) => i.id === id);
		if (!layer || layer.kind !== 'smooth') return;
		const seq = (this.#smoothSeq.get(id) ?? 0) + 1;
		this.#smoothSeq.set(id, seq);
		this.loading.add(id);
		this.errors.delete(id);
		try {
			const input = this.items.find((i) => i.id === layer.inputId);
			if (!input) {
				this.errors.set(id, 'input layer was deleted');
				return;
			}
			const values = this.results.get(layer.inputId);
			if (!values) {
				this.errors.set(id, `input not computed: ${input.name}`);
				return;
			}
			const geo = manifestState.data?.geo?.[layer.scale];
			const version = manifestState.data?.version;
			if (!geo?.centroidsRd || !version) {
				this.errors.set(id, 'centroids unavailable');
				return;
			}
			// Layer params are in km; the worker and RD centroids are in metres.
			// The power kernel's `decay` is a dimensionless exponent — not scaled.
			const out = await computeSmooth({
				scale: layer.scale,
				centroidsUrl: dataUrl(geo.centroidsRd, version),
				maxDist: layer.maxDist * 1000,
				kernel: layer.kernel,
				decay: layer.kernel === 'power' ? layer.decay : layer.decay * 1000,
				mode: layer.mode,
				includeSelf: layer.includeSelf,
				values: Object.fromEntries(values)
			});
			if (this.#smoothSeq.get(id) !== seq) return; // superseded by a newer request
			this.results.set(id, out);
			this.errors.delete(id);
		} catch (e) {
			if (this.#smoothSeq.get(id) === seq) {
				this.errors.set(id, /** @type {Error} */ (e)?.message ?? String(e));
			}
		} finally {
			if (this.#smoothSeq.get(id) === seq) this.loading.delete(id);
		}
	}

	// ── Model layers (NLM phase 1; SIM / GWR follow in later phases) ─────────
	//
	// A `'model'` parent owns a `spec` (family, glm, dependentId, covariateIds)
	// and a list of `childIds` pointing at `'model-output'` entries. Each child
	// represents one channel of the fit's per-observation output (fitted /
	// residual today; per-coefficient + local_r2 with GWR later). Children are
	// regular items[] entries with their own slug, so they render via the
	// existing `displayed` facade and can be referenced by calc expressions.
	//
	// Persistence: parents and child stubs are serialised as plain objects;
	// the fit metadata (`modelFits`) and per-area results are *not* persisted,
	// so a reload triggers a re-fit on first activation via `refreshAll`.

	/** Create a model parent + fitted/residual child stubs, and kick off the fit.
	 *  Returns `{ parentId, fitPromise }`. `parentId` is available synchronously
	 *  (callers that just need the slug can ignore the promise); `fitPromise`
	 *  resolves when the first `computeModelLayer` settles — success OR a
	 *  recorded error (`layers.errors.get(parentId)`). Lets the dock form
	 *  reset only after a real fit lands, and lets tests deterministically
	 *  await instead of polling `loading.has`.
	 *  Branches on `family`: NLM produces node-domain children; SIM produces
	 *  flow-domain children whose results are keyed by edgeKey ("o|d").
	 *  @param {{ name: string, family: 'nlm' | 'sim', spec: any }} opts
	 *  @returns {{ parentId: string, fitPromise: Promise<void> }} */
	saveModel({ name, family, spec }) {
		if (family !== 'nlm' && family !== 'sim') {
			throw new Error(`Unsupported model family: ${family}`);
		}
		const slug = slugify(name);
		if (!slug) throw new Error('Name required');
		if (this.slugTaken(slug)) throw new Error('Name already in use');

		const sameScale = this.items.filter((i) => i.scale === selection.scale);
		const byId = new Map(sameScale.map((i) => [i.id, i]));

		// Optional-input validator: when a spec refers to a layer by id we want
		// the failure mode to be a clear up-front error at save time rather than
		// a confusing webR stack trace mid-fit. `requireDomain` enforces the
		// shape expected at the call site.
		const requireOptionalLayer = (id, role, requireDomain = 'node') => {
			if (!id) return;
			const layer = byId.get(id);
			if (!layer) throw new Error(`${role} layer not found on this scale`);
			if ((layer.domain ?? 'node') !== requireDomain) {
				throw new Error(`${role} layer '${layer.name}' must be ${requireDomain}-domain`);
			}
		};

		if (family === 'nlm') {
			// Validate dependent + covariates: node-domain, on the current scale.
			const dep = byId.get(spec.dependentId);
			if (!dep) throw new Error('Dependent layer not found on this scale');
			if ((dep.domain ?? 'node') !== 'node') {
				throw new Error('Dependent must be a node-domain layer');
			}
			for (const cid of spec.covariateIds) {
				const c = byId.get(cid);
				if (!c) throw new Error(`Covariate not found: ${cid}`);
				if ((c.domain ?? 'node') !== 'node') {
					throw new Error(`Covariate '${c.name}' must be node-domain`);
				}
				if (cid === spec.dependentId) {
					throw new Error('Covariate cannot equal the dependent');
				}
			}
			requireOptionalLayer(spec.weightsId, 'Weights');
			requireOptionalLayer(spec.offsetId, 'Offset');
			// GWR config: only validated when enabled. Mirrors the runGwr
			// argument contract — kernel type/shape are enum-checked, bandwidth
			// is either 'auto' or a positive finite number.
			if (spec.gwr?.enabled) {
				const { kernelType, kernelShape, bandwidth } = spec.gwr;
				if (!['fixed', 'adaptive'].includes(kernelType)) {
					throw new Error(`GWR kernelType must be 'fixed' or 'adaptive' (got ${kernelType})`);
				}
				if (!['bi-square', 'gaussian'].includes(kernelShape)) {
					throw new Error(`GWR kernelShape must be 'bi-square' or 'gaussian' (got ${kernelShape})`);
				}
				if (bandwidth !== 'auto' && !(Number.isFinite(bandwidth) && bandwidth > 0)) {
					throw new Error(`GWR bandwidth must be 'auto' or a positive number (got ${bandwidth})`);
				}
			}
		} else {
			// SIM: dependent must be a flow layer; massO + massD must be node layers.
			const dep = byId.get(spec.flowId);
			if (!dep) throw new Error('Flow layer not found on this scale');
			if ((dep.domain ?? 'node') !== 'flow') {
				throw new Error('SIM dependent must be a flow-domain layer');
			}
			const mO = byId.get(spec.massOId);
			const mD = byId.get(spec.massDId);
			if (!mO || !mD) throw new Error('Mass layers not found on this scale');
			if ((mO.domain ?? 'node') !== 'node' || (mD.domain ?? 'node') !== 'node') {
				throw new Error('Mass layers must be node-domain');
			}
			requireOptionalLayer(spec.offsetId, 'Offset');
			// Study-area scope mode is SIM-specific. Default 'within' if
			// missing; reject any other value so a hand-edited storage entry
			// can't silently mean 'touches'.
			if (spec.studyAreaScoped) {
				const mode = spec.simScopeMode ?? 'within';
				if (!['within', 'touches'].includes(mode)) {
					throw new Error(`SIM simScopeMode must be 'within' or 'touches' (got ${mode})`);
				}
			}
			// CompDest has no layer id (the destination-mass series is reused via
			// massDId), but the kernel/decay tuple should be sanity-checked here
			// so we don't ship garbage into sim-design.js.
			if (spec.compDest) {
				const { kernel, decay } = spec.compDest;
				if (!['exp', 'gauss', 'power'].includes(kernel)) {
					throw new Error(`compDest.kernel must be exp/gauss/power (got ${kernel})`);
				}
				if (!(Number.isFinite(decay) && decay > 0)) {
					throw new Error(`compDest.decay must be a positive number (got ${decay})`);
				}
			}
		}

		const parentId = newId();
		const fittedId = newId();
		const residualId = newId();
		const fittedSlug = this.#freeChildSlug(slug, 'fitted');
		const residualSlug = this.#freeChildSlug(slug, 'residual');
		// Children's domain matches the dependent's: NLM → node; SIM → flow.
		// Drives whether they show up as map choropleth or flow-line overlay.
		const childDomain = family === 'sim' ? 'flow' : 'node';

		// GWR mode (Phase 3) is a toggle on NLM — same dependent + covariates,
		// but one fit per area instead of one global fit. It produces a richer
		// child layer set: fitted + residual (standard), plus one `beta_*`
		// layer per coefficient (so the user can map the local slope of each
		// covariate), plus `local_r2` and `bw_actual` diagnostic surfaces.
		const isGwr = family === 'nlm' && spec.gwr?.enabled === true;
		const childIds = [fittedId, residualId];
		const childItems = [
			{
				id: fittedId,
				name: `${name.trim()} — fitted`,
				slug: fittedSlug,
				kind: /** @type {const} */ ('model-output'),
				domain: childDomain,
				scale: selection.scale,
				parentId,
				channel: /** @type {const} */ ('fitted')
			},
			{
				id: residualId,
				name: `${name.trim()} — residual`,
				slug: residualSlug,
				kind: /** @type {const} */ ('model-output'),
				domain: childDomain,
				scale: selection.scale,
				parentId,
				channel: /** @type {const} */ ('residual')
			}
		];
		if (isGwr) {
			const coefNames = predictGwrCoefNames(spec, byId);
			for (const coefName of coefNames) {
				const cid = newId();
				const suffix = `beta_${gwrCoefSuffix(coefName)}`;
				const childSlug = this.#freeChildSlug(slug, suffix);
				childIds.push(cid);
				childItems.push({
					id: cid,
					name: `${name.trim()} — β(${coefName})`,
					slug: childSlug,
					kind: /** @type {const} */ ('model-output'),
					domain: childDomain,
					scale: selection.scale,
					parentId,
					channel: /** @type {const} */ ('beta'),
					coefName
				});
			}
			// local_r2: per-area weighted-R² diagnostic. bw_actual: the
			// resolved bandwidth used at each focal point (constant for
			// kernelType='fixed', varies for 'adaptive'). Both are essential
			// GWR sanity-check surfaces — show them as standard children.
			const lr2Id = newId();
			const bwId = newId();
			childIds.push(lr2Id, bwId);
			childItems.push(
				{
					id: lr2Id,
					name: `${name.trim()} — local R²`,
					slug: this.#freeChildSlug(slug, 'local_r2'),
					kind: /** @type {const} */ ('model-output'),
					domain: childDomain,
					scale: selection.scale,
					parentId,
					channel: /** @type {const} */ ('local_r2')
				},
				{
					id: bwId,
					name: `${name.trim()} — bw`,
					slug: this.#freeChildSlug(slug, 'bw_actual'),
					kind: /** @type {const} */ ('model-output'),
					domain: childDomain,
					scale: selection.scale,
					parentId,
					channel: /** @type {const} */ ('bw_actual')
				}
			);
		}

		const parent = {
			id: parentId,
			name: name.trim(),
			slug,
			kind: /** @type {const} */ ('model'),
			domain: childDomain,
			scale: selection.scale,
			family,
			spec: structuredClone($state.snapshot(spec)),
			childIds
		};

		this.items = [...this.items, parent, ...childItems];
		this.persist();
		// computeModelLayer never throws — it records any failure in
		// `this.errors` and resolves. We chain recomputeCalcs so downstream
		// calc layers see the fresh fitted/residual values; the awaitable
		// promise we return resolves when both steps have settled.
		const fitPromise = this.computeModelLayer(parentId).then(() => {
			this.recomputeCalcs();
		});
		return { parentId, fitPromise };
	}

	/** Compute (or rehydrate) a model. Resolves inputs from the latest
	 *  `results` snapshots; if any input hasn't been computed yet, leaves the
	 *  model's children empty and records a useful error.
	 *
	 *  **Immutability contract**: once a model has fit successfully — i.e.
	 *  `modelFits.has(id)` — we never re-fit. Likewise once an error is
	 *  recorded against it, we don't auto-retry (the user fixes inputs and
	 *  rebuilds, or page-reload wipes errors and rehydration tries again).
	 *  This is what stops cascading re-fits from any reactive trigger:
	 *  filter layer recompute, another model finishing, setActive, etc. —
	 *  none of them can re-enter the compute path for a model that already
	 *  fit. saveModel calls us once for the initial fit; refreshAll calls us
	 *  once per model on page-load rehydration; nothing else.
	 *
	 *  Cancellation: per-id seq number drops stale webR responses (and is
	 *  bumped explicitly by `cancelFit`). */
	async computeModelLayer(id) {
		const parent = this.items.find((i) => i.id === id);
		if (!parent || parent.kind !== 'model') return;
		// Immutable-snapshot short-circuit. A model that already has a fit OR
		// has an error stays in that state until the user explicitly resets
		// it (delete + recreate, or in the future a "Re-fit" action).
		if (this.modelFits.has(id) || this.errors.has(id)) return;

		const seq = (this.#modelSeq.get(id) ?? 0) + 1;
		this.#modelSeq.set(id, seq);
		this.loading.add(id);
		this.errors.delete(id);
		this.modelStatus.set(id, 'Preparing inputs…');

		/** Update the fit status — coarse-grained but enough to tell the user
		 *  the fit is making progress. Guarded on seq so a cancelled / superseded
		 *  fit can't keep writing status updates after a newer one starts. */
		const setStatus = (msg) => {
			if (this.#modelSeq.get(id) === seq) this.modelStatus.set(id, msg);
		};

		try {
			let result;
			/** Human-readable notices to surface alongside the fit (rounded y,
			 *  fallback fit family, etc.). Populated per-branch; written to
			 *  modelNotes after the fit lands. */
			const fitNotes = [];
			if (parent.family === 'sim') {
				// SIM path: dependent is a flow layer; covariates are two node
				// layers (origin + destination mass). Distance is computed from
				// RD centroids by sim-design.js, so the only inputs to track
				// for invalidation are the three layer-result identities + the
				// mass transforms.
				const flowRes = this.#resolveOptional(parent.spec.flowId, 'Flow');
				const massORes = this.#resolveOptional(parent.spec.massOId, 'Origin mass');
				const massDRes = this.#resolveOptional(parent.spec.massDId, 'Dest mass');
				if (!flowRes || !massORes || !massDRes) {
					// All three are required for SIM, so a missing id here means a
					// saved spec was corrupted (validation in saveModel rejects this
					// at creation time). Surface a single error message.
					throw new Error('SIM input layer was deleted');
				}
				const { layer: flowLayer, values: rawFlowValues } = flowRes;
				const { layer: massOLayer, values: rawMassOValues } = massORes;
				const { layer: massDLayer, values: rawMassDValues } = massDRes;

				// Study-area scoping (optional): restrict the OD edges + masses
				// before fitting. Two modes for SIM — 'within' (both o & d in
				// the study area) or 'touches' (either side in the study area).
				// NLM/GWR use the simpler "subset both dep + covariates by
				// areaCode" path further down.
				let flowValues = rawFlowValues;
				let massOValues = rawMassOValues;
				let massDValues = rawMassDValues;
				if (parent.spec.studyAreaScoped && studyArea.ids.size > 0) {
					const ids = studyArea.ids;
					const mode = parent.spec.simScopeMode ?? 'within';
					// Filter flows by mode.
					// eslint-disable-next-line svelte/prefer-svelte-reactivity -- local computed subset, not reactive
					const flowSubset = new Map();
					for (const [edgeKey, v] of rawFlowValues) {
						const sep = edgeKey.indexOf('|');
						const o = edgeKey.slice(0, sep);
						const d = edgeKey.slice(sep + 1);
						const oIn = ids.has(o);
						const dIn = ids.has(d);
						const keep = mode === 'within' ? oIn && dIn : oIn || dIn;
						if (keep) flowSubset.set(edgeKey, v);
					}
					flowValues = flowSubset;
					// Mass layers: restrict to area codes still relevant. For
					// 'within' we restrict both to studyArea. For 'touches' we
					// keep the full mass layers (the OD set spans both inside
					// and outside the study area, and either-side mass needs to
					// be available for the gravity term). Same logic for
					// distance computation in sim-design.
					if (mode === 'within') {
						massOValues = filterByStudyArea(rawMassOValues, ids);
						massDValues = filterByStudyArea(rawMassDValues, ids);
					}
					fitNotes.push(
						`Scoped to study area (${ids.size} areas, ${mode}-mode): ${flowSubset.size.toLocaleString()} of ${rawFlowValues.size.toLocaleString()} OD pairs.`
					);
				}

				const massOTx = parent.spec.massOTransform ?? 'log';
				const massDTx = parent.spec.massDTransform ?? 'log';
				// Optional origin offset: a node-domain layer; its (optionally
				// transformed) values are broadcast per-OD on the row's origin.
				const simOffsetRes = this.#resolveOptional(parent.spec.offsetId, 'Offset');
				let offsetByOrigin = null;
				if (simOffsetRes) {
					const oVals = simOffsetRes.values;
					const otx = parent.spec.offsetTransform ?? 'none';
					if (otx === 'none') {
						offsetByOrigin = oVals;
					} else {
						// Pre-apply the transform here so the SIM runner ships
						// already-on-linear-predictor-scale values.
						// eslint-disable-next-line svelte/prefer-svelte-reactivity -- local computed offset, not reactive
						const m = new Map();
						for (const [k, v] of oVals) {
							const t =
								otx === 'log'
									? Math.log(v)
									: otx === 'log1p'
										? Math.log1p(v)
										: otx === 'sqrt'
											? Math.sqrt(v)
											: v;
							if (Number.isFinite(t)) m.set(k, t);
						}
						offsetByOrigin = m;
					}
				}

				const compDest = parent.spec.compDest ?? null;
				const radiation = !!parent.spec.radiation;
				const zeroInflated = !!parent.spec.zeroInflated;

				// Detect fractional flow counts up front — weighted survey
				// datasets (ovin) carry per-respondent weights, so aggregated
				// flow values can be 1.282075 instead of an integer. The R
				// side now rounds before fitting (sim.R / sim_zeroinfl.R),
				// but the user deserves to know we did.
				for (const v of flowValues.values()) {
					if (Number.isFinite(v) && Math.floor(v) !== v) {
						fitNotes.push(
							'Weighted flow counts rounded to integers for Poisson MLE — coefficients are unbiased for the rounded sample.'
						);
						break;
					}
				}

				// Centroids come from the manifest's per-scale geo block. We
				// route through centroidsState so the per-scale JSON is fetched
				// + parsed at most once per session — every subsequent SIM /
				// GWR fit (and computeSmooth call) reuses the same object.
				const geo = manifestState.data?.geo?.[parent.scale];
				const centroidsUrl = geo?.centroidsRd
					? dataUrl(geo.centroidsRd, manifestState.data?.version)
					: null;
				if (!centroidsUrl) throw new Error('Centroids unavailable for this scale');
				const centroids = await centroidsState.ensureLoaded(parent.scale, centroidsUrl);

				setStatus('Fitting SIM…');
				result = await runSim({
					flows: flowValues,
					flowName: flowLayer.slug,
					massO: massOValues,
					massOName: massOLayer.slug,
					massOTransform: massOTx,
					massD: massDValues,
					massDName: massDLayer.slug,
					massDTransform: massDTx,
					centroids,
					includeSelfLoops: !!parent.spec.includeSelfLoops,
					expandToAllOD: !!parent.spec.expandToAllOD,
					constraint: parent.spec.constraint ?? 'none',
					offsetByOrigin,
					compDest: compDest ? { mass: massDValues, ...compDest } : null,
					radiation,
					zeroInflated
				});
			} else {
				const dep = this.items.find((i) => i.id === parent.spec.dependentId);
				if (!dep) throw new Error('Dependent layer was deleted');
				const rawDepValues = this.results.get(dep.id);
				if (!rawDepValues) throw new Error(`Dependent not computed: ${dep.name}`);

				// Study-area scoping for NLM/GWR: subset the dependent to areas
				// in studyArea.ids. The same subset is applied to each
				// covariate further down (alongside the lag computation, since
				// computeSmooth needs the FULL values for kernel weighting and
				// the subset is applied AFTER smoothing).
				const scoped =
					parent.spec.studyAreaScoped === true && studyArea.ids.size > 0 ? studyArea.ids : null;
				const depValues = scoped ? filterByStudyArea(rawDepValues, scoped) : rawDepValues;
				if (scoped) {
					fitNotes.push(
						`Scoped to study area: ${depValues.size.toLocaleString()} of ${rawDepValues.size.toLocaleString()} areas.`
					);
				}

				const covTx = parent.spec.covariateTransforms ?? {};
				const covLags = parent.spec.covariateLags ?? {};
				/** @type {Array<{ name: string, values: Map<string, number>, transform: string }>} */
				const covariates = [];
				for (const cid of parent.spec.covariateIds ?? []) {
					const c = this.items.find((i) => i.id === cid);
					if (!c) throw new Error('Covariate was deleted');
					const cv = this.results.get(c.id);
					if (!cv) throw new Error(`Covariate not computed: ${c.name}`);
					const lag = covLags[cid];
					let values = cv;
					let name = c.slug;
					if (lag) {
						// Apply spatial lag inline: pull centroids for the scale and
						// run the existing smooth worker (mean-mode, no self).
						// IMPORTANT: lag operates on the FULL covariate (not the
						// study-area subset) so kernel weights see neighbours that
						// straddle the study-area boundary. We subset AFTER lag.
						const geo = manifestState.data?.geo?.[parent.scale];
						if (!geo?.centroidsRd) throw new Error('Centroids unavailable for spatial lag');
						const centroidsUrl = dataUrl(geo.centroidsRd, manifestState.data?.version);
						const lagged = await computeSmooth({
							scale: parent.scale,
							centroidsUrl,
							maxDist: lag.maxDist * 1000,
							kernel: lag.kernel,
							decay: lag.kernel === 'power' ? lag.decay : lag.decay * 1000,
							mode: 'mean',
							includeSelf: false,
							values: Object.fromEntries(cv)
						});
						values = lagged;
						name = `lag(${c.slug},${lag.kernel},${lag.decay})`;
					}
					// Study-area subset (after lag so the kernel sees full neighbour
					// set even when the fit is restricted to a smaller area).
					if (scoped) values = filterByStudyArea(values, scoped);
					covariates.push({ name, values, transform: covTx[cid] ?? 'none' });
				}

				// Optional weights: a node-domain layer whose per-area values feed
				// speedglm's `weights=` arg. Resolved lazily so a model with no
				// weights still works. Also subset to study area when scoping
				// is on — keeps the weights vector aligned with the dep/cov rows
				// that survive the intersection in design-matrix.js.
				const weightsRes = this.#resolveOptional(parent.spec.weightsId, 'Weights');
				const weights = weightsRes
					? {
							name: weightsRes.layer.slug,
							values: scoped ? filterByStudyArea(weightsRes.values, scoped) : weightsRes.values
						}
					: null;

				// Optional offset: added to the linear predictor without a
				// coefficient. Transform handled in design-matrix (typical use
				// is offsetTransform='log' for Poisson rate-of-exposure models).
				const offsetRes = this.#resolveOptional(parent.spec.offsetId, 'Offset');
				const offset = offsetRes
					? {
							name: offsetRes.layer.slug,
							values: scoped ? filterByStudyArea(offsetRes.values, scoped) : offsetRes.values,
							transform: parent.spec.offsetTransform ?? 'none'
						}
					: null;

				const depTx = parent.spec.dependentTransform ?? 'none';
				const glm = parent.spec.glm ?? { family: 'gaussian', link: 'identity' };
				// GWR mode (Phase 3) — local weighted regressions per area
				// instead of one global fit. Kernel weights are computed from
				// RD centroids; bandwidth can be a fixed km value, an integer
				// k for adaptive nearest-neighbour kernels, or 'auto' which
				// triggers a golden-section search on residual SS.
				const gwrCfg = parent.spec.gwr?.enabled === true ? parent.spec.gwr : null;

				if (gwrCfg) {
					// GWR ignores weights + offset in v0 — the form disables
					// those controls when GWR is on, but if a stale spec sneaks
					// through (e.g. from saved storage where GWR was toggled on
					// after the offset was set) just drop them silently rather
					// than fail. Future: pass weights through to lm.wfit.
					const geo = manifestState.data?.geo?.[parent.scale];
					const centroidsUrl = geo?.centroidsRd
						? dataUrl(geo.centroidsRd, manifestState.data?.version)
						: null;
					if (!centroidsUrl) throw new Error('Centroids unavailable for this scale');
					const centroids = await centroidsState.ensureLoaded(parent.scale, centroidsUrl);

					setStatus(`Fitting GWR (${centroids ? Object.keys(centroids).length : '?'} areas)…`);
					result = await runGwr({
						dependentName: dep.slug,
						dependentTransform: depTx,
						dependentValues: depValues,
						covariates,
						family: glm.family,
						link: glm.link,
						centroids,
						kernelType: gwrCfg.kernelType ?? 'fixed',
						kernelShape: gwrCfg.kernelShape ?? 'bi-square',
						bandwidth: gwrCfg.bandwidth ?? 'auto'
					});
				} else {
					setStatus('Fitting NLM…');
					result = await runNlm({
						dependentName: dep.slug,
						dependentTransform: depTx,
						dependentValues: depValues,
						covariates,
						weights,
						offset,
						family: glm.family,
						link: glm.link
					});
				}
			}

			if (this.#modelSeq.get(id) !== seq) return; // superseded

			setStatus('Distributing per-area results…');

			// Coefficient table → kept on modelFits (no per-area data, just scalars).
			this.modelFits.set(id, {
				coefficients: {
					name: result.coefficients.name,
					est: Array.from(result.coefficients.est),
					se: Array.from(result.coefficients.se),
					z: Array.from(result.coefficients.z),
					p: Array.from(result.coefficients.p)
				},
				fit: result.fit
			});
			// Stash any informational notes — surfaced by ModelResults below
			// the fit stats. We always set (even if empty list) so the
			// component derive flips on/off cleanly.
			if (fitNotes.length > 0) this.modelNotes.set(id, fitNotes);
			else this.modelNotes.delete(id);

			// Distribute per-channel maps to the child layers. GWR adds three
			// channel types beyond fitted/residual: per-coefficient `beta`
			// surfaces (one child per coef, keyed by spec.coefName), plus a
			// single `local_r2` and a single `bw_actual` diagnostic surface.
			// `result.perNode` is only present for GWR fits — guard accesses.
			let fittedChildId = null;
			const perNode = /** @type {any} */ (result).perNode ?? null;
			for (const cid of parent.childIds ?? []) {
				const child = this.items.find((i) => i.id === cid);
				if (!child) continue;
				if (child.channel === 'fitted') {
					this.results.set(cid, new SvelteMap(result.fitted));
					fittedChildId = cid;
				} else if (child.channel === 'residual') {
					this.results.set(cid, new SvelteMap(result.residual));
				} else if (child.channel === 'beta' && perNode?.betas) {
					const m = perNode.betas[child.coefName];
					if (m) this.results.set(cid, new SvelteMap(m));
				} else if (child.channel === 'local_r2' && perNode?.localR2) {
					this.results.set(cid, new SvelteMap(perNode.localR2));
				} else if (child.channel === 'bw_actual' && perNode?.bwActual) {
					this.results.set(cid, new SvelteMap(perNode.bwActual));
				}
				this.errors.delete(cid);
			}

			// Auto-activate the fitted output exactly once per parent — on the
			// initial fit triggered by saveModel. Page-reload rehydration
			// hits this code path too, but `#autoActivated` already includes
			// the id from the original session (no — actually that set is
			// session-local; on a fresh page load it's empty and the persisted
			// `activeId` is what we want to keep). So: also skip when there's
			// already a persisted activeId pointing at something — only fire
			// when active is null (live view, never assigned).
			if (fittedChildId && !this.#autoActivated.has(id) && this.activeId === null) {
				this.activeId = fittedChildId;
				this.persist();
			}
			// Mark this parent as having gone through the auto-activate path,
			// regardless of whether we actually activated. Stops a later
			// computeModelLayer for this id (e.g. someone explicitly
			// `modelFits.delete(id)` + recompute) from overriding the user's
			// click after they've already interacted.
			this.#autoActivated.add(id);
		} catch (e) {
			if (this.#modelSeq.get(id) === seq) {
				const msg = /** @type {Error} */ (e)?.message ?? String(e);
				this.errors.set(id, msg);
				// Surface the full stack in devtools — the message alone usually
				// hides whether the failure was a webR install, an R syntax error
				// in fit_nlm, or a JS-side parsing issue on the way back.
				console.error(`[layers] model fit failed for ${id}:`, e);
			}
		} finally {
			if (this.#modelSeq.get(id) === seq) {
				this.loading.delete(id);
				this.modelStatus.delete(id);
			}
		}
	}

	/** Cancel an in-flight fit. Bumps the per-id sequence number (so the
	 *  in-flight runNlm/Sim/Gwr's result is treated as superseded when it
	 *  eventually resolves), calls `webR.interrupt()` to actually halt the
	 *  R-side computation, and records the cancellation as an error.
	 *  Cancelled models stay in the error state until the user deletes +
	 *  recreates them — same shape as a real fit failure. */
	async cancelFit(id) {
		const parent = this.items.find((i) => i.id === id);
		if (!parent || parent.kind !== 'model') return;
		if (!this.loading.has(id)) return; // nothing in flight
		// Bump seq so any in-flight runX result that arrives later is dropped
		// by the `#modelSeq.get(id) !== seq` guard in computeModelLayer.
		this.#modelSeq.set(id, (this.#modelSeq.get(id) ?? 0) + 1);
		this.loading.delete(id);
		this.modelStatus.delete(id);
		this.errors.set(id, 'Cancelled by user');
		// Best-effort R-side interrupt — webR.interrupt is fire-and-forget
		// and may no-op if webR isn't booted yet. Wrapped in try because
		// we don't want a teardown failure to mask the user-cancelled state.
		try {
			const mod = await import('$lib/models/webr-client.js');
			mod.interruptR();
		} catch {
			// Cancellation is still effective (seq bumped, error set) even
			// without the R-side interrupt — the in-flight call eventually
			// resolves and gets dropped by the seq guard.
		}
	}

	/** Resolve an optional layer reference inside computeModelLayer. Returns
	 *  `null` when the spec field is unset (no layer chosen), throws with a
	 *  helpful message when the layer's been deleted or hasn't computed yet.
	 *  Used for weights, offset, and the SIM compDest mass — all four sites
	 *  used to inline the same `if (id) { ... find ... results.get ... }`
	 *  pattern.
	 *  @param {string | null | undefined} id
	 *  @param {string} label — human-readable role for error messages.
	 *  @returns {{ layer: any, values: Map<string, number> } | null} */
	#resolveOptional(id, label) {
		if (!id) return null;
		const layer = this.items.find((i) => i.id === id);
		if (!layer) throw new Error(`${label} layer was deleted`);
		const values = this.results.get(id);
		if (!values) throw new Error(`${label} not computed: ${layer.name}`);
		return { layer, values };
	}

	/** Generate a child slug like `${parent}_${suffix}` that's free. Falls back
	 *  to numeric suffixes if a collision exists (rare — parents are unique). */
	#freeChildSlug(parentSlug, suffix) {
		const base = `${parentSlug}_${suffix}`;
		if (!this.slugTaken(base)) return base;
		for (let n = 2; ; n++) {
			const cand = `${base}_${n}`;
			if (!this.slugTaken(cand)) return cand;
		}
	}

	/** Patch a saved smooth layer's parameters, then recompute it and any later
	 *  layers that may depend on it. `persist: false` is used for live slider
	 *  drags (commit on `change` persists once). */
	updateSmoothParams(id, patch, { persist = true } = {}) {
		const idx = this.items.findIndex((x) => x.id === id);
		if (idx < 0 || this.items[idx].kind !== 'smooth') return;
		this.items = this.items.map((x, k) => (k === idx ? { ...x, ...patch } : x));
		if (persist) this.persist();
		this.#recomputeDerived(idx);
	}

	/** Re-run all filter layers at the current scale, then evaluate all calc layers.
	 *  If the active layer is on a different scale, clear it (fall back to live preview). */
	async refreshAll() {
		const scale = selection.scale;
		if (this.activeId) {
			const active = this.items.find((i) => i.id === this.activeId);
			if (active && active.scale !== scale) this.activeId = null;
		}
		for (const layer of this.items) {
			if (layer.kind === 'filter' && layer.scale === scale) {
				await this.refreshFilterLayer(layer.id);
			}
		}
		await this.#recomputeDerived(0);
	}
}

export const layers = new LayersState();

// Read-only facade used by /  and /print to choose between the live preview
// (queryResult, driven by selection) and the active saved-layer result. The
// active layer must be node-domain for the choropleth; flow-domain layers
// are inputs to calculations, not direct map sources for nodes.
const EMPTY = /** @type {Map<string, number>} */ (new SvelteMap());
export const displayed = {
	get data() {
		const id = layers.activeId;
		if (!id) return queryResult.data;
		const layer = layers.items.find((i) => i.id === id);
		if (!layer || layer.domain !== 'node') return queryResult.data;
		return layers.results.get(id) ?? EMPTY;
	},
	get loading() {
		const id = layers.activeId;
		if (!id) return queryResult.loading;
		return layers.loading.has(id);
	},
	get error() {
		const id = layers.activeId;
		if (!id) return queryResult.error;
		return layers.errors.get(id) ?? null;
	},
	get activeLayer() {
		const id = layers.activeId;
		if (!id) return null;
		return layers.items.find((i) => i.id === id) ?? null;
	},
	/** Active flow-domain layer's per-OD values, or null. Used by `+page.svelte`
	 *  to override the live `runFlows()` query so a saved flow filter / SIM
	 *  fitted child can render through the existing FlowLayer pipeline. The
	 *  shape is the raw `Map<edgeKey, number>` stored in `layers.results` —
	 *  the caller turns it into the `{flows:[{o,d,value}], min, max, weighted}`
	 *  shape FlowLayer + the inspect panel expect. */
	get flowsData() {
		const id = layers.activeId;
		if (!id) return null;
		const layer = layers.items.find((i) => i.id === id);
		if (!layer || layer.domain !== 'flow') return null;
		return layers.results.get(id) ?? null;
	}
};
