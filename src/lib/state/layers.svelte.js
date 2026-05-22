// Saved layers + calc layers, domain-aware.
//
// A filter layer is a snapshot of either the node-side `selection` (dataset/
// year/filters → choropleth values) or the flow-side `flow` state (dataset/
// year-range/filters → OD edge values). A calc layer is a math.js expression
// over other layer slugs of the same domain, optionally wrapping flow inputs
// with `inflow(...)` / `outflow(...)` / `net(...)` to produce node-domain
// output.
//
// Persisted to localStorage under v2; v1 records (no domain) are migrated to
// node-domain on first load.

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
import { selection } from './selection.svelte.js';
import { flow } from './flow.svelte.js';
import { queryResult } from './query-result.svelte.js';

const STORAGE_KEY_V1 = 'nprz.layers.v1';
const STORAGE_KEY = 'nprz.layers.v2';

function newId() {
	return `l_${Math.random().toString(36).slice(2, 10)}`;
}

function flowEdgeKey(o, d) {
	return `${o}|${d}`;
}

class LayersState {
	/** @type {Array<{
	 *   id: string, name: string, slug: string,
	 *   kind: 'filter' | 'calc', domain: 'node' | 'flow',
	 *   scale: string,
	 *   dataset?: string, year?: number,
	 *   yearMin?: number, yearMax?: number,
	 *   filters?: Record<string, number[]>,
	 *   includeSelfLoops?: boolean,
	 *   expression?: string
	 * }>} */
	items = $state([]);
	/** @type {SvelteMap<string, Map<string, number>>} */
	results = new SvelteMap();
	/** @type {SvelteSet<string>} */
	loading = new SvelteSet();
	/** @type {SvelteMap<string, string>} */
	errors = new SvelteMap();
	activeId = $state(/** @type {string | null} */ (null));

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
				filters: structuredClone($state.snapshot(flow.filters)),
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

	remove(id) {
		this.items = this.items.filter((i) => i.id !== id);
		this.results.delete(id);
		this.loading.delete(id);
		this.errors.delete(id);
		if (this.activeId === id) this.activeId = null;
		this.persist();
		this.recomputeCalcs();
	}

	setActive(id) {
		this.activeId = id;
		this.persist();
	}

	recomputeCalcs() {
		for (const l of this.items) {
			if (l.kind === 'calc' && l.scale === selection.scale) this.computeCalcLayer(l.id);
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
					filters: layer.filters,
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

	/** Re-run all filter layers at the current scale, then evaluate all calc layers.
	 *  If the active layer is on a different scale, clear it (fall back to live preview). */
	async refreshAll() {
		const scale = selection.scale;
		if (this.activeId) {
			const active = this.items.find((i) => i.id === this.activeId);
			if (active && active.scale !== scale) this.activeId = null;
		}
		const matching = this.items.filter((i) => i.scale === scale);
		for (const layer of matching) {
			if (layer.kind === 'filter') {
				await this.refreshFilterLayer(layer.id);
			}
		}
		for (const layer of matching) {
			if (layer.kind === 'calc') this.computeCalcLayer(layer.id);
		}
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
	}
};
