// Flow layer state — singletons read by `+page.svelte` to drive `runFlows(...)`
// and the FlowLayer paint expressions. Written by FlowDataPicker / Filters
// (data side) and ClassificationControls (cartography side).
//
// Persisted to localStorage so navigating to /print (or reloading the page)
// preserves the user's flow setup. Cartography is intentionally not persisted
// — it's a styling concern that re-defaults each session.
//
// Reactivity rule: when mutating `filters`, assign a new object
// (`flow.filters = { ...flow.filters, [k]: v }`) — deep mutation isn't tracked.
const STORAGE_KEY = 'nprz.flow.v1';

class FlowState {
	enabled = $state(false);
	dataset = $state('ovin');
	scale = $state('gem');
	// Inclusive year range. yearMin === yearMax = single year.
	yearMin = $state(2018);
	yearMax = $state(2018);
	filters = $state({});
	// Client-side filter: only render flows whose post-aggregation value is
	// >= minWeight. Updates do not re-query DuckDB.
	minWeight = $state(0);
	includeSelfLoops = $state(false);

	load() {
		if (typeof localStorage === 'undefined') return;
		try {
			const raw = localStorage.getItem(STORAGE_KEY);
			if (!raw) return;
			const p = JSON.parse(raw);
			if (typeof p?.dataset === 'string') this.dataset = p.dataset;
			if (typeof p?.scale === 'string') this.scale = p.scale;
			if (Number.isFinite(p?.yearMin)) this.yearMin = p.yearMin;
			if (Number.isFinite(p?.yearMax)) this.yearMax = p.yearMax;
			if (p?.filters && typeof p.filters === 'object') this.filters = p.filters;
			if (Number.isFinite(p?.minWeight)) this.minWeight = p.minWeight;
			if (typeof p?.includeSelfLoops === 'boolean') this.includeSelfLoops = p.includeSelfLoops;
		} catch {
			// corrupted storage — ignore
		}
	}

	persist() {
		if (typeof localStorage === 'undefined') return;
		try {
			localStorage.setItem(
				STORAGE_KEY,
				JSON.stringify({
					enabled: this.enabled,
					dataset: this.dataset,
					scale: this.scale,
					yearMin: this.yearMin,
					yearMax: this.yearMax,
					filters: this.filters,
					minWeight: this.minWeight,
					includeSelfLoops: this.includeSelfLoops
				})
			);
		} catch {
			// quota / private mode — non-fatal
		}
	}
}

class FlowCartographyState {
	method = $state('quantile');
	n = $state(5);
	palette = $state('YlOrRd');
	widthMin = $state(0.5);
	widthMax = $state(8);
	opacity = $state(0.75);
	curvature = $state(0.2);
}

export const flow = new FlowState();
export const flowCartography = new FlowCartographyState();
