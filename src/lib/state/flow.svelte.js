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
//
// Defaults for both classes live in their respective frozen `DEFAULTS` consts
// at the top of the file; field initializers and `reset()` both consume them.
import { applyDefaults } from './defaults.js';

const STORAGE_KEY = 'nprz.flow.v1';

const DEFAULTS = Object.freeze({
	enabled: false,
	dataset: 'ovin',
	scale: 'gem',
	// Inclusive year range. yearMin === yearMax = single year.
	yearMin: 2018,
	yearMax: 2018,
	filters: /** @type {Record<string, number[]>} */ ({}),
	// Client-side filters: only render flows whose post-aggregation
	// *magnitude* (|value|) is >= minWeight and (on weighted layers) whose
	// observation count is >= minCount. The abs() lets signed flow data
	// (SIM residuals etc.) filter symmetrically by strength rather than
	// dropping every negative value. Updates do not re-query DuckDB.
	minWeight: 0,
	minCount: 0,
	includeSelfLoops: false,
	/** Filter mode applied when a study area is active. Four modes form a
	 *  lattice from strictest to most inclusive: 'within' (both o and d in the
	 *  lasso), 'origin-in' (origin in → outflows), 'dest-in' (destination in →
	 *  inflows), 'touches' (either side in). Mirrors the SIM model spec field.
	 *  No "enabled" boolean — the lasso being drawn is itself the toggle. */
	studyAreaMode: 'within'
});

const CARTO_DEFAULTS = Object.freeze({
	method: 'quantile',
	n: 5,
	palette: 'YlOrRd',
	divergingPalette: 'RdBu',
	forceSequential: false,
	widthMin: 0.5,
	widthMax: 8,
	opacity: 0.75,
	curvature: 0.2,
	// Directional gradient mode: render one line per bidirectional pair,
	// width by total flow, color as a red→blue gradient split at the
	// directional proportion. Off = classic per-direction stepped lines.
	directional: false,
	// Show the balance-point dot at each bidirectional gradient split.
	showBalance: true,
	// Spider-view nodal circle radius bounds (px). Circles are area-scaled
	// between these by flow magnitude.
	pieMinRadius: 4,
	pieMaxRadius: 26
});

class FlowState {
	enabled = $state(DEFAULTS.enabled);
	dataset = $state(DEFAULTS.dataset);
	scale = $state(DEFAULTS.scale);
	yearMin = $state(DEFAULTS.yearMin);
	yearMax = $state(DEFAULTS.yearMax);
	filters = $state(structuredClone(DEFAULTS.filters));
	minWeight = $state(DEFAULTS.minWeight);
	minCount = $state(DEFAULTS.minCount);
	includeSelfLoops = $state(DEFAULTS.includeSelfLoops);
	studyAreaMode = $state(DEFAULTS.studyAreaMode);

	load() {
		if (typeof localStorage === 'undefined') return;
		try {
			const raw = localStorage.getItem(STORAGE_KEY);
			if (!raw) return;
			const p = JSON.parse(raw);
			if (typeof p?.enabled === 'boolean') this.enabled = p.enabled;
			if (typeof p?.dataset === 'string') this.dataset = p.dataset;
			if (typeof p?.scale === 'string') this.scale = p.scale;
			if (Number.isFinite(p?.yearMin)) this.yearMin = p.yearMin;
			if (Number.isFinite(p?.yearMax)) this.yearMax = p.yearMax;
			if (p?.filters && typeof p.filters === 'object') this.filters = p.filters;
			if (Number.isFinite(p?.minWeight)) this.minWeight = p.minWeight;
			if (Number.isFinite(p?.minCount)) this.minCount = p.minCount;
			if (typeof p?.includeSelfLoops === 'boolean') this.includeSelfLoops = p.includeSelfLoops;
			if (
				p?.studyAreaMode === 'within' ||
				p?.studyAreaMode === 'origin-in' ||
				p?.studyAreaMode === 'dest-in' ||
				p?.studyAreaMode === 'touches'
			) {
				this.studyAreaMode = p.studyAreaMode;
			}
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
					minCount: this.minCount,
					includeSelfLoops: this.includeSelfLoops,
					studyAreaMode: this.studyAreaMode
				})
			);
		} catch {
			// quota / private mode — non-fatal
		}
	}

	reset() {
		applyDefaults(this, DEFAULTS);
		this.persist();
	}
}

class FlowCartographyState {
	method = $state(CARTO_DEFAULTS.method);
	n = $state(CARTO_DEFAULTS.n);
	palette = $state(CARTO_DEFAULTS.palette);
	/** Diverging palette used when the active flow values span both signs
	 *  (e.g. SIM residuals). Same auto-detect machinery as node cartography
	 *  — see `+page.svelte`'s `flowHasBothSigns` derive. */
	divergingPalette = $state(CARTO_DEFAULTS.divergingPalette);
	/** Force sequential classification + palette even when both signs are
	 *  present. Off by default. */
	forceSequential = $state(CARTO_DEFAULTS.forceSequential);
	widthMin = $state(CARTO_DEFAULTS.widthMin);
	widthMax = $state(CARTO_DEFAULTS.widthMax);
	opacity = $state(CARTO_DEFAULTS.opacity);
	curvature = $state(CARTO_DEFAULTS.curvature);
	directional = $state(CARTO_DEFAULTS.directional);
	showBalance = $state(CARTO_DEFAULTS.showBalance);
	pieMinRadius = $state(CARTO_DEFAULTS.pieMinRadius);
	pieMaxRadius = $state(CARTO_DEFAULTS.pieMaxRadius);

	reset() {
		applyDefaults(this, CARTO_DEFAULTS);
	}
}

export const flow = new FlowState();
export const flowCartography = new FlowCartographyState();
