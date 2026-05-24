// Singleton selection state — what the user is currently mapping (node side).
// Read by `+page.svelte` to drive `runChoropleth(...)` and the legend/histogram.
// Written by ScaleToggle, DatasetPicker, YearPicker, CategoryFilters.
//
// Node datasets are single-year only (manifest field `year.type === 'single'`).
//
// Defaults live in the frozen `DEFAULTS` const — referenced by the field
// initializers and by `reset()` via the shared helper, so changing a default
// is a one-line edit.
import { applyDefaults } from './defaults.js';

const DEFAULTS = Object.freeze({
	enabled: true,
	dataset: 'demographics',
	scale: 'gem',
	year: 2018,
	// Multi-select filters keyed by field id, value lists of integer category ids.
	// Empty / missing key = no filter on that field.
	filters: /** @type {Record<string, number[]>} */ ({})
});

class SelectionState {
	enabled = $state(DEFAULTS.enabled);
	dataset = $state(DEFAULTS.dataset);
	scale = $state(DEFAULTS.scale);
	year = $state(DEFAULTS.year);
	filters = $state(structuredClone(DEFAULTS.filters));

	reset() {
		applyDefaults(this, DEFAULTS);
	}
}

export const selection = new SelectionState();
