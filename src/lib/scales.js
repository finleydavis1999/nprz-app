// Central registry of geographic scales. The single source of truth for the
// scale picker, the boundary-overlay picker, and any UI that labels a scale.
// Adding a scale here surfaces it everywhere; the manifest decides which
// datasets/geo are actually available at each scale.

/** @typedef {'gem' | 'pc4' | 'buurt'} ScaleId */

export const SCALES = [
	{ id: 'gem', label: 'Gemeente', unit: 'gemeenten' },
	{ id: 'pc4', label: 'PC4', unit: 'PC4s' },
	{ id: 'buurt', label: 'Buurt', unit: 'buurten' }
];

/** Human-readable name for a scale id (e.g. 'pc4' → 'PC4'). */
export function scaleLabel(id) {
	return SCALES.find((s) => s.id === id)?.label ?? id;
}

/** Plural noun for counting areas at a scale (e.g. 'pc4' → 'PC4s'). */
export function scaleUnit(id) {
	return SCALES.find((s) => s.id === id)?.unit ?? id;
}
