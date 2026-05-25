// Shared cartography state — singleton imported by ClassificationControls
// (writes) and the map page (reads to derive fill expression).
//
// Two palette fields: `palette` is the sequential choice (used when the
// active layer's data is single-signed), `divergingPalette` is the
// diverging choice (used automatically when data crosses zero). Splitting
// avoids the "remember which palette I picked last time it was diverging"
// problem.
//
// Defaults live in the frozen `DEFAULTS` const (referenced by initializers
// and `reset()`), so changing a default is a one-line edit.
import { applyDefaults } from './defaults.js';

const DEFAULTS = Object.freeze({
	method: 'jenks',
	n: 5,
	palette: 'Blues',
	divergingPalette: 'RdBu',
	forceSequential: false,
	fillOpacity: 0.75,
	lineColor: '#666',
	lineWidth: 0.4
});

class CartographyState {
	method = $state(DEFAULTS.method);
	n = $state(DEFAULTS.n);
	palette = $state(DEFAULTS.palette);
	/** Diverging palette used when the active layer's values span both
	 *  signs. Picked from palettes.js `DIVERGING` set. */
	divergingPalette = $state(DEFAULTS.divergingPalette);
	/** Force sequential classification + palette even when both signs are
	 *  present (e.g. residuals that the user wants to read as "all bad",
	 *  not "good vs bad"). Off by default. */
	forceSequential = $state(DEFAULTS.forceSequential);
	fillOpacity = $state(DEFAULTS.fillOpacity);
	lineColor = $state(DEFAULTS.lineColor);
	lineWidth = $state(DEFAULTS.lineWidth);

	reset() {
		applyDefaults(this, DEFAULTS);
	}
}

export const cartography = new CartographyState();
