// Shared cartography state — singleton imported by ClassificationControls
// (writes) and the map page (reads to derive fill expression).
//
// Defaults live in the frozen `DEFAULTS` const (referenced by initializers
// and `reset()`), so changing a default is a one-line edit.
import { applyDefaults } from './defaults.js';

const DEFAULTS = Object.freeze({
	method: 'jenks',
	n: 5,
	palette: 'Blues',
	fillOpacity: 0.75,
	lineColor: '#666',
	lineWidth: 0.4
});

class CartographyState {
	method = $state(DEFAULTS.method);
	n = $state(DEFAULTS.n);
	palette = $state(DEFAULTS.palette);
	fillOpacity = $state(DEFAULTS.fillOpacity);
	lineColor = $state(DEFAULTS.lineColor);
	lineWidth = $state(DEFAULTS.lineWidth);

	reset() {
		applyDefaults(this, DEFAULTS);
	}
}

export const cartography = new CartographyState();
