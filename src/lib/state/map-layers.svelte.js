// Singleton state for the cartographic map layers — basemap labels, the admin
// boundary overlay, the province boundary, and the built-up area. One panel
// (MapLayerControls) and one state module drive all of them.
//
// Defaults live in the frozen `DEFAULTS` const; field initializers and
// `reset()` both consume it, so changing a default is a one-line edit.
import { applyDefaults } from './defaults.js';

const DEFAULTS = Object.freeze({
	// Protomaps basemap labels — shown by default.
	labels: true,

	// Admin boundary overlay: outlines of the gemeente/PC4 scale, drawn over
	// the choropleth.
	boundary: false,
	boundaryScale: /** @type {'pc4' | 'gem'} */ ('gem'),
	boundaryColor: '#222222',
	boundaryWidth: 1.0,
	boundaryOpacity: 0.8,

	// Province boundary line.
	provinces: false,
	provinceColor: '#555555',
	provinceWidth: 1.5,

	// Built-up area fill — on by default to establish urban form at first sight.
	builtup: true,
	builtupColor: '#888888',
	builtupOpacity: 0.5
});

class MapLayersState {
	labels = $state(DEFAULTS.labels);

	boundary = $state(DEFAULTS.boundary);
	boundaryScale = $state(DEFAULTS.boundaryScale);
	boundaryColor = $state(DEFAULTS.boundaryColor);
	boundaryWidth = $state(DEFAULTS.boundaryWidth);
	boundaryOpacity = $state(DEFAULTS.boundaryOpacity);

	provinces = $state(DEFAULTS.provinces);
	provinceColor = $state(DEFAULTS.provinceColor);
	provinceWidth = $state(DEFAULTS.provinceWidth);

	builtup = $state(DEFAULTS.builtup);
	builtupColor = $state(DEFAULTS.builtupColor);
	builtupOpacity = $state(DEFAULTS.builtupOpacity);

	reset() {
		applyDefaults(this, DEFAULTS);
	}
}

export const mapLayers = new MapLayersState();
