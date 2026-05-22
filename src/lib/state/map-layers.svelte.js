// Singleton state for the cartographic map layers — basemap labels, the admin
// boundary overlay, the province boundary, and the built-up area. One panel
// (MapLayerControls) and one state module drive all of them.
class MapLayersState {
	// Protomaps basemap labels — shown by default.
	labels = $state(true);

	// Admin boundary overlay: outlines of the gemeente/PC4 scale, drawn over
	// the choropleth.
	boundary = $state(false);
	boundaryScale = $state(/** @type {'pc4' | 'gem'} */ ('gem'));
	boundaryColor = $state('#222222');
	boundaryWidth = $state(1.0);
	boundaryOpacity = $state(0.8);

	// Province boundary line.
	provinces = $state(false);
	provinceColor = $state('#555555');
	provinceWidth = $state(1.5);

	// Built-up area fill.
	builtup = $state(false);
	builtupColor = $state('#888888');
	builtupOpacity = $state(0.5);
}

export const mapLayers = new MapLayersState();
