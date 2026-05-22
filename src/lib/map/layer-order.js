// Stable z-ordering for app map layers.
//
// The Protomaps basemap places its label (symbol) layers near the top of the
// style. The desired stack, bottom -> top, is:
//
//   protomaps base . app data . built-up . province . protomaps labels
//
// Svelte layer components mount and re-mount in an unpredictable order (the
// choropleth re-mounts on every scale switch, the boundary overlay on every
// overlay-scale switch, flows on enable/disable). So instead of relying on
// insertion order, each layer is added with a `beforeId` pointing at one of
// two invisible anchor layers installed once when the style first loads.
// Anchors are never removed, so every layer — however late it (re)mounts —
// lands in the correct slot.

const ANCHOR_SOURCE = 'app-anchors';

/** `beforeId` target for the app data band (choropleth, overlay, flows, names). */
export const ANCHOR_DATA = 'app-anchor-data';

/** `beforeId` target for the built-up fill — sits above the data band. */
export const ANCHOR_BUILTUP = 'app-anchor-builtup';

/** True for a Protomaps basemap label layer. */
export function isLabelLayer(layer) {
	return !!layer && layer.source === 'protomaps' && layer.type === 'symbol';
}

// `getLayersOrder()` + `getLayer()` are used instead of `getStyle()`: the latter
// returns `undefined` until the style is fully serializable, whereas the layer
// registry is populated as soon as the style spec is parsed.

/** Id of the first basemap label layer, or `undefined` for the empty-style fallback. */
export function firstLabelLayerId(map) {
	for (const id of map.getLayersOrder()) {
		if (isLabelLayer(map.getLayer(id))) return id;
	}
	return undefined;
}

/** Ids of every basemap label layer (for the labels visibility toggle). */
export function labelLayerIds(map) {
	return map.getLayersOrder().filter((id) => isLabelLayer(map.getLayer(id)));
}

/**
 * Resolve a `beforeId` for `map.addLayer()`. Returns the anchor when it exists,
 * else the first basemap label (so app layers still stay below the labels),
 * else `undefined` (empty style — the layer simply appends on top).
 */
export function beforeId(map, anchorId) {
	if (map.getLayer(anchorId)) return anchorId;
	return firstLabelLayerId(map);
}

/**
 * Install the two invisible anchor layers once, just below the basemap labels.
 * Both are inserted before the first label layer; doing DATA first then BUILTUP
 * leaves them ordered `…base… · app-anchor-data · app-anchor-builtup · …labels…`.
 */
export function installLayerAnchors(map) {
	if (map.getLayer(ANCHOR_DATA)) return;
	const before = firstLabelLayerId(map);
	if (!map.getSource(ANCHOR_SOURCE)) {
		map.addSource(ANCHOR_SOURCE, {
			type: 'geojson',
			data: { type: 'FeatureCollection', features: [] }
		});
	}
	for (const id of [ANCHOR_DATA, ANCHOR_BUILTUP]) {
		map.addLayer(
			{ id, type: 'circle', source: ANCHOR_SOURCE, layout: { visibility: 'none' } },
			before
		);
	}
}
