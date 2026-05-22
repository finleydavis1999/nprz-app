<script>
	// Toggles the visibility of the Protomaps basemap label layers as a group.
	// Adds/removes nothing — the label layers are part of the basemap style and
	// already sit on top of every app layer; this only flips their `visibility`.
	import { getMapContext } from './context.js';
	import { labelLayerIds } from './layer-order.js';

	let { visible = true } = $props();

	const ctx = getMapContext();
	// Captured once on the first effect run; the basemap label set is static.
	let ids = [];

	$effect(() => {
		const map = ctx.map;
		if (!map) return;
		if (ids.length === 0) ids = labelLayerIds(map);
		for (const id of ids) {
			if (map.getLayer(id)) {
				map.setLayoutProperty(id, 'visibility', visible ? 'visible' : 'none');
			}
		}
	});
</script>
