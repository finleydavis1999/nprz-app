<script>
	// Renders text labels at each feature in the given source. Uses the
	// `name` property (falls back to `area_code`) so gemeenten get their names
	// and PC4s render their code. MapLibre's symbol placement handles
	// collisions automatically.
	import { onMount, onDestroy } from 'svelte';
	import { getMapContext } from './context.js';
	import { beforeId, ANCHOR_DATA } from './layer-order.js';

	let { sourceId, layerId = `${sourceId}-labels` } = $props();

	const ctx = getMapContext();
	let installed = false;

	onMount(() => {
		const map = ctx.map;
		if (!map) return;
		map.addLayer(
			{
				id: layerId,
				type: 'symbol',
				source: sourceId,
				layout: {
					'text-field': ['coalesce', ['get', 'name'], ['get', 'area_code']],
					'text-font': ['Noto Sans Regular'],
					'text-size': ['interpolate', ['linear'], ['zoom'], 6, 10, 12, 14],
					'text-anchor': 'center',
					'text-padding': 2,
					'text-allow-overlap': false,
					'text-ignore-placement': false,
					'symbol-sort-key': 1
				},
				paint: {
					'text-color': '#1f2328',
					'text-halo-color': 'rgba(255,255,255,0.92)',
					'text-halo-width': 1.5,
					'text-halo-blur': 0.5
				}
			},
			beforeId(map, ANCHOR_DATA)
		);
		installed = true;
	});

	onDestroy(() => {
		const map = ctx.map;
		if (!map || !installed) return;
		if (map.getLayer(layerId)) map.removeLayer(layerId);
	});
</script>
