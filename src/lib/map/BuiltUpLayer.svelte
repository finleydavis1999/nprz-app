<script>
	// Built-up area fill — a contextual cartographic layer drawn just above the
	// app data band (see layer-order.js). Single uniform fill; color + opacity
	// are user-adjustable and synced without remounting.
	import { onMount, onDestroy } from 'svelte';
	import { getMapContext } from './context.js';
	import { beforeId, ANCHOR_BUILTUP } from './layer-order.js';

	let { sourceId = 'builtup', geoUrl, color = '#888888', opacity = 0.5 } = $props();

	const ctx = getMapContext();
	const fillId = $derived(`${sourceId}-fill`);
	let installed = false;

	onMount(() => {
		const map = ctx.map;
		if (!map) return;
		if (!map.getSource(sourceId)) {
			map.addSource(sourceId, { type: 'geojson', data: geoUrl });
		}
		map.addLayer(
			{
				id: fillId,
				type: 'fill',
				source: sourceId,
				paint: { 'fill-color': color, 'fill-opacity': opacity }
			},
			beforeId(map, ANCHOR_BUILTUP)
		);
		installed = true;
	});

	$effect(() => {
		const map = ctx.map;
		if (!map || !installed) return;
		map.setPaintProperty(fillId, 'fill-color', color);
		map.setPaintProperty(fillId, 'fill-opacity', opacity);
	});

	onDestroy(() => {
		const map = ctx.map;
		if (!map) return;
		if (map.getLayer(fillId)) map.removeLayer(fillId);
		if (map.getSource(sourceId)) map.removeSource(sourceId);
	});
</script>
