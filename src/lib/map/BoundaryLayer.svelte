<script>
	import { onMount, onDestroy } from 'svelte';
	import { getMapContext } from './context.js';
	import { beforeId, ANCHOR_DATA } from './layer-order.js';

	let {
		sourceId,
		geoUrl,
		promoteId = 'area_code',
		lineColor = '#222',
		lineWidth = 1.0,
		lineOpacity = 0.8
	} = $props();

	const ctx = getMapContext();
	const lineId = $derived(`${sourceId}-boundary-line`);
	let installed = false;

	onMount(() => {
		const map = ctx.map;
		if (!map) return;

		if (!map.getSource(sourceId)) {
			map.addSource(sourceId, { type: 'geojson', data: geoUrl, promoteId });
		}
		map.addLayer(
			{
				id: lineId,
				type: 'line',
				source: sourceId,
				paint: {
					'line-color': lineColor,
					'line-width': lineWidth,
					'line-opacity': lineOpacity
				}
			},
			beforeId(map, ANCHOR_DATA)
		);
		installed = true;
	});

	$effect(() => {
		const map = ctx.map;
		if (!map || !installed) return;
		map.setPaintProperty(lineId, 'line-color', lineColor);
		map.setPaintProperty(lineId, 'line-width', lineWidth);
		map.setPaintProperty(lineId, 'line-opacity', lineOpacity);
	});

	onDestroy(() => {
		const map = ctx.map;
		if (!map) return;
		if (map.getLayer(lineId)) map.removeLayer(lineId);
		// Source may be shared with a ChoroplethLayer using the same sourceId;
		// only remove it if no other layer references it.
		if (map.getSource(sourceId)) {
			const referenced = map.getStyle().layers.some((l) => l.source === sourceId);
			if (!referenced) map.removeSource(sourceId);
		}
	});
</script>
