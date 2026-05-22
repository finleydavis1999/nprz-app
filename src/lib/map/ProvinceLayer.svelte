<script>
	// Province boundary line — drawn above every app layer but below the
	// basemap labels (`beforeId` = first label layer; see layer-order.js).
	// Color + width are user-adjustable and synced without remounting.
	import { onMount, onDestroy } from 'svelte';
	import { getMapContext } from './context.js';
	import { firstLabelLayerId } from './layer-order.js';

	let { sourceId = 'provinces', geoUrl, lineColor = '#555555', lineWidth = 1.5 } = $props();

	const ctx = getMapContext();
	const lineId = $derived(`${sourceId}-line`);
	let installed = false;

	onMount(() => {
		const map = ctx.map;
		if (!map) return;
		if (!map.getSource(sourceId)) {
			map.addSource(sourceId, { type: 'geojson', data: geoUrl });
		}
		map.addLayer(
			{
				id: lineId,
				type: 'line',
				source: sourceId,
				layout: { 'line-join': 'round' },
				paint: { 'line-color': lineColor, 'line-width': lineWidth }
			},
			firstLabelLayerId(map)
		);
		installed = true;
	});

	$effect(() => {
		const map = ctx.map;
		if (!map || !installed) return;
		map.setPaintProperty(lineId, 'line-color', lineColor);
		map.setPaintProperty(lineId, 'line-width', lineWidth);
	});

	onDestroy(() => {
		const map = ctx.map;
		if (!map) return;
		if (map.getLayer(lineId)) map.removeLayer(lineId);
		if (map.getSource(sourceId)) map.removeSource(sourceId);
	});
</script>
