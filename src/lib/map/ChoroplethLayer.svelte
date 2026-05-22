<script>
	import { onMount, onDestroy } from 'svelte';
	import { getMapContext } from './context.js';
	import { beforeId, ANCHOR_DATA } from './layer-order.js';

	let {
		sourceId,
		geoUrl,
		promoteId = 'area_code',
		valueByArea = new Map(),
		selectedIds = new Set(),
		// Step 5 will replace this with a classification-driven `step` expression.
		fillColor = [
			'case',
			['==', ['feature-state', 'value'], null],
			'#eee',
			[
				'interpolate',
				['linear'],
				['feature-state', 'value'],
				0,
				'#fee5d9',
				50000,
				'#fb6a4a',
				200000,
				'#a50f15'
			]
		],
		fillOpacity = 0.75,
		lineColor = '#666',
		lineWidth = 0.4
	} = $props();

	const ctx = getMapContext();
	const fillId = $derived(`${sourceId}-fill`);
	const lineId = $derived(`${sourceId}-line`);
	let installed = false;
	let sourceLoaded = false;
	let prevSelected = new Set();

	function applyValues() {
		const map = ctx.map;
		if (!map || !installed || !sourceLoaded) return;
		for (const [areaCode, value] of valueByArea) {
			map.setFeatureState({ source: sourceId, id: areaCode }, { value });
		}
	}

	function applySelection() {
		const map = ctx.map;
		if (!map || !installed || !sourceLoaded) return;
		for (const id of selectedIds) {
			if (!prevSelected.has(id)) {
				map.setFeatureState({ source: sourceId, id }, { selected: true });
			}
		}
		for (const id of prevSelected) {
			if (!selectedIds.has(id)) {
				map.removeFeatureState({ source: sourceId, id }, 'selected');
			}
		}
		prevSelected = new Set(selectedIds);
	}

	onMount(() => {
		const map = ctx.map;
		if (!map) return;

		if (!map.getSource(sourceId)) {
			map.addSource(sourceId, { type: 'geojson', data: geoUrl, promoteId });
		}
		// `beforeId` keeps these below the basemap labels (and the built-up /
		// province overlays) — see layer-order.js.
		const anchor = beforeId(map, ANCHOR_DATA);
		map.addLayer(
			{
				id: fillId,
				type: 'fill',
				source: sourceId,
				paint: { 'fill-color': fillColor, 'fill-opacity': fillOpacity }
			},
			anchor
		);
		map.addLayer(
			{
				id: lineId,
				type: 'line',
				source: sourceId,
				paint: { 'line-color': lineColor, 'line-width': lineWidth }
			},
			anchor
		);
		installed = true;

		const onSourceData = (e) => {
			if (e.sourceId === sourceId && e.isSourceLoaded) {
				sourceLoaded = true;
				applyValues();
				applySelection();
			}
		};
		map.on('sourcedata', onSourceData);

		return () => map.off('sourcedata', onSourceData);
	});

	$effect(() => {
		void valueByArea;
		applyValues();
	});

	$effect(() => {
		void selectedIds;
		applySelection();
	});

	$effect(() => {
		const map = ctx.map;
		if (!map || !installed) return;
		const hasSelection = selectedIds.size > 0;
		const fillOpacityPaint = hasSelection
			? ['case', ['==', ['feature-state', 'selected'], true], fillOpacity, fillOpacity * 0.25]
			: fillOpacity;
		const lineWidthPaint = hasSelection
			? ['case', ['==', ['feature-state', 'selected'], true], lineWidth * 3, lineWidth]
			: lineWidth;
		map.setPaintProperty(fillId, 'fill-color', fillColor);
		map.setPaintProperty(fillId, 'fill-opacity', fillOpacityPaint);
		map.setPaintProperty(lineId, 'line-color', lineColor);
		map.setPaintProperty(lineId, 'line-width', lineWidthPaint);
	});

	onDestroy(() => {
		const map = ctx.map;
		if (!map) return;
		if (map.getLayer(fillId)) map.removeLayer(fillId);
		if (map.getLayer(lineId)) map.removeLayer(lineId);
		if (map.getSource(sourceId)) map.removeSource(sourceId);
	});
</script>
