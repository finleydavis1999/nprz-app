<script>
	import { onMount, onDestroy } from 'svelte';
	import { getMapContext } from './context.js';
	import { beforeId, ANCHOR_DATA } from './layer-order.js';
	import { bezierLine } from '$lib/cartography/curve.js';
	import { stepExpression } from '$lib/cartography/expression.js';

	let {
		sourceId = 'flow',
		flows = [],
		centroids = {},
		breaks,
		colors,
		widthMin = 0.5,
		widthMax = 8,
		opacity = 0.75,
		curvature = 0.2,
		// When set, restrict rendered flows to those touching this node id.
		// `mode` controls how: 'in' = arriving at the node, 'out' = leaving it,
		// 'unified' = one bidirectional line per neighbor with summed value.
		selectedNode = null,
		mode = 'unified'
	} = $props();

	const ctx = getMapContext();
	const casingId = $derived(`${sourceId}-casing`);
	const lineId = $derived(`${sourceId}-line`);
	let installed = false;

	function classIndex(value, bks) {
		const n = bks.length - 1;
		for (let i = 1; i < n; i++) if (value < bks[i]) return i - 1;
		return n - 1;
	}

	// Filter / combine flows based on the optional selected node and mode. For
	// 'in' / 'out' this is a straightforward filter; 'unified' merges each pair
	// touching the selected node into a single bidirectional edge whose value
	// sums both directions.
	const effectiveFlows = $derived.by(() => {
		if (!selectedNode) return flows;
		if (mode === 'in') return flows.filter((f) => f.d === selectedNode);
		if (mode === 'out') return flows.filter((f) => f.o === selectedNode);
		// unified: combine each {selectedNode, neighbor} pair into one entry.
		/** @type {Map<string, number>} */
		// eslint-disable-next-line svelte/prefer-svelte-reactivity -- local accumulator, not reactive
		const byNeighbor = new Map();
		for (const f of flows) {
			if (f.o !== selectedNode && f.d !== selectedNode) continue;
			const neighbor = f.o === selectedNode ? f.d : f.o;
			const prev = byNeighbor.get(neighbor) ?? 0;
			byNeighbor.set(neighbor, prev + f.value);
		}
		const out = [];
		for (const [neighbor, value] of byNeighbor) {
			out.push({ o: selectedNode, d: neighbor, value });
		}
		return out;
	});

	const features = $derived.by(() => {
		if (!effectiveFlows.length || !breaks) return [];
		const sorted = [...effectiveFlows].sort((a, b) => b.value - a.value);
		const out = [];
		for (const f of sorted) {
			const o = centroids[f.o];
			const d = centroids[f.d];
			if (!o || !d) continue;
			out.push({
				type: 'Feature',
				geometry: { type: 'LineString', coordinates: bezierLine(o, d, { curvature }) },
				properties: { o: f.o, d: f.d, value: f.value, classIdx: classIndex(f.value, breaks) }
			});
		}
		return out;
	});

	const featureCollection = $derived({ type: 'FeatureCollection', features });

	const colorExpr = $derived.by(() => {
		if (!breaks || !colors || colors.length === 0) return '#888';
		return stepExpression({ breaks, colors, input: ['get', 'value'] });
	});

	const widthExpr = $derived.by(() => {
		if (!colors || colors.length === 0) return widthMin;
		const n = colors.length;
		if (n === 1) return widthMax;
		return ['interpolate', ['linear'], ['get', 'classIdx'], 0, widthMin, n - 1, widthMax];
	});

	// Casing: scaled width for a subtle white halo behind the main line.
	const casingWidthExpr = $derived.by(() => {
		if (!colors || colors.length === 0) return widthMin * 2;
		const n = colors.length;
		if (n === 1) return widthMax * 2;
		return ['interpolate', ['linear'], ['get', 'classIdx'], 0, widthMin * 2.5, n - 1, widthMax * 2];
	});

	const casingOpacity = $derived(Math.min(1, opacity * 0.5));

	onMount(() => {
		const map = ctx.map;
		if (!map) return;
		if (!map.getSource(sourceId)) {
			map.addSource(sourceId, { type: 'geojson', data: featureCollection });
		}
		const anchor = beforeId(map, ANCHOR_DATA);
		map.addLayer(
			{
				id: casingId,
				type: 'line',
				source: sourceId,
				paint: {
					'line-color': '#ffffff',
					'line-width': casingWidthExpr,
					'line-opacity': casingOpacity,
					'line-blur': 0.5
				},
				layout: { 'line-cap': 'round', 'line-join': 'round' }
			},
			anchor
		);
		map.addLayer(
			{
				id: lineId,
				type: 'line',
				source: sourceId,
				paint: {
					'line-color': colorExpr,
					'line-width': widthExpr,
					'line-opacity': opacity
				},
				layout: { 'line-cap': 'round', 'line-join': 'round' }
			},
			anchor
		);
		installed = true;
	});

	// Push new geometry into the source whenever the derived collection changes.
	$effect(() => {
		const map = ctx.map;
		if (!map || !installed) return;
		const src = map.getSource(sourceId);
		if (src) src.setData(featureCollection);
	});

	// Restyle without remounting on paint changes.
	$effect(() => {
		const map = ctx.map;
		if (!map || !installed) return;
		map.setPaintProperty(lineId, 'line-color', colorExpr);
		map.setPaintProperty(lineId, 'line-width', widthExpr);
		map.setPaintProperty(lineId, 'line-opacity', opacity);
		map.setPaintProperty(casingId, 'line-width', casingWidthExpr);
		map.setPaintProperty(casingId, 'line-opacity', casingOpacity);
	});

	onDestroy(() => {
		const map = ctx.map;
		if (!map) return;
		if (map.getLayer(lineId)) map.removeLayer(lineId);
		if (map.getLayer(casingId)) map.removeLayer(casingId);
		if (map.getSource(sourceId)) map.removeSource(sourceId);
	});
</script>
