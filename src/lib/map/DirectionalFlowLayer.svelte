<script>
	import { onMount, onDestroy } from 'svelte';
	import { getMapContext } from './context.js';
	import { beforeId, ANCHOR_DATA } from './layer-order.js';
	import { bezierLine } from '$lib/cartography/curve.js';
	import { classify } from '$lib/cartography/classify.js';

	let {
		sourceId = 'flow-directional',
		pairs = [],
		centroids = {},
		widthMin = 0.5,
		widthMax = 8,
		opacity = 0.75,
		curvature = 0.2,
		fwdColor = '#d62728',
		revColor = '#1f77b4',
		method = 'quantile',
		n = 5,
		selectedNode = null,
		mode = 'unified'
	} = $props();

	const ctx = getMapContext();
	const lineId = $derived(`${sourceId}-line`);
	const casingId = $derived(`${sourceId}-casing`);
	let installed = false;

	// Classify pair totals into buckets, like the classic FlowLayer (by class
	// index, not raw value) so width spreads evenly across buckets regardless
	// of how skewed the flow distribution is. Without this a few huge flows
	// compress everything small into an indistinguishable thin band.
	const breaks = $derived.by(() => {
		const totals = pairs.map((p) => p.total).filter((v) => Number.isFinite(v) && v > 0);
		if (totals.length === 0) return null;
		return classify(totals, { method, n });
	});

	function classIndex(value, bks) {
		const count = bks.length - 1;
		for (let i = 1; i < count; i++) if (value < bks[i]) return i - 1;
		return count - 1;
	}

	// Width interpolates by class index from widthMin to widthMax.
	function widthForClass(classIdx) {
		const buckets = breaks ? breaks.length - 1 : 1;
		if (buckets <= 1) return widthMax;
		const t = classIdx / (buckets - 1);
		return t * (widthMax - widthMin) + widthMin;
	}

	// Linear-interpolate a point along the bezier polyline at fraction t of its
	// total length (by cumulative segment length, not by index — segments are
	// near-uniform for a bezier but length is the honest measure).
	function pointAtFraction(coords, t) {
		if (coords.length < 2) return coords[0];
		if (t <= 0) return coords[0];
		if (t >= 1) return coords[coords.length - 1];
		let totalLen = 0;
		const segLen = [];
		for (let i = 1; i < coords.length; i++) {
			const dx = coords[i][0] - coords[i - 1][0];
			const dy = coords[i][1] - coords[i - 1][1];
			const l = Math.hypot(dx, dy);
			segLen.push(l);
			totalLen += l;
		}
		const targetLen = totalLen * t;
		let acc = 0;
		for (let i = 0; i < segLen.length; i++) {
			if (acc + segLen[i] >= targetLen) {
				const frac = segLen[i] === 0 ? 0 : (targetLen - acc) / segLen[i];
				const a = coords[i];
				const b = coords[i + 1];
				return [a[0] + (b[0] - a[0]) * frac, a[1] + (b[1] - a[1]) * frac];
			}
			acc += segLen[i];
		}
		return coords[coords.length - 1];
	}

	const featureCollection = $derived.by(() => {
		const features = [];
		const activePairs = selectedNode
			? pairs.filter((p) => p.o === selectedNode || p.d === selectedNode)
			: pairs;

		for (const p of activePairs) {
			const oC = centroids[p.o];
			const dC = centroids[p.d];
			if (!oC || !dC) continue;
			const total = p.total;
			if (total <= 0) continue;
			const ci = breaks ? classIndex(total, breaks) : 0;
			const w = widthForClass(ci);

			// In/out modes (only meaningful with a selected node) render a single
			// directed line per neighbour, single-colored, sized by that one
			// direction. Unified renders the merged red/blue split pair.
			if (selectedNode && (mode === 'in' || mode === 'out')) {
				const selIsO = p.o === selectedNode;
				// out = flow from the selected node to the neighbour;
				// in  = flow from the neighbour to the selected node.
				let val;
				if (mode === 'out') {
					val = selIsO ? p.fwdVal : p.revVal;
				} else {
					val = selIsO ? p.revVal : p.fwdVal;
				}
				if (!(val > 0)) continue;
				const coords = bezierLine(oC, dC, { curvature });
				const sci = breaks ? classIndex(val, breaks) : 0;
				features.push({
					type: 'Feature',
					geometry: { type: 'LineString', coordinates: coords },
					properties: {
						dir: mode === 'out' ? 'fwd' : 'rev',
						width: widthForClass(sci),
						o: p.o,
						d: p.d,
						total: val
					}
				});
				continue;
			}

			// Unified: one line per pair, split red (o→d) / blue (d→o) at the
			// directional proportion.
			const coords = bezierLine(oC, dC, { curvature });
			const fwdFrac = p.fwdFrac ?? p.fwdVal / total;
			const split = pointAtFraction(coords, fwdFrac);

			// Forward segment: o → split.
			const fwdCoords = [coords[0]];
			{
				let totalLen = 0;
				const segLen = [];
				for (let i = 1; i < coords.length; i++) {
					segLen.push(Math.hypot(coords[i][0] - coords[i - 1][0], coords[i][1] - coords[i - 1][1]));
					totalLen += segLen[i - 1];
				}
				const targetLen = totalLen * fwdFrac;
				let acc = 0;
				for (let i = 0; i < segLen.length; i++) {
					if (acc + segLen[i] >= targetLen) break;
					acc += segLen[i];
					fwdCoords.push(coords[i + 1]);
				}
				fwdCoords.push(split);
			}
			// Reverse segment: split → d.
			const revCoords = [split];
			{
				let totalLen = 0;
				const segLen = [];
				for (let i = 1; i < coords.length; i++) {
					segLen.push(Math.hypot(coords[i][0] - coords[i - 1][0], coords[i][1] - coords[i - 1][1]));
					totalLen += segLen[i - 1];
				}
				const targetLen = totalLen * fwdFrac;
				let acc = 0;
				let startIdx = coords.length - 1;
				for (let i = 0; i < segLen.length; i++) {
					if (acc + segLen[i] >= targetLen) {
						startIdx = i + 1;
						break;
					}
					acc += segLen[i];
				}
				for (let i = startIdx; i < coords.length; i++) revCoords.push(coords[i]);
			}

			if (fwdFrac > 0.001) {
				features.push({
					type: 'Feature',
					geometry: { type: 'LineString', coordinates: fwdCoords },
					properties: { dir: 'fwd', width: w, o: p.o, d: p.d, total }
				});
			}
			if (fwdFrac < 0.999) {
				features.push({
					type: 'Feature',
					geometry: { type: 'LineString', coordinates: revCoords },
					properties: { dir: 'rev', width: w, o: p.o, d: p.d, total }
				});
			}
		}
		return { type: 'FeatureCollection', features };
	});

	const colorExpr = $derived(['match', ['get', 'dir'], 'fwd', fwdColor, 'rev', revColor, '#888']);
	const widthExpr = $derived(['get', 'width']);

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
					'line-width': ['+', ['get', 'width'], 2],
					'line-opacity': Math.min(1, opacity * 0.5),
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

	$effect(() => {
		const map = ctx.map;
		if (!map || !installed) return;
		const src = map.getSource(sourceId);
		if (src) src.setData(featureCollection);
	});

	$effect(() => {
		const map = ctx.map;
		if (!map || !installed) return;
		if (map.getLayer(lineId)) {
			map.setPaintProperty(lineId, 'line-color', colorExpr);
			map.setPaintProperty(lineId, 'line-opacity', opacity);
		}
		if (map.getLayer(casingId)) {
			map.setPaintProperty(casingId, 'line-opacity', Math.min(1, opacity * 0.5));
		}
	});

	onDestroy(() => {
		const map = ctx.map;
		if (!map) return;
		if (map.getLayer(lineId)) map.removeLayer(lineId);
		if (map.getLayer(casingId)) map.removeLayer(casingId);
		if (map.getSource(sourceId)) map.removeSource(sourceId);
	});
</script>
