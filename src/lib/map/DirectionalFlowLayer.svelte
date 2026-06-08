<script>
	import { onMount, onDestroy } from 'svelte';
	import { getMapContext } from './context.js';
	import { beforeId, ANCHOR_DATA } from './layer-order.js';
	import { bezierLine } from '$lib/cartography/curve.js';
	import { classify } from '$lib/cartography/classify.js';
	import { INFLOW, OUTFLOW, FLOW_MAJOR, FLOW_MINOR } from './flow-colors.js';

	let {
		sourceId = 'flow-directional',
		pairs = [],
		centroids = {},
		widthMin = 0.5,
		widthMax = 8,
		opacity = 0.75,
		curvature = 0.2,
		fwdColor = OUTFLOW,
		revColor = INFLOW,
		majorColor = FLOW_MAJOR,
		minorColor = FLOW_MINOR,
		method = 'quantile',
		n = 5,
		showBalance = true,
		selectedNode = null,
		mode = 'unified'
	} = $props();

	const ctx = getMapContext();
	const lineId = $derived(`${sourceId}-line`);
	const casingId = $derived(`${sourceId}-casing`);
	const balanceSourceId = $derived(`${sourceId}-balance`);
	const balanceId = $derived(`${sourceId}-balance-dot`);
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

	// Split a bezier polyline at fraction t of its total length (by cumulative
	// segment length, not by index — segments are near-uniform for a bezier but
	// length is the honest measure). Returns the two sub-polylines: `fwd`
	// (origin → split point) and `rev` (split point → destination). t is
	// clamped to [0, 1]. Segment lengths are computed once.
	function splitPolyline(coords, t) {
		if (coords.length < 2) return { fwd: [...coords], rev: [...coords] };
		let totalLen = 0;
		const segLen = [];
		for (let i = 1; i < coords.length; i++) {
			const l = Math.hypot(coords[i][0] - coords[i - 1][0], coords[i][1] - coords[i - 1][1]);
			segLen.push(l);
			totalLen += l;
		}
		const targetLen = totalLen * Math.min(1, Math.max(0, t));
		let acc = 0;
		for (let i = 0; i < segLen.length; i++) {
			if (acc + segLen[i] >= targetLen) {
				const frac = segLen[i] === 0 ? 0 : (targetLen - acc) / segLen[i];
				const a = coords[i];
				const b = coords[i + 1];
				const split = [a[0] + (b[0] - a[0]) * frac, a[1] + (b[1] - a[1]) * frac];
				return { fwd: [...coords.slice(0, i + 1), split], rev: [split, ...coords.slice(i + 1)] };
			}
			acc += segLen[i];
		}
		const last = coords[coords.length - 1];
		return { fwd: [...coords], rev: [last] };
	}

	// Builds both the line features and the balance-point dots in one pass.
	const geometry = $derived.by(() => {
		const features = [];
		const balance = [];
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
			// direction. These ARE relative to the focal node, so they keep the
			// blue/red in/out colors. No balance dot — there's a single direction.
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
						ck: mode === 'out' ? 'out' : 'in',
						width: widthForClass(sci),
						o: p.o,
						d: p.d,
						total: val
					}
				});
				continue;
			}

			// Unified: one line per pair, split at the directional proportion.
			// o→d is the dominant direction (see +page.svelte), so the fwd segment
			// is the major share (orange) and the rev segment the minority (teal).
			const coords = bezierLine(oC, dC, { curvature });
			const fwdFrac = p.fwdFrac ?? p.fwdVal / total;
			const { fwd: fwdCoords, rev: revCoords } = splitPolyline(coords, fwdFrac);

			if (fwdFrac > 0.001) {
				features.push({
					type: 'Feature',
					geometry: { type: 'LineString', coordinates: fwdCoords },
					properties: { ck: 'major', width: w, o: p.o, d: p.d, total }
				});
			}
			if (fwdFrac < 0.999) {
				features.push({
					type: 'Feature',
					geometry: { type: 'LineString', coordinates: revCoords },
					properties: { ck: 'minor', width: w, o: p.o, d: p.d, total }
				});
			}
			// Balance dot at the split (where the two directions meet) — only when
			// both segments are present.
			if (fwdFrac > 0.001 && fwdFrac < 0.999) {
				const split = fwdCoords[fwdCoords.length - 1];
				balance.push({
					type: 'Feature',
					geometry: { type: 'Point', coordinates: split },
					properties: { o: p.o, d: p.d, total }
				});
			}
		}
		return { features, balance };
	});

	const featureCollection = $derived({ type: 'FeatureCollection', features: geometry.features });
	const balanceCollection = $derived({ type: 'FeatureCollection', features: geometry.balance });

	const colorExpr = $derived([
		'match',
		['get', 'ck'],
		'major',
		majorColor,
		'minor',
		minorColor,
		'out',
		fwdColor,
		'in',
		revColor,
		'#888'
	]);
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
		// Balance-point dots sit at the gradient split. Added with NO beforeId so
		// they land on top of the stack — always drawn above the flow lines.
		if (!map.getSource(balanceSourceId)) {
			map.addSource(balanceSourceId, { type: 'geojson', data: balanceCollection });
		}
		map.addLayer({
			id: balanceId,
			type: 'circle',
			source: balanceSourceId,
			layout: { visibility: showBalance ? 'visible' : 'none' },
			paint: {
				'circle-radius': 3.5,
				'circle-color': '#404040',
				'circle-opacity': opacity
			}
		});
		installed = true;
	});

	$effect(() => {
		const map = ctx.map;
		if (!map || !installed) return;
		const src = map.getSource(sourceId);
		if (src) src.setData(featureCollection);
		const bsrc = map.getSource(balanceSourceId);
		if (bsrc) bsrc.setData(balanceCollection);
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
		if (map.getLayer(balanceId)) {
			map.setPaintProperty(balanceId, 'circle-opacity', opacity);
			map.setLayoutProperty(balanceId, 'visibility', showBalance ? 'visible' : 'none');
		}
	});

	onDestroy(() => {
		const map = ctx.map;
		if (!map) return;
		if (map.getLayer(balanceId)) map.removeLayer(balanceId);
		if (map.getLayer(lineId)) map.removeLayer(lineId);
		if (map.getLayer(casingId)) map.removeLayer(casingId);
		if (map.getSource(balanceSourceId)) map.removeSource(balanceSourceId);
		if (map.getSource(sourceId)) map.removeSource(sourceId);
	});
</script>
