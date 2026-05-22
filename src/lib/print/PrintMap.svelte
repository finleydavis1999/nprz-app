<script>
	import { feature } from 'topojson-client';
	import { geoPath } from 'd3-geo';
	import { rdProjection } from './projection.js';
	import { bezierLine } from '$lib/cartography/curve.js';

	/**
	 * @typedef {Object} FlowFeat
	 * @property {string} o
	 * @property {string} d
	 * @property {number} value
	 */
	let {
		topojsonUrl,
		objectKey = null,
		valueByArea,
		breaks,
		colors,
		width = 800,
		height = 1000,
		strokeColor = '#666',
		strokeWidth = 0.2,
		nullColor = '#eee',
		idProp = 'area_code',
		// Flow rendering (optional). If `flows` is non-empty, draws curved OD
		// lines on top of the choropleth using the same projection.
		flows = /** @type {FlowFeat[]} */ ([]),
		centroids = /** @type {Record<string, [number, number]>} */ ({}),
		flowBreaks = /** @type {number[] | null} */ (null),
		flowColors = /** @type {string[]} */ ([]),
		widthMin = 0.5,
		widthMax = 6,
		opacity = 0.8,
		curvature = 0.2,
		selectedFlowNode = /** @type {string | null} */ (null),
		flowMode = /** @type {'in' | 'out' | 'unified'} */ ('unified'),
		// Optional name lookup for pie labels (Map<area_code, name>).
		nameByCode = /** @type {Map<string, string> | null} */ (null),
		piesEnabled = true,
		labelLayer = false,
		// Cartographic overlays (optional): separate RD TopoJSON files drawn with
		// the same projection. Pass a URL to enable the layer; null omits it.
		boundaryUrl = /** @type {string | null} */ (null),
		boundaryColor = '#222222',
		boundaryWidth = 1.0,
		boundaryOpacity = 0.8,
		builtupUrl = /** @type {string | null} */ (null),
		builtupColor = '#888888',
		builtupOpacity = 0.5,
		provinceUrl = /** @type {string | null} */ (null),
		provinceColor = '#555555',
		provinceWidth = 1.5
	} = $props();

	// Derive objectKey from the URL when not explicitly given.
	// `geo/pc4.topo.json` → `pc4`. The R pipeline writes the same key.
	const resolvedKey = $derived(objectKey ?? topojsonUrl.split('/').pop()?.split('.')[0] ?? null);

	let topo = $state(null);
	let topoError = $state(/** @type {string | null} */ (null));

	$effect(() => {
		topo = null;
		topoError = null;
		fetch(topojsonUrl)
			.then((r) => {
				if (!r.ok) throw new Error(`HTTP ${r.status}`);
				return r.json();
			})
			.then((t) => {
				topo = t;
			})
			.catch((e) => {
				topoError = e.message;
			});
	});

	const features = $derived.by(() => {
		if (!topo || !resolvedKey) return null;
		const obj = topo.objects?.[resolvedKey];
		if (!obj) return null;
		return feature(topo, obj);
	});

	// Overlay TopoJSON (province boundary, built-up area) — each a separate file
	// fetched on demand and drawn with the same RD projection as the main map.
	// The R pipeline names the TopoJSON object after the file basename.
	function topoToFeatures(t, url) {
		if (!t || !url) return null;
		const key = url.split('/').pop()?.split('.')[0];
		const obj = key ? t.objects?.[key] : null;
		return obj ? feature(t, obj) : null;
	}

	let boundaryTopo = $state(null);
	$effect(() => {
		boundaryTopo = null;
		if (!boundaryUrl) return;
		fetch(boundaryUrl)
			.then((r) => (r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`))))
			.then((t) => {
				boundaryTopo = t;
			})
			.catch(() => {
				boundaryTopo = null;
			});
	});
	const boundaryFeatures = $derived(topoToFeatures(boundaryTopo, boundaryUrl));

	let builtupTopo = $state(null);
	$effect(() => {
		builtupTopo = null;
		if (!builtupUrl) return;
		fetch(builtupUrl)
			.then((r) => (r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`))))
			.then((t) => {
				builtupTopo = t;
			})
			.catch(() => {
				builtupTopo = null;
			});
	});
	const builtupFeatures = $derived(topoToFeatures(builtupTopo, builtupUrl));

	let provinceTopo = $state(null);
	$effect(() => {
		provinceTopo = null;
		if (!provinceUrl) return;
		fetch(provinceUrl)
			.then((r) => (r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`))))
			.then((t) => {
				provinceTopo = t;
			})
			.catch(() => {
				provinceTopo = null;
			});
	});
	const provinceFeatures = $derived(topoToFeatures(provinceTopo, provinceUrl));

	const projection = $derived.by(() => {
		if (!features) return null;
		return rdProjection([width, height], features);
	});
	const path = $derived.by(() => (projection ? geoPath(projection) : null));

	function fillFor(value) {
		if (value == null || !breaks || breaks.length < 2) return nullColor;
		// breaks is length n+1; classes are [breaks[i], breaks[i+1])
		for (let i = 1; i < breaks.length - 1; i++) {
			if (value < breaks[i]) return colors[i - 1];
		}
		return colors[colors.length - 1];
	}

	function classIndex(v, bks) {
		const n = bks.length - 1;
		for (let i = 1; i < n; i++) if (v < bks[i]) return i - 1;
		return n - 1;
	}

	function flowColorFor(value) {
		if (!flowBreaks || !flowColors.length) return '#888';
		return flowColors[classIndex(value, flowBreaks)];
	}

	function flowWidthFor(value) {
		if (!flowBreaks || flowColors.length === 0) return widthMin;
		const n = flowColors.length;
		if (n === 1) return widthMax;
		const idx = classIndex(value, flowBreaks);
		return widthMin + (widthMax - widthMin) * (idx / (n - 1));
	}

	// Filter / combine flows to mirror the live map's selectedNode behavior.
	const effectiveFlows = $derived.by(() => {
		if (!selectedFlowNode) return flows;
		if (flowMode === 'in') return flows.filter((f) => f.d === selectedFlowNode);
		if (flowMode === 'out') return flows.filter((f) => f.o === selectedFlowNode);
		// unified: combine pairs touching the selected node.
		// eslint-disable-next-line svelte/prefer-svelte-reactivity -- local accumulator
		const byNeighbor = new Map();
		for (const f of flows) {
			if (f.o !== selectedFlowNode && f.d !== selectedFlowNode) continue;
			const neighbor = f.o === selectedFlowNode ? f.d : f.o;
			byNeighbor.set(neighbor, (byNeighbor.get(neighbor) ?? 0) + f.value);
		}
		const out = [];
		for (const [neighbor, value] of byNeighbor) {
			out.push({ o: selectedFlowNode, d: neighbor, value });
		}
		return out;
	});

	const flowPathGenerator = $derived.by(() => (projection ? geoPath(projection) : null));

	function flowPath(o, d) {
		if (!flowPathGenerator) return '';
		const co = centroids?.[o];
		const cd = centroids?.[d];
		if (!co || !cd) return '';
		const coords = bezierLine(co, cd, { curvature });
		return flowPathGenerator({ type: 'LineString', coordinates: coords }) ?? '';
	}

	// Per-node pie aggregation, mirroring FlowPies.svelte's semantics.
	const pies = $derived.by(() => {
		if (!piesEnabled || !selectedFlowNode || !flows.length || !projection) {
			return { items: [], max: 0 };
		}
		// eslint-disable-next-line svelte/prefer-svelte-reactivity -- local accumulator
		const m = new Map();
		const ensure = (k) => {
			let cur = m.get(k);
			if (!cur) {
				cur = { inflow: 0, outflow: 0 };
				m.set(k, cur);
			}
			return cur;
		};
		ensure(selectedFlowNode);
		for (const f of flows) {
			if (f.o === selectedFlowNode && f.d !== selectedFlowNode) {
				ensure(selectedFlowNode).outflow += f.value;
				ensure(f.d).inflow += f.value;
			} else if (f.d === selectedFlowNode && f.o !== selectedFlowNode) {
				ensure(selectedFlowNode).inflow += f.value;
				ensure(f.o).outflow += f.value;
			}
		}
		let max = 0;
		const items = [];
		for (const [code, v] of m) {
			const total = v.inflow + v.outflow;
			if (total <= 0) continue;
			if (total > max) max = total;
			const c = centroids?.[code];
			if (!c) continue;
			const projected = projection(c);
			if (!projected) continue;
			items.push({
				code,
				inflow: v.inflow,
				outflow: v.outflow,
				total,
				x: projected[0],
				y: projected[1],
				name: nameByCode?.get(code) ?? code,
				primary: code === selectedFlowNode
			});
		}
		items.sort((a, b) => b.total - a.total);
		return { items, max };
	});

	function pieRadius(total, max) {
		if (max <= 0) return 0;
		const maxR = 28;
		const minR = 4;
		const a = total / max;
		return Math.sqrt(a) * (maxR - minR) + minR;
	}

	function pieSlicePath(cx, cy, r, startAngle, endAngle) {
		if (endAngle - startAngle >= Math.PI * 2 - 1e-6) {
			return `M ${cx - r} ${cy} a ${r} ${r} 0 1 0 ${r * 2} 0 a ${r} ${r} 0 1 0 ${-r * 2} 0 Z`;
		}
		const x1 = cx + r * Math.cos(startAngle);
		const y1 = cy + r * Math.sin(startAngle);
		const x2 = cx + r * Math.cos(endAngle);
		const y2 = cy + r * Math.sin(endAngle);
		const large = endAngle - startAngle > Math.PI ? 1 : 0;
		return `M ${cx} ${cy} L ${x1} ${y1} A ${r} ${r} 0 ${large} 1 ${x2} ${y2} Z`;
	}

	const INFLOW = '#1f77b4';
	const OUTFLOW = '#d62728';
</script>

{#if topoError}
	<p class="err">Failed to load {topojsonUrl}: {topoError}</p>
{:else if !features || !path}
	<p class="hint">Loading map…</p>
{:else}
	<svg
		viewBox="0 0 {width} {height}"
		preserveAspectRatio="xMidYMid meet"
		xmlns="http://www.w3.org/2000/svg"
	>
		<g class="features">
			{#each features.features as f, i (f.properties?.[idProp] ?? i)}
				<path
					d={path(f)}
					fill={fillFor(valueByArea.get(f.properties?.[idProp]))}
					stroke={strokeColor}
					stroke-width={strokeWidth}
					stroke-linejoin="round"
				/>
			{/each}
		</g>
		{#if boundaryFeatures}
			<g class="boundary">
				{#each boundaryFeatures.features as f, i (i)}
					<path
						d={path(f)}
						fill="none"
						stroke={boundaryColor}
						stroke-width={boundaryWidth}
						stroke-opacity={boundaryOpacity}
						stroke-linejoin="round"
					/>
				{/each}
			</g>
		{/if}
		{#if builtupFeatures}
			<g class="builtup">
				{#each builtupFeatures.features as f, i (i)}
					<path d={path(f)} fill={builtupColor} fill-opacity={builtupOpacity} stroke="none" />
				{/each}
			</g>
		{/if}
		{#if provinceFeatures}
			<g class="provinces">
				{#each provinceFeatures.features as f, i (i)}
					<path
						d={path(f)}
						fill="none"
						stroke={provinceColor}
						stroke-width={provinceWidth}
						stroke-linejoin="round"
					/>
				{/each}
			</g>
		{/if}
		{#if effectiveFlows.length > 0 && flowBreaks}
			<g class="flows" {opacity}>
				{#each [...effectiveFlows].sort((a, b) => b.value - a.value) as f, i (`${f.o}|${f.d}|${i}`)}
					{@const d = flowPath(f.o, f.d)}
					{#if d}
						<path
							{d}
							fill="none"
							stroke="#fff"
							stroke-width={flowWidthFor(f.value) * 2.2}
							stroke-opacity="0.55"
							stroke-linecap="round"
							stroke-linejoin="round"
						/>
						<path
							{d}
							fill="none"
							stroke={flowColorFor(f.value)}
							stroke-width={flowWidthFor(f.value)}
							stroke-linecap="round"
							stroke-linejoin="round"
						/>
					{/if}
				{/each}
			</g>
		{/if}
		{#if labelLayer && nameByCode}
			<g class="labels">
				{#each features.features as f, i (`lbl-${f.properties?.[idProp] ?? i}`)}
					{@const code = f.properties?.[idProp]}
					{@const c = projection && centroids?.[code]}
					{@const pt = c ? projection(c) : null}
					{@const name = nameByCode.get(String(code))}
					{#if pt && name}
						<text class="map-label" x={pt[0]} y={pt[1]} text-anchor="middle">{name}</text>
					{/if}
				{/each}
			</g>
		{/if}
		{#if pies.items.length > 0}
			<g class="pies">
				{#each pies.items as p (p.code)}
					{@const r = pieRadius(p.total, pies.max)}
					{@const inAngle = (p.inflow / p.total) * Math.PI * 2}
					{#if p.inflow > 0 && p.outflow > 0}
						<path
							d={pieSlicePath(p.x, p.y, r, -Math.PI / 2, -Math.PI / 2 + inAngle)}
							fill={INFLOW}
						/>
						<path
							d={pieSlicePath(p.x, p.y, r, -Math.PI / 2 + inAngle, -Math.PI / 2 + Math.PI * 2)}
							fill={OUTFLOW}
						/>
					{:else if p.inflow > 0}
						<circle cx={p.x} cy={p.y} {r} fill={INFLOW} />
					{:else}
						<circle cx={p.x} cy={p.y} {r} fill={OUTFLOW} />
					{/if}
					<circle cx={p.x} cy={p.y} {r} fill="none" stroke="#fff" stroke-width="1.5" />
					{#if p.primary}
						<circle
							cx={p.x}
							cy={p.y}
							{r}
							fill="none"
							stroke="#1f2328"
							stroke-width="2"
							stroke-dasharray="3 2"
						/>
					{/if}
					<text class="pie-label" x={p.x + r + 4} y={p.y + 4}>{p.name}</text>
				{/each}
			</g>
		{/if}
	</svg>
{/if}

<style>
	svg {
		width: 100%;
		height: auto;
		display: block;
	}
	.err {
		color: #cf222e;
		font-size: var(--text-sm);
	}
	.hint {
		color: var(--color-hint);
		font-size: var(--text-sm);
	}
	.pie-label,
	.map-label {
		font-size: 9px;
		font-family: system-ui, sans-serif;
		fill: #1f2328;
		paint-order: stroke;
		stroke: rgba(255, 255, 255, 0.95);
		stroke-width: 2;
		stroke-linejoin: round;
	}
	.map-label {
		font-size: 6px;
	}
</style>
