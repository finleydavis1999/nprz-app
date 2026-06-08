<script>
	import { feature } from 'topojson-client';
	import { geoPath } from 'd3-geo';
	import { rdProjection } from './projection.js';
	import { bezierLine } from '$lib/cartography/curve.js';
	import { INFLOW, OUTFLOW } from '$lib/map/flow-colors.js';
	import { pieSlicePath } from '$lib/map/pie.js';

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
		provinceWidth = 1.5,
		// Optional RD bbox `[[minX,minY],[maxX,maxY]]` (metres). When set, the
		// projection fits this extent into the SVG instead of fitting all
		// features — used to mirror what the user framed on the live map.
		extent = /** @type {[[number, number], [number, number]] | null} */ (null),
		// Scale bar (bottom-left). Hidden when `null`.
		showScaleBar = true,
		// Place labels (city/town/region names) in RD coordinates, captured
		// from the live map's Protomaps tiles at frame time. Rendered as SVG
		// <text> with halo, styled by `kind`.
		placeLabels = /** @type {{text:string, x:number, y:number, kind:string, kindDetail?:string, populationRank?:number}[]} */ ([]),
		// Optional curation: keys (`text|kind`) the user has turned off, plus
		// a minimum population-rank threshold (0 = no threshold). Higher
		// threshold = fewer, more important labels. The underlying
		// `placeLabels` array stays whole so toggling things back on doesn't
		// need a re-capture.
		disabledLabelKeys = /** @type {Set<string> | string[] | null} */ (null),
		minPopulationRank = 0
	} = $props();

	// Resolve the user's curation to a Set for O(1) membership checks.
	const disabledSet = $derived.by(() => {
		if (!disabledLabelKeys) return null;
		if (disabledLabelKeys instanceof Set) return disabledLabelKeys;
		return new Set(disabledLabelKeys);
	});

	// Pre-filter so the template stays simple.
	const visiblePlaceLabels = $derived(
		placeLabels.filter(
			(l) =>
				(!disabledSet || !disabledSet.has(`${l.text}|${l.kind}`)) &&
				(l.populationRank ?? 0) >= minPopulationRank
		)
	);

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
		return rdProjection([width, height], features, extent);
	});
	const path = $derived.by(() => (projection ? geoPath(projection) : null));

	// Scale bar geometry. For `geoIdentity()` over RD metres, the projection's
	// `scale()` is the px-per-metre factor (modulo reflectY). We pick a target
	// pixel length ~90px and round the corresponding ground distance to the
	// nearest 1/2/5 × 10^n so the bar shows a "nice" round number.
	const scaleBar = $derived.by(() => {
		if (!showScaleBar || !projection) return null;
		const pxPerMetre = projection.scale();
		if (!Number.isFinite(pxPerMetre) || pxPerMetre <= 0) return null;
		const targetPx = Math.min(120, width * 0.18);
		const rawMetres = targetPx / pxPerMetre;
		const niceMetres = niceRound(rawMetres);
		const px = niceMetres * pxPerMetre;
		const label = niceMetres >= 1000 ? `${niceMetres / 1000} km` : `${niceMetres} m`;
		return { px, label };
	});

	function niceRound(v) {
		if (v <= 0) return 0;
		const exp = Math.floor(Math.log10(v));
		const base = Math.pow(10, exp);
		const f = v / base;
		// Step to the nearest "nice" 1/2/5 multiplier — slightly biased downward
		// so the bar never overshoots the target pixel width.
		const step = f < 1.5 ? 1 : f < 3.5 ? 2 : f < 7.5 ? 5 : 10;
		return step * base;
	}

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
			// `magnitude` drives the radius: one direction in in/out mode, the
			// combined total in unified mode (mirrors FlowPies.svelte).
			const magnitude = flowMode === 'in' ? v.inflow : flowMode === 'out' ? v.outflow : total;
			if (magnitude <= 0) continue;
			if (magnitude > max) max = magnitude;
			const c = centroids?.[code];
			if (!c) continue;
			const projected = projection(c);
			if (!projected) continue;
			items.push({
				code,
				inflow: v.inflow,
				outflow: v.outflow,
				total,
				magnitude,
				x: projected[0],
				y: projected[1],
				name: nameByCode?.get(code) ?? code,
				primary: code === selectedFlowNode
			});
		}
		items.sort((a, b) => b.magnitude - a.magnitude);
		return { items, max };
	});

	function pieRadius(total, max) {
		if (max <= 0) return 0;
		const maxR = 28;
		const minR = 4;
		const a = total / max;
		return Math.sqrt(a) * (maxR - minR) + minR;
	}
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
		{#if visiblePlaceLabels.length > 0 && projection}
			<g class="place-labels">
				{#each visiblePlaceLabels as p, i (`pl-${p.text}-${p.kind}-${i}`)}
					{@const pt = projection([p.x, p.y])}
					{#if pt}
						<text
							class="place-label place-label--{p.kind}"
							x={pt[0]}
							y={pt[1]}
							text-anchor="middle"
						>
							{p.text}
						</text>
					{/if}
				{/each}
			</g>
		{/if}
		{#if scaleBar}
			<g class="scalebar" transform="translate(20 {height - 24})">
				<line
					x1="0"
					y1="0"
					x2={scaleBar.px}
					y2="0"
					stroke="#1f2328"
					stroke-width="2"
					stroke-linecap="square"
				/>
				<line x1="0" y1="-4" x2="0" y2="4" stroke="#1f2328" stroke-width="1.5" />
				<line
					x1={scaleBar.px}
					y1="-4"
					x2={scaleBar.px}
					y2="4"
					stroke="#1f2328"
					stroke-width="1.5"
				/>
				<text class="scalebar-label" x={scaleBar.px / 2} y="14" text-anchor="middle">
					{scaleBar.label}
				</text>
			</g>
		{/if}
		{#if pies.items.length > 0}
			<g class="pies">
				{#each pies.items as p (p.code)}
					{@const r = pieRadius(p.magnitude, pies.max)}
					{@const inAngle = (p.inflow / p.total) * Math.PI * 2}
					{#if flowMode === 'in'}
						<circle cx={p.x} cy={p.y} {r} fill={INFLOW} />
					{:else if flowMode === 'out'}
						<circle cx={p.x} cy={p.y} {r} fill={OUTFLOW} />
					{:else if p.inflow > 0 && p.outflow > 0}
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
	/* Basemap place labels — sized by hierarchy. Halo (paint-order: stroke)
	   keeps the text legible against any choropleth colour underneath. */
	.place-label {
		font-family: system-ui, sans-serif;
		fill: #1f2328;
		paint-order: stroke;
		stroke: rgba(255, 255, 255, 0.95);
		stroke-width: 2.5;
		stroke-linejoin: round;
		font-weight: 500;
		pointer-events: none;
	}
	.place-label--country {
		font-size: 14px;
		font-weight: 700;
		letter-spacing: 0.08em;
		text-transform: uppercase;
	}
	.place-label--region {
		font-size: 11px;
		font-weight: 600;
		letter-spacing: 0.04em;
		text-transform: uppercase;
	}
	.place-label--locality {
		font-size: 10px;
		font-weight: 600;
	}
	.place-label--subplace {
		font-size: 8px;
		font-weight: 500;
		fill: #4a5159;
	}
	.scalebar-label {
		font-size: 10px;
		font-family: system-ui, sans-serif;
		fill: #1f2328;
		paint-order: stroke;
		stroke: rgba(255, 255, 255, 0.95);
		stroke-width: 2.5;
		stroke-linejoin: round;
	}
</style>
