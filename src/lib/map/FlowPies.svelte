<script>
	// Overlay of small pie symbols at every node touching the user-selected
	// flow node — plus the selected node itself. Each pie shows the balance of
	// inflow vs outflow (relative to the selected node), and its radius is
	// scaled by total flow so the biggest connections dominate visually.
	//
	// Rendered as an SVG positioned over the map canvas. Pies stay aligned
	// during pan/zoom via `map.project()` updated on `move`/`zoom`.
	import { onMount, onDestroy } from 'svelte';
	import { getMapContext } from './context.js';
	import { geoNames } from '$lib/state/geo-names.svelte.js';

	let {
		selectedNode,
		flows = [],
		centroids = {},
		scale = 'gem',
		maxRadius = 26,
		minRadius = 4,
		inflowColor = '#1f77b4',
		outflowColor = '#d62728'
	} = $props();

	const ctx = getMapContext();

	// Aggregate inflow / outflow per touching node *relative to the selected
	// node*. For the selected node itself: inflow = sum of incoming, outflow =
	// sum of outgoing. For each neighbor Y of selected X:
	//   X→Y contributes outflow at X and inflow at Y.
	//   Y→X contributes inflow at X and outflow at Y.
	// `inCount` / `outCount` accumulate the raw observation count the same way,
	// and stay 0 on non-weighted layers (flows there carry no `count`).
	const pies = $derived.by(() => {
		if (!selectedNode || !flows.length) return { items: [], max: 0 };
		/** @type {Map<string, { inflow: number, outflow: number, inCount: number, outCount: number }>} */
		// eslint-disable-next-line svelte/prefer-svelte-reactivity -- local accumulator
		const m = new Map();
		const ensure = (k) => {
			let cur = m.get(k);
			if (!cur) {
				cur = { inflow: 0, outflow: 0, inCount: 0, outCount: 0 };
				m.set(k, cur);
			}
			return cur;
		};
		ensure(selectedNode);
		for (const f of flows) {
			if (f.o === selectedNode && f.d !== selectedNode) {
				ensure(selectedNode).outflow += f.value;
				ensure(f.d).inflow += f.value;
				if (f.count != null) {
					ensure(selectedNode).outCount += f.count;
					ensure(f.d).inCount += f.count;
				}
			} else if (f.d === selectedNode && f.o !== selectedNode) {
				ensure(selectedNode).inflow += f.value;
				ensure(f.o).outflow += f.value;
				if (f.count != null) {
					ensure(selectedNode).inCount += f.count;
					ensure(f.o).outCount += f.count;
				}
			}
		}
		let max = 0;
		/** @type {Array<{ code: string, inflow: number, outflow: number, inCount: number, outCount: number, total: number }>} */
		const items = [];
		for (const [code, v] of m) {
			const total = v.inflow + v.outflow;
			if (total <= 0) continue;
			if (total > max) max = total;
			items.push({
				code,
				inflow: v.inflow,
				outflow: v.outflow,
				inCount: v.inCount,
				outCount: v.outCount,
				total
			});
		}
		items.sort((a, b) => b.total - a.total);
		return { items, max };
	});

	let projected = $state(
		/** @type {Array<{ code: string, x: number, y: number, inflow: number, outflow: number, inCount: number, outCount: number, total: number, name: string, primary: boolean }>} */ ([])
	);

	/** @type {string | null} */
	let hoveredCode = $state(null);

	const selectedName = $derived(selectedNode ? geoNames.get(scale, selectedNode) : '');
	const hoveredItem = $derived(projected.find((p) => p.code === hoveredCode) ?? null);

	function fmt(v) {
		if (!Number.isFinite(v)) return '—';
		if (Math.abs(v) >= 10_000) return Math.round(v).toLocaleString();
		if (Math.abs(v) >= 100) return v.toFixed(0);
		if (Math.abs(v) >= 1) return v.toFixed(1);
		return v.toFixed(2);
	}

	// Inline " · N obs" suffix for tooltip rows (weighted layers only).
	function obsLabel(n) {
		return ` · ${n.toLocaleString()} obs`;
	}

	function projectAll() {
		const map = ctx.map;
		if (!map || !pies.items.length || pies.max <= 0) {
			projected = [];
			return;
		}
		const out = [];
		for (const item of pies.items) {
			const c = centroids[item.code];
			if (!c) continue;
			const pt = map.project(c);
			out.push({
				...item,
				x: pt.x,
				y: pt.y,
				name: geoNames.get(scale, item.code),
				primary: item.code === selectedNode
			});
		}
		projected = out;
	}

	function radiusFor(total) {
		if (pies.max <= 0) return 0;
		// Area-proportional scaling so a "twice the flow" pie has 2× the area.
		const a = total / pies.max;
		const r = Math.sqrt(a) * (maxRadius - minRadius) + minRadius;
		return r;
	}

	function pieSlicePath(cx, cy, r, startAngle, endAngle) {
		// Full circle case — single arc would be degenerate.
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

	let handlers = null;
	onMount(() => {
		const map = ctx.map;
		if (!map) return;
		const onMove = () => projectAll();
		map.on('move', onMove);
		map.on('zoom', onMove);
		handlers = onMove;
		projectAll();
	});

	// Reproject whenever the pies (or upstream selectedNode/flows) change.
	$effect(() => {
		void pies;
		projectAll();
	});

	onDestroy(() => {
		const map = ctx.map;
		if (map && handlers) {
			map.off('move', handlers);
			map.off('zoom', handlers);
		}
	});
</script>

{#if projected.length > 0}
	<div class="overlay">
		<svg class="pies" xmlns="http://www.w3.org/2000/svg">
			{#each projected as p (p.code)}
				{@const r = radiusFor(p.total)}
				{@const inAngle = (p.inflow / p.total) * Math.PI * 2}
				<g
					class="pie"
					class:primary={p.primary}
					class:hovered={p.code === hoveredCode}
					onpointerenter={() => (hoveredCode = p.code)}
					onpointerleave={() => {
						if (hoveredCode === p.code) hoveredCode = null;
					}}
					role="img"
					aria-label="{p.name}: in {fmt(p.inflow)}, out {fmt(p.outflow)}"
				>
					{#if p.inflow > 0 && p.outflow > 0}
						<path
							d={pieSlicePath(p.x, p.y, r, -Math.PI / 2, -Math.PI / 2 + inAngle)}
							fill={inflowColor}
						/>
						<path
							d={pieSlicePath(p.x, p.y, r, -Math.PI / 2 + inAngle, -Math.PI / 2 + Math.PI * 2)}
							fill={outflowColor}
						/>
					{:else if p.inflow > 0}
						<circle cx={p.x} cy={p.y} {r} fill={inflowColor} />
					{:else}
						<circle cx={p.x} cy={p.y} {r} fill={outflowColor} />
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
					<text class="label" x={p.x + r + 4} y={p.y + 4}>{p.name}</text>
				</g>
			{/each}
		</svg>
		{#if hoveredItem}
			{@const r = radiusFor(hoveredItem.total)}
			<div
				class="tooltip"
				style:left="{hoveredItem.x + r + 8}px"
				style:top="{hoveredItem.y - 28}px"
			>
				<div class="tt-name">{hoveredItem.name}{hoveredItem.primary ? ' — selected' : ''}</div>
				{#if hoveredItem.primary}
					<div class="tt-row" style:color={inflowColor}>
						Total inflow: {fmt(hoveredItem.inflow)}{#if hoveredItem.inCount > 0}<span
								class="tt-count">{obsLabel(hoveredItem.inCount)}</span
							>{/if}
					</div>
					<div class="tt-row" style:color={outflowColor}>
						Total outflow: {fmt(hoveredItem.outflow)}{#if hoveredItem.outCount > 0}<span
								class="tt-count">{obsLabel(hoveredItem.outCount)}</span
							>{/if}
					</div>
				{:else}
					<div class="tt-row" style:color={inflowColor}>
						{selectedName} → {hoveredItem.name}: {fmt(
							hoveredItem.inflow
						)}{#if hoveredItem.inCount > 0}<span class="tt-count"
								>{obsLabel(hoveredItem.inCount)}</span
							>{/if}
					</div>
					<div class="tt-row" style:color={outflowColor}>
						{hoveredItem.name} → {selectedName}: {fmt(
							hoveredItem.outflow
						)}{#if hoveredItem.outCount > 0}<span class="tt-count"
								>{obsLabel(hoveredItem.outCount)}</span
							>{/if}
					</div>
				{/if}
			</div>
		{/if}
	</div>
{/if}

<style>
	.overlay {
		position: absolute;
		inset: 0;
		pointer-events: none;
		z-index: 2;
	}
	.pies {
		width: 100%;
		height: 100%;
	}
	.pie {
		pointer-events: auto;
		cursor: help;
	}
	.pie.hovered circle:nth-child(2),
	.pie.hovered path,
	.pie.hovered circle:first-child {
		filter: brightness(1.08);
	}
	.label {
		font-size: 11px;
		font-family: system-ui, sans-serif;
		fill: #1f2328;
		paint-order: stroke;
		stroke: rgba(255, 255, 255, 0.95);
		stroke-width: 3;
		stroke-linejoin: round;
		pointer-events: none;
	}
	.tooltip {
		position: absolute;
		pointer-events: none;
		background: rgba(255, 255, 255, 0.97);
		border: 1px solid var(--color-line);
		border-radius: var(--radius);
		box-shadow: 0 4px 12px rgba(0, 0, 0, 0.12);
		padding: 6px 8px;
		font-size: var(--text-xs);
		white-space: nowrap;
		font-variant-numeric: tabular-nums;
		max-width: 320px;
	}
	.tt-name {
		font-weight: 600;
		color: var(--color-text);
		margin-bottom: 2px;
	}
	.tt-row {
		font-weight: 500;
	}
	.tt-count {
		color: var(--color-muted);
		font-weight: 400;
	}
</style>
