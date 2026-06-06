<script>
	import Histogram from '$lib/cartography/Histogram.svelte';
	import Field from './Field.svelte';
	import SegmentedControl from './SegmentedControl.svelte';
	import { ui } from '$lib/state/ui.svelte.js';
	import { geoNames } from '$lib/state/geo-names.svelte.js';
	import { scaleLabel } from '$lib/scales.js';
	import { INFLOW, OUTFLOW } from '$lib/map/flow-colors.js';
	import { pieSlicePath } from '$lib/map/pie.js';
	import { fmtFlowMaybeBelow } from '$lib/flow-format.js';

	let {
		nodeValueByArea = new Map(),
		nodeValues = [],
		nodeBreaks = null,
		nodeColors = [],
		nodeLabel = '',
		nodeScale = 'gem',
		flowEnabled = false,
		flowScale = 'gem',
		flowsByPair = new Map(),
		flowValues = [],
		flowBreaks = null,
		flowColors = [],
		flowMinWeight = 0
	} = $props();

	// Pinned (clicked) wins over hovered for the displayed target — so once a
	// user clicks they can move the mouse away without losing the inspection.
	const target = $derived(ui.selected ?? ui.hovered);
	const pinned = $derived(!!ui.selected);

	function fmt(v) {
		if (!Number.isFinite(v)) return '—';
		if (Math.abs(v) >= 1000) return v.toLocaleString();
		if (Math.abs(v) >= 1) return v.toFixed(2);
		return v.toFixed(3);
	}

	const nodeValue = $derived.by(() => {
		if (target?.kind !== 'node') return null;
		const v = nodeValueByArea.get(target.id);
		return v == null ? null : v;
	});

	const flowEdge = $derived.by(() => {
		if (target?.kind !== 'flow') return null;
		const forward = flowsByPair.get(`${target.o}|${target.d}`) ?? null;
		const reverse = flowsByPair.get(`${target.d}|${target.o}`) ?? null;
		if (!forward && !reverse) return null;
		return {
			fwdVal: forward?.value ?? 0,
			revVal: reverse?.value ?? 0,
			fwdCount: forward?.count ?? null,
			revCount: reverse?.count ?? null,
			reverseMissing: !reverse,
			total: (forward?.value ?? 0) + (reverse?.value ?? 0),
			net: (forward?.value ?? 0) - (reverse?.value ?? 0)
		};
	});
	// Inline directional pie: a small two-slice circle showing the o->d vs d->o
	// split. Fixed radius (it encodes proportion, not magnitude). The forward
	// (o->d, red) slice is drawn on top of a full reverse (d->o, blue) disc.
	const FLOW_PIE_R = 14;
	const FLOW_PIE_C = FLOW_PIE_R + 1; // center / padding for the 1.5px stroke
	function forwardSlicePath(edge) {
		const frac = edge.total > 0 ? edge.fwdVal / edge.total : 0;
		return pieSlicePath(
			FLOW_PIE_C,
			FLOW_PIE_C,
			FLOW_PIE_R,
			-Math.PI / 2,
			-Math.PI / 2 + frac * Math.PI * 2
		);
	}

	// Reverse direction may have been filtered out by the min-weight slider —
	// show "< {min}" rather than claiming it's zero. See fmtFlowMaybeBelow.
	const fmtReverse = (edge) =>
		fmtFlowMaybeBelow(edge.revVal, edge.reverseMissing, flowMinWeight, fmt);
</script>

<div class="inspect">
	{#if !target}
		<p class="hint">Hover or click a feature on the map.</p>
	{:else if target.kind === 'node'}
		{@const name = geoNames.get(nodeScale, target.id)}
		<div class="row">
			<span class="badge node">{scaleLabel(nodeScale).toLowerCase()}</span>
			<span class="name">{name}</span>
			{#if pinned}<span class="pin" title="Pinned (click empty space or press Esc to clear)"
					>📌</span
				>{/if}
		</div>
		{#if name !== target.id}
			<code class="meta id">{target.id}</code>
		{/if}
		<div class="meta">{nodeLabel || 'live'}</div>
		<div class="value-row">
			<span class="value-label">value</span>
			<span class="value">{fmt(nodeValue)}</span>
		</div>
		{#if nodeBreaks && nodeValues.length}
			<Histogram
				values={nodeValues}
				breaks={nodeBreaks}
				colors={nodeColors}
				highlightValue={nodeValue}
			/>
		{/if}

		{#if pinned && flowEnabled}
			<div class="divider"></div>
			<div class="meta">Flows touching this node</div>
			<Field label="Show">
				<SegmentedControl
					ariaLabel="Flow direction"
					value={ui.flowMode}
					onChange={(v) => (ui.flowMode = v)}
					options={[
						{ value: 'in', label: 'In' },
						{ value: 'out', label: 'Out' },
						{ value: 'unified', label: 'Unified' }
					]}
				/>
			</Field>
		{/if}
	{:else}
		{@const oName = geoNames.get(flowScale, target.o)}
		{@const dName = geoNames.get(flowScale, target.d)}
		<div class="row">
			<span class="badge flow">flow</span>
			<span class="name">{oName} → {dName}</span>
			{#if pinned}<span class="pin">📌</span>{/if}
		</div>
		<div class="value-row">
			<span class="value-label" style:color={OUTFLOW}>{oName} → {dName}</span>
			<span class="value"
				>{fmt(flowEdge?.fwdVal)}{#if flowEdge?.fwdCount != null}<span class="sub">
						· {flowEdge.fwdCount.toLocaleString()} obs</span
					>{/if}</span
			>
		</div>
		<div class="value-row">
			<span class="value-label" style:color={INFLOW}>{dName} → {oName}</span>
			<span class="value"
				>{flowEdge ? fmtReverse(flowEdge) : '—'}{#if flowEdge?.revCount != null}<span class="sub">
						· {flowEdge.revCount.toLocaleString()} obs</span
					>{/if}</span
			>
		</div>
		<div class="value-row total">
			<span class="value-label">total</span>
			<span class="value"
				>{fmt(flowEdge?.total)}{#if flowEdge?.reverseMissing && flowMinWeight > 0}<span class="sub">
						(partial)</span
					>{/if}</span
			>
		</div>
		<div class="value-row">
			<span class="value-label" title="Forward (o→d) minus reverse (d→o)">net ({oName})</span>
			<span class="value">{fmt(flowEdge?.net)}</span>
		</div>
		{#if flowEdge && flowEdge.total > 0}
			{@const fwdPct = Math.round((flowEdge.fwdVal / flowEdge.total) * 100)}
			{@const dim = FLOW_PIE_C * 2}
			<div class="flow-pie-wrap">
				<svg
					width={dim}
					height={dim}
					viewBox="0 0 {dim} {dim}"
					role="img"
					aria-label="Directional split: {fwdPct}% {oName} to {dName}"
				>
					<!-- Reverse (blue) as full disc underneath -->
					<circle cx={FLOW_PIE_C} cy={FLOW_PIE_C} r={FLOW_PIE_R} fill={INFLOW} />
					<!-- Forward (red) slice on top -->
					<path d={forwardSlicePath(flowEdge)} fill={OUTFLOW} />
					<circle
						cx={FLOW_PIE_C}
						cy={FLOW_PIE_C}
						r={FLOW_PIE_R}
						fill="none"
						stroke="#fff"
						stroke-width="1.5"
					/>
				</svg>
				<span class="flow-pie-label">{fwdPct}% / {100 - fwdPct}%</span>
			</div>
		{/if}
		{#if flowBreaks && flowValues.length}
			<Histogram
				values={flowValues}
				breaks={flowBreaks}
				colors={flowColors}
				highlightValue={flowEdge?.fwdVal ?? null}
			/>
		{/if}
	{/if}
</div>

<style>
	.inspect {
		display: flex;
		flex-direction: column;
		gap: var(--spacing-2);
	}
	.row {
		display: flex;
		align-items: center;
		gap: var(--spacing-2);
	}
	.badge {
		font-size: var(--text-xs);
		padding: 1px var(--spacing-2);
		border-radius: var(--radius-pill);
		color: var(--color-accent-fg);
		background: var(--color-accent);
	}
	.badge.flow {
		background: #fff;
		color: var(--color-accent);
		border: 1px solid var(--color-accent);
	}
	.name {
		font-size: var(--text-sm);
		font-weight: 600;
		color: var(--color-text);
		overflow: hidden;
		text-overflow: ellipsis;
		white-space: nowrap;
	}
	.id {
		font-family: ui-monospace, monospace;
		font-size: var(--text-xs);
		color: var(--color-hint);
	}
	.pin {
		margin-left: auto;
		font-size: var(--text-sm);
	}
	.meta {
		font-size: var(--text-xs);
		color: var(--color-muted);
	}
	.value-row {
		display: flex;
		justify-content: space-between;
		align-items: baseline;
		font-variant-numeric: tabular-nums;
	}
	.value-label {
		font-size: var(--text-xs);
		color: var(--color-muted);
	}
	.value {
		font-size: var(--text-base);
		font-weight: 600;
		color: var(--color-text);
	}
	.sub {
		font-size: var(--text-xs);
		color: var(--color-muted);
		font-weight: 400;
	}
	.value-row.total {
		border-top: 1px solid var(--color-line);
		padding-top: 2px;
	}
	.flow-pie-wrap {
		display: flex;
		align-items: center;
		gap: var(--spacing-2);
		margin-top: var(--spacing-1);
	}
	.flow-pie-label {
		font-size: var(--text-xs);
		color: var(--color-muted);
		font-variant-numeric: tabular-nums;
	}
	.divider {
		border-top: 1px solid var(--color-line);
		margin-top: var(--spacing-1);
	}
	.hint {
		color: var(--color-hint);
		font-size: var(--text-sm);
		margin: 0;
	}
</style>
