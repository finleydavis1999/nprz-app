<script>
	import Histogram from '$lib/cartography/Histogram.svelte';
	import Field from './Field.svelte';
	import { ui } from '$lib/state/ui.svelte.js';
	import { geoNames } from '$lib/state/geo-names.svelte.js';

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
		flowColors = []
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
		return flowsByPair.get(`${target.o}|${target.d}`) ?? null;
	});
</script>

<div class="inspect">
	{#if !target}
		<p class="hint">Hover or click a feature on the map.</p>
	{:else if target.kind === 'node'}
		{@const name = geoNames.get(nodeScale, target.id)}
		<div class="row">
			<span class="badge node">{nodeScale === 'gem' ? 'gemeente' : nodeScale}</span>
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
				<div class="seg" role="radiogroup" aria-label="Flow direction">
					<button
						type="button"
						class:active={ui.flowMode === 'in'}
						aria-pressed={ui.flowMode === 'in'}
						onclick={() => (ui.flowMode = 'in')}
					>
						In
					</button>
					<button
						type="button"
						class:active={ui.flowMode === 'out'}
						aria-pressed={ui.flowMode === 'out'}
						onclick={() => (ui.flowMode = 'out')}
					>
						Out
					</button>
					<button
						type="button"
						class:active={ui.flowMode === 'unified'}
						aria-pressed={ui.flowMode === 'unified'}
						onclick={() => (ui.flowMode = 'unified')}
					>
						Unified
					</button>
				</div>
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
			<span class="value-label">value</span>
			<span class="value">{fmt(flowEdge?.value)}</span>
		</div>
		{#if flowEdge?.count != null}
			<div class="value-row">
				<span class="value-label">count</span>
				<span class="value">{flowEdge.count.toLocaleString()}</span>
			</div>
		{/if}
		{#if flowBreaks && flowValues.length}
			<Histogram
				values={flowValues}
				breaks={flowBreaks}
				colors={flowColors}
				highlightValue={flowEdge?.value ?? null}
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
	.divider {
		border-top: 1px solid var(--color-line);
		margin-top: var(--spacing-1);
	}
	.hint {
		color: var(--color-hint);
		font-size: var(--text-sm);
		margin: 0;
	}
	.seg {
		display: inline-flex;
		border: 1px solid var(--color-line);
		border-radius: var(--radius);
		overflow: hidden;
	}
	.seg button {
		background: transparent;
		border: none;
		padding: 2px var(--spacing-2);
		font-size: var(--text-xs);
		color: var(--color-muted);
		cursor: pointer;
	}
	.seg button + button {
		border-left: 1px solid var(--color-line);
	}
	.seg button.active {
		background: var(--color-accent);
		color: var(--color-accent-fg);
	}
</style>
