<script>
	// Floating legend panel — fixed bottom-right of the map area, just left
	// of the right sidebar. Renders node + flow legends side-by-side when
	// both are present; collapses cleanly when one or neither has data.
	//
	// Previously the legends lived inside the Node / Flow cartography
	// panels. Pulling them out into a dedicated overlay (a) frees the
	// cartography panels for controls, (b) keeps the legend visible
	// without expanding sub-panels, (c) matches the "legend in the
	// corner of the map" cartographic convention.
	//
	// The container is `display: flex` so both legends share one rounded
	// rectangle with a divider — feels like one piece of chrome, not two.
	// Either side renders only when its `breaks` + `colors` props are
	// populated, so a flow-only scene (no node layer) doesn't waste space
	// on an empty node legend slot.

	import Legend from '$lib/cartography/Legend.svelte';

	/** @typedef {{ breaks: number[] | null, colors: string[], title?: string }} LegendSlot */
	/** @type {{ node?: LegendSlot, flow?: LegendSlot }} */
	let { node = null, flow = null } = $props();

	const hasNode = $derived(node && node.breaks && node.colors?.length);
	const hasFlow = $derived(flow && flow.breaks && flow.colors?.length);
	const visible = $derived(hasNode || hasFlow);
</script>

{#if visible}
	<aside class="map-legend" aria-label="Map legend">
		{#if hasNode}
			<div class="slot">
				<Legend breaks={node.breaks} colors={node.colors} title={node.title ?? 'Areas'} />
			</div>
		{/if}
		{#if hasNode && hasFlow}
			<div class="sep" aria-hidden="true"></div>
		{/if}
		{#if hasFlow}
			<div class="slot">
				<Legend breaks={flow.breaks} colors={flow.colors} title={flow.title ?? 'Flows'} />
			</div>
		{/if}
	</aside>
{/if}

<style>
	/* Pinned bottom-right of the map area. The right sidebar is 320px wide
	   and lives at `right: var(--spacing-4)` — we sit just to its left so
	   the legend doesn't overlap or scroll with it. */
	.map-legend {
		position: fixed;
		bottom: var(--spacing-4);
		right: calc(320px + var(--spacing-4) * 2);
		z-index: 2;
		display: flex;
		gap: var(--spacing-3);
		padding: var(--spacing-2) var(--spacing-3);
		background: var(--color-bg-panel);
		border: 1px solid var(--color-line);
		border-radius: var(--radius);
		box-shadow: 0 2px 8px rgba(0, 0, 0, 0.06);
		max-width: calc(100vw - 320px - var(--spacing-4) * 4);
		max-height: 50vh;
		overflow: auto;
	}
	.slot {
		min-width: 0;
	}
	.sep {
		flex: 0 0 1px;
		background: var(--color-line);
	}
</style>
