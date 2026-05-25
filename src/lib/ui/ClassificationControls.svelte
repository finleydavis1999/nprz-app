<script>
	import { cartography } from '$lib/state/cartography.svelte.js';
	import { paletteNames } from '$lib/cartography/palettes.js';
	import Field from './Field.svelte';

	// `target` defaults to the node-cartography singleton; pass a flow-cartography
	// state to drive the flow layer instead. Width/curvature sliders are shown
	// only when those keys exist on the target (flow side); fill-opacity/line
	// controls only when those keys exist (node side).
	//
	// `useDiverging` is computed by the caller from the active layer's data
	// (`hasBothSigns && !target.forceSequential`). When true, the palette
	// picker shows DIVERGING palette names and writes to `target.divergingPalette`;
	// when false, sequential names and `target.palette`. The user sees ONE
	// palette dropdown that swaps option set + binding automatically.
	let { target = cartography, useDiverging = false } = $props();

	const methods = [
		{ id: 'jenks', label: 'Jenks (natural breaks)' },
		{ id: 'quantile', label: 'Quantile' },
		{ id: 'equal', label: 'Equal interval' }
	];
	const seqPalettes = paletteNames('sequential');
	const divPalettes = paletteNames('diverging');
	const palettes = $derived(useDiverging ? divPalettes : seqPalettes);

	const hasFill = $derived('fillOpacity' in target);
	const hasLine = $derived('lineColor' in target);
	const hasWidth = $derived('widthMin' in target && 'widthMax' in target);
	const hasOpacity = $derived('opacity' in target);
	const hasCurvature = $derived('curvature' in target);
	const hasDiverging = $derived('divergingPalette' in target && 'forceSequential' in target);

	// One-knob palette: read + write the field that matches the current
	// mode. The "other" field stays as it was so a toggle of
	// `forceSequential` restores the last choice for that mode.
	function getPalette() {
		return useDiverging ? target.divergingPalette : target.palette;
	}
	function setPalette(name) {
		if (useDiverging) target.divergingPalette = name;
		else target.palette = name;
	}
</script>

<div class="stack">
	<Field label="Method">
		<select bind:value={target.method}>
			{#each methods as m (m.id)}
				<option value={m.id}>{m.label}</option>
			{/each}
		</select>
	</Field>

	<Field label="Classes">
		<input type="number" min="3" max="9" bind:value={target.n} />
	</Field>

	<Field
		label="Palette"
		info={useDiverging
			? 'Diverging palette — used because the active layer has both positive and negative values. Anchored at 0: the palette centre maps to zero, with classes on each side fit by the chosen Method. Toggle "Force sequential" below to override.'
			: 'Sequential palette. When the active layer has both positive and negative values, this picker switches to diverging palettes (anchored at 0) automatically — toggle "Force sequential" to override.'}
	>
		<select value={getPalette()} onchange={(e) => setPalette(e.currentTarget.value)}>
			{#each palettes as name (name)}
				<option value={name}>{name}</option>
			{/each}
		</select>
	</Field>

	{#if hasDiverging}
		<Field
			label="Force sequential"
			info="Override the auto-diverging detection — render signed data with the sequential palette + classification instead. Useful when you want to read signed values as 'all bad' rather than 'good vs bad'."
		>
			<input type="checkbox" bind:checked={target.forceSequential} />
		</Field>
	{/if}

	{#if hasFill}
		<Field label="Fill opacity" value={target.fillOpacity.toFixed(2)}>
			<input type="range" min="0" max="1" step="0.05" bind:value={target.fillOpacity} />
		</Field>
	{/if}

	{#if hasLine}
		<Field label="Line width" value="{target.lineWidth.toFixed(1)}px">
			<input type="range" min="0" max="2" step="0.1" bind:value={target.lineWidth} />
		</Field>

		<Field label="Line color">
			<input type="color" bind:value={target.lineColor} />
		</Field>
	{/if}

	{#if hasWidth}
		<Field label="Width min" value="{target.widthMin.toFixed(1)}px">
			<input type="range" min="0" max="6" step="0.1" bind:value={target.widthMin} />
		</Field>

		<Field label="Width max" value="{target.widthMax.toFixed(1)}px">
			<input type="range" min="1" max="20" step="0.5" bind:value={target.widthMax} />
		</Field>
	{/if}

	{#if hasOpacity}
		<Field label="Opacity" value={target.opacity.toFixed(2)}>
			<input type="range" min="0" max="1" step="0.05" bind:value={target.opacity} />
		</Field>
	{/if}

	{#if hasCurvature}
		<Field label="Curvature" value={target.curvature.toFixed(2)}>
			<input type="range" min="0" max="0.6" step="0.02" bind:value={target.curvature} />
		</Field>
	{/if}
</div>

<style>
	.stack {
		display: flex;
		flex-direction: column;
		gap: var(--spacing-2);
	}
</style>
