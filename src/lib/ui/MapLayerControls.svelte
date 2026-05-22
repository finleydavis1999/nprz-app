<script>
	import { mapLayers } from '$lib/state/map-layers.svelte.js';
	import Toggle from './Toggle.svelte';
	import Field from './Field.svelte';

	const boundaryScales = [
		{ id: 'gem', label: 'Gemeente boundaries' },
		{ id: 'pc4', label: 'PC4 boundaries' }
	];
</script>

<div class="layers">
	<div class="layer">
		<Toggle bind:checked={mapLayers.labels} label="Basemap labels" />
	</div>

	<div class="layer">
		<Toggle bind:checked={mapLayers.boundary} label="Boundary overlay" />
		{#if mapLayers.boundary}
			<div class="opts">
				<Field label="Scale">
					<select bind:value={mapLayers.boundaryScale}>
						{#each boundaryScales as s (s.id)}
							<option value={s.id}>{s.label}</option>
						{/each}
					</select>
				</Field>
				<Field label="Color">
					<input type="color" bind:value={mapLayers.boundaryColor} />
				</Field>
				<Field label="Width" value="{mapLayers.boundaryWidth.toFixed(1)}px">
					<input type="range" min="0.2" max="4" step="0.1" bind:value={mapLayers.boundaryWidth} />
				</Field>
				<Field label="Opacity" value={mapLayers.boundaryOpacity.toFixed(2)}>
					<input type="range" min="0" max="1" step="0.05" bind:value={mapLayers.boundaryOpacity} />
				</Field>
			</div>
		{/if}
	</div>

	<div class="layer">
		<Toggle bind:checked={mapLayers.provinces} label="Province boundaries" />
		{#if mapLayers.provinces}
			<div class="opts">
				<Field label="Color">
					<input type="color" bind:value={mapLayers.provinceColor} />
				</Field>
				<Field label="Width" value="{mapLayers.provinceWidth.toFixed(1)}px">
					<input type="range" min="0.2" max="4" step="0.1" bind:value={mapLayers.provinceWidth} />
				</Field>
			</div>
		{/if}
	</div>

	<div class="layer">
		<Toggle bind:checked={mapLayers.builtup} label="Built-up areas" />
		{#if mapLayers.builtup}
			<div class="opts">
				<Field label="Color">
					<input type="color" bind:value={mapLayers.builtupColor} />
				</Field>
				<Field label="Opacity" value={mapLayers.builtupOpacity.toFixed(2)}>
					<input type="range" min="0" max="1" step="0.05" bind:value={mapLayers.builtupOpacity} />
				</Field>
			</div>
		{/if}
	</div>
</div>

<style>
	.layers {
		display: flex;
		flex-direction: column;
		gap: var(--spacing-3);
	}
	.layer {
		display: flex;
		flex-direction: column;
		gap: var(--spacing-2);
	}
	/* Sub-controls nested under their layer so each reads as one unit. */
	.opts {
		display: flex;
		flex-direction: column;
		gap: var(--spacing-2);
		margin-left: var(--spacing-1);
		padding-left: var(--spacing-3);
		border-left: 1px solid var(--color-line);
	}
</style>
