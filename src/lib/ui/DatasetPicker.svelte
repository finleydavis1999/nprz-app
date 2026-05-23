<script>
	import { selection } from '$lib/state/selection.svelte.js';
	import Field from './Field.svelte';

	let { manifest, state = selection, section = 'datasets', label = 'Dataset' } = $props();

	// Only datasets with data at the current scale are selectable — existing
	// datasets are pc4+gem only, cbs-vk100 adds buurt.
	const options = $derived(
		Object.entries(manifest?.[section] ?? {})
			.filter(([, ds]) => ds.scales?.[state.scale])
			.map(([id, ds]) => ({ id, label: ds.name ?? id }))
	);

	// Switch dataset: clear filters (fields differ between datasets) and clamp a
	// single-year selection to the new dataset's value list so the query stays
	// valid. Range-typed years are clamped by YearPicker's $effect.
	function selectDataset(id) {
		state.dataset = id;
		state.filters = {};
		const yearField = manifest?.[section]?.[id]?.fields?.year;
		if (yearField?.type === 'single') {
			const years = yearField.values?.map((v) => v.id) ?? [];
			if (years.length && !years.includes(state.year)) {
				state.year = yearField.default ?? years[years.length - 1];
			}
		}
	}

	// Changing scale can leave `state.dataset` pointing at a dataset with no data
	// at the new scale — fall back to the first scale-valid one. No-op when no
	// dataset is available (e.g. flows at buurt scale).
	$effect(() => {
		if (!manifest?.[section]) return;
		if (manifest[section][state.dataset]?.scales?.[state.scale]) return;
		const first = options[0]?.id;
		if (first && first !== state.dataset) selectDataset(first);
	});
</script>

<Field {label}>
	<select value={state.dataset} onchange={(e) => selectDataset(e.currentTarget.value)}>
		{#each options as o (o.id)}
			<option value={o.id}>{o.label}</option>
		{/each}
	</select>
</Field>
{#if manifest?.[section]?.[state.dataset]?.warning}
	<p class="warning">{manifest[section][state.dataset].warning}</p>
{/if}

<style>
	.warning {
		margin: var(--spacing-1) 0 0 0;
		font-size: var(--text-xs);
		color: var(--color-muted);
		line-height: 1.4;
	}
</style>
