<script>
	import { selection } from '$lib/state/selection.svelte.js';
	import Field from './Field.svelte';

	let { manifest, state = selection, section = 'datasets' } = $props();

	const dataset = $derived(manifest?.[section]?.[state.dataset]);
	const variableField = $derived(dataset?.fields?.variable);
	const options = $derived(variableField?.values ?? []);

	// The currently active variable id — read from filters, fall back to default
	const activeVarId = $derived(
		state.filters?.variable?.[0] ?? variableField?.default ?? options[0]?.id
	);

	// When switching to a dataset that has a variable field, set the default
	// immediately. Use $effect on the dataset key so it fires on dataset change.
	$effect(() => {
		const ds = state.dataset; // track dataset changes
		void ds;
		if (!variableField || options.length === 0) return;
		const cur = state.filters?.variable?.[0];
		const validIds = options.map((o) => String(o.id));
		if (!cur || !validIds.includes(String(cur))) {
			// Use $state.snapshot to avoid mutating during derivation
			const defaultId = variableField.default ?? options[0]?.id;
			state.filters = { ...state.filters, variable: [defaultId] };
		}
	});

	function onSelect(e) {
		state.filters = { ...state.filters, variable: [e.currentTarget.value] };
	}
</script>

{#if variableField && options.length > 0}
	<Field label={variableField.label ?? 'Variabele'}>
		<select value={String(activeVarId)} onchange={onSelect}>
			{#each options as o (o.id)}
				<option value={String(o.id)}>{o.label}</option>
			{/each}
		</select>
	</Field>
{/if}
