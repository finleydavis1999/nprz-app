<script>
	import { selection } from '$lib/state/selection.svelte.js';
	import Field from './Field.svelte';

	let { manifest, state = selection, section = 'datasets' } = $props();

	// Datasets with a `variable` field (e.g. cbs-vk100) pick one column to map
	// via a single-select. Datasets without one render nothing.
	const field = $derived(manifest?.[section]?.[state.dataset]?.fields?.variable);
	const options = $derived(field?.values ?? []);

	// Bucket options into <optgroup>s, preserving first-seen order.
	const groups = $derived.by(() => {
		/** @type {{label:string, opts:{id:number,label:string,group?:string}[]}[]} */
		const out = [];
		for (const o of options) {
			const g = o.group ?? '';
			let grp = out.find((x) => x.label === g);
			if (!grp) {
				grp = { label: g, opts: [] };
				out.push(grp);
			}
			grp.opts.push(o);
		}
		return out;
	});

	const activeId = $derived(state.filters?.variable?.[0] ?? field?.default ?? options[0]?.id);

	// Keep `filters.variable` a valid one-element array while the dataset has a
	// variable field — an empty value would make the query sum every variable.
	$effect(() => {
		if (!field || options.length === 0) return;
		const cur = state.filters?.variable?.[0];
		if (!options.some((o) => o.id === cur)) {
			state.filters = { ...state.filters, variable: [field.default ?? options[0].id] };
		}
	});
</script>

{#if field && options.length > 0}
	<Field label={field.label ?? 'Variabele'}>
		<select
			value={String(activeId)}
			onchange={(e) =>
				(state.filters = { ...state.filters, variable: [Number(e.currentTarget.value)] })}
		>
			{#each groups as grp (grp.label)}
				{#if grp.label}
					<optgroup label={grp.label}>
						{#each grp.opts as o (o.id)}
							<option value={String(o.id)}>{o.label}</option>
						{/each}
					</optgroup>
				{:else}
					{#each grp.opts as o (o.id)}
						<option value={String(o.id)}>{o.label}</option>
					{/each}
				{/if}
			{/each}
		</select>
	</Field>
{/if}
