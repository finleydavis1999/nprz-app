<script>
	// Renders every `type: "toggle"` field of the active flow dataset as an
	// on/off switch, manifest-driven exactly like CategoryFilters does for
	// `multi` fields. The boolean lives in `state.toggles[id]` (a record), so we
	// assign a fresh object on change to keep reactivity (per house rules).
	import { flow } from '$lib/state/flow.svelte.js';
	import Toggle from '$lib/ui/Toggle.svelte';

	let { manifest, state = flow, section = 'flows' } = $props();

	const fields = $derived.by(() => {
		const ds = manifest?.[section]?.[state.dataset];
		if (!ds) return [];
		return Object.entries(ds.fields)
			.filter(([, f]) => f.type === 'toggle')
			.map(([id, f]) => ({ id, label: f.label ?? id }));
	});

	function set(id, value) {
		state.toggles = { ...state.toggles, [id]: value };
	}
</script>

{#if fields.length > 0}
	<div class="toggles">
		{#each fields as f (f.id)}
			<Toggle label={f.label} checked={!!state.toggles[f.id]} onchange={(v) => set(f.id, v)} />
		{/each}
	</div>
{/if}

<style>
	.toggles {
		display: flex;
		flex-direction: column;
		gap: var(--spacing-2);
	}
</style>
