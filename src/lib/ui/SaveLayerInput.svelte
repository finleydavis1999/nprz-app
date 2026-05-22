<script>
	import Field from './Field.svelte';
	import { selection } from '$lib/state/selection.svelte.js';
	import { layers } from '$lib/state/layers.svelte.js';
	import { slugify, defaultLayerName } from '$lib/data/layer-calc.js';

	let { manifest } = $props();

	// User-editable name. When empty (or untouched), we suggest a default
	// derived from the current selection — kept in sync until the user types.
	let name = $state('');
	let touched = $state(false);

	const suggestion = $derived(defaultLayerName(manifest, selection));
	// The untouched default is uniquified up front, so the gray placeholder
	// always shows the exact name the layer will be saved as.
	const effective = $derived(touched ? name : layers.uniqueName(suggestion));

	const slug = $derived(slugify(effective));
	// Only block + warn on collisions the user typed deliberately.
	const taken = $derived(touched && !!slug && layers.slugTaken(slug));
	const disabled = $derived(!slug || taken);

	function onSubmit(e) {
		e.preventDefault();
		if (disabled) return;
		layers.saveCurrent(effective);
		name = '';
		touched = false;
	}
</script>

<form class="save-row" onsubmit={onSubmit}>
	<Field label="Save as">
		<input
			type="text"
			placeholder={effective}
			value={touched ? name : ''}
			oninput={(e) => {
				const v = /** @type {HTMLInputElement} */ (e.currentTarget).value;
				name = v;
				touched = v.length > 0;
			}}
			autocomplete="off"
		/>
	</Field>
	<div class="row">
		{#if taken}
			<span class="err-msg">Name in use</span>
		{:else if effective}
			<span class="hint" title="Slug used in expressions">→ {slug}</span>
		{/if}
		<button type="submit" class="primary" {disabled} title={effective}>Save layer</button>
	</div>
</form>

<style>
	.save-row {
		display: flex;
		flex-direction: column;
		gap: var(--spacing-1);
	}
	.row {
		display: flex;
		justify-content: space-between;
		align-items: center;
		gap: var(--spacing-2);
	}
	.primary {
		padding: 2px var(--spacing-2);
		background: var(--color-accent);
		color: var(--color-accent-fg);
		border: none;
		border-radius: var(--radius);
		font-size: var(--text-sm);
		cursor: pointer;
	}
	.primary:disabled {
		background: var(--color-line);
		cursor: default;
	}
	.hint {
		font-size: var(--text-xs);
		color: var(--color-hint);
		font-family: ui-monospace, monospace;
		overflow: hidden;
		text-overflow: ellipsis;
		white-space: nowrap;
		min-width: 0;
	}
	.err-msg {
		font-size: var(--text-xs);
		color: #cf222e;
	}
</style>
