<script>
	import { selection } from '$lib/state/selection.svelte.js';
	import { SCALES } from '$lib/scales.js';
</script>

<div class="seg">
	{#each SCALES as s (s.id)}
		<label class:active={selection.scale === s.id}>
			<input type="radio" name="scale" value={s.id} bind:group={selection.scale} />
			{s.label}
		</label>
	{/each}
</div>

<style>
	.seg {
		display: inline-flex;
		border: 1px solid var(--color-line);
		border-radius: var(--radius);
		overflow: hidden;
		font-size: var(--text-sm);
	}
	label {
		padding: var(--spacing-1) var(--spacing-3);
		cursor: pointer;
		background: #fff;
		color: var(--color-muted);
	}
	label + label {
		border-left: 1px solid var(--color-line);
	}
	label.active {
		background: var(--color-accent);
		color: var(--color-accent-fg);
	}
	input {
		display: none;
	}
</style>
