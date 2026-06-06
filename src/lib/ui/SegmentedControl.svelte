<script>
	// A small radiogroup-style segmented toggle. Replaces the `.seg` markup +
	// CSS that was being copy-pasted into each panel.
	// options: Array<{ value, label, title? }>
	let { options, value, onChange, ariaLabel } = $props();
</script>

<div class="seg" role="radiogroup" aria-label={ariaLabel}>
	{#each options as opt (opt.value)}
		<button
			type="button"
			class:active={value === opt.value}
			aria-pressed={value === opt.value}
			title={opt.title}
			onclick={() => onChange(opt.value)}
		>
			{opt.label}
		</button>
	{/each}
</div>

<style>
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
