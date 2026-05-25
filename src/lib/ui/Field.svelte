<script>
	// Generic labelled-input row used by all single-control panels.
	// Renders: <label>span(label) ⓘ | children | optional trailing value</label>
	//
	// `info` prop renders a small (i) glyph next to the label whose
	// native title attribute carries the doc string. Plain CSS tooltip
	// (no JS) — fast, accessible, no portal/positioning bugs.
	let { label, value = null, info = null, children } = $props();
</script>

<label class="field">
	<span class="label">
		{label}
		{#if info}<span class="info" title={info} aria-label={info}>ⓘ</span>{/if}
	</span>
	<span class="control">{@render children?.()}</span>
	{#if value !== null}
		<span class="value">{value}</span>
	{/if}
</label>

<style>
	.field {
		display: grid;
		grid-template-columns: var(--label-col) 1fr auto;
		align-items: center;
		gap: var(--spacing-2);
		font-size: var(--text-sm);
	}
	.label {
		color: var(--color-muted);
		display: inline-flex;
		align-items: baseline;
		gap: 4px;
	}
	/* (i) glyph next to the label — same muted tone, lighter weight,
	   cursor flips to help-cursor so users discover the tooltip. */
	.info {
		color: var(--color-hint);
		font-size: var(--text-xs);
		cursor: help;
	}
	.info:hover {
		color: var(--color-muted);
	}
	.value {
		font-variant-numeric: tabular-nums;
		color: var(--color-muted);
		min-width: 3em;
		text-align: right;
	}
	.control :global(select),
	.control :global(input[type='number']),
	.control :global(input[type='range']),
	.control :global(input[type='color']),
	.control :global(input[type='text']) {
		width: 100%;
		min-width: 0;
	}
</style>
