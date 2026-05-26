<script>
	// Pick decimals so a 0–1 percentage legend doesn't collapse to "0 – 0"
	// while keeping integer-valued data integer (OViN flows, populations).
	function pickDecimals(breaks) {
		if (!breaks || breaks.length < 2) return 0;
		if (breaks.every((b) => Number.isInteger(b))) return 0;
		const range = Math.abs(breaks[breaks.length - 1] - breaks[0]);
		if (!Number.isFinite(range) || range === 0) return 0;
		return Math.max(0, Math.min(6, 2 - Math.floor(Math.log10(range))));
	}

	let { breaks, colors, format, title = '' } = $props();

	const decimals = $derived(pickDecimals(breaks));
	const fmt = $derived(
		format ??
			((n) =>
				n.toLocaleString(undefined, {
					minimumFractionDigits: decimals,
					maximumFractionDigits: decimals
				}))
	);
</script>

<div class="legend">
	{#if title}<div class="title">{title}</div>{/if}
	{#each colors as color, i (i)}
		<div class="row">
			<span class="swatch" style="background: {color}"></span>
			<span class="label">{fmt(breaks[i])} – {fmt(breaks[i + 1])}</span>
		</div>
	{/each}
</div>

<style>
	.legend {
		font-size: var(--text-xs);
		color: var(--color-text);
	}
	.title {
		font-weight: 600;
		margin-bottom: var(--spacing-1);
	}
	.row {
		display: flex;
		align-items: center;
		gap: var(--spacing-2);
		line-height: 1.5;
	}
	.swatch {
		width: 18px;
		height: 12px;
		display: inline-block;
		border: 1px solid var(--color-line);
		flex-shrink: 0;
	}
	.label {
		font-variant-numeric: tabular-nums;
		color: var(--color-muted);
	}
</style>
