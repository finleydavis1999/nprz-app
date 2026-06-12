<script>
	// Presentational two-thumb range slider rendered inside a labelled Field row.
	// Stateless: the parent owns `lo`/`hi` and reacts to `onLo`/`onHi` callbacks
	// (which receive the raw input string). Used by YearPicker (period range) and
	// AgePicker (age range). Push semantics live in the parent's setters.
	import Field from './Field.svelte';

	let { min, max, lo, hi, label, valueText, onLo, onHi } = $props();

	const span = $derived(Math.max(max - min, 1));
	const fillLeftPct = $derived(((lo - min) / span) * 100);
	const fillWidthPct = $derived(((hi - lo) / span) * 100);
</script>

<Field {label} value={valueText}>
	<div class="rangeslider">
		<div class="track"></div>
		<div class="fill" style:left="{fillLeftPct}%" style:width="{fillWidthPct}%"></div>
		<input
			type="range"
			class="thumb"
			{min}
			{max}
			step="1"
			value={lo}
			oninput={(e) => onLo(e.currentTarget.value)}
		/>
		<input
			type="range"
			class="thumb"
			{min}
			{max}
			step="1"
			value={hi}
			oninput={(e) => onHi(e.currentTarget.value)}
		/>
	</div>
</Field>

<style>
	.rangeslider {
		position: relative;
		height: 22px;
	}
	.track {
		position: absolute;
		left: 0;
		right: 0;
		top: 50%;
		height: 4px;
		transform: translateY(-50%);
		background: var(--color-line);
		border-radius: 2px;
	}
	.fill {
		position: absolute;
		top: 50%;
		height: 4px;
		transform: translateY(-50%);
		background: var(--color-accent, #4682b4);
		border-radius: 2px;
		pointer-events: none;
	}
	/* Two stacked range inputs, transparent track, only thumbs receive pointer events. */
	.thumb {
		position: absolute;
		inset: 0;
		width: 100%;
		height: 100%;
		margin: 0;
		appearance: none;
		background: transparent;
		pointer-events: none;
	}
	.thumb:focus {
		outline: none;
	}
	.thumb::-webkit-slider-runnable-track {
		background: transparent;
		border: none;
	}
	.thumb::-moz-range-track {
		background: transparent;
		border: none;
	}
	.thumb::-webkit-slider-thumb {
		-webkit-appearance: none;
		appearance: none;
		pointer-events: auto;
		width: 14px;
		height: 14px;
		border-radius: 50%;
		background: var(--color-accent, #4682b4);
		border: 2px solid #fff;
		box-shadow: 0 0 0 1px var(--color-line);
		cursor: pointer;
		margin-top: -5px;
	}
	.thumb::-moz-range-thumb {
		pointer-events: auto;
		width: 14px;
		height: 14px;
		border-radius: 50%;
		background: var(--color-accent, #4682b4);
		border: 2px solid #fff;
		box-shadow: 0 0 0 1px var(--color-line);
		cursor: pointer;
	}
</style>
