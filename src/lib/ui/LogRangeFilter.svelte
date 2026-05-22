<script>
	// Labelled filter row: a log-scaled range slider paired with a typed number
	// input for an exact cutoff. The log mapping only affects the slider track
	// position — the number shown / typed is always the real linear value.
	import Field from './Field.svelte';

	let {
		label,
		value = $bindable(0),
		floor = 0,
		max = 1,
		disabled = false,
		integer = false
	} = $props();

	// Track granularity: number of discrete thumb stops. Not a data value —
	// the data range enters through the log mapping below.
	const RES = 1000;

	// The log scale needs a strictly positive lower bound. Prefer the data min
	// (`floor`); fall back to a small fraction of `max` when floor is unusable.
	const logFloor = $derived(floor > 0 && floor < max ? floor : Math.max(max / 1e4, 1e-6));
	const logSpan = $derived(Math.log(max) - Math.log(logFloor));

	function valueToPos(v) {
		if (!(logSpan > 0) || v <= logFloor) return 0;
		return ((Math.log(Math.min(v, max)) - Math.log(logFloor)) / logSpan) * RES;
	}

	function posToValue(pos) {
		if (!(logSpan > 0)) return 0;
		const frac = pos / RES;
		if (frac <= 0) return 0;
		const raw = Math.exp(Math.log(logFloor) + frac * logSpan);
		return integer ? Math.round(raw) : raw;
	}

	function fmtValue(v) {
		return integer ? String(Math.round(v)) : String(Math.round(v * 100) / 100);
	}

	const sliderPos = $derived(valueToPos(value));

	// Writable derived: normally mirrors the real `value`, but a keystroke can
	// override it; the override is discarded when `value` changes (slider drag,
	// re-anchor) so the box re-syncs. Committed back to `value` on `change`.
	let text = $derived(fmtValue(value));

	function onSlider(e) {
		value = posToValue(Number(e.currentTarget.value));
	}

	function commitNumber() {
		const n = Number(text);
		const clamped = Number.isFinite(n) ? Math.min(Math.max(n, 0), max) : value;
		value = integer ? Math.round(clamped) : clamped;
		text = fmtValue(value);
	}
</script>

<Field {label}>
	<div class="logfilter">
		<input
			type="range"
			min="0"
			max={RES}
			step="1"
			value={sliderPos}
			oninput={onSlider}
			{disabled}
		/>
		<input
			type="number"
			min="0"
			{max}
			step={integer ? 1 : 'any'}
			value={text}
			oninput={(e) => (text = e.currentTarget.value)}
			onchange={commitNumber}
			{disabled}
		/>
	</div>
</Field>

<style>
	.logfilter {
		display: grid;
		grid-template-columns: 1fr 6em;
		align-items: center;
		gap: var(--spacing-2);
	}
</style>
