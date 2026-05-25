<script>
	import { selection } from '$lib/state/selection.svelte.js';
	import Field from './Field.svelte';

	let { manifest, state = selection, section = 'datasets' } = $props();

	const yearField = $derived(manifest?.[section]?.[state.dataset]?.fields?.year);
	const isRange = $derived(yearField?.type === 'range');
	// A 'multi'-type year is a categorical period filter (woon-werk has 3
	// discrete periods); CategoryFilters already renders it as a multi-select.
	// Don't double-render it here as a single-year dropdown — that's where
	// the duplicate "Year" + "Period" UI was coming from. Only render this
	// picker for 'range' (composite slider) and 'single' (single-year
	// dropdown). When the field is absent entirely we also render nothing.
	const supported = $derived(yearField?.type === 'range' || yearField?.type === 'single');

	// --- single-year mode (dropdown) ---
	const yearOptions = $derived(yearField?.values ?? []);

	// --- range mode (one composite slider with two handles) ---
	const rangeMin = $derived(yearField?.min ?? 0);
	const rangeMax = $derived(yearField?.max ?? 0);
	const span = $derived(Math.max(rangeMax - rangeMin, 1));
	const lo = $derived(state.yearMin ?? rangeMin);
	const hi = $derived(state.yearMax ?? rangeMax);
	const fillLeftPct = $derived(((lo - rangeMin) / span) * 100);
	const fillWidthPct = $derived(((hi - lo) / span) * 100);

	// "Push" semantics so the user can never get stuck when both thumbs overlap:
	// dragging one thumb past the other carries the other along.
	function setLo(v) {
		const n = Number(v);
		state.yearMin = n;
		if ((state.yearMax ?? rangeMax) < n) state.yearMax = n;
	}
	function setHi(v) {
		const n = Number(v);
		state.yearMax = n;
		if ((state.yearMin ?? rangeMin) > n) state.yearMin = n;
	}

	// Clamp on dataset change so the saved range stays within the new dataset's
	// bounds. Only relevant when the new dataset is also range-typed.
	$effect(() => {
		if (!isRange) return;
		if (state.yearMin == null) state.yearMin = yearField?.defaultMin ?? rangeMin;
		if (state.yearMax == null) state.yearMax = yearField?.defaultMax ?? rangeMax;
		if (state.yearMin < rangeMin) state.yearMin = rangeMin;
		if (state.yearMax > rangeMax) state.yearMax = rangeMax;
		if (state.yearMin > state.yearMax) state.yearMin = state.yearMax;
	});
</script>

{#if !supported}
	<!-- intentionally empty; CategoryFilters handles 'multi'-type year fields -->
{:else if isRange}
	<Field label={yearField.label ?? 'Periode'} value="{lo}–{hi}">
		<div class="rangeslider">
			<div class="track"></div>
			<div class="fill" style:left="{fillLeftPct}%" style:width="{fillWidthPct}%"></div>
			<input
				type="range"
				class="thumb"
				min={rangeMin}
				max={rangeMax}
				step="1"
				value={lo}
				oninput={(e) => setLo(e.currentTarget.value)}
			/>
			<input
				type="range"
				class="thumb"
				min={rangeMin}
				max={rangeMax}
				step="1"
				value={hi}
				oninput={(e) => setHi(e.currentTarget.value)}
			/>
		</div>
	</Field>
{:else}
	<Field label="Year">
		<select bind:value={state.year}>
			{#each yearOptions as y (y.id)}
				<option value={y.id}>{y.label}</option>
			{/each}
		</select>
	</Field>
{/if}

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
