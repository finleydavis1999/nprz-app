<script>
	import { selection } from '$lib/state/selection.svelte.js';
	import Field from './Field.svelte';
	import RangeSlider from './RangeSlider.svelte';

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
	const lo = $derived(state.yearMin ?? rangeMin);
	const hi = $derived(state.yearMax ?? rangeMax);

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
	<RangeSlider
		min={rangeMin}
		max={rangeMax}
		{lo}
		{hi}
		label={yearField.label ?? 'Periode'}
		valueText="{lo}–{hi}"
		onLo={setLo}
		onHi={setHi}
	/>
{:else}
	<Field label="Year">
		<select bind:value={state.year}>
			{#each yearOptions as y (y.id)}
				<option value={y.id}>{y.label}</option>
			{/each}
		</select>
	</Field>
{/if}
