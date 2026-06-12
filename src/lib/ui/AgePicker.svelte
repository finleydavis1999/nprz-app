<script>
	// Age range picker for flow datasets that declare a `range`-typed `age`
	// field (ODiN). Mirrors YearPicker's range branch but writes
	// `state.ageMin` / `state.ageMax`, which `runFlows` applies as a
	// `age BETWEEN ageMin AND ageMax` WHERE filter (it does NOT aggregate the
	// value the way the year range does). Renders nothing for datasets without
	// an age range field, so it's safe to mount unconditionally in the panel.
	import { flow } from '$lib/state/flow.svelte.js';
	import RangeSlider from './RangeSlider.svelte';

	let { manifest, state = flow, section = 'flows' } = $props();

	const ageField = $derived(manifest?.[section]?.[state.dataset]?.fields?.age);
	const isRange = $derived(ageField?.type === 'range');

	const rangeMin = $derived(ageField?.min ?? 0);
	const rangeMax = $derived(ageField?.max ?? 0);
	const lo = $derived(state.ageMin ?? rangeMin);
	const hi = $derived(state.ageMax ?? rangeMax);

	// Push semantics: dragging one thumb past the other carries it along.
	function setLo(v) {
		const n = Number(v);
		state.ageMin = n;
		if ((state.ageMax ?? rangeMax) < n) state.ageMax = n;
	}
	function setHi(v) {
		const n = Number(v);
		state.ageMax = n;
		if ((state.ageMin ?? rangeMin) > n) state.ageMin = n;
	}

	// Clamp on dataset change so a saved range stays within the new dataset's
	// bounds. Only relevant when the new dataset is also age-range-typed.
	$effect(() => {
		if (!isRange) return;
		if (state.ageMin == null) state.ageMin = ageField?.defaultMin ?? rangeMin;
		if (state.ageMax == null) state.ageMax = ageField?.defaultMax ?? rangeMax;
		if (state.ageMin < rangeMin) state.ageMin = rangeMin;
		if (state.ageMax > rangeMax) state.ageMax = rangeMax;
		if (state.ageMin > state.ageMax) state.ageMin = state.ageMax;
	});
</script>

{#if isRange}
	<RangeSlider
		min={rangeMin}
		max={rangeMax}
		{lo}
		{hi}
		label={ageField.label ?? 'Leeftijd'}
		valueText="{lo}–{hi}"
		onLo={setLo}
		onHi={setHi}
	/>
{/if}
