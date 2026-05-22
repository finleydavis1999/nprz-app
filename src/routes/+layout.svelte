<script>
	import { onMount } from 'svelte';
	import './layout.css';
	import favicon from '$lib/assets/favicon.svg';
	import { manifestState } from '$lib/state/manifest.svelte.js';
	import { queryResult } from '$lib/state/query-result.svelte.js';
	import { selection } from '$lib/state/selection.svelte.js';
	import { layers } from '$lib/state/layers.svelte.js';
	import { ui } from '$lib/state/ui.svelte.js';
	import { flow } from '$lib/state/flow.svelte.js';

	let { children } = $props();

	// Hydrate persisted singletons before any reactive effect can fire — these
	// mutate state, which Svelte 5 disallows from inside an $effect once the
	// targets have ever been read in a tracked context.
	let hydrated = $state(false);
	onMount(() => {
		layers.load();
		flow.load();
		ui.load();
		hydrated = true;
	});

	// Kick off the manifest fetch once on mount.
	$effect(() => {
		manifestState.ensureLoaded();
	});

	// Persist flow state on every change so navigating to /print (or reloading)
	// preserves the user's flow setup. Skip until hydration finishes so we
	// don't immediately overwrite stored values with defaults.
	$effect(() => {
		if (!hydrated) return;
		void flow.enabled;
		void flow.dataset;
		void flow.scale;
		void flow.yearMin;
		void flow.yearMax;
		void flow.filters;
		void flow.minWeight;
		void flow.minCount;
		void flow.includeSelfLoops;
		flow.persist();
	});
	$effect(() => {
		if (!manifestState.data) return;
		// Touch reactive deps so the effect re-runs on selection changes.
		void selection.dataset;
		void selection.scale;
		void selection.year;
		void selection.filters;
		queryResult.refresh();
	});
	// Re-run all saved layers when the manifest is ready and whenever the
	// active scale changes (area_codes differ across scales, so cached
	// results from the other scale are invalid).
	$effect(() => {
		if (!manifestState.data) return;
		void selection.scale;
		layers.refreshAll();
	});
	// Clear ephemeral inspect state on scale switch — area_codes from the
	// previous scale don't exist on the new layer, so pinned/hovered values
	// would point at non-existent features and confuse the inspect panel +
	// the click-based flow filter.
	$effect(() => {
		void selection.scale;
		ui.selected = null;
		ui.hovered = null;
		ui.selectedFlowNode = null;
	});
</script>

<svelte:head><link rel="icon" href={favicon} /></svelte:head>
{@render children()}
