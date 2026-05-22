<script>
	import { Plot, AreaY, Line, AxisX, AxisY } from 'svelteplot';
	import { kernelWeight } from '$lib/data/spatial-lag.js';

	// Live preview of a smoothing kernel — how a neighbour's weight falls off
	// with distance, to help pick decay / max-distance. `decay` is a length
	// scale in km for exp/gauss, or an exponent for power; `maxDist` is in km.
	// Weights are peak-normalised so every kernel shares a 0–1 axis (power → ∞
	// as d → 0, so its peak is the nearest sampled neighbour).
	let { kernel, decay, maxDist, height = 96 } = $props();

	const curve = $derived.by(() => {
		const n = 64;
		const pts = [];
		let peak = 0;
		// Sample d > 0 (skip the self point) so the power kernel stays finite.
		for (let i = 1; i <= n; i++) {
			const dist = (maxDist * i) / n;
			const w = kernelWeight(kernel, dist, decay);
			if (!Number.isFinite(w) || w <= 0) continue;
			pts.push({ dist, w });
			if (w > peak) peak = w;
		}
		return peak > 0 ? pts.map((p) => ({ dist: p.dist, weight: p.w / peak })) : [];
	});
</script>

<div class="kernel-curve">
	{#if curve.length > 1}
		<Plot
			{height}
			marginTop={6}
			marginBottom={28}
			marginLeft={34}
			marginRight={8}
			x={{ domain: [0, maxDist], label: 'distance (km) →' }}
			y={{ domain: [0, 1], label: '↑ weight' }}
		>
			<AreaY data={curve} x="dist" y="weight" fill="var(--color-accent)" fillOpacity={0.14} />
			<Line data={curve} x="dist" y="weight" stroke="var(--color-accent)" strokeWidth={1.75} />
			<AxisX tickCount={5} />
			<AxisY tickCount={3} />
		</Plot>
	{/if}
</div>

<style>
	.kernel-curve {
		font-size: var(--text-xs);
		color: var(--color-muted);
	}
</style>
