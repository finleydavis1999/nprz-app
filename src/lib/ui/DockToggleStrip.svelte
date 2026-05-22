<script>
	import { resolve } from '$app/paths';
	import { ui } from '$lib/state/ui.svelte.js';

	const docks = /** @type {const} */ ([
		{ id: 'calculator', label: 'Layer Calculator', glyph: 'ƒx', title: 'Layer Calculator' },
		{ id: 'studyArea', label: 'Study area', glyph: '⌒', title: 'Study area & lasso' }
	]);
</script>

<div class="strip" role="toolbar" aria-label="Tools">
	{#each docks as d (d.id)}
		<button
			type="button"
			class="tool"
			class:active={ui.openDocks[d.id]}
			onclick={() => ui.toggleDock(d.id)}
			title={d.title}
		>
			<span class="glyph">{d.glyph}</span>
			<span class="label">{d.label}</span>
		</button>
	{/each}
	<a class="tool print" href={resolve('/print')} title="Print preview">
		<span class="glyph">⎙</span>
		<span class="label">Print</span>
	</a>
</div>

<style>
	.strip {
		position: fixed;
		bottom: var(--spacing-4);
		left: var(--spacing-4);
		z-index: 4;
		display: flex;
		gap: var(--spacing-1);
		padding: var(--spacing-1);
		background: var(--color-bg-panel);
		border: 1px solid var(--color-line);
		border-radius: var(--radius);
		box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
	}
	.tool {
		display: inline-flex;
		align-items: center;
		gap: 6px;
		padding: 4px var(--spacing-2);
		background: transparent;
		border: 1px solid transparent;
		border-radius: var(--radius);
		color: var(--color-text);
		font-size: var(--text-sm);
		cursor: pointer;
		text-decoration: none;
	}
	.tool:hover {
		background: rgba(31, 35, 40, 0.06);
	}
	.tool.active {
		background: var(--color-accent);
		color: var(--color-accent-fg);
		border-color: var(--color-accent);
	}
	.tool.print {
		background: var(--color-accent);
		color: var(--color-accent-fg);
		border-color: var(--color-accent);
		font-weight: 600;
	}
	.tool.print:hover {
		filter: brightness(1.1);
	}
	.glyph {
		font-size: var(--text-sm);
		font-family: ui-monospace, monospace;
	}
	.label {
		font-size: var(--text-sm);
	}
</style>
