<script>
	// A clear on/off switch with a label. Wraps a native checkbox so it stays
	// keyboard-accessible and bindable; the checkbox is transparent but full-size
	// (and on top) so it is always the click target — for users and for tests.
	// `onchange` reports the new state for callers that can't two-way bind (e.g.
	// a switch whose value lives in a `Record<string, boolean>`).
	let { checked = $bindable(false), label, onchange } = $props();
</script>

<label class="toggle">
	<input type="checkbox" bind:checked onchange={() => onchange?.(checked)} />
	<span class="track"></span>
	<span class="text">{label}</span>
</label>

<style>
	.toggle {
		position: relative;
		display: flex;
		align-items: center;
		gap: var(--spacing-2);
		cursor: pointer;
		user-select: none;
		font-size: var(--text-sm);
		color: var(--color-text);
	}
	input {
		position: absolute;
		inset: 0;
		z-index: 1;
		margin: 0;
		opacity: 0;
		cursor: pointer;
	}
	.track {
		--track-h: var(--spacing-4);
		--track-w: calc(var(--spacing-4) * 2);
		--thumb: var(--spacing-3);
		--gap: calc((var(--track-h) - var(--thumb)) / 2);
		position: relative;
		flex: none;
		width: var(--track-w);
		height: var(--track-h);
		border-radius: var(--radius-pill);
		background: var(--color-line);
		transition: background 0.15s ease;
	}
	.track::after {
		content: '';
		position: absolute;
		top: var(--gap);
		left: var(--gap);
		width: var(--thumb);
		height: var(--thumb);
		border-radius: var(--radius-pill);
		background: var(--color-accent-fg);
		box-shadow: 0 1px 2px rgba(0, 0, 0, 0.3);
		transition: transform 0.15s ease;
	}
	input:checked + .track {
		background: var(--color-accent);
	}
	input:checked + .track::after {
		transform: translateX(calc(var(--track-w) - var(--thumb) - 2 * var(--gap)));
	}
	input:focus-visible + .track {
		outline: 2px solid var(--color-accent);
		outline-offset: 2px;
	}
</style>
