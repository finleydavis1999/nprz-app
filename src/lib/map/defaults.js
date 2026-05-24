// The initial NPRZ map viewport. Used as the <MapView> prop fallback in
// Map.svelte, as the explicit values passed from +page.svelte, and as the
// target of the header reset button's map.jumpTo(). One source of truth.

export const MAP_DEFAULTS = Object.freeze({
	center: /** @type {[number, number]} */ ([5.3, 52.1]),
	zoom: 7
});
