// Live-map view state — `{center, zoom}` of the maplibre map, persisted so
// the view survives navigation to `/print` (or any other route) and back, and
// across reloads. Without this the user gets dropped back at the nationwide
// default every time they cycle through the print preview.
//
// Capture: a `moveend` listener inside `Map.svelte` writes the new view here
// (rAF-debounced to keep us out of Svelte 5's effect-update-depth trap).
// Restore: `Map.svelte` reads from this singleton when constructing the
// maplibre `Map` instance.
//
// Reactivity rule: when mutating `center`, assign a fresh array
// (`mapView.center = [...]`) — deep mutation isn't tracked.
import { MAP_DEFAULTS } from '$lib/map/defaults.js';

const STORAGE_KEY = 'nprz.mapview.v1';

class MapViewState {
	/** @type {[number, number]} */
	center = $state(/** @type {[number, number]} */ ([...MAP_DEFAULTS.center]));
	zoom = $state(MAP_DEFAULTS.zoom);

	load() {
		if (typeof localStorage === 'undefined') return;
		try {
			const raw = localStorage.getItem(STORAGE_KEY);
			if (!raw) return;
			const p = JSON.parse(raw);
			if (
				Array.isArray(p?.center) &&
				p.center.length === 2 &&
				Number.isFinite(p.center[0]) &&
				Number.isFinite(p.center[1])
			) {
				this.center = [p.center[0], p.center[1]];
			}
			if (Number.isFinite(p?.zoom)) this.zoom = p.zoom;
		} catch {
			// corrupted storage — ignore
		}
	}

	persist() {
		if (typeof localStorage === 'undefined') return;
		try {
			localStorage.setItem(STORAGE_KEY, JSON.stringify({ center: this.center, zoom: this.zoom }));
		} catch {
			// quota / private mode — non-fatal
		}
	}
}

export const mapView = new MapViewState();
