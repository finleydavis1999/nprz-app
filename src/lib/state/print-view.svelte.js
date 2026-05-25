// Print viewport state — drives the live-map A4 framing overlay and the
// `/print` page. Persisted so framing survives reloads and worktree switches.
//
// `rdExtent` is `[[minX,minY],[maxX,maxY]]` in RD New (EPSG:28992) metres,
// computed by `PrintFrameOverlay.svelte` from the current map view. `null`
// means "nationwide" — the print map falls back to fitting all features.
//
// Reactivity rule: when updating `rdExtent`, assign a fresh array
// (`printView.rdExtent = [[...], [...]]`) — deep mutation isn't tracked.
const STORAGE_KEY = 'nprz.print.v1';

/**
 * @typedef {Object} PlaceLabel
 * @property {string} text
 * @property {number} x  RD New x in metres
 * @property {number} y  RD New y in metres
 * @property {string} kind          Protomaps layer subclass — 'country' | 'region' | 'subregion' | 'locality' | 'neighbourhood' | …
 * @property {string} [kindDetail]  Protomaps `kind_detail` — 'city' | 'town' | 'village' | 'hamlet' | …
 * @property {number} [populationRank]  Protomaps `population_rank` (int, **higher = bigger**); 0 when unknown.
 */

class PrintViewState {
	/** @type {'portrait' | 'landscape'} */
	orientation = $state('portrait');
	showOverlay = $state(false);
	/** @type {[[number, number], [number, number]] | null} */
	rdExtent = $state(null);
	/** @type {PlaceLabel[]}
	 *  Captured at frame time from the live map's visible Protomaps place
	 *  labels (cities/towns/regions). Persisted alongside the extent so the
	 *  print preview still shows them after the frame overlay is closed. */
	placeLabels = $state([]);
	/** Set of `text|kind` keys the user has turned off in the print preview.
	 *  Persisted as an array; rehydrated to a plain Array (PrintMap turns it
	 *  into a Set lazily). Defaults to empty (every captured label shown). */
	disabledLabels = $state(/** @type {string[]} */ ([]));
	/** Population-rank threshold — only show labels with
	 *  `populationRank >= minPopulationRank`. Protomaps' `population_rank`
	 *  is **higher = bigger** (so a higher threshold = fewer, more important
	 *  labels). `0` (default) imposes no filtering. */
	minPopulationRank = $state(0);

	load() {
		if (typeof localStorage === 'undefined') return;
		try {
			const raw = localStorage.getItem(STORAGE_KEY);
			if (!raw) return;
			const p = JSON.parse(raw);
			if (p?.orientation === 'portrait' || p?.orientation === 'landscape') {
				this.orientation = p.orientation;
			}
			if (typeof p?.showOverlay === 'boolean') this.showOverlay = p.showOverlay;
			if (
				Array.isArray(p?.rdExtent) &&
				p.rdExtent.length === 2 &&
				Array.isArray(p.rdExtent[0]) &&
				Array.isArray(p.rdExtent[1])
			) {
				this.rdExtent = p.rdExtent;
			}
			if (Array.isArray(p?.placeLabels)) {
				this.placeLabels = p.placeLabels.filter(
					(l) =>
						l &&
						typeof l.text === 'string' &&
						Number.isFinite(l.x) &&
						Number.isFinite(l.y) &&
						typeof l.kind === 'string'
				);
			}
			if (Array.isArray(p?.disabledLabels)) {
				this.disabledLabels = p.disabledLabels.filter((k) => typeof k === 'string');
			}
			if (Number.isFinite(p?.minPopulationRank)) {
				this.minPopulationRank = p.minPopulationRank;
			}
		} catch {
			// corrupted storage — ignore
		}
	}

	persist() {
		if (typeof localStorage === 'undefined') return;
		try {
			localStorage.setItem(
				STORAGE_KEY,
				JSON.stringify({
					orientation: this.orientation,
					showOverlay: this.showOverlay,
					rdExtent: this.rdExtent,
					placeLabels: this.placeLabels,
					disabledLabels: this.disabledLabels,
					minPopulationRank: this.minPopulationRank
				})
			);
		} catch {
			// quota / private mode — non-fatal
		}
	}
}

export const printView = new PrintViewState();
