// Cross-cutting UI state: which floating docks are open + their positions,
// and the inspect-panel state (hovered/selected node, optional flow-direction
// mode used when a node is clicked while flows are active).
//
// Persisted to localStorage under `nprz.ui.v1` so dock positions survive
// reloads. Inspect state is ephemeral and not persisted.

const STORAGE_KEY = 'nprz.ui.v1';

/** @typedef {{ x: number, y: number }} DockPos */
/** @typedef {'calculator' | 'studyArea' | 'model'} DockId */

const DEFAULT_POSITIONS = {
	calculator: { x: 360, y: 24 },
	studyArea: { x: 360, y: 360 },
	model: { x: 720, y: 24 }
};

class UIState {
	/** @type {Record<DockId, boolean>} */
	openDocks = $state({ calculator: false, studyArea: false, model: false });

	/** @type {Record<DockId, DockPos>} */
	dockPositions = $state({ ...DEFAULT_POSITIONS });

	// Inspect panel — feature currently under the cursor (hovered) or pinned (selected).
	// Either or both can be null. `kind` distinguishes node vs flow.
	/** @typedef {{ kind: 'node', id: string } | { kind: 'flow', o: string, d: string }} InspectTarget */
	/** @type {InspectTarget | null} */
	hovered = $state(null);

	/** @type {InspectTarget | null} */
	selected = $state(null);

	// Resolved area_code (in *flow* scale) used to filter the flow layer.
	// Usually equals `selected.id` when the inspect target and flows share a
	// scale; differs when the user clicks a PC4 while flows are gem-scale,
	// in which case the click is resolved to the containing gemeente.
	/** @type {string | null} */
	selectedFlowNode = $state(null);

	// When a node is selected and flows are active, which direction to show.
	/** @type {'in' | 'out' | 'unified'} */
	flowMode = $state('unified');

	// Toggle for the on-map nodal name labels layer.
	showLabels = $state(false);

	load() {
		if (typeof localStorage === 'undefined') return;
		try {
			const raw = localStorage.getItem(STORAGE_KEY);
			if (!raw) return;
			const parsed = JSON.parse(raw);
			if (parsed?.openDocks) this.openDocks = { ...this.openDocks, ...parsed.openDocks };
			if (parsed?.dockPositions) {
				this.dockPositions = { ...this.dockPositions, ...parsed.dockPositions };
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
				JSON.stringify({ openDocks: this.openDocks, dockPositions: this.dockPositions })
			);
		} catch {
			// quota / private mode — non-fatal
		}
	}

	toggleDock(/** @type {DockId} */ id) {
		this.openDocks = { ...this.openDocks, [id]: !this.openDocks[id] };
		this.persist();
	}

	setDockPosition(/** @type {DockId} */ id, /** @type {DockPos} */ pos) {
		this.dockPositions = { ...this.dockPositions, [id]: pos };
		this.persist();
	}

	selectNode(/** @type {string | null} */ id) {
		this.selected = id ? { kind: 'node', id } : null;
	}

	hoverNode(/** @type {string | null} */ id) {
		this.hovered = id ? { kind: 'node', id } : null;
	}

	selectFlow(/** @type {{o: string, d: string} | null} */ edge) {
		this.selected = edge ? { kind: 'flow', o: edge.o, d: edge.d } : null;
	}

	hoverFlow(/** @type {{o: string, d: string} | null} */ edge) {
		this.hovered = edge ? { kind: 'flow', o: edge.o, d: edge.d } : null;
	}

	clearSelection() {
		this.selected = null;
	}
}

export const ui = new UIState();
