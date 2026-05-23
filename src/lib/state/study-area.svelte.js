import { SvelteSet } from 'svelte/reactivity';

// Singleton study-area state — a set of area_codes selected via the lasso tool
// (or pasted in). Persisted to localStorage so it survives reloads. Future
// flow-map filtering will read `studyArea.ids` to restrict origins/destinations.
//
// Scoping: a current set is bound to a particular `selection.scale` (PC4 or
// gemeente) — area_codes are not interchangeable between scales. Saved sets
// record their scale and refuse to load when it differs from the active scale.

const KEY_CURRENT = 'nprz:study-area:current';
const KEY_SAVED = 'nprz:study-area:saved';

function readJSON(key) {
	try {
		const raw = localStorage.getItem(key);
		return raw ? JSON.parse(raw) : null;
	} catch {
		return null;
	}
}

function writeJSON(key, value) {
	try {
		localStorage.setItem(key, JSON.stringify(value));
	} catch {
		// Quota or privacy-mode failures: fail silent — feature degrades to in-memory.
	}
}

class StudyAreaState {
	ids = $state(new SvelteSet());
	scale = $state(/** @type {'pc4' | 'gem' | 'buurt' | null} */ (null));
	saved = $state(/** @type {Record<string, { scale: string, ids: string[] }>} */ ({}));
	#hydrated = false;

	init() {
		if (this.#hydrated || typeof localStorage === 'undefined') return;
		this.#hydrated = true;
		const current = readJSON(KEY_CURRENT);
		if (current && Array.isArray(current.ids)) {
			this.ids = new SvelteSet(current.ids.map(String));
			this.scale = current.scale ?? null;
		}
		const saved = readJSON(KEY_SAVED);
		if (saved && typeof saved === 'object') this.saved = saved;
	}

	#persistCurrent() {
		writeJSON(KEY_CURRENT, { scale: this.scale, ids: [...this.ids] });
	}

	#persistSaved() {
		writeJSON(KEY_SAVED, this.saved);
	}

	#assign(next) {
		this.ids = next;
		this.#persistCurrent();
	}

	add(id) {
		const s = String(id);
		if (this.ids.has(s)) return;
		const next = new SvelteSet(this.ids);
		next.add(s);
		this.#assign(next);
	}

	remove(id) {
		const s = String(id);
		if (!this.ids.has(s)) return;
		const next = new SvelteSet(this.ids);
		next.delete(s);
		this.#assign(next);
	}

	toggle(id) {
		const s = String(id);
		const next = new SvelteSet(this.ids);
		if (next.has(s)) next.delete(s);
		else next.add(s);
		this.#assign(next);
	}

	clear() {
		if (this.ids.size === 0) return;
		this.#assign(new SvelteSet());
	}

	replace(idList) {
		this.#assign(new SvelteSet([...idList].map(String)));
	}

	addMany(idList) {
		const next = new SvelteSet(this.ids);
		for (const id of idList) next.add(String(id));
		this.#assign(next);
	}

	removeMany(idList) {
		const next = new SvelteSet(this.ids);
		for (const id of idList) next.delete(String(id));
		this.#assign(next);
	}

	bindToScale(currentScale) {
		if (this.scale === currentScale) return;
		this.scale = currentScale;
		if (this.ids.size > 0) this.#assign(new SvelteSet());
		else this.#persistCurrent();
	}

	saveAs(name) {
		const trimmed = name.trim();
		if (!trimmed || this.ids.size === 0 || !this.scale) return false;
		this.saved = {
			...this.saved,
			[trimmed]: { scale: this.scale, ids: [...this.ids] }
		};
		this.#persistSaved();
		return true;
	}

	load(name) {
		const entry = this.saved[name];
		if (!entry) return { ok: false, reason: 'not-found' };
		if (entry.scale !== this.scale) {
			return { ok: false, reason: 'scale-mismatch', expected: entry.scale, actual: this.scale };
		}
		this.#assign(new SvelteSet(entry.ids.map(String)));
		return { ok: true };
	}

	delete(name) {
		if (!(name in this.saved)) return;
		const next = { ...this.saved };
		delete next[name];
		this.saved = next;
		this.#persistSaved();
	}
}

export const studyArea = new StudyAreaState();
