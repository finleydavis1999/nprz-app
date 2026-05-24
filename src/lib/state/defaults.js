// Tiny helper used by every state singleton's `reset()` so each default lives
// in exactly one place — the module's top-level `DEFAULTS` const. Object/array
// values are deep-cloned so a later mutation of e.g. `selection.filters` can't
// leak back into the frozen DEFAULTS source.

/**
 * Re-apply a defaults object to a state singleton's reactive fields.
 *
 * @template T
 * @param {T} instance
 * @param {Partial<T>} defaults
 */
export function applyDefaults(instance, defaults) {
	for (const [k, v] of Object.entries(defaults)) {
		/** @type {any} */ (instance)[k] = v !== null && typeof v === 'object' ? structuredClone(v) : v;
	}
}
