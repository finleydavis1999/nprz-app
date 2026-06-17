// Shared parquet-registration helpers used by both choropleth (`query.js`) and
// flow (`flowQuery.js`) queries. Resolves a manifest entry → fetches via OPFS
// cache → registers a stable filename inside DuckDB-WASM so SQL can do
// `read_parquet('<name>')`.
import { getDb, duckdbNamespace } from './duckdb.js';
import { initCache, getOrFetch } from './opfs-cache.js';
import { loadManifest } from './manifest.js';
import { dataUrl } from './url.js';

const registered = new Map();

// `section` is 'datasets' or 'flows' — the top-level manifest key the dataset
// lives under. Returns { name, entry } where `name` is the registered filename
// to use in SQL and `entry` is the manifest record (for yearAggregation etc).
export async function ensureRegistered({ section, dataset, scale }) {
	const key = `${section}-${dataset}-${scale}`;
	if (!registered.has(key)) registered.set(key, registerParquet({ section, dataset, scale }));
	return registered.get(key);
}

async function registerParquet({ section, dataset, scale }) {
	const manifest = await loadManifest();
	const entry = manifest[section]?.[dataset];
	if (!entry) throw new Error(`unknown ${section} entry: ${dataset}`);
	const relPath = entry.scales[scale];
	if (!relPath) throw new Error(`no scale '${scale}' for ${section}.${dataset}`);

	const versionRoot = await initCache(manifest.version);
	const handle = await getOrFetch(versionRoot, relPath, dataUrl(relPath, manifest.version));

	const db = await getDb();
	const duckdb = await duckdbNamespace();
	const name = `${section}-${dataset}-${scale}.parquet`;
	await db.registerFileHandle(name, handle, duckdb.DuckDBDataProtocol.BROWSER_FSACCESS, true);
	return { name, entry };
}

// Quote a numeric IN-list value safely (defense-in-depth — manifest is trusted
// but filter values originate from UI state).
export function num(v) {
	const n = Number(v);
	if (!Number.isFinite(n)) throw new Error(`non-numeric filter value: ${v}`);
	return n;
}

// Build the SQL `value` expression that aggregates the dataset's weight column
// (`entry.weightCol`, falling back to `count` for datasets that don't declare
// one) and normalises across the selected year range per `yearAggregation`:
//   'sum'   (default) → SUM(weight)
//   'mean'            → SUM(weight) / years     (years = yearMax - yearMin + 1)
//   'daily'           → SUM(weight) / (years * 365)
// `sumExpr` overrides what gets summed (defaults to the weight column). Pass a
// pre-built column/expression — e.g. `'hw'` for an already-aggregated household
// weight in an hhfilter subquery — when the same year normalisation must apply
// to a different quantity.
export function valueExpr({ entry, yearMin, yearMax, alias = 'value', sumExpr }) {
	const years = num(yearMax) - num(yearMin) + 1;
	if (years < 1) throw new Error(`invalid year range: ${yearMin}..${yearMax}`);
	const mode = entry.yearAggregation ?? 'sum';
	let divisor;
	switch (mode) {
		case 'mean':
			divisor = years;
			break;
		case 'daily':
			divisor = years * 365;
			break;
		case 'sum':
			divisor = 1;
			break;
		default:
			throw new Error(`unknown yearAggregation: ${mode}`);
	}
	const col = sumExpr ?? entry.weightCol ?? 'count';
	const expr = divisor === 1 ? `SUM(${col})::DOUBLE` : `(SUM(${col})::DOUBLE / ${divisor})`;
	return alias ? `${expr} AS ${alias}` : expr;
}
