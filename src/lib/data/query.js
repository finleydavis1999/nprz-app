// Choropleth query layer. Builds parameterized DuckDB queries against
// OPFS-cached parquet files registered through `parquet-register`.
//
// Node datasets are single-year. The valueExpr divisor is driven by the
// dataset's manifest `yearAggregation` and is 1 for `sum` (the default for
// node datasets), so the SQL is effectively `SUM(count)` for the chosen year.
import { getDb } from './duckdb.js';
import { ensureRegistered, num, valueExpr } from './parquet-register.js';

function buildSql(parquetName, valueSql, { year, filters = {}, needsGroupBy = true }) {
	const wheres = [`year = ${num(year)}`];
	for (const [field, values] of Object.entries(filters)) {
		if (!values || values.length === 0) continue;
		if (field === 'variable') continue;
		wheres.push(`${field} IN (${values.map(num).join(',')})`);
	}
	const groupBy = needsGroupBy ? 'GROUP BY area_code' : '';
	return `
		SELECT area_code, ${valueSql}
		FROM read_parquet('${parquetName}')
		WHERE ${wheres.join(' AND ')}
		${groupBy}
	`;
}

export async function runChoropleth({ dataset, scale, year, filters }) {
	const { name, entry } = await ensureRegistered({ section: 'datasets', dataset, scale });
	const selectedVar = filters?.variable?.[0];
	const effectiveEntry = selectedVar ? { ...entry, countCol: selectedVar } : entry;
	const col = effectiveEntry.countCol ?? 'count';
	const isRawCount = col === 'count';
	const valueSql = isRawCount
		? valueExpr({ entry: effectiveEntry, yearMin: year, yearMax: year })
		: `"${col}"::DOUBLE AS value`;
	// CBS pre-aggregated parquets have one row per area — no GROUP BY needed
	const needsGroupBy = isRawCount;
	const db = await getDb();
	const conn = await db.connect();
	try {
		const sql = buildSql(name, valueSql, { year, filters, needsGroupBy });
		const result = await conn.query(sql);
		const map = new Map();
		for (const row of result) {
			map.set(row.area_code, row.value);
		}
		return map;
	} finally {
		await conn.close();
	}
}
