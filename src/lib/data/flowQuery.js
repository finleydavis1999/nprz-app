// Flow (OD) query layer. Aggregates edge parquet by (o_code, d_code) for the
// current year range + filters. Returns the full aggregated set so downstream
// calculations have everything; the UI applies client-side `minWeight` /
// `minCount` cutoffs to keep the rendered set tractable.
//
// For weighted-survey layers (`entry.weighted`, e.g. OViN/ODiN) the query also
// returns the raw observation `count` alongside the weighted `value`.
import { getDb } from './duckdb.js';
import { ensureRegistered, num, valueExpr } from './parquet-register.js';

function buildSql(
	parquetName,
	valueSql,
	{ yearMin, yearMax, filters = {}, includeSelfLoops, yearRange, weightCol, countSql }
) {
	const wheres = [];
	if (yearRange) wheres.push(`year BETWEEN ${num(yearMin)} AND ${num(yearMax)}`);
	for (const [field, values] of Object.entries(filters)) {
		if (!values || values.length === 0) continue;
		wheres.push(`${field} IN (${values.map(num).join(',')})`);
	}
	if (!includeSelfLoops) wheres.push('o_code <> d_code');
	const where = wheres.length ? `WHERE ${wheres.join(' AND ')}` : '';
	return `
		SELECT o_code AS o, d_code AS d, ${valueSql}${countSql}
		FROM read_parquet('${parquetName}')
		${where}
		GROUP BY o_code, d_code
		HAVING SUM(${weightCol}) > 0
		ORDER BY value DESC
	`;
}

// Run a flow query and return { flows: [{o,d,value,count?}], min, max, weighted }.
// `count` (raw observations) is present on each flow only for weighted layers.
export async function runFlows({
	dataset,
	scale = 'gem',
	yearMin,
	yearMax,
	filters = {},
	includeSelfLoops = false
}) {
	const { name, entry } = await ensureRegistered({ section: 'flows', dataset, scale });
	const yearRange = entry.fields?.year?.type === 'range';
	const weightCol = entry.weightCol ?? 'count';
	const valueSql = yearRange
		? valueExpr({ entry, yearMin, yearMax })
		: `SUM(${weightCol})::DOUBLE AS value`;
	const weighted = !!entry.weighted;
	const countSql = weighted ? `, SUM(${entry.countCol})::BIGINT AS count` : '';
	const db = await getDb();
	const conn = await db.connect();
	try {
		const sql = buildSql(name, valueSql, {
			yearMin,
			yearMax,
			filters,
			includeSelfLoops,
			yearRange,
			weightCol,
			countSql
		});
		const result = await conn.query(sql);
		const flows = [];
		let min = Infinity;
		let max = -Infinity;
		for (const row of result) {
			const v = Number(row.value);
			if (!Number.isFinite(v)) continue;
			const flow = { o: row.o, d: row.d, value: v };
			if (weighted) flow.count = Number(row.count);
			flows.push(flow);
			if (v < min) min = v;
			if (v > max) max = v;
		}
		return { flows, min: flows.length ? min : 0, max: flows.length ? max : 0, weighted };
	} finally {
		await conn.close();
	}
}
