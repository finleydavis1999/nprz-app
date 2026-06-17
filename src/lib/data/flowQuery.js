// Flow (OD) query layer. Aggregates edge parquet by (o_code, d_code) for the
// current year range + filters. Returns the full aggregated set so downstream
// calculations have everything; the UI applies client-side `minWeight` /
// `minCount` cutoffs to keep the rendered set tractable.
//
// For weighted-survey layers (`entry.weighted`, e.g. OViN/ODiN) the query also
// returns the raw observation `count` alongside the weighted `value`.
//
// Two manifest-driven `type: "toggle"` query modifiers are supported (read from
// `toggles` keyed by field id):
//   - `divideYears` — for categorical-period flows, normalise each period's
//     total to a per-year figure by dividing by that period's calendar-year
//     span (`year` value's `years`). Applied per-period before the cross-period
//     sum, so multi-period selections stay correct.
//   - `hhfilter` — for trip-grain weighted flows that declare `hhDedupCol`
//     (ODiN's `hhid`), count distinct *households* (movers) instead of *trips*
//     (movements): dedupe to one weight per (o,d,household,year) post-filter,
//     then sum; `count` becomes COUNT(DISTINCT household).
import { getDb } from './duckdb.js';
import { ensureRegistered, num, valueExpr } from './parquet-register.js';

// Build the shared WHERE clause (year range, age range, multi filters,
// self-loop exclusion) — identical across all three query modes.
function buildWhere({
	yearMin,
	yearMax,
	ageMin,
	ageMax,
	ageRange,
	filters,
	includeSelfLoops,
	yearRange
}) {
	const wheres = [];
	if (yearRange) wheres.push(`year BETWEEN ${num(yearMin)} AND ${num(yearMax)}`);
	// Age is a plain row filter (unlike `year`, it never aggregates the value).
	// Only applied when the dataset declares a `range`-typed `age` field and
	// both bounds are finite — old saved layers without age params skip it.
	if (ageRange && Number.isFinite(ageMin) && Number.isFinite(ageMax)) {
		wheres.push(`age BETWEEN ${num(ageMin)} AND ${num(ageMax)}`);
	}
	for (const [field, values] of Object.entries(filters ?? {})) {
		if (!values || values.length === 0) continue;
		wheres.push(`${field} IN (${values.map(num).join(',')})`);
	}
	if (!includeSelfLoops) wheres.push('o_code <> d_code');
	return wheres.length ? `WHERE ${wheres.join(' AND ')}` : '';
}

// `CASE year WHEN <id> THEN <years> ... ELSE 1 END` divisor for divideYears,
// built from the manifest `year` value list. Periods without a `years` span
// fall through to 1 (no division).
function periodYearsCase(yearValues) {
	const arms = (yearValues ?? [])
		.filter((v) => Number.isFinite(v?.years) && v.years > 0)
		.map((v) => `WHEN ${num(v.id)} THEN ${num(v.years)}`);
	return arms.length ? `CASE year ${arms.join(' ')} ELSE 1 END` : '1';
}

// Run a flow query and return { flows: [{o,d,value,count?}], min, max, weighted }.
// `count` (raw observations) is present on each flow only for weighted layers.
export async function runFlows({
	dataset,
	scale = 'gem',
	yearMin,
	yearMax,
	ageMin,
	ageMax,
	filters = {},
	toggles = {},
	includeSelfLoops = false
}) {
	const { name, entry } = await ensureRegistered({ section: 'flows', dataset, scale });
	const yearRange = entry.fields?.year?.type === 'range';
	const ageRange = entry.fields?.age?.type === 'range';
	const weightCol = entry.weightCol ?? 'count';
	const weighted = !!entry.weighted;

	const hhMode = !!toggles.hhfilter && !!entry.hhDedupCol;
	const divideMode = !!toggles.divideYears && !yearRange && entry.fields?.year?.type === 'multi';

	const where = buildWhere({
		yearMin,
		yearMax,
		ageMin,
		ageMax,
		ageRange,
		filters,
		includeSelfLoops,
		yearRange
	});

	let sql;
	if (hhMode) {
		// Dedupe to one survey weight per (o, d, household, year), then sum — so a
		// household making several matching trips counts once per year (movers,
		// not movements). `count` = distinct households. Same year normalisation
		// (`yearAggregation`) as the trip path, applied to the deduped weight.
		const valueSql = valueExpr({ entry, yearMin, yearMax, sumExpr: 'hw' });
		sql = `
			WITH hh AS (
				SELECT o_code AS o, d_code AS d, ${entry.hhDedupCol} AS hid, year,
				       ANY_VALUE(${weightCol}) AS hw
				FROM read_parquet('${name}')
				${where}
				GROUP BY o_code, d_code, ${entry.hhDedupCol}, year
			)
			SELECT o, d, ${valueSql}, COUNT(DISTINCT hid)::BIGINT AS count
			FROM hh
			GROUP BY o, d
			HAVING SUM(hw) > 0
			ORDER BY value DESC
		`;
	} else if (divideMode) {
		// Per-period division: SUM(weight / period_year_span). Off → plain SUM.
		const divCase = periodYearsCase(entry.fields?.year?.values);
		const countSql = weighted ? `, SUM(${entry.countCol})::BIGINT AS count` : '';
		sql = `
			SELECT o_code AS o, d_code AS d,
			       SUM(${weightCol} / (${divCase}))::DOUBLE AS value${countSql}
			FROM read_parquet('${name}')
			${where}
			GROUP BY o_code, d_code
			HAVING SUM(${weightCol}) > 0
			ORDER BY value DESC
		`;
	} else {
		const valueSql = yearRange
			? valueExpr({ entry, yearMin, yearMax })
			: `SUM(${weightCol})::DOUBLE AS value`;
		const countSql = weighted ? `, SUM(${entry.countCol})::BIGINT AS count` : '';
		sql = `
			SELECT o_code AS o, d_code AS d, ${valueSql}${countSql}
			FROM read_parquet('${name}')
			${where}
			GROUP BY o_code, d_code
			HAVING SUM(${weightCol}) > 0
			ORDER BY value DESC
		`;
	}

	const db = await getDb();
	const conn = await db.connect();
	try {
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
