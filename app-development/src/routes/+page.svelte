<script lang="ts">
  import { MapLibre, GeoJSONSource, FillLayer, LineLayer } from 'svelte-maplibre-gl';
  import 'maplibre-gl/dist/maplibre-gl.css';
  import { browser } from '$app/environment';
  import * as duckdb from '@duckdb/duckdb-wasm';

  // ── Scale configuration ──────────────────────────────────
  const INNER_SCALES = [
    { key: '100m',  label: '100m grid',  file: 'grid_100m_rijnmond.parquet', idCol: 'crs28992res100m' },
    { key: '500m',  label: '500m grid',  file: 'grid_500m_rijnmond.parquet', idCol: 'crs28992res100m' },
    { key: 'buurt', label: 'Buurt',      file: 'buurt_2024.parquet',          idCol: 'buurtcode' },
  ];

  const OUTER_SCALES = [
    { key: 'pc4',       label: 'PC4 postcode', file: 'pc4_zh_2024.parquet',   idCol: 'postcode' },
    { key: 'wijk',      label: 'Wijk',         file: 'wijk_2024.parquet',      idCol: 'wijkcode' },
    { key: 'gemeente',  label: 'Gemeente',     file: 'gemeente_2024.parquet',  idCol: 'gemeentecode' },
  ];

  // ── Variables available at all scales ────────────────────
  // Key must exist in 100m grid (most restrictive)
  // Variables only in admin files will be added separately later
  const VARIABLES = [
    { key: 'aantal_inwoners',                              label: 'Total population',               canNormalise: false },
    { key: 'aantal_mannen',                                label: 'Men',                            canNormalise: true  },
    { key: 'aantal_vrouwen',                               label: 'Women',                          canNormalise: true  },
    { key: 'aantal_inwoners_0_tot_15_jaar',                label: 'Age 0–15',                       canNormalise: true  },
    { key: 'aantal_inwoners_15_tot_25_jaar',               label: 'Age 15–25',                      canNormalise: true  },
    { key: 'aantal_inwoners_25_tot_45_jaar',               label: 'Age 25–45',                      canNormalise: true  },
    { key: 'aantal_inwoners_45_tot_65_jaar',               label: 'Age 45–65',                      canNormalise: true  },
    { key: 'aantal_inwoners_65_jaar_en_ouder',             label: 'Age 65+',                        canNormalise: true  },
    { key: 'percentage_geb_nederland_herkomst_nederland',  label: '% Dutch origin',                 canNormalise: false },
    { key: 'percentage_geb_nederland_herkomst_overig_europa', label: '% European origin (NL-born)', canNormalise: false },
    { key: 'percentage_geb_nederland_herkomst_buiten_europa', label: '% Non-European (NL-born)',    canNormalise: false },
    { key: 'percentage_geb_buiten_nederland_herkomst_europa', label: '% European (foreign-born)',   canNormalise: false },
    { key: 'percentage_geb_buiten_nederland_herkmst_buiten_europa', label: '% Non-European (foreign-born)', canNormalise: false },
    { key: 'aantal_part_huishoudens',                      label: 'Total households',               canNormalise: true  },
    { key: 'aantal_eenpersoonshuishoudens',                label: 'Single-person households',       canNormalise: true  },
    { key: 'gemiddelde_huishoudensgrootte',                label: 'Avg household size',             canNormalise: false },
    { key: 'aantal_woningen',                              label: 'Total dwellings',                canNormalise: true  },
    { key: 'percentage_koopwoningen',                      label: '% Owner-occupied',               canNormalise: false },
    { key: 'percentage_huurwoningen',                      label: '% Rental',                       canNormalise: false },
    { key: 'aantal_huurwoningen_in_bezit_woningcorporaties', label: 'Social housing units',         canNormalise: true  },
    { key: 'aantal_niet_bewoonde_woningen',                label: 'Unoccupied dwellings',           canNormalise: true  },
    { key: 'aantal_personen_met_uitkering_onder_aowlft',   label: 'Persons on benefits',            canNormalise: true  },
  ];

  const NORMALISATIONS = [
    { key: 'none',            label: 'Raw value' },
    { key: 'per_km2',         label: 'Per km²' },
    { key: 'per_1000',        label: 'Per 1,000 inhabitants' },
  ];

  // ── Colour scheme (4 quantile classes + no-data) ─────────
  const COLOURS = ['#f1eef6', '#bdc9e1', '#74a9cf', '#045a8d'];
  const NO_DATA_COLOUR = '#e0e0e0';

  // ── App state ─────────────────────────────────────────────
  let innerScale     = $state(INNER_SCALES[0].key);
  let outerScale     = $state(OUTER_SCALES[2].key);  // gemeente default — widest view
  let selectedVar    = $state(VARIABLES[0].key);
  let normalisation  = $state('none');

  let innerGeojson   = $state<any>(null);
  let outerGeojson   = $state<any>(null);
  let innerBreaks    = $state<number[]>([]);
  let outerBreaks    = $state<number[]>([]);
  let loading        = $state(false);
  let error          = $state<string | null>(null);

  let db: duckdb.AsyncDuckDB | null = null;
  let conn: any = null;

  // ── WKT parser — handles POLYGON and MULTIPOLYGON ────────
  function parseWKT(wkt: string): any {
    if (!wkt) return null;
    try {
      if (wkt.startsWith('MULTIPOLYGON')) {
        // Strip outer wrapper, split into polygons
        const inner = wkt.replace(/^MULTIPOLYGON\s*\(\(\(/, '').replace(/\)\)\)$/, '');
        const polygons = inner.split(')), ((').map(poly => {
          const rings = poly.split('),(').map(ring =>
            ring.replace(/^\(+/, '').replace(/\)+$/, '')
                .split(',').map(pair => {
                  const [x, y] = pair.trim().split(/\s+/).map(Number);
                  return [x, y];
                })
          );
          return rings;
        });
        return { type: 'MultiPolygon', coordinates: polygons };
      } else if (wkt.startsWith('POLYGON')) {
        const inner = wkt.replace(/^POLYGON\s*\(\(/, '').replace(/\)\)$/, '');
        const rings = inner.split('),(').map(ring =>
          ring.replace(/^\(+/, '').replace(/\)+$/, '')
              .split(',').map(pair => {
                const [x, y] = pair.trim().split(/\s+/).map(Number);
                return [x, y];
              })
        );
        return { type: 'Polygon', coordinates: rings };
      }
    } catch { return null; }
    return null;
  }

  // ── Quantile breaks ───────────────────────────────────────
  function quantileBreaks(values: number[], n: number): number[] {
    const sorted = [...values].filter(v => isFinite(v)).sort((a, b) => a - b);
    if (sorted.length === 0) return [];
    return Array.from({ length: n - 1 }, (_, i) => {
      const idx = Math.floor(((i + 1) / n) * sorted.length);
      return sorted[Math.min(idx, sorted.length - 1)];
    });
  }

  function classifyValue(value: number, breaks: number[]): string {
    if (value === null || value === undefined || !isFinite(value) || 
        value <= -99990) return 'nodata';
    for (let i = 0; i < breaks.length; i++) {
      if (value <= breaks[i]) return String(i);
    }
    return String(breaks.length);
  }

  // ── Build normalisation SQL expression ───────────────────
  function normExpr(varKey: string, norm: string): string {
    const v = `"${varKey}"`;
    if (norm === 'per_km2') {
      return `CASE WHEN "oppervlakte_land_in_ha" > 0 
              THEN ${v} / ("oppervlakte_land_in_ha" / 100.0)
              ELSE NULL END`;
    }
    if (norm === 'per_1000') {
      return `CASE WHEN "aantal_inwoners" > 0 
              THEN (${v}::DOUBLE / "aantal_inwoners") * 1000.0
              ELSE NULL END`;
    }
    return v;  // none
  }

  // ── Register a parquet file with DuckDB ───────────────────
  async function registerFile(filename: string) {
    const url = new URL(`/data/${filename}`, window.location.origin).href;
    await db!.registerFileURL(filename, url, duckdb.DuckDBDataProtocol.HTTP, false);
  }

  // ── Query one scale layer ─────────────────────────────────
  async function queryLayer(
    scaleKey: string,
    scales: typeof INNER_SCALES | typeof OUTER_SCALES,
    varKey: string,
    norm: string
  ): Promise<{ geojson: any; breaks: number[] }> {

    const scale = scales.find(s => s.key === scaleKey)!;
    await registerFile(scale.file);

    // Check if variable exists in this file
    const colsResult = await conn.query(
      `DESCRIBE SELECT * FROM read_parquet('${scale.file}') LIMIT 0`
    );
    const cols = colsResult.toArray().map((r: any) => r.toJSON().column_name as string);

    const varExists    = cols.includes(varKey);
    const areaExists   = cols.includes('oppervlakte_land_in_ha');
    const popExists    = cols.includes('aantal_inwoners');

    // Fall back to raw if normalisation columns missing
    const canNorm = (norm === 'per_km2' && areaExists) ||
                    (norm === 'per_1000' && popExists) ||
                    norm === 'none';
    const effectiveNorm = canNorm ? norm : 'none';

    if (!varExists) {
      return { geojson: null, breaks: [] };
    }

    const expr = normExpr(varKey, effectiveNorm);

    const result = await conn.query(`
      SELECT geometry_wkt,
             "${scale.idCol}" as id,
             ${expr} as value
      FROM   read_parquet('${scale.file}')
      WHERE  "${varKey}" > -99990
    `);

    const rows = result.toArray().map((r: any) => r.toJSON());
    const values = rows
      .map((r: any) => Number(r.value))
      .filter((v: number) => isFinite(v) && v !== null);

    const breaks = quantileBreaks(values, 4);

    const geojson = {
      type: 'FeatureCollection',
      features: rows
        .map((row: any) => {
          const geom = parseWKT(row.geometry_wkt);
          if (!geom) return null;
          return {
            type: 'Feature',
            geometry: geom,
            properties: {
              id:    row.id,
              value: row.value,
              class: classifyValue(Number(row.value), breaks),
            },
          };
        })
        .filter(Boolean),
    };

    return { geojson, breaks };
  }

  // ── Load both layers ──────────────────────────────────────
  async function loadLayers() {
    if (!conn) return;
    loading = true;
    error = null;
    try {
      const [inner, outer] = await Promise.all([
        queryLayer(innerScale, INNER_SCALES, selectedVar, normalisation),
        queryLayer(outerScale, OUTER_SCALES, selectedVar, normalisation),
      ]);
      innerGeojson = inner.geojson;
      innerBreaks  = inner.breaks;
      outerGeojson = outer.geojson;
      outerBreaks  = outer.breaks;
    } catch (e) {
      error = `Query failed: ${e}`;
    }
    loading = false;
  }

  // ── DuckDB initialisation ─────────────────────────────────
  $effect(() => {
    if (!browser) return;
    async function init() {
      try {
        const bundles = duckdb.getJsDelivrBundles();
        const bundle  = await duckdb.selectBundle(bundles);
        const workerUrl = URL.createObjectURL(
          new Blob([`importScripts("${bundle.mainWorker}");`], { type: 'text/javascript' })
        );
        const worker = new Worker(workerUrl);
        db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);
        await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
        URL.revokeObjectURL(workerUrl);
        conn = await db.connect();
        await loadLayers();
      } catch (e) {
        error = `DuckDB init failed: ${e}`;
        loading = false;
      }
    }
    init();
  });

  // ── Reload when any control changes ──────────────────────
  $effect(() => {
    const _ = [innerScale, outerScale, selectedVar, normalisation];
    if (conn) loadLayers();
  });

  // ── MapLibre fill-color expression from breaks ────────────
  function colourExpression(breaks: number[]): any {
  if (!breaks.length) return NO_DATA_COLOUR;
  return [
    'match', ['get', 'class'],
    'nodata', NO_DATA_COLOUR,
    '0', COLOURS[0],
    '1', COLOURS[1],
    '2', COLOURS[2],
    '3', COLOURS[3],
    COLOURS[0],
  ] as any;
}

  // ── Current variable meta ─────────────────────────────────
  const currentVar = $derived(VARIABLES.find(v => v.key === selectedVar)!);
</script>

<!-- ── Controls ───────────────────────────────────────────── -->
<div class="controls">
  <div class="control-group">
    <span class="group-label">Inner scale (Rotterdam area)</span>
    <div class="button-row">
      {#each INNER_SCALES as s}
        <button
          class:active={innerScale === s.key}
          onclick={() => innerScale = s.key}
        >{s.label}</button>
      {/each}
    </div>
  </div>

  <div class="control-group">
    <span class="group-label">Outer scale (context)</span>
    <div class="button-row">
      {#each OUTER_SCALES as s}
        <button
          class:active={outerScale === s.key}
          onclick={() => outerScale = s.key}
        >{s.label}</button>
      {/each}
    </div>
  </div>

  <div class="control-group">
    <label for="var-select" class="group-label">Variable</label>
    <select id="var-select" bind:value={selectedVar}>
      {#each VARIABLES as v}
        <option value={v.key}>{v.label}</option>
      {/each}
    </select>
  </div>

  <div class="control-group">
    <span class="group-label">Normalise</span>
    <div class="button-row">
      {#each NORMALISATIONS as n}
        <button
          class:active={normalisation === n.key}
          class:disabled={n.key !== 'none' && !currentVar?.canNormalise}
          onclick={() => { if (n.key === 'none' || currentVar?.canNormalise) normalisation = n.key; }}
        >{n.label}</button>
      {/each}
    </div>
  </div>

  {#if loading}<div class="status">Loading…</div>{/if}
  {#if error}<div class="status error">{error}</div>{/if}
</div>

<!-- ── Legend ─────────────────────────────────────────────── -->
{#if innerBreaks.length}
  <div class="legend">
    <div class="legend-title">{currentVar?.label}</div>
    <div class="legend-row">
      <span class="swatch" style="background:{NO_DATA_COLOUR}"></span>
      No data
    </div>
    {#each COLOURS as colour, i}
      <div class="legend-row">
        <span class="swatch" style="background:{colour}"></span>
        {#if i === 0}
          ≤ {innerBreaks[0]?.toFixed(1)}
        {:else if i === COLOURS.length - 1}
          > {innerBreaks[innerBreaks.length - 1]?.toFixed(1)}
        {:else}
          ≤ {innerBreaks[i]?.toFixed(1)}
        {/if}
      </div>
    {/each}
  </div>
{/if}

<!-- ── Map ────────────────────────────────────────────────── -->
<MapLibre
  style="https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json"
  zoom={10}
  center={{ lng: 4.48, lat: 51.92 }}
>
  {#if outerGeojson}
    <GeoJSONSource id="outer-layer" data={outerGeojson}>
      <FillLayer
        id="outer-fill"
        paint={{
          'fill-color': colourExpression(outerBreaks),
          'fill-opacity': 0.6,
        }}
      />
      <LineLayer
        id="outer-outline"
        paint={{ 'line-color': '#ffffff', 'line-width': 0.5 }}
      />
    </GeoJSONSource>
  {/if}

  {#if innerGeojson}
    <GeoJSONSource id="inner-layer" data={innerGeojson}>
      <FillLayer
        id="inner-fill"
        paint={{
          'fill-color': colourExpression(innerBreaks),
          'fill-opacity': 0.85,
        }}
      />
      <LineLayer
        id="inner-outline"
        paint={{ 'line-color': '#ffffff', 'line-width': 0.2 }}
      />
    </GeoJSONSource>
  {/if}
</MapLibre>

<style>
  :global(.maplibregl-map) {
    height: 100vh;
    width: 100vw;
  }

  .controls {
    position: absolute;
    top: 1rem;
    left: 1rem;
    z-index: 10;
    background: white;
    padding: 1rem;
    border-radius: 8px;
    box-shadow: 0 2px 10px rgba(0,0,0,0.2);
    font-family: sans-serif;
    font-size: 0.85rem;
    max-width: 320px;
    display: flex;
    flex-direction: column;
    gap: 0.75rem;
  }

  .control-group {
    display: flex;
    flex-direction: column;
    gap: 0.3rem;
  }

  .button-row {
    display: flex;
    gap: 0.3rem;
    flex-wrap: wrap;
  }

  button {
    padding: 0.25rem 0.6rem;
    border: 1px solid #ccc;
    border-radius: 4px;
    background: white;
    cursor: pointer;
    font-size: 0.82rem;
    transition: all 0.15s;
  }

  button:hover { background: #f0f0f0; }
  button.active { background: #045a8d; color: white; border-color: #045a8d; }
  button.disabled { opacity: 0.4; cursor: not-allowed; }

  select {
    padding: 0.3rem;
    border: 1px solid #ccc;
    border-radius: 4px;
    font-size: 0.82rem;
    width: 100%;
  }

  .status { font-size: 0.8rem; color: #888; }
  .error  { color: #c00; }

  .legend {
    position: absolute;
    bottom: 2rem;
    left: 1rem;
    z-index: 10;
    background: white;
    padding: 0.75rem 1rem;
    border-radius: 8px;
    box-shadow: 0 2px 8px rgba(0,0,0,0.15);
    font-family: sans-serif;
    font-size: 0.82rem;
    min-width: 160px;
  }

  .legend-title {
    font-weight: bold;
    margin-bottom: 0.5rem;
    font-size: 0.85rem;
  }

  .legend-row {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    margin: 0.2rem 0;
  }

  .swatch {
    width: 14px;
    height: 14px;
    border-radius: 2px;
    border: 1px solid #ccc;
    flex-shrink: 0;
  }

  .group-label {
  font-weight: 600;
  font-size: 0.78rem;
  text-transform: uppercase;
  letter-spacing: 0.04em;
  color: #555;
}
</style>
