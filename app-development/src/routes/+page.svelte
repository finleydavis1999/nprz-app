<script lang="ts">
  import maplibregl from 'maplibre-gl';
  import 'maplibre-gl/dist/maplibre-gl.css';
  import { browser } from '$app/environment';
  import * as duckdb from '@duckdb/duckdb-wasm';

  // ── Dataset configuration ─────────────────────────────────
  // Each scale has a GeoJSON file (geometry only) and a stats parquet
  // The id field links them — MapLibre uses it as the feature identifier

  const SCALES = {
    inner: [
      { key: '100m',  label: '100m grid',  geojson: 'grid_100m_rijnmond.geojson', stats: 'grid_100m_rijnmond_stats.parquet', id: 'crs28992res100m', type: 'point', pointSize: 60 },
      { key: '500m',  label: '500m grid',  geojson: 'grid_500m_rijnmond.geojson', stats: 'grid_500m_rijnmond_stats.parquet', id: 'crs28992res500m', type: 'point', pointSize: 300 },
      { key: 'buurt', label: 'Buurt',       geojson: 'buurt_2024.geojson',          stats: 'buurt_2024_stats.parquet',          id: 'buurtcode',       type: 'polygon' },
    ],
    outer: [
      { key: 'pc4',      label: 'PC4',      geojson: 'pc4_zh_2024.geojson',   stats: 'pc4_zh_2024_stats.parquet',   id: 'postcode',    type: 'polygon' },
      { key: 'wijk',     label: 'Wijk',     geojson: 'wijk_2024.geojson',     stats: 'wijk_2024_stats.parquet',     id: 'wijkcode',    type: 'polygon' },
      { key: 'gemeente', label: 'Gemeente', geojson: 'gemeente_2024.geojson', stats: 'gemeente_2024_stats.parquet', id: 'gemeentecode', type: 'polygon' },
    ]
  };

  const DISPLAY_MODES = [
  { key: 'both',  label: 'Inner + Outer' },
  { key: 'inner', label: 'Inner only'    },
  { key: 'outer', label: 'Outer only'    },
] as const;

  // ── Variable definitions ──────────────────────────────────
  // availableAt controls which scale types show this variable
  // canNormalise flags whether per-km2 / per-1000 make sense

  const VARIABLES = [
    // Population
    { key: 'aantal_inwoners',                 label: 'Total population',          group: 'Population', canNormalise: true,  availableAt: ['100m','500m','buurt','wijk','gemeente','pc4'] },
    { key: 'aantal_mannen',                   label: 'Men',                       group: 'Population', canNormalise: true,  availableAt: ['100m','500m','buurt','wijk','gemeente','pc4'] },
    { key: 'aantal_vrouwen',                  label: 'Women',                     group: 'Population', canNormalise: true,  availableAt: ['100m','500m','buurt','wijk','gemeente','pc4'] },
    { key: 'aantal_inwoners_0_tot_15_jaar',   label: 'Age 0–15',                  group: 'Population', canNormalise: true,  availableAt: ['100m','500m','buurt','wijk','gemeente','pc4'] },
    { key: 'aantal_inwoners_15_tot_25_jaar',  label: 'Age 15–25',                 group: 'Population', canNormalise: true,  availableAt: ['100m','500m','buurt','wijk','gemeente','pc4'] },
    { key: 'aantal_inwoners_25_tot_45_jaar',  label: 'Age 25–45',                 group: 'Population', canNormalise: true,  availableAt: ['100m','500m','buurt','wijk','gemeente','pc4'] },
    { key: 'aantal_inwoners_45_tot_65_jaar',  label: 'Age 45–65',                 group: 'Population', canNormalise: true,  availableAt: ['100m','500m','buurt','wijk','gemeente','pc4'] },
    { key: 'aantal_inwoners_65_jaar_en_ouder',label: 'Age 65+',                   group: 'Population', canNormalise: true,  availableAt: ['100m','500m','buurt','wijk','gemeente','pc4'] },
    // Origin
    { key: 'percentage_geb_nederland_herkomst_nederland',          label: '% Dutch origin',             group: 'Origin', canNormalise: false, availableAt: ['100m','500m','buurt','wijk','gemeente','pc4'] },
    { key: 'percentage_geb_nederland_herkomst_overig_europa',      label: '% European (NL-born)',        group: 'Origin', canNormalise: false, availableAt: ['100m','500m','buurt','wijk','gemeente','pc4'] },
    { key: 'percentage_geb_nederland_herkomst_buiten_europa',      label: '% Non-European (NL-born)',    group: 'Origin', canNormalise: false, availableAt: ['100m','500m','buurt','wijk','gemeente','pc4'] },
    { key: 'percentage_geb_buiten_nederland_herkomst_europa',      label: '% European (foreign-born)',   group: 'Origin', canNormalise: false, availableAt: ['100m','500m','buurt','wijk','gemeente','pc4'] },
    { key: 'percentage_geb_buiten_nederland_herkmst_buiten_europa',label: '% Non-European (foreign-born)',group: 'Origin', canNormalise: false, availableAt: ['100m','500m','buurt','wijk','gemeente','pc4'] },
    // Households
    { key: 'aantal_part_huishoudens',                label: 'Total households',          group: 'Households', canNormalise: true,  availableAt: ['100m','500m','buurt','wijk','gemeente','pc4'] },
    { key: 'aantal_eenpersoonshuishoudens',          label: 'Single-person households',  group: 'Households', canNormalise: true,  availableAt: ['100m','500m','buurt','wijk','gemeente','pc4'] },
    { key: 'gemiddelde_huishoudensgrootte',          label: 'Avg household size',        group: 'Households', canNormalise: false, availableAt: ['100m','500m','buurt','wijk','gemeente','pc4'] },
    // Housing
    { key: 'aantal_woningen',                        label: 'Total dwellings',           group: 'Housing', canNormalise: true,  availableAt: ['100m','500m','buurt','wijk','gemeente','pc4'] },
    { key: 'percentage_koopwoningen',                label: '% Owner-occupied',          group: 'Housing', canNormalise: false, availableAt: ['100m','500m','buurt','wijk','gemeente','pc4'] },
    { key: 'percentage_huurwoningen',                label: '% Rental',                  group: 'Housing', canNormalise: false, availableAt: ['100m','500m','buurt','wijk','gemeente','pc4'] },
    { key: 'aantal_huurwoningen_in_bezit_woningcorporaties', label: 'Social housing units', group: 'Housing', canNormalise: true, availableAt: ['100m','500m','buurt','wijk','gemeente','pc4'] },
    { key: 'gemiddelde_woningwaarde',                label: 'Avg property value (WOZ)',  group: 'Housing', canNormalise: false, availableAt: ['buurt','wijk','gemeente','pc4'] },
    // Income & benefits (admin scales only — not in 100m grid)
    { key: 'aantal_personen_met_uitkering_onder_aowlft', label: 'Persons on benefits', group: 'Income', canNormalise: true, availableAt: ['100m','500m','buurt','wijk','gemeente','pc4'] },
    { key: 'gemiddeld_inkomen_per_inwoner',          label: 'Avg income per resident',   group: 'Income', canNormalise: false, availableAt: ['buurt','wijk','gemeente'] },
    // Area
    { key: 'oppervlakte_land_in_ha',                 label: 'Land area (ha)',            group: 'Area', canNormalise: false, availableAt: ['buurt','wijk','gemeente','pc4'] },
  ];

  const NORMALISATIONS = [
    { key: 'none',     label: 'Raw value' },
    { key: 'per_km2',  label: 'Per km²' },
    { key: 'per_1000', label: 'Per 1,000 residents' },
  ];

  const COLOURS   = ['#f1eef6', '#bdc9e1', '#74a9cf', '#045a8d'];
  const NO_DATA   = '#d0d0d0';

  // ── App state ─────────────────────────────────────────────
  type DisplayMode = 'both' | 'inner' | 'outer';

  let displayMode   = $state<DisplayMode>('both');
  let innerScaleKey = $state('buurt');
  let outerScaleKey = $state('gemeente');
  let sharedVar     = $state(true);          // same variable on both layers
  let innerVarKey   = $state('aantal_inwoners');
  let outerVarKey   = $state('aantal_inwoners');
  let normalisation = $state('none');
  let loading       = $state(false);
  let statusMsg     = $state('Initialising…');
  let error         = $state<string | null>(null);
  let innerBreaks   = $state<number[]>([]);
  let outerBreaks   = $state<number[]>([]);

  // ── Derived helpers ───────────────────────────────────────
  const innerScale   = $derived(SCALES.inner.find(s => s.key === innerScaleKey)!);
  const outerScale   = $derived(SCALES.outer.find(s => s.key === outerScaleKey)!);
  const innerVar     = $derived(VARIABLES.find(v => v.key === innerVarKey)!);
  const outerVar     = $derived(VARIABLES.find(v => v.key === outerVarKey)!);

  // Variables available at a given scale
  function varsForScale(scaleKey: string) {
    return VARIABLES.filter(v => v.availableAt.includes(scaleKey));
  }

  // When inner scale changes, ensure selected variable is available there
  $effect(() => {
    const available = varsForScale(innerScaleKey).map(v => v.key);
    if (!available.includes(innerVarKey)) innerVarKey = available[0];
  });

  $effect(() => {
    const available = varsForScale(outerScaleKey).map(v => v.key);
    if (!available.includes(outerVarKey)) outerVarKey = available[0];
  });

  // Sync outer variable to inner when sharedVar is on
  $effect(() => {
    if (sharedVar) outerVarKey = innerVarKey;
  });

  // ── MapLibre & DuckDB state ───────────────────────────────
  let map: maplibregl.Map | null = null;
  let mapReady = $state(false);
  let db: duckdb.AsyncDuckDB | null = null;
  let conn: any = null;
  let dbReady = $state(false);
  let registeredStats = new Set<string>();

  // ── Quantile classification ───────────────────────────────
  function quantileBreaks(values: number[], n = 4): number[] {
    const sorted = [...values].filter(v => isFinite(v) && v > -99990)
                              .sort((a, b) => a - b);
    if (sorted.length < n) return [];
    return Array.from({ length: n - 1 }, (_, i) => {
      const idx = Math.floor(((i + 1) / n) * sorted.length);
      return sorted[Math.min(idx, sorted.length - 1)];
    });
  }

  function classify(value: number, breaks: number[]): number {
    if (value === null || value === undefined || value <= -99990 || !isFinite(value)) return -1;
    for (let i = 0; i < breaks.length; i++) {
      if (value <= breaks[i]) return i;
    }
    return breaks.length;
  }

  // ── SQL normalisation expression ──────────────────────────
  function normSQL(col: string, norm: string): string {
    if (norm === 'per_km2') {
      return `CASE WHEN oppervlakte_land_in_ha > 0 THEN "${col}" / (oppervlakte_land_in_ha / 100.0) ELSE NULL END`;
    }
    if (norm === 'per_1000') {
      return `CASE WHEN aantal_inwoners > 0 THEN ("${col}"::DOUBLE / aantal_inwoners) * 1000.0 ELSE NULL END`;
    }
    return `"${col}"`;
  }

  // ── Register a stats parquet file with DuckDB ─────────────
  async function ensureStats(filename: string) {
    if (registeredStats.has(filename)) return;
    const url = new URL(`/data/${filename}`, window.location.origin).href;
    await db!.registerFileURL(filename, url, duckdb.DuckDBDataProtocol.HTTP, false);
    registeredStats.add(filename);
  }

  // ── Query stats and apply feature state to map ────────────
  async function applyLayer(
    scale: typeof SCALES.inner[0],
    varKey: string,
    norm: string,
    layerId: string
  ): Promise<number[]> {

    await ensureStats(scale.stats);

    // Check columns available in this stats file
    const colsResult = await conn.query(
      `DESCRIBE SELECT * FROM read_parquet('${scale.stats}') LIMIT 0`
    );
    const cols = colsResult.toArray().map((r: any) => r.toJSON().column_name as string);

    if (!cols.includes(varKey)) {
      // Variable not available at this scale — clear feature state
      statusMsg = `"${varKey}" not available at ${scale.label} scale`;
      return [];
    }

    const expr   = normSQL(varKey, norm);
    const result = await conn.query(`
      SELECT "${scale.id}" as id, ${expr} as value
      FROM   read_parquet('${scale.stats}')
    `);

    const rows   = result.toArray().map((r: any) => r.toJSON());
    const values = rows.map((r: any) => Number(r.value)).filter((v: number) => isFinite(v) && v > -99990);
    const breaks = quantileBreaks(values);

    // Apply feature state — this updates colours without reloading geometry
    for (const row of rows) {
      const cls = classify(Number(row.value), breaks);
      map!.setFeatureState(
        { source: layerId, id: String(row.id) },
        { value: row.value, cls }
      );
    }

    return breaks;
  }

  // ── Load both active layers ───────────────────────────────
  async function loadLayers() {
  if (!mapReady || !dbReady) return;
  loading  = true;
  error    = null;
  statusMsg = 'Loading data…';

  try {
    const tasks: Promise<number[]>[] = [];

    if (displayMode === 'inner' || displayMode === 'both') {
      tasks.push(
        applyLayer(innerScale, innerVarKey, normalisation, `${innerScaleKey}-source`)
          .then(b => { innerBreaks = b; return b; })
      );
    }

    if (displayMode === 'outer' || displayMode === 'both') {
      const effectiveOuterVar = sharedVar ? innerVarKey : outerVarKey;
      tasks.push(
        applyLayer(outerScale, effectiveOuterVar, normalisation, `${outerScaleKey}-source`)
          .then(b => { outerBreaks = b; return b; })
      );
    }

    await Promise.all(tasks);
    statusMsg = '';
  } catch (e) {
    error = `Error: ${e}`;
  }

  loading = false;
}

  // ── Initialise MapLibre ───────────────────────────────────
  function initMap(container: HTMLDivElement) {
    map = new maplibregl.Map({
      container,
      style: 'https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json',
      center: [4.48, 51.92],
      zoom: 10,
    });

    map.on('load', async () => {
      // Add all GeoJSON sources upfront — geometry loads once
      // promoteId tells MapLibre to use our ID field as the feature identifier
      // enabling setFeatureState to work

      for (const scale of [...SCALES.inner, ...SCALES.outer]) {
        const url = new URL(`/data/${scale.geojson}`, window.location.origin).href;
        map!.addSource(`${scale.key}-source`, {
          type: 'geojson',
          data: url,
          promoteId: scale.id,
        });
      }

      // Add Rijnmond boundary source
      map!.addSource('boundary-source', {
  type: 'geojson',
  data: new URL('/data/rotterdam_boundary.geojson', window.location.origin).href,
});

      // ── Inner layers ────────────────────────────────────
      for (const scale of SCALES.inner) {
        if (scale.type === 'point') {
          // Grid files are points — render as circles
          // Circle radius in pixels approximates the grid cell size
          map!.addLayer({
            id:     `${scale.key}-fill`,
            type:   'circle',
            source: `${scale.key}-source`,
            layout: { visibility: 'none' },
            paint: {
              'circle-radius': scale.key === '100m' ? 4 : 12,
              'circle-color': [
                'case',
                ['==', ['feature-state', 'cls'], -1], NO_DATA,
                ['==', ['feature-state', 'cls'], 0],  COLOURS[0],
                ['==', ['feature-state', 'cls'], 1],  COLOURS[1],
                ['==', ['feature-state', 'cls'], 2],  COLOURS[2],
                ['==', ['feature-state', 'cls'], 3],  COLOURS[3],
                NO_DATA
              ],
              'circle-opacity': 0.85,
              'circle-stroke-width': 0.3,
              'circle-stroke-color': '#ffffff',
            }
          });
        } else {
          // Buurt — polygon fill
          map!.addLayer({
            id:     `${scale.key}-fill`,
            type:   'fill',
            source: `${scale.key}-source`,
            layout: { visibility: 'none' },
            paint: {
              'fill-color': [
                'case',
                ['==', ['feature-state', 'cls'], -1], NO_DATA,
                ['==', ['feature-state', 'cls'], 0],  COLOURS[0],
                ['==', ['feature-state', 'cls'], 1],  COLOURS[1],
                ['==', ['feature-state', 'cls'], 2],  COLOURS[2],
                ['==', ['feature-state', 'cls'], 3],  COLOURS[3],
                NO_DATA
              ],
              'fill-opacity': 0.85,
            }
          });
          map!.addLayer({
            id: `${scale.key}-outline`, type: 'line',
            source: `${scale.key}-source`,
            layout: { visibility: 'none' },
            paint: { 'line-color': '#ffffff', 'line-width': 0.4 }
          });
        }
      }

      // ── Outer layers ────────────────────────────────────
      for (const scale of SCALES.outer) {
        map!.addLayer({
          id:     `${scale.key}-fill`,
          type:   'fill',
          source: `${scale.key}-source`,
          layout: { visibility: 'none' },
          paint: {
            'fill-color': [
              'case',
              ['==', ['feature-state', 'cls'], -1], NO_DATA,
              ['==', ['feature-state', 'cls'], 0],  COLOURS[0],
              ['==', ['feature-state', 'cls'], 1],  COLOURS[1],
              ['==', ['feature-state', 'cls'], 2],  COLOURS[2],
              ['==', ['feature-state', 'cls'], 3],  COLOURS[3],
              NO_DATA
            ],
            'fill-opacity': 0.6,
          }
        });
        map!.addLayer({
          id: `${scale.key}-outline`, type: 'line',
          source: `${scale.key}-source`,
          layout: { visibility: 'none' },
          paint: { 'line-color': '#ffffff', 'line-width': 0.5 }
        });
      }

      // ── Rijnmond boundary line ───────────────────────────
      map!.addLayer({
        id: 'boundary-line', type: 'line',
        source: 'boundary-source',
        paint: {
          'line-color': '#e63946',
          'line-width': 2,
          'line-dasharray': [4, 2],
        }
      });

      mapReady = true;
      updateVisibleLayers();
      if (dbReady) loadLayers();
    });
  }

  // ── Show/hide layers based on display mode and scale ─────
  function updateVisibleLayers() {
    if (!map || !mapReady) return;

    const showInner = displayMode === 'inner' || displayMode === 'both';
    const showOuter = displayMode === 'outer' || displayMode === 'both';

    for (const scale of SCALES.inner) {
      const vis = showInner && scale.key === innerScaleKey ? 'visible' : 'none';
      if (map.getLayer(`${scale.key}-fill`))    map.setLayoutProperty(`${scale.key}-fill`,    'visibility', vis);
      if (map.getLayer(`${scale.key}-outline`)) map.setLayoutProperty(`${scale.key}-outline`, 'visibility', vis);
    }

    for (const scale of SCALES.outer) {
      const vis = showOuter && scale.key === outerScaleKey ? 'visible' : 'none';
      if (map.getLayer(`${scale.key}-fill`))    map.setLayoutProperty(`${scale.key}-fill`,    'visibility', vis);
      if (map.getLayer(`${scale.key}-outline`)) map.setLayoutProperty(`${scale.key}-outline`, 'visibility', vis);
    }
  }

  // ── Initialise DuckDB ─────────────────────────────────────
  async function initDuckDB() {
    const bundles  = duckdb.getJsDelivrBundles();
    const bundle   = await duckdb.selectBundle(bundles);
    const workerUrl = URL.createObjectURL(
      new Blob([`importScripts("${bundle.mainWorker}");`], { type: 'text/javascript' })
    );
    const worker = new Worker(workerUrl);
    db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    URL.revokeObjectURL(workerUrl);
    conn   = await db.connect();
    dbReady = true;
    if (mapReady) loadLayers();
  }

  // ── Bootstrap ─────────────────────────────────────────────
  $effect(() => {
    if (!browser) return;
    initDuckDB().catch(e => { error = `DuckDB failed: ${e}`; });
  });

  // ── React to control changes ──────────────────────────────
  $effect(() => {
    const _ = [displayMode, innerScaleKey, outerScaleKey,
                innerVarKey, outerVarKey, sharedVar, normalisation];
    updateVisibleLayers();
    loadLayers();
  });

  // ── Legend helper ─────────────────────────────────────────
  function formatBreak(v: number): string {
    return v >= 1000 ? `${(v/1000).toFixed(1)}k` : v.toFixed(1);
  }
</script>

<!-- ── Map container ──────────────────────────────────────── -->
<div use:initMap class="map-container"></div>

<!-- ── Control panel ──────────────────────────────────────── -->
<div class="panel">
  <div class="panel-title">NPRZ Spatial Explorer</div>

  <!-- Display mode -->
    <div class="control-group">
    <span class="group-label">Display</span>
    <div class="btn-row">
      {#each DISPLAY_MODES as mode}
        <button
          class:active={displayMode === mode.key}
          onclick={() => displayMode = mode.key as DisplayMode}
        >{mode.label}</button>
      {/each}
    </div>
  </div>
 

  <!-- Inner scale selector -->
  {#if displayMode === 'inner' || displayMode === 'both'}
    <div class="control-group">
      <span class="group-label">Inner scale</span>
      <div class="btn-row">
        {#each SCALES.inner as s}
          <button class:active={innerScaleKey === s.key}
                  onclick={() => innerScaleKey = s.key}>
            {s.label}
          </button>
        {/each}
      </div>
    </div>
  {/if}

  <!-- Outer scale selector -->
  {#if displayMode === 'outer' || displayMode === 'both'}
    <div class="control-group">
      <span class="group-label">Outer scale</span>
      <div class="btn-row">
        {#each SCALES.outer as s}
          <button class:active={outerScaleKey === s.key}
                  onclick={() => outerScaleKey = s.key}>
            {s.label}
          </button>
        {/each}
      </div>
    </div>
  {/if}

  <!-- Variable — shared or split -->
  {#if displayMode === 'both'}
    <div class="control-group">
      <label class="toggle-row">
        <input type="checkbox" bind:checked={sharedVar} />
        <span class="group-label">Same variable on both layers</span>
      </label>
    </div>
  {/if}

  <!-- Inner variable -->
  {#if displayMode === 'inner' || displayMode === 'both'}
    <div class="control-group">
      <label for="inner-var" class="group-label">
        {displayMode === 'both' && !sharedVar ? 'Inner variable' : 'Variable'}
      </label>
      <select id="inner-var" bind:value={innerVarKey}>
        {#each Object.entries(Object.groupBy(varsForScale(innerScaleKey), v => v.group)) as [group, vars]}
          <optgroup label={group}>
            {#each vars! as v}
              <option value={v.key}>{v.label}</option>
            {/each}
          </optgroup>
        {/each}
      </select>
    </div>
  {/if}

  <!-- Outer variable (only if split mode) -->
  {#if displayMode === 'both' && !sharedVar}
    <div class="control-group">
      <label for="outer-var" class="group-label">Outer variable</label>
      <select id="outer-var" bind:value={outerVarKey}>
        {#each Object.entries(Object.groupBy(varsForScale(outerScaleKey), v => v.group)) as [group, vars]}
          <optgroup label={group}>
            {#each vars! as v}
              <option value={v.key}>{v.label}</option>
            {/each}
          </optgroup>
        {/each}
      </select>
    </div>
  {/if}

  <!-- Outer variable (outer only mode) -->
  {#if displayMode === 'outer'}
    <div class="control-group">
      <label for="outer-var-only" class="group-label">Variable</label>
      <select id="outer-var-only" bind:value={outerVarKey}>
        {#each Object.entries(Object.groupBy(varsForScale(outerScaleKey), v => v.group)) as [group, vars]}
          <optgroup label={group}>
            {#each vars! as v}
              <option value={v.key}>{v.label}</option>
            {/each}
          </optgroup>
        {/each}
      </select>
    </div>
  {/if}

  <!-- Normalisation -->
  <div class="control-group">
    <span class="group-label">Normalise</span>
    <div class="btn-row">
      {#each NORMALISATIONS as n}
        {@const disabled = n.key !== 'none' && !(innerVar?.canNormalise)}
        <button
          class:active={normalisation === n.key}
          class:disabled
          onclick={() => { if (!disabled) normalisation = n.key; }}
        >{n.label}</button>
      {/each}
    </div>
  </div>

  <!-- Status -->
  {#if loading}
    <div class="status">Loading…</div>
  {:else if statusMsg}
    <div class="status">{statusMsg}</div>
  {/if}
  {#if error}
    <div class="status error">{error}</div>
  {/if}
</div>

<!-- ── Legend ─────────────────────────────────────────────── -->
{#if innerBreaks.length || outerBreaks.length}
  {@const breaks = innerBreaks.length ? innerBreaks : outerBreaks}
  {@const varLabel = displayMode === 'outer'
    ? (VARIABLES.find(v => v.key === outerVarKey)?.label ?? '')
    : (VARIABLES.find(v => v.key === innerVarKey)?.label ?? '')}
  <div class="legend">
    <div class="legend-title">{varLabel}</div>
    <div class="legend-row">
      <span class="swatch" style="background:{NO_DATA}"></span>
      No data
    </div>
    {#each COLOURS as colour, i}
      <div class="legend-row">
        <span class="swatch" style="background:{colour}"></span>
        {#if i === 0}
          ≤ {formatBreak(breaks[0] ?? 0)}
        {:else if i === COLOURS.length - 1}
          > {formatBreak(breaks[breaks.length - 1] ?? 0)}
        {:else}
          ≤ {formatBreak(breaks[i] ?? 0)}
        {/if}
      </div>
    {/each}
  </div>
{/if}

<style>
  .map-container {
  position: fixed;
  inset: 0;
  width: 100vw;
  height: 100vh;
  pointer-events: none;
}

:global(.maplibregl-map) {
  pointer-events: all;
}

.panel {
  position: fixed;
  top: 1rem;
  left: 1rem;
  z-index: 1000;
  pointer-events: all;
  background: white;
  padding: 1rem;
  border-radius: 10px;
  box-shadow: 0 2px 12px rgba(0,0,0,0.18);
  font-family: sans-serif;
  font-size: 0.83rem;
  width: 280px;
  display: flex;
  flex-direction: column;
  gap: 0.8rem;
}

.legend {
  position: fixed;
  bottom: 2rem;
  left: 1rem;
  z-index: 1000;
  pointer-events: all;
  background: white;
  padding: 0.75rem 1rem;
  border-radius: 8px;
  box-shadow: 0 2px 8px rgba(0,0,0,0.15);
  font-family: sans-serif;
  font-size: 0.8rem;
  min-width: 150px;
}

  .panel-title {
    font-weight: 700;
    font-size: 0.95rem;
    color: #1a1a2e;
    border-bottom: 2px solid #045a8d;
    padding-bottom: 0.4rem;
  }

  .control-group {
    display: flex;
    flex-direction: column;
    gap: 0.3rem;
  }

  .group-label {
    font-weight: 600;
    font-size: 0.72rem;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    color: #666;
  }

  .btn-row {
    display: flex;
    gap: 0.3rem;
    flex-wrap: wrap;
  }

  button {
    padding: 0.22rem 0.55rem;
    border: 1px solid #ccc;
    border-radius: 4px;
    background: white;
    cursor: pointer;
    font-size: 0.8rem;
    transition: background 0.12s, border-color 0.12s;
  }

  button:hover:not(.disabled) { background: #f0f4f8; }
  button.active  { background: #045a8d; color: white; border-color: #045a8d; }
  button.disabled { opacity: 0.35; cursor: not-allowed; }

  select {
    width: 100%;
    padding: 0.3rem;
    border: 1px solid #ccc;
    border-radius: 4px;
    font-size: 0.8rem;
    background: white;
  }

  .toggle-row {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    cursor: pointer;
  }

  .status       { font-size: 0.78rem; color: #888; font-style: italic; }
  .error        { color: #c00; font-style: normal; }

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
    font-size: 0.8rem;
    min-width: 150px;
  }

  .legend-title {
    font-weight: 700;
    margin-bottom: 0.45rem;
    font-size: 0.82rem;
    color: #1a1a2e;
  }

  .legend-row {
    display: flex;
    align-items: center;
    gap: 0.45rem;
    margin: 0.18rem 0;
  }

  .swatch {
    width: 13px;
    height: 13px;
    border-radius: 2px;
    border: 1px solid #ddd;
    flex-shrink: 0;
  }
</style>
