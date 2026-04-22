<script lang="ts">
  import maplibregl from 'maplibre-gl';
  import 'maplibre-gl/dist/maplibre-gl.css';
  import { browser } from '$app/environment';
  import * as duckdb from '@duckdb/duckdb-wasm';

  import {
    INNER_SCALES, OUTER_SCALES, ALL_SCALES,
    VARIABLES, EDGE_DATASETS,
    NORMALISATIONS, DISPLAY_MODES,
    COLOURS, NO_DATA,
    varsForScale, findScale,
  } from '$lib/config';
  import type { DisplayMode, Normalisation } from '$lib/types';

  import type { FeatureCollection, Feature } from 'geojson';

  // ── App state ─────────────────────────────────────────────
  let displayMode   = $state<DisplayMode>('both');
  let innerScaleKey = $state('buurt');
  let outerScaleKey = $state('gemeente');
  let sharedVar     = $state(true);
  let innerVarKey   = $state('aantal_inwoners');
  let outerVarKey   = $state('aantal_inwoners');
  let normalisation = $state<Normalisation>('none');
  let loading       = $state(false);
  let statusMsg     = $state('Initialising…');
  let error         = $state<string | null>(null);
  let innerBreaks   = $state<number[]>([]);
  let outerBreaks   = $state<number[]>([]);
  let showFlows     = $state(false);
  let flowsLoaded   = $state(false);
  let flowsLoading  = $state(false);


  
  // ── Derived helpers ───────────────────────────────────────
  const innerScale = $derived(findScale(innerScaleKey)!);
  const outerScale = $derived(findScale(outerScaleKey)!);
  const innerVar   = $derived(VARIABLES.find(v => v.key === innerVarKey)!);
  const outerVar   = $derived(VARIABLES.find(v => v.key === outerVarKey)!);

  // When scale changes, ensure selected variable is available there
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
  $effect(() => {
  if (displayMode !== 'both') sharedVar = false;
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

  // ── Get full URL for stats parquet file ──────────────────
  function getStatsURL(filename: string): string {
    return new URL(`/data/${filename}`, window.location.origin).href;
  }

  // ── Query stats and apply feature state to map ────────────
async function applyLayer(
  scaleKey: string,
  varKey: string,
  norm: string,
  sourceId: string
): Promise<number[]> {
  const scale = findScale(scaleKey)!;

  const isFlowVar = varKey === 'total_outflow' || varKey === 'n_destinations';
  const isBanenVar = ['total_banen_werk', 'total_banen_woon', 'total_inwoners',
                      'ratio_banen_inwoners', 'ratio_werkenden_inwoners'].includes(varKey);

  let rows: any[];

  if (isFlowVar) {
    const edgeDataset = EDGE_DATASETS.find(e => e.scaleKey === scaleKey);
    if (!edgeDataset) return [];
    const flowUrl = getStatsURL(edgeDataset.flowSummary);
    const result = await conn.query(`
      SELECT origin_id AS id, "${varKey}" AS value
      FROM read_parquet('${flowUrl}')
    `);
    rows = result.toArray().map((r: any) => r.toJSON());

  } else if (isBanenVar) {
    const nodesUrl = getStatsURL('nodes_summary_gem.parquet');
    const result = await conn.query(`
      SELECT gemeentecode AS id, "${varKey}" AS value
      FROM read_parquet('${nodesUrl}')
      WHERE jaar = 2017
    `);
    rows = result.toArray().map((r: any) => r.toJSON());

  } else {
    const statsUrl = getStatsURL(scale.stats);
    const colsResult = await conn.query(
      `DESCRIBE SELECT * FROM read_parquet('${statsUrl}') LIMIT 0`
    );
    const cols = colsResult.toArray().map((r: any) => r.toJSON().column_name as string);
    if (!cols.includes(varKey)) {
      statusMsg = `"${varKey}" not available at ${scale.label} scale`;
      return [];
    }
    const expr = normSQL(varKey, norm);
    const result = await conn.query(`
      SELECT "${scale.id}" AS id, ${expr} AS value
      FROM read_parquet('${statsUrl}')
    `);
    rows = result.toArray().map((r: any) => r.toJSON());
  }

  const values = rows.map((r: any) => Number(r.value))
                     .filter((v: number) => isFinite(v) && v > -99990);
  const breaks = quantileBreaks(values);

  for (const row of rows) {
    const cls = classify(Number(row.value), breaks);
    map!.setFeatureState(
      { source: sourceId, id: String(row.id) },
      { value: row.value, cls }
    );
  }

  return breaks;
}

 // ── Load both active layers ───────────────────────────────
  async function loadLayers() {
    if (!mapReady || !dbReady) return;
    loading   = true;
    error     = null;
    statusMsg = 'Loading data…';

    try {
      const tasks: Promise<number[]>[] = [];

      if (displayMode === 'inner' || displayMode === 'both') {
        tasks.push(
          applyLayer(innerScaleKey, innerVarKey, normalisation, `${innerScaleKey}-source`)
            .then(b => { innerBreaks = b; return b; })
        );
      }

      if (displayMode === 'outer' || displayMode === 'both') {
        const effectiveOuterVar = (displayMode === 'both' && sharedVar) ? innerVarKey : outerVarKey;
        tasks.push(
          applyLayer(outerScaleKey, effectiveOuterVar, normalisation, `${outerScaleKey}-source`)
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

  // ── Flow period state ─────────────────────────────────────
  let flowPeriod = $state<'20072012' | '20122017'>('20122017');

  // ── Load flow lines ───────────────────────────────────────
  async function loadFlows() {
    if (!map || !mapReady || !dbReady) return;
    if (flowsLoading) return;
    flowsLoading = true;

    try {
      const centroidsUrl = new URL('/data/gemeente_centroids.json', window.location.origin).href;
      const centroidsRaw = await fetch(centroidsUrl).then(r => r.json());
      const centroids = new Map<string, [number, number]>(
        centroidsRaw.map((c: any) => [c.id, [c.lng, c.lat]])
      );

      const flowUrl = new URL('/data/edges_woonwerk_gem.parquet', window.location.origin).href;
      const result  = await conn.query(`
        SELECT origin_id, destination_id, flow_value
        FROM read_parquet('${flowUrl}')
        WHERE periode = '${flowPeriod}'
        ORDER BY flow_value DESC
        LIMIT 500
      `);
      const flows = result.toArray().map((r: any) => r.toJSON());

      const features: Feature[] = [];
      for (const flow of flows) {
        const origin = centroids.get(flow.origin_id);
        const dest   = centroids.get(flow.destination_id);
        if (!origin || !dest) continue;
        features.push({
          type: 'Feature',
          properties: {
            origin_id:      flow.origin_id,
            destination_id: flow.destination_id,
            flow_value:     flow.flow_value,
          },
          geometry: {
            type:        'LineString',
            coordinates: [origin, dest],
          },
        });
      }

      const geojson: FeatureCollection = { type: 'FeatureCollection', features };

      if (map.getSource('flows-source')) {
        (map.getSource('flows-source') as maplibregl.GeoJSONSource).setData(geojson);
      } else {
        map.addSource('flows-source', { type: 'geojson', data: geojson });
        map.addLayer({
          id:     'flows-layer',
          type:   'line',
          source: 'flows-source',
          layout: { 'line-join': 'round', 'line-cap': 'round' },
          paint: {
            'line-color':   '#e63946',
            'line-opacity': 0.5,
            'line-width': [
              'interpolate', ['linear'], ['get', 'flow_value'],
              500,   0.5,
              5000,  2,
              20000, 5,
            ],
          },
        });
      }

      flowsLoaded  = true;
      flowsLoading = false;

    } catch (e) {
      error        = `Flows error: ${e}`;
      flowsLoading = false;
    }
  }

  // ── Toggle flows visibility ───────────────────────────────
  function toggleFlows() {
    showFlows = !showFlows;
    if (showFlows) {
      loadFlows();
    } else if (map && map.getLayer('flows-layer')) {
      map.setLayoutProperty('flows-layer', 'visibility', 'none');
    }
  }

  // ── Switch flow period ────────────────────────────────────
  function switchPeriod(period: '20072012' | '20122017') {
    flowPeriod  = period;
    flowsLoaded = false;
    if (showFlows) loadFlows();
  }

  // ── Zoom map to fit the active scale boundary ─────────────
  function zoomToScale() {
    if (!map || !mapReady) return;
    // Bounds per scale key — defined from known extents
    const bounds: Record<string, maplibregl.LngLatBoundsLike> = {
      '100m':     [[4.35, 51.84], [4.62, 52.01]],  // Rotterdam core
      '500m':     [[4.35, 51.84], [4.62, 52.01]],  // Rotterdam core
      'buurt':    [[4.35, 51.84], [4.62, 52.01]],  // Rotterdam core
      'pc4':      [[3.80, 51.50], [5.40, 52.60]],  // Greater metro
      'wijk':     [[3.80, 51.50], [5.40, 52.60]],  // Greater metro
      'gemeente': [[3.30, 50.75], [7.23, 53.58]],  // Full NL
    };

    const activeKey = displayMode === 'inner' ? innerScaleKey
                    : displayMode === 'outer' ? outerScaleKey
                    : outerScaleKey; // 'both' — zoom to outer extent

    const bound = bounds[activeKey];
    if (bound) {
      map.fitBounds(bound, { padding: 40, duration: 800 });
    }
  }


  // ── Initialise MapLibre ───────────────────────────────────
  function initMap(container: HTMLDivElement) {
    map = new maplibregl.Map({
      container,
      style:  'https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json',
      center: [4.48, 51.92],
      zoom:   10,
      // Ensure map controls work — do not interfere with page scroll
      scrollZoom:    true,
      dragPan:       true,
      dragRotate:    false,
      touchZoomRotate: true,
    });

    // Scale bar bottom-right
    map.addControl(new maplibregl.ScaleControl({ unit: 'metric' }), 'bottom-right');

    // Navigation control (zoom buttons)
    map.addControl(new maplibregl.NavigationControl({ showCompass: false }), 'bottom-right');

    map.on('load', async () => {
      // Add all GeoJSON sources upfront
      for (const scale of ALL_SCALES) {
        const url = new URL(`/data/${scale.geojson}`, window.location.origin).href;
        map!.addSource(`${scale.key}-source`, {
          type:      'geojson',
          data:      url,
          promoteId: scale.id,
        });
      }

      // Rijnmond boundary
      map!.addSource('boundary-source', {
        type: 'geojson',
        data: new URL('/data/rotterdam_boundary.geojson', window.location.origin).href,
      });
      
      // ── Inner layers ──────────────────────────────────────
      for (const scale of INNER_SCALES) {
        if (scale.type === 'point') {
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
              'circle-opacity':       0.85,
              'circle-stroke-width':  0.3,
              'circle-stroke-color':  '#ffffff',
            },
          });
        } else {
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
            },
          });
          map!.addLayer({
            id:     `${scale.key}-outline`,
            type:   'line',
            source: `${scale.key}-source`,
            layout: { visibility: 'none' },
            paint:  { 'line-color': '#ffffff', 'line-width': 0.4 },
          });
        }
      }

      // ── Outer layers ──────────────────────────────────────
      for (const scale of OUTER_SCALES) {
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
          },
        });
        map!.addLayer({
          id:     `${scale.key}-outline`,
          type:   'line',
          source: `${scale.key}-source`,
          layout: { visibility: 'none' },
          paint:  { 'line-color': '#ffffff', 'line-width': 0.5 },
        });
      }

      // ── Boundary line ─────────────────────────────────────
      map!.addLayer({
        id:     'boundary-line',
        type:   'line',
        source: 'boundary-source',
        paint: {
          'line-color':     '#e63946',
          'line-width':     2,
          'line-dasharray': [4, 2],
        },
      });

      mapReady = true;
      updateVisibleLayers();
      if (dbReady) loadLayers();
    });
  }

  // ── Show/hide layers based on display mode ────────────────
  function updateVisibleLayers() {
    if (!map || !mapReady) return;

    const showInner = displayMode === 'inner' || displayMode === 'both';
    const showOuter = displayMode === 'outer' || displayMode === 'both';

    for (const scale of INNER_SCALES) {
      const vis = showInner && scale.key === innerScaleKey ? 'visible' : 'none';
      if (map.getLayer(`${scale.key}-fill`))    map.setLayoutProperty(`${scale.key}-fill`,    'visibility', vis);
      if (map.getLayer(`${scale.key}-outline`)) map.setLayoutProperty(`${scale.key}-outline`, 'visibility', vis);
    }

    for (const scale of OUTER_SCALES) {
      const vis = showOuter && scale.key === outerScaleKey ? 'visible' : 'none';
      if (map.getLayer(`${scale.key}-fill`))    map.setLayoutProperty(`${scale.key}-fill`,    'visibility', vis);
      if (map.getLayer(`${scale.key}-outline`)) map.setLayoutProperty(`${scale.key}-outline`, 'visibility', vis);
    }
  }

  // ── Initialise DuckDB ─────────────────────────────────────
  async function initDuckDB() {
    const bundles   = duckdb.getJsDelivrBundles();
    const bundle    = await duckdb.selectBundle(bundles);
    const workerUrl = URL.createObjectURL(
      new Blob([`importScripts("${bundle.mainWorker}");`], { type: 'text/javascript' })
    );
    const worker = new Worker(workerUrl);
    db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    URL.revokeObjectURL(workerUrl);
    conn    = await db.connect();
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
    // Only zoom when switching to single-layer modes, not when selecting both
$effect(() => {
  const _ = displayMode;
  if (displayMode !== 'both') zoomToScale();
});

  // ── Legend helper ─────────────────────────────────────────
  function formatBreak(v: number): string {
    return v >= 1000 ? `${(v / 1000).toFixed(1)}k` : v.toFixed(1);
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

  <!-- Inner scale -->
  {#if displayMode === 'inner' || displayMode === 'both'}
    <div class="control-group">
      <span class="group-label">Inner scale</span>
      <div class="btn-row">
        {#each INNER_SCALES as s}
          <button class:active={innerScaleKey === s.key}
                  onclick={() => innerScaleKey = s.key}>
            {s.label}
          </button>
        {/each}
      </div>
    </div>
  {/if}

  <!-- Outer scale -->
  {#if displayMode === 'outer' || displayMode === 'both'}
    <div class="control-group">
      <span class="group-label">Outer scale</span>
      <div class="btn-row">
        {#each OUTER_SCALES as s}
          <button class:active={outerScaleKey === s.key}
                  onclick={() => outerScaleKey = s.key}>
            {s.label}
          </button>
        {/each}
      </div>
    </div>
  {/if}

  <!-- Shared variable toggle -->
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

  <!-- Outer variable (split mode) -->
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

  <!-- Outer variable (outer only) -->
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
          onclick={() => { if (!disabled) normalisation = n.key as Normalisation; }}
        >{n.label}</button>
      {/each}
    </div>
  </div>

 <!-- Commuting flows (only for gemeente outer layer) -->
{#if outerScaleKey === 'gemeente' && (displayMode === 'outer' || displayMode === 'both')}
  <div class="control-group">
    <span class="group-label">Commuting flows</span>
    <div class="btn-row">
      <button
        class:active={showFlows}
        onclick={toggleFlows}
        disabled={flowsLoading}
      >
        {flowsLoading ? 'Loading...' : showFlows ? 'Hide flows' : 'Show flows'}
      </button>
    </div>
    {#if showFlows}
      <div class="btn-row">
        <button
          class:active={flowPeriod === '20072012'}
          onclick={() => switchPeriod('20072012')}
        >2007-2012</button>
        <button
          class:active={flowPeriod === '20122017'}
          onclick={() => switchPeriod('20122017')}
        >2012-2017</button>
      </div>
    {/if}
  </div>
{/if}

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
  {@const breaks   = innerBreaks.length ? innerBreaks : outerBreaks}
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
  pointer-events: all;  /* ← THIS is blocking drag */
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
  button.active   { background: #045a8d; color: white; border-color: #045a8d; }
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

  .status { font-size: 0.78rem; color: #888; font-style: italic; }
  .error  { color: #c00; font-style: normal; }

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
