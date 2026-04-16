<script lang="ts">
    import { MapLibre, GeoJSONSource, FillLayer, LineLayer } from 'svelte-maplibre-gl';
    import 'maplibre-gl/dist/maplibre-gl.css';
    import { browser } from '$app/environment';
    import * as duckdb from '@duckdb/duckdb-wasm';

    const variables = [
        { key: 'aantal_inwoners', label: 'Total population' },
        { key: 'aantal_mannen', label: 'Men' },
        { key: 'aantal_vrouwen', label: 'Women' },
        { key: 'aantal_inwoners_0_tot_15_jaar', label: 'Age 0–15' },
        { key: 'aantal_inwoners_15_tot_25_jaar', label: 'Age 15–25' },
        { key: 'aantal_inwoners_25_tot_45_jaar', label: 'Age 25–45' },
        { key: 'aantal_inwoners_45_tot_65_jaar', label: 'Age 45–65' },
        { key: 'aantal_inwoners_65_jaar_en_ouder', label: 'Age 65+' },
        { key: 'percentage_geb_nederland_herkomst_nederland', label: '% Dutch origin' },
        { key: 'percentage_geb_nederland_herkomst_overig_europa', label: '% European origin (NL-born)' },
        { key: 'percentage_geb_nederland_herkomst_buiten_europa', label: '% Non-European origin (NL-born)' },
        { key: 'percentage_geb_buiten_nederland_herkomst_europa', label: '% European origin (foreign-born)' },
        { key: 'percentage_geb_buiten_nederland_herkmst_buiten_europa', label: '% Non-European origin (foreign-born)' },
        { key: 'aantal_part_huishoudens', label: 'Total households' },
        { key: 'aantal_eenpersoonshuishoudens', label: 'Single-person households' },
        { key: 'aantal_meerpersoonshuishoudens_zonder_kind', label: 'Multi-person households (no children)' },
        { key: 'aantal_eenouderhuishoudens', label: 'Single-parent households' },
        { key: 'aantal_tweeouderhuishoudens', label: 'Two-parent households' },
        { key: 'gemiddelde_huishoudensgrootte', label: 'Average household size' },
        { key: 'aantal_woningen', label: 'Total dwellings' },
        { key: 'aantal_woningen_bouwjaar_voor_1945', label: 'Dwellings built before 1945' },
        { key: 'aantal_woningen_bouwjaar_45_tot_65', label: 'Dwellings built 1945–65' },
        { key: 'aantal_woningen_bouwjaar_65_tot_75', label: 'Dwellings built 1965–75' },
        { key: 'aantal_woningen_bouwjaar_75_tot_85', label: 'Dwellings built 1975–85' },
        { key: 'aantal_woningen_bouwjaar_85_tot_95', label: 'Dwellings built 1985–95' },
        { key: 'aantal_woningen_bouwjaar_95_tot_05', label: 'Dwellings built 1995–05' },
        { key: 'aantal_woningen_bouwjaar_05_tot_15', label: 'Dwellings built 2005–15' },
        { key: 'aantal_woningen_bouwjaar_15_en_later', label: 'Dwellings built 2015+' },
        { key: 'aantal_meergezins_woningen', label: 'Multi-family dwellings' },
        { key: 'percentage_koopwoningen', label: '% Owner-occupied' },
        { key: 'percentage_huurwoningen', label: '% Rental' },
        { key: 'aantal_huurwoningen_in_bezit_woningcorporaties', label: 'Social housing units' },
        { key: 'aantal_niet_bewoonde_woningen', label: 'Unoccupied dwellings' },
        { key: 'aantal_personen_met_uitkering_onder_aowlft', label: 'Persons on benefits (under pension age)' },
    ];

    let selectedVar = $state(variables[0].key);
    let geojson = $state<any>(null);
    let loading = $state(true);
    let error = $state<string | null>(null);
    let conn = $state<any>(null);
    let currentBreaks = $state<number[]>([]);

    function wktToGeometry(wkt: string) {
        try {
            const inner = wkt
                .replace('MULTIPOLYGON (((', '')
                .replace(')))', '')
                .trim();
            const coords = inner.split(', ').map((pair: string) => {
                const [lng, lat] = pair.trim().split(' ').map(Number);
                return [lng, lat];
            });
            return { type: 'Polygon', coordinates: [coords] };
        } catch {
            return null;
        }
    }

    function quantileBreaks(values: number[], n: number): number[] {
        const sorted = [...values].sort((a, b) => a - b);
        const breaks = [];
        for (let i = 1; i < n; i++) {
            const idx = Math.floor((i / n) * sorted.length);
            breaks.push(sorted[idx]);
        }
        return breaks;
    }

    async function loadVariable(connection: any, varName: string) {
    loading = true;
    try {
        const result = await connection.query(`
            SELECT geometry_wkt, "${varName}" as value
            FROM read_parquet('zh_grid.parquet')
            WHERE "${varName}" != -99995
        `);
        const rows = result.toArray().map((row: any) => row.toJSON());
        
        // Separate valid values from suppressed/unknown
        const validRows = rows.filter((r: any) => r.value !== -99997 && r.value !== null && isFinite(Number(r.value)));
        const values = validRows.map((r: any) => Number(r.value));
        const breaks = quantileBreaks(values, 4); // 4 breaks = 4 classes for valid data

        geojson = {
            type: 'FeatureCollection',
            features: rows.map((row: any) => ({
                type: 'Feature',
                geometry: wktToGeometry(row.geometry_wkt),
                properties: {
                    value: row.value,
                    class: row.value === -99997 || row.value === null
                        ? 'nodata'
                        : String(breaks.findIndex((b) => row.value <= b) === -1 ? 3 : breaks.findIndex((b) => row.value <= b))
                }
            })).filter((f: any) => f.geometry !== null)
        };

        currentBreaks = breaks;
    } catch(e) {
        error = `Query error: ${e}`;
    }
    loading = false;
}

    $effect(() => {
        if (!browser) return;
        async function setup() {
            try {
                const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();
                const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);
                const worker_url = URL.createObjectURL(
                    new Blob([`importScripts("${bundle.mainWorker}");`], { type: 'text/javascript' })
                );
                const worker = new Worker(worker_url);
                const logger = new duckdb.ConsoleLogger();
                const db = new duckdb.AsyncDuckDB(logger, worker);
                await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
                URL.revokeObjectURL(worker_url);
                await db.registerFileURL(
                    'zh_grid.parquet',
                    new URL('/zh_grid.parquet', window.location.origin).href,
                    duckdb.DuckDBDataProtocol.HTTP,
                    false
                );
                const connection = await db.connect();
                conn = connection;
                await loadVariable(connection, selectedVar);
            } catch(e) {
                error = `DuckDB init error: ${e}`;
                loading = false;
            }
        }
        setup();
    });

    $effect(() => {
        const v = selectedVar;
        if (conn) loadVariable(conn, v);
    });

    const colours = ['#f1eef6', '#bdc9e1', '#74a9cf', '#045a8d'];
</script>

<!-- Controls -->
<div class="controls">
    <label for="varselect"><strong>Display variable:</strong></label>
    <select id="varselect" bind:value={selectedVar}>
        {#each variables as v}
            <option value={v.key}>{v.label}</option>
        {/each}
    </select>
    {#if loading}<span class="status">Loading...</span>{/if}
    {#if error}<span class="status error">{error}</span>{/if}
</div>

<!-- Legend -->
{#if currentBreaks.length === 3}
    <div class="legend">
        <div class="legend-title">{variables.find(v => v.key === selectedVar)?.label}</div>
        <div class="legend-row">
            <span class="swatch" style="background:#d3d3d3"></span>
            No data / unknown
        </div>
        {#each colours as colour, i}
            <div class="legend-row">
                <span class="swatch" style="background:{colour}"></span>
                {#if i === 0}
                    ≤ {currentBreaks[0].toFixed(1)}
                {:else if i === 3}
                    > {currentBreaks[2].toFixed(1)}
                {:else}
                    ≤ {currentBreaks[i].toFixed(1)}
                {/if}
            </div>
        {/each}
    </div>
{/if}

<!-- Map -->
<MapLibre
    style="https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json"
    zoom={8}
    center={{ lng: 4.5, lat: 52.0 }}
>
    {#if geojson}
        <GeoJSONSource id="cbs-grid" data={geojson}>
            <FillLayer
    id="cbs-fill"
    paint={{
        'fill-color': [
            'match', ['get', 'class'],
            'nodata', '#d3d3d3',
            '0', '#f1eef6',
            '1', '#bdc9e1',
            '2', '#74a9cf',
            '3', '#045a8d',
            '#f1eef6'
        ],
        'fill-opacity': 0.8
    }}
/>      
            <LineLayer
                id="cbs-outline"
                paint={{ 'line-color': '#ffffff', 'line-width': 0.3 }}
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
        padding: 0.75rem 1rem;
        border-radius: 8px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.2);
        font-family: sans-serif;
    }
    .status { margin-left: 0.75rem; color: grey; }
    .error { color: red; }
    .legend {
        position: absolute;
        bottom: 2rem;
        left: 1rem;
        z-index: 10;
        background: white;
        padding: 0.75rem 1rem;
        border-radius: 8px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.2);
        font-family: sans-serif;
        font-size: 0.85rem;
        min-width: 160px;
    }
    .legend-title {
        font-weight: bold;
        margin-bottom: 0.5rem;
    }
    .legend-row {
        display: flex;
        align-items: center;
        gap: 0.5rem;
        margin: 0.25rem 0;
    }
    .swatch {
        width: 16px;
        height: 16px;
        border-radius: 3px;
        display: inline-block;
        border: 1px solid #ccc;
    }
</style>
