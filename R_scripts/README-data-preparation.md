# R Data Pipeline — CBS Administrative & Grid Data Preparation

This directory contains the complete R pipeline for downloading, processing, and exporting CBS (Statistics Netherlands) spatial data for use in web applications.

## Main Script: `01_setup_and_process.R`

**This is the only script you need to run.** It's self-contained and orchestrates the entire pipeline in 10 sections.

### Prerequisites

Ensure these R packages are installed:

```r
install.packages(c("sf", "arrow", "dplyr", "stringr", "cbsodataR"))
```

### How to Run

1. Open `app_core_setup/01_setup_and_process.R` in R or RStudio
2. Run the entire script from top to bottom
3. Monitor the console output for progress messages
4. Outputs appear in `data/export/` when complete

### Runtime Expectations

- **First run:** 10–30 minutes (downloads ~1 GB of data)
- **Subsequent runs:** 5–10 minutes (reuses cached downloads)
- **National geometry operations:** May take 2–5 minutes (expected for large datasets)

---

## Pipeline Sections Explained

### SECTION 1: CONFIG & SETUP
Defines study areas and output directories:
- **Rijnmond:** 13 municipalities (Rotterdam, Schiedam, Vlaardingen, etc.)
- **Zuid-Holland:** Province code PV28
- **Output Dir:** `C:/NPRZ_project/data/`

### SECTION 2: LIBRARIES
Loads required R packages: `sf`, `arrow`, `dplyr`, `cbsodataR`, `stringr`

### SECTION 3: DOWNLOAD DATA
Downloads raw data from CBS (only if not already cached):
- **100m grid (2024):** ~200 MB
- **500m grid (2024):** ~20 MB
- **PC4 postcodes (2024):** ~50 MB
- **Kerncijfers (core statistics):** Via OData API
- **Geometries:** Wijken/Buurten/Gemeenten

**Cached in:** `data/raw/`

### SECTION 4: BUILD BOUNDARY & PROCESS GRIDS
Creates a boundary polygon for the Rijnmond study area:
1. Loads gemeente (municipality) geometries
2. Filters to 13 Rijnmond gemeentes
3. Unions into a single boundary polygon
4. Used as spatial filter for grids

### SECTION 5–6: GRID PROCESSING
For 100m and 500m grids:
1. **Pass 1:** Loose bbox pre-filter (2 km buffer around boundary)
2. **Pass 2:** Precise polygon clip using centroid-in-polygon test
3. **Drop suppressed columns:** Removes fully empty data columns
4. **Project to WGS84 (EPSG:4326):** Web-standard CRS
5. **Export to GeoParquet:** Geometry stored as WKT text

**Output files:**
- `grid_100m_rijnmond.parquet` (~6,000 rows)
- `grid_500m_rijnmond.parquet` (~250 rows)

### SECTION 7: SUMMARY
Progress report for grid processing

### SECTION 8: ADMINISTRATIVE SCALES
Joins statistics to administrative boundaries:

| Scale | Region | Rows | Output |
|-------|--------|------|--------|
| **Buurt** (neighbourhood) | Rijnmond | ~300 | `buurt_2024.parquet` |
| **Wijk** (district/ward) | All NL | ~3,500 | `wijk_2024.parquet` |
| **Gemeente** (municipality) | All NL | ~344 | `gemeente_2024.parquet` |

**Process:**
1. Download kerncijfers from CBS OData (dataset 85984NED)
2. Extract `WijkenEnBuurten` codes and split by administrative level
3. Load geometry layers from GPKG file
4. Filter geometry to only codes present in statistics (performance optimization)
5. Left-join statistics onto filtered geometry
6. Drop suppressed data columns (values of -99995, -99997)
7. Reproject to WGS84
8. Export to GeoParquet

### SECTION 9: PC4 POSTCODES
Filters postcode (PC4) data to Zuid-Holland province:

1. Downloads PC4 geometries (all NL)
2. Downloads PC6-to-province lookup table
3. Extracts unique PC4 codes for South Holland
4. Filters PC4 geometry using lookup
5. Drops suppressed columns
6. Exports to GeoParquet

**Output:** `pc4_zh_2024.parquet` (~1,500 rows)

### SECTION 10: FINAL SUMMARY
Reports all generated files and file sizes

---

## Output Files

All files are saved to `data/export/` as **GeoParquet** format:

| File | Description | Geographic Scope | Rows |
|------|-------------|------|------|
| `grid_100m_rijnmond.parquet` | 100m grid cells | Rijnmond | ~6,000 |
| `grid_500m_rijnmond.parquet` | 500m grid cells | Rijnmond | ~250 |
| `buurt_2024.parquet` | Neighbourhoods | Rijnmond | ~300 |
| `wijk_2024.parquet` | Districts/wards | All Netherlands | ~3,500 |
| `gemeente_2024.parquet` | Municipalities | All Netherlands | ~344 |
| `pc4_zh_2024.parquet` | Postcodes (PC4) | Zuid-Holland | ~1,500 |

### File Format: GeoParquet

Each parquet file contains:
- **Geometry column:** `geometry_wkt` (WKT text format)
- **Administrative codes:** `gemeentecode`, `wijkcode`, `buurtcode`, `pc4`, etc.
- **Statistical variables:** Population, housing, energy, proximity metrics
- **Metadata:** Encoding in UTF-8, compatible with GDAL, GDAL, Geopandas, DuckDB

---

## Utility Scripts

These are called by `01_setup_and_process.R` — normally you don't need to run them directly:

- **`create_areas.R`** — Defines `gm_rijnmond` and `pv_zuidholland` vectors
- **`grid_setup.R`** — Old version of grid processing logic (for reference only)
- **`uploading_data.R`** — Old data download code (superseded by Section 3)

---

## Data Sources & Attribution

| Dataset | Source | License |
|---------|--------|---------|
| Vierkantstatistieken (grids) | https://download.cbs.nl/ | © CBS |
| Wijken/Buurten (boundaries) | CBS Geodata | © CBS |
| Kerncijfers (statistics) | CBS OData API | © CBS |
| PC4 postcodes | https://download.cbs.nl/postcode/ | © CBS |
| PC6 lookup | CBS household file | © CBS |

**All data is © Statistics Netherlands (CBS)** — attribution required in derived products.

---

## Troubleshooting

### "object 'WijkenEnBuurten' not found"
- **Cause:** Old kerncijfers RDS file with wrong dataset
- **Fix:** Delete `data/raw/kerncijfers/kwb_2024_raw.rds` and re-run the script

### Script hangs for 5+ minutes during administrative scales
- **Cause:** Geometry joins on large national datasets
- **Expected:** Normal — geometry is pre-filtered before joining
- **Time estimate:** ~2–5 minutes for wijk/gemeente operations
- **Tip:** Monitor console output; you should see "Geometry rows before/after filter" messages

### Parquet files missing from `data/export/`
- **Check:** Did the script complete successfully? (look for "FULL PIPELINE COMPLETE" message)
- **Check:** Do intermediate .rds files exist in `data/processed/`?
- **Fix:** Delete entire `data/raw/` and `data/processed/` folders and re-run

### ZIP extraction fails
- **Cause:** Download corrupted or incomplete
- **Fix:** Delete the `.zip` file in `data/raw/` and re-run (will re-download)

### DuckDB/Arrow/SF installation fails
- **Fix:** Ensure you have R ≥ 4.0 and a C++ compiler
- **Windows:** Install Rtools (https://cran.r-project.org/bin/windows/Rtools/)
- **Mac:** Install Xcode Command Line Tools (`xcode-select --install`)

---

## Performance Notes

- **Geometry operations are slow by design:** Exact spatial intersection is more accurate than bbox filters
- **PC6 lookup download (2023 data):** Large file (~500 MB), downloaded once and cached
- **National-scale geometry:** `wijk_nl` and `gemeente_nl` include ~3,500 and ~344 features respectively
- **Pre-filtering optimization:** Geometry is filtered to match statistics BEFORE joining (massive speedup vs. join-then-filter)

---

## Future Enhancements

- [ ] Add multi-year support (2022, 2023, 2024)
- [ ] Extend spatial coverage beyond Zuid-Holland
- [ ] Cache more intermediate results to speed up re-runs
- [ ] Add data validation checks (e.g., missing geometries)
- [ ] Modularize into separate scripts for independence
- [ ] Add unit tests for grid clipping and attribute joins

---

## Questions or Issues?

Refer to:
- Main documentation: [../README.md](../README.md)
- Web app setup: [../cbs-map/README.md](../cbs-map/README.md)
- CBS data portal: https://www.cbs.nl/en-gb