# NPRZ Project — CBS Spatial Data Pipeline

A complete pipeline for preparing Dutch CBS administrative & grid data (2024) and packaging it for web-based interactive visualization. Includes Svelte/SvelteKit frontend with DuckDB-WASM for client-side spatial queries.

## Quick Start

### Prerequisites
- **R** ≥ 4.0 (with `sf`, `arrow`, `dplyr`, `cbsodataR`)
- **Node.js** ≥ 18
- **Git** (for version control)

### 1. Generate Data (R Pipeline)

```bash
cd R_scripts/app_core_setup
# Open 01_setup_and_process.R in R/RStudio and run it
# This downloads CBS data and generates parquet/RDS files
```

The script will:
- Download CBS grids (100m, 500m, PC4) and kerncijfers data
- Filter grids to Rijnmond study area (13 municipalities)
- Generate GeoParquet files for all administrative scales
- Save outputs to `first_data/export/`

See [R_scripts/README.md](R_scripts/README.md) for detailed documentation.

### 2. Build & Run the Web App

```bash
cd cbs-map
npm install
npm run dev
```

The app loads preprocessed parquet files from `first_data/export/` and provides an interactive dashboard for exploring the data.

See [cbs-map/README.md](cbs-map/README.md) for frontend details.

---

## Project Structure

```
NPRZ_PROJECT/
├── README.md                              # Main overview (this file)
├── .gitignore                              # Ignores raw/processed, commits export
│
├── R_scripts/
│   ├── README.md                           # Data pipeline documentation
│   ├── app_core_setup/
│   │   ├── 01_setup_and_process.R          # Main pipeline (RUN THIS)
│   │   ├── create_areas.R                  # Utility: define study areas
│   │   ├── grid_setup.R                    # Utility: grid processing
│   │   └── uploading_data.R                # Utility: data download
│   └── first\ play\ arounds/               # Exploratory scripts (optional)
│
├── first_data/                             # Data processing directories
│   ├── raw/                                # Downloaded but not committed
│   ├── processed/                          # Intermediate files (not committed)
│   └── export/         		             # COMMITTED parquet files (what we share)
│
├── cbs-map/                                # SvelteKit web application
│   ├── README.md                           # Frontend setup & usage
│   ├── src/
│   │   ├── routes/
│   │   │   └── +page.svelte                # Main map & dashboard page
│   │   └── lib/
│   └── static/                             # Static assets
│
└── svelte-tutorials/                       # (optional reference)
```

---

## Data Outputs

The pipeline generates 6 GeoParquet files saved to `first_data/export/`:

| File | Scale | Geographic Region | Rows |
|------|-------|-------------------|------|
| `grid_100m_rijnmond.parquet` | 100m² grid | Rijnmond (13 municipalities) | ~6,000 |
| `grid_500m_rijnmond.parquet` | 500m² grid | Rijnmond | ~250 |
| `buurt_2024.parquet` | Neighbourhood | Rijnmond | ~300 |
| `wijk_2024.parquet` | District/Ward | All Netherlands | ~3,500 |
| `gemeente_2024.parquet` | Municipality | All Netherlands | ~344 |
| `pc4_zh_2024.parquet` | Postcode (PC4) | Zuid-Holland province | ~1,500 |

Each file includes:
- CBS statistical variables (population, housing, energy, proximity data)
- Geometry (WKT format for parquet compatibility)
- Administrative codes for joining with other datasets

---

## Data Sources

- **Grid Data (100m, 500m, PC4):** https://download.cbs.nl/
- **Administrative Geometries:** CBS Wijkenbuurt 2024
- **Kerncijfers (Core Statistics):** CBS OData dataset 85984NED
- **PC6-to-Province Mapping:** CBS PC6 household address file

All data is **© CBS** — attribution required in final products.

---

## Key Features

- **Automated & Reproducible:** One-click R script generates all outputs
- **Spatial Filtering:** Rijnmond region pre-extracted for performance
- **Multi-scale Data:** Grid, neighbourhood, district, municipality, postcode
- **Web-Ready Formats:** GeoParquet with embedded WKT geometry
- **Client-Side Querying:** DuckDB-WASM processes data in the browser

---

## Troubleshooting

**R script hangs/runs slowly?**
- Large geometry joins scale with data size — expected for national-scale wijken/gemeente
- Geometry is pre-filtered before joining (optimized as of latest update)
- Monitor message output to track progress

**Parquet files missing?**
- Ensure `01_setup_and_process.R` has completed successfully
- Check `first_data/export/` for output files

**Web app won't load data?**
- Verify parquet files are in `cbs-map/static/data/` (symlink or copy from `first_data/export/`)
- Check browser console for DuckDB-WASM errors
- Ensure Node version ≥ 18

---

## Next Steps

1. **Extend Data Scope:** Add other provinces or national coverage
2. **Add More Metrics:** Include demographic or economic variables
3. **Temporal Analysis:** Incorporate multiple years (2023, 2024, 2025)
4. **Map Styling:** Customize color schemes and legend labels
5. **Export Features:** Add ability to download filtered data as CSV/GeoJSON

---

## License

Data products: © Statistics Netherlands (CBS) — see source links above  
Code: TBD (add your preferred license here)

---

## Questions?

See detailed documentation in:
- [R_scripts/README.md](R_scripts/README.md) — Pipeline & data processing
- [cbs-map/README.md](cbs-map/README.md) — Web app setup & components