# NPRZ Project — Spatial Data Preparation

## Overview
This repository contains R scripts for preparing CBS 100m grid statistics 
(Vierkantstatistieken 2024) for use in a spatial analysis web application 
built with Svelte and DuckDB-WASM.

## Data Source
- **Dataset:** CBS Vierkantstatistieken 2024 (vk100, 100m grid)
- **Provider:** Statistics Netherlands (CBS)
- **Download:** https://download.cbs.nl/vierkant/100/2025-cbs_vk100_2024_v1.zip
- **CRS:** EPSG:28992 (RD New) — retained throughout for accuracy within NL

## Repository Structure
```
NPRZ_PROJECT/
├── R_scripts/
│   ├── playbook_prepare_grid.R           # manual version
│   └── playbook_ai_assisted_prepare_grid.R  # reviewed and cleaned version
├── first_data/                           # gitignored — generated outputs
│   ├── basic_grid.duckdb
│   └── basic_grid.parquet
├── svelte-tutorials/                     # gitignored
├── .gitignore
└── README.md
```

## How to Run
1. Download the CBS zip file from the link above
2. Open `R_scripts/playbook_ai_assisted_prepare_grid.R`
3. Edit the two path variables at the top of the script:
   - `zip_path` — where you saved the downloaded zip
   - `output_dir` — where you want the output files saved
4. Run the script top to bottom in R (version 4.0 or higher)

## Required R Packages
- `sf` — spatial data handling
- `duckdb` — DuckDB database connection
- `arrow` — Parquet file export
- `dplyr` — data manipulation

Install with:
```r
install.packages("sf")
install.packages("duckdb", repos = "https://duckdb.r-universe.dev")
install.packages("arrow", repos = "https://apache.r-universe.dev")
install.packages("dplyr")
```

## Output Files
Both outputs are saved to `first_data/` and are gitignored due to file size.
To regenerate them, run the preparation script as described above.

| File | Format | Purpose |
|------|--------|---------|
| `basic_grid.duckdb` | DuckDB | Local querying and exploration |
| `basic_grid.parquet` | GeoParquet | Consumption by Svelte/DuckDB-WASM app |

## Variables
96 of 131 original CBS columns were dropped as fully suppressed (all values 
-99995, meaning too few inhabitants to report). 35 variables are retained 
covering population, housing, energy, and proximity to amenities.

## Next Steps
- Spatial filter to study area (pending province boundary dataset)
- Integration with Svelte app skeleton (pending colleague's repository setup)
- Display variables on interactive map via DuckDB-WASM


## getting data ready for map
-you need to run 02_filter_south_holland.R to generate zh_grid.parquet and place it in cbs-map/static/ before running the app.

CBS Map App (cbs-map/)
A SvelteKit application that loads the CBS 100m grid data via DuckDB-WASM directly in the browser and displays it as an interactive choropleth map of Zuid-Holland.
Setup: before running the app, generate the data file by running R_scripts/02_filter_south_holland.R in R, then copy the output zh_grid.parquet into cbs-map/static/.
To run locally: navigate to cbs-map/, run npm install then npm run dev.
What +page.svelte does: initialises DuckDB-WASM in the browser and registers the parquet file as a queryable data source. When a variable is selected from the dropdown, it runs a SQL query filtering out suppressed values (-99995, -9997) and any negatives, calculates 4 quantile class breaks from the valid distribution, assigns each grid square to a class, and passes the result as GeoJSON to MapLibre GL for rendering as a choropleth with a grey "no data" category for unknown values. The legend updates dynamically with each variable switch.