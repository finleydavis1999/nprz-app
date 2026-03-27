# ============================================================
# playbook_prepare_grid.R
# Purpose: Load CBS 100m grid (vk100 2024), remove suppressed
#          columns, export to DuckDB and GeoParquet
# Data:    CBS Vierkantstatistieken 2024 (vk100, 100m grid)
# CRS:     EPSG:28992 (RD New) - kept throughout
# Author:  Finley Davis
# Date:    26th March 2026
# ============================================================

# --- 0. Libraries -------------------------------------------
# Run install lines once if packages are missing, then comment out
# install.packages("sf")
# install.packages("duckdb", repos = "https://duckdb.r-universe.dev")
# install.packages("arrow", repos = "https://apache.r-universe.dev")
# install.packages("dplyr")

library(sf)
library(duckdb)
library(arrow)
library(dplyr)

# --- 1. Paths -----------------------------------------------
# EDIT THESE two lines to match your machine:
# zip_path should point to your downloaded CBS zip file
# output_dir should point to your project data folder

zip_path <- "C:/Users/finle/Downloads/2025-cbs_vk100_2024_v1.zip"
output_dir <- "C:/NPRZ_project/first_data"

# --- 2. Unzip and load --------------------------------------
# Extracts zip contents into first_data/cbs_raw/
# dir.create does nothing if folder already exists (showWarnings = FALSE)

dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
unzip(zip_path, exdir = file.path(output_dir, "cbs_raw"))

# Read the GeoPackage - geometry stored as EPSG:28992 polygons
gpkg_path <- file.path(output_dir, "cbs_raw", "cbs_vk100_2024_v1.gpkg")
grid <- st_read(gpkg_path)

cat("Rows:", nrow(grid), "\n")
cat("Columns:", ncol(grid), "\n")
cat("CRS:", st_crs(grid)$input, "\n")

# --- 3. Remove suppressed columns ---------------------------
# CBS uses -99995 to suppress cells with too few inhabitants (privacy)
# Columns that are entirely -99995 contain no usable data

suppressed_cols <- names(grid)[
  sapply(st_drop_geometry(grid), function(col) {
    all(col == -99995, na.rm = TRUE)
  })
]

cat("Dropping", length(suppressed_cols), "fully suppressed columns\n")

grid_filtered <- grid %>%
  dplyr::select(-all_of(suppressed_cols))

cat("Columns remaining:", ncol(grid_filtered) - 1, "(excl. geometry)\n")

# --- 4. Visual check ----------------------------------------
# Plots grid geometry only - may be slow at full NL resolution

plot(st_geometry(grid_filtered),
     main = "CBS 100m grid - full Netherlands",
     col  = "steelblue",
     border = NA)

# --- 5. Export to DuckDB ------------------------------------
# Geometry converted to WKT text for DuckDB compatibility
# DuckDB file can contain multiple tables under one .duckdb file

duckdb_path <- file.path(output_dir, "basic_grid.duckdb")
con <- dbConnect(duckdb(), duckdb_path)
dbExecute(con, "INSTALL spatial;")
dbExecute(con, "LOAD spatial;")

grid_for_db <- grid_filtered %>%
  dplyr::mutate(geom_wkt = st_as_text(geom)) %>%
  st_drop_geometry()

dbWriteTable(con, "cbs_grid", grid_for_db, overwrite = TRUE)
cat("Rows written to DuckDB:", dbGetQuery(con, "SELECT COUNT(*) FROM cbs_grid")[[1]], "\n")
dbDisconnect(con)
cat("DuckDB saved to:", duckdb_path, "\n")

# --- 6. Export to GeoParquet --------------------------------
# Geometry converted to WKT text for Parquet compatibility
# GeoParquet is a single-table format (unlike DuckDB)
# Preferred format for DuckDB-WASM consumption in Svelte app

geoparquet_path <- file.path(output_dir, "basic_grid.parquet")

grid_parquet <- grid_filtered %>%
  dplyr::mutate(geometry_wkt = st_as_text(geom)) %>%
  st_drop_geometry() %>%
  as.data.frame()

write_parquet(grid_parquet, geoparquet_path)
cat("GeoParquet saved to:", geoparquet_path, "\n")

# --- 7. Summary ---------------------------------------------
cat("\n=== Done ===\n")
cat("Grid cells:        ", nrow(grid_filtered), "\n")
cat("Variables kept:    ", ncol(grid_filtered) - 1, "(excl. geometry)\n")
cat("Variables dropped: ", length(suppressed_cols), "(all -99995)\n")
cat("DuckDB:            ", duckdb_path, "\n")
cat("GeoParquet:        ", geoparquet_path, "\n")
cat("CRS:                EPSG:28992 (RD New)\n")


# --- 8. Export lightweight GeoJSON for Svelte testing -----------------------
# Full grid is too large for browser rendering
# Instead we export centroids (points) which are far lighter
# and sufficient for initial map display and variable inspection

# Compute centroid of each 100m square
grid_centroids <- st_centroid(grid_filtered)

# Export centroids as GeoJSON - reproject to WGS84 (EPSG:4326)
# because web mapping libraries (MapLibre etc) expect standard lat/lon
grid_centroids_wgs84 <- st_transform(grid_centroids, 4326)

geojson_path <- file.path(output_dir, "cbs_centroids.geojson")

st_write(grid_centroids_wgs84,
         geojson_path,
         driver = "GeoJSON",
         delete_dsn = TRUE)

cat("GeoJSON centroids saved to:", geojson_path, "\n")
cat("Features:", nrow(grid_centroids_wgs84), "\n")