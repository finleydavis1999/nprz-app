# ============================================================
# 02_filter_south_holland.R
# Purpose: Filter CBS grid to Zuid-Holland bounding box
#          for lightweight Svelte development and testing, make duckDB path as well
# Method:  Approximate RD New (EPSG:28992) bounding box
#          Not an exact province boundary - edges are rough
# ============================================================

library(sf)
library(arrow)
library(dplyr)

# --- Paths ---------------------------------------------------
# grid_filtered must already exist from running 01 script
# If starting fresh, re-run playbook_ai_assisted_prepare_grid.R first

output_dir <- "C:/NPRZ_project/first_data"

# --- Load full filtered grid ---------------------------------
# We reload from the parquet we already made rather than 
# re-running the full pipeline

full_parquet <- read_parquet(file.path(output_dir, "basic_grid.parquet"))

cat("Full grid rows:", nrow(full_parquet), "\n")

# --- Bounding box filter -------------------------------------
# Approximate RD New (EPSG:28992) bbox for Zuid-Holland
# X = Easting, Y = Northing
# These bounds are intentionally slightly generous at edges

zh_filtered <- full_parquet %>%
  filter(
    grepl("^E0[6-9]|^E1[0-2]", crs28992res100m) |  # fallback if coord in ID
      TRUE  # we'll use geometry_wkt coordinates below
  )

# Actually filter using the coordinate values encoded in geometry_wkt
# Parse X and Y from WKT POINT string - format is "POINT (X Y)"
zh_filtered <- full_parquet %>%
  mutate(
    coords  = sub(".*\\(\\(\\(([0-9.]+ [0-9.]+).*", "\\1", geometry_wkt),
    x_coord = as.numeric(sub("([0-9.]+) [0-9.]+", "\\1", coords)),
    y_coord = as.numeric(sub("[0-9.]+ ([0-9.]+)", "\\1", coords))
  ) %>%
  filter(
    !is.na(x_coord),  
    x_coord >= 60000  & x_coord <= 120000,
    y_coord >= 420000 & y_coord <= 480000
  ) %>%
  select(-coords)


cat("Zuid-Holland filtered rows:", nrow(zh_filtered), "\n")

# --- Save filtered parquet -----------------------------------
# Reproject to WGS84 for browser use
zh_sf_wgs84 <- st_as_sf(zh_filtered, wkt = "geometry_wkt", crs = 28992) %>%
  select(-x_coord, -y_coord) %>%
  st_transform(4326)

# Convert back to WKT in WGS84 for parquet storage
zh_wgs84_df <- zh_sf_wgs84 %>%
  st_drop_geometry() %>%                          # drop the old geometry_wkt column name conflict
  bind_cols(
    geometry_wkt = st_as_text(st_geometry(zh_sf_wgs84))  # extract WGS84 geometry as WKT
  ) %>%
  as.data.frame()

zh_parquet_path <- file.path(output_dir, "zh_grid.parquet")
write_parquet(zh_wgs84_df, zh_parquet_path)
cat("Saved WGS84 parquet:", zh_parquet_path, "\n")
cat("Rows:", nrow(zh_wgs84_df), "\n")

# Save Zuid-Holland to DuckDB
duckdb_path <- file.path(output_dir, "basic_grid.duckdb")
con <- dbConnect(duckdb(), duckdb_path)
dbExecute(con, "INSTALL spatial;")
dbExecute(con, "LOAD spatial;")
dbWriteTable(con, "zh_grid", zh_filtered, overwrite = TRUE)
cat("ZH rows in DuckDB:", dbGetQuery(con, "SELECT COUNT(*) FROM zh_grid")[[1]], "\n")
dbDisconnect(con)