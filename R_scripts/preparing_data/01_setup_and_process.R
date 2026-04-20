# ============================================================
# 01_setup_and_process.R
# Full CBS data pipeline for NPRZ spatial analysis app
#
# Sections:
#   1.  Config & paths
#   2.  Libraries
#   3.  Download raw data (grids, PC4, Kerncijfers)
#   4.  Build Rijnmond boundary
#   5.  Grid processing function
#   6.  Process 100m and 500m grids
#   7.  Summary (grids)
#   8.  Administrative scales (Buurt / Wijk / Gemeente)
#   9.  PC4 (South Holland)
#   10. Export Rijnmond boundary
#   11. Final summary
#
# Outputs → app-development/static/data/
#   grid_100m_rijnmond.geojson + _stats.parquet
#   grid_500m_rijnmond.geojson + _stats.parquet
#   buurt_2024.geojson         + _stats.parquet
#   wijk_2024.geojson          + _stats.parquet
#   gemeente_2024.geojson      + _stats.parquet
#   pc4_zh_2024.geojson        + _stats.parquet
#   rotterdam_boundary.geojson
#
# Adding a new dataset? See R_scripts/docs/ADDING_DATASETS.md
# ============================================================

# ============================================================
# SECTION 1: CONFIG & PATHS
# ============================================================

# Shared helpers (drop_suppressed, export_admin_scale, validate_flows)
source("R_scripts/preparing_data/utils.R")

# --- Local data directory (not committed to git) ------------
# Raw downloads and intermediate files stay here on your machine
local_data_dir <- "C:/NPRZ_project/data"
raw_dir        <- file.path(local_data_dir, "raw")
processed_dir  <- file.path(local_data_dir, "processed")

# --- Export: writes directly into the app's static folder ---
# Committed to git — the app cannot run without these files
# Adjust this path if your repo lives somewhere else
export_dir <- "C:/NPRZ_project/app-development/static/data"

# --- Study area: Rijnmond municipalities --------------------
gm_rijnmond <- c(
  "GM0489",  # Barendrecht
  "GM0502",  # Capelle aan den IJssel
  "GM0542",  # Krimpen aan den IJssel
  "GM0556",  # Maassluis
  "GM0597",  # Ridderkerk
  "GM0599",  # Rotterdam
  "GM0606",  # Schiedam
  "GM0613",  # Albrandswaard
  "GM0622",  # Vlaardingen
  "GM1621",  # Nissewaard
  "GM1723",  # Lansingerland
  "GM1924",  # Goeree-Overflakkee
  "GM1984"   # Voorne aan Zee
)

# ============================================================
# SECTION 2: LIBRARIES
# ============================================================

library(sf)
library(arrow)
library(dplyr)
library(stringr)
library(cbsodataR)

sf_use_s2(FALSE)

# ============================================================
# SECTION 3: DOWNLOAD RAW DATA
# ============================================================

message("\n=== SECTION 3: Download raw data ===")

# Create directory structure
dirs <- c(
  raw_dir, processed_dir, export_dir,
  file.path(raw_dir, "grid_100m"),
  file.path(raw_dir, "grid_500m"),
  file.path(raw_dir, "pc4"),
  file.path(raw_dir, "kerncijfers"),
  file.path(raw_dir, "geometries")
)
for (d in dirs) dir.create(d, recursive = TRUE, showWarnings = FALSE)

# Static CBS file downloads
static_downloads <- list(
  list(
    label = "100m grid 2024",
    url   = "https://download.cbs.nl/vierkant/100/2025-cbs_vk100_2024_v1.zip",
    dest  = file.path(raw_dir, "grid_100m", "cbs_vk100_2024.zip")
  ),
  list(
    label = "500m grid 2024",
    url   = "https://download.cbs.nl/vierkant/500/2025-cbs_vk500_2024_v1.zip",
    dest  = file.path(raw_dir, "grid_500m", "cbs_vk500_2024.zip")
  ),
  list(
    label = "PC4 2024",
    url   = "https://download.cbs.nl/postcode/2025-cbs_pc4_2024_v1.zip",
    dest  = file.path(raw_dir, "pc4", "cbs_pc4_2024.zip")
  )
)

for (f in static_downloads) {
  if (!file.exists(f$dest)) {
    message("Downloading: ", f$label, " ...")
    download.file(f$url, destfile = f$dest, mode = "wb")
    message("  Saved: ", f$dest)
  } else {
    message("Already downloaded, skipping: ", f$label)
  }
}

# Kerncijfers wijken en buurten via OData (CBS table 85984NED)
kerncijfers_path <- file.path(raw_dir, "kerncijfers", "kwb_2024_raw.rds")

if (!file.exists(kerncijfers_path)) {
  message("Downloading Kerncijfers 2024 via OData ...")
  kwb_raw <- cbs_get_data("85984NED")
  saveRDS(kwb_raw, kerncijfers_path)
  message("  Saved: ", kerncijfers_path, " (", nrow(kwb_raw), " rows)")
} else {
  message("Kerncijfers already downloaded, skipping.")
}

# ============================================================
# SECTION 4: BUILD RIJNMOND BOUNDARY
# ============================================================

message("\n=== SECTION 4: Build Rijnmond boundary ===")

# Download gemeente geometry if not already present
geom_extract <- file.path(raw_dir, "geometries", "wijkbuurt_2024")
gpkg_geom    <- list.files(geom_extract, pattern = "\\.gpkg$",
                            full.names = TRUE, recursive = TRUE)[1]

if (is.na(gpkg_geom) || !file.exists(gpkg_geom)) {
  message("Geometry not found — downloading ...")
  geom_zip <- file.path(raw_dir, "geometries", "wijkbuurt_2024.zip")
  download.file(
    "https://geodata.cbs.nl/files/Wijkenbuurtkaart/WijkBuurtkaart_2024_v2.zip",
    destfile = geom_zip, mode = "wb"
  )
  unzip(geom_zip, exdir = geom_extract, overwrite = FALSE)
  gpkg_geom <- list.files(geom_extract, pattern = "\\.gpkg$",
                           full.names = TRUE, recursive = TRUE)[1]
  message("  Available layers: ",
          paste(st_layers(gpkg_geom)$name, collapse = ", "))
}

# Build boundary from gemeente polygons (using utils.R helper)
rijnmond_boundary <- build_rijnmond_boundary(gpkg_geom, gm_rijnmond)

bb <- st_bbox(st_transform(rijnmond_boundary, 28992))
message("  Rijnmond bbox (RD New):")
message("    xmin=", round(bb["xmin"]), " xmax=", round(bb["xmax"]),
        " ymin=", round(bb["ymin"]), " ymax=", round(bb["ymax"]))

# ============================================================
# SECTION 5: GRID PROCESSING FUNCTION
# ============================================================
# Handles both 100m and 500m CBS grid files.
# Steps: unzip → bbox pre-filter → centroid-within clip →
#        drop suppressed columns → export GeoJSON + Parquet

process_grid <- function(zip_path, resolution_label, boundary_polygon, id_col) {

  message("\n--- Processing ", resolution_label, " grid ---")

  # Unzip
  extract_dir <- file.path(raw_dir, paste0("grid_", resolution_label), "extracted")
  dir.create(extract_dir, recursive = TRUE, showWarnings = FALSE)
  unzip(zip_path, exdir = extract_dir, overwrite = FALSE)

  gpkg_file <- list.files(extract_dir, pattern = "\\.gpkg$",
                           full.names = TRUE, recursive = TRUE)[1]
  message("  Reading full NL grid ...")
  grid_nl <- st_read(gpkg_file, quiet = TRUE)
  message("  Full NL rows: ", nrow(grid_nl))

  # Pass 1: loose bbox pre-filter (fast)
  boundary_proj <- st_transform(boundary_polygon, st_crs(grid_nl))
  bb  <- st_bbox(boundary_proj)
  buf <- 2000  # metres

  cents  <- st_coordinates(st_centroid(grid_nl))
  grid_rough <- grid_nl[
    cents[, "X"] >= (bb["xmin"] - buf) &
    cents[, "X"] <= (bb["xmax"] + buf) &
    cents[, "Y"] >= (bb["ymin"] - buf) &
    cents[, "Y"] <= (bb["ymax"] + buf),
  ]
  message("  After bbox pre-filter: ", nrow(grid_rough), " rows")

  # Pass 2: precise centroid-within clip (prevents border bleed)
  grid_cents <- st_centroid(grid_rough)
  inside     <- st_within(grid_cents, boundary_proj, sparse = FALSE)[, 1]
  grid_rijnmond <- grid_rough[inside, ]
  message("  After centroid-within clip: ", nrow(grid_rijnmond), " rows")

  if (nrow(grid_rijnmond) == 0) {
    message("  WARNING: No grid cells found — check boundary CRS")
    return(list(res = resolution_label, rows = 0, cols_kept = 0, cols_dropped = 0))
  }

  # Drop fully-suppressed columns (all -99995 within study area)
  data_only       <- st_drop_geometry(grid_rijnmond)
  suppressed_cols <- names(data_only)[
    sapply(data_only, function(col) {
      is.numeric(col) && all(col %in% c(-99995, -99997), na.rm = TRUE)
    })
  ]
  message("  Suppressed columns dropped: ", length(suppressed_cols))
  grid_clean <- grid_rijnmond |> select(-all_of(suppressed_cols))

  # Reproject to WGS84 then replace remaining suppressed values with NA
  grid_wgs84  <- st_transform(grid_clean, 4326)
  stats_clean <- drop_suppressed(st_drop_geometry(grid_wgs84),
                                  label = paste("grid", resolution_label))

  # Export centroid GeoJSON (geometry only, ID column)
  grid_cents_wgs84 <- st_centroid(grid_wgs84)
  geom_only <- st_sf(
    setNames(list(grid_cents_wgs84[[id_col]]), id_col),
    geometry = st_geometry(grid_cents_wgs84),
    crs = 4326
  )
  geojson_path <- file.path(export_dir,
                             paste0("grid_", resolution_label, "_rijnmond.geojson"))
  st_write(geom_only, geojson_path, driver = "GeoJSON", delete_dsn = TRUE)
  message("  GeoJSON exported: ", basename(geojson_path))

  # Export stats Parquet (all variables, no geometry)
  parquet_path <- file.path(export_dir,
                             paste0("grid_", resolution_label, "_rijnmond_stats.parquet"))
  write_parquet(stats_clean, parquet_path)
  message("  Parquet exported: ", basename(parquet_path))

  list(
    res          = resolution_label,
    rows         = nrow(grid_rijnmond),
    cols_kept    = ncol(grid_clean) - 1,
    cols_dropped = length(suppressed_cols)
  )
}

# ============================================================
# SECTION 6: PROCESS BOTH GRID RESOLUTIONS
# ============================================================

message("\n=== SECTION 6: Process grids ===")

grid_results <- list(
  process_grid(
    zip_path         = file.path(raw_dir, "grid_100m", "cbs_vk100_2024.zip"),
    resolution_label = "100m",
    boundary_polygon = rijnmond_boundary,
    id_col           = "crs28992res100m"
  ),
  process_grid(
    zip_path         = file.path(raw_dir, "grid_500m", "cbs_vk500_2024.zip"),
    resolution_label = "500m",
    boundary_polygon = rijnmond_boundary,
    id_col           = "crs28992res500m"
  )
)

# ============================================================
# SECTION 7: GRID SUMMARY
# ============================================================

message("\n=== SECTION 7: Grid summary ===")
for (r in grid_results) {
  message("  ", r$res, ": ", r$rows, " cells | ",
          r$cols_kept, " vars kept | ", r$cols_dropped, " dropped")
}

# ============================================================
# SECTION 8: ADMINISTRATIVE SCALES
# ============================================================
# Buurt / Wijk / Gemeente from CBS Kerncijfers + WijkBuurtkaart geometry
# Uses export_admin_scale() from utils.R

message("\n=== SECTION 8: Administrative scales ===")

# Load Kerncijfers
kwb_raw <- readRDS(kerncijfers_path) |>
  mutate(WijkenEnBuurten = str_trim(WijkenEnBuurten))

# Split by administrative level prefix
buurt_nl    <- kwb_raw |> filter(str_starts(WijkenEnBuurten, "BU"))
wijk_nl     <- kwb_raw |> filter(str_starts(WijkenEnBuurten, "WK"))
gemeente_nl <- kwb_raw |> filter(str_starts(WijkenEnBuurten, "GM"))

message("Kerncijfers split — Buurt: ", nrow(buurt_nl),
        " | Wijk: ", nrow(wijk_nl),
        " | Gemeente: ", nrow(gemeente_nl))

# Load geometry layers
geom_buurt    <- st_read(gpkg_geom, layer = "buurten",   quiet = TRUE)
geom_wijk     <- st_read(gpkg_geom, layer = "wijken",    quiet = TRUE)
geom_gemeente <- st_read(gpkg_geom, layer = "gemeenten", quiet = TRUE)

# Filter Buurt to Rijnmond municipalities only
rijnmond_nums  <- str_extract(gm_rijnmond, "\\d+")
buurt_rijnmond <- buurt_nl |>
  filter(str_sub(WijkenEnBuurten, 3, 6) %in% rijnmond_nums)
message("Buurt rows in Rijnmond: ", nrow(buurt_rijnmond))

# Helper: join stats to geometry, clean, reproject, then export via utils.R
process_admin_scale <- function(stats_df, geom_sf,
                                 stats_id_col, geom_id_col,
                                 scale_name) {
  message("\n--- ", scale_name, " ---")

  # Drop fully-suppressed columns within this subset
  stats_clean <- drop_suppressed(stats_df, label = scale_name)

  # Filter geometry to only codes present in stats (speed + coverage)
  valid_codes  <- unique(stats_clean[[stats_id_col]])
  geom_sub     <- geom_sf |> filter(.data[[geom_id_col]] %in% valid_codes)
  message("  Geometry rows: ", nrow(geom_sub))

  # Join stats onto geometry and reproject to WGS84
  joined <- geom_sub |>
    left_join(
      stats_clean,
      by = setNames(stats_id_col, geom_id_col)
    ) |>
    st_transform(4326)

  message("  Features after join: ", nrow(joined))

  # Export using utils.R — handles simplification, GeoJSON + Parquet
  export_admin_scale(joined,
                     id_col     = geom_id_col,
                     scale_name = scale_name,
                     export_dir = export_dir)

  nrow(joined)
}

n_buurt    <- process_admin_scale(buurt_rijnmond, geom_buurt,
                                   "WijkenEnBuurten", "buurtcode",    "buurt_2024")
n_wijk     <- process_admin_scale(wijk_nl,         geom_wijk,
                                   "WijkenEnBuurten", "wijkcode",     "wijk_2024")
n_gemeente <- process_admin_scale(gemeente_nl,      geom_gemeente,
                                   "WijkenEnBuurten", "gemeentecode", "gemeente_2024")

message("\nAdmin summary:")
message("  buurt_2024    (Rijnmond):  ", n_buurt,    " features")
message("  wijk_2024     (national):  ", n_wijk,     " features")
message("  gemeente_2024 (national):  ", n_gemeente, " features")

# ============================================================
# SECTION 9: PC4 (SOUTH HOLLAND)
# ============================================================

message("\n=== SECTION 9: PC4 ===")

pc4_zip     <- file.path(raw_dir, "pc4", "cbs_pc4_2024.zip")
pc4_extract <- file.path(raw_dir, "pc4", "extracted")
dir.create(pc4_extract, showWarnings = FALSE, recursive = TRUE)
unzip(pc4_zip, exdir = pc4_extract, overwrite = FALSE)

pc4_gpkg <- list.files(pc4_extract, pattern = "\\.gpkg$",
                        full.names = TRUE, recursive = TRUE)[1]
message("Reading PC4: ", basename(pc4_gpkg))
pc4_nl <- st_read(pc4_gpkg, quiet = TRUE)
message("Full NL PC4 rows: ", nrow(pc4_nl))

# Filter to South Holland via PC6 lookup table
pc6_lookup_path <- file.path(raw_dir, "pc4", "pc6_lookup.zip")
if (!file.exists(pc6_lookup_path)) {
  message("Downloading PC6 lookup for province mapping ...")
  download.file(
    "https://download.cbs.nl/postcode/2023-cbs-pc6huisnr20230801_buurt_20250225.zip",
    destfile = pc6_lookup_path, mode = "wb"
  )
}

pc6_extract <- file.path(raw_dir, "pc4", "pc6_lookup_extracted")
dir.create(pc6_extract, showWarnings = FALSE, recursive = TRUE)
unzip(pc6_lookup_path, exdir = pc6_extract, overwrite = FALSE)

pc6_files <- list.files(pc6_extract, pattern = "\\.(csv|parquet)$",
                         full.names = TRUE, recursive = TRUE)

if (length(pc6_files) > 0) {
  pc6_lookup <- if (grepl("\\.parquet$", pc6_files[1])) {
    read_parquet(pc6_files[1])
  } else {
    read.csv(pc6_files[1])
  }

  if (all(c("PC4", "provincienaam") %in% names(pc6_lookup))) {
    zh_pc4_codes <- pc6_lookup |>
      filter(provincienaam == "Zuid-Holland") |>
      distinct(PC4) |>
      pull(PC4)
    pc4_zh <- pc4_nl |> filter(pc4 %in% zh_pc4_codes)
    message("PC4 South Holland: ", nrow(pc4_zh), " areas")
  } else {
    message("WARNING: PC6 lookup missing expected columns — using full PC4")
    pc4_zh <- pc4_nl
  }
} else {
  message("WARNING: PC6 lookup file not found — using full PC4")
  pc4_zh <- pc4_nl
}

# Clean, reproject, export via utils.R
pc4_clean <- drop_suppressed(st_drop_geometry(pc4_zh), label = "pc4")
pc4_sf    <- st_sf(pc4_clean, geometry = st_geometry(pc4_zh), crs = st_crs(pc4_zh)) |>
  st_transform(4326)

export_admin_scale(pc4_sf,
                   id_col     = "postcode",
                   scale_name = "pc4_zh_2024",
                   export_dir = export_dir)

message("PC4 exported: ", nrow(pc4_sf), " features")

# ============================================================
# SECTION 10: EXPORT RIJNMOND BOUNDARY
# ============================================================

message("\n=== SECTION 10: Export boundary ===")

sf_use_s2(FALSE)

boundary_wgs84  <- st_transform(rijnmond_boundary, 4326)
boundary_simple <- st_simplify(boundary_wgs84, dTolerance = 0.0005,
                                preserveTopology = TRUE)

# NOTE: named rotterdam_boundary to match what +page.svelte expects
boundary_path <- file.path(export_dir, "rotterdam_boundary.geojson")
st_write(
  st_sf(geometry = st_geometry(boundary_simple)),
  boundary_path,
  driver     = "GeoJSON",
  delete_dsn = TRUE
)
message("  Boundary exported: ",
        round(file.size(boundary_path) / 1024, 1), " KB")

# ============================================================
# SECTION 11: FINAL SUMMARY
# ============================================================

message("\n========================================")
message("=== FULL PIPELINE COMPLETE ===")
message("========================================")
message("\nAll files written to: ", export_dir)
message("\nFiles produced:")
message("  grid_100m_rijnmond  — ", grid_results[[1]]$rows, " cells")
message("  grid_500m_rijnmond  — ", grid_results[[2]]$rows, " cells")
message("  buurt_2024          — ", n_buurt,         " features")
message("  wijk_2024           — ", n_wijk,          " features")
message("  gemeente_2024       — ", n_gemeente,      " features")
message("  pc4_zh_2024         — ", nrow(pc4_sf),    " features")
message("  rotterdam_boundary")
message("\nNext: run npm run dev in app-development/ to verify the app loads")