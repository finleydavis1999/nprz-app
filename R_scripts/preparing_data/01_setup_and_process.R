# ============================================================
# 01_setup_and_process.R
# Purpose: Complete pipeline — config, download, and process grids
#          Single self-contained script (no source() calls)
#          - Config & study area definitions (create_areas)
#          - Download raw CBS data (uploading_data)
#          - Build geometry & process grids (grid_setup)
# ============================================================

# ============================================================
# SECTION 1: CONFIG & SETUP
# ============================================================

# --- Root output directory ----------------------------------
output_dir <- "C:/NPRZ_project/data"

# --- Sub-directories ----------------------------------------
raw_dir       <- file.path(output_dir, "raw")
processed_dir <- file.path(output_dir, "processed")
export_dir    <- file.path(output_dir, "export")

# --- Study area definitions ---------------------------------
# SCALE 1: Rijnmond region — used for 100m², 500m², PC4, Buurt
gm_rijnmond <- c(
  "GM0489",   # Barendrecht
  "GM0502",   # Capelle aan den IJssel
  "GM0542",   # Krimpen aan den IJssel
  "GM0556",   # Maassluis
  "GM0597",   # Ridderkerk
  "GM0599",   # Rotterdam
  "GM0606",   # Schiedam
  "GM0613",   # Albrandswaard
  "GM0622",   # Vlaardingen
  "GM1621",   # Nissewaard
  "GM1723",   # Lansingerland
  "GM1924",   # Goeree-Overflakkee
  "GM1984"    # Voorne aan Zee
)

# SCALE 2: Zuid-Holland province
pv_zuidholland <- "PV28"

# SCALE 3: Rotterdam core
gm_rotterdam <- "GM0599"

# --- RD New bbox for Rijnmond (EPSG:28992) ----------------
bbox_rijnmond_rd <- list(
  xmin = 70000,
  xmax = 110000,
  ymin = 420000,
  ymax = 460000
)

# ============================================================
# SECTION 2: LIBRARIES
# ============================================================

library(sf)
library(arrow)
library(dplyr)
library(cbsodataR)

sf_use_s2(FALSE)

# ============================================================
# SECTION 3: DOWNLOAD DATA
# ============================================================

message("\n=== SECTION 3: Download raw data ===")

# --- Create directory structure ---------------------------
dirs <- c(raw_dir, processed_dir, export_dir,
          file.path(raw_dir, "grid_100m"),
          file.path(raw_dir, "grid_500m"),
          file.path(raw_dir, "pc4"),
          file.path(raw_dir, "kerncijfers"),
          file.path(raw_dir, "geometries"))

for (d in dirs) dir.create(d, recursive = TRUE, showWarnings = FALSE)

# --- Static CBS downloads: grids & PC4 -------------------
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
    message("Downloading: ", f$label, "...")
    download.file(f$url, destfile = f$dest, mode = "wb")
    message("  Saved to: ", f$dest)
  } else {
    message("Already exists, skipping: ", f$label)
  }
}

# --- Kerncijfers via OData ----------------------------------
kerncijfers_path <- file.path(raw_dir, "kerncijfers", "kwb_2024_raw.rds")

if (!file.exists(kerncijfers_path)) {
  message("Downloading Kerncijfers wijken en buurten 2024 via OData (85984NED)...")
  kwb_raw <- cbs_get_data("85984NED")
  saveRDS(kwb_raw, kerncijfers_path)
  message("  Saved: ", kerncijfers_path)
  message("  Total rows: ", nrow(kwb_raw))
} else {
  message("Kerncijfers already downloaded, skipping.")
}

# ============================================================
# SECTION 4: BUILD BOUNDARY & PROCESS GRIDS
# ============================================================

message("\n=== SECTION 4: Build geometry boundary ===")

# --- Download gemeente geometry if needed -----------------
geom_extract <- file.path(raw_dir, "geometries", "wijkbuurt_2024")
gpkg_geom    <- list.files(geom_extract, pattern = "\\.gpkg$",
                           full.names = TRUE, recursive = TRUE)[1]

if (is.na(gpkg_geom) || !file.exists(gpkg_geom)) {
  message("  Geometry not found — downloading...")
  geom_zip <- file.path(raw_dir, "geometries", "wijkbuurt_2024.zip")
  download.file(
    "https://geodata.cbs.nl/files/Wijkenbuurtkaart/WijkBuurtkaart_2024_v2.zip",
    destfile = geom_zip, mode = "wb"
  )
  unzip(geom_zip, exdir = geom_extract, overwrite = FALSE)
  gpkg_geom <- list.files(geom_extract, pattern = "\\.gpkg$",
                          full.names = TRUE, recursive = TRUE)[1]
  message("  Available layers: ", paste(st_layers(gpkg_geom)$name, collapse = ", "))
}

# --- Read gemeente layer & build Rijnmond boundary -------
gemeenten <- st_read(gpkg_geom, layer = "gemeenten", quiet = TRUE)

rijnmond_boundary <- gemeenten |>
  filter(gemeentecode %in% gm_rijnmond) |>
  st_union() |>
  st_make_valid()

message("  Rijnmond boundary built from ",
        sum(gemeenten$gemeentecode %in% gm_rijnmond),
        " municipalities")

bb <- st_bbox(rijnmond_boundary)
message("  Actual Rijnmond bbox (RD New):")
message("    xmin=", round(bb["xmin"]), " xmax=", round(bb["xmax"]),
        " ymin=", round(bb["ymin"]), " ymax=", round(bb["ymax"]))

# ============================================================
# SECTION 5: GRID PROCESSING FUNCTION
# ============================================================

process_grid <- function(zip_path, resolution_label, boundary_polygon, id_col) {

  message("\n--- Processing ", resolution_label, " grid ---")

  # Unzip
  extract_dir <- file.path(raw_dir, paste0("grid_", resolution_label), "extracted")
  dir.create(extract_dir, recursive = TRUE, showWarnings = FALSE)
  unzip(zip_path, exdir = extract_dir, overwrite = FALSE)

  gpkg_file <- list.files(extract_dir, pattern = "\\.gpkg$",
                          full.names = TRUE, recursive = TRUE)[1]
  message("  Reading full NL grid: ", basename(gpkg_file))
  grid_nl <- st_read(gpkg_file, quiet = TRUE)
  message("  Full NL rows: ", nrow(grid_nl))

  # --- Pass 1: loose bbox pre-filter -------------------------
  bb  <- st_bbox(boundary_polygon)
  buf <- 2000  # metres

  centroids <- st_centroid(grid_nl)
  coords    <- st_coordinates(centroids)

  grid_rough <- grid_nl[
    coords[, "X"] >= (bb["xmin"] - buf) &
      coords[, "X"] <= (bb["xmax"] + buf) &
      coords[, "Y"] >= (bb["ymin"] - buf) &
      coords[, "Y"] <= (bb["ymax"] + buf),
  ]
  message("  After bbox pre-filter: ", nrow(grid_rough), " rows")

  # --- Pass 2: precise polygon clip --------------------------
  # Use st_filter to get cells whose centroid falls within boundary
  # --- Pass 2: precise polygon clip (strict centroid-within) -
  boundary_polygon_proj <- st_transform(boundary_polygon, st_crs(grid_rough))
  
  # Use centroid-within rather than polygon-intersects
  # prevents large border cells bleeding outside Rijnmond
  grid_cents <- st_centroid(grid_rough)
  inside     <- st_within(grid_cents, boundary_polygon_proj, sparse = FALSE)[, 1]
  grid_rijnmond <- grid_rough[inside, ]

  message("  After municipality polygon clip: ", nrow(grid_rijnmond), " rows")

  # Skip if no cells in study area
  if (nrow(grid_rijnmond) == 0) {
    message("  WARNING: No grid cells found in study area!")
    return(list(res = resolution_label, rows = 0, cols_kept = 0, cols_dropped = 0))
  }

  # --- Drop suppressed columns -----
  data_only <- st_drop_geometry(grid_rijnmond)
  suppressed_cols <- names(data_only)[
    sapply(data_only, function(col) {
      is.numeric(col) && all(col == -99995, na.rm = TRUE)
    })
  ]
  message("  Suppressed columns in study area (dropping): ",
          length(suppressed_cols))

  grid_clean <- grid_rijnmond |> select(-all_of(suppressed_cols))
  message("  Columns kept: ", ncol(grid_clean) - 1, " (excl. geometry)")

  # --- Reproject to WGS84 ------------------------------------
  grid_wgs84 <- st_transform(grid_clean, 4326)

  # --- Export: geometry GeoJSON (centroid points, ID only) ---
  grid_cents <- st_centroid(grid_wgs84)
  geom_only  <- st_sf(
    setNames(list(grid_cents[[id_col]]), id_col),
    geometry = st_geometry(grid_cents),
    crs = 4326
  )
  geojson_path <- file.path(export_dir,
                             paste0("grid_", resolution_label, "_rijnmond.geojson"))
  st_write(geom_only, geojson_path, driver = "GeoJSON", delete_dsn = TRUE)
  message("  Exported geometry GeoJSON: ", geojson_path)

  # --- Export: stats parquet (all variables, no geometry) ----
  stats_df   <- st_drop_geometry(grid_wgs84) |> as.data.frame()
  stats_path <- file.path(export_dir,
                           paste0("grid_", resolution_label, "_rijnmond_stats.parquet"))
  write_parquet(stats_df, stats_path)
  message("  Exported stats parquet: ", stats_path)

  list(res = resolution_label, rows = nrow(grid_rijnmond),
       cols_kept    = ncol(grid_clean) - 1,
       cols_dropped = length(suppressed_cols))
}

# ============================================================
# SECTION 6: PROCESS BOTH RESOLUTIONS
# ============================================================
results <- list(
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
# SECTION 7: SUMMARY
# ============================================================

message("\n========================================")
message("=== PIPELINE COMPLETE ===")
message("========================================\n")

message("Output Directory: ", output_dir)
message("\nGrid Processing Summary:")
for (r in results) {
  message("  ", r$res, ": ", r$rows, " cells | ", r$cols_kept,
          " vars kept | ", r$cols_dropped, " dropped")
}
message("\nExport files in: ", export_dir)
message("Processed files in: ", processed_dir)

# ============================================================
# SECTION 8: ADMINISTRATIVE SETUP (Buurt / Wijk / Gemeente)
# ============================================================

message("\n=== SECTION 8: Administrative scales ===")

library(stringr)

# --- Load raw Kerncijfers -----------------------------------
message("Loading Kerncijfers 2024...")
kwb_raw <- readRDS(file.path(raw_dir, "kerncijfers", "kwb_2024_raw.rds"))

# Strip trailing whitespace — CBS OData quirk
kwb_raw <- kwb_raw |>
  mutate(WijkenEnBuurten = str_trim(WijkenEnBuurten))

# Split by administrative level
buurt_nl    <- kwb_raw |> filter(str_starts(WijkenEnBuurten, "BU"))
wijk_nl     <- kwb_raw |> filter(str_starts(WijkenEnBuurten, "WK"))
gemeente_nl <- kwb_raw |> filter(str_starts(WijkenEnBuurten, "GM"))

message("Split: Buurt=", nrow(buurt_nl),
        " | Wijk=", nrow(wijk_nl),
        " | Gemeente=", nrow(gemeente_nl))

# --- Load geometry layers -----------------------------------
# gpkg_geom already set from Section 4
message("Reading geometry layers...")
geom_buurt    <- st_read(gpkg_geom, layer = "buurten",   quiet = TRUE)
geom_wijk     <- st_read(gpkg_geom, layer = "wijken",    quiet = TRUE)
geom_gemeente <- st_read(gpkg_geom, layer = "gemeenten", quiet = TRUE)

message("Geometry loaded:")
message("  Buurt:    ", nrow(geom_buurt),    " features")
message("  Wijk:     ", nrow(geom_wijk),     " features")
message("  Gemeente: ", nrow(geom_gemeente), " features")

# Print first 10 column names of each to confirm join keys
message("\nBuurt columns (first 10):    ",
        paste(names(geom_buurt)[1:10],    collapse = ", "))
message("Wijk columns (first 10):     ",
        paste(names(geom_wijk)[1:10],     collapse = ", "))
message("Gemeente columns (first 10): ",
        paste(names(geom_gemeente)[1:10], collapse = ", "))

# --- Helper: drop suppressed columns within a subset --------
drop_suppressed <- function(df, label) {
  numeric_cols <- df |> select(where(is.numeric))
  bad_cols <- names(numeric_cols)[
    sapply(numeric_cols, function(col) {
      all(col %in% c(-99995, -99997), na.rm = TRUE)
    })
  ]
  message("  ", label, ": dropping ", length(bad_cols),
          " suppressed cols, keeping ", ncol(df) - length(bad_cols))
  df |> select(-all_of(bad_cols))
}

# --- Helper: join stats to geometry, reproject, export ------
export_admin_scale <- function(stats_df,
                                geom_sf,
                                stats_code_col,
                                geom_code_col,
                                scale_label) {

  message("\n--- ", scale_label, " ---")
  message("  Stats rows: ", nrow(stats_df))
  message("  Geometry rows before filter: ", nrow(geom_sf))

  # Drop suppressed columns independently per scale
  stats_clean <- drop_suppressed(stats_df, scale_label)

  # Filter geometry to only codes in stats (massive speedup)
  codes_in_stats <- unique(st_drop_geometry(stats_clean)[[stats_code_col]])
  geom_filtered <- geom_sf |>
    filter(.data[[geom_code_col]] %in% codes_in_stats)
  
  message("  Geometry rows after filter: ", nrow(geom_filtered))

  # Join stats onto filtered geometry by code column
  joined <- geom_filtered |>
    left_join(
      st_drop_geometry(stats_clean),
      by = setNames(stats_code_col, geom_code_col)
    ) |>
    st_transform(4326)

  message("  Features after join: ", nrow(joined))

# --- Export: geometry GeoJSON (ID + simplified polygon only) 
  geom_only    <- st_sf(
    setNames(list(joined[[geom_code_col]]), geom_code_col),
    geometry = st_geometry(joined),
    crs = 4326
  )
  geom_simple  <- st_simplify(geom_only, dTolerance = 0.001,
                               preserveTopology = TRUE)
  geom_simple  <- st_make_valid(geom_simple)
  geojson_path <- file.path(export_dir, paste0(scale_label, "_2024.geojson"))
  st_write(geom_simple, geojson_path, driver = "GeoJSON", delete_dsn = TRUE)
  message("  Exported geometry GeoJSON: ", geojson_path)

  # --- Export: stats parquet (all variables, no geometry) ----
  stats_df   <- st_drop_geometry(joined) |> as.data.frame()
  stats_path <- file.path(export_dir, paste0(scale_label, "_2024_stats.parquet"))
  write_parquet(stats_df, stats_path)
  message("  Exported stats parquet: ", stats_path)

  nrow(joined)
}

# --- Buurt: filter to Rijnmond municipalities ---------------
# Buurt code: BU + 4-digit GM number + digits
# e.g. BU05990101 -> str_sub(,3,6) = "0599" = Rotterdam
rijnmond_nums <- str_extract(gm_rijnmond, "\\d+")

buurt_rijnmond <- buurt_nl |>
  filter(str_sub(WijkenEnBuurten, 3, 6) %in% rijnmond_nums)
message("Buurt rows in Rijnmond: ", nrow(buurt_rijnmond))

# --- Wijk & Gemeente: keep at national scale ----------------
# Simpler approach: no additional filtering needed
message("Wijk rows (national): ", nrow(wijk_nl))
message("Gemeente rows (national): ", nrow(gemeente_nl))

# --- Run all three scales -----------------------------------
n_buurt    <- export_admin_scale(buurt_rijnmond, geom_buurt,
                                  "WijkenEnBuurten", "buurtcode",    "buurt")
n_wijk     <- export_admin_scale(wijk_nl,         geom_wijk,
                                  "WijkenEnBuurten", "wijkcode",     "wijk")
n_gemeente <- export_admin_scale(gemeente_nl,      geom_gemeente,
                                  "WijkenEnBuurten", "gemeentecode", "gemeente")

message("\nAdmin summary:")
message("  Buurt (Rijnmond):       ", n_buurt,    " features")
message("  Wijk (Zuid-Holland):    ", n_wijk,     " features")
message("  Gemeente (all NL):      ", n_gemeente, " features")

# ============================================================
# SECTION 9: PC4 SETUP
# ============================================================

message("\n=== SECTION 9: PC4 ===")

pc4_zip     <- file.path(raw_dir, "pc4", "cbs_pc4_2024.zip")
pc4_extract <- file.path(raw_dir, "pc4", "extracted")
dir.create(pc4_extract, showWarnings = FALSE, recursive = TRUE)
unzip(pc4_zip, exdir = pc4_extract, overwrite = FALSE)

pc4_gpkg <- list.files(pc4_extract, pattern = "\\.gpkg$",
                         full.names = TRUE, recursive = TRUE)[1]
message("Reading: ", basename(pc4_gpkg))
message("Available layers: ",
        paste(st_layers(pc4_gpkg)$name, collapse = ", "))

pc4 <- st_read(pc4_gpkg, quiet = TRUE)
message("Full NL rows: ", nrow(pc4), " | Columns: ", ncol(pc4))

# --- Download PC6 lookup table for province mapping ---------
pc6_lookup_path <- file.path(raw_dir, "pc4", "pc6_lookup.zip")
if (!file.exists(pc6_lookup_path)) {
  message("Downloading PC6 lookup file for province mapping...")
  download.file(
    "https://download.cbs.nl/postcode/2023-cbs-pc6huisnr20230801_buurt_20250225.zip",
    destfile = pc6_lookup_path,
    mode = "wb"
  )
}

# Extract and read PC6 lookup to get PC4-province mapping
pc6_extract <- file.path(raw_dir, "pc4", "pc6_lookup_extracted")
dir.create(pc6_extract, showWarnings = FALSE, recursive = TRUE)
unzip(pc6_lookup_path, exdir = pc6_extract, overwrite = FALSE)

# Find the data file (typically .csv or .parquet)
pc6_files <- list.files(pc6_extract, pattern = "\\.(csv|parquet)$",
                        full.names = TRUE, recursive = TRUE)
if (length(pc6_files) > 0) {
  message("Found PC6 lookup file: ", basename(pc6_files[1]))
  
  # Try to read; detect format
  if (grepl("\\.parquet$", pc6_files[1])) {
    pc6_lookup <- read_parquet(pc6_files[1])
  } else {
    pc6_lookup <- read.csv(pc6_files[1])
  }
  
  message("PC6 lookup rows: ", nrow(pc6_lookup))
  message("PC6 columns: ", paste(names(pc6_lookup)[1:10], collapse = ", "))
  
  # Extract unique PC4 codes with their provinces
  if ("PC4" %in% names(pc6_lookup) && 
      "provincienaam" %in% names(pc6_lookup)) {
    pc4_province <- pc6_lookup |>
      select(PC4, provincienaam) |>
      distinct()
    
    message("Unique PC4-province pairs: ", nrow(pc4_province))
    
    # Filter PC4 geometries to South Holland using lookup
    pc4_zh_lookup <- pc4_province |>
      filter(provincienaam == "Zuid-Holland") |>
      pull(PC4)
    
    message("PC4 codes in Zuid-Holland: ", length(pc4_zh_lookup))
    
    # Join with geometry
    pc4_zh <- pc4 |>
      filter(pc4 %in% pc4_zh_lookup)
    
    message("PC4 South Holland rows: ", nrow(pc4_zh))
  } else {
    message("Warning: PC6 lookup missing expected columns, using full PC4 dataset")
    pc4_zh <- pc4
  }
} else {
  message("PC6 lookup file not found, using full PC4 dataset")
  pc4_zh <- pc4
}

# --- Drop suppressed columns (ZH subset only) ---------------
pc4_clean <- drop_suppressed(pc4_zh, "PC4")

# --- Reproject and export -----------------------------------
pc4_wgs84 <- st_transform(pc4_clean, 4326)
  # Geometry GeoJSON — postcode + simplified polygon only
  geom_only   <- st_sf(
    postcode = pc4_wgs84[["postcode"]],
    geometry = st_geometry(pc4_wgs84),
    crs = 4326
  )
  geom_simple <- st_simplify(geom_only, dTolerance = 0.001,
                              preserveTopology = TRUE)
  geom_simple <- st_make_valid(geom_simple)
  st_write(geom_simple,
           file.path(export_dir, "pc4_zh_2024.geojson"),
           driver = "GeoJSON", delete_dsn = TRUE)
  message("  Exported PC4 geometry GeoJSON")

  # Stats parquet — all variables, no geometry
  stats_df <- st_drop_geometry(pc4_wgs84) |> as.data.frame()
  write_parquet(stats_df, file.path(export_dir, "pc4_zh_2024_stats.parquet"))
  message("  Exported PC4 stats parquet")

# ============================================================
# SECTION 10: EXPORT RIJNMOND BOUNDARY
# ============================================================

message("\n=== SECTION 10: Export Rijnmond boundary ===")

sf_use_s2(FALSE)

boundary_simple <- st_simplify(
  st_transform(rijnmond_boundary, 4326),
  dTolerance = 0.0005,
  preserveTopology = TRUE
)

st_write(
  st_sf(geometry = st_geometry(boundary_simple)),
  file.path(export_dir, "rijnmond_boundary.geojson"),
  driver = "GeoJSON",
  delete_dsn = TRUE
)

message("  Boundary exported: ",
        round(file.size(file.path(export_dir,
              "rijnmond_boundary.geojson"))/1024^2, 2), " MB")

message("\n=== Section 10 complete ===")

# Admin polygon files — simplify to reduce file size
# Grid files use centroids (points) so no simplification needed
simplify_geojson("buurt_2024.geojson",    tolerance = 0.0005)
simplify_geojson("wijk_2024.geojson",     tolerance = 0.002)
simplify_geojson("gemeente_2024.geojson", tolerance = 0.002)
simplify_geojson("pc4_zh_2024.geojson",  tolerance = 0.001)

message("\n=== Section 10 complete ===")

# ============================================================
# SECTION 11: FINAL SUMMARY
# ============================================================

message("\n========================================")
message("=== FULL PIPELINE COMPLETE ===")
message("========================================\n")
message("Export files ready in: ", export_dir)
message("\nFiles produced:")
message("  grid_100m_rijnmond.parquet  — ", 
        results[[1]]$rows, " cells")
message("  grid_500m_rijnmond.parquet  — ", 
        results[[2]]$rows, " cells")
message("  buurt_2024.parquet          — ", n_buurt,    " features")
message("  wijk_2024.parquet           — ", n_wijk,     " features")
message("  gemeente_2024.parquet       — ", n_gemeente, " features")
message("  pc4_zh_2024.parquet         — ", nrow(pc4_zh), " areas")
message("\nNext step: copy export/ contents to app-development/static/data/")
message("Then run npm run dev in app-development/\n")