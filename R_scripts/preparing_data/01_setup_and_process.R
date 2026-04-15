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

process_grid <- function(zip_path, resolution_label, boundary_polygon) {

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
  boundary_polygon_proj <- st_transform(boundary_polygon, st_crs(grid_rough))
  grid_rijnmond <- st_filter(grid_rough, boundary_polygon_proj)

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

  # --- Export GeoParquet ------------------------------------
  # Extract WKT first, avoiding the . pronoun issue
  geometry_wkt_col <- st_as_text(st_geometry(grid_wgs84))
  
  grid_df <- cbind(
    st_drop_geometry(grid_wgs84),
    geometry_wkt = geometry_wkt_col
  ) |> as.data.frame()

  parquet_path <- file.path(export_dir,
                            paste0("grid_", resolution_label, "_rijnmond.parquet"))
  write_parquet(grid_df, parquet_path)
  message("  Exported parquet: ", parquet_path)

  # --- Export RDS ---
  rds_path <- file.path(processed_dir,
                        paste0("grid_", resolution_label, "_rijnmond.rds"))
  saveRDS(grid_wgs84, rds_path)
  message("  Exported RDS: ", rds_path)

  list(res = resolution_label, rows = nrow(grid_rijnmond),
       cols_kept  = ncol(grid_clean) - 1,
       cols_dropped = length(suppressed_cols))
}

# ============================================================
# SECTION 6: PROCESS BOTH RESOLUTIONS
# ============================================================

message("\n=== SECTION 6: Process grid resolutions ===")

results <- list(
  process_grid(
    zip_path         = file.path(raw_dir, "grid_100m", "cbs_vk100_2024.zip"),
    resolution_label = "100m",
    boundary_polygon = rijnmond_boundary
  ),
  process_grid(
    zip_path         = file.path(raw_dir, "grid_500m", "cbs_vk500_2024.zip"),
    resolution_label = "500m",
    boundary_polygon = rijnmond_boundary
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

  # Export GeoParquet
  geom_wkt_col <- st_as_text(st_geometry(joined))
  joined_df <- cbind(
    st_drop_geometry(joined),
    geometry_wkt = geom_wkt_col
  ) |> as.data.frame()

  parquet_out <- file.path(export_dir,
                            paste0(scale_label, "_2024.parquet"))
  write_parquet(joined_df, parquet_out)
  message("  Exported parquet: ", parquet_out)

  # Export RDS
  rds_out <- file.path(processed_dir,
                        paste0(scale_label, "_2024.rds"))
  saveRDS(joined, rds_out)
  message("  Exported RDS: ", rds_out)

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

pc4_wkt <- st_as_text(st_geometry(pc4_wgs84))
pc4_df  <- cbind(
  st_drop_geometry(pc4_wgs84),
  geometry_wkt = pc4_wkt
) |> as.data.frame()

write_parquet(pc4_df, file.path(export_dir, "pc4_zh_2024.parquet"))
saveRDS(pc4_wgs84, file.path(processed_dir, "pc4_zh_2024.rds"))
message("Exported PC4: ", nrow(pc4_zh), " areas")
message("Note: Attribution required — © CBS, © ESRI Nederland")

# ============================================================
# SECTION 10: GEOMETRY CLEANUP & SPATIAL FILTERING
# Purpose: Fix duplicate columns, filter invalid rows,
#          simplify gemeente/wijk geometry, apply spatial
#          cutoffs per scale. Run after all exports complete.
# ============================================================

message("\n=== SECTION 10: Geometry cleanup & spatial filtering ===")

sf_use_s2(FALSE)  # Use GEOS engine — tolerant of imperfect CBS geometries

# --- Helper: clean one parquet file --------------------------
clean_parquet <- function(filename, 
                           filter_negative_inwoners = FALSE,
                           zh_bbox_filter = FALSE,
                           simplify_tolerance = NULL) {
  
  path <- file.path(export_dir, filename)
  temp <- file.path(export_dir, paste0("TEMP_", filename))
  
  df <- arrow::read_parquet(path)
  message("  ", filename, ": ", nrow(df), " rows | ", ncol(df), " cols | ",
          round(file.size(path)/1024^2, 2), " MB")
  
  # Drop CamelCase_N duplicate columns from geometry+stats join
  camel_cols <- names(df)[grepl("_\\d+$", names(df))]
  if (length(camel_cols) > 0) {
    df <- df[, !names(df) %in% camel_cols]
    message("    Dropped ", length(camel_cols), " duplicate cols -> ", ncol(df), " remain")
  }
  
  # Filter out CBS suppressed rows (-99997) using aantal_inwoners
  if (filter_negative_inwoners && "aantal_inwoners" %in% names(df)) {
    before <- nrow(df)
    df <- df[!is.na(df$aantal_inwoners) & df$aantal_inwoners > 0, ]
    message("    Removed ", before - nrow(df), " suppressed rows -> ", nrow(df), " remain")
  }
  
  # Spatial filter to Zuid-Holland bbox (WGS84)
  if (zh_bbox_filter) {
    sf_obj <- st_as_sf(df, wkt = "geometry_wkt", crs = 4326)
    sf_obj <- st_make_valid(sf_obj)
    pts    <- st_point_on_surface(sf_obj)
    coords <- st_coordinates(pts)
    keep   <- coords[,"X"] >= 3.8 & coords[,"X"] <= 5.2 &
              coords[,"Y"] >= 51.6 & coords[,"Y"] <= 52.4
    df <- df[keep, ]
    message("    After ZH filter: ", nrow(df), " rows")
  }
  
  # Simplify geometry — reduces file size for coarser scales
  if (!is.null(simplify_tolerance)) {
    sf_obj     <- st_as_sf(df, wkt = "geometry_wkt", crs = 4326)
    sf_obj     <- st_make_valid(sf_obj)
    sf_simple  <- st_simplify(sf_obj, dTolerance = simplify_tolerance,
                               preserveTopology = TRUE)
    df <- cbind(
      st_drop_geometry(sf_simple),
      geometry_wkt = st_as_text(st_geometry(sf_simple))
    ) |> as.data.frame()
    message("    After simplify (", simplify_tolerance, "deg): ",
            round(object.size(df$geometry_wkt)/1024^2, 2), " MB WKT")
  }
  
  # Write to temp then rename (avoids Arrow file lock)
  arrow::write_parquet(df, temp)
  rm(df); gc()
  file.rename(temp, path)
  
  message("    Final: ", nrow(arrow::read_parquet(path)), " rows | ",
          round(file.size(path)/1024^2, 2), " MB")
}

# --- Buurt: dedup columns only (already Rijnmond-filtered) ---
clean_parquet("buurt_2024.parquet",
              filter_negative_inwoners = TRUE)

# --- Wijk: dedup + ZH filter + simplify ----------------------
clean_parquet("wijk_2024.parquet",
              filter_negative_inwoners = TRUE,
              zh_bbox_filter           = TRUE,
              simplify_tolerance       = 0.001)

# --- Gemeente: dedup + remove -99997 rows + simplify ---------
clean_parquet("gemeente_2024.parquet",
              filter_negative_inwoners = TRUE,
              simplify_tolerance       = 0.002)

# --- PC4: dedup + ZH filter ----------------------------------
clean_parquet("pc4_zh_2024.parquet",
              zh_bbox_filter = TRUE)

# --- Grids: no cleanup needed --------------------------------
message("  Grid files unchanged (no duplicates, already clipped)")

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
message("\nNext step: copy export/ contents to cbs-map/static/data/")
message("Then build the SvelteKit scale-switcher.")



wijk <- arrow::read_parquet(file.path(export_dir, "wijk_2024.parquet"))
cat("Wijk NAs in first numeric col:", 
    sum(is.na(wijk[[names(wijk)[3]]]), na.rm = TRUE), "\n")
cat("Wijk total rows:", nrow(wijk), "\n")
cat("Wijk columns:", ncol(wijk), "\n")

# Also print column names so we know what variables are available
cat("Column names:\n")
print(names(wijk))


wijk <- arrow::read_parquet(file.path(export_dir, "wijk_2024.parquet"))
cat("Wijk NAs in first numeric col:", 
    sum(is.na(wijk[[names(wijk)[3]]]), na.rm = TRUE), "\n")
cat("Wijk total rows:", nrow(wijk), "\n")
cat("Wijk columns:", ncol(wijk), "\n")