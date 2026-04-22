# ============================================================
# 01_setup_and_process.R
# Full CBS data pipeline for NPRZ spatial analysis app
#
# Sections:
#   1.  Config & paths
#   2.  Libraries
#   3.  Download raw data (grids, PC4, Kerncijfers)
#   4.  Build study area boundaries
#   5.  Grid processing function
#   6.  Process 100m and 500m grids
#   7.  Grid summary
#   8.  Administrative scales (Buurt / Wijk / Gemeente)
#   9.  PC4 (middle boundary)
#   10. Export boundary files
#   11. Final summary
#
# Spatial boundaries:
#   Inner  (level1_full) : 12 Rijnmond municipalities
#   Middle               : broader metro area (irregular polygon)
#   Outer                : full NL (gemeente only)
#
# Outputs to app-development/static/data/
# ============================================================

# ============================================================
# SECTION 1: CONFIG & PATHS
# ============================================================

setwd("C:/NPRZ_project")
source("R_scripts/preparing_data/utils.R")

local_data_dir <- "C:/NPRZ_project/data"
raw_dir        <- file.path(local_data_dir, "raw")
processed_dir  <- file.path(local_data_dir, "processed")
export_dir     <- "C:/NPRZ_project/app-development/static/data"

# Inner boundary: 12 Rijnmond municipalities
gm_inner <- c(
  "GM0489",  # Barendrecht
  "GM0502",  # Capelle aan den IJssel
  "GM0542",  # Krimpen aan den IJssel
  "GM0556",  # Maassluis
  "GM0597",  # Ridderkerk
  "GM0599",  # Rotterdam
  "GM0606",  # Schiedam
  "GM0613",  # Albrandswaard
  "GM0622",  # Vlaardingen
  "GM1621",  # Lansingerland
  "GM1930",  # Nissewaard
  "GM1992"   # Voorne aan Zee
)

# Middle boundary: broader metro area (WGS84 lon/lat corners)
MIDDLE_POLYGON <- matrix(c(
  4.556747, 52.459138,   # NW
  5.242207, 52.386457,   # NE
  4.928226, 51.572253,   # SE
  3.613450, 51.651428,   # SW
  4.556747, 52.459138    # close polygon
), ncol = 2, byrow = TRUE)

# Sea clip: prevents Voorne coastal geometry extending into North Sea
SEA_CLIP_BBOX <- c(xmin = 3.85, ymin = 51.45, xmax = 5.10, ymax = 52.15)

# ============================================================
# SECTION 2: LIBRARIES
# ============================================================

library(sf)
library(arrow)
library(dplyr)
library(stringr)
library(cbsodataR)
library(jsonlite)

sf_use_s2(FALSE)

# Safe write helpers: delete before writing to avoid Windows memory-map lock
safe_write_parquet <- function(df, path) {
  tmp <- paste0(path, ".tmp")
  write_parquet(df, tmp)
  if (file.exists(path)) file.remove(path)
  file.rename(tmp, path)
  message("  Parquet: ", basename(path), " (",
          round(file.size(path)/1024, 1), " KB, ", nrow(df), " rows)")
}

safe_write_geojson <- function(sf_obj, path, layer_options = "ID_FIELD=id") {
  tmp <- paste0(path, ".tmp.geojson")
  if (length(layer_options) == 0) {
    st_write(sf_obj, tmp, driver = "GeoJSON", delete_dsn = TRUE, quiet = TRUE)
  } else {
    st_write(sf_obj, tmp, driver = "GeoJSON",
             layer_options = layer_options, delete_dsn = TRUE, quiet = TRUE)
  }
  if (file.exists(path)) file.remove(path)
  file.rename(tmp, path)
  message("  GeoJSON: ", basename(path), " (",
          round(file.size(path)/1024, 1), " KB, ", nrow(sf_obj), " features)")
}

# ============================================================
# SECTION 3: DOWNLOAD RAW DATA
# ============================================================

message("\n=== SECTION 3: Download raw data ===")

dirs <- c(raw_dir, processed_dir, export_dir,
          file.path(raw_dir, "grid_100m"), file.path(raw_dir, "grid_500m"),
          file.path(raw_dir, "pc4"), file.path(raw_dir, "kerncijfers"),
          file.path(raw_dir, "geometries"))
for (d in dirs) dir.create(d, recursive = TRUE, showWarnings = FALSE)

static_downloads <- list(
  list(label = "100m grid 2024",
       url   = "https://download.cbs.nl/vierkant/100/2025-cbs_vk100_2024_v1.zip",
       dest  = file.path(raw_dir, "grid_100m", "cbs_vk100_2024.zip")),
  list(label = "500m grid 2024",
       url   = "https://download.cbs.nl/vierkant/500/2025-cbs_vk500_2024_v1.zip",
       dest  = file.path(raw_dir, "grid_500m", "cbs_vk500_2024.zip")),
  list(label = "PC4 2024",
       url   = "https://download.cbs.nl/postcode/2025-cbs_pc4_2024_v1.zip",
       dest  = file.path(raw_dir, "pc4", "cbs_pc4_2024.zip"))
)

for (f in static_downloads) {
  if (!file.exists(f$dest)) {
    message("Downloading: ", f$label, " ...")
    download.file(f$url, destfile = f$dest, mode = "wb")
  } else {
    message("Already downloaded: ", f$label)
  }
}

kerncijfers_path <- file.path(raw_dir, "kerncijfers", "kwb_2024_raw.rds")
if (!file.exists(kerncijfers_path)) {
  message("Downloading Kerncijfers 2024 via OData ...")
  kwb_raw <- cbs_get_data("85984NED")
  saveRDS(kwb_raw, kerncijfers_path)
  message("  Saved: ", nrow(kwb_raw), " rows")
} else {
  message("Kerncijfers already downloaded")
}

# ============================================================
# SECTION 4: BUILD STUDY AREA BOUNDARIES
# ============================================================

message("\n=== SECTION 4: Build study area boundaries ===")

geom_extract <- file.path(raw_dir, "geometries", "wijkbuurt_2024")
gpkg_geom    <- list.files(geom_extract, pattern = "\\.gpkg$",
                            full.names = TRUE, recursive = TRUE)[1]

if (is.na(gpkg_geom) || !file.exists(gpkg_geom)) {
  message("Geometry not found — downloading ...")
  geom_zip <- file.path(raw_dir, "geometries", "wijkbuurt_2024.zip")
  download.file(
    "https://geodata.cbs.nl/files/Wijkenbuurtkaart/WijkBuurtkaart_2024_v2.zip",
    destfile = geom_zip, mode = "wb")
  unzip(geom_zip, exdir = geom_extract, overwrite = FALSE)
  gpkg_geom <- list.files(geom_extract, pattern = "\\.gpkg$",
                           full.names = TRUE, recursive = TRUE)[1]
}

# Load all geometry layers once
geom_gemeente <- st_read(gpkg_geom, layer = "gemeenten", quiet = TRUE)
geom_wijk     <- st_read(gpkg_geom, layer = "wijken",    quiet = TRUE)
geom_buurt    <- st_read(gpkg_geom, layer = "buurten",   quiet = TRUE)

message("Geometry loaded: ",
        nrow(geom_gemeente), " gemeente | ",
        nrow(geom_wijk), " wijk | ",
        nrow(geom_buurt), " buurt")

# Verify codes
missing_codes <- gm_inner[!gm_inner %in% geom_gemeente$gemeentecode]
if (length(missing_codes) > 0)
  stop("Missing gemeente codes: ", paste(missing_codes, collapse = ", "))

# Inner boundary in RD New (matches geometry CRS)
inner_boundary_rd <- geom_gemeente |>
  filter(gemeentecode %in% gm_inner) |>
  st_union() |> st_make_valid()

# Sea clip and inner boundary in WGS84
sea_clip <- st_bbox(SEA_CLIP_BBOX, crs = 4326) |> st_as_sfc()

inner_boundary_wgs84 <- inner_boundary_rd |>
  st_transform(4326) |>
  st_intersection(sea_clip) |>
  st_simplify(dTolerance = 0.0005, preserveTopology = TRUE) |>
  st_make_valid()

# Middle boundary in WGS84
middle_boundary_wgs84 <- st_polygon(list(MIDDLE_POLYGON)) |>
  st_sfc(crs = 4326)

message("  Inner boundary built from ", length(gm_inner), " municipalities")
message("  Inner bbox: ",
        paste(round(st_bbox(inner_boundary_wgs84), 3), collapse = ", "))

# ============================================================
# SECTION 5: GRID PROCESSING FUNCTION
# ============================================================

process_grid <- function(zip_path, resolution_label,
                          boundary_rd, id_col,
                          filter_populated = TRUE) {

  message("\n--- Processing ", resolution_label, " grid ---")

  extract_dir <- file.path(raw_dir, paste0("grid_", resolution_label), "extracted")
  dir.create(extract_dir, recursive = TRUE, showWarnings = FALSE)
  unzip(zip_path, exdir = extract_dir, overwrite = FALSE)

  gpkg_file <- list.files(extract_dir, pattern = "\\.gpkg$",
                           full.names = TRUE, recursive = TRUE)[1]
  grid_nl   <- st_read(gpkg_file, quiet = TRUE)
  message("  Full NL rows: ", nrow(grid_nl))

  # Pass 1: bbox pre-filter
  boundary_proj <- st_transform(boundary_rd, st_crs(grid_nl))
  bb  <- st_bbox(boundary_proj)
  buf <- 2000
  cents      <- st_coordinates(st_centroid(grid_nl))
  grid_rough <- grid_nl[
    cents[,"X"] >= (bb["xmin"]-buf) & cents[,"X"] <= (bb["xmax"]+buf) &
    cents[,"Y"] >= (bb["ymin"]-buf) & cents[,"Y"] <= (bb["ymax"]+buf), ]
  message("  After bbox filter: ", nrow(grid_rough))

  # Pass 2: centroid-within clip
  grid_cents <- st_centroid(grid_rough)
  inside     <- st_within(grid_cents, boundary_proj, sparse = FALSE)[,1]
  grid_clip  <- grid_rough[inside,]
  message("  After boundary clip: ", nrow(grid_clip))

  if (nrow(grid_clip) == 0) {
    message("  WARNING: No cells found"); return(NULL)
  }

  # Drop fully-suppressed columns
  data_only <- st_drop_geometry(grid_clip)
  supp_cols <- names(data_only)[sapply(data_only, function(col) {
    is.numeric(col) && all(col %in% c(-99995, -99997), na.rm = TRUE)
  })]
  grid_clean <- grid_clip |> select(-all_of(supp_cols))
  message("  Suppressed columns dropped: ", length(supp_cols))

  # Reproject and clean suppressed values
  grid_wgs84  <- st_transform(grid_clean, 4326)
  stats_clean <- drop_suppressed(st_drop_geometry(grid_wgs84),
                                  label = paste("grid", resolution_label))

  # Filter to populated cells only
  if (filter_populated && "aantal_inwoners" %in% names(stats_clean)) {
    pop_ids     <- stats_clean[[id_col]][
      !is.na(stats_clean$aantal_inwoners) & stats_clean$aantal_inwoners > 0]
    stats_clean <- stats_clean |> filter(.data[[id_col]] %in% pop_ids)
    grid_wgs84  <- grid_wgs84[grid_wgs84[[id_col]] %in% pop_ids,]
    message("  After population filter: ", nrow(grid_wgs84), " cells")
  }

  # Export centroid GeoJSON with rounded coordinates
  coords   <- st_coordinates(st_centroid(grid_wgs84))
  geom_pts <- st_sf(
    setNames(list(grid_wgs84[[id_col]]), id_col),
    geometry = st_sfc(
      lapply(seq_len(nrow(grid_wgs84)), function(i)
        st_point(round(c(coords[i,"X"], coords[i,"Y"]), 5))),
      crs = 4326)
  )
  geom_pts$id <- geom_pts[[id_col]]

  geojson_path <- file.path(export_dir,
                             paste0("grid_", resolution_label, "_rijnmond.geojson"))
  safe_write_geojson(geom_pts, geojson_path,
                     layer_options = c("ID_FIELD=id", "COORDINATE_PRECISION=5"))

  parquet_path <- file.path(export_dir,
                             paste0("grid_", resolution_label, "_rijnmond_stats.parquet"))
  safe_write_parquet(stats_clean, parquet_path)

  list(res = resolution_label, rows = nrow(grid_wgs84),
       cols_kept = ncol(grid_clean)-1, cols_dropped = length(supp_cols))
}

# ============================================================
# SECTION 6: PROCESS BOTH GRID RESOLUTIONS
# ============================================================

message("\n=== SECTION 6: Process grids ===")

grid_results <- list(
  process_grid(
    zip_path         = file.path(raw_dir, "grid_100m", "cbs_vk100_2024.zip"),
    resolution_label = "100m",
    boundary_rd      = inner_boundary_rd,
    id_col           = "crs28992res100m",
    filter_populated = TRUE
  ),
  process_grid(
    zip_path         = file.path(raw_dir, "grid_500m", "cbs_vk500_2024.zip"),
    resolution_label = "500m",
    boundary_rd      = inner_boundary_rd,
    id_col           = "crs28992res500m",
    filter_populated = TRUE
  )
)

# ============================================================
# SECTION 7: GRID SUMMARY
# ============================================================

message("\n=== SECTION 7: Grid summary ===")
for (r in grid_results) {
  if (!is.null(r))
    message("  ", r$res, ": ", r$rows, " cells | ",
            r$cols_kept, " vars | ", r$cols_dropped, " suppressed dropped")
}

# ============================================================
# SECTION 8: ADMINISTRATIVE SCALES
# ============================================================

message("\n=== SECTION 8: Administrative scales ===")

kwb_raw <- readRDS(kerncijfers_path) |>
  mutate(WijkenEnBuurten = str_trim(WijkenEnBuurten))

buurt_nl    <- kwb_raw |> filter(str_starts(WijkenEnBuurten, "BU"))
wijk_nl     <- kwb_raw |> filter(str_starts(WijkenEnBuurten, "WK"))
gemeente_nl <- kwb_raw |> filter(str_starts(WijkenEnBuurten, "GM"))

message("Kerncijfers: Buurt=", nrow(buurt_nl),
        " | Wijk=", nrow(wijk_nl),
        " | Gemeente=", nrow(gemeente_nl))

process_admin_scale <- function(stats_df, geom_sf,
                                 stats_id_col, geom_id_col,
                                 scale_name,
                                 clip_boundary      = NULL,
                                 simplify_tolerance = 0.001) {
  message("\n--- ", scale_name, " ---")

  # Clean stats: drop suppressed values and raw OData CamelCase columns
  stats_clean <- drop_suppressed(stats_df, label = scale_name) |>
    select(-matches("_\\d+$"))

  valid_codes <- unique(stats_clean[[stats_id_col]])
  geom_sub    <- geom_sf |> filter(.data[[geom_id_col]] %in% valid_codes)

  # Spatial clip
  if (!is.null(clip_boundary)) {
    clip_proj <- st_transform(clip_boundary, st_crs(geom_sub))
    geom_sub  <- geom_sub |> st_filter(clip_proj)
    valid_codes <- geom_sub[[geom_id_col]]
    stats_clean <- stats_clean |> filter(.data[[stats_id_col]] %in% valid_codes)
    message("  After boundary clip: ", nrow(geom_sub), " features")
  }

  # Remove water-only features
  if ("water" %in% names(geom_sub)) {
    n_before <- nrow(geom_sub)
    geom_sub <- geom_sub |> filter(is.na(water) | water != "WATER")
    if (n_before > nrow(geom_sub))
      message("  Water features removed: ", n_before - nrow(geom_sub))
    valid_codes <- geom_sub[[geom_id_col]]
    stats_clean <- stats_clean |> filter(.data[[stats_id_col]] %in% valid_codes)
  }

  message("  Final features: ", nrow(geom_sub))

  # Join and reproject
  joined <- geom_sub |>
    left_join(stats_clean, by = setNames(stats_id_col, geom_id_col)) |>
    st_transform(4326)

  # Export GeoJSON — geometry + id only
  geom_only <- st_sf(
    setNames(list(joined[[geom_id_col]]), geom_id_col),
    geometry = st_geometry(joined),
    crs = 4326
  )
  geom_only <- st_simplify(geom_only, dTolerance = simplify_tolerance,
                             preserveTopology = TRUE) |>
    st_make_valid()
  geom_only$id <- geom_only[[geom_id_col]]

  safe_write_geojson(geom_only,
                     file.path(export_dir, paste0(scale_name, ".geojson")))

  # Export stats parquet — no geometry
  safe_write_parquet(
    st_drop_geometry(joined) |> as.data.frame(),
    file.path(export_dir, paste0(scale_name, "_stats.parquet"))
  )

  nrow(joined)
}

# Buurt: filter stats to inner area before processing
rijnmond_nums  <- str_extract(gm_inner, "\\d+")
buurt_rijnmond <- buurt_nl |>
  filter(str_sub(WijkenEnBuurten, 3, 6) %in% rijnmond_nums)
message("Buurt rows in inner area: ", nrow(buurt_rijnmond))

n_buurt <- process_admin_scale(
  buurt_rijnmond, geom_buurt,
  "WijkenEnBuurten", "buurtcode", "buurt_2024",
  clip_boundary      = inner_boundary_rd,
  simplify_tolerance = 0.0005
)

n_wijk <- process_admin_scale(
  wijk_nl, geom_wijk,
  "WijkenEnBuurten", "wijkcode", "wijk_2024",
  clip_boundary      = middle_boundary_wgs84,
  simplify_tolerance = 0.001
)

n_gemeente <- process_admin_scale(
  gemeente_nl, geom_gemeente,
  "WijkenEnBuurten", "gemeentecode", "gemeente_2024",
  clip_boundary      = NULL,
  simplify_tolerance = 0.002
)

message("\nAdmin summary:")
message("  buurt_2024    (inner):  ", n_buurt,    " features")
message("  wijk_2024     (middle): ", n_wijk,     " features")
message("  gemeente_2024 (NL):     ", n_gemeente, " features")

# ============================================================
# SECTION 9: PC4 (MIDDLE BOUNDARY)
# ============================================================

message("\n=== SECTION 9: PC4 ===")

pc4_zip     <- file.path(raw_dir, "pc4", "cbs_pc4_2024.zip")
pc4_extract <- file.path(raw_dir, "pc4", "extracted")
dir.create(pc4_extract, showWarnings = FALSE, recursive = TRUE)
unzip(pc4_zip, exdir = pc4_extract, overwrite = FALSE)

pc4_gpkg <- list.files(pc4_extract, pattern = "\\.gpkg$",
                        full.names = TRUE, recursive = TRUE)[1]
pc4_nl   <- st_read(pc4_gpkg, quiet = TRUE)
message("Full NL PC4: ", nrow(pc4_nl), " areas")

# Clip to middle boundary
middle_proj <- st_transform(middle_boundary_wgs84, st_crs(pc4_nl))
pc4_clip    <- pc4_nl |> st_filter(middle_proj)
message("After middle boundary clip: ", nrow(pc4_clip))

# Clean and export
pc4_stats <- drop_suppressed(st_drop_geometry(pc4_clip), label = "pc4")
pc4_sf    <- st_sf(pc4_stats,
                   geometry = st_geometry(pc4_clip),
                   crs      = st_crs(pc4_clip)) |>
  st_transform(4326)

geom_pc4 <- st_sf(
  postcode = pc4_sf$postcode,
  geometry = st_geometry(pc4_sf),
  crs = 4326
)
geom_pc4 <- st_simplify(geom_pc4, dTolerance = 0.001,
                          preserveTopology = TRUE) |>
  st_make_valid()
geom_pc4$id <- geom_pc4$postcode

safe_write_geojson(geom_pc4,
                   file.path(export_dir, "pc4_zh_2024.geojson"))
safe_write_parquet(pc4_stats,
                   file.path(export_dir, "pc4_zh_2024_stats.parquet"))

message("PC4 exported: ", nrow(pc4_sf), " areas")

# ============================================================
# SECTION 10: EXPORT BOUNDARY FILES
# ============================================================

message("\n=== SECTION 10: Export boundary files ===")

# Main boundary line (inner = level1_full)
safe_write_geojson(
  st_sf(geometry = st_geometry(inner_boundary_wgs84)),
  file.path(export_dir, "rotterdam_boundary.geojson"),
  layer_options = character(0)
)

# Four boundary levels
boundary_levels <- list(
  level1_full      = gm_inner,
  level2_mid       = c("GM0489","GM0502","GM0542","GM0556","GM0597",
                        "GM0599","GM0606","GM0613","GM0622","GM1621"),
  level3_core      = c("GM0489","GM0597","GM0599","GM0613"),
  level4_rotterdam = c("GM0599")
)

for (name in names(boundary_levels)) {
  codes <- boundary_levels[[name]]
  b <- geom_gemeente |>
    filter(gemeentecode %in% codes) |>
    st_union() |> st_make_valid() |>
    st_transform(4326) |>
    st_intersection(sea_clip) |>
    st_simplify(dTolerance = 0.0005, preserveTopology = TRUE) |>
    st_make_valid()
  outpath <- file.path(export_dir, paste0("boundary_", name, ".geojson"))
  tmp     <- paste0(outpath, ".tmp.geojson")
  st_write(st_sf(geometry = st_geometry(b)), tmp,
           driver = "GeoJSON", delete_dsn = TRUE, quiet = TRUE)
  if (file.exists(outpath)) file.remove(outpath)
  file.rename(tmp, outpath)
  message("  ", name, ": ", length(codes), " municipalities, ",
          round(file.size(outpath)/1024, 1), " KB")
}

# Gemeente centroids for flow line rendering
cents_coords <- st_coordinates(st_centroid(st_transform(geom_gemeente, 4326)))
centroids    <- data.frame(
  id  = geom_gemeente$gemeentecode,
  lng = round(cents_coords[,"X"], 6),
  lat = round(cents_coords[,"Y"], 6)
)
# Rotterdam: manually corrected (geometric centroid pulled south by Rozenburg)
centroids$lng[centroids$id == "GM0599"] <- 4.479448
centroids$lat[centroids$id == "GM0599"] <- 51.928931

write_json(centroids,
           file.path(export_dir, "gemeente_centroids.json"),
           dataframe = "rows")
message("  gemeente_centroids.json: ", nrow(centroids), " centroids")

# ============================================================
# SECTION 11: FINAL SUMMARY
# ============================================================

message("\n========================================")
message("=== FULL PIPELINE COMPLETE ===")
message("========================================")
message("\nAll files written to: ", export_dir)

files <- list.files(export_dir, full.names = TRUE)
sizes <- data.frame(file = basename(files),
                    kb   = round(file.size(files)/1024, 1)) |>
  arrange(desc(kb))
message("\nTop 10 files by size:")
print(head(sizes, 10))

message("\nNext: run npm run dev in app-development/ to verify the app loads")
