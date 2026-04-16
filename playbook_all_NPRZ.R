install.packages("sf", type = "binary")
library(sf)

basic_square_data <- st_read("C:\\Users\\finle\\Downloads\\cbs_vk100_2024_v1.gpkg")

#translating a row
library(deeplr)
my_key <- "59f55501-d037-4a25-abd6-0ec2b2e5609a:fx"
translated_names <- sapply(names(data), function(term) {
  Sys.sleep(0.5)  # wait 0.5 seconds between each call
  term_spaced <- gsub("_", " ", term)
  res <- POST("https://api-free.deepl.com/v2/translate",
              body = list(text = term, target_lang = "EN", source_lang = "NL", auth_key = my_key),
              encode = "form")
  content(res)$translations[[1]]$text
})
english_basic_square_data <- basic_square_data
names(english_basic_square_data) <- translated_names

#saving and reuploading key sets to then compare them

st_write(english_basic_square_data, "C:/Users/finle/Downloads/cbs_vk100_2024_english.gpkg")
onekm_square_data <- st_read("C:/Users/finle/Downloads/cbs_vk100_2024_english.gpkg")
untranslated_onekm_square_data <- st_read("C:\\Users\\finle\\Downloads\\cbs_vk100_2024_v1.gpkg")
fivekm_square_data <- st_read("C:\\Users\\finle\\Downloads\\cbs_vk500_2024_v1.gpkg")

setdiff(names(untranslated_onekm_square_data), names(fivekm_square_data))
setdiff(names(fivekm_square_data), names(untranslated_onekm_square_data))


# =============================================================
# playbook_prepare_grid.R
# Purpose: Load CBS 100m grid, remove suppressed columns,
#          export to DuckDB and GeoParquet
# Data source: CBS Vierkantstatistieken 2024 (vk100)
# CRS: EPSG:28992 (RD New)
# Author: finley davis
# Date: 26 March 2026
# =============================================================
install.packages(c("sf", "dplyr"))  
install.packages("duckdb", repos = "https://duckdb.r-universe.dev")
install.packages("arrow", repos = "https://apache.r-universe.dev")

library(sf)
library(duckdb)
library(arrow)
library(dplyr)

zip_path <- "C:/Users/finle/Downloads/2025-cbs_vk100_2024_v1.zip"
output_dir  <- "C:/NPRZ_project/first_data"

#creating folder to output to (first_data), unzipping file, importing & inspecting it
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE) ##this actually did nothing because file in path under "output_dir" does already exist, would create it at this location otherwise
unzip(zip_path, exdir = file.path(output_dir, "cbs_raw"))


gpkg_path <- file.path(output_dir, "cbs_raw", "cbs_vk100_2024_v1.gpkg")
grid <- st_read(gpkg_path)

cat("\n--- Basic info ---\n")
cat("Rows (grid squares):", nrow(grid), "\n")
cat("Columns:", ncol(grid), "\n")
cat("CRS:", st_crs(grid)$input, "\n")
cat("\nColumn names:\n")
print(names(grid))

cat("\nFirst few rows:\n")
print(head(grid, 3))

#filtering empty columns
suppressed_cols <- names(grid)[
  sapply(st_drop_geometry(grid), function(col) {
    all(col == -99995, na.rm = TRUE)
  })
]

cat("Dropping", length(suppressed_cols), "fully suppressed columns:\n")
print(suppressed_cols)

grid_filtered <- grid %>%
  dplyr::select(-all_of(suppressed_cols))

#to inspect map
plot(st_geometry(grid_filtered),
     main = "CBS 100m grid - full Netherlands",
     col  = "steelblue",
     border = NA)

#saving in two different formats
duckdb_path <- file.path(output_dir, "basic_grid.duckdb") 

con <- dbConnect(duckdb(), duckdb_path) #establishes duckDB connection to my duckDB database (duck files in my project folder)
dbExecute(con, "INSTALL spatial;")
dbExecute(con, "LOAD spatial;")

grid_for_db <- grid_filtered %>%
  dplyr::mutate(geom_wkt = st_as_text(geom)) %>%
  st_drop_geometry() #here we replace the gpkg geometry column in place of a format (as text, not type st type understood by sf) understood by duckDB as well 

dbWriteTable(con, "cbs_grid", grid_for_db, overwrite = TRUE) #applies created duck connection to selected/created table to create duckDB table, through using con which links this new duckDB connected/queried table to the duck db "filing cabinet" of "basic_data" saved within the project folder 

cat("\nRows in DuckDB:", dbGetQuery(con, "SELECT COUNT(*) FROM cbs_grid")[[1]], "\n") #this is actually running a simple fetch rows SQL query to my duck database(the file in my project folder with the earlier defined "duckdp_path" path), we can see its working. "cat" and [[1]] just means it prints one row and doesnt create an object, dbgetquery means it executes the SQL format query but retrieves using the duckDB engine (and way of receiving the data), which is in standard SQL format following con,(with con representing the path to our duck file)
dbDisconnect(con) #closes the connection to the file, while it is open the file is not accessible so make sure to close, opens when con is ran because we saved this function as a duckdb connection function
cat("DuckDB saved to:", duckdb_path, "\n") #prints file destination, this is stil basic_grid, we have now attached the cbs_grid table inside that duck file, can have multiple tables inside this DuckDB file



geoparquet_path <- file.path(output_dir, "basic_grid.parquet") ##this and below same as last, except this parquet file path is just to one file it doesn't work like duckDB with its query-able database of multiple files, then mutating geomtery column to be understandble format for parquet file, then making sure its saved in right place

grid_parquet <- grid_filtered %>%
  dplyr::mutate(geometry_wkt = st_as_text(geom)) %>%
  st_drop_geometry() %>%
  as.data.frame()

write_parquet(grid_parquet, geoparquet_path)
cat("GeoParquet saved to:", geoparquet_path, "\n")


cat("\n=== Done ===\n")
cat("Grid cells:       ", nrow(grid_filtered), "\n")
cat("Variables kept:   ", ncol(grid_filtered) - 1, "(excl. geometry)\n")
cat("Variables dropped:", length(suppressed_cols), "(all -99995)\n")
cat("DuckDB:           ", duckdb_path, "\n")
cat("GeoParquet:       ", geoparquet_path, "\n")
cat("CRS:               EPSG:28992 (RD New)\n")



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

# ============================================================
# create_areas.R (1)
# Purpose: Shared paths, GM codes, and study area definitions
#          Source this at the top of every other script
# ============================================================

# --- Root output directory ----------------------------------
output_dir <- "C:/NPRZ_project/first_data"

# --- Sub-directories (created by 01_download.R) -------------
raw_dir       <- file.path(output_dir, "raw")
processed_dir <- file.path(output_dir, "processed")
export_dir    <- file.path(output_dir, "export")   # final parquets for app

# --- Study area definitions ---------------------------------

# SCALE 1: Rijnmond region — used for 100m², 500m², PC4, Buurt
# Full GGD/DCMR Rijnmond gemeenten (2024 codes, no herindeling in 2024)
gm_rijnmond <- c(
  "GM0489",  # Barendrecht
  "GM0502",  # Capelle aan den IJssel
  "GM0542",  # Krimpen aan den IJssel
  "GM0556",  # Maassluis
  "GM0597",  # Ridderkerk
  "GM0599",  # Rotterdam          ← core study area
  "GM0606",  # Schiedam
  "GM0613",  # Albrandswaard
  "GM0622",  # Vlaardingen
  "GM1621",  # Nissewaard         (formed 2015 from Spijkenisse + Bernisse)
  "GM1723",  # Lansingerland      (formed 2007)
  "GM1924",  # Voorne aan Zee     (formed 2023 from Brielle+Hellevoetsluis+Westvoorne)
  "GM0530",  # Goeree-Overflakkee (formed 2013, included in GGD Rijnmond)
  "GM1924"   # Voorne aan Zee     (double-check — see note below)
)
# Note on Nissewaard: GM1621, confirmed formed Jan 2015
# Note on Voorne aan Zee: GM1984 — let's verify below; 
# formed Jan 2023 from Brielle (GM0501), Hellevoetsluis (GM0530->moved), Westvoorne (GM0614)  # nolint: line_length_linter.
# Goeree-Overflakkee: GM1924 — formed 2013

# CORRECTED — splitting out uncertain codes:
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

# SCALE 2: Zuid-Holland province — used for Wijk
# Province code PV28; filter via Kerncijfers GemeentecodeGM prefix list
# rather than hardcoding ~50 gemeente codes — see 03_kerncijfers.R

pv_zuidholland <- "PV28"

# SCALE 3: Gemeente — keep all NL (~342 gemeenten), dataset is tiny

# --- Rotterdam core (for masking / focused analysis later) --
gm_rotterdam <- "GM0599"


# ============================================================
# uploading_data.R (2)
# Purpose: Download all CBS raw data files
#          Safe to re-run — skips existing files
# Run once, then leave raw/ folder untouched
# ============================================================

library(cbsodataR)

# --- Create directory structure -----------------------------
dirs <- c(raw_dir, processed_dir, export_dir,
          file.path(raw_dir, "grid_100m"),
          file.path(raw_dir, "grid_500m"),
          file.path(raw_dir, "pc4"),
          file.path(raw_dir, "kerncijfers"),
          file.path(raw_dir, "geometries"))

for (d in dirs) dir.create(d, recursive = TRUE, showWarnings = FALSE)

# --- Static CBS downloads -----------------------------------
# These are NOT in the OData catalogue — direct zip downloads only

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
# One dataset covers Buurt + Wijk + Gemeente — split by code prefix

kerncijfers_path <- file.path(raw_dir, "kerncijfers", "kwb_2024_raw.rds")

if (!file.exists(kerncijfers_path)) {
  message("Downloading Kerncijfers 2024 via OData (85615NED)...")
  kwb_raw <- cbs_get_data("85615NED")
  saveRDS(kwb_raw, kerncijfers_path)
  message("  Saved: ", kerncijfers_path)
  message("  Total rows: ", nrow(kwb_raw))
} else {
  message("Kerncijfers already downloaded, skipping.")
}

message("\n=== 01_download.R complete ===")
message("Raw files in: ", raw_dir)



# ============================================================
# grid_setup.R (3)
# Purpose: Process CBS 100m² and 500m² grid files
#          - Unzip GeoPackages
#          - Two-pass filter: loose bbox THEN precise municipality polygon clip
#          - Drop columns suppressed within study area only
#          - Reproject to WGS84, export GeoParquet + RDS
# ============================================================
# Declare global variables sourced from create_areas.R
utils::globalVariables(c("raw_dir", "processed_dir", "export_dir",
                         "output_dir", "gm_rijnmond", "gm_rotterdam",
                         "pv_zuidholland", "bbox_rijnmond_rd", "."))

library(sf)
library(arrow)
library(dplyr)

# --- Build Rijnmond union polygon from wijk/buurt geometry ---
# We use the gemeente layer (already downloaded by 03 or available
# separately) to get exact municipal boundaries

message("Building Rijnmond union boundary...")

geom_extract <- file.path(raw_dir, "geometries", "wijkbuurt_2024")
gpkg_geom    <- list.files(geom_extract, pattern = "\\.gpkg$",
                           full.names = TRUE, recursive = TRUE)[1]

# If geometry not yet downloaded, do it now
if (is.na(gpkg_geom) || !file.exists(gpkg_geom)) {
  message("  Geometry not found — downloading wijk/buurt/gemeente layer...")
  geom_zip <- file.path(raw_dir, "geometries", "wijkbuurt_2024.zip")
  download.file(
    "https://geodata.cbs.nl/files/Wijkenbuurtkaart/WijkBuurtkaart_2024_v2.zip",
    destfile = geom_zip, mode = "wb"
  )
  unzip(geom_zip, exdir = geom_extract, overwrite = FALSE)
  gpkg_geom <- list.files(geom_extract, pattern = "\\.gpkg$",
                          full.names = TRUE, recursive = TRUE)[1]
  
  # Print available layers — run this once to confirm names
  message("Available layers: ", paste(st_layers(gpkg_geom)$name, collapse = ", "))
}


# Read gemeente layer (stays in RD New / EPSG:28992)
gemeenten <- st_read(gpkg_geom, layer = "gemeenten", quiet = TRUE)

# Filter to Rijnmond gemeente codes and union into one polygon
# This is the precise study area boundary for grid clipping
rijnmond_boundary <- gemeenten |>
  filter(gemeentecode %in% gm_rijnmond) |>
  st_union() |>                   # dissolve internal boundaries
  st_make_valid()                  # guard against topology issues

message("  Rijnmond boundary built from ",
        sum(gemeenten$gemeentecode %in% gm_rijnmond),
        " municipalities")

# Quick sanity check — print the actual bbox so you can inspect it
bb <- st_bbox(rijnmond_boundary)
message("  Actual Rijnmond bbox (RD New):")
message("    xmin=", round(bb["xmin"]), " xmax=", round(bb["xmax"]),
        " ymin=", round(bb["ymin"]), " ymax=", round(bb["ymax"]))

# --- Helper: process one grid resolution ---------------------
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
  # Cheap row reduction before the expensive spatial operation
  # Use bbox of the Rijnmond boundary + 2km buffer
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
  # st_filter keeps only cells whose centroid falls within the boundary
  # Using centroids is correct for grid cells — avoids partial border squares
  centroids_rough <- st_centroid(grid_rough)
  in_boundary     <- st_filter(centroids_rough, boundary_polygon)
  grid_rijnmond   <- grid_rough[row.names(grid_rough) %in%
                                  row.names(in_boundary), ]
  
  message("  After municipality polygon clip: ", nrow(grid_rijnmond), " rows")
  
  # --- Drop suppressed columns (within study area only) ------
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
  
  # --- Export GeoParquet -------------------------------------
  grid_df <- grid_wgs84 |>
    mutate(geometry_wkt = st_as_text(st_geometry(.))) |>
    st_drop_geometry() |>
    as.data.frame()
  
  parquet_path <- file.path(export_dir,
                            paste0("grid_", resolution_label, "_rijnmond.parquet"))
  write_parquet(grid_df, parquet_path)
  message("  Exported parquet: ", parquet_path)
  
  # --- Export RDS (retains sf geometry for R) ----------------
  rds_path <- file.path(processed_dir,
                        paste0("grid_", resolution_label, "_rijnmond.rds"))
  saveRDS(grid_wgs84, rds_path)
  message("  Exported RDS: ", rds_path)
  
  list(res = resolution_label, rows = nrow(grid_rijnmond),
       cols_kept  = ncol(grid_clean) - 1,
       cols_dropped = length(suppressed_cols))
}

# --- Run for both resolutions --------------------------------
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

message("\n=== 02_process_grids.R complete ===")
for (r in results) {
  message(r$res, ": ", r$rows, " cells | ", r$cols_kept,
          " vars kept | ", r$cols_dropped, " dropped")
}


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
output_dir <- "C:/NPRZ_project/first_data"

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
  message("Downloading Kerncijfers 2024 via OData (85615NED)...")
  kwb_raw <- cbs_get_data("85615NED")
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



#####code from 8 on  not working, I suspect using wrong dataset that doesn't actually have these codes/names

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
output_dir <- "C:/NPRZ_project/first_data"

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
  message("Downloading Kerncijfers 2024 via OData (85615NED)...")
  kwb_raw <- cbs_get_data("85615NED")
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
# SECTION 10: FINAL SUMMARY
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
