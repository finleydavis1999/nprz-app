# =============================================================================
# testprovince.R -- OPTIONAL: run the pipeline on a single province
# =============================================================================
# A development aid, not part of the build. It runs the SAME code path as
# ttbuildnational.R scoped to one province, so a full download -> network ->
# matrix -> verify cycle finishes in well under an hour on a laptop. Useful if
# you want to see the pipeline work end to end before committing a machine to
# the national run. You do not need this to build the national matrix.
#
#   setwd("<project root>")
#   HEAP <- "-Xmx11G"                       # laptop-sized
#   source("R/traveltimes/ttbuildnational.R")
#   build_test(c("gem","pc4","buurt"))
#
# It sources ttbuildnational.R rather than copying it, so the two cannot drift.
# All paths redirect to *_test directories; real inputs and outputs are never
# touched. ASCII only.
# =============================================================================

TEST_PROVINCE <- "Zuid-Holland"     # any name in PROV_CODE below

.tp <- tolower(TEST_PROVINCE)
NET_DIR     <- paste0("D:/NPRZ_net_test/", .tp)
OUT_DIR     <- paste0("D:/NPRZ_tt_out_test/", .tp)
PARQUET_DIR <- paste0("D:/NPRZ_tt_out_test/", .tp, "/parquet")

source("R/traveltimes/ttbuildnational.R")

PROV_GEO  <- "static/data/geo/provincies.geojson"
PROV_CODE <- c(Groningen="PV20", Friesland="PV21", Drenthe="PV22",
               Overijssel="PV23", Flevoland="PV24", Gelderland="PV25",
               Utrecht="PV26", `Noord-Holland`="PV27", `Zuid-Holland`="PV28",
               Zeeland="PV29", `Noord-Brabant`="PV30", Limburg="PV31")
PROV_SLUG <- c(Groningen="groningen", Friesland="friesland", Drenthe="drenthe",
               Overijssel="overijssel", Flevoland="flevoland",
               Gelderland="gelderland", Utrecht="utrecht",
               `Noord-Holland`="noord-holland", `Zuid-Holland`="zuid-holland",
               Zeeland="zeeland", `Noord-Brabant`="noord-brabant",
               Limburg="limburg")

fetch_inputs_test <- function(dir = NET_DIR) {
  options(timeout = 7200)
  slug <- PROV_SLUG[[TEST_PROVINCE]]
  if (is.null(slug)) stop("unknown province: ", TEST_PROVINCE)
  osm <- file.path(dir, paste0(slug, ".osm.pbf"))
  .get_file(sprintf("https://download.geofabrik.de/europe/netherlands/%s-latest.osm.pbf",
                    slug), osm, 1e7)
  .get_file("https://gtfs.ovapi.nl/nl/gtfs-nl.zip",
            file.path(dir, "gtfs-nl.zip"), 5e7)
  others <- setdiff(list.files(dir, pattern="\\.osm\\.pbf$", full.names=TRUE), osm)
  if (length(others)) file.remove(others)
  invisible(basename(osm))
}

points_in_province <- function(scale) {
  pts  <- load_points(CENTROIDS[[scale]])
  prov <- sf::st_read(PROV_GEO, quiet = TRUE)
  df   <- sf::st_drop_geometry(prov)
  code <- PROV_CODE[[TEST_PROVINCE]] %||% "___none___"

  hit <- rep(FALSE, nrow(prov))
  for (cl in names(df)) {
    v <- as.character(df[[cl]])
    if (all(is.na(v))) next
    hit <- hit | ((!is.na(v)) & (grepl(TEST_PROVINCE, v, ignore.case=TRUE) | v == code))
  }
  if (!any(hit)) { print(utils::head(df, 3)); stop("province not matched in ", PROV_GEO) }

  p    <- sf::st_as_sf(pts, coords=c("lon","lat"), crs=4326)
  keep <- lengths(sf::st_within(p, sf::st_transform(prov[hit,], 4326))) > 0
  message(sprintf("  TEST: %d of %d %s points inside %s",
                  sum(keep), nrow(pts), scale, TEST_PROVINCE))
  pts[keep, ]
}

# The network must be rebuilt whenever TEST_PROVINCE changes, or the previous
# province's network.dat is silently reused and almost nothing routes.
build_test <- function(scales = c("gem","pc4"), rebuild = TRUE) {
  cat("\n########## TEST MODE:", TEST_PROVINCE, "##########\n")
  check_requirements(scales)
  fetch_inputs_test()
  depart <- resolve_depart(dir = NET_DIR)
  core <- build_national_network(dir = NET_DIR, rebuild = rebuild)

  for (sc in scales) {
    pts <- points_in_province(sc)
    snap_report(core, sc, points = pts)
    run_scale(core, sc, depart, points = pts)
    df <- assemble_scale(sc, points = pts)
    verify_scale(df, sc, expected_ids = pts$id)
    rm(df); gc()
  }
  cat("\nTEST COMPLETE -- outputs in ", PARQUET_DIR, "\n",
      "This proves correctness, not capacity: a clean province run says nothing\n",
      "about whether the national network fits in RAM.\n", sep="")
}