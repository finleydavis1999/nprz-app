# Buurt polygons → simplified WGS84 GeoJSON + simplified RD TopoJSON.
#
# Uses CBS buurt_niet_gegeneraliseerd (14 729 features, 2025 boundaries) — the
# full-detail layer; the `gegeneraliseerd` layer is too coarse (median 10
# vertices/polygon at source — many buurten are already triangles before any
# further simplification). 14729 polygons make this the heaviest geo file
# even at keep_pct 20 (~22 MB geojson, gzipped to ~5 MB on the wire).
suppressPackageStartupMessages({
  library(sf)
  library(dplyr)
  library(jsonlite)
})
source("R/lib/geo.R")

build_buurt <- function() {
  src <- "raw-data/geo-data/cbsgebiedsindelingen2025.gpkg"
  buurt <- sf::read_sf(src, layer = "buurt_niet_gegeneraliseerd") |>
    dplyr::transmute(area_code = statcode, name = statnaam)

  simplify_to_geojson_and_topojson(
    buurt,
    geojson_out  = "static/data/geo/buurt.geojson",
    topojson_out = "static/data/geo/buurt.topo.json",
    keep_pct     = 20,
    precision    = 4
  )

  # Centroids in two CRSs — see R/geo/gemeenten.R for rationale.
  pts84 <- buurt |> sf::st_point_on_surface() |> sf::st_transform(4326)
  coords84 <- sf::st_coordinates(pts84)
  centroids84 <- setNames(
    lapply(seq_len(nrow(coords84)), function(i) c(coords84[i, "X"], coords84[i, "Y"])),
    pts84$area_code
  )
  out84 <- "static/data/geo/buurt-centroids.json"
  dir.create(dirname(out84), recursive = TRUE, showWarnings = FALSE)
  jsonlite::write_json(centroids84, out84, auto_unbox = FALSE, digits = 6)
  cat("wrote", out84, "(", length(centroids84), "centroids )\n")

  pts_rd <- buurt |> sf::st_point_on_surface() |> sf::st_transform(28992)
  coords_rd <- sf::st_coordinates(pts_rd)
  centroids_rd <- setNames(
    lapply(seq_len(nrow(coords_rd)), function(i) c(coords_rd[i, "X"], coords_rd[i, "Y"])),
    pts_rd$area_code
  )
  out_rd <- "static/data/geo/buurt-centroids-rd.json"
  jsonlite::write_json(centroids_rd, out_rd, auto_unbox = FALSE, digits = 1)
  cat("wrote", out_rd, "(", length(centroids_rd), "centroids )\n")

  list(
    geojson     = "geo/buurt.geojson",
    topojson    = "geo/buurt.topo.json",
    centroids   = "geo/buurt-centroids.json",
    centroidsRd = "geo/buurt-centroids-rd.json",
    idProp      = "area_code"
  )
}
