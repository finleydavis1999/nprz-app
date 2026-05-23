# Gemeente polygons → simplified WGS84 GeoJSON + simplified RD TopoJSON.
#
# Uses CBS gemeente_niet_gegeneraliseerd (342 features, 2025 boundaries) — the
# full-detail layer; the `gegeneraliseerd` layer is too coarse (median ~105
# vertices/polygon) and triangulates small coastal/island gemeenten when
# re-simplified. Polygons include water bodies (IJsselmeer, Markermeer,
# Oosterschelde, Waddenzee) as part of municipal territory.
suppressPackageStartupMessages({
  library(sf)
  library(dplyr)
  library(jsonlite)
})
source("R/lib/geo.R")

build_gemeenten <- function() {
  src <- "raw-data/geo-data/cbsgebiedsindelingen2025.gpkg"
  gem <- sf::read_sf(src, layer = "gemeente_niet_gegeneraliseerd") |>
    dplyr::transmute(area_code = statcode, name = statnaam)

  simplify_to_geojson_and_topojson(
    gem,
    geojson_out  = "static/data/geo/gemeenten.geojson",
    topojson_out = "static/data/geo/gemeenten.topo.json",
    keep_pct     = 12,
    precision    = 4
  )

  # Centroids (point-on-surface for safety with multi-part / coastal polygons).
  # Two coordinate systems are emitted:
  #   - EPSG:4326 (WGS84 lng/lat) — used by the live MapLibre flow layer.
  #   - EPSG:28992 (RD New)       — used by the print SVG flow rendering,
  #                                  which uses geoIdentity over RD-projected
  #                                  topojson and thus expects RD coords.
  pts84 <- gem |> sf::st_point_on_surface() |> sf::st_transform(4326)
  coords84 <- sf::st_coordinates(pts84)
  centroids84 <- setNames(
    lapply(seq_len(nrow(coords84)), function(i) c(coords84[i, "X"], coords84[i, "Y"])),
    pts84$area_code
  )
  out84 <- "static/data/geo/gem-centroids.json"
  dir.create(dirname(out84), recursive = TRUE, showWarnings = FALSE)
  jsonlite::write_json(centroids84, out84, auto_unbox = FALSE, digits = 6)
  cat("wrote", out84, "(", length(centroids84), "centroids )\n")

  pts_rd <- gem |> sf::st_point_on_surface() |> sf::st_transform(28992)
  coords_rd <- sf::st_coordinates(pts_rd)
  centroids_rd <- setNames(
    lapply(seq_len(nrow(coords_rd)), function(i) c(coords_rd[i, "X"], coords_rd[i, "Y"])),
    pts_rd$area_code
  )
  out_rd <- "static/data/geo/gem-centroids-rd.json"
  jsonlite::write_json(centroids_rd, out_rd, auto_unbox = FALSE, digits = 1)
  cat("wrote", out_rd, "(", length(centroids_rd), "centroids )\n")

  list(
    geojson     = "geo/gemeenten.geojson",
    topojson    = "geo/gemeenten.topo.json",
    centroids   = "geo/gem-centroids.json",
    centroidsRd = "geo/gem-centroids-rd.json",
    idProp      = "area_code"
  )
}
