# PC4 polygons → simplified WGS84 GeoJSON + simplified RD TopoJSON.
suppressPackageStartupMessages({
  library(sf)
  library(dplyr)
  library(jsonlite)
})
source("R/lib/geo.R")

build_pc4 <- function() {
  src <- "raw-data/geo-data/cbs_pc4_2024_v1.gpkg"
  pc4 <- sf::read_sf(src, layer = "cbs_pc4_2024") |>
    dplyr::transmute(area_code = sprintf("%04d", as.integer(postcode)))

  simplify_to_geojson_and_topojson(
    pc4,
    geojson_out  = "static/data/geo/pc4.geojson",
    topojson_out = "static/data/geo/pc4.topo.json",
    keep_pct     = 15,
    precision    = 4
  )

  # Centroids in two CRSs — see R/geo/gemeenten.R for rationale.
  pts84 <- pc4 |> sf::st_point_on_surface() |> sf::st_transform(4326)
  coords84 <- sf::st_coordinates(pts84)
  centroids84 <- setNames(
    lapply(seq_len(nrow(coords84)), function(i) c(coords84[i, "X"], coords84[i, "Y"])),
    pts84$area_code
  )
  out84 <- "static/data/geo/pc4-centroids.json"
  dir.create(dirname(out84), recursive = TRUE, showWarnings = FALSE)
  jsonlite::write_json(centroids84, out84, auto_unbox = FALSE, digits = 6)
  cat("wrote", out84, "(", length(centroids84), "centroids )\n")

  pts_rd <- pc4 |> sf::st_point_on_surface() |> sf::st_transform(28992)
  coords_rd <- sf::st_coordinates(pts_rd)
  centroids_rd <- setNames(
    lapply(seq_len(nrow(coords_rd)), function(i) c(coords_rd[i, "X"], coords_rd[i, "Y"])),
    pts_rd$area_code
  )
  out_rd <- "static/data/geo/pc4-centroids-rd.json"
  jsonlite::write_json(centroids_rd, out_rd, auto_unbox = FALSE, digits = 1)
  cat("wrote", out_rd, "(", length(centroids_rd), "centroids )\n")

  list(
    geojson     = "geo/pc4.geojson",
    topojson    = "geo/pc4.topo.json",
    centroids   = "geo/pc4-centroids.json",
    centroidsRd = "geo/pc4-centroids-rd.json",
    idProp      = "area_code"
  )
}
