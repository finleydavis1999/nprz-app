# Contextual cartographic overlays -> simplified WGS84 GeoJSON + RD TopoJSON.
#
#   - provinces: CBS provincie_gegeneraliseerd (12 features, 2025 boundaries)
#   - builtup:   Top10NL "Plaats kern" built-up areas (2904 polygons, 2018)
#
# Each emits a GeoJSON (for the live MapLibre map) and a paired TopoJSON (for
# the d3-geo print/export path), like the pc4 / gemeenten geometry.
suppressPackageStartupMessages({
  library(sf)
  library(dplyr)
})
source("R/lib/geo.R")

build_provincies <- function() {
  src <- "raw-data/geo-data/cbsgebiedsindelingen2025.gpkg"
  pv <- sf::read_sf(src, layer = "provincie_gegeneraliseerd") |>
    dplyr::transmute(area_code = statcode, name = statnaam)

  simplify_to_geojson_and_topojson(
    pv,
    geojson_out  = "static/data/geo/provincies.geojson",
    topojson_out = "static/data/geo/provincies.topo.json",
    keep_pct     = 15
  )

  list(
    geojson  = "geo/provincies.geojson",
    topojson = "geo/provincies.topo.json",
    idProp   = "area_code"
  )
}

build_builtup <- function() {
  src <- "raw-data/top10_buildup/Top10NL-Plaats_kern.shp"
  # Drop all attributes — this renders as one uniform fill — and repair any
  # invalid input geometry before the dissolve.
  bu <- sf::read_sf(src)
  bu <- sf::st_sf(geometry = sf::st_make_valid(sf::st_geometry(bu)))

  # ~11 m coordinate precision is invisible at the zoom levels a national
  # built-up overlay is viewed at, and keeps the GeoJSON around ~1.5 MB.
  simplify_to_geojson_and_topojson(
    bu,
    geojson_out  = "static/data/geo/builtup.geojson",
    topojson_out = "static/data/geo/builtup.topo.json",
    keep_pct     = 5,
    dissolve     = TRUE,
    precision    = 4
  )

  list(
    geojson  = "geo/builtup.geojson",
    topojson = "geo/builtup.topo.json"
  )
}
