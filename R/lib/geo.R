# Simplify an sf polygon dataset and write paired outputs:
#  - <name>.geojson   (WGS84, simplified)            for MapLibre
#  - <name>.topo.json (input CRS, simplified)        for d3-geo print
#
# Uses rmapshaper for simplification (V8-bundled mapshaper), sf for the
# WGS84 GeoJSON write, and geojsonio for the TopoJSON write.
#
#  - `dissolve = TRUE` merges every feature into one before simplifying:
#    removes internal seams / overlap double-blending, and shrinks the file.
#  - `precision` (decimal degrees) sets the GeoJSON COORDINATE_PRECISION to
#    quantize coordinates; left full-float when NULL. 6 ~ 0.1 m, 4 ~ 11 m.
suppressPackageStartupMessages({
  library(sf)
  library(rmapshaper)
  library(geojsonio)
})

simplify_to_geojson_and_topojson <- function(x, geojson_out, topojson_out, keep_pct,
                                             dissolve = FALSE, precision = NULL) {
  stopifnot(inherits(x, "sf"))
  stopifnot(!is.na(sf::st_crs(x)))
  dir.create(dirname(geojson_out), recursive = TRUE, showWarnings = FALSE)

  if (dissolve) x <- rmapshaper::ms_dissolve(x)

  simplified <- rmapshaper::ms_simplify(
    x,
    keep        = keep_pct / 100,
    keep_shapes = TRUE,
    method      = "vis"   # Visvalingam (mapshaper default)
  ) |> sf::st_make_valid()

  # TopoJSON: kept in source CRS (RD/EPSG:28992) for the d3-geo print path.
  # Quantize to ~3 m precision (NL bbox is ~300 km, 3e5 / 1e5 = 3 m); without
  # quantization the file is ~2x larger because coords serialize as full floats.
  if (file.exists(topojson_out)) file.remove(topojson_out)
  geojsonio::topojson_write(
    simplified,
    file         = topojson_out,
    object_name  = sub("\\..*$", "", basename(topojson_out)),
    quantization = 1e5
  )

  # GeoJSON: WGS84 for MapLibre. `precision` quantizes coordinates when set.
  layer_opts <- if (is.null(precision)) character(0) else paste0("COORDINATE_PRECISION=", precision)
  sf::st_write(
    sf::st_transform(simplified, 4326),
    geojson_out, delete_dsn = TRUE, quiet = TRUE,
    layer_options = layer_opts
  )

  cat(
    "wrote", geojson_out, "(", file.size(geojson_out) %/% 1024, "KB ),",
    topojson_out, "(", file.size(topojson_out) %/% 1024, "KB )\n"
  )
}
