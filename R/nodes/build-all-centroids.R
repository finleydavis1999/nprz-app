# build-all-centroids.R
# -----------------------------------------------------------------------------
# Population-weighted centroids at PC4 / buurt / gemeente. Reuses the cbs-vk100.R
# cell method: CBS 100m grid cell centre = SW-corner code +50 m in RD; weight by
# aantal_inwoners within each polygon. One grid read, one function, three calls.
# -----------------------------------------------------------------------------

library(sf)
library(dplyr)
library(jsonlite)

GRID <- "raw-data/cbs/grid_100m_2024/cbs_vk100_2024_v1.gpkg"
WB   <- "raw-data/cbs/wijkbuurtkaart_2024/WijkBuurtkaart_2024_v2/wijkenbuurten_2024_v2.gpkg"

# --- read 100m cells once: population + reconstructed RD centre -----------------
message("reading 100m grid...")
glayer <- st_layers(GRID)$name[1]
cells <- st_read(GRID, query = sprintf(
  "SELECT crs28992res100m, aantal_inwoners FROM \"%s\"", glayer), quiet = TRUE)
cells <- st_drop_geometry(cells)
cells$aantal_inwoners <- suppressWarnings(as.numeric(cells$aantal_inwoners))
cells$aantal_inwoners[is.na(cells$aantal_inwoners) | cells$aantal_inwoners < 0] <- 0
e <- as.numeric(sub("^E(\\d+)N(\\d+)$", "\\1", cells$crs28992res100m))
n <- as.numeric(sub("^E(\\d+)N(\\d+)$", "\\2", cells$crs28992res100m))
cells$.x <- e * 100 + 50
cells$.y <- n * 100 + 50
cells <- cells[!is.na(cells$.x) & !is.na(cells$.y), ]
cell_sf <- st_as_sf(cells, coords = c(".x", ".y"), crs = 28992)
message("  ", nrow(cell_sf), " cells, ", sum(cell_sf$aantal_inwoners), " inhabitants")

# --- reusable: population-weighted centroids for a polygon set ------------------
build_weighted <- function(polys, code_col, out) {
  polys <- polys |>
    transmute(area_code = as.character(.data[[code_col]])) |>
    st_transform(28992)
  joined <- st_join(cell_sf, polys, join = st_within)
  xy <- st_coordinates(joined)
  tab <- st_drop_geometry(joined)
  tab$.x <- xy[, 1]; tab$.y <- xy[, 2]
  tab <- tab[!is.na(tab$area_code), ]

  wc <- tab |>
    group_by(area_code) |>
    summarise(
      x = if (sum(aantal_inwoners) > 0) weighted.mean(.x, aantal_inwoners) else mean(.x),
      y = if (sum(aantal_inwoners) > 0) weighted.mean(.y, aantal_inwoners) else mean(.y),
      .groups = "drop")

  ll <- wc |> st_as_sf(coords = c("x", "y"), crs = 28992) |> st_transform(4326)
  co <- st_coordinates(ll)
  res <- setNames(lapply(seq_len(nrow(wc)),
                         function(i) c(co[i, 1], co[i, 2])), wc$area_code)
  write_json(res, out, auto_unbox = TRUE, digits = 6)
  message("  wrote ", out, " (", length(res), " areas)")
  invisible(wc)
}

# --- PC4 (app's own boundaries; already validated) ------------------------------
pc4 <- st_read("static/data/geo/pc4.geojson", quiet = TRUE)
build_weighted(pc4, "area_code", "static/data/geo/pc4-centroids-weighted.json")

# --- buurt (drop water-only buurten, which have no population) -------------------
buurt <- st_read(WB, layer = "buurten", quiet = TRUE)
if ("water" %in% names(buurt)) {
  buurt <- buurt[is.na(buurt$water) | !(buurt$water %in% c("JA", "Ja", "ja", "1", 1)), ]
}
build_weighted(buurt, "buurtcode", "static/data/geo/buurt-centroids-weighted.json")

# --- gemeente -------------------------------------------------------------------
gem <- st_read(WB, layer = "gemeenten", quiet = TRUE)
build_weighted(gem, "gemeentecode", "static/data/geo/gem-centroids-weighted.json")

message("done: PC4, buurt, gemeente centroids written.")