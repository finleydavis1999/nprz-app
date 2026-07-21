# =============================================================================
# build-centroid.R -- population-weighted centroids for PC4 / buurt / gemeente
# =============================================================================
# Produces the centroid files the travel-time matrix uses as origins/destinations:
#     static/data/geo/{pc4,buurt,gem}-centroids-weighted.json
#
# HOW TO RUN (from the project root):
#     setwd("C:/NPRZ_project")
#     source("R/traveltimes/build-centroids.R")
#     cents <- build_all_centroids()             # JSON + static PNG maps
#
#     # optional interactive checks (needs: install.packages("mapview"))
#     inspect_fallbacks(cents$pc4, "pc4")        # only the fallback points
#     inspect_all(cents$gem, "gem")              # every point, any scale
#     find_centroid(cents$pc4, "3088")           # look up one area
#
# Run BEFORE the travel-time build -- that reads these files.
#
# -----------------------------------------------------------------------------
# CENTROID HIERARCHY (each centroid carries a `source` flag)
#   (a)  population-weighted mean of CBS 100m cell centres      [people live here]
#   (a2) unweighted mean of CBS cell centres                    [cells but no
#        recorded population -- excludes water/empty land, and catches areas
#        whose few residents were suppressed by CBS's <5-inhabitant rule]
#   (b)  area-weighted centroid of the built-up parts           [no cells, but
#        built land -- e.g. port / industrial areas]
#   (c)  st_point_on_surface(polygon)                           [genuinely empty:
#        water, nature, tidal flats]
#   (d)  CONTAINMENT SNAP -- any centroid falling outside its own polygon
#        (possible whenever a shape is concave, crescent, ring-like or split by
#        water) is snapped to the nearest populated cell inside it. Suffix
#        "-snapped" on the source flag.
#
# Every polygon in the app's geo file gets exactly one centroid; nothing is
# dropped. All three scales use the app's OWN shapefiles, so centroids always
# correspond to the polygons the app renders.
#
# NOTE ON JOBS-WEIGHTING: we cannot weight by jobs *within* a polygon. Job data
# is a count PER AREA, not a spatial distribution inside it. Fallbacks (a2)/(b)
# are the best available proxy for "where the activity is" until LISA provides
# actual job locations.
#
# ASCII only (non-ASCII breaks webR's R parser elsewhere in this repo).
# =============================================================================


# --------------------------- CONFIG ------------------------------------------

GRID    <- "raw-data/cbs/grid_100m_2024/cbs_vk100_2024_v1.gpkg"
BUILTUP <- "static/data/geo/builtup.geojson"        # optional
FIG_DIR <- "figs/centroids"                         # root-level output

GEO <- c(pc4   = "static/data/geo/pc4.geojson",
         buurt = "static/data/geo/buurt.geojson",
         gem   = "static/data/geo/gemeenten.geojson")

OUT <- c(pc4   = "static/data/geo/pc4-centroids-weighted.json",
         buurt = "static/data/geo/buurt-centroids-weighted.json",
         gem   = "static/data/geo/gem-centroids-weighted.json")


# --------------------- REQUIREMENTS CHECK -------------------------------------

check_requirements <- function() {
  need <- c("sf", "dplyr", "jsonlite", "ggplot2")
  missing <- need[!vapply(need, requireNamespace, logical(1), quietly = TRUE)]
  if (length(missing))
    stop("missing R packages. Install with:\n    install.packages(c(",
         paste(sprintf('"%s"', missing), collapse = ", "), "))")
  if (!dir.exists("static/data/geo"))
    stop("run from the project root -- 'static/data/geo' not found in ", getwd())
  for (f in GEO) if (!file.exists(f)) stop("missing app geo file: ", f)
  if (!file.exists(GRID))
    stop("missing CBS 100m grid: ", GRID,
         "\n  Download 'cbs_vk100_2024_v1.gpkg' (CBS Vierkantstatistieken 100m,",
         " 2024) and place it at that path.")
  if (!file.exists(BUILTUP))
    message("note: ", BUILTUP, " not found -- built-up fallback unavailable.")
  invisible(TRUE)
}


# --------------------- 1. CBS 100m cells --------------------------------------

load_cells <- function(grid = GRID) {
  message("reading 100m grid...")
  glayer <- sf::st_layers(grid)$name[1]
  cells <- sf::st_read(grid, quiet = TRUE, query = sprintf(
    "SELECT crs28992res100m, aantal_inwoners FROM \"%s\"", glayer))
  cells <- sf::st_drop_geometry(cells)
  cells$aantal_inwoners <- suppressWarnings(as.numeric(cells$aantal_inwoners))
  cells$aantal_inwoners[is.na(cells$aantal_inwoners) | cells$aantal_inwoners < 0] <- 0
  # crs28992res100m = "E<easting/100>N<northing/100>" of the SW corner;
  # centre is +50 m on each axis (same method as R/nodes/cbs-vk100.R).
  e <- as.numeric(sub("^E(\\d+)N(\\d+)$", "\\1", cells$crs28992res100m))
  n <- as.numeric(sub("^E(\\d+)N(\\d+)$", "\\2", cells$crs28992res100m))
  cells$.x <- e * 100 + 50
  cells$.y <- n * 100 + 50
  cells <- cells[!is.na(cells$.x) & !is.na(cells$.y), ]
  message("  ", nrow(cells), " cells, ",
          format(sum(cells$aantal_inwoners), big.mark = ","), " inhabitants")
  sf::st_as_sf(cells, coords = c(".x", ".y"), crs = 28992)
}


# --------------------- 2. containment snap ------------------------------------

# Centre of the AREA'S MAIN POPULATION CLUSTER: take the densest cell, then the
# population-weighted centroid of cells within `radius` of it. Robust for areas
# split into separate settlements, where a plain weighted mean lands in the
# empty middle.
# Population "potential" medoid: among populated cells, pick the one with the
# greatest gravity-weighted access to the area's population. Always lands ON a
# populated cell, in the heart of the dominant cluster -- never in a field, on a
# railway, in water, or outside the polygon. This is the "median-like" estimator.
potential_medoid <- function(cand, max_cand = 200) {
  ord <- order(-cand$aantal_inwoners)
  cs  <- cand[ord[seq_len(min(max_cand, nrow(cand)))], ]
  best <- -Inf; bx <- cs$.x[1]; by <- cs$.y[1]
  for (i in seq_len(nrow(cs))) {
    d <- sqrt((cand$.x - cs$.x[i])^2 + (cand$.y - cs$.y[i])^2)
    s <- sum(cand$aantal_inwoners / (1 + d / 1000))
    if (s > best) { best <- s; bx <- cs$.x[i]; by <- cs$.y[i] }
  }
  c(x = bx, y = by)
}

# Share of the area's population living within `radius` of a point. Low share =
# the weighted mean landed somewhere nobody lives (between separated clusters).
local_pop_share <- function(cand, px, py, radius = 250) {
  d2 <- (cand$.x - px)^2 + (cand$.y - py)^2
  sum(cand$aantal_inwoners[d2 <= radius^2]) / sum(cand$aantal_inwoners)
}

# Quality gate. Relocates to the potential medoid when the weighted mean sits
# in a local void (min_share), or falls outside its own polygon. Good centroids
# are left untouched.
fix_centroids <- function(all_c, polys, tab, min_share = 0.03) {
  own      <- match(all_c$area_code, polys$area_code)
  popcells <- tab[tab$aantal_inwoners > 0, ]
  by_area  <- split(popcells, popcells$area_code)

  nvoid <- 0
  for (i in seq_len(nrow(all_c))) {
    cand <- by_area[[all_c$area_code[i]]]
    if (is.null(cand) || !nrow(cand)) next
    if (local_pop_share(cand, all_c$x[i], all_c$y[i]) < min_share) {
      m <- potential_medoid(cand)
      all_c$x[i] <- m[["x"]]; all_c$y[i] <- m[["y"]]
      all_c$source[i] <- paste0(all_c$source[i], "-medoid")
      nvoid <- nvoid + 1
    }
  }
  message("  (d1) local-void check: ", nvoid, " moved to population medoid")

  pts  <- sf::st_as_sf(all_c, coords = c("x", "y"), crs = 28992, remove = FALSE)
  hits <- sf::st_within(pts, polys)
  bad  <- which(!mapply(function(h, o) o %in% h, hits, own))
  for (i in bad) {
    cand <- by_area[[all_c$area_code[i]]]
    if (!is.null(cand) && nrow(cand)) {
      m <- potential_medoid(cand); all_c$x[i] <- m[["x"]]; all_c$y[i] <- m[["y"]]
    } else {
      co <- sf::st_coordinates(sf::st_point_on_surface(sf::st_geometry(polys[own[i], ])))
      all_c$x[i] <- co[1, 1]; all_c$y[i] <- co[1, 2]
    }
    all_c$source[i] <- paste0(all_c$source[i], "-snapped")
  }
  message("  (d2) containment: ", length(bad), " outside own polygon")
  all_c
}


# --------------------- 3. centroids for one scale -----------------------------

build_scale_centsnap_insideroids <- function(scale, cells, builtup = NULL) {
  message("[", scale, "] reading ", GEO[[scale]])
  polys <- sf::st_read(GEO[[scale]], quiet = TRUE)
  if (!"area_code" %in% names(polys))
    stop("expected an `area_code` column in ", GEO[[scale]],
         " -- found: ", paste(names(polys), collapse = ", "))
  polys <- polys |>
    dplyr::transmute(area_code = as.character(area_code)) |>
    sf::st_transform(28992) |> sf::st_make_valid()
  message("  ", nrow(polys), " polygons")

  joined <- sf::st_join(cells, polys, join = sf::st_within)
  tab <- sf::st_drop_geometry(joined)
  xy  <- sf::st_coordinates(joined)
  tab$.x <- xy[, 1]; tab$.y <- xy[, 2]
  tab <- tab[!is.na(tab$area_code), ]

  # (a) population-weighted
  pop <- tab[tab$aantal_inwoners > 0, ] |>
    dplyr::group_by(area_code) |>
    dplyr::summarise(x = weighted.mean(.x, aantal_inwoners),
                     y = weighted.mean(.y, aantal_inwoners),
                     pop = sum(aantal_inwoners), .groups = "drop") |>
    dplyr::mutate(source = "population")
  message("  (a) population-weighted: ", nrow(pop))
  need <- setdiff(polys$area_code, pop$area_code)

  # (a2) cells exist but no recorded population
  cc <- NULL
  if (length(need)) {
    sub <- tab[tab$area_code %in% need, ]
    if (nrow(sub)) {
      cc <- sub |> dplyr::group_by(area_code) |>
        dplyr::summarise(x = mean(.x), y = mean(.y), pop = 0, .groups = "drop") |>
        dplyr::mutate(source = "cells")
      message("  (a2) cell-centroid fallback: ", nrow(cc))
      need <- setdiff(need, cc$area_code)
    }
  }

  # (b) area-weighted centroid of built-up parts
  bu <- NULL
  if (length(need) && !is.null(builtup)) {
    cand  <- polys |> dplyr::filter(area_code %in% need)
    inter <- suppressWarnings(sf::st_intersection(cand, builtup))
    if (nrow(inter)) {
      ctr <- sf::st_coordinates(sf::st_point_on_surface(sf::st_geometry(inter)))
      idf <- data.frame(area_code = inter$area_code,
                        .a = as.numeric(sf::st_area(inter)),
                        .x = ctr[, 1], .y = ctr[, 2])
      idf <- idf[idf$.a > 0, ]
      bu <- idf |> dplyr::group_by(area_code) |>
        dplyr::summarise(x = weighted.mean(.x, .a), y = weighted.mean(.y, .a),
                         pop = 0, .groups = "drop") |>
        dplyr::mutate(source = "builtup")
      message("  (b) built-up fallback: ", nrow(bu))
      need <- setdiff(need, bu$area_code)
    }
  }

  # (c) genuinely empty
  ps <- NULL
  if (length(need)) {
    cand <- polys |> dplyr::filter(area_code %in% need)
    co   <- sf::st_coordinates(sf::st_point_on_surface(sf::st_geometry(cand)))
    ps <- data.frame(area_code = cand$area_code, x = co[, 1], y = co[, 2],
                     pop = 0, source = "surface")
    message("  (c) point-on-surface fallback: ", nrow(ps))
  }

  all_c <- fix_centroids(all_c, polys, tab)
  stopifnot(nrow(all_c) == nrow(polys))
  stopifnot(!any(duplicated(all_c$area_code)))

  # (d) containment
  all_c <- snap_inside(all_c, polys, tab)

  ll <- all_c |> sf::st_as_sf(coords = c("x", "y"), crs = 28992) |>
    sf::st_transform(4326)
  co <- sf::st_coordinates(ll)
  all_c$lon <- co[, 1]; all_c$lat <- co[, 2]

  res <- setNames(lapply(seq_len(nrow(all_c)),
                         function(i) c(all_c$lon[i], all_c$lat[i])), all_c$area_code)
  jsonlite::write_json(res, OUT[[scale]], auto_unbox = TRUE, digits = 6)
  message("  wrote ", OUT[[scale]], " (", length(res), " areas)")

  attr(all_c, "polys") <- polys
  all_c
}


# --------------------- 4. maps and lookups ------------------------------------

plot_centroids <- function(cent, scale, zoom_area = NULL) {
  library(ggplot2)
  dir.create(FIG_DIR, recursive = TRUE, showWarnings = FALSE)
  polys <- attr(cent, "polys")
  pts   <- cent |> sf::st_as_sf(coords = c("x", "y"), crs = 28992)
  if (!is.null(zoom_area)) {
    polys <- polys[polys$area_code %in% zoom_area, ]
    pts   <- pts[pts$area_code %in% zoom_area, ]
  }
  p <- ggplot() +
    geom_sf(data = polys, fill = "grey95", color = "grey70", linewidth = 0.15) +
    geom_sf(data = pts, aes(color = source), size = 0.5) +
    labs(title = sprintf("%s centroids (%d areas)", scale, nrow(pts))) +
    theme_void() +
    theme(plot.background  = element_rect(fill = "white", color = NA),
          panel.background = element_rect(fill = "white", color = NA))
  f <- file.path(FIG_DIR, sprintf("centroids_%s%s.png", scale,
                                  if (is.null(zoom_area)) "" else "_zoom"))
  ggsave(f, p, width = 7, height = 8, dpi = 150)
  message("  figure: ", f)
  invisible(f)
}

# Fallback points only, on an OSM basemap (water + built-up visible).
# Population-weighted centroids sit at the weighted mean of INHABITED cells, so
# they cannot be in water by construction -- nothing to inspect there.
inspect_fallbacks <- function(cent, scale) {
  if (!requireNamespace("mapview", quietly = TRUE))
    stop('mapview not installed. Run: install.packages("mapview")')
  fb <- cent[cent$source != "population", ]
  if (!nrow(fb)) { message("[", scale, "] no fallback centroids"); return(invisible(NULL)) }
  pts   <- sf::st_as_sf(fb, coords = c("x", "y"), crs = 28992) |> sf::st_transform(4326)
  polys <- attr(cent, "polys") |> sf::st_transform(4326)
  polys <- polys[polys$area_code %in% fb$area_code, ]
  message("[", scale, "] inspecting ", nrow(pts), " fallback centroids")
  mapview::mapview(polys, alpha.regions = 0.15, color = "grey40", layer.name = "area") +
    mapview::mapview(pts, zcol = "source", layer.name = "centroid source")
}

# EVERY centroid for a scale. Fine for gemeente (~342) and PC4 (~4071);
# buurt (~14.7k) will be slow to render.
inspect_all <- function(cent, scale) {
  if (!requireNamespace("mapview", quietly = TRUE))
    stop('mapview not installed. Run: install.packages("mapview")')
  pts   <- sf::st_as_sf(cent, coords = c("x", "y"), crs = 28992) |> sf::st_transform(4326)
  polys <- attr(cent, "polys") |> sf::st_transform(4326)
  message("[", scale, "] ", nrow(pts), " centroids on an OSM basemap")
  mapview::mapview(polys, alpha.regions = 0.10, color = "grey40", layer.name = "area") +
    mapview::mapview(pts, zcol = "source", layer.name = "centroid source")
}

# Look up specific areas -- confirms a centroid exists without hunting for a dot.
#   find_centroid(cents$pc4, c("3088", "2236"))
find_centroid <- function(cent, area_codes) {
  r <- cent[cent$area_code %in% area_codes,
            c("area_code", "source", "pop", "lon", "lat")]
  if (!nrow(r)) message("no such area_code at this scale") else print(as.data.frame(r))
  invisible(r)
}
# Non-visual QA: ranks centroids by how far they sit from the nearest populated
# cell. Use for buurt, where visual checking of ~14.7k points is impractical --
# inspect the top 20 rather than all of them.
#   rep <- centroid_report(cents$buurt); head(rep, 20)
centroid_report <- function(cent, n_flag = 500) {
  polys <- attr(cent, "polys")
  pts   <- sf::st_as_sf(cent, coords = c("x", "y"), crs = 28992, remove = FALSE)
  own   <- match(cent$area_code, polys$area_code)
  hits  <- sf::st_within(pts, polys)
  cent$inside <- mapply(function(h, o) o %in% h, hits, own)

  gc_ <- sf::st_coordinates(sf::st_centroid(sf::st_geometry(polys)))[own, ]
  cent$dist_from_geom_centroid <- round(sqrt((cent$x - gc_[, 1])^2 + (cent$y - gc_[, 2])^2))

  out <- cent[, c("area_code", "source", "pop", "inside", "dist_from_geom_centroid")]
  out <- out[order(-out$dist_from_geom_centroid), ]
  cat("centroids outside their polygon: ", sum(!cent$inside), "\n", sep = "")
  cat("sources: ", paste(names(table(cent$source)), table(cent$source),
                         sep = "=", collapse = "  "), "\n", sep = "")
  as.data.frame(out)
}

# --------------------- 5. run all ---------------------------------------------

build_all_centroids <- function(zoom_zuid = TRUE) {
  check_requirements()
  cells <- load_cells()
  builtup <- NULL
  if (file.exists(BUILTUP))
    builtup <- sf::st_read(BUILTUP, quiet = TRUE) |>
      sf::st_transform(28992) |> sf::st_make_valid()

  out <- list()
  for (sc in names(GEO)) {
    cent <- build_scale_centroids(sc, cells, builtup)
    plot_centroids(cent, sc)
    tb <- table(cent$source)
    cat("[", sc, "] sources: ", paste(names(tb), tb, sep = "=", collapse = "  "),
        "\n", sep = "")
    out[[sc]] <- cent
  }

  zuid_def <- "R/accessibility_analysis/rotterdam-zuid-pc4.R"
  if (zoom_zuid && file.exists(zuid_def)) {
    source(zuid_def); plot_centroids(out$pc4, "pc4", zoom_area = ZUID_PC4)
  }

  cat("\nFigures in ", FIG_DIR, "\n", sep = "")
  cat("Interactive checks:\n",
      "  inspect_fallbacks(cents$pc4, \"pc4\")\n",
      "  inspect_all(cents$gem, \"gem\")\n",
      "  find_centroid(cents$pc4, \"3088\")\n", sep = "")
  invisible(out)
}
cat("\nRun qa <- qa_centroids(cents) for a quality summary.\n")

# --------------------- 6. QA for handover -------------------------------------
# One call summarising centroid quality and flagging only what's worth a human
# look. Gemeente is small enough to inspect fully; PC4 and buurt are flagged by
# exception, so you check ~25 areas rather than 14,700.
#
#   qa <- qa_centroids(cents)
#   inspect_all(cents$gem, "gem")            # all gemeente, interactive
#   inspect_flagged(cents$pc4,   qa$pc4)     # only the flagged PC4s
#   inspect_flagged(cents$buurt, qa$buurt)   # only the flagged buurten
qa_centroids <- function(cents, top_n = 25) {
  flagged <- list()
  for (sc in names(cents)) {
    cent  <- cents[[sc]]
    polys <- attr(cent, "polys")
    pts   <- sf::st_as_sf(cent, coords = c("x","y"), crs = 28992, remove = FALSE)
    own   <- match(cent$area_code, polys$area_code)
    hits  <- sf::st_within(pts, polys)
    inside <- mapply(function(h, o) o %in% h, hits, own)

    gc_ <- sf::st_coordinates(sf::st_centroid(sf::st_geometry(polys)))[own, , drop = FALSE]
    dist_geom <- round(sqrt((cent$x - gc_[,1])^2 + (cent$y - gc_[,2])^2))

    cat("\n=== ", sc, " (", nrow(cent), " areas) ===\n", sep = "")
    cat("  sources: ", paste(names(table(cent$source)), table(cent$source),
                             sep = "=", collapse = "  "), "\n", sep = "")
    cat("  outside own polygon: ", sum(!inside), "\n", sep = "")

    # unusual = any fallback/relocation, or an extreme offset from the shape's
    # geometric middle (a legitimate signal for odd or split areas)
    odd <- cent$source != "population" | !inside |
           dist_geom >= sort(dist_geom, decreasing = TRUE)[min(top_n, length(dist_geom))]
    fl <- data.frame(area_code = cent$area_code[odd], source = cent$source[odd],
                     pop = cent$pop[odd], inside = inside[odd],
                     offset_m = dist_geom[odd])
    fl <- fl[order(-fl$offset_m), ]
    cat("  flagged for inspection: ", nrow(fl), "\n", sep = "")
    print(utils::head(fl, top_n), row.names = FALSE)
    flagged[[sc]] <- fl$area_code
  }
  cat("\nInteractive follow-up:\n",
      "  inspect_all(cents$gem, \"gem\")\n",
      "  inspect_flagged(cents$pc4,   qa$pc4)\n",
      "  inspect_flagged(cents$buurt, qa$buurt)\n",
      "Note: find_centroid prints lon,lat -- swap the order for Google Maps.\n", sep = "")
  invisible(flagged)
}

# Interactive map of a specific set of area codes.
inspect_flagged <- function(cent, area_codes) {
  if (!requireNamespace("mapview", quietly = TRUE))
    stop('mapview not installed. Run: install.packages("mapview")')
  sub <- cent[cent$area_code %in% area_codes, ]
  if (!nrow(sub)) { message("nothing flagged"); return(invisible(NULL)) }
  pts   <- sf::st_as_sf(sub, coords = c("x","y"), crs = 28992) |> sf::st_transform(4326)
  polys <- attr(cent, "polys") |> sf::st_transform(4326)
  polys <- polys[polys$area_code %in% area_codes, ]
  mapview::mapview(polys, alpha.regions = 0.15, color = "grey40", layer.name = "area") +
    mapview::mapview(pts, zcol = "source", layer.name = "centroid source")
}
