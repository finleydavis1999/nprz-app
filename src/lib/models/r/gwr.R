# Geographically weighted regression (GWR) — Phase 3.
#
# Fits one local weighted regression per area, using a spatial kernel
# (fixed or adaptive bandwidth × bi-square or gaussian shape) to weight
# observations by distance from each regression point. Returns per-area
# coefficient vectors that the JS side spreads into one node-domain
# child layer per coefficient, plus local_r2.
#
# Pure base R — no spatial packages. The (N²) distance matrix is built
# JS-side and passed in as a flat numeric vector (column-major), since
# webR's R↔JS bridge handles plain numeric vectors well.
#
# Inputs (bound from runGwr via webR env):
#   y          numeric length N (dependent)
#   X          matrix N×p (design — intercept added in R)
#   col_names  character length p
#   D_flat     numeric length N² — column-major distance matrix; entry
#              (i,j) = distance between regression point i and obs j
#   bw         numeric scalar bandwidth (km for fixed; integer k for adaptive)
#   kernel_type   "fixed" | "adaptive"
#   kernel_shape  "bi-square" | "gaussian"
#   family     "gaussian" | "poisson"
#   link       "identity" | "log"
#
# Output mirrors fit_nlm shape so the JS-side runner reuses listToObject:
#   list(
#     coefficients    = list(name=, est=, se=, z=, p=)   # GLOBAL summary
#                                                          (mean coef across
#                                                          areas with SE = sd
#                                                          across areas)
#     fit             = list(rSquared, adjRSquared, rmse, aic, bic, ...)
#     perObs          = list(fitted, residual)
#     perNode         = list(areaCodes, betas, localR2, bwActual)
#                       betas is a list(name → numeric length N)
#   )

# Kernel weight from distance d, bandwidth h, and shape.
#   bi-square: (1 - (d/h)^2)^2  for d <= h, else 0
#   gaussian:  exp(-0.5 * (d/h)^2)
gwr_kernel_weight <- function(d, h, shape) {
  if (h <= 0) return(rep(0, length(d)))
  r <- d / h
  if (shape == "gaussian") {
    return(exp(-0.5 * r * r))
  }
  # default to bi-square
  w <- (1 - r * r)^2
  w[d > h] <- 0
  w
}

# Pick the per-area effective bandwidth h_i from the row of distances:
#   "fixed":   h_i = bw (km) for every point
#   "adaptive": h_i = distance to the k-th nearest neighbour (bw = k)
#
# Convention: k counts neighbours, EXCLUDING the focal point. d_row[focal]==0
# sorts to index 1, so the k-th neighbour is at sorted[k+1]. With a bi-square
# kernel the point at d==h gets weight 0, so picking the (k+1)-th element
# gives the user exactly k surrounding points with non-zero weight plus the
# focal point — matching GWmodel / spgwr behaviour. (Older code returned
# sorted[k], leaving the user one short.)
gwr_local_bw <- function(d_row, bw, kernel_type) {
  if (kernel_type == "adaptive") {
    k <- as.integer(bw)
    if (k < 2) k <- 2
    # k-th smallest distance (sort is fine at our scales; partial sort would
    # be faster but matters only at vierkant scale).
    sorted <- sort(d_row)
    return(sorted[min(k + 1L, length(sorted))])
  }
  bw
}

fit_gwr <- function(y, X, col_names, D_flat, bw,
                    kernel_type = "fixed", kernel_shape = "bi-square",
                    family = "gaussian", link = "identity") {
  N <- length(y)
  X <- as.matrix(X)
  storage.mode(X) <- "double"
  Xi <- cbind(`(Intercept)` = 1, X)
  colnames(Xi)[-1L] <- col_names
  p <- ncol(Xi)

  D <- matrix(D_flat, nrow = N, ncol = N)
  betas <- matrix(NA_real_, nrow = N, ncol = p)
  colnames(betas) <- colnames(Xi)
  local_r2 <- rep(NA_real_, N)
  bw_actual <- rep(NA_real_, N)
  fitted_vals <- rep(NA_real_, N)

  fam <- switch(
    family,
    gaussian = stats::gaussian(link = link),
    poisson = stats::poisson(link = link),
    stop(sprintf("Unsupported family for GWR: %s", family))
  )

  for (i in seq_len(N)) {
    d_row <- D[i, ]
    h_i <- gwr_local_bw(d_row, bw, kernel_type)
    bw_actual[i] <- h_i
    w <- gwr_kernel_weight(d_row, h_i, kernel_shape)
    # Drop near-zero weights for the local fit — keeps the design well-
    # conditioned and avoids "essentially singular" failures.
    keep <- w > 1e-12
    if (sum(keep) <= p) next  # not enough effective points
    yw <- y[keep]
    Xw <- Xi[keep, , drop = FALSE]
    ww <- w[keep]

    fit <- if (family == "gaussian") {
      stats::lm.wfit(Xw, yw, ww)
    } else {
      tryCatch(
        stats::glm.fit(Xw, yw, weights = ww, family = fam, intercept = FALSE),
        error = function(e) NULL
      )
    }
    if (is.null(fit)) next
    coefs_i <- as.numeric(fit$coefficients)
    if (any(is.na(coefs_i))) next
    betas[i, ] <- coefs_i

    # Predict the focal observation only (i-th row of Xi).
    eta_i <- as.numeric(Xi[i, , drop = FALSE] %*% coefs_i)
    fitted_vals[i] <- if (link == "log") exp(eta_i) else eta_i

    # Local R²: weighted SS-residual / SS-total. Standard GWR diagnostic.
    if (family == "gaussian") {
      resid_local <- yw - as.numeric(Xw %*% coefs_i)
      ss_res <- sum(ww * resid_local^2)
      mu_w <- sum(ww * yw) / sum(ww)
      ss_tot <- sum(ww * (yw - mu_w)^2)
      local_r2[i] <- if (ss_tot > 0) max(0, 1 - ss_res / ss_tot) else NA_real_
    } else {
      # Pseudo-R² for Poisson: 1 - residual_dev / null_dev.
      dev_res <- fit$deviance %||% NA_real_
      dev_null <- fit$null.deviance %||% NA_real_
      local_r2[i] <- if (!is.na(dev_res) && !is.na(dev_null) && dev_null > 0) {
        max(0, 1 - dev_res / dev_null)
      } else {
        NA_real_
      }
    }
  }

  residuals_vec <- y - fitted_vals

  # Global summary: report the *mean* of each coefficient across areas as
  # the headline estimate, with the spatial sd as a heuristic "stability"
  # measure (NOT a proper standard error — GWR doesn't have one without
  # a heavier framework). Surfacing it in the same shape as NLM keeps the
  # ModelResults coefficient table consistent.
  est <- colMeans(betas, na.rm = TRUE)
  se <- apply(betas, 2, stats::sd, na.rm = TRUE)
  z <- est / se
  pvals <- 2 * stats::pnorm(-abs(z))

  fit_stats <- make_fit_list(
    y = y, yhat = fitted_vals, residuals = residuals_vec,
    n_coefs = p,
    aic = NA_real_, # not well-defined for GWR; AICc would need trace(S)
    bic = NA_real_
  )

  # Per-node coefficient vectors: list of vectors (one per coefficient
  # name) keyed by the same order as `colnames(Xi)`. The JS side spreads
  # these into one child layer each.
  betas_list <- setNames(
    lapply(seq_len(p), function(j) as.numeric(betas[, j])),
    colnames(Xi)
  )

  list(
    coefficients = list(
      name = colnames(Xi),
      est = unname(est),
      se = unname(se),
      z = unname(z),
      p = unname(pvals)
    ),
    fit = fit_stats,
    perObs = list(
      fitted = fitted_vals,
      residual = residuals_vec
    ),
    perNode = list(
      betas = betas_list,
      localR2 = local_r2,
      bwActual = bw_actual
    )
  )
}

# Golden-section bandwidth search on AICc proxy (sum of weighted SS-resid
# proxied — for v0 we use sum of |residual|^2 across all focal points as a
# cheap surrogate). Bracketed by [bw_lo, bw_hi]; ~10 iterations converges
# to ~1% accuracy.
gwr_bandwidth_aic <- function(y, X, col_names, D_flat, bw_lo, bw_hi,
                              kernel_type, kernel_shape, family, link,
                              max_iter = 12) {
  phi <- (sqrt(5) - 1) / 2

  score <- function(bw) {
    # tryCatch keeps the search alive when a candidate bandwidth produces a
    # degenerate fit (e.g. every local regression skipped → cor() in
    # r_squared() throws "no complete element pairs"). r_squared was made
    # NA-safe in fit_stats.R, but an Inf-on-failure shield here is still
    # cheap insurance against other surprises (rank deficiency, etc.).
    fit <- tryCatch(
      fit_gwr(
        y = y, X = X, col_names = col_names, D_flat = D_flat, bw = bw,
        kernel_type = kernel_type, kernel_shape = kernel_shape,
        family = family, link = link
      ),
      error = function(e) NULL
    )
    if (is.null(fit)) return(Inf)
    res <- fit$perObs$residual
    N <- length(res)
    ok <- is.finite(res)
    n_ok <- sum(ok)
    # Coverage gate: at small bandwidths most local fits skip (too few effective
    # neighbours), residuals stay NA. Without this, the search collapses onto
    # tiny bandwidths where the handful of surviving fits have zero residual
    # (perfect local fit with p+1 points) and the proxy SS-resid ≈ 0 — a false
    # optimum. Require ≥ 70% coverage; below that, treat the bandwidth as
    # infeasible. Above the gate, scale SS by (N / n_ok) so partial-coverage
    # solutions are comparable to full-coverage ones rather than artificially
    # cheap. The end result is a search that picks bandwidths where GWR
    # actually works.
    if (n_ok < ceiling(0.7 * N)) return(Inf)
    sum(res[ok]^2) * (N / n_ok)
  }

  a <- bw_lo
  b <- bw_hi
  c <- b - phi * (b - a)
  d <- a + phi * (b - a)
  fc <- score(c)
  fd <- score(d)
  # Track the best (lowest, finite) score we ever saw, so an all-Inf search
  # degrades gracefully instead of returning (a+b)/2 which the caller would
  # then use for an inevitably-also-failed final fit. Falls back to bw_hi
  # (the most forgiving end of the bracket) with a warning if nothing ever
  # produced a finite score.
  best_bw <- NA_real_
  best_score <- Inf
  if (is.finite(fc) && fc < best_score) { best_bw <- c; best_score <- fc }
  if (is.finite(fd) && fd < best_score) { best_bw <- d; best_score <- fd }
  for (k in seq_len(max_iter)) {
    if (fc < fd) {
      b <- d
      d <- c
      fd <- fc
      c <- b - phi * (b - a)
      fc <- score(c)
      if (is.finite(fc) && fc < best_score) { best_bw <- c; best_score <- fc }
    } else {
      a <- c
      c <- d
      fc <- fd
      d <- a + phi * (b - a)
      fd <- score(d)
      if (is.finite(fd) && fd < best_score) { best_bw <- d; best_score <- fd }
    }
    if (abs(b - a) < 0.01 * (bw_hi - bw_lo)) break
  }
  if (is.na(best_bw)) {
    warning("GWR bandwidth search: every candidate failed the coverage gate; falling back to bw_hi")
    return(bw_hi)
  }
  (a + b) / 2
}
