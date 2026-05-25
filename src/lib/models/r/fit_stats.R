# Goodness-of-fit helpers shared by NLM, SIM, and GWR. Ported from
# spipr/R/fit_statistics.R, simplified to pure base R (no `Metrics` dep).
#
# This file is sourced first by model-runner.js (`ensureRSource`), so anything
# defined here is reachable from every other R file. We use that to host the
# `%||%` operator below — keeps the other fitter files (nlm.R / sim.R /
# gwr.R / sim_zeroinfl.R) order-independent at source time.

# A %||% B  ->  A if non-NULL, else B. Tidied operator borrowed from rlang
# style — saves a few `if (is.null(...))` lines in the speedglm result-
# unwrapping code without taking a dep. Defined here (the shared base file)
# rather than nlm.R so source order between nlm/sim/gwr doesn't matter.
`%||%` <- function(a, b) if (is.null(a)) b else a

# Pearson R-squared between observed `y` and fitted `yhat`. Uses complete obs.
# Returns NA_real_ when cor() can't be computed — too few complete pairs (e.g.
# every local GWR fit failed) or zero variance. The bare cor() throws in those
# cases ("no complete element pairs", "standard deviation is zero"), which
# crashes any caller that doesn't tryCatch. Guard here so callers get a clean
# NA they can present as "fit diagnostics unavailable" instead of an error.
r_squared <- function(y, yhat) {
  ok <- is.finite(y) & is.finite(yhat)
  if (sum(ok) < 2L) return(NA_real_)
  yo <- y[ok]; yho <- yhat[ok]
  if (stats::sd(yo) == 0 || stats::sd(yho) == 0) return(NA_real_)
  c <- stats::cor(yo, yho, method = "pearson")
  c * c
}

# R-squared adjusted for the number of fitted coefficients `p` over `N` obs.
# (`p` includes the intercept — i.e. it's the column count of the design
# matrix Xi after we prepend the intercept column.)
adj_r_squared <- function(r2, p, N) {
  if (N - p - 1L <= 0L) return(NA_real_)
  1 - ((1 - r2) * (N - 1L) / (N - p - 1L))
}

# Root-mean-square error.
rmse <- function(y, yhat) {
  sqrt(mean((y - yhat)^2, na.rm = TRUE))
}

# Sørensen–Dice coefficient (spatial-interaction agreement measure). The
# +0.001 offset matches the spipr implementation, keeping the metric finite
# when y==yhat==0 (a "perfect agreement on zero" cell) instead of 0/0 NaN.
# Without the offset, OD pairs with zero observed AND zero predicted flow
# (the bulk of any sparse SIM design) would all contribute NaN and pull the
# average down to NaN — destroying the diagnostic.
sorensen <- function(y, yhat) {
  (1 / length(y)) * sum(
    (2 * pmin(y + 0.001, yhat + 0.001)) /
      (y + 0.001 + yhat + 0.001)
  )
}

# Pack fit metrics + residual moments into a single named list, ready for
# round-tripping back to JS as a webR list object. `n_coefs` is the number of
# fitted coefficients (intercept included) — used for adjusted-R² df only.
make_fit_list <- function(y, yhat, residuals, n_coefs, aic, bic, include_sorensen = FALSE) {
  r2 <- r_squared(y, yhat)
  out <- list(
    rSquared = r2,
    adjRSquared = adj_r_squared(r2, n_coefs, length(y)),
    rmse = rmse(y, yhat),
    aic = aic,
    bic = bic,
    meanResid = mean(residuals, na.rm = TRUE),
    varResid = stats::var(residuals, na.rm = TRUE)
  )
  if (include_sorensen) out$sorensen <- sorensen(y, yhat)
  out
}
