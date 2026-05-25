# Zero-inflated Poisson SIM via pscl::zeroinfl.
#
# Two processes:
#   1. Bernoulli (logit link) — probability the OD pair is a "structural zero"
#      (e.g. impossible movement, infrastructure barrier, regulatory ban).
#   2. Poisson (log link) — count for "active" pairs.
# Both parts use the SAME predictor set in this v0 to keep the UI sane (the
# pscl formula is `y ~ X | X`). A future extension could split them.
#
# Constraints (production / attraction) are NOT supported here in v0 — pscl
# accepts factor() terms but the coefficient table would explode and the
# count + zero parts would each carry N origin/dest dummies. Out of scope.
#
# Output shape mirrors fit_sim so the JS-side runner reuses its parsing path.
# Coefficient names are prefixed `count.<name>` and `zero.<name>` so the
# ModelResults table reads them as one flat list grouped by part.

fit_sim_zeroinfl <- function(y, X, col_names, offset = NULL) {
  if (!requireNamespace("pscl", quietly = TRUE)) {
    stop("Zero-inflated SIM requires the `pscl` R package; webR install may have failed")
  }
  N <- length(y)
  X <- as.matrix(X)
  storage.mode(X) <- "double"
  p_cont <- ncol(X)
  if (p_cont > 0) colnames(X) <- col_names

  # Round y for the same reason as fit_sim — Poisson + ZI both require
  # non-negative integers; fractional weighted survey counts otherwise
  # spam "non-integer x" warnings.
  y <- round(as.numeric(y))

  # pscl wants a data.frame + a formula. Reconstruct one from X + y.
  df <- as.data.frame(X, stringsAsFactors = FALSE)
  df$.y <- y
  if (!is.null(offset)) df$.offset <- offset

  # Backtick-quote column names so `log(distance_km)` etc. survive parsing.
  rhs <- if (p_cont > 0) {
    paste(paste0("`", col_names, "`"), collapse = " + ")
  } else {
    "1"
  }
  fml <- stats::as.formula(paste0(".y ~ ", rhs, " | ", rhs))

  fit <- if (is.null(offset)) {
    pscl::zeroinfl(fml, data = df, dist = "poisson")
  } else {
    # pscl takes the offset via a separate formula argument.
    pscl::zeroinfl(fml, data = df, dist = "poisson", offset = ~ .offset)
  }

  count_coef <- fit$coefficients$count
  zero_coef <- fit$coefficients$zero
  all_names <- c(paste0("count.", names(count_coef)), paste0("zero.", names(zero_coef)))
  all_est <- c(unname(count_coef), unname(zero_coef))

  vc <- stats::vcov(fit)
  all_se <- sqrt(diag(vc))
  all_z <- all_est / all_se
  all_p <- 2 * stats::pnorm(-abs(all_z))

  fitted_vals <- as.numeric(stats::predict(fit, type = "response"))
  residuals_vec <- y - fitted_vals

  ll <- as.numeric(stats::logLik(fit))
  p <- length(all_est)
  aic_val <- -2 * ll + 2 * p
  bic_val <- -2 * ll + log(N) * p

  fit_stats <- make_fit_list(
    y = y, yhat = fitted_vals, residuals = residuals_vec,
    n_coefs = p, aic = aic_val, bic = bic_val,
    include_sorensen = TRUE
  )

  list(
    coefficients = list(
      name = all_names,
      est = all_est,
      se = all_se,
      z = all_z,
      p = all_p
    ),
    fit = fit_stats,
    perObs = list(
      fitted = fitted_vals,
      residual = residuals_vec
    )
  )
}
