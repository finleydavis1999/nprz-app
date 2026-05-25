# Node-level model: GLM fit on a pre-assembled design matrix.
#
# Inputs come pre-staged from the JS side (the DuckDB design-matrix step
# computes columns, joins layer results, and ships Float64Arrays in). The R
# half only fits — no data wrangling, no formula parsing.
#
# Uses speedglm when available (CG-based, faster than base glm() once N is in
# the low thousands) and silently falls back to base glm.fit so the unit test
# environment doesn't need to install speedglm just to validate the wrapper.
#
# Bound on the JS side via webR-client.runR(), so this file expects:
#   y          numeric, length N (dependent)
#   X          matrix, N x p (design — intercept column NOT included; we add it)
#   family     "gaussian" or "poisson"
#   link       "identity" or "log"
#   col_names  character vector of length p — covariate names (no intercept)
#   weights    NULL or numeric vector length N — observation weights (WLS for
#              gaussian; iterative reweighting for poisson). Positive only;
#              the design-matrix step drops non-positive weights.
#   offset     NULL or numeric vector length N — added to the linear
#              predictor without estimating a coefficient. Typical use:
#              `log(population)` for Poisson rate models.

fit_nlm <- function(y, X, family, link, col_names, weights = NULL, offset = NULL) {
  N <- length(y)
  X <- as.matrix(X)
  storage.mode(X) <- "double"
  # Prepend intercept column.
  Xi <- cbind(`(Intercept)` = 1, X)
  colnames(Xi)[-1L] <- col_names
  p <- ncol(Xi)

  fam <- switch(
    family,
    gaussian = stats::gaussian(link = link),
    poisson = stats::poisson(link = link),
    stop(sprintf("Unsupported family: %s", family))
  )

  use_speedglm <- requireNamespace("speedglm", quietly = TRUE)
  # speedglm.wfit accepts NULL weights/offset; explicit forwarding keeps the
  # default behaviour when either is unset.
  if (use_speedglm) {
    fit <- speedglm::speedglm.wfit(
      y = y, X = Xi, family = fam, intercept = FALSE,
      weights = weights, offset = offset
    )
    coefs <- as.numeric(fit$coefficients)
    # speedglm stores Cov.unscaled (the (X'WX)^-1 matrix); dispersion = sigma2
    # for gaussian, 1 for poisson. SEs are sqrt(diag) * sqrt(dispersion).
    cov_unscaled <- as.matrix(fit$Cov.unscaled %||% diag(NA_real_, p))
    dispersion <- if (family == "gaussian") (fit$RSS %||% NA_real_) / fit$df else 1
    ses <- sqrt(diag(cov_unscaled) * dispersion)
    fitted_vals <- as.numeric(fit$linear.predictors %||% (Xi %*% coefs))
    if (link == "log") fitted_vals <- exp(fitted_vals)
    deviance_val <- fit$deviance %||% NA_real_
    n_params <- p
    aic_val <- if (!is.na(deviance_val)) deviance_val + 2 * n_params else NA_real_
    bic_val <- if (!is.na(deviance_val)) deviance_val + log(N) * n_params else NA_real_
  } else {
    # Fallback: base glm.fit. Equivalent results, slower past N≈few thousand.
    fit <- stats::glm.fit(
      x = Xi, y = y, family = fam, intercept = FALSE,
      weights = weights, offset = offset
    )
    coefs <- as.numeric(fit$coefficients)
    fitted_vals <- as.numeric(fit$fitted.values)
    sm_X <- solve(crossprod(Xi * sqrt(pmax(fit$weights, .Machine$double.eps))))
    dispersion <- if (family == "gaussian") sum(fit$residuals^2 * fit$weights) / fit$df.residual else 1
    ses <- sqrt(diag(sm_X) * dispersion)
    n_params <- p
    aic_val <- fit$aic
    bic_val <- fit$aic - 2 * n_params + log(N) * n_params
  }

  residuals_vec <- y - fitted_vals
  z <- coefs / ses
  pvals <- 2 * stats::pnorm(-abs(z))

  # make_fit_list (and the %||% operator used above) live in fit_stats.R,
  # which model-runner.js sources first — see ensureRSource(). No `source()`
  # here on purpose: keeps path coupling out of the R files.
  fit_stats <- make_fit_list(
    y = y, yhat = fitted_vals, residuals = residuals_vec,
    n_coefs = n_params, aic = aic_val, bic = bic_val
  )

  list(
    coefficients = list(
      name = colnames(Xi),
      est = coefs,
      se = ses,
      z = z,
      p = pvals
    ),
    fit = fit_stats,
    perObs = list(
      fitted = fitted_vals,
      residual = residuals_vec
    )
  )
}
