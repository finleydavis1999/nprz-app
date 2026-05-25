# Spatial-interaction model: Poisson gravity fit.
#
# Phase 2 scope: three constraint modes:
#   "none"        — unconstrained:           y ~ log(d) + f(mass_o) + f(mass_d)
#   "production"  — origin-constrained:      y ~ log(d) + f(mass_d) + factor(o)
#   "attraction"  — destination-constrained: y ~ log(d) + f(mass_o) + factor(d)
#   (Doubly-constrained is intentionally out of scope.)
#
# log link is implicit (Poisson canonical). The dependent `y` stays raw
# (interactions counts); deterrence and mass terms come in pre-logged from
# sim-design.js (decorated column names + applied transforms).
#
# Inputs:
#   y          numeric, length N (flow counts)
#   X          matrix N x p_continuous (intercept NOT included; distance +
#              whichever mass(es) weren't absorbed by the constraint)
#   col_names  character vector for X's columns
#   o, d       character vectors length N — origin / dest area codes. Only
#              used for constraint != "none", but always sent so we don't have
#              to special-case the JS-side env shape.
#   constraint "none" | "production" | "attraction"
#
# Output: same `list(coefficients, fit, perObs)` shape as fit_nlm; SIM
# additions include Sørensen-Dice. For constrained fits the coefficients
# table includes the per-area fixed effects (named o.<code> / d.<code>) —
# the UI is responsible for collapsing / hiding them if they're noisy.

fit_sim <- function(y, X, col_names, o, d, constraint = "none", offset = NULL) {
  N <- length(y)
  X <- as.matrix(X)
  storage.mode(X) <- "double"
  p_cont <- ncol(X)
  if (p_cont > 0) colnames(X) <- col_names

  # Round y to integers — Poisson MLE is only defined for non-negative
  # integer counts. Weighted survey flows (e.g. ovin where each
  # respondent carries a survey weight) come in fractional; without
  # this round R prints `Warning: non-integer x = 1.282075` for every
  # weighted row. The JS side surfaces a one-line "weighted counts
  # rounded" notice in the dock when it detects fractional y.
  y <- round(as.numeric(y))

  fam <- stats::poisson(link = "log")

  use_speedglm <- requireNamespace("speedglm", quietly = TRUE)
  has_matrix <- requireNamespace("Matrix", quietly = TRUE)

  if (constraint == "none") {
    # Dense design with hand-rolled intercept column.
    Xi <- cbind(`(Intercept)` = 1, X)
    if (use_speedglm) {
      fit <- speedglm::speedglm.wfit(
        y = y, X = Xi, family = fam, intercept = FALSE, offset = offset
      )
    } else {
      fit <- stats::glm.fit(
        x = Xi, y = y, family = fam, intercept = FALSE, offset = offset
      )
    }
    final_names <- colnames(Xi)
  } else {
    if (!has_matrix) stop("constrained SIM needs the `Matrix` R package")
    factor_var <- if (constraint == "production") factor(o) else factor(d)
    # sparse.model.matrix builds one column per level with the -1 to drop the
    # automatic intercept (the per-level dummies are the intercepts here).
    fac_mat <- Matrix::sparse.model.matrix(~ factor_var - 1)
    lvl <- levels(factor_var)
    prefix <- if (constraint == "production") "o." else "d."
    colnames(fac_mat) <- paste0(prefix, lvl)

    if (p_cont > 0) {
      X_sparse <- methods::as(X, "CsparseMatrix")
      colnames(X_sparse) <- col_names
      Xi <- Matrix::cbind2(X_sparse, fac_mat)
    } else {
      Xi <- fac_mat
    }

    if (use_speedglm) {
      fit <- speedglm::speedglm.wfit(
        y = y, X = Xi, family = fam, intercept = FALSE, sparse = TRUE, offset = offset
      )
    } else {
      # base glm.fit can't take a sparse matrix; densify as a fallback. Only
      # hits in test envs where speedglm isn't installed.
      Xi_dense <- as.matrix(Xi)
      fit <- stats::glm.fit(
        x = Xi_dense, y = y, family = fam, intercept = FALSE, offset = offset
      )
    }
    final_names <- colnames(Xi)
  }

  coefs <- as.numeric(fit$coefficients)
  p <- length(coefs)
  cov_unscaled <- as.matrix(fit$Cov.unscaled %||% diag(NA_real_, p))
  ses <- sqrt(diag(cov_unscaled))
  eta <- as.numeric(fit$linear.predictors %||% (if (constraint == "none") Xi %*% coefs else as.numeric(Xi %*% coefs)))
  fitted_vals <- exp(eta)
  deviance_val <- fit$deviance %||% NA_real_
  aic_val <- if (!is.na(deviance_val)) deviance_val + 2 * p else NA_real_
  bic_val <- if (!is.na(deviance_val)) deviance_val + log(N) * p else NA_real_

  residuals_vec <- y - fitted_vals
  z <- coefs / ses
  pvals <- 2 * stats::pnorm(-abs(z))

  fit_stats <- make_fit_list(
    y = y, yhat = fitted_vals, residuals = residuals_vec,
    n_coefs = p, aic = aic_val, bic = bic_val,
    include_sorensen = TRUE
  )

  list(
    coefficients = list(
      name = final_names,
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

# %||% lives in fit_stats.R (sourced first); reachable from this global env.
