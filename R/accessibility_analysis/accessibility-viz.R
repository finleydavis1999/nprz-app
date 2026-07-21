# accessibility-viz.R  (quick R visuals of the overlay -- NOT app-wired)
# -----------------------------------------------------------------------------
# Produces demonstration maps + plots from a Stage-3 overlay (`ov`), to show how
# the pipeline highlights WHERE structural vs personal policy is needed.
# Outputs PNGs to accessibility_analysis/figs/. Uses the app's pc4.geojson for
# geometry. ASCII only.
#
# Assumes `ov` (from build_overlay) and `res` (from compute_accessibility) exist
# in the session. Run the overlay block first, then source this.

library(sf); library(dplyr); library(ggplot2); library(tidyr)

FIG_DIR <- "R/accessibility_analysis/figs"
dir.create(FIG_DIR, recursive = TRUE, showWarnings = FALSE)

# ---- geometry: Zuid PC4 polygons ---------------------------------------------
pc4 <- st_read("static/data/geo/pc4.geojson", quiet = TRUE) |>
  filter(area_code %in% ov$area_code)
geo <- pc4 |> left_join(ov, by = "area_code")

save_map <- function(p, name, w = 6, h = 6)
  ggsave(file.path(FIG_DIR, name), p, width = w, height = h, dpi = 150)

theme_map <- theme_void() + theme(
  legend.position = "right",
  plot.title = element_text(face = "bold", size = 12),
  plot.background = element_rect(fill = "white", color = NA),
  panel.background = element_rect(fill = "white", color = NA))

# ---- 1. Accessibility choropleth (low access = dark) -------------------------
p_acc <- ggplot(geo) +
  geom_sf(aes(fill = accessibility), color = "white", size = 0.2) +
  geom_sf_text(aes(label = area_code), size = 2.5, color = "white") +
  scale_fill_viridis_c(option = "magma", name = "Accessibility") +
  labs(title = "Job accessibility -- low-educated Zuid residents (car)") +
  theme_map
save_map(p_acc, "01_accessibility.png")

# ---- 2. Population choropleth (how many of the group live here) --------------
p_pop <- ggplot(geo) +
  geom_sf(aes(fill = group_pop), color = "white", size = 0.2) +
  scale_fill_viridis_c(option = "viridis", name = "Low-educated\nresidents") +
  labs(title = "Where low-educated residents live (total)") +
  theme_map
save_map(p_pop, "02_population.png")

# ---- 3. STRUCTURAL policy map (many people * poor access) --------------------
p_str <- ggplot(geo) +
  geom_sf(aes(fill = structural), color = "white", size = 0.2) +
  scale_fill_viridis_c(option = "inferno", name = "Structural\nneed") +
  labs(title = "Structural-policy priority (large populations, poor access)") +
  theme_map
save_map(p_str, "03_structural.png")

# ---- 4. The 2x2 policy quadrant (the key diagnostic) -------------------------
# x = access (low -> high), y = group population (few -> many). Quadrants:
#   many people + low access  -> STRUCTURAL policy
#   few people  + low access  -> PERSONAL policy
#   high access               -> lower priority
med_acc <- median(ov$accessibility)
med_pop <- median(ov$group_pop)
quad <- ov |> mutate(
  policy = case_when(
    accessibility <= med_acc & group_pop >  med_pop ~ "Structural",
    accessibility <= med_acc & group_pop <= med_pop ~ "Personal",
    TRUE ~ "Lower priority"))

p_quad <- ggplot(quad, aes(accessibility, group_pop, label = area_code, color = policy)) +
  geom_vline(xintercept = med_acc, linetype = 2, color = "grey60") +
  geom_hline(yintercept = med_pop, linetype = 2, color = "grey60") +
  geom_point(size = 3) +
  geom_text(vjust = -1, size = 3, show.legend = FALSE) +
  scale_color_manual(values = c(Structural = "#d1495b", Personal = "#edae49",
                                `Lower priority` = "#66a182")) +
  labs(title = "Policy quadrant: where structural vs personal action fits",
       x = "Job accessibility (low -> high)",
       y = "Low-educated residents (few -> many)", color = NULL) +
  theme_minimal()
ggsave(file.path(FIG_DIR, "04_policy_quadrant.png"), p_quad, width = 7, height = 6, dpi = 150)

# ---- 5. Decay curve (the group's willingness-to-travel) ----------------------
p_decay <- ggplot(res$decay$curve, aes(bin, share)) +
  geom_col(fill = "#2a4d69") +
  labs(title = "Decay curve -- low-educated Zuid (share of commutes by car time)",
       x = "Travel time (min)", y = "Share of commutes") +
  theme_minimal()
ggsave(file.path(FIG_DIR, "05_decay_curve.png"), p_decay, width = 6, height = 4, dpi = 150)

message("wrote figures to ", FIG_DIR, ":")
print(list.files(FIG_DIR))