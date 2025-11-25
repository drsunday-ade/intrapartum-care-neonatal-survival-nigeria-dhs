############################################################
# Results Section 2:
# Distribution of Obstetric Risk, Skilled Care, and C-section
# Data: Nigeria DHS births recode 2018 (NGBR8AFL)
# Output folder: outputs_section2/
############################################################

## 0. Packages ------------------------------------------------------------

required_pkgs <- c(
  "tidyverse",   # dplyr, tidyr, ggplot2, readr, etc.
  "data.table",  # fast reading of CSVs if present
  "haven",       # read_dta
  "janitor",     # clean_names
  "survey",      # complex survey design
  "reticulate",  # Python bridge
  "jsonlite",    # read meta json if present
  "scales"       # pretty labels if needed
)

new_pkgs <- required_pkgs[!(required_pkgs %in% installed.packages()[, "Package"])]
if (length(new_pkgs) > 0) {
  install.packages(new_pkgs)
}

library(tidyverse)
library(data.table)
library(haven)
library(janitor)
library(survey)
library(reticulate)
library(jsonlite)
library(scales)

options(dplyr.summarise.inform = FALSE)
options(survey.lonely.psu = "adjust")  # robust handling of lonely PSUs

## 1. Output directory ----------------------------------------------------

out_dir <- "outputs_section2"
if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)

## 2. Optional DHS helper files (rawcodes / codebook / labels / meta) ----

raw_csv      <- "NGBR8AFL_rawcodes.csv"
codebook_csv <- "NGBR8AFL_codebook.csv"
val_labels   <- "NGBR8AFL_value_labels.csv"
meta_json    <- "NGBR8AFL_meta.json"

if (file.exists(raw_csv)) {
  raw_preview <- data.table::fread(raw_csv, nrows = 200)
  readr::write_csv(
    as_tibble(raw_preview),
    file.path(out_dir, "section2_rawcodes_preview_first200.csv")
  )
}

if (file.exists(codebook_csv)) {
  cb <- data.table::fread(codebook_csv)
  readr::write_csv(
    as_tibble(cb),
    file.path(out_dir, "section2_codebook_full.csv")
  )
}

if (file.exists(val_labels)) {
  vl <- data.table::fread(val_labels)
  readr::write_csv(
    as_tibble(vl),
    file.path(out_dir, "section2_value_labels_full.csv")
  )
}

if (file.exists(meta_json)) {
  meta_list <- jsonlite::fromJSON(meta_json, simplifyDataFrame = TRUE)
  sink(file.path(out_dir, "section2_meta_summary.txt"))
  cat("Top-level elements in NGBR8AFL_meta.json:\n\n")
  print(names(meta_list))
  cat("\n\nStructure up to depth 1:\n")
  utils::str(meta_list, max.level = 1)
  sink()
}

## 3. Locate births Stata file -------------------------------------------

dta_file1 <- "NGBR8AFL.dta"
dta_file2 <- "NGBR8AFL.DTA"

births_candidates <- c(dta_file1, dta_file2)
existing <- births_candidates[file.exists(births_candidates)]

if (length(existing) == 0L) {
  stop(
    "Could not find NGBR8AFL Stata file in working directory.\n",
    "Place 'NGBR8AFL.dta' (or .DTA) in this folder."
  )
}

births_file <- existing[1]
message("Using births file: ", births_file)

## 4. Helper: read births via Python (if available) or haven --------------

read_births_file <- function(path) {
  if (!file.exists(path)) stop("Births file not found at: ", path)
  
  births <- NULL
  
  # Try Python + pandas (fast, robust)
  if (reticulate::py_available(initialize = TRUE)) {
    message("Python detected; attempting pandas read of Stata file...")
    try({
      pd    <- reticulate::import("pandas", convert = FALSE)
      df_py <- pd$read_stata(path)
      births <- reticulate::py_to_r(df_py)
      births <- tibble::as_tibble(births)
      message("Loaded births file with Python/pandas.")
    }, silent = TRUE)
  }
  
  # Fallback to haven if Python route fails
  if (is.null(births)) {
    message("Falling back to haven::read_dta() for births file...")
    births <- haven::read_dta(path)
  }
  
  births
}

## 5. Load and prepare births data ---------------------------------------

births_raw <- read_births_file(births_file)

# Ensure optional vars exist so later code never crashes
optional_vars <- c(
  "v022","v023","b0","b19","h2",
  "m3a","m3b","m3c","m3d","m3e","m3f","m3g","m3h","m3i","m3j","m3k"
)
for (nm in optional_vars) {
  if (!nm %in% names(births_raw)) births_raw[[nm]] <- NA_real_
}

# Core variables we absolutely need
core_vars <- c(
  "caseid",
  "v001","v002","v003","v005","v024","v025","v012","v106","v190","v201",
  "bidx","b2","b3","b4","b5","b7","b19","b0",
  "m14","m15","m17","m18",
  "m3a","m3b","m3c"
)

missing_core <- setdiff(core_vars, names(births_raw))
if (length(missing_core) > 0) {
  stop("Missing core variables in births file: ",
       paste(missing_core, collapse = ", "))
}

# Weighted helper functions ----------------------------------------------

wtd_mean <- function(x, w) {
  ok <- !is.na(x) & !is.na(w)
  if (!any(ok)) return(NA_real_)
  sum(w[ok] * x[ok]) / sum(w[ok])
}

wtd_var <- function(x, w) {
  ok <- !is.na(x) & !is.na(w)
  if (!any(ok)) return(NA_real_)
  m <- sum(w[ok] * x[ok]) / sum(w[ok])
  sum(w[ok] * (x[ok] - m)^2) / sum(w[ok])
}

wtd_prop_and_ci <- function(x, w) {
  ok <- !is.na(x) & !is.na(w)
  if (!any(ok)) {
    return(list(p = NA_real_, se = NA_real_,
                ci_l = NA_real_, ci_u = NA_real_))
  }
  x <- x[ok]; w <- w[ok]
  p_hat <- sum(w * x) / sum(w)
  neff  <- (sum(w)^2) / sum(w^2)
  se    <- sqrt(p_hat * (1 - p_hat) / neff)
  ci_l  <- p_hat - 1.96 * se
  ci_u  <- p_hat + 1.96 * se
  list(p = p_hat, se = se, ci_l = ci_l, ci_u = ci_u)
}

## 6. Clean and derive analysis variables --------------------------------

births_prepped <-
  births_raw %>%
  clean_names() %>%        # snake_case
  haven::zap_labels() %>%  # drop value labels -> numeric/character
  mutate(
    # survey design
    weight       = v005 / 1e6,
    cluster      = v001,
    hh_id        = v002,
    woman_line   = v003,
    stratum      = coalesce(v022, v023),
    region       = v024,
    urban        = if_else(v025 == 1, 1L, 0L, missing = NA_integer_),
    
    # maternal
    maternal_age = v012,
    parity       = v201,
    educ         = v106,
    wealth_q     = v190,
    
    # child / birth
    birth_order       = bidx,
    sex               = b4,
    multiple          = case_when(
      is.na(b0)        ~ 0L,
      b0 == 0          ~ 0L,
      b0 > 0           ~ 1L
    ),
    birth_cmc         = b3,
    birth_year        = b2,
    child_age_months  = b19,
    last5y_birth      = if_else(is.na(child_age_months),
                                TRUE,
                                child_age_months <= 59),
    
    # ANC
    anc_visits        = m14,
    
    # place + mode of delivery
    place_deliv       = m15,
    facility          = place_deliv >= 21 & place_deliv <= 39,
    csection          = (m17 == 1),
    
    # skilled attendant (doctor, nurse/midwife, auxiliary midwife)
    skilled_attendant = (m3a == 1 | m3b == 1 | m3c == 1),
    
    # outcomes
    early_neonatal_death = case_when(
      b5 == 0 & !is.na(b7) & b7 <= 7 ~ 1L,
      b5 == 1                        ~ 0L,
      TRUE                           ~ NA_integer_
    ),
    very_small_size = case_when(
      m18 == 5                      ~ 1L,
      m18 %in% c(1,2,3,4)           ~ 0L,
      TRUE                          ~ NA_integer_
    ),
    
    # facility sector
    facility_sector = case_when(
      place_deliv %in% c(21,22,23,24) ~ "public",
      place_deliv %in% c(31,32,33,34) ~ "private",
      facility                        ~ "other_facility",
      TRUE                            ~ "home_or_other"
    )
  )

## 7. Define sample & analytic cohort ------------------------------------

births_last5y             <- births_prepped %>% filter(last5y_birth)
births_facility           <- births_last5y %>% filter(facility)
births_facility_singleton <- births_facility %>% filter(multiple == 0L)
births_facility_recent    <- births_facility_singleton %>% filter(bidx == 1)

analysis_vars <- c(
  "caseid",
  "weight","cluster","stratum",
  "region","urban",
  "maternal_age","parity","educ","wealth_q",
  "birth_order","multiple","sex","birth_year",
  "anc_visits","facility_sector",
  "skilled_attendant","csection",
  "early_neonatal_death","very_small_size"
)

births_analytic <- births_facility_recent %>%
  select(any_of(analysis_vars)) %>%
  mutate(
    region          = factor(region),
    urban           = factor(urban),
    educ            = factor(educ),
    wealth_q        = factor(wealth_q),
    facility_sector = factor(facility_sector),
    sex             = factor(sex),
    skilled_attendant = as.integer(skilled_attendant),
    csection          = as.integer(csection)
  )

key_vars_for_cc <- c(
  "weight","cluster","stratum",
  "region","urban",
  "maternal_age","parity","educ","wealth_q",
  "birth_order","sex","birth_year",
  "anc_visits","facility_sector",
  "skilled_attendant","csection",
  "early_neonatal_death","very_small_size"
)

births_cc <- births_analytic %>%
  filter(if_all(all_of(key_vars_for_cc), ~ !is.na(.)))

message("Section 2 analytic complete-case sample size: ", nrow(births_cc))

readr::write_csv(
  births_cc,
  file.path(out_dir, "section2_births_analytic_cc.csv")
)

## 8. Simple survey design helper ----------------------------------------

make_design <- function(dat, wt_var = "weight") {
  svydesign(
    ids     = ~cluster,
    strata  = ~stratum,
    weights = as.formula(paste0("~", wt_var)),
    data    = dat,
    nest    = TRUE
  )
}

des <- make_design(births_cc, wt_var = "weight")

## 9. Overall SBA and C-section coverage ---------------------------------

overall_care <- tibble(
  indicator = c("Skilled birth attendance", "Caesarean delivery"),
  varname   = c("skilled_attendant", "csection")
) %>%
  rowwise() %>%
  mutate(
    prevalence = as.numeric(svymean(as.formula(paste0("~", varname)),
                                    design = des, na.rm = TRUE)),
    se         = as.numeric(SE(svymean(as.formula(paste0("~", varname)),
                                       design = des, na.rm = TRUE))),
    ci_l       = prevalence - 1.96 * se,
    ci_u       = prevalence + 1.96 * se
  ) %>%
  ungroup() %>%
  mutate(
    prevalence_pct = 100 * prevalence,
    ci_l_pct       = 100 * ci_l,
    ci_u_pct       = 100 * ci_u
  )

readr::write_csv(
  overall_care,
  file.path(out_dir, "section2_overall_care_coverage.csv")
)

## 10. Table 2: distribution of covariates by SBA and C-section ----------

# Numeric covariates for Table 2
num_covars <- c("maternal_age", "parity", "anc_visits", "birth_order")

# Categorical covariates for Table 2
cat_covars <- c("region", "urban", "educ", "wealth_q", "facility_sector", "sex")

# Helper to summarise numeric covariates by exposure
summarise_numeric_by_exposure <- function(dat, exposure, vars) {
  dat %>%
    select(all_of(c("weight", exposure, vars))) %>%
    pivot_longer(cols = all_of(vars),
                 names_to = "variable",
                 values_to = "value") %>%
    filter(!is.na(.data[[exposure]]), !is.na(value)) %>%
    group_by(variable, .data[[exposure]]) %>%
    summarise(
      n_unweighted = n(),
      n_weighted   = sum(weight, na.rm = TRUE),
      mean_w       = wtd_mean(value, weight),
      sd_w         = sqrt(wtd_var(value, weight)),
      .groups      = "drop"
    ) %>%
    mutate(
      exposure     = exposure,
      exposure_val = .data[[exposure]]
    ) %>%
    select(exposure, exposure_val, variable,
           n_unweighted, n_weighted, mean_w, sd_w)
}

# Helper to summarise categorical covariates by exposure
summarise_categorical_by_exposure <- function(dat, exposure, vars) {
  dat %>%
    select(all_of(c("weight", exposure, vars))) %>%
    pivot_longer(cols = all_of(vars),
                 names_to = "variable",
                 values_to = "level") %>%
    filter(!is.na(.data[[exposure]]), !is.na(level)) %>%
    group_by(variable, level, .data[[exposure]]) %>%
    summarise(
      n_unweighted = n(),
      w            = sum(weight, na.rm = TRUE),
      .groups      = "drop_last"
    ) %>%
    group_by(variable, .data[[exposure]]) %>%
    mutate(
      total_w = sum(w, na.rm = TRUE),
      prop    = w / total_w
    ) %>%
    ungroup() %>%
    mutate(
      prop_pct     = 100 * prop,
      exposure     = exposure,
      exposure_val = .data[[exposure]]
    ) %>%
    select(exposure, exposure_val, variable, level,
           n_unweighted, w, prop, prop_pct)
}

# SBA tables
table2_sba_numeric <-
  summarise_numeric_by_exposure(births_cc, "skilled_attendant", num_covars)
table2_sba_categorical <-
  summarise_categorical_by_exposure(births_cc, "skilled_attendant", cat_covars)

readr::write_csv(
  table2_sba_numeric,
  file.path(out_dir, "section2_table2_sba_numeric.csv")
)
readr::write_csv(
  table2_sba_categorical,
  file.path(out_dir, "section2_table2_sba_categorical.csv")
)

# C-section tables
table2_cs_numeric <-
  summarise_numeric_by_exposure(births_cc, "csection", num_covars)
table2_cs_categorical <-
  summarise_categorical_by_exposure(births_cc, "csection", cat_covars)

readr::write_csv(
  table2_cs_numeric,
  file.path(out_dir, "section2_table2_csection_numeric.csv")
)
readr::write_csv(
  table2_cs_categorical,
  file.path(out_dir, "section2_table2_csection_categorical.csv")
)

## 11. Equity gradients: SBA & C-section by wealth & region --------------

calc_prop_by_group <- function(dat, exposure, group_var, indicator_label) {
  dat %>%
    filter(!is.na(.data[[group_var]]), !is.na(.data[[exposure]])) %>%
    group_by(group_level = .data[[group_var]]) %>%
    group_modify(function(df, key) {
      tmp <- wtd_prop_and_ci(df[[exposure]], df$weight)
      tibble(
        n_unweighted   = nrow(df),
        n_weighted     = sum(df$weight, na.rm = TRUE),
        prevalence     = tmp$p,
        se             = tmp$se,
        ci_l           = tmp$ci_l,
        ci_u           = tmp$ci_u,
        prevalence_pct = 100 * tmp$p,
        ci_l_pct       = 100 * tmp$ci_l,
        ci_u_pct       = 100 * tmp$ci_u
      )
    }) %>%
    ungroup() %>%
    mutate(
      group_var = group_var,
      indicator = indicator_label,
      varname   = exposure
    ) %>%
    select(group_var, group_level, indicator, varname,
           n_unweighted, n_weighted,
           prevalence, se, ci_l, ci_u,
           prevalence_pct, ci_l_pct, ci_u_pct)
}

eq_sba_wealth <- calc_prop_by_group(
  births_cc, "skilled_attendant", "wealth_q",
  indicator_label = "Skilled birth attendance"
)
eq_sba_region <- calc_prop_by_group(
  births_cc, "skilled_attendant", "region",
  indicator_label = "Skilled birth attendance"
)

eq_cs_wealth <- calc_prop_by_group(
  births_cc, "csection", "wealth_q",
  indicator_label = "Caesarean delivery"
)
eq_cs_region <- calc_prop_by_group(
  births_cc, "csection", "region",
  indicator_label = "Caesarean delivery"
)

equity_gradients <-
  bind_rows(eq_sba_wealth, eq_sba_region,
            eq_cs_wealth, eq_cs_region)

readr::write_csv(
  equity_gradients,
  file.path(out_dir, "section2_equity_gradients.csv")
)

## 12. Figures 2A: coverage by wealth and region -------------------------

# Wealth figure
fig2a_wealth <-
  equity_gradients %>%
  filter(group_var == "wealth_q") %>%
  mutate(
    group_level = factor(group_level, levels = sort(unique(group_level))),
    indicator   = factor(indicator,
                         levels = c("Skilled birth attendance",
                                    "Caesarean delivery"))
  ) %>%
  ggplot(aes(x = group_level, y = prevalence_pct,
             fill = indicator)) +
  geom_col(position = position_dodge(width = 0.8), width = 0.7) +
  labs(
    x = "Wealth quintile",
    y = "Coverage (%)",
    title = "Coverage of skilled birth attendance and C-section by wealth quintile"
  ) +
  theme_minimal(base_size = 12)

ggsave(
  filename = file.path(out_dir, "figure_section2_coverage_by_wealth.png"),
  plot     = fig2a_wealth,
  width    = 7,
  height   = 4.5,
  dpi      = 300
)

# Region figure
fig2a_region <-
  equity_gradients %>%
  filter(group_var == "region") %>%
  mutate(
    group_level = factor(group_level, levels = sort(unique(group_level))),
    indicator   = factor(indicator,
                         levels = c("Skilled birth attendance",
                                    "Caesarean delivery"))
  ) %>%
  ggplot(aes(x = group_level, y = prevalence_pct,
             fill = indicator)) +
  geom_col(position = position_dodge(width = 0.8), width = 0.7) +
  labs(
    x = "Region (DHS code)",
    y = "Coverage (%)",
    title = "Coverage of skilled birth attendance and C-section by region"
  ) +
  theme_minimal(base_size = 12) +
  coord_flip()

ggsave(
  filename = file.path(out_dir, "figure_section2_coverage_by_region.png"),
  plot     = fig2a_region,
  width    = 7,
  height   = 5,
  dpi      = 300
)

## 13. Overlap weights + covariate balance (SMD) --------------------------

# Propensity-score overlap weights (shared with later sections)
compute_overlap_weights <- function(dat, exposure, covars, base_wt = "weight") {
  fml_ps <- as.formula(
    paste0(exposure, " ~ ", paste(covars, collapse = " + "))
  )
  ps_mod <- suppressWarnings(glm(
    fml_ps,
    data    = dat,
    family  = binomial(),
    weights = dat[[base_wt]]
  ))
  e_hat <- pmin(pmax(fitted(ps_mod), 0.01), 0.99)
  a     <- dat[[exposure]]
  if (!all(a %in% c(0,1))) stop("Exposure must be coded 0/1.")
  ifelse(a == 1, 1 - e_hat, e_hat)
}

# Covariates for PS models (same as in Section 3)
ps_covars <- c(
  "maternal_age","parity","educ","wealth_q",
  "urban","region",
  "anc_visits","birth_order",
  "sex","birth_year","facility_sector"
)

# Build SBA overlap-weighted data
dat_sba <- births_cc %>%
  mutate(
    ow_sba        = compute_overlap_weights(., "skilled_attendant", ps_covars),
    weight_ow_sba = weight * ow_sba
  )

# Build C-section overlap-weighted data
dat_cs <- births_cc %>%
  mutate(
    ow_cs        = compute_overlap_weights(., "csection", ps_covars),
    weight_ow_cs = weight * ow_cs
  )

# Function: SMD for one covariate, one exposure, one weight
compute_smd_one <- function(dat, exposure, covar, wt_var) {
  x <- dat[[covar]]
  a <- dat[[exposure]]
  w <- dat[[wt_var]]
  
  ok <- !is.na(x) & !is.na(a) & !is.na(w)
  x <- x[ok]; a <- a[ok]; w <- w[ok]
  
  if (length(unique(a)) < 2) {
    return(tibble(covariate = covar, smd = NA_real_))
  }
  
  if (is.numeric(x)) {
    w0 <- w[a == 0]; x0 <- x[a == 0]
    w1 <- w[a == 1]; x1 <- x[a == 1]
    m0 <- sum(w0 * x0) / sum(w0)
    m1 <- sum(w1 * x1) / sum(w1)
    v0 <- sum(w0 * (x0 - m0)^2) / sum(w0)
    v1 <- sum(w1 * (x1 - m1)^2) / sum(w1)
    sd_p <- sqrt((v0 + v1) / 2)
    smd  <- ifelse(sd_p > 0, (m1 - m0) / sd_p, 0)
    tibble(covariate = covar, smd = smd)
  } else {
    f <- as.factor(x)
    levs <- levels(f)
    if (length(levs) < 2) {
      return(tibble(covariate = covar, smd = 0))
    }
    smd_levels <- c()
    for (l in levs[-1]) { # compare each non-reference level
      ind <- as.numeric(f == l)
      w0 <- w[a == 0]; i0 <- ind[a == 0]
      w1 <- w[a == 1]; i1 <- ind[a == 1]
      p0 <- sum(w0 * i0) / sum(w0)
      p1 <- sum(w1 * i1) / sum(w1)
      v0 <- p0 * (1 - p0)
      v1 <- p1 * (1 - p1)
      denom <- sqrt((v0 + v1) / 2)
      smd_l <- ifelse(denom > 0, (p1 - p0) / denom, 0)
      smd_levels <- c(smd_levels, smd_l)
    }
    tibble(covariate = covar, smd = max(abs(smd_levels)))
  }
}

compute_smd_prepost <- function(dat, exposure, covars,
                                wt_pre = "weight", wt_post) {
  pre <- purrr::map_dfr(
    covars,
    ~ compute_smd_one(dat, exposure = exposure, covar = .x, wt_var = wt_pre)
  ) %>%
    rename(smd_pre = smd)
  
  post <- purrr::map_dfr(
    covars,
    ~ compute_smd_one(dat, exposure = exposure, covar = .x, wt_var = wt_post)
  ) %>%
    rename(smd_post = smd)
  
  left_join(pre, post, by = "covariate")
}

# SBA balance
balance_sba <- compute_smd_prepost(
  dat    = dat_sba,
  exposure = "skilled_attendant",
  covars   = ps_covars,
  wt_pre   = "weight",
  wt_post  = "weight_ow_sba"
) %>%
  mutate(exposure_label = "Skilled birth attendance")

# C-section balance
balance_cs <- compute_smd_prepost(
  dat    = dat_cs,
  exposure = "csection",
  covars   = ps_covars,
  wt_pre   = "weight",
  wt_post  = "weight_ow_cs"
) %>%
  mutate(exposure_label = "Caesarean delivery")

balance_all <- bind_rows(balance_sba, balance_cs)

readr::write_csv(
  balance_all,
  file.path(out_dir, "section2_covariate_balance_smd.csv")
)

## 14. Figure 2B: SMD before vs after overlap weighting -------------------

var_labels <- c(
  maternal_age   = "Maternal age",
  parity         = "Parity",
  educ           = "Education",
  wealth_q       = "Wealth quintile",
  urban          = "Urban residence",
  region         = "Region",
  anc_visits     = "ANC visits",
  birth_order    = "Birth order",
  sex            = "Infant sex",
  birth_year     = "Birth year",
  facility_sector = "Facility sector"
)

balance_plot_df <-
  balance_all %>%
  mutate(
    covariate_clean = dplyr::recode(covariate, !!!var_labels)
  ) %>%
  pivot_longer(cols = c("smd_pre", "smd_post"),
               names_to = "time",
               values_to = "smd") %>%
  mutate(
    time = recode(time,
                  smd_pre  = "Before overlap weighting",
                  smd_post = "After overlap weighting"),
    covariate_clean = factor(
      covariate_clean,
      levels = rev(unique(covariate_clean))
    )
  )

fig2b_smd <-
  balance_plot_df %>%
  ggplot(aes(x = covariate_clean, y = abs(smd),
             color = time, group = time)) +
  geom_point() +
  geom_line() +
  coord_flip() +
  facet_wrap(~ exposure_label) +
  geom_hline(yintercept = 0.1, linetype = "dashed") +
  labs(
    x = NULL,
    y = "Absolute standardised mean difference",
    title = "Covariate balance before and after overlap weighting",
    color = NULL
  ) +
  theme_minimal(base_size = 12)

ggsave(
  filename = file.path(out_dir, "figure_section2_balance_smd.png"),
  plot     = fig2b_smd,
  width    = 8,
  height   = 6,
  dpi      = 300
)

message("Section 2 script completed: all tables and figures saved in '",
        out_dir, "'.")




############################################################
## 15. Manuscript “hero” Table 2 and Figure 2  ------------
############################################################

# Safety checks so nothing crashes silently
needed_objects <- c(
  "table2_sba_numeric", "table2_sba_categorical",
  "table2_cs_numeric",  "table2_cs_categorical",
  "equity_gradients"
)

missing_objs <- needed_objects[!vapply(needed_objects, exists, logical(1))]
if (length(missing_objs) > 0) {
  warning("The following objects are missing and Table 2 / Figure 2 ",
          "cannot be fully generated: ",
          paste(missing_objs, collapse = ", "))
} else {
  
  ## 15.1 Construct a clean, manuscript-ready Table 2 --------------------
  ##     - SBA: mean (SD) or n (%)
  ##     - C-section: mean (SD) or n (%)
  
  # SBA: numeric variables -> mean (SD)
  sba_num <- table2_sba_numeric %>%
    mutate(
      exposure_val = if_else(exposure_val == 1, "SBA_yes", "SBA_no"),
      level        = NA_character_,
      stat         = sprintf("%.1f (%.1f)", mean_w, sd_w)
    ) %>%
    select(variable, level, exposure_val,
           n_unweighted, stat)
  
  # SBA: categorical variables -> n (%)
  sba_cat <- table2_sba_categorical %>%
    mutate(
      exposure_val = if_else(exposure_val == 1, "SBA_yes", "SBA_no"),
      level        = as.character(level),
      stat         = sprintf("%d (%.1f%%)",
                             round(w), prop_pct),
      n_unweighted = n_unweighted
    ) %>%
    select(variable, level, exposure_val,
           n_unweighted, stat)
  
  sba_all <- bind_rows(sba_num, sba_cat)
  
  table2_sba_main <-
    sba_all %>%
    mutate(
      exp_label = if_else(exposure_val == "SBA_yes", "SBA: skilled", "SBA: non-skilled")
    ) %>%
    select(variable, level, exp_label, n_unweighted, stat) %>%
    mutate(
      exp_label = factor(exp_label,
                         levels = c("SBA: non-skilled", "SBA: skilled"))
    ) %>%
    arrange(variable, level, exp_label) %>%
    pivot_wider(
      id_cols     = c(variable, level),
      names_from  = exp_label,
      values_from = c(n_unweighted, stat),
      names_glue  = "{exp_label}_{.value}"
    )
  
  readr::write_csv(
    table2_sba_main,
    file.path(out_dir, "section2_table2_sba_main.csv")
  )
  
  # C-section: numeric variables -> mean (SD)
  cs_num <- table2_cs_numeric %>%
    mutate(
      exposure_val = if_else(exposure_val == 1, "CS_yes", "CS_no"),
      level        = NA_character_,
      stat         = sprintf("%.1f (%.1f)", mean_w, sd_w)
    ) %>%
    select(variable, level, exposure_val,
           n_unweighted, stat)
  
  # C-section: categorical variables -> n (%)
  cs_cat <- table2_cs_categorical %>%
    mutate(
      exposure_val = if_else(exposure_val == 1, "CS_yes", "CS_no"),
      level        = as.character(level),
      stat         = sprintf("%d (%.1f%%)",
                             round(w), prop_pct),
      n_unweighted = n_unweighted
    ) %>%
    select(variable, level, exposure_val,
           n_unweighted, stat)
  
  cs_all <- bind_rows(cs_num, cs_cat)
  
  table2_cs_main <-
    cs_all %>%
    mutate(
      exp_label = if_else(exposure_val == "CS_yes", "CS: caesarean", "CS: vaginal")
    ) %>%
    select(variable, level, exp_label, n_unweighted, stat) %>%
    mutate(
      exp_label = factor(exp_label,
                         levels = c("CS: vaginal", "CS: caesarean"))
    ) %>%
    arrange(variable, level, exp_label) %>%
    pivot_wider(
      id_cols     = c(variable, level),
      names_from  = exp_label,
      values_from = c(n_unweighted, stat),
      names_glue  = "{exp_label}_{.value}"
    )
  
  readr::write_csv(
    table2_cs_main,
    file.path(out_dir, "section2_table2_csection_main.csv")
  )
  
  # Optional: pretty HTML versions if knitr is available
  if (requireNamespace("knitr", quietly = TRUE)) {
    html_sba <- knitr::kable(
      table2_sba_main,
      format = "html",
      caption = "Table 2A. Baseline characteristics by skilled birth attendance"
    )
    writeLines(
      html_sba,
      con = file.path(out_dir, "section2_table2_sba_main.html")
    )
    
    html_cs <- knitr::kable(
      table2_cs_main,
      format = "html",
      caption = "Table 2B. Baseline characteristics by caesarean delivery"
    )
    writeLines(
      html_cs,
      con = file.path(out_dir, "section2_table2_csection_main.html")
    )
  }
  
  ## 15.2 Manuscript Figure 2A: Coverage by wealth quintile ---------------
  
  # If equity_gradients is missing this block would have failed earlier,
  # but we check again for safety.
  if (!exists("equity_gradients")) {
    warning("Object 'equity_gradients' not found; cannot generate Figure 2A.")
  } else {
    fig2_wealth_main <-
      equity_gradients %>%
      filter(group_var == "wealth_q") %>%
      mutate(
        group_level = factor(group_level, levels = sort(unique(group_level))),
        indicator   = factor(indicator,
                             levels = c("Skilled birth attendance",
                                        "Caesarean delivery"))
      ) %>%
      ggplot(aes(x = group_level,
                 y = prevalence_pct,
                 group = indicator)) +
      geom_line() +
      geom_point() +
      labs(
        x = "Wealth quintile",
        y = "Coverage (%)",
        title = "Figure 2A. Coverage of skilled birth attendance\nand caesarean delivery by wealth quintile",
        color = NULL
      ) +
      scale_y_continuous(labels = label_percent(accuracy = 1, scale = 1)) +
      theme_minimal(base_size = 12)
    
    ggsave(
      filename = file.path(out_dir, "figure_section2_main_coverage_wealth.png"),
      plot     = fig2_wealth_main,
      width    = 7,
      height   = 4.5,
      dpi      = 300
    )
  }
  
  ## 15.3 Manuscript Figure 2B: Covariate balance (if already computed) ----
  
  if (exists("balance_all")) {
    var_labels <- c(
      maternal_age   = "Maternal age",
      parity         = "Parity",
      educ           = "Education",
      wealth_q       = "Wealth quintile",
      urban          = "Urban residence",
      region         = "Region",
      anc_visits     = "ANC visits",
      birth_order    = "Birth order",
      sex            = "Infant sex",
      birth_year     = "Birth year",
      facility_sector = "Facility sector"
    )
    
    balance_plot_df <-
      balance_all %>%
      mutate(
        covariate_clean = dplyr::recode(covariate, !!!var_labels)
      ) %>%
      pivot_longer(
        cols = c("smd_pre", "smd_post"),
        names_to = "time",
        values_to = "smd"
      ) %>%
      mutate(
        time = recode(
          time,
          smd_pre  = "Before overlap weighting",
          smd_post = "After overlap weighting"
        ),
        covariate_clean = factor(
          covariate_clean,
          levels = rev(unique(covariate_clean))
        )
      )
    
    fig2b_balance_main <-
      balance_plot_df %>%
      ggplot(aes(x = covariate_clean,
                 y = abs(smd),
                 color = time,
                 group = time)) +
      geom_point() +
      geom_line() +
      coord_flip() +
      facet_wrap(~ exposure_label) +
      geom_hline(yintercept = 0.1, linetype = "dashed") +
      labs(
        x = NULL,
        y = "Absolute standardized mean difference",
        title = "Figure 2B. Covariate balance before and after overlap weighting",
        color = NULL
      ) +
      theme_minimal(base_size = 12)
    
    ggsave(
      filename = file.path(out_dir, "figure_section2_main_balance_smd.png"),
      plot     = fig2b_balance_main,
      width    = 8,
      height   = 6,
      dpi      = 300
    )
  } else {
    warning("Object 'balance_all' not found; Figure 2B was not created.")
  }
}

message("Section 2: hero Table 2 and Figure 2 generation complete.")
