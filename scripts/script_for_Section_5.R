############################################################
# Results Section 5: Associations with Very Small Size at Birth
# Data: Nigeria DHS births recode 2018 (NGBR8AFL)
# Output folder: outputs_section5/
############################################################

## 0. Packages -----------------------------------------------------------

required_pkgs <- c(
  "tidyverse",   # dplyr, tidyr, ggplot2, readr, etc.
  "data.table",  # fast reading of CSVs if present
  "haven",       # read_dta
  "janitor",     # clean_names
  "survey",      # complex survey design + svyglm/svyolr
  "reticulate",  # optional Python bridge
  "jsonlite",    # meta json
  "scales",      # nice labels
  "broom"        # tidy model outputs
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
library(broom)

options(dplyr.summarise.inform = FALSE)
options(survey.lonely.psu = "adjust")

## 1. Output directory ---------------------------------------------------

out_dir <- "outputs_section5"
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
    file.path(out_dir, "section5_rawcodes_preview_first200.csv")
  )
}

if (file.exists(codebook_csv)) {
  cb <- data.table::fread(codebook_csv)
  readr::write_csv(
    as_tibble(cb),
    file.path(out_dir, "section5_codebook_full.csv")
  )
}

if (file.exists(val_labels)) {
  vl <- data.table::fread(val_labels)
  readr::write_csv(
    as_tibble(vl),
    file.path(out_dir, "section5_value_labels_full.csv")
  )
}

if (file.exists(meta_json)) {
  meta_list <- jsonlite::fromJSON(meta_json, simplifyDataFrame = TRUE)
  sink(file.path(out_dir, "section5_meta_summary.txt"))
  cat("Top-level elements in NGBR8AFL_meta.json:\n\n")
  print(names(meta_list))
  cat("\n\nStructure up to depth 1:\n")
  utils::str(meta_list, max.level = 1)
  sink()
}

## 3. Helper: read births via Python (if available) or haven --------------

read_births_file <- function(path) {
  if (!file.exists(path)) stop("Births file not found at: ", path)
  
  births <- NULL
  
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
  
  if (is.null(births)) {
    message("Falling back to haven::read_dta() for births file...")
    births <- haven::read_dta(path)
  }
  
  births
}

## 4. Load or rebuild analytic cohort -----------------------------------
##    Prefer Section 1 analytic cohort; else rebuild from raw births.

analytic_from_section1 <- file.path("outputs_section1", "section1_births_analytic_cc.csv")

if (file.exists(analytic_from_section1)) {
  message("Loading analytic cohort from Section 1 CSV...")
  births_cc <- readr::read_csv(analytic_from_section1, show_col_types = FALSE)
  
  births_cc <- births_cc %>%
    mutate(
      region          = factor(region),
      urban           = factor(urban),
      educ            = factor(educ),
      wealth_q        = factor(wealth_q),
      facility_sector = factor(facility_sector),
      sex             = factor(sex),
      skilled_attendant = as.integer(skilled_attendant),
      csection          = as.integer(csection),
      early_neonatal_death = as.integer(early_neonatal_death),
      very_small_size     = as.integer(very_small_size)
    )
  
} else {
  message("Section 1 analytic cohort not found; rebuilding from raw births file...")
  
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
  
  births_raw <- read_births_file(births_file)
  
  optional_vars <- c(
    "v022","v023","b0","b19","h2",
    "m3a","m3b","m3c","m3d","m3e","m3f","m3g","m3h","m3i","m3j","m3k",
    "m18"
  )
  for (nm in optional_vars) {
    if (!nm %in% names(births_raw)) births_raw[[nm]] <- NA_real_
  }
  
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
  
  births_prepped <-
    births_raw %>%
    clean_names() %>%
    haven::zap_labels() %>%
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
      
      # skilled attendant
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
    "early_neonatal_death","very_small_size",
    "m18"
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
  
  message("Rebuilt analytic complete-case sample size: ", nrow(births_cc))
  
  readr::write_csv(
    births_cc,
    file.path(out_dir, "section5_births_analytic_cc.csv")
  )
}

## 5. Ensure coding & (optional) 5-level size category -------------------

births_cc <- births_cc %>%
  mutate(
    csection          = if_else(csection == 1L, 1L, 0L, missing = NA_integer_),
    skilled_attendant = if_else(skilled_attendant == 1L, 1L, 0L,
                                missing = NA_integer_),
    very_small_size   = if_else(very_small_size == 1L, 1L, 0L,
                                missing = NA_integer_)
  )

# Optional ordered 5-category size variable if original m18 is present
if ("m18" %in% names(births_cc)) {
  births_cc <- births_cc %>%
    mutate(
      size_cat5 = case_when(
        m18 == 1 ~ "very_large",
        m18 == 2 ~ "larger_than_average",
        m18 == 3 ~ "average",
        m18 == 4 ~ "smaller_than_average",
        m18 == 5 ~ "very_small",
        TRUE     ~ NA_character_
      ),
      size_cat5 = factor(
        size_cat5,
        levels = c("very_large", "larger_than_average",
                   "average", "smaller_than_average", "very_small"),
        ordered = TRUE
      )
    )
}

message("Section 5 analytic sample size (non-missing exposures/outcome): ",
        nrow(births_cc %>%
               filter(!is.na(skilled_attendant),
                      !is.na(csection),
                      !is.na(very_small_size))))

## 6. Survey design helper -----------------------------------------------

make_design <- function(dat, wt_var = "weight") {
  svydesign(
    ids     = ~cluster,
    strata  = ~stratum,
    weights = as.formula(paste0("~", wt_var)),
    data    = dat,
    nest    = TRUE
  )
}

get_se_col <- function(tbl) {
  se_col <- names(tbl)[grepl("^se", names(tbl))][1]
  if (is.na(se_col)) stop("No SE column in svyby output.")
  se_col
}

## 7. Crude association function (survey-weighted) -----------------------

analyze_crude <- function(dat, exposure, outcome) {
  dat_use <- dat %>% filter(!is.na(.data[[exposure]]), !is.na(.data[[outcome]]))
  des <- make_design(dat_use, wt_var = "weight")
  
  fml_mean <- as.formula(paste0("~", outcome))
  by_var   <- as.formula(paste0("~", exposure))
  
  risk_by_exp <- svyby(
    formula = fml_mean,
    by      = by_var,
    design  = des,
    FUN     = svymean,
    vartype = c("se","ci"),
    na.rm   = TRUE
  )
  
  se_col <- get_se_col(risk_by_exp)
  
  risk0 <- risk_by_exp %>% filter(.data[[exposure]] == 0)
  risk1 <- risk_by_exp %>% filter(.data[[exposure]] == 1)
  
  p0  <- risk0[[outcome]]
  se0 <- risk0[[se_col]]
  p1  <- risk1[[outcome]]
  se1 <- risk1[[se_col]]
  
  # Risk difference
  rd    <- p1 - p0
  se_rd <- sqrt(se0^2 + se1^2)
  ci_rd <- rd + c(-1, 1) * 1.96 * se_rd
  
  # Risk ratio (guard against zero)
  if (p0 > 0 && p1 > 0) {
    rr <- p1 / p0
    se_log_rr <- sqrt((se1^2 / p1^2) + (se0^2 / p0^2))
    ci_log_rr <- log(rr) + c(-1,1) * 1.96 * se_log_rr
    ci_rr <- exp(ci_log_rr)
  } else {
    rr    <- NA_real_
    ci_rr <- c(NA_real_, NA_real_)
  }
  
  # Logistic regression for OR
  fml_glm <- as.formula(paste0(outcome, " ~ ", exposure))
  fit_glm <- svyglm(fml_glm, design = des, family = quasibinomial())
  
  beta    <- coef(fit_glm)[[exposure]]
  se_beta <- sqrt(vcov(fit_glm)[exposure, exposure])
  or      <- exp(beta)
  ci_or   <- exp(beta + c(-1,1) * 1.96 * se_beta)
  
  tibble(
    adjustment   = "Crude (survey-weighted)",
    exposure     = exposure,
    outcome      = outcome,
    risk_exposed = p1,
    risk_unexp   = p0,
    rd           = rd,
    rd_ci_l      = ci_rd[1],
    rd_ci_u      = ci_rd[2],
    rr           = rr,
    rr_ci_l      = ci_rr[1],
    rr_ci_u      = ci_rr[2],
    or           = or,
    or_ci_l      = ci_or[1],
    or_ci_u      = ci_or[2],
    events_diff_per_1000 = (p1 - p0) * 1000
  )
}

## 8. Propensity-score overlap weights -----------------------------------

compute_overlap_weights <- function(data, exposure, covars, base_wt = "weight") {
  fml_ps <- as.formula(
    paste0(exposure, " ~ ", paste(covars, collapse = " + "))
  )
  
  ps_mod <- suppressWarnings(glm(
    fml_ps,
    data    = data,
    family  = binomial(),
    weights = data[[base_wt]]
  ))
  
  e_hat <- pmin(pmax(fitted(ps_mod), 0.01), 0.99)
  a     <- data[[exposure]]
  if (!all(a %in% c(0,1))) stop("Exposure must be coded 0/1.")
  
  ifelse(a == 1, 1 - e_hat, e_hat)
}

analyze_overlap <- function(dat, exposure, outcome, covars) {
  dat_use <- dat %>% filter(!is.na(.data[[exposure]]), !is.na(.data[[outcome]]))
  
  dat_use <- dat_use %>%
    mutate(
      ow        = compute_overlap_weights(., exposure, covars),
      weight_ow = weight * ow
    )
  
  des_ow <- make_design(dat_use, wt_var = "weight_ow")
  
  fml_mean <- as.formula(paste0("~", outcome))
  by_var   <- as.formula(paste0("~", exposure))
  
  risk_by_exp <- svyby(
    formula = fml_mean,
    by      = by_var,
    design  = des_ow,
    FUN     = svymean,
    vartype = c("se","ci"),
    na.rm   = TRUE
  )
  
  se_col <- get_se_col(risk_by_exp)
  
  risk0 <- risk_by_exp %>% filter(.data[[exposure]] == 0)
  risk1 <- risk_by_exp %>% filter(.data[[exposure]] == 1)
  
  p0  <- risk0[[outcome]]
  se0 <- risk0[[se_col]]
  p1  <- risk1[[outcome]]
  se1 <- risk1[[se_col]]
  
  # Risk difference
  rd    <- p1 - p0
  se_rd <- sqrt(se0^2 + se1^2)
  ci_rd <- rd + c(-1, 1) * 1.96 * se_rd
  
  # Risk ratio (guard against zero)
  if (p0 > 0 && p1 > 0) {
    rr <- p1 / p0
    se_log_rr <- sqrt((se1^2 / p1^2) + (se0^2 / p0^2))
    ci_log_rr <- log(rr) + c(-1,1) * 1.96 * se_log_rr
    ci_rr <- exp(ci_log_rr)
  } else {
    rr    <- NA_real_
    ci_rr <- c(NA_real_, NA_real_)
  }
  
  # Logistic regression for OR
  fml_glm <- as.formula(paste0(outcome, " ~ ", exposure))
  fit_glm <- svyglm(fml_glm, design = des_ow, family = quasibinomial())
  
  beta    <- coef(fit_glm)[[exposure]]
  se_beta <- sqrt(vcov(fit_glm)[exposure, exposure])
  or      <- exp(beta)
  ci_or   <- exp(beta + c(-1,1) * 1.96 * se_beta)
  
  tibble(
    adjustment   = "Adjusted (PS overlap-weighted + survey)",
    exposure     = exposure,
    outcome      = outcome,
    risk_exposed = p1,
    risk_unexp   = p0,
    rd           = rd,
    rd_ci_l      = ci_rd[1],
    rd_ci_u      = ci_rd[2],
    rr           = rr,
    rr_ci_l      = ci_rr[1],
    rr_ci_u      = ci_rr[2],
    or           = or,
    or_ci_l      = ci_or[1],
    or_ci_u      = ci_or[2],
    events_diff_per_1000 = (p1 - p0) * 1000
  )
}

## 9. Covariates for PS models -------------------------------------------

ps_covars <- c(
  "maternal_age","parity","educ","wealth_q",
  "urban","region",
  "anc_visits","birth_order",
  "sex","birth_year","facility_sector"
)

## 10. SBA vs very small size --------------------------------------------

sba_crude <- analyze_crude(
  dat      = births_cc,
  exposure = "skilled_attendant",
  outcome  = "very_small_size"
)

sba_adjust <- analyze_overlap(
  dat      = births_cc,
  exposure = "skilled_attendant",
  outcome  = "very_small_size",
  covars   = ps_covars
)

results_sba_size <- bind_rows(sba_crude, sba_adjust)

readr::write_csv(
  results_sba_size,
  file.path(out_dir, "section5_sba_very_small_size_results.csv")
)

## 11. C-section vs very small size --------------------------------------

cs_crude_size <- analyze_crude(
  dat      = births_cc,
  exposure = "csection",
  outcome  = "very_small_size"
)

cs_adjust_size <- analyze_overlap(
  dat      = births_cc,
  exposure = "csection",
  outcome  = "very_small_size",
  covars   = ps_covars
)

results_cs_size <- bind_rows(cs_crude_size, cs_adjust_size)

readr::write_csv(
  results_cs_size,
  file.path(out_dir, "section5_csection_very_small_size_results.csv")
)

## 12. Optional ordered-outcome analysis (5 DHS size categories) ---------

if ("size_cat5" %in% names(births_cc)) {
  message("Running ordered logistic model on 5-level size outcome...")
  
  births_size <- births_cc %>%
    filter(!is.na(size_cat5), !is.na(csection))
  
  births_size <- births_size %>%
    mutate(
      ow_size    = compute_overlap_weights(., "csection", ps_covars),
      weight_ows = weight * ow_size
    )
  
  des_ows <- make_design(births_size, wt_var = "weight_ows")
  
  fml_olr <- as.formula(
    paste0("size_cat5 ~ csection + ",
           paste(ps_covars, collapse = " + "))
  )
  
  fit_olr <- svyolr(fml_olr, design = des_ows)
  
  olr_tidy <- broom::tidy(fit_olr, conf.int = TRUE, exponentiate = TRUE)
  
  readr::write_csv(
    olr_tidy,
    file.path(out_dir, "section5_ordered_size_csection_results.csv")
  )
  
} else {
  msg <- "Original 5-category size variable (m18) not available; ordered model skipped."
  writeLines(msg, con = file.path(out_dir, "section5_ordered_model_skipped.txt"))
  message(msg)
}

## 13. Tables for manuscript (HTML, optional) ----------------------------

if (requireNamespace("knitr", quietly = TRUE)) {
  
  tbl5 <- results_sba_size %>%
    mutate(
      risk_exposed_pct = 100 * risk_exposed,
      risk_unexp_pct   = 100 * risk_unexp,
      rd_pct           = 100 * rd
    )
  
  html5 <- knitr::kable(
    tbl5,
    format  = "html",
    digits  = 3,
    caption = "Table 5. Skilled birth attendance vs non-skilled attendance: crude and overlap-weighted associations with very small size at birth"
  )
  writeLines(html5,
             con = file.path(out_dir, "section5_table_sba_very_small_size.html"))
  
  tbl6 <- results_cs_size %>%
    mutate(
      risk_exposed_pct = 100 * risk_exposed,
      risk_unexp_pct   = 100 * risk_unexp,
      rd_pct           = 100 * rd
    )
  
  html6 <- knitr::kable(
    tbl6,
    format  = "html",
    digits  = 3,
    caption = "Table 6. Caesarean vs vaginal delivery: crude and overlap-weighted associations with very small size at birth"
  )
  writeLines(html6,
             con = file.path(out_dir, "section5_table_csection_very_small_size.html"))
}

## 14. Key figures for Section 5 -----------------------------------------

# 14.1 Combine SBA and C-section into one long table
results_all_size <- bind_rows(
  results_sba_size %>% mutate(exposure_label = "Skilled birth attendance"),
  results_cs_size %>% mutate(exposure_label = "Caesarean section")
) %>%
  mutate(
    adj_type = case_when(
      grepl("Crude", adjustment)    ~ "Crude",
      grepl("Adjusted", adjustment) ~ "Adjusted",
      TRUE                          ~ adjustment
    ),
    adj_type = factor(adj_type, levels = c("Crude", "Adjusted"))
  )

# 14.2 Figure 5A: risk differences (% points) for very small size
fig5A <- results_all_size %>%
  mutate(rd_pct = 100 * rd) %>%
  ggplot(aes(x = adj_type, y = rd_pct)) +
  geom_hline(yintercept = 0, linetype = "dashed") +
  geom_col(width = 0.5) +
  facet_wrap(~ exposure_label, nrow = 1) +
  labs(
    x    = NULL,
    y    = "Risk difference (% points)\nVery small size at birth",
    title = "Figure 5A. Crude vs overlap-weighted risk differences\nfor very small size at birth (SBA and caesarean section)"
  ) +
  theme_minimal(base_size = 12)

ggsave(
  filename = file.path(out_dir, "figure5_1_size_risk_diff.png"),
  plot     = fig5A,
  width    = 8,
  height   = 4.5,
  dpi      = 300
)

# 14.3 Figure 5B: events added/averted per 1,000 births
fig5B <- results_all_size %>%
  ggplot(aes(x = adj_type, y = events_diff_per_1000)) +
  geom_hline(yintercept = 0, linetype = "dashed") +
  geom_col(width = 0.5) +
  facet_wrap(~ exposure_label, nrow = 1) +
  labs(
    x    = NULL,
    y    = "Difference in very-small events per 1,000 births\n(>0 = more very small; <0 = fewer very small)",
    title = "Figure 5B. Change in very small size events per 1,000 facility singleton births\nunder SBA and caesarean section (crude vs overlap-weighted)"
  ) +
  theme_minimal(base_size = 12)

ggsave(
  filename = file.path(out_dir, "figure5_2_size_events_diff_per1000.png"),
  plot     = fig5B,
  width    = 8,
  height   = 4.5,
  dpi      = 300
)

message("Section 5 analysis (very small size at birth) completed successfully.")
