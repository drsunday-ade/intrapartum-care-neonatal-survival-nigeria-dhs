############################################################
# Results Section 6: Effect Modification, Equity Gradients,
# and Policy-Relevant Patterns
# Data: Nigeria DHS births recode 2018 (NGBR8AFL)
# Output folder: outputs_section6/
############################################################

## 0. Packages -----------------------------------------------------------

required_pkgs <- c(
  "tidyverse",   # dplyr, tidyr, ggplot2, readr, purrr, etc.
  "data.table",  # fast CSV read
  "haven",       # read_dta
  "janitor",     # clean_names
  "survey",      # complex survey design + svymean/svyglm
  "reticulate",  # optional Python bridge
  "jsonlite",    # meta json if present
  "scales",      # pretty labels
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

out_dir <- "outputs_section6"
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
    file.path(out_dir, "section6_rawcodes_preview_first200.csv")
  )
}

if (file.exists(codebook_csv)) {
  cb <- data.table::fread(codebook_csv)
  readr::write_csv(
    as_tibble(cb),
    file.path(out_dir, "section6_codebook_full.csv")
  )
}

if (file.exists(val_labels)) {
  vl <- data.table::fread(val_labels)
  readr::write_csv(
    as_tibble(vl),
    file.path(out_dir, "section6_value_labels_full.csv")
  )
}

if (file.exists(meta_json)) {
  meta_list <- jsonlite::fromJSON(meta_json, simplifyDataFrame = TRUE)
  sink(file.path(out_dir, "section6_meta_summary.txt"))
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

## 4. Load analytic cohort (prefer Section 1), else rebuild ---------------

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
      skilled_attendant   = as.integer(skilled_attendant),
      csection            = as.integer(csection),
      early_neonatal_death = as.integer(early_neonatal_death),
      very_small_size      = as.integer(very_small_size)
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
  
  # Ensure necessary variables exist
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
      weight       = v005 / 1e6,
      cluster      = v001,
      hh_id        = v002,
      woman_line   = v003,
      stratum      = coalesce(v022, v023),
      region       = v024,
      urban        = if_else(v025 == 1, 1L, 0L, missing = NA_integer_),
      
      maternal_age = v012,
      parity       = v201,
      educ         = v106,
      wealth_q     = v190,
      
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
      
      anc_visits        = m14,
      
      place_deliv       = m15,
      facility          = place_deliv >= 21 & place_deliv <= 39,
      csection          = (m17 == 1),
      
      skilled_attendant = (m3a == 1 | m3b == 1 | m3c == 1),
      
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
      skilled_attendant   = as.integer(skilled_attendant),
      csection            = as.integer(csection)
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
    file.path(out_dir, "section6_births_analytic_cc.csv")
  )
}

## 5. Recode key variables & build obstetric risk strata ------------------

births_cc <- births_cc %>%
  mutate(
    csection          = if_else(csection == 1L, 1L, 0L, missing = NA_integer_),
    skilled_attendant = if_else(skilled_attendant == 1L, 1L, 0L, missing = NA_integer_),
    early_neonatal_death = if_else(early_neonatal_death == 1L, 1L, 0L,
                                   missing = NA_integer_),
    very_small_size   = if_else(very_small_size == 1L, 1L, 0L,
                                missing = NA_integer_),
    
    parity_group = case_when(
      is.na(parity)        ~ NA_character_,
      parity <= 1          ~ "Primiparous (0–1)",
      parity >= 2          ~ "Multiparous (>=2)"
    ),
    parity_group = factor(
      parity_group,
      levels = c("Primiparous (0–1)", "Multiparous (>=2)")
    )
  )

## 5b. Previous C-section & BCG negative control (from raw, if available) -

dta_file1 <- "NGBR8AFL.dta"
dta_file2 <- "NGBR8AFL.DTA"
births_candidates <- c(dta_file1, dta_file2)
existing_dta <- births_candidates[file.exists(births_candidates)]

if (length(existing_dta) > 0) {
  births_file2 <- existing_dta[1]
  message("Reading raw births file for prev_csection and BCG negative control...")
  births_raw2 <- read_births_file(births_file2)
  
  # minimal prepped data for history
  births_small <- births_raw2 %>%
    clean_names() %>%
    haven::zap_labels()
  
  # ensure needed vars exist
  needed_hist <- c("caseid","bidx","m17","h2")
  for (nm in needed_hist) {
    if (!nm %in% names(births_small)) births_small[[nm]] <- NA_real_
  }
  
  births_small <- births_small %>%
    select(caseid, bidx, m17, h2)
  
  # Previous C-section (any earlier birth by C-section)
  prev_cs <- births_small %>%
    mutate(csection_birth = (m17 == 1)) %>%
    group_by(caseid) %>%
    summarise(
      prev_csection = as.integer(any(csection_birth & bidx > 1)),
      .groups = "drop"
    )
  
  births_cc <- births_cc %>%
    left_join(prev_cs, by = "caseid") %>%
    mutate(
      prev_cs_group = case_when(
        is.na(prev_csection)         ~ NA_character_,
        prev_csection == 1L         ~ "Previous C-section",
        prev_csection == 0L         ~ "No previous C-section"
      ),
      prev_cs_group = factor(
        prev_cs_group,
        levels = c("No previous C-section", "Previous C-section")
      )
    )
  
  # BCG at birth negative-control outcome (if h2 present)
  if (!all(is.na(births_small$h2))) {
    bcg_tbl <- births_small %>%
      filter(bidx == 1) %>%
      transmute(caseid, bcg_raw = h2)
    
    births_cc <- births_cc %>%
      left_join(bcg_tbl, by = "caseid") %>%
      mutate(
        bcg_birth = case_when(
          bcg_raw %in% c(1,2,3) ~ 1L,  # typical DHS coding: 1=card,2=recall,3=both
          bcg_raw == 0         ~ 0L,
          TRUE                 ~ NA_integer_
        )
      )
  }
  
} else {
  message("Stata births file not found; prev_csection and BCG negative-control not available.")
}

message("Section 6 analytic sample size (non-missing exposures/outcome): ",
        nrow(births_cc %>%
               filter(!is.na(skilled_attendant),
                      !is.na(csection),
                      !is.na(early_neonatal_death))))

## 6. Survey design helpers ----------------------------------------------

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
  if (nrow(dat_use) == 0) {
    return(tibble())
  }
  
  if (length(unique(na.omit(dat_use[[exposure]]))) < 2L) {
    return(tibble())  # no variation in exposure
  }
  
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
  
  if (nrow(risk0) == 0 || nrow(risk1) == 0) {
    return(tibble())
  }
  
  p0  <- risk0[[outcome]]
  se0 <- risk0[[se_col]]
  p1  <- risk1[[outcome]]
  se1 <- risk1[[se_col]]
  
  rd    <- p1 - p0
  se_rd <- sqrt(se0^2 + se1^2)
  ci_rd <- rd + c(-1, 1) * 1.96 * se_rd
  
  if (p0 > 0 && p1 > 0) {
    rr <- p1 / p0
    se_log_rr <- sqrt((se1^2 / p1^2) + (se0^2 / p0^2))
    ci_log_rr <- log(rr) + c(-1,1) * 1.96 * se_log_rr
    ci_rr <- exp(ci_log_rr)
  } else {
    rr    <- NA_real_
    ci_rr <- c(NA_real_, NA_real_)
  }
  
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
    deaths_diff_per_1000 = (p1 - p0) * 1000
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
  if (nrow(dat_use) == 0) return(tibble())
  
  if (length(unique(na.omit(dat_use[[exposure]]))) < 2L) {
    return(tibble())
  }
  
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
  
  if (nrow(risk0) == 0 || nrow(risk1) == 0) {
    return(tibble())
  }
  
  p0  <- risk0[[outcome]]
  se0 <- risk0[[se_col]]
  p1  <- risk1[[outcome]]
  se1 <- risk1[[se_col]]
  
  rd    <- p1 - p0
  se_rd <- sqrt(se0^2 + se1^2)
  ci_rd <- rd + c(-1, 1) * 1.96 * se_rd
  
  if (p0 > 0 && p1 > 0) {
    rr <- p1 / p0
    se_log_rr <- sqrt((se1^2 / p1^2) + (se0^2 / p0^2))
    ci_log_rr <- log(rr) + c(-1,1) * 1.96 * se_log_rr
    ci_rr <- exp(ci_log_rr)
  } else {
    rr    <- NA_real_
    ci_rr <- c(NA_real_, NA_real_)
  }
  
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
    deaths_diff_per_1000 = (p1 - p0) * 1000
  )
}

## 9. Covariates for PS models -------------------------------------------

ps_covars <- c(
  "maternal_age","parity","educ","wealth_q",
  "urban","region",
  "anc_visits","birth_order",
  "sex","birth_year","facility_sector"
)

## 10. Effect modification by obstetric risk (Table 7, Fig 6B) -----------

effect_by_stratum <- function(dat, exposure, outcome, covars, strat_var) {
  if (!strat_var %in% names(dat)) return(tibble())
  
  dat_use <- dat %>% filter(!is.na(.data[[strat_var]]))
  if (nrow(dat_use) == 0) return(tibble())
  
  splits <- split(dat_use, dat_use[[strat_var]])
  
  res_list <- lapply(names(splits), function(lev) {
    d <- splits[[lev]]
    
    crude  <- tryCatch(
      analyze_crude(d, exposure, outcome),
      error = function(e) tibble()
    )
    adj    <- tryCatch(
      analyze_overlap(d, exposure, outcome, covars),
      error = function(e) tibble()
    )
    
    out <- bind_rows(crude, adj)
    if (nrow(out) == 0) return(tibble())
    
    out %>%
      mutate(
        strat_var   = strat_var,
        strat_level = lev,
        n_unweighted = nrow(d),
        n_weighted   = sum(d$weight, na.rm = TRUE)
      )
  })
  
  bind_rows(res_list)
}

# C-section effect by parity and previous C-section
cs_parity_effect <- effect_by_stratum(
  births_cc,
  exposure = "csection",
  outcome  = "early_neonatal_death",
  covars   = ps_covars,
  strat_var = "parity_group"
)

if ("prev_cs_group" %in% names(births_cc)) {
  cs_prevcs_effect <- effect_by_stratum(
    births_cc,
    exposure = "csection",
    outcome  = "early_neonatal_death",
    covars   = ps_covars,
    strat_var = "prev_cs_group"
  )
} else {
  cs_prevcs_effect <- tibble()
}

# SBA effect by parity (for completeness)
sba_parity_effect <- effect_by_stratum(
  births_cc,
  exposure = "skilled_attendant",
  outcome  = "early_neonatal_death",
  covars   = ps_covars,
  strat_var = "parity_group"
)

effect_obstetric_all <- bind_rows(
  cs_parity_effect,
  cs_prevcs_effect,
  sba_parity_effect
)

readr::write_csv(
  effect_obstetric_all,
  file.path(out_dir, "section6_effect_by_obstetric_risk.csv")
)

## 11. Equity gradients: wealth, urban/rural, region, facility (Table 8) --

equity_summary_for <- function(dat, group_var) {
  if (!group_var %in% names(dat)) return(tibble())
  
  dat_use <- dat %>% filter(!is.na(.data[[group_var]]))
  if (nrow(dat_use) == 0) return(tibble())
  
  splits <- split(dat_use, dat_use[[group_var]])
  
  res_list <- lapply(names(splits), function(lev) {
    d <- splits[[lev]]
    
    if (nrow(d) == 0) return(tibble())
    
    des <- make_design(d, wt_var = "weight")
    
    means <- tryCatch(
      svymean(~ early_neonatal_death + skilled_attendant + csection,
              design = des, na.rm = TRUE),
      error = function(e) NULL
    )
    
    if (is.null(means)) {
      enn_risk <- NA_real_
      sba_cov  <- NA_real_
      cs_cov   <- NA_real_
    } else {
      enn_risk <- as.numeric(means["early_neonatal_death"])
      sba_cov  <- as.numeric(means["skilled_attendant"])
      cs_cov   <- as.numeric(means["csection"])
    }
    
    # Adjusted effects for SBA and C-section (overlap-weighted)
    # extract only adjusted row
    get_adj_row <- function(res_tbl) {
      if (is.null(res_tbl) || nrow(res_tbl) == 0) return(NULL)
      res_tbl %>%
        filter(grepl("Adjusted", adjustment)) %>%
        slice(1)
    }
    
    # SBA effect
    vals_sba <- na.omit(d$skilled_attendant)
    if (length(unique(vals_sba)) >= 2L) {
      res_sba <- tryCatch(
        analyze_overlap(d, "skilled_attendant", "early_neonatal_death", ps_covars),
        error = function(e) tibble()
      )
      res_sba_adj <- get_adj_row(res_sba)
    } else {
      res_sba_adj <- NULL
    }
    
    # C-section effect
    vals_cs <- na.omit(d$csection)
    if (length(unique(vals_cs)) >= 2L) {
      res_cs <- tryCatch(
        analyze_overlap(d, "csection", "early_neonatal_death", ps_covars),
        error = function(e) tibble()
      )
      res_cs_adj <- get_adj_row(res_cs)
    } else {
      res_cs_adj <- NULL
    }
    
    rd_sba <- if (!is.null(res_sba_adj)) res_sba_adj$rd else NA_real_
    rr_sba <- if (!is.null(res_sba_adj)) res_sba_adj$rr else NA_real_
    or_sba <- if (!is.null(res_sba_adj)) res_sba_adj$or else NA_real_
    
    rd_cs  <- if (!is.null(res_cs_adj)) res_cs_adj$rd else NA_real_
    rr_cs  <- if (!is.null(res_cs_adj)) res_cs_adj$rr else NA_real_
    or_cs  <- if (!is.null(res_cs_adj)) res_cs_adj$or else NA_real_
    
    tibble(
      stratifier     = group_var,
      level          = lev,
      n_unweighted   = nrow(d),
      n_weighted     = sum(d$weight, na.rm = TRUE),
      enn_risk       = enn_risk,
      sba_coverage   = sba_cov,
      cs_coverage    = cs_cov,
      rd_sba         = rd_sba,
      rr_sba         = rr_sba,
      or_sba         = or_sba,
      rd_cs          = rd_cs,
      rr_cs          = rr_cs,
      or_cs          = or_cs
    )
  })
  
  bind_rows(res_list)
}

equity_wealth   <- equity_summary_for(births_cc, "wealth_q")
equity_urban    <- equity_summary_for(births_cc, "urban")
equity_region   <- equity_summary_for(births_cc, "region")
equity_facility <- equity_summary_for(births_cc, "facility_sector")

equity_all <- bind_rows(
  equity_wealth,
  equity_urban,
  equity_region,
  equity_facility
)

readr::write_csv(
  equity_all,
  file.path(out_dir, "section6_equity_summary_allstrata.csv")
)

## 12. Joint care patterns (Table 9, Fig 6C) -----------------------------

births_care <- births_cc %>%
  mutate(
    care_pattern = case_when(
      skilled_attendant == 0L & csection == 0L ~ "Non-skilled vaginal",
      skilled_attendant == 1L & csection == 0L ~ "Skilled vaginal",
      skilled_attendant == 1L & csection == 1L ~ "Skilled C-section",
      TRUE ~ NA_character_
    )
  ) %>%
  filter(!is.na(care_pattern), !is.na(early_neonatal_death))

if (nrow(births_care) > 0) {
  des_care <- make_design(births_care, wt_var = "weight")
  
  risk_by_pattern <- svyby(
    ~ early_neonatal_death,
    ~ care_pattern,
    design  = des_care,
    FUN     = svymean,
    vartype = c("se","ci"),
    na.rm   = TRUE
  )
  
  care_counts <- births_care %>%
    group_by(care_pattern) %>%
    summarise(
      n_unweighted = n(),
      n_weighted   = sum(weight, na.rm = TRUE),
      .groups      = "drop"
    )
  
  care_tbl <- care_counts %>%
    left_join(
      as_tibble(risk_by_pattern),
      by = "care_pattern"
    ) %>%
    rename(
      risk_enn = early_neonatal_death
    ) %>%
    mutate(
      risk_enn_pct = 100 * risk_enn
    )
  
  readr::write_csv(
    care_tbl,
    file.path(out_dir, "section6_care_pattern_risk_table.csv")
  )
  
  fig6C <- care_tbl %>%
    ggplot(aes(
      x = reorder(care_pattern, risk_enn),
      y = risk_enn_pct
    )) +
    geom_col(width = 0.5) +
    labs(
      x    = NULL,
      y    = "Early neonatal death risk (%)",
      title = "Figure 6C. Gradient of early neonatal mortality\nby joint care pattern"
    ) +
    theme_minimal(base_size = 12)
  
  ggsave(
    filename = file.path(out_dir, "figure6_3_care_pattern_gradient.png"),
    plot     = fig6C,
    width    = 6,
    height   = 4,
    dpi      = 300
  )
}

## 13. Negative-control analysis: BCG at birth (optional) -----------------

if ("bcg_birth" %in% names(births_cc)) {
  nc_result <- tryCatch(
    analyze_overlap(
      dat      = births_cc,
      exposure = "csection",
      outcome  = "bcg_birth",
      covars   = ps_covars
    ),
    error = function(e) {
      message("Negative-control model (BCG~C-section) failed: ", e$message)
      NULL
    }
  )
  
  if (!is.null(nc_result) && nrow(nc_result) > 0) {
    readr::write_csv(
      nc_result,
      file.path(out_dir, "section6_negative_control_bcg_csection.csv")
    )
  }
} else {
  message("BCG negative-control outcome not available; skipping.")
}

## 14. Scenario modelling: avoidable deaths (simple example) --------------

# Design for full analytic cohort
des_all <- make_design(births_cc, wt_var = "weight")

# National SBA/C-section coverage & ENN risk
overall_means <- tryCatch(
  svymean(~ early_neonatal_death + skilled_attendant + csection,
          design = des_all, na.rm = TRUE),
  error = function(e) NULL
)

if (!is.null(overall_means)) {
  enn_risk_all   <- as.numeric(overall_means["early_neonatal_death"])
  sba_cov_all    <- as.numeric(overall_means["skilled_attendant"])
  cs_cov_all     <- as.numeric(overall_means["csection"])
} else {
  enn_risk_all <- sba_cov_all <- cs_cov_all <- NA_real_
}

# Poorest wealth quintile (assuming first level is poorest)
if ("wealth_q" %in% names(births_cc)) {
  poorest_level <- levels(as.factor(births_cc$wealth_q))[1]
  
  des_poor <- tryCatch(
    subset(des_all, wealth_q == poorest_level),
    error = function(e) NULL
  )
  
  if (!is.null(des_poor)) {
    means_poor <- tryCatch(
      svymean(~ early_neonatal_death + skilled_attendant + csection,
              design = des_poor, na.rm = TRUE),
      error = function(e) NULL
    )
    
    if (!is.null(means_poor)) {
      enn_risk_poor <- as.numeric(means_poor["early_neonatal_death"])
      sba_cov_poor  <- as.numeric(means_poor["skilled_attendant"])
      cs_cov_poor   <- as.numeric(means_poor["csection"])
    } else {
      enn_risk_poor <- sba_cov_poor <- cs_cov_poor <- NA_real_
    }
    
    births_poor_5y   <- sum(births_cc$weight[births_cc$wealth_q == poorest_level],
                            na.rm = TRUE)
    births_poor_year <- births_poor_5y / 5
    
    # Need adjusted RD for SBA and C-section vs ENN (national)
    sba_enn_adj <- analyze_overlap(
      dat      = births_cc,
      exposure = "skilled_attendant",
      outcome  = "early_neonatal_death",
      covars   = ps_covars
    ) %>%
      filter(grepl("Adjusted", adjustment)) %>%
      slice(1)
    
    cs_enn_adj <- analyze_overlap(
      dat      = births_cc,
      exposure = "csection",
      outcome  = "early_neonatal_death",
      covars   = ps_covars
    ) %>%
      filter(grepl("Adjusted", adjustment)) %>%
      slice(1)
    
    rd_sba_adj <- if (nrow(sba_enn_adj) > 0) sba_enn_adj$rd else NA_real_
    rd_cs_adj  <- if (nrow(cs_enn_adj) > 0) cs_enn_adj$rd  else NA_real_
    
    # Scenario: raise SBA coverage in poorest quintile to national SBA coverage
    delta_cov_sba <- sba_cov_all - sba_cov_poor
    risk_change_sba <- delta_cov_sba * rd_sba_adj
    deaths_averted_sba_year <- -1 * risk_change_sba * births_poor_year
    
    # Scenario: raise C-section coverage in poorest quintile to national C-section coverage
    delta_cov_cs <- cs_cov_all - cs_cov_poor
    risk_change_cs <- delta_cov_cs * rd_cs_adj
    deaths_averted_cs_year <- -1 * risk_change_cs * births_poor_year
    
    scenario_tbl <- tibble(
      scenario = c(
        "Raise SBA coverage in poorest quintile to national SBA coverage",
        "Raise C-section coverage in poorest quintile to national C-section coverage"
      ),
      poorest_level      = poorest_level,
      births_poor_year   = births_poor_year,
      sba_cov_poor       = sba_cov_poor,
      sba_cov_all        = sba_cov_all,
      cs_cov_poor        = cs_cov_poor,
      cs_cov_all         = cs_cov_all,
      rd_sba_adj         = rd_sba_adj,
      rd_cs_adj          = rd_cs_adj,
      deaths_averted_per_year = c(
        deaths_averted_sba_year,
        deaths_averted_cs_year
      )
    )
    
    readr::write_csv(
      scenario_tbl,
      file.path(out_dir, "section6_scenario_modelling_avoidable_deaths.csv")
    )
  }
}

## 15. Key figures for Section 6: wealth/effect plots ---------------------

# 15.1 Wealth gradient in RD (C-section & SBA)
if (nrow(equity_wealth) > 0) {
  equity_wealth_long <- equity_wealth %>%
    mutate(level = as.factor(level)) %>%
    select(level, rd_sba, rd_cs) %>%
    pivot_longer(
      cols      = c(rd_sba, rd_cs),
      names_to  = "exposure",
      values_to = "rd"
    ) %>%
    mutate(
      exposure = recode(
        exposure,
        "rd_sba" = "Skilled birth attendance",
        "rd_cs"  = "Caesarean section"
      ),
      rd_per_1000 = 1000 * rd
    ) %>%
    # drop strata where the RD could not be estimated
    filter(!is.na(rd_per_1000))
  
  if (nrow(equity_wealth_long) > 0) {
    fig6A <- equity_wealth_long %>%
      ggplot(aes(
        x = level,
        y = rd_per_1000,
        group = exposure,
        color = exposure
      )) +
      geom_hline(yintercept = 0, linetype = "dashed") +
      geom_line(na.rm = TRUE) +
      geom_point(na.rm = TRUE) +
      labs(
        x     = "Wealth quintile",
        y     = "Adjusted risk difference in early neonatal death\n(per 1,000 facility singleton births)",
        color = "Exposure",
        title = "Figure 6A. Wealth gradient in adjusted risk differences\nfor SBA and caesarean section"
      ) +
      theme_minimal(base_size = 12)
    
    ggsave(
      filename = file.path(out_dir, "figure6_1_wealth_rd_by_exposure.png"),
      plot     = fig6A,
      width    = 7,
      height   = 4.5,
      dpi      = 300
    )
  }
}

# 15.2 Obstetric risk interaction plots for C-section (parity/prev CS)

# Safely extract only adjusted rows if they exist
safe_adjusted_filter <- function(tbl) {
  if (is.null(tbl) || nrow(tbl) == 0) return(tibble())
  if (!("adjustment" %in% names(tbl))) return(tibble())
  tbl %>%
    filter(grepl("Adjusted", adjustment)) %>%
    slice(1:n())  # keep all adjusted rows, if any
}

cs_parity_adj <- cs_parity_effect %>%
  safe_adjusted_filter() %>%
  mutate(stratifier = "Parity")

cs_prevcs_adj <- cs_prevcs_effect %>%
  safe_adjusted_filter() %>%
  mutate(stratifier = "Previous C-section")

cs_obrisk <- bind_rows(cs_parity_adj, cs_prevcs_adj)

if (nrow(cs_obrisk) > 0) {
  cs_obrisk <- cs_obrisk %>%
    mutate(
      rd_per_1000 = 1000 * rd,
      strat_level = factor(strat_level)
    ) %>%
    filter(!is.na(rd_per_1000))
  
  if (nrow(cs_obrisk) > 0) {
    fig6B <- cs_obrisk %>%
      ggplot(aes(x = strat_level, y = rd_per_1000)) +
      geom_hline(yintercept = 0, linetype = "dashed") +
      geom_col(width = 0.5) +
      facet_wrap(~ stratifier, scales = "free_x") +
      labs(
        x    = NULL,
        y    = "Adjusted risk difference in early neonatal death\n(per 1,000 births)",
        title = "Figure 6B. Effect of caesarean vs vaginal delivery\nby obstetric risk strata"
      ) +
      theme_minimal(base_size = 12)
    
    ggsave(
      filename = file.path(out_dir, "figure6_2_obstetric_risk_csection_rd.png"),
      plot     = fig6B,
      width    = 8,
      height   = 4.5,
      dpi      = 300
    )
  }
}

message("Section 6 analysis (effect modification & equity gradients) completed successfully.")

## 16. Manuscript-ready key outputs for Section 6 -------------------------
##    (Main equity table + main equity figure)

## 16.1 Main table: Equity by wealth quintile (Table 8 core)
##      - Early neonatal risk (per 1,000)
##      - SBA & C-section coverage (%)
##      - Adjusted RD for SBA & C-section (per 1,000)

if (exists("equity_wealth") && nrow(equity_wealth) > 0) {
  table6_main <- equity_wealth %>%
    mutate(
      wealth_quintile    = as.factor(level),
      enn_risk_per_1000  = enn_risk * 1000,
      sba_coverage_pct   = sba_coverage * 100,
      cs_coverage_pct    = cs_coverage * 100,
      rd_sba_per_1000    = rd_sba * 1000,
      rd_cs_per_1000     = rd_cs * 1000
    ) %>%
    select(
      wealth_quintile,
      n_unweighted,
      n_weighted,
      enn_risk_per_1000,
      sba_coverage_pct,
      cs_coverage_pct,
      rd_sba_per_1000,
      rd_cs_per_1000
    ) %>%
    arrange(wealth_quintile)
  
  readr::write_csv(
    table6_main,
    file.path(out_dir, "section6_table_equity_wealth_main.csv")
  )
}

## 16.2 Main figure: Wealth gradient in adjusted risk differences
##      (SBA and C-section, per 1,000 births – Figure 6A core)

if (exists("equity_wealth") && nrow(equity_wealth) > 0) {
  equity_wealth_long <- equity_wealth %>%
    mutate(wealth_quintile = as.factor(level)) %>%
    select(wealth_quintile, rd_sba, rd_cs) %>%
    pivot_longer(
      cols      = c(rd_sba, rd_cs),
      names_to  = "exposure",
      values_to = "rd"
    ) %>%
    mutate(
      exposure = recode(
        exposure,
        "rd_sba" = "Skilled birth attendance",
        "rd_cs"  = "Caesarean section"
      ),
      rd_per_1000 = 1000 * rd
    ) %>%
    filter(!is.na(rd_per_1000))
  
  if (nrow(equity_wealth_long) > 0) {
    fig6_main <- equity_wealth_long %>%
      ggplot(aes(
        x     = wealth_quintile,
        y     = rd_per_1000,
        group = exposure,
        color = exposure
      )) +
      geom_hline(yintercept = 0, linetype = "dashed") +
      geom_line(na.rm = TRUE) +
      geom_point(na.rm = TRUE) +
      labs(
        x     = "Wealth quintile",
        y     = "Adjusted risk difference in early neonatal death\n(per 1,000 facility singleton births)",
        color = "Exposure",
        title = "Figure 6A. Wealth gradient in adjusted effects\nfor SBA and caesarean section"
      ) +
      theme_minimal(base_size = 12)
    
    ggsave(
      filename = file.path(out_dir, "figure6_main_wealth_rd_by_exposure.png"),
      plot     = fig6_main,
      width    = 7,
      height   = 4.5,
      dpi      = 300
    )
  }
}

## 16.3 (Optional but powerful) Joint care-pattern figure (already created)
## If you previously created `care_tbl` above, this will NOT rerun anything.
## It just ensures the key care-gradient figure exists in the output folder.

if (exists("care_tbl") && nrow(care_tbl) > 0) {
  fig6_care <- care_tbl %>%
    ggplot(aes(
      x = reorder(care_pattern, risk_enn),
      y = risk_enn * 100
    )) +
    geom_col(width = 0.5) +
    labs(
      x    = NULL,
      y    = "Early neonatal death risk (%)",
      title = "Figure 6C. Gradient of early neonatal mortality\nby joint care pattern"
    ) +
    theme_minimal(base_size = 12)
  
  ggsave(
    filename = file.path(out_dir, "figure6_main_care_pattern_gradient.png"),
    plot     = fig6_care,
    width    = 6,
    height   = 4,
    dpi      = 300
  )
}

message("Section 6 key manuscript outputs (main table + figures) saved in: ", out_dir)
