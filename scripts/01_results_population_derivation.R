############################################################
# Results Section 1: Study Population & Outcome Frequencies
# Data: Nigeria DHS births recode 2018 (NGBR8AFL)
# Output folder: outputs_section1/
############################################################

## 0. Packages -----------------------------------------------------------

required_pkgs <- c(
  "tidyverse",   # dplyr, tidyr, ggplot2, readr, etc.
  "data.table",  # fast reading of CSVs if present
  "haven",       # read_dta
  "janitor",     # clean_names
  "survey",      # complex survey design + svymean
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
options(survey.lonely.psu = "adjust")  # handle lonely PSUs robustly

## 1. Output directory ---------------------------------------------------

out_dir <- "outputs_section1"
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
    file.path(out_dir, "section1_rawcodes_preview_first200.csv")
  )
}

if (file.exists(codebook_csv)) {
  cb <- data.table::fread(codebook_csv)
  readr::write_csv(
    as_tibble(cb),
    file.path(out_dir, "section1_codebook_full.csv")
  )
}

if (file.exists(val_labels)) {
  vl <- data.table::fread(val_labels)
  readr::write_csv(
    as_tibble(vl),
    file.path(out_dir, "section1_value_labels_full.csv")
  )
}

if (file.exists(meta_json)) {
  meta_list <- jsonlite::fromJSON(meta_json, simplifyDataFrame = TRUE)
  sink(file.path(out_dir, "section1_meta_summary.txt"))
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

# Core variables we absolutely need for Section 1
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
    # births in last 5 years (or assume TRUE if age missing)
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

## 7. Define sample at each step -----------------------------------------

births_total <- births_prepped

births_last5y <- births_prepped %>%
  filter(last5y_birth)

births_facility <- births_last5y %>%
  filter(facility)

births_facility_singleton <- births_facility %>%
  filter(multiple == 0L)

births_facility_singleton_recent <- births_facility_singleton %>%
  filter(bidx == 1)

# Define analytic complete-case cohort (same structure we'll use later)
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

births_analytic <- births_facility_singleton_recent %>%
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

# Variables required for complete-case analysis
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

message("Section 1 analytic complete-case sample size: ", nrow(births_cc))

# Save analytic cohort for cross-checks / later sections
readr::write_csv(
  births_cc,
  file.path(out_dir, "section1_births_analytic_cc.csv")
)

## 8. Flow table: unweighted & weighted N at each step -------------------

summarise_flow <- function(data, label) {
  tibble(
    step_label   = label,
    n_unweighted = nrow(data),
    n_weighted   = sum(data$weight, na.rm = TRUE)
  )
}

flow_counts <- bind_rows(
  summarise_flow(births_total,
                 "All births in births recode"),
  summarise_flow(births_last5y,
                 "Births in last 5 years (child age <= 59 months)"),
  summarise_flow(births_facility,
                 "Facility births (delivery in health facility)"),
  summarise_flow(births_facility_singleton,
                 "Facility singleton births"),
  summarise_flow(births_facility_singleton_recent,
                 "Most recent facility singleton births per woman (bidx = 1)"),
  summarise_flow(births_cc,
                 "Analytic cohort (complete data on exposures, outcomes, covariates)")
) %>%
  mutate(
    step_id          = row_number(),
    n_weighted_round = round(n_weighted)
  )

readr::write_csv(
  flow_counts,
  file.path(out_dir, "section1_flow_counts.csv")
)

## 9. Early neonatal death & very small size (weighted) ------------------

# Survey design helper
make_design <- function(dat, wt_var = "weight") {
  svydesign(
    ids     = ~cluster,
    strata  = ~stratum,
    weights = as.formula(paste0("~", wt_var)),
    data    = dat,
    nest    = TRUE
  )
}

des_analytic <- make_design(births_cc, wt_var = "weight")

out_means <- svymean(
  ~ early_neonatal_death + very_small_size,
  design = des_analytic,
  na.rm  = TRUE
)

out_se <- SE(out_means)
out_ci <- confint(out_means)

outcomes_tbl <- tibble(
  indicator = c("Early neonatal death (0–7 days)", "Very small size at birth"),
  varname   = c("early_neonatal_death", "very_small_size"),
  prevalence      = as.numeric(out_means),
  se              = as.numeric(out_se),
  ci_l            = out_ci[, 1],
  ci_u            = out_ci[, 2]
) %>%
  mutate(
    prevalence_pct = 100 * prevalence,
    ci_l_pct       = 100 * ci_l,
    ci_u_pct       = 100 * ci_u
  )

readr::write_csv(
  outcomes_tbl,
  file.path(out_dir, "section1_outcome_prevalence.csv")
)

## 10. Baseline characteristics (weighted) --------------------------------

analytic_with_w <- births_cc

# Numeric variables
num_vars <- c("maternal_age", "parity", "anc_visits", "birth_order")

baseline_numeric <-
  analytic_with_w %>%
  pivot_longer(cols = all_of(num_vars),
               names_to = "variable",
               values_to = "value") %>%
  group_by(variable) %>%
  summarise(
    n_unweighted = sum(!is.na(value)),
    n_weighted   = sum(weight[!is.na(value)], na.rm = TRUE),
    mean_w       = wtd_mean(value, weight),
    sd_w         = sqrt(wtd_var(value, weight)),
    .groups      = "drop"
  )

readr::write_csv(
  baseline_numeric,
  file.path(out_dir, "section1_baseline_numeric.csv")
)

# Categorical variables
cat_vars <- c(
  "region", "urban", "educ", "wealth_q",
  "facility_sector", "sex", "skilled_attendant", "csection"
)

# *** KEY FIX ***
# Make all categorical vars character so pivot_longer can combine them safely
analytic_cat <-
  analytic_with_w %>%
  mutate(across(all_of(cat_vars), ~ as.character(.)))

baseline_categorical <-
  analytic_cat %>%
  select(weight, all_of(cat_vars)) %>%
  pivot_longer(cols = all_of(cat_vars),
               names_to = "variable",
               values_to = "level") %>%
  filter(!is.na(level)) %>%
  group_by(variable, level) %>%
  summarise(
    n_unweighted = n(),
    w            = sum(weight, na.rm = TRUE),
    .groups      = "drop_last"
  ) %>%
  group_by(variable) %>%
  mutate(
    total_w = sum(w, na.rm = TRUE),
    prop    = w / total_w
  ) %>%
  ungroup() %>%
  mutate(
    prop_pct = 100 * prop
  )

readr::write_csv(
  baseline_categorical,
  file.path(out_dir, "section1_baseline_categorical.csv")
)

## 11. Included vs excluded (selection bias check) ------------------------

eligible_base <- births_facility_singleton_recent %>%
  mutate(
    included_analysis = if_else(
      !is.na(weight) &
        !is.na(cluster) &
        !is.na(stratum) &
        !is.na(region) &
        !is.na(urban) &
        !is.na(maternal_age) &
        !is.na(parity) &
        !is.na(educ) &
        !is.na(wealth_q) &
        !is.na(birth_order) &
        !is.na(sex) &
        !is.na(birth_year) &
        !is.na(anc_visits) &
        !is.na(facility_sector) &
        !is.na(skilled_attendant) &
        !is.na(csection) &
        !is.na(early_neonatal_death) &
        !is.na(very_small_size),
      1L, 0L
    )
  )

included_flagged <- eligible_base %>%
  mutate(
    group = if_else(included_analysis == 1L, "Included", "Excluded")
  )

# Numeric comparison
num_comp_vars <- c("maternal_age", "parity", "anc_visits")

included_num_comp <-
  included_flagged %>%
  filter(!is.na(group)) %>%
  pivot_longer(cols = all_of(num_comp_vars),
               names_to = "variable",
               values_to = "value") %>%
  filter(!is.na(value)) %>%
  group_by(group, variable) %>%
  summarise(
    n_unweighted = n(),
    n_weighted   = sum(weight, na.rm = TRUE),
    mean_w       = wtd_mean(value, weight),
    sd_w         = sqrt(wtd_var(value, weight)),
    .groups      = "drop"
  )

readr::write_csv(
  included_num_comp,
  file.path(out_dir, "section1_included_excluded_numeric.csv")
)

# Categorical comparison
cat_comp_vars <- c("educ", "wealth_q", "urban", "region")

included_cat_comp <-
  included_flagged %>%
  filter(!is.na(group)) %>%
  select(group, weight, all_of(cat_comp_vars)) %>%
  pivot_longer(cols = all_of(cat_comp_vars),
               names_to = "variable",
               values_to = "level") %>%
  filter(!is.na(level)) %>%
  group_by(group, variable, level) %>%
  summarise(
    w = sum(weight, na.rm = TRUE),
    .groups = "drop_last"
  ) %>%
  group_by(group, variable) %>%
  mutate(
    total_w = sum(w, na.rm = TRUE),
    prop    = w / total_w
  ) %>%
  ungroup() %>%
  mutate(prop_pct = 100 * prop)

readr::write_csv(
  included_cat_comp,
  file.path(out_dir, "section1_included_excluded_categorical.csv")
)

## 12. Missingness summary at facility singleton recent stage -------------

missing_summary <-
  births_facility_singleton_recent %>%
  select(any_of(key_vars_for_cc)) %>%
  summarise(across(everything(), ~ sum(is.na(.)))) %>%
  pivot_longer(cols = everything(),
               names_to = "variable",
               values_to = "n_missing") %>%
  mutate(
    n_total      = nrow(births_facility_singleton_recent),
    pct_missing  = 100 * n_missing / n_total
  )

readr::write_csv(
  missing_summary,
  file.path(out_dir, "section1_missing_summary.csv")
)

## 13. Simple PRISMA-style flow figure -----------------------------------

flow_plot <-
  flow_counts %>%
  ggplot(aes(x = reorder(step_label, step_id),
             y = n_weighted / 1000)) +
  geom_col() +
  coord_flip() +
  labs(
    x     = NULL,
    y     = "Weighted number of births (thousands)",
    title = "Sample derivation for Nigeria DHS facility singleton analytic cohort"
  ) +
  theme_minimal(base_size = 12)

ggsave(
  filename = file.path(out_dir, "figure_section1_flow_bar.png"),
  plot     = flow_plot,
  width    = 8,
  height   = 5,
  dpi      = 300
)

message("Section 1 script completed successfully. All outputs saved in 'outputs_section1/'.")


############################################################
## 14. Key Table 1 and Outcomes Figure for Section 1
############################################################

# Install/load gt for publication-quality table
if (!"gt" %in% rownames(installed.packages())) {
  install.packages("gt")
}
library(gt)

# Human-readable labels for variables
var_labels <- c(
  maternal_age    = "Maternal age (years)",
  parity          = "Parity",
  anc_visits      = "Number of ANC visits",
  birth_order     = "Birth order",
  region          = "Region",
  urban           = "Place of residence",
  educ            = "Maternal education",
  wealth_q        = "Wealth quintile",
  facility_sector = "Facility sector",
  sex             = "Infant sex",
  skilled_attendant = "Skilled birth attendant",
  csection        = "Caesarean delivery"
)

## 14.1 Format baseline numeric characteristics ---------------------------

table1_num <-
  baseline_numeric %>%
  mutate(
    Characteristic   = dplyr::recode(variable, !!!var_labels),
    `N (unweighted)` = n_unweighted,
    `Weighted summary` = sprintf("Mean %.1f (SD %.1f)", mean_w, sd_w),
    Block            = "Baseline characteristics"
  ) %>%
  select(Block, Characteristic, `N (unweighted)`, `Weighted summary`)

## 14.2 Format baseline categorical characteristics -----------------------

table1_cat <-
  baseline_categorical %>%
  mutate(
    var_label      = dplyr::recode(variable, !!!var_labels),
    Characteristic = paste0(var_label, " = ", level),
    `N (unweighted)` = n_unweighted,
    `Weighted summary` = sprintf("%.1f%%", prop_pct),
    Block            = "Baseline characteristics"
  ) %>%
  select(Block, Characteristic, `N (unweighted)`, `Weighted summary`)

## 14.3 Format outcomes (ENND and very small size) ------------------------

table1_outcomes <-
  outcomes_tbl %>%
  mutate(
    Characteristic   = indicator,
    `N (unweighted)` = NA_integer_,
    `Weighted summary` = sprintf("%.2f%% (%.2f–%.2f)",
                                 prevalence_pct, ci_l_pct, ci_u_pct),
    Block            = "Outcomes"
  ) %>%
  select(Block, Characteristic, `N (unweighted)`, `Weighted summary`)

## 14.4 Combine everything into Table 1 -----------------------------------

table1_all <-
  bind_rows(
    table1_num,
    table1_cat,
    table1_outcomes
  ) %>%
  arrange(Block, Characteristic)

gt_table1 <-
  table1_all %>%
  gt(groupname_col = "Block") %>%
  tab_header(
    title = "Table 1. Weighted baseline characteristics and outcomes in the analytic cohort"
  ) %>%
  cols_label(
    `N (unweighted)` = "N (unweighted)",
    `Weighted summary` = "Weighted mean / % (95% CI for outcomes)"
  )

gtsave(
  gt_table1,
  filename = file.path(out_dir, "section1_table1_baseline_outcomes.html")
)

############################################################
## 14.5 Key outcomes figure: ENND and very small size
############################################################

outcome_plot <-
  outcomes_tbl %>%
  mutate(
    indicator = factor(indicator, levels = indicator)
  ) %>%
  ggplot(aes(x = indicator, y = prevalence_pct)) +
  geom_col(width = 0.6) +
  geom_errorbar(aes(ymin = ci_l_pct, ymax = ci_u_pct), width = 0.2) +
  labs(
    x     = NULL,
    y     = "Prevalence (%)",
    title = "Prevalence of early neonatal death and very small size at birth"
  ) +
  theme_minimal(base_size = 12)

ggsave(
  filename = file.path(out_dir, "figure_section1_outcome_prevalence.png"),
  plot     = outcome_plot,
  width    = 6,
  height   = 4,
  dpi      = 300
)

message("Section 1: Table 1 and key outcomes figure saved in '", out_dir, "'.")
