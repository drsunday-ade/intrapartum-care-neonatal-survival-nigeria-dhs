# intrapartum-care-neonatal-survival-nigeria-dhs
Code and reproducible analyses for a manuscript on intrapartum care and neonatal survival in Nigeria using the 2018 DHS. Includes data processing scripts, causal and risk models, figures, and tables for submission-ready results.
# Intrapartum Care and Neonatal Survival in Nigeria (DHS 2018)

This repository contains the reproducible code and output files for the analysis of intrapartum care and neonatal survival in Nigeria using the **2018 Nigeria Demographic and Health Survey (DHS) Births Recode**.

The project quantifies coverage and equity of intrapartum care and estimates risk differences and deaths averted associated with skilled birth attendance and cesarean section among facility births.

---

## Manuscript

**Working title**

> *Intrapartum care and neonatal survival in Nigeria: Evidence from the 2018 Demographic and Health Survey*

**Lead author:** Sunday A. Adetunji, MD  
**Institution:** Department of Epidemiology, College of Health, Oregon State University, USA  

(Please update with the final manuscript title, journal, and DOI when available.)

---

## Repository structure

```text
.
├── data/
│   ├── DATA_AVAILABILITY.txt        # Describes DHS 2018 Nigeria data access and restrictions
│   └── NGBR8AFL_*.{csv,dta,json}    # Local copies of DHS-derived files (NOT tracked on GitHub)
│
├── outputs(figures/tables)/
│   ├── outputs_section1/            # Sample flow, inclusion/exclusion, baseline characteristics
│   ├── outputs_section2/            # Coverage & equity of intrapartum care indicators
│   ├── outputs_section3/            # Main risk-difference models & deaths averted (SBA, CS)
│   ├── outputs_section4/            # Cesarean section effect estimates & additional scenarios
│   ├── outputs_section5/            # Size- and population-based scenario analyses
│   └── outputs_section6/            # Cascades, equity gradients, final risk tables
│
└── scripts/
    ├── 01_results_p..._derivation.R # Data prep, cohort definition, Section 1 outputs
    ├── 02_results_s...rtum_care.R   # Intrapartum care coverage & equity, Section 2 outputs
    ├── section3_results.R           # Outcome models and Section 3 results
    ├── results_section4_size.R      # Additional size / scaling analyses for Section 4
    ├── script_for_Section_5.R       # Scenario analyses for Section 5
    └── results_secti...od_equity.R  # Equity-focused summaries and Section 6 outputs
