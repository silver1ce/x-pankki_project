# Assumptions

This file lists every `# ASSUMPTION:` made in the demo so they can be reviewed in one place. None of these is a claim about how X-Pankki's production books actually work.

1. **Reporting date and fiscal year.** The default as-of date is 2025-12-31. ESG and financials are for fiscal year 2025, taken as the calendar year of the as-of date.
2. **Loans have no portfolio_id in the SAS extract.** All surviving business loans are booked to portfolio P03 (Business Lending), the only book whose `asset_class_scope` is `business_loans`.
3. **FX table is the only SAS DATE9. extract.** Silver parses `31DEC2025`; every other table uses ISO dates.
4. **Restricted MSCI data** is stored under schema `esg_restricted`. Locally that is a folder; on Databricks it is a Unity Catalog schema with separate grants.
5. **Unmapped issuers are kept**, not dropped. Financed-emissions reports only include rows where entity resolution found a provider that exists in `msci_esg`. Coverage report R09 explains the gaps.
6. **EVIC = 0** is kept in silver. Gold sets financed emissions to null for that issuer and records `zero_or_missing_denominator`.
7. **PCAF / SFDR formulas are simplified.** Each simplification is marked `# SIMPLIFIED:` next to the formula.
8. **Frozen SAS baselines** are produced by running this pipeline once and then applying two documented mutations (R01 rounding, R09 legacy mapping for ISS007). A real migration would import SAS exports instead.
9. **Batch id** is `batch-<as-of-date>` so local reruns stay comparable. Production would use a unique run id.
10. **Instrument types** in the extract are `equity` and `corporate_bond`. Loan purposes are `general_corporate`, `capex`, `working_capital`.
