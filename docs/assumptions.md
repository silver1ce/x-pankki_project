# Assumptions

This is the challenge list for a working session with the client. Every item is also marked `# ASSUMPTION:` or `# SIMPLIFIED:` in code. None of these is a claim about how a production X-Pankki book actually works. If an assumption is wrong, change it here and in the code together.

## For business readers

A demo that hid these choices would look more “finished” and would be dangerous. PCAF, SFDR and taxonomy are real regimes; we shortened them so the pipeline is teachable in one sitting. Sign-off on a real migration means walking this list, not only looking at R01 totals.

## Demo and operating assumptions

1. **Reporting date and fiscal year.** Default as-of is 2025-12-31. ESG and financials are fiscal year 2025, taken as the calendar year of the as-of date.
2. **Loans have no `portfolio_id` in the SAS extract.** All surviving business loans are booked to portfolio P03 (Business Lending).
3. **FX table is the only SAS `DATE9.` extract.** Silver parses `31DEC2025`; every other table uses ISO dates.
4. **Restricted MSCI data** is stored under schema `esg_restricted`. Locally that is a folder; on Databricks it is a Unity Catalog schema with separate grants.
5. **Unmapped issuers are kept**, not dropped. Financed-emissions reports only attribute where entity resolution found a provider that exists in `msci_esg`. R09 explains the gaps.
6. **EVIC = 0** is kept in silver. Gold sets financed emissions to null and records `zero_or_missing_denominator`.
7. **PCAF / SFDR / taxonomy formulas are simplified.** Each shortcut is marked in the report catalog and in code.
8. **Frozen SAS baselines** are produced by running this pipeline once and then applying two mutations (R01 rounding, R09 legacy mapping for ISS007). A real migration would import SAS exports.
9. **Batch id** is `batch-<as-of-date>` so local reruns stay comparable. Production would use a unique run id.
10. **Instrument types** in the extract are `equity` and `corporate_bond`. Loan purposes are `general_corporate`, `capex`, `working_capital`.

## Methodology shortcuts (simplified on purpose)

- Holdings and loans are converted to **EUR first**; we do not convert EVIC into the holding currency.
- Scope 1+2+3 are **summed**; missing Scope 3 is zero plus a worse PCAF score.
- PCAF scores use a **four-rule** card, not the official 1a–5 option set.
- SFDR PAI 1 is one combined GHG total, not three scope lines.
- PAI 4 uses the **vendor fossil flag**, not a full NACE list.
- Taxonomy uses **one eligible share and one aligned share** per issuer, no turnover/CapEx split and no DNSH tests.

Owners who need the full rule should treat gold as a pattern, not as a disclosure engine.
