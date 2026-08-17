# Start here

This folder explains the x-pankki ESG reporting demo in language a risk officer, product owner, or client sponsor can use in a meeting, with enough technical detail underneath for engineers and SAS developers. The bank currently produces nine ESG reports in SAS. This project shows how the same reports would be produced on Databricks, how we prove the numbers match, and which rules were simplified for the demo.

## Who should read which document

| If you are… | Read this first | Then |
|---|---|---|
| A business sponsor or risk / ESG lead | This page, then [03_report_catalog.md](03_report_catalog.md) | [06_reconciliation.md](06_reconciliation.md) |
| A SAS developer moving to Spark | [04_sas_to_pyspark_mapping.md](04_sas_to_pyspark_mapping.md) | Any report module under `src/xpankki_esg/reports/` |
| A data engineer | [01_architecture.md](01_architecture.md), [02_data_model.md](02_data_model.md) | [05_runbook.md](05_runbook.md) |
| Planning the Azure Databricks landing | [07_databricks_deployment.md](07_databricks_deployment.md) | The three ADRs in `adr/` |
| Reviewing demo shortcuts | [assumptions.md](assumptions.md) | Comments marked `SIMPLIFIED:` in the code |

## What problem this solves

ESG reports (financed emissions, SFDR principal adverse impacts, EU taxonomy) are still produced in many banks from a SAS data warehouse. The numbers matter for disclosure, so a migration is not “rewrite the code and hope”. The new platform must:

1. Ingest the same overnight extracts the SAS job uses today.
2. Apply the same business rules (attribution, FX, coverage).
3. Show every row that was rejected and why — never drop data silently.
4. Reconcile new output to frozen SAS output before cutover.

This repository is a **reference implementation** of that flow. It runs on a laptop with no Azure account. The same Python modules are what you would deploy to Databricks by changing configuration, not by rewriting the reports.

## The nine reports in one page

| ID | Business question | Regime it supports |
|---|---|---|
| R01 | How much GHG do our **listed equities** finance? | PCAF |
| R02 | How much GHG do our **corporate bonds** finance? | PCAF |
| R03 | How much GHG do our **business loans** finance? | PCAF |
| R04 | How reliable is the emissions data (score 1–5)? | PCAF data quality |
| R05 | How carbon-intensive is each portfolio (WACI)? | TCFD / SFDR PAI 3 |
| R06 | What are SFDR PAI 1–3 at legal-entity level? | SFDR RTS |
| R07 | What share of AUM is in fossil-fuel companies? | SFDR PAI 4 |
| R08 | How much of eligible AUM is EU-taxonomy aligned? | EU Taxonomy |
| R09 | Where are the data gaps, and how large are they? | Internal control |

Plain-language formulas and known simplifications are in [03_report_catalog.md](03_report_catalog.md).

## How a client walkthrough usually goes

1. **Show the flow** — SAS files land, bronze keeps them raw, silver cleans, gold calculates, reports slice gold. Diagram: [01_architecture.md](01_architecture.md).
2. **Show a messy source** — duplicate holdings, a negative loan, SAS `DATE9.` FX dates, an incomplete MSCI crosswalk. That is intentional; operations always looks like this.
3. **Open R01** — `data/output/R01.csv`. Walk ISS001: market value ÷ EVIC × issuer emissions.
4. **Open R09** — coverage by cause. Unmapped issuers are kept and explained, not deleted.
5. **Run reconciliation** — seven reports match the frozen SAS baseline; two fail on purpose (rounding, and one issuer the old SAS map still had). See [06_reconciliation.md](06_reconciliation.md).
6. **Say what changes on Databricks** — folders become Unity Catalog schemas; licensed MSCI data gets its own grants. See [07_databricks_deployment.md](07_databricks_deployment.md).

## Words used in this demo

| Term | Meaning |
|---|---|
| **AUM** | Assets under management — the euro value of the book we are reporting on. |
| **EVIC** | Enterprise value including cash. PCAF uses it as the “size of the company” when attributing listed equity and bonds. |
| **Financed emissions** | The slice of a company’s greenhouse gases that our holding or loan is deemed to finance. |
| **PCAF** | Partnership for Carbon Accounting Financials — the market standard for those calculations. |
| **SFDR PAI** | Principal Adverse Impact indicators under the EU Sustainable Finance Disclosure Regulation. |
| **WACI** | Weighted average carbon intensity — tCO2e per million euro of company revenue, weighted by our exposure. |
| **Medallion** | Bronze (raw) → silver (clean) → gold (calculations). A lakehouse pattern, not a product name. |
| **Entity resolution** | Matching our internal issuer id to the ESG vendor’s company id. This is where coverage is won or lost. |

## What this demo is not

It is not a production PCAF engine, not a licensed MSCI feed, and not legal advice. Where a real rule is simplified, the docs and the code both say so. Assumptions are listed in [assumptions.md](assumptions.md) so they can be challenged in one sitting.
