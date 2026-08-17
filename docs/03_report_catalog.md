# Report catalog

Each report answers one question a disclosure or risk committee would actually ask. Reports do not recalculate PCAF from scratch: they read gold tables and aggregate. Formulas below are the demo rules. Where the real PCAF or SFDR text is stricter, the section says **simplified**.

Default as-of date: **31 December 2025**. Output files: `data/output/R01.csv` … `R09.csv`. Parameters and recon tolerances: `conf/reports/R0N.yaml`.

---

## R01 — Financed emissions, listed equity

**Business question.** How many tonnes of CO2e do our listed-equity holdings finance?

**Who uses it.** PCAF financed-emissions disclosure for listed equity; input to SFDR PAI 1.

**Inputs.** `gold.financed_emissions` filtered to `asset_class = listed_equity`.

**Formula in plain language.** For each holding: take the euro market value, divide by the issuer’s EVIC (the PCAF “size of the company”), multiply by the issuer’s Scope 1+2+3 emissions. Then sum to portfolio × issuer.

Example in the demo: P01 holds €25m of ISS001; EVIC is €40bn; emissions are 3.7m tCO2e. Attribution is 25m / 40bn = 0.000625. Financed emissions = 0.000625 × 3.7m = **2,312.5 tCO2e**.

**Output grain.** `portfolio_id`, `issuer_id`.

**Simplified.** Real PCAF listed-equity uses EVIC including cash in the holding’s currency. We convert the holding to EUR and use EVIC already in EUR. Missing Scope 3 is treated as 0 and the PCAF score is worsened instead of failing the row.

---

## R02 — Financed emissions, corporate bonds

**Business question.** How many tonnes of CO2e do our corporate-bond holdings finance?

**Who uses it.** PCAF listed equity and corporate bonds standard (same attribution idea as equity).

**Inputs.** `gold.financed_emissions` filtered to `asset_class = corporate_bond`.

**Formula in plain language.** Same as R01: market value ÷ EVIC × issuer emissions, summed to portfolio × issuer.

**Output grain.** `portfolio_id`, `issuer_id`.

**Simplified.** Same EVIC and Scope 3 shortcuts as R01. Bond-specific PCAF options (outstanding vs market value in some cases) are not modelled; we always use market value.

---

## R03 — Financed emissions, business loans

**Business question.** How many tonnes of CO2e does the business-loan book finance?

**Who uses it.** PCAF business loans and unlisted equity.

**Inputs.** `gold.financed_emissions` filtered to `asset_class = business_loan`.

**Formula in plain language.** Outstanding in EUR ÷ borrower’s total assets × borrower emissions.

**Output grain.** `portfolio_id`, `issuer_id` (all demo loans sit in P03).

**Simplified.** Real PCAF has options based on known vs unknown use of proceeds. We treat every surviving loan as general corporate / capex / working capital using total assets as the denominator. Negative outstanding is rejected in silver, not estimated.

---

## R04 — PCAF data quality score

**Business question.** How much of this number is verified company data versus an estimate or a gap?

**Who uses it.** PCAF requires a weighted data-quality score (1 = best, 5 = worst) alongside financed emissions.

**Inputs.** `gold.pcaf_data_quality` (already weighted in gold).

**Formula in plain language.** Each position gets a score 1–5. The report is the exposure-weighted average by portfolio and asset class, plus the share of exposure that actually received emissions.

Demo scorecard (simplified vs official 1a/1b/2a/…/5):

| Score | When we assign it |
|---|---|
| 1 | Vendor source = verified, Scope 1, 2 and 3 all present |
| 2 | Source = reported, all three scopes present |
| 3 | Source = reported, Scope 1+2 present, Scope 3 missing |
| 4 | Source = estimated |
| 5 | Not mapped to MSCI, or nothing usable |

**Output grain.** `portfolio_id`, `asset_class`.

**Simplified.** Official PCAF options depend on physical activity data, energy consumption, and sector averages. We do not implement those options.

---

## R05 — WACI by portfolio

**Business question.** How carbon-intensive is each portfolio, per million euro of company revenue?

**Who uses it.** TCFD-style intensity; closely related to SFDR PAI 3.

**Inputs.** `gold.carbon_intensity`.

**Formula in plain language.** For each company: emissions ÷ (revenue in € millions) = intensity. Weight those intensities by our euro exposure. Carbon footprint is financed emissions ÷ (portfolio AUM in € millions).

**Output grain.** `portfolio_id`.

**Simplified.** We use Scope 1+2+3 in the numerator. Some TCFD packs publish Scope 1+2 WACI separately.

---

## R06 — SFDR PAI core (indicators 1–3)

**Business question.** For each legal entity, what are GHG emissions, carbon footprint, and GHG intensity?

**Who uses it.** SFDR RTS Annex I Table 1, PAI 1–3.

**Inputs.** `gold.financed_emissions` and `gold.carbon_intensity`, rolled to `legal_entity`.

**Formula in plain language.**

- **PAI 1** — sum of financed emissions (tCO2e).
- **PAI 2** — PAI 1 divided by AUM in € millions (carbon footprint).
- **PAI 3** — exposure-weighted WACI of that entity’s portfolios.

**Output grain.** `legal_entity`, `pai_indicator`.

**Simplified.** The RTS asks for Scope 1, 2 and 3 as separate PAI 1 lines. We publish one combined GHG total. Legal entity is taken from the portfolio master after trimming (so the trailing space on P01 does not split the entity).

---

## R07 — Fossil fuel exposure (PAI 4)

**Business question.** What share of each portfolio is in companies active in fossil fuels?

**Who uses it.** SFDR PAI 4.

**Inputs.** `gold.financed_emissions` and the vendor `fossil_fuel_flag`.

**Formula in plain language.** Sum euro exposure where the flag is 1, divide by portfolio AUM, show as a percentage. Also show how much AUM even has a flag (coverage).

**Output grain.** `portfolio_id`.

**Simplified.** Real PAI 4 uses a defined NACE list plus “companies active in the fossil fuel sector”. We trust the vendor flag. Unmapped issuers do not count as fossil; they reduce flag coverage instead.

---

## R08 — EU taxonomy alignment

**Business question.** Of the exposure that is taxonomy-eligible, how much is aligned — and what is that as a share of AUM?

**Who uses it.** Taxonomy Regulation / SFDR taxonomy templates.

**Inputs.** `gold.taxonomy_alignment`.

**Formula in plain language.** Weight each issuer’s eligible share and aligned share by our euro exposure. Aligned share of eligible = aligned euros ÷ eligible euros. Aligned share of AUM = aligned euros ÷ AUM.

**Output grain.** `portfolio_id`.

**Simplified.** Real templates split turnover, CapEx and OpEx and apply DNSH, safeguards and technical screening. We have one eligible share and one aligned share per issuer. ISS004’s row (aligned > eligible) is rejected in silver and therefore does not enter this report.

---

## R09 — Coverage and data-quality summary

**Business question.** Where are the ESG gaps, and how much AUM do they represent?

**Who uses it.** Internal control; the slide you show before anyone quotes a financed-emissions total.

**Inputs.** `gold.coverage`.

**Formula in plain language.** Every position is tagged with one gap cause, then counted and summed in euros.

| `gap_cause` | Meaning |
|---|---|
| `mapped` | We attributed emissions |
| `unmapped_issuer` | No row in the MSCI crosswalk |
| `orphan_provider` | Crosswalk points at an id that is not in MSCI |
| `zero_or_missing_denominator` | EVIC or total assets cannot be used (e.g. EVIC = 0) |
| `missing_esg_data` | Mapped but still no emissions (should be rare in this demo) |

**Output grain.** `gap_cause`.

**Simplified.** A production control pack would also split gaps by asset class, legal entity and vendor feed date. R09 is one table on purpose so recon is easy to read.

---

## How to change a report without rewriting code

Edit `conf/reports/R0N.yaml` (filters, grain, recon tolerances). That is the point of [adr/0003-config-driven-report-definitions.md](adr/0003-config-driven-report-definitions.md). New *maths* still belongs in gold, not in a one-off report copy-paste.
