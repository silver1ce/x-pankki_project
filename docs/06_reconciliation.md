# Reconciliation

Cutover is not a go-live party; it is a controlled comparison of two machines that should tell the same economic story. This demo freezes a “legacy SAS” copy of each report, then compares the new pipeline to it with the tolerance in that report’s YAML. Seven reports match. Two fail on purpose so a steering group can practise the investigation instead of discovering it in production.

## For business readers

After `make recon` you get a table with PASS/FAIL per report. **PASS** means every key (for example portfolio + issuer) exists on both sides and the value columns differ by less than the agreed tolerance. **FAIL** prints the breaking keys. In this demo:

- **R01 FAIL** is a **rounding convention**, not missing trades.
- **R09 FAIL** is a **mapping convention**: the old SAS crosswalk still had ISS007; the new one does not.

Neither is a Spark bug. Both need an explicit decision: change SAS, change Databricks, or document a permanent exemption.

## How to run and read the output

```bash
make recon
```

You should see a table like:

```text
report status   rows_new  rows_base  breaks  note
R01    FAIL          13         13      n    seeded: SAS rounded financed emissions to nearest 10 tCO2e
R02    PASS           7          7      0    match
…
R09    FAIL           4          4      n    seeded: SAS still mapped ISS007
```

Then a short list of breaking rows, for example:

```text
P01 ISS001 | financed_emissions_tco2e new=2312.5 sas=2310.0
```

Files compared:

| Side | Path |
|---|---|
| New pipeline | `data/output/R0N.csv` |
| Frozen SAS stand-in | `data/baselines/R0N.csv` |
| Tolerances | `conf/reports/R0N.yaml` → `reconciliation:` |

A row **passes** if the absolute difference is ≤ `max(absolute_tolerance, relative_tolerance × max(|SAS|, 1))`. Example: R01 allows 0.5 tCO2e or 0.1% of the SAS number, whichever is larger. Rounding to the nearest 10 tCO2e is larger than that, so R01 fails.

`make recon` returns success in the demo even with the two seeded fails (so a training run is not “red”). A production job should fail the build on any unexpected report.

## How baselines are created in this demo

**ASSUMPTION:** we do not have a real SAS export. `make generate` runs the new pipeline, copies `data/output/*.csv` into `data/baselines/`, then applies two mutations. A real migration would import the SAS `PROC EXPORT` files instead and never mutate them.

Code: `apply_seeded_baseline_diffs()` in `src/xpankki_esg/generate_data.py`.

---

## Seeded difference 1 — R01 rounding

### What you see

R01 keys match (same portfolios and issuers). `financed_emissions_tco2e` differs by a few tonnes, typically up to 5 given nearest-10 rounding (e.g. 2312.5 vs 2310.0).

### What it represents in a real bank

SAS `PROC MEANS` (or a `ROUND` in a DATA step) often stores disclosure tables at a coarser precision than the working dataset. PySpark kept full precision and wrote 6 decimal places before CSV export.

### How to investigate

1. Open `data/output/R01.csv` and `data/baselines/R01.csv`.
2. Confirm keys align (`portfolio_id`, `issuer_id`). If a key is missing, this is **not** the rounding issue — stop and treat it as a mapping/filter bug.
3. For a breaking issuer, recompute by hand: `market_value_eur / evic × emissions` (see [03_report_catalog.md](03_report_catalog.md) ISS001 example). If the new file matches the hand calc, the new engine is right and SAS rounded.
4. Check `conf/reports/R01.yaml` tolerances. Do not silently widen them to hide the break.

### How to resolve (pick one, write it down)

| Decision | Action |
|---|---|
| Disclose at 1 tCO2e or 0.01 | Round in the **report** module (both platforms), regenerate baseline, recon should PASS |
| Keep SAS nearest-10 for one more year | Round the Databricks R01 output the same way until SAS is retired (not recommended, but explicit) |
| Publish full precision | Change the SAS job, replace the baseline with a new SAS export, recon PASS |

Until someone picks, R01 stays FAIL. That is correct.

---

## Seeded difference 2 — R09 ISS007 mapping

### What you see

R09 still has the same gap-cause labels, but `unmapped_issuer` / `mapped` **row counts and AUM** differ. The new pipeline has **one more unmapped** position (€4,000,000, issuer ISS007, instrument E007 in P01). The SAS baseline treats that AUM as mapped.

### What it represents in a real bank

Security master or the ESG ops team had ISS007 on the old SAS lookup. The new crosswalk (`mapping_issuer_to_provider.csv`) omitted it. This is the classic cutover defect: **the engine is fine; the reference data is not**.

ISS007 is named “Legacy Match Corp Oy” in the issuer file so it is easy to find in a demo.

### How to investigate

1. Confirm ISS007 in `data/landing/issuers.csv`.
2. Confirm ISS007 is **absent** from `data/landing/mapping_issuer_to_provider.csv`.
3. Confirm a holding exists: P01 / E007 / 4,000,000 EUR in `holdings.csv`.
4. Confirm R01 **does not** attribute ISS007 in the new output (unmapped issuers are kept on R09, not silently given emissions). The SAS baseline for R01 was **not** given extra ISS007 emissions — only R09 was mutated — so financed-emissions totals are not double-counting this story.
5. Ask data management: should ISS007 map to a real `provider_entity_id`? If yes, add the row and rerun silver → gold → reports. R09 recon will then fail until the SAS baseline is refreshed, which is what you want.

### How to resolve

| Decision | Action |
|---|---|
| New crosswalk is right (ISS007 should not map) | Accept R09 FAIL against old SAS; refresh baseline from SAS after they drop ISS007, or document a known difference until SAS is decommissioned |
| SAS was right | Add ISS007 to `mapping_issuer_to_provider` with a **real** MSCI id (not PROV999), rerun, replace baseline |
| Nobody knows | Leave it as unmapped; do not invent emissions. R09 is doing its job |

Do not “fix” R09 by deleting unmapped rows. Coverage is the control.

---

## What “done” looks like for a real cutover

- Every report either PASS or has a signed exception with an owner and a date.
- No new FAIL appears when you rerun with the same extracts (determinism).
- R09 gap causes are understood in euros, not only in row counts.
- Methodology (rounding, Scope 3, EVIC) is the same paragraph in the SAS runbook and in [03_report_catalog.md](03_report_catalog.md).
