# Runbook

This is the monthly operating sheet: what you run, what “good” looks like, and what to do when it is not. You do not need Spark knowledge to follow the checks. You do need access to the project folder (locally) or to the Databricks job (in production).

## For business readers

A reporting month is successful when (1) the nine CSVs are produced, (2) rejects are only the kinds of issues we already understand, (3) coverage in R09 is explained, and (4) reconciliation against the agreed SAS baseline is either all green or only has **documented** breaks. This demo always has two documented breaks (R01 rounding, R09 ISS007). A live cutover should not invent new ones without an owner.

## Local monthly run (this laptop)

Prerequisites: Python 3.11. `make setup` installs packages and, if needed, Temurin JDK 17 under `~/.jdks/temurin-17` (PySpark will not start without a real JDK).

```bash
cd ~/Desktop/x-pankki_project
make setup                         # first machine only, or after requirements.txt changes
make generate                      # first time, or when you want a clean synthetic refresh
make run                           # bronze → silver → gold → R01–R09
make recon                         # compare to frozen SAS baselines
```

Default date is **2025-12-31**. To run a different date (only useful once generate supports it):

```bash
.venv/bin/python -m xpankki_esg.pipeline run --layer all --as-of 2025-12-31
```

Layer-by-layer (when you are debugging):

```bash
.venv/bin/python -m xpankki_esg.pipeline run --layer bronze --as-of 2025-12-31
.venv/bin/python -m xpankki_esg.pipeline run --layer silver --as-of 2025-12-31
.venv/bin/python -m xpankki_esg.pipeline run --layer gold --as-of 2025-12-31
.venv/bin/python -m xpankki_esg.pipeline run --layer reports --as-of 2025-12-31
.venv/bin/python -m xpankki_esg.pipeline run --report R01 --as-of 2025-12-31
```

`make clean` deletes `data/landing`, `data/lakehouse`, `data/baselines` and `data/output`. It keeps `.venv`.

## What to check after a run (15-minute control)

| Check | Where | Good looks like |
|---|---|---|
| All nine reports exist and have a header plus data rows | `data/output/R01.csv` … `R09.csv` | R01 has more than one issuer; R09 has `mapped` plus gap causes |
| Rejects are explained | logs `step=silver.* rows_dropped=` and table `silver/_rejects` | Demo: 1 missing MV, 1 negative loan, 1 taxonomy aligned>eligible |
| Holdings dedup happened | log `silver.holdings` | `duplicates_removed=1` |
| FX parsed | log `silver.fx_rates` | `rows_dropped=0` for `unparseable_sas_date9` |
| Entity coverage | log `silver.entity_resolution` | 12 issuers out; some unmapped/orphan **kept** |
| Restricted MSCI not copied to open bronze | folders | `msci_esg` under `data/lakehouse/esg_restricted/`, not under `bronze/` |
| Recon | `make recon` | 7 PASS, R01 and R09 FAIL with the seeded notes |

In the log, every step prints:

`step=… input_rows=… output_rows=… rows_dropped=… drop_reason=… duration_s=…`

If `rows_dropped` is large and `drop_reason` is `-`, stop and investigate. Silent loss is a defect in this design.

## Databricks monthly run (when deployed)

Not automated in this repo. The intended pattern:

1. SAS DW (or a landing service) writes extracts into Volume `esg_prod.landing.sas_extracts`.
2. A Workflows job runs `python -m xpankki_esg.pipeline run --layer all --as-of <month-end>` with `XPANKKI_ENV=databricks`.
3. Reports land in Volume `esg_prod.output` and as gold tables.
4. A recon task compares to baselines in `esg_prod.baselines`.
5. Fail the job if any **unexpected** report fails (keep the two seeded diffs only in the demo).

See [07_databricks_deployment.md](07_databricks_deployment.md).

## Common failures

### 1. `Unable to locate a Java Runtime` / Spark never starts

**Meaning.** macOS `/usr/bin/java` is a stub. PySpark needs JDK 11 or 17.

**Do this.** `make setup` (installs Temurin 17 to `~/.jdks/temurin-17`) or set `JAVA_HOME` yourself. Confirm with `"$JAVA_HOME/bin/java" -version`.

### 2. `No such file or directory` on a landing CSV

**Meaning.** `make run` was started before `make generate`, or `make clean` wiped landing.

**Do this.** `make generate` then `make run`. In production, check the overnight SAS drop landed in the volume.

### 3. `DELTA_DUPLICATE_COLUMNS_FOUND` on `_batch_id` / `_ingested_at`

**Meaning.** Two tables with bronze audit columns were joined without dropping the stamps. Silver is supposed to drop them.

**Do this.** Do not add `_ingested_at` to silver joins. Fix is already in `clean_tables._write_clean`. If you join bronze to bronze, drop or alias audit columns first.

### 4. FX dates all rejected (`unparseable_sas_date9`)

**Meaning.** `fx_rates` was delivered as ISO `2025-12-31` while silver still parses `DATE9.`, or the other way round.

**Do this.** Open `data/landing/fx_rates.csv`. Values should look like `31DEC2025`. Parser: `to_date(upper(as_of_date), "ddMMMyyyy")` with Spark `LEGACY` time parser. If the SAS job changes format, update `conf/sources.yaml` and silver together.

### 5. R01 (or any report) is empty

**Meaning.** Wrong as-of date, or gold has no listed equity for that date (holdings filtered out).

**Do this.** Confirm `--as-of` matches holdings `as_of_date`. Confirm silver holdings still has 2025-12-31 rows (the 2024-12-31 row is historical and correctly excluded from gold).

### 6. Financed emissions null for a large issuer

**Meaning.** Usually unmapped, orphan provider, or EVIC = 0 — not a Spark crash.

**Do this.** Look up `issuer_id` in R09 / `gold.coverage` `gap_cause`. ISS008 is the planted zero-EVIC case. ISS006/ISS007 are unmapped. ISS010 is orphan. Fix data (crosswalk or financials), do not “fill” emissions in the report.

### 7. Recon FAIL on R02–R08 (unexpected)

**Meaning.** Gold maths drifted, or someone regenerated baselines without the seeded mutations, or floating-point output formatting changed.

**Do this.** Do not accept it as “Spark is different”. Diff `data/output/R0N.csv` vs `data/baselines/R0N.csv`. If you changed a formula, update the baseline only after a business sign-off. Demo expected fails are **R01 and R09 only**.

### 8. Databricks job cannot read `esg_restricted.msci_esg`

**Meaning.** The job identity has bronze/gold grants but not the restricted schema.

**Do this.** This is a feature: ask the data owner to grant `USE SCHEMA` + `SELECT` on `esg_restricted` to the job principal. See ADR 0002.

## Who to call (demo RACI)

| Symptom | Owner in a real bank |
|---|---|
| Extract missing or late | SAS DW / operations |
| Crosswalk gaps (R09 unmapped) | Security master / ESG data management |
| PCAF formula dispute | Climate risk / methodology |
| Job failure, Spark, Delta | Data engineering |
| Sign-off that recon is “good enough” to cut over | Report owner (risk / disclosure) |
