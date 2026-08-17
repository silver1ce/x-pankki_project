# SAS to PySpark mapping

If you can maintain a SAS ESG job today, you can read this pipeline. Spark is not a new accounting standard; it is a different way to express the same steps: read a table, join, filter, summarise, write. This page is the translation sheet. After it, open any file under `src/xpankki_esg/reports/` — each one has a one-line business question and numbered `# Step 1` comments.

## For business readers

The migration risk is not “Python versus SAS”. It is whether **PROC MEANS rounding**, **MERGE BY** match rates, and **macro as-of dates** were re-implemented the same way. That is why we freeze SAS-like baselines and reconcile. Two differences are planted on purpose (see [06_reconciliation.md](06_reconciliation.md)) so the cutover conversation is concrete.

## Construct map

| SAS construct | What it does in the ESG job | PySpark equivalent in this repo |
|---|---|---|
| `LIBNAME` / `SAS-data-set` | Named table in a library | Delta table path or `catalog.schema.table` via `table_identifier()` in `config.py` |
| `%LET asof = 31DEC2025;` | Macro variable for the run date | CLI `--as-of 2025-12-31` (and YAML `default_as_of_date`) |
| `DATA` step | Row-by-row create / if-then | `DataFrame.withColumn` + `when` / `otherwise` |
| `SET` | Read a dataset | `spark.read.format("delta").load(...)` or `spark.read.csv` |
| `PROC SQL` | Joins and filters in SQL | `df.join(...)` or `spark.sql(...)` — we use the DataFrame API so the code is reviewable without a SQL warehouse |
| `PROC MEANS` / `PROC SUMMARY` | Totals, means, weighted means | `groupBy(...).agg(sum, avg, ...)` |
| `MERGE a b; BY key;` | Match two sorted tables | `join` with `"inner"` / `"left"`. Spark does **not** require a prior `PROC SORT` |
| `BY` group processing (`first.` / `last.`) | Dedup keep first | `dropDuplicates([keys])` after you have decided the sort order |
| `IF x THEN OUTPUT; ELSE DELETE;` | Drop rows | `filter`. In this project, bad rows go to `_rejects` instead of vanishing |
| `FORMAT date date9.;` | SAS date display `31DEC2025` | Silver: `to_date(as_of_date, "ddMMMyyyy")` on `fx_rates` only |
| `UPCASE` / `STRIP` | Normalise codes | `upper`, `trim` in `silver/clean_tables.py` |
| `PUT` / `INPUT` | Type conversion | `.cast("double")`, `.cast("int")` |
| `PROC SORT NODUPKEY` | Unique keys | `dropDuplicates` |
| `%INCLUDE` / macro library | Shared logic | Ordinary Python functions (`pcaf_score_column`, `write_report`) — no macro preprocessor |
| `PROC EXPORT` | Write CSV for the business | `io_utils.write_csv` → `data/output/R0N.csv` |

Spark difference that surprises SAS developers: **there is no implicit retain or `first.` unless you code a window**. Joins can duplicate rows if the right-hand table is not unique on the key. We keep mapping and MSCI unique on `(issuer_id)` and `(provider_entity_id, fiscal_year)`.

## Worked example: holdings into euro

This is the heart of PCAF: convert native market value to EUR using the reporting-date FX table. In SAS many shops still write it as SQL. Below is the same rule both ways.

### Legacy SAS (what the old job looks like)

```sas
%let asof = 31DEC2025;

proc sql;
    create table work.holdings_eur as
    select
        h.portfolio_id,
        h.instrument_id,
        h.as_of_date,
        i.issuer_id,
        i.instrument_type,
        h.market_value * coalesce(f.rate_to_eur, 1) as exposure_eur
    from
        sasdw.holdings h
        left join sasdw.instruments i
            on h.instrument_id = i.instrument_id
        left join sasdw.fx_rates f
            on h.currency = f.currency
           and h.as_of_date = f.as_of_date
    where
        h.as_of_date = "&asof"d
        and h.market_value is not missing;
quit;
```

Notes a reviewer should catch:

- `"&asof"d` is a SAS date constant. Our CLI uses ISO `2025-12-31`; FX still *arrives* as `31DEC2025` and is parsed in silver.
- `coalesce(..., 1)` is the same fallback we use if EUR has no rate row.
- SAS would often `PROC SORT` both tables before a `MERGE`. SQL here does not need that; neither does Spark.

### This project (PySpark)

From `src/xpankki_esg/gold/pcaf.py` (holdings branch, shortened):

```python
holdings = read_delta(spark, cfg, "silver", "holdings").filter(
    F.col("as_of_date") == F.lit(as_of_date)
)
instruments = read_delta(spark, cfg, "silver", "instruments")
fx = (
    read_delta(spark, cfg, "silver", "fx_rates")
    .filter(F.col("as_of_date") == F.lit(as_of_date))
    .select(F.col("currency").alias("fx_currency"), F.col("rate_to_eur"))
)

listed = (
    holdings.join(instruments, "instrument_id", "left")
    .join(fx, holdings["currency"] == fx["fx_currency"], "left")
    .withColumn(
        "exposure_eur",
        F.col("market_value") * F.coalesce(F.col("rate_to_eur"), F.lit(1.0)),
    )
)
```

Same three tables, same left joins, same `coalesce` to 1. Missing market value never reaches this step: silver already rejected it.

### `PROC MEANS` → `groupBy`

SAS:

```sas
proc means data=work.financed noprint;
    class portfolio_id issuer_id;
    var financed_emissions_tco2e market_value_eur;
    output out=r01 sum=;
run;
```

PySpark (R01):

```python
out = filtered.groupBy("portfolio_id", "issuer_id").agg(
    F.sum("exposure_eur").alias("market_value_eur"),
    F.sum("financed_emissions_tco2e").alias("financed_emissions_tco2e"),
)
```

**Seeded cutover issue:** SAS `PROC MEANS` in this demo is treated as having rounded financed emissions to the nearest 10 tCO2e. Spark keeps full precision. That is why R01 fails recon until someone agrees a rounding policy. See [06_reconciliation.md](06_reconciliation.md).

## Reading a report module after this page

Every report file has the same shape:

1. Read gold.
2. Filter (asset class, date, legal entity).
3. Aggregate to the grain in the YAML.
4. Add coverage / quality columns.
5. Write Delta + CSV.

Start with `src/xpankki_esg/reports/r01_financed_emissions_equity.py`. If that is clear, R02–R09 will be too.
