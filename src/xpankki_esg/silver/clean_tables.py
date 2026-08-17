"""Typing, deduplication, validity checks. Rejects are written, never silent.

Business question this module answers: which rows are usable for reporting,
and for every row that is not, what was wrong with it?
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

from xpankki_esg.io_utils import read_delta, write_delta
from xpankki_esg.logging_utils import get_logger, log_step, timed_step

LOGGER = get_logger(__name__)


def clean_all(spark: SparkSession, cfg: dict, as_of_date: str) -> None:
    """Clean every bronze table into silver. Union rejects into silver._rejects."""
    reject_frames: list[DataFrame] = []

    reject_frames.append(_clean_portfolios(spark, cfg))
    reject_frames.append(_clean_instruments(spark, cfg))
    reject_frames.append(_clean_issuers(spark, cfg))
    reject_frames.append(_clean_holdings(spark, cfg))
    reject_frames.append(_clean_loans(spark, cfg))
    reject_frames.append(_clean_company_financials(spark, cfg))
    reject_frames.append(_clean_msci(spark, cfg))
    reject_frames.append(_clean_taxonomy(spark, cfg))
    reject_frames.append(_clean_fx(spark, cfg))
    reject_frames.append(_clean_mapping(spark, cfg))

    nonempty = [frame for frame in reject_frames if frame is not None and frame.head(1)]
    if nonempty:
        rejects = nonempty[0]
        for extra in nonempty[1:]:
            rejects = rejects.unionByName(extra, allowMissingColumns=True)
    else:
        rejects = spark.createDataFrame(
            [],
            "source_table string, reject_reason string, business_key string, details string",
        )
    write_delta(rejects, cfg, "silver", "_rejects")
    LOGGER.info("step=silver.rejects output_rows=%s", rejects.count())
    _ = as_of_date


def _audit_cols():
    return ["_ingested_at", "_source_file", "_batch_id"]


def _trim(df: DataFrame, columns: list[str]) -> DataFrame:
    for col in columns:
        df = df.withColumn(col, F.trim(F.col(col)))
    return df


def _write_clean(df: DataFrame, cfg: dict, table: str, schema_key: str = "silver") -> None:
    # Audit columns stay on bronze. Silver is the cleaned business table;
    # keeping them here would duplicate names on every join.
    for col in ("_ingested_at", "_source_file", "_batch_id"):
        if col in df.columns:
            df = df.drop(col)
    write_delta(df, cfg, schema_key, table)


def _rejects(df: DataFrame, source_table: str, reason: str, key_col: str) -> DataFrame:
    return df.select(
        F.lit(source_table).alias("source_table"),
        F.lit(reason).alias("reject_reason"),
        F.col(key_col).cast("string").alias("business_key"),
        F.to_json(F.struct(*[c for c in df.columns if not c.startswith("_")])).alias("details"),
    )


def _clean_portfolios(spark: SparkSession, cfg: dict) -> DataFrame:
    with timed_step(LOGGER, "silver.portfolios") as timer:
        raw = read_delta(spark, cfg, "bronze", "portfolios")
        n_in = raw.count()
        clean = _trim(raw, ["portfolio_id", "portfolio_name", "legal_entity", "asset_class_scope", "currency"])
        _write_clean(clean, cfg, "portfolios")
        log_step(LOGGER, "silver.portfolios", n_in, clean.count(), 0, "trim legal_entity trailing space", timer.duration_s)
        return spark.createDataFrame([], "source_table string, reject_reason string, business_key string, details string")


def _clean_instruments(spark: SparkSession, cfg: dict) -> DataFrame:
    with timed_step(LOGGER, "silver.instruments") as timer:
        raw = read_delta(spark, cfg, "bronze", "instruments")
        n_in = raw.count()
        clean = _trim(raw, ["instrument_id", "isin", "instrument_type", "issuer_id", "currency"])
        # Step 1: ISINs are compared case-insensitively in market data; normalise here.
        clean = clean.withColumn("isin", F.upper(F.col("isin")))
        _write_clean(clean, cfg, "instruments")
        log_step(LOGGER, "silver.instruments", n_in, clean.count(), 0, "trim and upper-case ISIN", timer.duration_s)
        return spark.createDataFrame([], "source_table string, reject_reason string, business_key string, details string")


def _clean_issuers(spark: SparkSession, cfg: dict) -> DataFrame:
    with timed_step(LOGGER, "silver.issuers") as timer:
        raw = read_delta(spark, cfg, "bronze", "issuers")
        n_in = raw.count()
        clean = _trim(raw, ["issuer_id", "lei", "issuer_name", "country", "nace_sector"])
        # Empty LEI stays as null. We flag it; we do not drop the issuer.
        clean = clean.withColumn("lei", F.when(F.col("lei") == "", F.lit(None)).otherwise(F.col("lei")))
        clean = clean.withColumn("lei_missing_flag", F.col("lei").isNull().cast("int"))
        _write_clean(clean, cfg, "issuers")
        log_step(LOGGER, "silver.issuers", n_in, clean.count(), 0, "null LEI kept and flagged", timer.duration_s)
        return spark.createDataFrame([], "source_table string, reject_reason string, business_key string, details string")


def _clean_holdings(spark: SparkSession, cfg: dict) -> DataFrame:
    with timed_step(LOGGER, "silver.holdings") as timer:
        raw = read_delta(spark, cfg, "bronze", "holdings")
        n_in = raw.count()
        typed = _trim(raw, ["portfolio_id", "instrument_id", "as_of_date", "currency"])
        typed = typed.withColumn("as_of_date", F.to_date("as_of_date", "yyyy-MM-dd"))
        typed = typed.withColumn("quantity", F.col("quantity").cast(DoubleType()))
        typed = typed.withColumn("market_value", F.col("market_value").cast(DoubleType()))

        missing = typed.filter(F.col("market_value").isNull())
        usable = typed.filter(F.col("market_value").isNotNull())
        # Step 1: identical duplicate keys from the SAS extract — keep one.
        before_dedup = usable.count()
        usable = usable.dropDuplicates(["portfolio_id", "instrument_id", "as_of_date"])
        n_dup = before_dedup - usable.count()

        _write_clean(usable, cfg, "holdings")
        n_out = usable.count()
        n_drop = n_in - n_out
        log_step(
            LOGGER,
            "silver.holdings",
            n_in,
            n_out,
            n_drop,
            f"missing_market_value={missing.count()}; duplicates_removed={n_dup}",
            timer.duration_s,
        )
        dup_reason = spark.createDataFrame([], "source_table string, reject_reason string, business_key string, details string")
        rejects = _rejects(missing, "holdings", "missing_market_value", "instrument_id")
        return rejects.unionByName(dup_reason, allowMissingColumns=True)


def _clean_loans(spark: SparkSession, cfg: dict) -> DataFrame:
    with timed_step(LOGGER, "silver.loans") as timer:
        raw = read_delta(spark, cfg, "bronze", "loans")
        n_in = raw.count()
        typed = _trim(raw, ["loan_id", "borrower_issuer_id", "as_of_date", "currency", "loan_purpose"])
        typed = typed.withColumn("as_of_date", F.to_date("as_of_date", "yyyy-MM-dd"))
        typed = typed.withColumn("outstanding_amount", F.col("outstanding_amount").cast(DoubleType()))
        bad = typed.filter(F.col("outstanding_amount") < 0)
        good = typed.filter(F.col("outstanding_amount") >= 0)
        _write_clean(good, cfg, "loans")
        log_step(LOGGER, "silver.loans", n_in, good.count(), bad.count(), "negative_outstanding_amount", timer.duration_s)
        return _rejects(bad, "loans", "negative_outstanding_amount", "loan_id")


def _clean_company_financials(spark: SparkSession, cfg: dict) -> DataFrame:
    with timed_step(LOGGER, "silver.company_financials") as timer:
        raw = read_delta(spark, cfg, "bronze", "company_financials")
        n_in = raw.count()
        clean = _trim(raw, ["issuer_id"])
        clean = (
            clean.withColumn("fiscal_year", F.col("fiscal_year").cast(IntegerType()))
            .withColumn("evic_eur", F.col("evic_eur").cast(DoubleType()))
            .withColumn("total_assets_eur", F.col("total_assets_eur").cast(DoubleType()))
            .withColumn("revenue_eur", F.col("revenue_eur").cast(DoubleType()))
        )
        # EVIC = 0 stays; gold will refuse to use it as a denominator.
        _write_clean(clean, cfg, "company_financials")
        log_step(LOGGER, "silver.company_financials", n_in, clean.count(), 0, "evic_zero_kept", timer.duration_s)
        return spark.createDataFrame([], "source_table string, reject_reason string, business_key string, details string")


def _clean_msci(spark: SparkSession, cfg: dict) -> DataFrame:
    with timed_step(LOGGER, "silver.msci_esg") as timer:
        # Restricted vendor data stays in esg_restricted through silver.
        raw = read_delta(spark, cfg, "esg_restricted", "msci_esg")
        n_in = raw.count()
        clean = _trim(raw, ["provider_entity_id", "emission_data_source", "esg_rating"])
        clean = (
            clean.withColumn("fiscal_year", F.col("fiscal_year").cast(IntegerType()))
            .withColumn("scope1_tco2e", F.col("scope1_tco2e").cast(DoubleType()))
            .withColumn("scope2_tco2e", F.col("scope2_tco2e").cast(DoubleType()))
            .withColumn("scope3_tco2e", F.col("scope3_tco2e").cast(DoubleType()))
            .withColumn("fossil_fuel_flag", F.col("fossil_fuel_flag").cast(IntegerType()))
        )
        clean = clean.withColumn(
            "esg_rating",
            F.when(F.col("esg_rating") == "", F.lit(None)).otherwise(F.col("esg_rating")),
        )
        for col in ("_ingested_at", "_source_file", "_batch_id"):
            if col in clean.columns:
                clean = clean.drop(col)
        write_delta(clean, cfg, "esg_restricted", "msci_esg_silver")
        log_step(LOGGER, "silver.msci_esg", n_in, clean.count(), 0, "missing_scope3_kept", timer.duration_s)
        return spark.createDataFrame([], "source_table string, reject_reason string, business_key string, details string")


def _clean_taxonomy(spark: SparkSession, cfg: dict) -> DataFrame:
    with timed_step(LOGGER, "silver.taxonomy_data") as timer:
        raw = read_delta(spark, cfg, "bronze", "taxonomy_data")
        n_in = raw.count()
        typed = _trim(raw, ["issuer_id"])
        typed = (
            typed.withColumn("fiscal_year", F.col("fiscal_year").cast(IntegerType()))
            .withColumn("taxonomy_eligible_share", F.col("taxonomy_eligible_share").cast(DoubleType()))
            .withColumn("taxonomy_aligned_share", F.col("taxonomy_aligned_share").cast(DoubleType()))
        )
        bad = typed.filter(F.col("taxonomy_aligned_share") > F.col("taxonomy_eligible_share"))
        good = typed.filter(F.col("taxonomy_aligned_share") <= F.col("taxonomy_eligible_share"))
        _write_clean(good, cfg, "taxonomy_data")
        log_step(LOGGER, "silver.taxonomy_data", n_in, good.count(), bad.count(), "aligned_gt_eligible", timer.duration_s)
        return _rejects(bad, "taxonomy_data", "aligned_gt_eligible", "issuer_id")


def _clean_fx(spark: SparkSession, cfg: dict) -> DataFrame:
    with timed_step(LOGGER, "silver.fx_rates") as timer:
        raw = read_delta(spark, cfg, "bronze", "fx_rates")
        n_in = raw.count()
        typed = _trim(raw, ["currency", "as_of_date"])
        # SAS DATE9. (31DEC2025). Spark MMM tokens need an English locale.
        typed = typed.withColumn("as_of_date", F.to_date(F.upper("as_of_date"), "ddMMMyyyy"))
        typed = typed.withColumn("rate_to_eur", F.col("rate_to_eur").cast(DoubleType()))
        bad = typed.filter(F.col("as_of_date").isNull())
        good = typed.filter(F.col("as_of_date").isNotNull())
        _write_clean(good, cfg, "fx_rates")
        log_step(LOGGER, "silver.fx_rates", n_in, good.count(), bad.count(), "unparseable_sas_date9", timer.duration_s)
        return _rejects(bad, "fx_rates", "unparseable_sas_date9", "currency")


def _clean_mapping(spark: SparkSession, cfg: dict) -> DataFrame:
    with timed_step(LOGGER, "silver.mapping_issuer_to_provider") as timer:
        raw = read_delta(spark, cfg, "bronze", "mapping_issuer_to_provider")
        n_in = raw.count()
        clean = _trim(raw, ["issuer_id", "provider_entity_id"])
        _write_clean(clean, cfg, "mapping_issuer_to_provider")
        log_step(LOGGER, "silver.mapping_issuer_to_provider", n_in, clean.count(), 0, "incomplete_crosswalk_kept", timer.duration_s)
        return spark.createDataFrame([], "source_table string, reject_reason string, business_key string, details string")
