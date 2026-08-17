"""R07 Fossil fuel exposure."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from xpankki_esg.io_utils import read_delta, write_report
from xpankki_esg.logging_utils import get_logger, log_step, timed_step

LOGGER = get_logger(__name__)


def build(spark: SparkSession, cfg: dict, as_of_date: str) -> DataFrame:
    """What share of each portfolio is exposed to companies active in fossil fuels (SFDR PAI 4)?"""
    with timed_step(LOGGER, "report.R07") as timer:
        # Step 1: read the gold tables this report depends on
        fe = read_delta(spark, cfg, "gold", "financed_emissions")
        n_in = fe.count()

        # Step 2: apply report-specific filters (reporting date)
        fe = fe.filter(F.col("as_of_date") == F.lit(as_of_date))

        # Step 3: aggregate to the reporting grain (portfolio)
        # SIMPLIFIED: SFDR PAI 4 uses a defined NACE list plus 'companies active
        # in the fossil fuel sector'. We use the vendor fossil_fuel_flag.
        out = fe.groupBy("as_of_date", "portfolio_id").agg(
            F.round(F.sum("exposure_eur"), 2).alias("aum_eur"),
            F.round(
                F.sum(
                    F.when(F.col("fossil_fuel_flag") == 1, F.col("exposure_eur")).otherwise(0.0)
                ),
                2,
            ).alias("fossil_fuel_market_value_eur"),
        ).withColumn(
            "fossil_fuel_exposure_pct",
            F.round(100.0 * F.col("fossil_fuel_market_value_eur") / F.col("aum_eur"), 6),
        )

        # Step 4: add coverage and data-quality columns
        flagged = fe.groupBy("portfolio_id").agg(
            F.sum(F.when(F.col("fossil_fuel_flag").isNotNull(), F.col("exposure_eur")).otherwise(0.0)).alias(
                "flagged_exposure_eur"
            )
        )
        out = out.join(flagged, "portfolio_id", "left").withColumn(
            "flag_coverage_pct",
            F.round(100.0 * F.col("flagged_exposure_eur") / F.col("aum_eur"), 4),
        )

        # Step 5: write to gold + CSV, and return the DataFrame for testing
        write_report(out, cfg, "R07")
        log_step(LOGGER, "report.R07", n_in, out.count(), 0, "", timer.duration_s)
        return out
