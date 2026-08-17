"""R09 Coverage and data quality summary."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from xpankki_esg.io_utils import read_delta, write_report
from xpankki_esg.logging_utils import get_logger, log_step, timed_step

LOGGER = get_logger(__name__)


def build(spark: SparkSession, cfg: dict, as_of_date: str) -> DataFrame:
    """Where are the ESG data gaps, and how much AUM do they represent?"""
    with timed_step(LOGGER, "report.R09") as timer:
        # Step 1: read the gold tables this report depends on
        coverage = read_delta(spark, cfg, "gold", "coverage")
        n_in = coverage.count()

        # Step 2: apply report-specific filters (reporting date)
        filtered = coverage.filter(F.col("as_of_date") == F.lit(as_of_date))

        # Step 3: aggregate to the reporting grain (gap cause)
        totals = filtered.agg(F.sum("exposure_eur").alias("total_aum")).collect()[0]["total_aum"] or 0.0
        out = (
            filtered.groupBy("gap_cause")
            .agg(
                F.count("*").alias("row_count"),
                F.round(F.sum("exposure_eur"), 2).alias("market_value_eur"),
            )
            .withColumn(
                "share_of_aum_pct",
                F.round(100.0 * F.col("market_value_eur") / F.lit(float(totals)), 6),
            )
        )

        # Step 4: add coverage and data-quality columns
        out = out.withColumn("as_of_date", F.lit(as_of_date))

        # Step 5: write to gold + CSV, and return the DataFrame for testing
        write_report(out, cfg, "R09")
        log_step(LOGGER, "report.R09", n_in, out.count(), 0, "", timer.duration_s)
        return out
