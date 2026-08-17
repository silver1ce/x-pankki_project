"""R04 PCAF data quality score."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from xpankki_esg.io_utils import read_delta, write_report
from xpankki_esg.logging_utils import get_logger, log_step, timed_step

LOGGER = get_logger(__name__)


def build(spark: SparkSession, cfg: dict, as_of_date: str) -> DataFrame:
    """What is the outstanding-weighted PCAF data-quality score of each book?"""
    with timed_step(LOGGER, "report.R04") as timer:
        # Step 1: read the gold tables this report depends on
        dq = read_delta(spark, cfg, "gold", "pcaf_data_quality")
        n_in = dq.count()

        # Step 2: apply report-specific filters (reporting date)
        out = dq.filter(F.col("as_of_date") == F.lit(as_of_date))

        # Step 3: aggregate to the reporting grain (already portfolio x asset class)
        out = out.select(
            "as_of_date",
            "portfolio_id",
            "asset_class",
            "weighted_pcaf_score",
            "coverage_pct",
            "exposure_eur",
        )

        # Step 4: add coverage and data-quality columns (already on the gold table)
        # Step 5: write to gold + CSV, and return the DataFrame for testing
        write_report(out, cfg, "R04")
        log_step(LOGGER, "report.R04", n_in, out.count(), n_in - out.count(), "other_date", timer.duration_s)
        return out
