"""R05 WACI by portfolio."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from xpankki_esg.io_utils import read_delta, write_report
from xpankki_esg.logging_utils import get_logger, log_step, timed_step

LOGGER = get_logger(__name__)


def build(spark: SparkSession, cfg: dict, as_of_date: str) -> DataFrame:
    """What is the weighted average carbon intensity of each portfolio?"""
    with timed_step(LOGGER, "report.R05") as timer:
        # Step 1: read the gold tables this report depends on
        intensity = read_delta(spark, cfg, "gold", "carbon_intensity")
        n_in = intensity.count()

        # Step 2: apply report-specific filters (reporting date)
        out = intensity.filter(F.col("as_of_date") == F.lit(as_of_date))

        # Step 3: aggregate to the reporting grain (already one row per portfolio)
        out = out.select(
            "as_of_date",
            "portfolio_id",
            "waci_tco2e_per_m_eur",
            "carbon_footprint_tco2e_per_m_eur",
            "financed_emissions_tco2e",
            "aum_eur",
        )

        # Step 4: add coverage and data-quality columns
        out = out.withColumn(
            "coverage_note",
            F.lit("WACI uses positions with emissions and revenue; footprint uses full AUM"),
        )

        # Step 5: write to gold + CSV, and return the DataFrame for testing
        write_report(out, cfg, "R05")
        log_step(LOGGER, "report.R05", n_in, out.count(), n_in - out.count(), "other_date", timer.duration_s)
        return out
