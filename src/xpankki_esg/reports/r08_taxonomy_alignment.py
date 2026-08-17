"""R08 EU taxonomy alignment."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from xpankki_esg.io_utils import read_delta, write_report
from xpankki_esg.logging_utils import get_logger, log_step, timed_step

LOGGER = get_logger(__name__)


def build(spark: SparkSession, cfg: dict, as_of_date: str) -> DataFrame:
    """What share of each portfolio's eligible exposure is EU-taxonomy aligned?"""
    with timed_step(LOGGER, "report.R08") as timer:
        # Step 1: read the gold tables this report depends on
        tax = read_delta(spark, cfg, "gold", "taxonomy_alignment")
        n_in = tax.count()

        # Step 2: apply report-specific filters (reporting date)
        out = tax.filter(F.col("as_of_date") == F.lit(as_of_date))

        # Step 3: aggregate to the reporting grain (already one row per portfolio)
        out = out.select(
            "as_of_date",
            "portfolio_id",
            "eligible_share_pct",
            "aligned_share_of_eligible_pct",
            "aligned_share_of_aum_pct",
            "aum_eur",
        )

        # Step 4: add coverage and data-quality columns
        out = out.withColumn(
            "simplification_note",
            F.lit("SIMPLIFIED: issuer-level shares weighted by exposure; no turnover/CapEx split"),
        )

        # Step 5: write to gold + CSV, and return the DataFrame for testing
        write_report(out, cfg, "R08")
        log_step(LOGGER, "report.R08", n_in, out.count(), n_in - out.count(), "other_date", timer.duration_s)
        return out
