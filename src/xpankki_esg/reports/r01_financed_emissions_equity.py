"""R01 Financed emissions — listed equity."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from xpankki_esg.io_utils import read_delta, write_report
from xpankki_esg.logging_utils import get_logger, log_step, timed_step

LOGGER = get_logger(__name__)


def build(spark: SparkSession, cfg: dict, as_of_date: str) -> DataFrame:
    """How much of portfolio listed-equity AUM finances investee GHG emissions?"""
    with timed_step(LOGGER, "report.R01") as timer:
        # Step 1: read the gold tables this report depends on
        fe = read_delta(spark, cfg, "gold", "financed_emissions")
        n_in = fe.count()

        # Step 2: apply report-specific filters (asset class, reporting date)
        filtered = fe.filter(
            (F.col("as_of_date") == F.lit(as_of_date)) & (F.col("asset_class") == "listed_equity")
        )

        # Step 3: aggregate to the reporting grain (portfolio x issuer)
        out = filtered.groupBy("as_of_date", "portfolio_id", "issuer_id", "issuer_name").agg(
            F.round(F.sum("exposure_eur"), 2).alias("market_value_eur"),
            F.round(F.sum("financed_emissions_tco2e"), 6).alias("financed_emissions_tco2e"),
            F.round(F.max("attribution_factor"), 10).alias("attribution_factor"),
        )

        # Step 4: add coverage and data-quality columns
        covered = filtered.groupBy("portfolio_id", "issuer_id").agg(
            F.round(F.avg("pcaf_score"), 4).alias("pcaf_score"),
            F.max(F.when(F.col("financed_emissions_tco2e").isNotNull(), 1).otherwise(0)).alias(
                "has_emissions_flag"
            ),
        )
        out = out.join(covered, ["portfolio_id", "issuer_id"], "left")

        # Step 5: write to gold + CSV, and return the DataFrame for testing
        write_report(out, cfg, "R01")
        log_step(LOGGER, "report.R01", n_in, out.count(), n_in - filtered.count(), "non_equity_or_other_date", timer.duration_s)
        return out
