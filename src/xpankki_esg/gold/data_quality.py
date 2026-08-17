"""PCAF data-quality scoring (1 = verified, 5 = no usable data).

Business question this module answers: how much of the financed-emissions
number is backed by reported company data versus estimates or gaps?
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from xpankki_esg.io_utils import read_delta, write_delta
from xpankki_esg.logging_utils import get_logger, log_step, timed_step

LOGGER = get_logger(__name__)


def pcaf_score_column(df: DataFrame) -> DataFrame:
    """Add pcaf_score on a position-level financed-emissions frame.

    SIMPLIFIED: the real PCAF scorecard is option-based (1a verified, 1b
    unaudited, 2a reported physical, ... 5 estimated). This demo uses a
    four-rule version that a SAS developer can read in one glance.
    """
    has_s1s2 = F.col("scope1_tco2e").isNotNull() & F.col("scope2_tco2e").isNotNull()
    has_s3 = F.col("scope3_tco2e").isNotNull()
    return df.withColumn(
        "pcaf_score",
        F.when(F.col("coverage_status") != "mapped", F.lit(5))
        .when((F.col("emission_data_source") == "verified") & has_s1s2 & has_s3, F.lit(1))
        .when((F.col("emission_data_source") == "reported") & has_s1s2 & has_s3, F.lit(2))
        .when((F.col("emission_data_source") == "reported") & has_s1s2, F.lit(3))
        .when(F.col("emission_data_source") == "estimated", F.lit(4))
        .otherwise(F.lit(5)),
    )


def build_data_quality(spark: SparkSession, cfg: dict, as_of_date: str) -> DataFrame:
    """Outstanding-weighted PCAF score by portfolio and asset class."""
    with timed_step(LOGGER, "gold.data_quality") as timer:
        fe = read_delta(spark, cfg, "gold", "financed_emissions").filter(
            F.col("as_of_date") == F.lit(as_of_date)
        )
        n_in = fe.count()

        scored = (
            fe.groupBy("as_of_date", "portfolio_id", "asset_class")
            .agg(
                F.sum("exposure_eur").alias("exposure_eur"),
                F.sum(F.col("pcaf_score") * F.col("exposure_eur")).alias("_score_num"),
                F.sum(
                    F.when(F.col("financed_emissions_tco2e").isNotNull(), F.col("exposure_eur")).otherwise(0.0)
                ).alias("covered_exposure_eur"),
            )
            .withColumn("weighted_pcaf_score", F.round(F.col("_score_num") / F.col("exposure_eur"), 4))
            .withColumn(
                "coverage_pct",
                F.round(100.0 * F.col("covered_exposure_eur") / F.col("exposure_eur"), 4),
            )
            .drop("_score_num")
        )
        write_delta(scored, cfg, "gold", "pcaf_data_quality")
        log_step(LOGGER, "gold.data_quality", n_in, scored.count(), 0, "", timer.duration_s)
        return scored
