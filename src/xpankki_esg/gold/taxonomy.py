"""EU taxonomy alignment share.

Business question this module answers: of the exposure that is taxonomy-eligible,
how much is taxonomy-aligned, and what share of AUM does that represent?
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from xpankki_esg.io_utils import read_delta, write_delta
from xpankki_esg.logging_utils import get_logger, log_step, timed_step

LOGGER = get_logger(__name__)


def build_taxonomy(spark: SparkSession, cfg: dict, as_of_date: str) -> DataFrame:
    """Portfolio taxonomy eligible / aligned shares.

    SIMPLIFIED: the real EU taxonomy templates split turnover, CapEx and OpEx
    and apply the four alignment tests (substantial contribution, DNSH, social
    safeguards, technical screening). Here one issuer-level eligible share and
    one aligned share are weighted by exposure.
    """
    with timed_step(LOGGER, "gold.taxonomy") as timer:
        fe = read_delta(spark, cfg, "gold", "financed_emissions").filter(
            F.col("as_of_date") == F.lit(as_of_date)
        )
        tax = read_delta(spark, cfg, "silver", "taxonomy_data")
        fiscal_year = int(as_of_date[:4])
        tax = tax.filter(F.col("fiscal_year") == fiscal_year).select(
            "issuer_id", "taxonomy_eligible_share", "taxonomy_aligned_share"
        )
        n_in = fe.count()

        joined = fe.join(tax, "issuer_id", "left")
        agg = (
            joined.groupBy("as_of_date", "portfolio_id")
            .agg(
                F.sum("exposure_eur").alias("aum_eur"),
                F.sum(F.col("exposure_eur") * F.coalesce(F.col("taxonomy_eligible_share"), F.lit(0.0))).alias(
                    "eligible_eur"
                ),
                F.sum(F.col("exposure_eur") * F.coalesce(F.col("taxonomy_aligned_share"), F.lit(0.0))).alias(
                    "aligned_eur"
                ),
            )
            .withColumn(
                "eligible_share_pct",
                F.round(100.0 * F.col("eligible_eur") / F.col("aum_eur"), 6),
            )
            .withColumn(
                "aligned_share_of_eligible_pct",
                F.round(
                    F.when(F.col("eligible_eur") > 0, 100.0 * F.col("aligned_eur") / F.col("eligible_eur")).otherwise(0.0),
                    6,
                ),
            )
            .withColumn(
                "aligned_share_of_aum_pct",
                F.round(100.0 * F.col("aligned_eur") / F.col("aum_eur"), 6),
            )
        )
        write_delta(agg, cfg, "gold", "taxonomy_alignment")
        log_step(LOGGER, "gold.taxonomy", n_in, agg.count(), 0, "", timer.duration_s)
        return agg


def build_coverage(spark: SparkSession, cfg: dict, as_of_date: str) -> DataFrame:
    """Position-level gap causes used by R09. Not a regulatory report by itself."""
    fe = read_delta(spark, cfg, "gold", "financed_emissions").filter(F.col("as_of_date") == F.lit(as_of_date))
    coverage = fe.withColumn(
        "gap_cause",
        F.when(F.col("coverage_status") == "unmapped_issuer", F.lit("unmapped_issuer"))
        .when(F.col("coverage_status") == "orphan_provider", F.lit("orphan_provider"))
        .when(F.col("attribution_factor").isNull(), F.lit("zero_or_missing_denominator"))
        .when(F.col("financed_emissions_tco2e").isNull(), F.lit("missing_esg_data"))
        .otherwise(F.lit("mapped")),
    )
    write_delta(coverage, cfg, "gold", "coverage")
    return coverage
