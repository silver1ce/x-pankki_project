"""WACI and carbon footprint.

Business question this module answers: how carbon-intensive is each
portfolio per euro of revenue and per euro of assets under management?
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from xpankki_esg.io_utils import read_delta, write_delta
from xpankki_esg.logging_utils import get_logger, log_step, timed_step

LOGGER = get_logger(__name__)


def build_intensity(spark: SparkSession, cfg: dict, as_of_date: str) -> DataFrame:
    """Portfolio-level WACI and SFDR carbon footprint.

    SIMPLIFIED: TCFD WACI weights issuer intensity (tCO2e / EURm revenue) by
    portfolio market value. SFDR PAI 3 is the same idea at entity level.
    We use Scope 1+2+3 in the numerator, matching financed emissions.
    """
    with timed_step(LOGGER, "gold.intensity") as timer:
        fe = read_delta(spark, cfg, "gold", "financed_emissions").filter(
            F.col("as_of_date") == F.lit(as_of_date)
        )
        n_in = fe.count()

        # Intensity is only defined where we have both emissions and revenue.
        usable = fe.filter(
            F.col("issuer_emissions_tco2e").isNotNull()
            & (F.col("revenue_eur") > 0)
            & F.col("financed_emissions_tco2e").isNotNull()
        )
        usable = usable.withColumn(
            "issuer_intensity",
            F.col("issuer_emissions_tco2e") / (F.col("revenue_eur") / 1_000_000.0),
        )

        totals = fe.groupBy("portfolio_id").agg(F.sum("exposure_eur").alias("aum_eur"))
        weighted = (
            usable.groupBy("as_of_date", "portfolio_id")
            .agg(
                F.sum(F.col("issuer_intensity") * F.col("exposure_eur")).alias("_waci_num"),
                F.sum("exposure_eur").alias("covered_exposure_eur"),
                F.sum("financed_emissions_tco2e").alias("financed_emissions_tco2e"),
            )
            .join(totals, "portfolio_id", "left")
            .withColumn("waci_tco2e_per_m_eur", F.round(F.col("_waci_num") / F.col("covered_exposure_eur"), 6))
            .withColumn(
                "carbon_footprint_tco2e_per_m_eur",
                F.round(F.col("financed_emissions_tco2e") / (F.col("aum_eur") / 1_000_000.0), 6),
            )
            .drop("_waci_num")
        )
        write_delta(weighted, cfg, "gold", "carbon_intensity")
        dropped = n_in - usable.count()
        log_step(LOGGER, "gold.intensity", n_in, weighted.count(), dropped, "missing_emissions_or_revenue", timer.duration_s)
        return weighted
