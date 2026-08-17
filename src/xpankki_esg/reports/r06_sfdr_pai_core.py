"""R06 SFDR PAI core indicators."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from xpankki_esg.io_utils import read_delta, write_report
from xpankki_esg.logging_utils import get_logger, log_step, timed_step

LOGGER = get_logger(__name__)


def build(spark: SparkSession, cfg: dict, as_of_date: str) -> DataFrame:
    """What are SFDR PAI 1–3 (GHG emissions, carbon footprint, GHG intensity) per legal entity?"""
    with timed_step(LOGGER, "report.R06") as timer:
        # Step 1: read the gold tables this report depends on
        fe = read_delta(spark, cfg, "gold", "financed_emissions")
        intensity = read_delta(spark, cfg, "gold", "carbon_intensity")
        n_in = fe.count()

        # Step 2: apply report-specific filters (reporting date)
        fe = fe.filter(F.col("as_of_date") == F.lit(as_of_date))
        intensity = intensity.filter(F.col("as_of_date") == F.lit(as_of_date))
        portfolios = read_delta(spark, cfg, "silver", "portfolios").select("portfolio_id", "legal_entity")
        intensity = intensity.join(portfolios, "portfolio_id", "left")

        # Step 3: aggregate to the reporting grain (legal entity x PAI indicator)
        # SIMPLIFIED: SFDR PAI 1 is Scope 1, 2 and 3 financed emissions reported
        # separately in the RTS. We publish one combined GHG total per entity.
        pai1 = fe.groupBy("legal_entity").agg(
            F.round(F.sum("financed_emissions_tco2e"), 6).alias("pai_value"),
            F.round(F.sum("exposure_eur"), 2).alias("aum_eur"),
        ).withColumn("pai_indicator", F.lit("PAI1_ghg_emissions_tco2e"))

        pai2 = pai1.withColumn(
            "pai_value",
            F.round(F.col("pai_value") / (F.col("aum_eur") / 1_000_000.0), 6),
        ).withColumn("pai_indicator", F.lit("PAI2_carbon_footprint_tco2e_per_m_eur"))

        # PAI 3: exposure-weighted WACI across the entity's portfolios.
        pai3 = (
            intensity.groupBy("legal_entity")
            .agg(
                F.round(
                    F.sum(F.col("waci_tco2e_per_m_eur") * F.col("aum_eur")) / F.sum("aum_eur"),
                    6,
                ).alias("pai_value")
            )
            .withColumn("pai_indicator", F.lit("PAI3_ghg_intensity_tco2e_per_m_eur"))
        )

        out = (
            pai1.select("legal_entity", "pai_indicator", "pai_value")
            .unionByName(pai2.select("legal_entity", "pai_indicator", "pai_value"))
            .unionByName(pai3.select("legal_entity", "pai_indicator", "pai_value"))
        )

        # Step 4: add coverage and data-quality columns
        out = out.withColumn("as_of_date", F.lit(as_of_date))

        # Step 5: write to gold + CSV, and return the DataFrame for testing
        write_report(out, cfg, "R06")
        log_step(LOGGER, "report.R06", n_in, out.count(), 0, "", timer.duration_s)
        return out
