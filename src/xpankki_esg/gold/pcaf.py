"""PCAF attribution factors and financed emissions.

Business question this module answers: what share of each issuer's GHG
emissions is attributable to this bank's equity, bond, and loan exposure?
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from xpankki_esg.gold.data_quality import pcaf_score_column
from xpankki_esg.io_utils import read_delta, write_delta
from xpankki_esg.logging_utils import get_logger, log_step, timed_step

LOGGER = get_logger(__name__)


def build_financed_emissions(spark: SparkSession, cfg: dict, as_of_date: str) -> DataFrame:
    """Position-level financed emissions for equity, bonds, and business loans."""
    with timed_step(LOGGER, "gold.financed_emissions") as timer:
        positions = _positions_in_eur(spark, cfg, as_of_date)
        n_in = positions.count()
        fiscal_year = int(as_of_date[:4])

        financials = read_delta(spark, cfg, "silver", "company_financials").filter(
            F.col("fiscal_year") == fiscal_year
        )
        entity_map = read_delta(spark, cfg, "silver", "issuer_entity_map").select(
            "issuer_id",
            "issuer_name",
            "provider_entity_id",
            "coverage_status",
            "lei_missing_flag",
        )
        msci = read_delta(spark, cfg, "esg_restricted", "msci_esg_silver").filter(
            F.col("fiscal_year") == fiscal_year
        ).select(
            "provider_entity_id",
            "scope1_tco2e",
            "scope2_tco2e",
            "scope3_tco2e",
            "emission_data_source",
            "esg_rating",
            "fossil_fuel_flag",
        )

        # Step 1: attach issuer, mapping status, and (where mapped) vendor emissions.
        fe = (
            positions.join(financials, "issuer_id", "left")
            .join(entity_map, "issuer_id", "left")
            .join(msci, "provider_entity_id", "left")
        )

        # SIMPLIFIED: real PCAF listed-equity uses EVIC including cash, converted
        # at the reporting-date FX into the holding currency. We convert the
        # holding to EUR and divide by EVIC already expressed in EUR.
        equity_bond = F.col("asset_class").isin("listed_equity", "corporate_bond")
        loan = F.col("asset_class") == "business_loan"

        fe = fe.withColumn(
            "attribution_factor",
            F.when(equity_bond & (F.col("evic_eur") > 0), F.col("exposure_eur") / F.col("evic_eur"))
            .when(loan & (F.col("total_assets_eur") > 0), F.col("exposure_eur") / F.col("total_assets_eur"))
            .otherwise(F.lit(None)),
        )

        # SIMPLIFIED: real PCAF attributes the scopes the institution has chosen
        # to disclose (often 1+2, with 3 optional by sector). Here we sum 1+2+3
        # and treat a missing scope as 0, then let the data-quality score say so.
        fe = fe.withColumn(
            "issuer_emissions_tco2e",
            F.when(
                F.col("coverage_status") == "mapped",
                F.coalesce(F.col("scope1_tco2e"), F.lit(0.0))
                + F.coalesce(F.col("scope2_tco2e"), F.lit(0.0))
                + F.coalesce(F.col("scope3_tco2e"), F.lit(0.0)),
            ),
        )
        fe = fe.withColumn(
            "financed_emissions_tco2e",
            F.col("attribution_factor") * F.col("issuer_emissions_tco2e"),
        )
        fe = pcaf_score_column(fe)
        fe = fe.withColumn("as_of_date", F.lit(as_of_date))
        fe = fe.withColumn("financed_emissions_tco2e", F.round("financed_emissions_tco2e", 6))
        fe = fe.withColumn("attribution_factor", F.round("attribution_factor", 10))

        write_delta(fe, cfg, "gold", "financed_emissions")
        n_out = fe.count()
        n_unattributed = fe.filter(F.col("financed_emissions_tco2e").isNull()).count()
        log_step(
            LOGGER,
            "gold.financed_emissions",
            n_in,
            n_out,
            n_unattributed,
            "null_financed_emissions_kept_for_coverage",
            timer.duration_s,
        )
        return fe


def _positions_in_eur(spark: SparkSession, cfg: dict, as_of_date: str) -> DataFrame:
    """Holdings plus loans, converted to EUR using the reporting-date FX table."""
    holdings = read_delta(spark, cfg, "silver", "holdings").filter(F.col("as_of_date") == F.lit(as_of_date))
    instruments = read_delta(spark, cfg, "silver", "instruments")
    loans = read_delta(spark, cfg, "silver", "loans").filter(F.col("as_of_date") == F.lit(as_of_date))
    fx = read_delta(spark, cfg, "silver", "fx_rates").filter(F.col("as_of_date") == F.lit(as_of_date)).select(
        F.col("currency").alias("fx_currency"),
        F.col("rate_to_eur"),
    )
    portfolios = read_delta(spark, cfg, "silver", "portfolios").select(
        "portfolio_id", "legal_entity", "asset_class_scope"
    )

    listed = (
        holdings.join(instruments, "instrument_id", "left")
        .join(fx, holdings["currency"] == fx["fx_currency"], "left")
        .join(portfolios, "portfolio_id", "left")
        .withColumn("exposure_eur", F.col("market_value") * F.coalesce(F.col("rate_to_eur"), F.lit(1.0)))
        .withColumn(
            "asset_class",
            F.when(F.col("instrument_type") == "equity", F.lit("listed_equity")).otherwise(F.col("instrument_type")),
        )
        .withColumn("position_id", F.col("instrument_id"))
        .withColumn("position_kind", F.lit("holding"))
        .select(
            "portfolio_id",
            "legal_entity",
            "issuer_id",
            "position_id",
            "position_kind",
            "asset_class",
            "instrument_type",
            "exposure_eur",
            F.col("market_value").alias("native_amount"),
            holdings["currency"].alias("native_currency"),
        )
    )

    # ASSUMPTION: all business loans are booked to P03, the only loan portfolio.
    loan_book = (
        loans.withColumn("portfolio_id", F.lit("P03"))
        .join(fx, loans["currency"] == fx["fx_currency"], "left")
        .join(portfolios, "portfolio_id", "left")
        .withColumn("exposure_eur", F.col("outstanding_amount") * F.coalesce(F.col("rate_to_eur"), F.lit(1.0)))
        .withColumn("asset_class", F.lit("business_loan"))
        .withColumn("instrument_type", F.lit("business_loan"))
        .withColumn("position_id", F.col("loan_id"))
        .withColumn("position_kind", F.lit("loan"))
        .withColumn("issuer_id", F.col("borrower_issuer_id"))
        .select(
            "portfolio_id",
            "legal_entity",
            "issuer_id",
            "position_id",
            "position_kind",
            "asset_class",
            "instrument_type",
            "exposure_eur",
            F.col("outstanding_amount").alias("native_amount"),
            loans["currency"].alias("native_currency"),
        )
    )
    return listed.unionByName(loan_book)
