"""Instrument -> issuer -> ESG provider entity, with coverage status on every issuer.

Business question this module answers: for each internal issuer, do we have a
usable row in the licensed MSCI extract, and if not, why?
"""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from xpankki_esg.io_utils import read_delta, write_delta
from xpankki_esg.logging_utils import get_logger, log_step, timed_step

LOGGER = get_logger(__name__)


def resolve_entities(spark: SparkSession, cfg: dict, as_of_date: str) -> None:
    """Build silver.issuer_entity_map. Gaps stay visible; nothing is dropped."""
    with timed_step(LOGGER, "silver.entity_resolution") as timer:
        issuers = read_delta(spark, cfg, "silver", "issuers")
        mapping = read_delta(spark, cfg, "silver", "mapping_issuer_to_provider")
        # Silver MSCI lives in the restricted schema so grants can stay separate.
        msci = read_delta(spark, cfg, "esg_restricted", "msci_esg_silver")
        n_in = issuers.count()

        # Step 1: left-join the crosswalk so unmapped issuers remain.
        mapped = issuers.join(mapping, "issuer_id", "left")

        # Step 2: a mapping is only useful if that provider exists in MSCI for this year.
        fiscal_year = int(as_of_date[:4])  # ASSUMPTION: fiscal year = calendar year of as-of
        providers = (
            msci.filter(F.col("fiscal_year") == fiscal_year)
            .select("provider_entity_id")
            .dropDuplicates()
            .withColumn("provider_found", F.lit(1))
        )
        resolved = mapped.join(providers, "provider_entity_id", "left")

        # Step 3: classify every issuer. Reports use this status instead of guessing.
        resolved = resolved.withColumn(
            "coverage_status",
            F.when(F.col("provider_entity_id").isNull(), F.lit("unmapped_issuer"))
            .when(F.col("provider_found").isNull(), F.lit("orphan_provider"))
            .otherwise(F.lit("mapped")),
        ).drop("provider_found")

        write_delta(resolved, cfg, "silver", "issuer_entity_map")
        n_out = resolved.count()
        n_unmapped = resolved.filter(F.col("coverage_status") != "mapped").count()
        log_step(
            LOGGER,
            "silver.entity_resolution",
            n_in,
            n_out,
            n_unmapped,
            "unmapped_or_orphan_kept_not_dropped",
            timer.duration_s,
        )
