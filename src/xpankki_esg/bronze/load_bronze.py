"""Landing CSV -> bronze Delta, source columns unchanged plus audit fields.

Business question this module answers: can we prove exactly what file landed
on a given night, without having already "fixed" it?
"""

from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from xpankki_esg.config import landing_file
from xpankki_esg.io_utils import write_delta
from xpankki_esg.logging_utils import get_logger, log_step, timed_step

LOGGER = get_logger(__name__)


def load_bronze(spark: SparkSession, cfg: dict, as_of_date: str) -> None:
    """Ingest every source extract. Restricted MSCI goes to esg_restricted."""
    batch_id = f"batch-{as_of_date}"  # ASSUMPTION: deterministic batch id for the demo
    ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    for source_name, spec in cfg["sources"].items():
        with timed_step(LOGGER, f"bronze.{source_name}") as timer:
            path = landing_file(cfg, source_name)
            # Step 1: read the CSV exactly as delivered (everything as string).
            raw = (
                spark.read.option("header", True)
                .option("inferSchema", False)
                .csv(str(path))
            )
            n_in = raw.count()

            # Step 2: add audit columns. Do not trim, parse, or drop anything.
            bronze = (
                raw.withColumn("_ingested_at", F.lit(ingested_at))
                .withColumn("_source_file", F.lit(path.name))
                .withColumn("_batch_id", F.lit(batch_id))
            )

            schema_key = spec.get("bronze_schema", "bronze")
            write_delta(bronze, cfg, schema_key, source_name)
            log_step(LOGGER, f"bronze.{source_name}", n_in, bronze.count(), 0, "", timer.duration_s)
