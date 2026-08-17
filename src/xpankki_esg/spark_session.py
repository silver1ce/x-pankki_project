"""Build a Spark + Delta session that works locally and on Databricks.

Business question this module answers: how do we get a SparkSession that
can read and write Delta tables, without forcing the caller to know whether
they are on a laptop or on a Databricks cluster?
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession


def is_databricks() -> bool:
    """Databricks Runtime sets this on every cluster; laptops do not."""
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def get_spark(cfg: dict) -> SparkSession:
    """Return an active SparkSession configured for this environment.

    On Databricks the platform already started Spark. Reusing that session
    is mandatory: creating a second one on a cluster is a common beginner
    mistake and wastes the cluster configuration (photon, UC, secrets).
    """
    # Step 1: if we are already on Databricks, take the session the cluster made.
    if is_databricks() or cfg["environment"] == "databricks":
        spark = SparkSession.builder.getOrCreate()
        spark.conf.set("spark.sql.session.timeZone", cfg["spark"]["timezone"])
        return spark

    # Step 2: local session. Point Spark at the venv Python so executors do
    # not silently fall back to a system interpreter with no project packages.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    # macOS sometimes resolves the machine hostname to a non-loopback address;
    # this is a local-only workaround and is not used on Databricks.
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

    warehouse = Path(cfg["paths"]["lakehouse"])
    warehouse.mkdir(parents=True, exist_ok=True)

    # Step 3: enable the Delta Lake extension. configure_spark_with_delta_pip
    # adds the matching delta-spark JARs to the local classpath so we do not
    # have to download them by hand.
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName(cfg["spark"]["app_name"])
        .master(cfg["spark"]["master"])
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.sql.shuffle.partitions", str(cfg["spark"]["shuffle_partitions"]))
        .config("spark.sql.session.timeZone", cfg["spark"]["timezone"])
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
