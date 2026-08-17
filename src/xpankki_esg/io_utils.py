"""Read and write Delta tables and report CSVs with consistent naming.

Business question this module answers: given a logical table (bronze.holdings,
esg_restricted.msci_esg, gold.R01), where do we read and write it locally
or on Databricks, without each transformation hard-coding paths?
"""

from __future__ import annotations

import csv
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from xpankki_esg.config import table_identifier


def read_delta(spark: SparkSession, cfg: dict, schema_key: str, table: str) -> DataFrame:
    """Read a Delta table by logical schema key (bronze / silver / gold / esg_restricted)."""
    ident = table_identifier(cfg, schema_key, table)
    if cfg["environment"] == "databricks":
        return spark.table(ident)
    return spark.read.format("delta").load(ident)


def write_delta(df: DataFrame, cfg: dict, schema_key: str, table: str) -> None:
    """Overwrite a Delta table. Local uses a folder path; Databricks uses UC names."""
    ident = table_identifier(cfg, schema_key, table)
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if cfg["environment"] == "databricks":
        writer.saveAsTable(ident)
    else:
        Path(ident).parent.mkdir(parents=True, exist_ok=True)
        writer.save(ident)


def write_csv(df: DataFrame, path: Path) -> None:
    """Write one CSV with a header. Reports are small, so we collect on the driver.

    Spark's native CSV writer produces a folder of part files; a single file is
    easier to open in Excel and to diff in reconciliation.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = df.collect()
    columns = df.columns
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        for row in rows:
            writer.writerow([_csv_value(row[col]) for col in columns])


def read_csv_rows(path: Path) -> tuple[list[str], list[dict]]:
    """Read a report CSV back as dicts. Used by reconciliation."""
    with Path(path).open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        return list(reader.fieldnames or []), rows


def write_report(df: DataFrame, cfg: dict, report_id: str) -> DataFrame:
    """Persist a report as gold Delta (reusable) and as CSV (what recon diffs)."""
    write_delta(df, cfg, "gold", report_id.lower())
    write_csv(df, Path(cfg["paths"]["output"]) / f"{report_id}.csv")
    return df


def _csv_value(value):
    if value is None:
        return ""
    return value
