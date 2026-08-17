"""Load YAML configuration and resolve table names per environment.

Business question this module answers: given we are running locally or on
Databricks, where does each table live, and which report parameters apply?

Callers receive a plain dict. There is no config class and no injection
framework — open this file, read top to bottom, and you know how settings
get from YAML onto disk (local) or Unity Catalog (Databricks).
"""

from __future__ import annotations

import os
from pathlib import Path

import yaml

# src/xpankki_esg/config.py -> parents[2] is the project root.
PROJECT_ROOT = Path(__file__).resolve().parents[2]


def load_yaml(path: Path) -> dict:
    """Read a YAML file into a dict. Empty files become {} so callers can iterate safely."""
    with path.open(encoding="utf-8") as handle:
        loaded = yaml.safe_load(handle)
    return loaded or {}


def load_config(config_path: Path | None = None, environment: str | None = None) -> dict:
    """Load conf/config.yaml plus conf/sources.yaml and resolve the active environment.

    Resolution order for the environment name:
    1. explicit argument (used by tests)
    2. XPANKKI_ENV environment variable (used on Databricks jobs)
    3. the `environment:` key in config.yaml (local default)
    """
    # Step 1: read the two YAML files that describe the whole project.
    path = Path(config_path) if config_path else PROJECT_ROOT / "conf" / "config.yaml"
    raw = load_yaml(path)
    sources = load_yaml(PROJECT_ROOT / "conf" / "sources.yaml")

    # Step 2: pick local vs databricks without rewriting any transformation.
    env = environment or os.environ.get("XPANKKI_ENV") or raw["environment"]
    if env not in raw:
        raise ValueError(f"Unknown environment '{env}'. Expected 'local' or 'databricks'.")
    env_block = raw[env]

    # Step 3: turn relative local paths into absolute paths so Spark does not
    # depend on the current working directory. Databricks volume paths stay as-is.
    paths = dict(env_block["paths"])
    if env == "local":
        paths = {key: str(PROJECT_ROOT / value) for key, value in paths.items()}

    return {
        "environment": env,
        "random_seed": raw["random_seed"],
        "reporting": raw["reporting"],
        "spark": raw["spark"],
        "catalog": env_block["catalog"],
        "schemas": env_block["schemas"],
        "paths": paths,
        "sources": sources.get("tables", {}),
        "project_root": PROJECT_ROOT,
        "reports_dir": PROJECT_ROOT / "conf" / "reports",
    }


def load_report_config(cfg: dict, report_id: str) -> dict:
    """Load conf/reports/R0N.yaml for one report (parameters, grain, recon tolerances)."""
    report_path = cfg["reports_dir"] / f"{report_id}.yaml"
    if not report_path.exists():
        raise FileNotFoundError(f"No report config at {report_path}")
    return load_yaml(report_path)


def list_report_ids(cfg: dict) -> list[str]:
    """Return R01..R09 in order, driven by the files in conf/reports/."""
    files = sorted(cfg["reports_dir"].glob("R*.yaml"))
    return [path.stem for path in files]


def schema_name(cfg: dict, schema_key: str) -> str:
    """Look up a logical schema key (bronze, silver, gold, esg_restricted)."""
    try:
        return cfg["schemas"][schema_key]
    except KeyError as exc:
        raise KeyError(f"Unknown schema key '{schema_key}'. Check conf/config.yaml.") from exc


def table_identifier(cfg: dict, schema_key: str, table: str) -> str:
    """Return the Spark identifier for a table in the active environment.

    Local:      absolute path under data/lakehouse/<schema>/<table>
    Databricks: three-level Unity Catalog name catalog.schema.table

    Restricted MSCI tables use schema_key='esg_restricted' so they never
    land in the open gold schema. That is the local analogue of a separate
    Unity Catalog schema with its own grants.
    """
    schema = schema_name(cfg, schema_key)
    if cfg["environment"] == "databricks":
        return f"{cfg['catalog']}.{schema}.{table}"
    return str(Path(cfg["paths"]["lakehouse"]) / schema / table)


def landing_file(cfg: dict, source_name: str) -> Path:
    """Absolute path to one SAS-DW extract in the landing zone."""
    filename = cfg["sources"][source_name]["filename"]
    return Path(cfg["paths"]["landing"]) / filename


def ensure_local_dirs(cfg: dict) -> None:
    """Create landing / lakehouse / baselines / output folders on a local run."""
    if cfg["environment"] != "local":
        return
    for key in ("landing", "lakehouse", "baselines", "output"):
        Path(cfg["paths"][key]).mkdir(parents=True, exist_ok=True)
