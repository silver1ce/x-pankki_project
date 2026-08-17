"""CLI orchestrator: generate, run layers, recon.

Business question this module answers: how does an operator run the same
pipeline locally that a Databricks job will run, with one command per action?
"""

from __future__ import annotations

import argparse
import sys

from xpankki_esg.bronze.load_bronze import load_bronze
from xpankki_esg.config import ensure_local_dirs, load_config
from xpankki_esg.generate_data import apply_seeded_baseline_diffs, generate_landing
from xpankki_esg.gold.data_quality import build_data_quality
from xpankki_esg.gold.intensity import build_intensity
from xpankki_esg.gold.pcaf import build_financed_emissions
from xpankki_esg.gold.taxonomy import build_coverage, build_taxonomy
from xpankki_esg.logging_utils import get_logger, setup_logging
from xpankki_esg.recon.compare import compare_all
from xpankki_esg.reports import (
    r01_financed_emissions_equity,
    r02_financed_emissions_bonds,
    r03_financed_emissions_loans,
    r04_pcaf_data_quality,
    r05_waci_by_portfolio,
    r06_sfdr_pai_core,
    r07_fossil_fuel_exposure,
    r08_taxonomy_alignment,
    r09_coverage_summary,
)
from xpankki_esg.silver.clean_tables import clean_all
from xpankki_esg.silver.entity_resolution import resolve_entities
from xpankki_esg.spark_session import get_spark

LOGGER = get_logger(__name__)

REPORTS = {
    "R01": r01_financed_emissions_equity.build,
    "R02": r02_financed_emissions_bonds.build,
    "R03": r03_financed_emissions_loans.build,
    "R04": r04_pcaf_data_quality.build,
    "R05": r05_waci_by_portfolio.build,
    "R06": r06_sfdr_pai_core.build,
    "R07": r07_fossil_fuel_exposure.build,
    "R08": r08_taxonomy_alignment.build,
    "R09": r09_coverage_summary.build,
}


def main(argv: list[str] | None = None) -> int:
    setup_logging()
    parser = argparse.ArgumentParser(prog="xpankki_esg.pipeline")
    sub = parser.add_subparsers(dest="command", required=True)

    gen = sub.add_parser("generate", help="Write SAS-DW extracts and freeze SAS baselines")
    gen.add_argument("--as-of", required=True)

    run = sub.add_parser("run", help="Run bronze / silver / gold / reports")
    run.add_argument("--as-of", required=True)
    run.add_argument("--layer", default="all", choices=["bronze", "silver", "gold", "reports", "all"])
    run.add_argument("--report", default=None, help="Run one report (R01..R09) after gold exists")

    rec = sub.add_parser("recon", help="Compare new reports to frozen SAS baselines")
    rec.add_argument("--as-of", required=True)

    args = parser.parse_args(argv)
    cfg = load_config()
    ensure_local_dirs(cfg)

    if args.command == "generate":
        return cmd_generate(cfg, args.as_of)
    if args.command == "run":
        return cmd_run(cfg, args.as_of, args.layer, args.report)
    if args.command == "recon":
        return cmd_recon(cfg, args.as_of)
    parser.error(f"unknown command {args.command}")
    return 2


def cmd_generate(cfg: dict, as_of_date: str) -> int:
    generate_landing(cfg, as_of_date)
    # Produce report CSVs with the new pipeline, then freeze them as SAS
    # baselines after applying the two seeded differences.
    rc = cmd_run(cfg, as_of_date, layer="all", report_id=None)
    if rc != 0:
        return rc
    apply_seeded_baseline_diffs(cfg)
    LOGGER.info("step=generate status=done as_of=%s", as_of_date)
    return 0


def cmd_run(cfg: dict, as_of_date: str, layer: str, report_id: str | None) -> int:
    spark = get_spark(cfg)
    try:
        if report_id:
            if report_id not in REPORTS:
                LOGGER.error("Unknown report %s", report_id)
                return 1
            REPORTS[report_id](spark, cfg, as_of_date)
            return 0

        if layer in ("bronze", "all"):
            load_bronze(spark, cfg, as_of_date)
        if layer in ("silver", "all"):
            clean_all(spark, cfg, as_of_date)
            resolve_entities(spark, cfg, as_of_date)
        if layer in ("gold", "all"):
            build_financed_emissions(spark, cfg, as_of_date)
            build_data_quality(spark, cfg, as_of_date)
            build_intensity(spark, cfg, as_of_date)
            build_taxonomy(spark, cfg, as_of_date)
            build_coverage(spark, cfg, as_of_date)
        if layer in ("reports", "all"):
            for name, builder in REPORTS.items():
                builder(spark, cfg, as_of_date)
                LOGGER.info("step=reports status=wrote report=%s", name)
        LOGGER.info("step=run status=done layer=%s as_of=%s", layer, as_of_date)
        return 0
    finally:
        # Locally we stop the session so a second CLI command can start a fresh one.
        if cfg["environment"] == "local":
            spark.stop()


def cmd_recon(cfg: dict, as_of_date: str) -> int:
    return compare_all(cfg, as_of_date)


if __name__ == "__main__":
    sys.exit(main())
