"""Synthetic SAS-DW extracts and the two seeded baseline differences.

Business question this module answers: what would overnight files from the
legacy SAS warehouse look like, including the messiness operations always
sees, so the rest of the pipeline has something deterministic to run on?

Rows are fully specified (not drawn from a distribution) so a reader can
trace ISS002 through every layer. random_seed is still applied so any later
random draw stays repeatable.
"""

from __future__ import annotations

import csv
import random
from pathlib import Path

from xpankki_esg.config import ensure_local_dirs, landing_file
from xpankki_esg.logging_utils import get_logger, log_step, timed_step

LOGGER = get_logger(__name__)

# ISS007 is present in holdings but omitted from the new crosswalk.
# The frozen SAS baseline pretends the legacy mapping did match it (R09).
SAS_ONLY_ISSUER = "ISS007"
SAS_ONLY_HOLDING_EUR = 4_000_000.0


def generate_landing(cfg: dict, as_of_date: str) -> None:
    """Write the 9 source CSVs plus the incomplete issuer-to-provider crosswalk."""
    random.seed(cfg["random_seed"])
    ensure_local_dirs(cfg)

    with timed_step(LOGGER, "generate.landing") as timer:
        writers = {
            "portfolios": _portfolios(),
            "instruments": _instruments(),
            "issuers": _issuers(),
            "holdings": _holdings(as_of_date),
            "loans": _loans(as_of_date),
            "company_financials": _company_financials(),
            "msci_esg": _msci_esg(),
            "taxonomy_data": _taxonomy_data(),
            "fx_rates": _fx_rates(as_of_date),
            "mapping_issuer_to_provider": _mapping(),
        }
        total_rows = 0
        for source_name, (header, rows) in writers.items():
            path = landing_file(cfg, source_name)
            _write_csv(path, header, rows)
            total_rows += len(rows)
            LOGGER.info("step=generate.landing file=%s rows=%s", path.name, len(rows))
        log_step(LOGGER, "generate.landing", 0, total_rows, 0, "", timer.duration_s)


def apply_seeded_baseline_diffs(cfg: dict) -> None:
    """Copy pipeline CSVs into baselines/ then apply the two documented SAS gaps.

    SEEDED DIFF 1 (R01): legacy SAS PROC MEANS stored financed emissions rounded
    to the nearest 10 tCO2e. The new pipeline keeps full precision.

    SEEDED DIFF 2 (R09): legacy SAS still mapped ISS007 to a vendor entity; the
    new crosswalk does not. Baseline coverage therefore shows one fewer
    unmapped issuer.
    """
    output_dir = Path(cfg["paths"]["output"])
    baseline_dir = Path(cfg["paths"]["baselines"])
    baseline_dir.mkdir(parents=True, exist_ok=True)

    for report_id in [f"R{i:02d}" for i in range(1, 10)]:
        src = output_dir / f"{report_id}.csv"
        dst = baseline_dir / f"{report_id}.csv"
        dst.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")

    _seeded_diff_r01_rounding(baseline_dir / "R01.csv")
    _seeded_diff_r09_legacy_mapping(baseline_dir / "R09.csv")
    LOGGER.info("step=generate.baselines status=seeded_diffs_applied reports=R01,R09")


def _write_csv(path: Path, header: list[str], rows: list[list]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        writer.writerows(rows)


def _portfolios():
    header = ["portfolio_id", "portfolio_name", "legal_entity", "asset_class_scope", "currency"]
    rows = [
        ["P01", "Equity Core", "X-Pankki Asset Management ", "listed_equity", "EUR"],  # trailing space
        ["P02", "Credit Income", "X-Pankki Asset Management", "corporate_bonds", "EUR"],
        ["P03", "Business Lending", "X-Pankki Bank", "business_loans", "EUR"],
        ["P04", "Mixed Growth", "X-Pankki Asset Management", "mixed", "EUR"],
    ]
    return header, rows


def _instruments():
    header = ["instrument_id", "isin", "instrument_type", "issuer_id", "currency"]
    rows = [
        ["E001", " fi0000000001 ", "equity", "ISS001", "EUR"],  # spaces + lower case
        ["E002", "FI0000000002", "equity", "ISS002", "EUR"],
        ["E003", "FI0000000003", "equity", "ISS003", "EUR"],
        ["E004", "se0000000004", "equity", "ISS004", "SEK"],
        ["E005", "FI0000000005", "equity", "ISS005", "EUR"],
        ["E006", "FI0000000006", "equity", "ISS006", "EUR"],
        ["E007", "FI0000000007", "equity", "ISS007", "EUR"],
        ["E008", "FI0000000008", "equity", "ISS008", "EUR"],
        ["E009", " fi0000000009", "equity", "ISS009", "EUR"],
        ["E010", "FI0000000010", "equity", "ISS011", "EUR"],
        ["E011", "gb0000000011", "equity", "ISS012", "GBP"],
        ["B001", "FI0000001001", "corporate_bond", "ISS001", "EUR"],
        ["B002", "FI0000001002", "corporate_bond", "ISS002", "EUR"],
        ["B003", "FI0000001003", "corporate_bond", "ISS003", "EUR"],
        ["B004", "FI0000001004", "corporate_bond", "ISS005", "EUR"],
        ["B005", "FI0000001005", "corporate_bond", "ISS010", "EUR"],
        ["B006", "GB0000001006", "corporate_bond", "ISS012", "GBP"],
    ]
    return header, rows


def _issuers():
    header = ["issuer_id", "lei", "issuer_name", "country", "nace_sector"]
    rows = [
        ["ISS001", "LEI000000000000000001", "Pohjola Industrials Oyj", "FI", "C27"],
        ["ISS002", "LEI000000000000000002", "Baltic Energy Oyj", "FI", "D35"],
        ["ISS003", "LEI000000000000000003", "Nordic Bank Oyj", "FI", "K64"],
        ["ISS004", "LEI000000000000000004", "Sveaberg Mining AB", "SE", "B07"],
        ["ISS005", "LEI000000000000000005", "Atlantic Oil ASA", "NO", "B06"],
        ["ISS006", "LEI000000000000000006", "Unmapped Forestry Oy", "FI", "A02"],
        ["ISS007", "LEI000000000000000007", "Legacy Match Corp Oy", "FI", "C10"],
        ["ISS008", "LEI000000000000000008", "Zero EVIC Holdings Oy", "FI", "C28"],
        ["ISS009", "", "Missing LEI Shipping Oy", "FI", "H50"],  # missing LEI
        ["ISS010", "LEI000000000000000010", "Orphan Map Textiles Oy", "FI", "C13"],
        ["ISS011", "LEI000000000000000011", "Green Utilities Oyj", "FI", "D35"],
        ["ISS012", "LEI000000000000000012", "Capital Markets Plc", "GB", "K64"],
    ]
    return header, rows


def _holdings(as_of_date: str):
    header = ["portfolio_id", "instrument_id", "as_of_date", "quantity", "market_value", "currency"]
    rows = [
        ["P01", "E001", as_of_date, 100000, 25000000, "EUR"],
        ["P01", "E001", as_of_date, 100000, 25000000, "EUR"],  # duplicate
        ["P01", "E002", as_of_date, 80000, 18000000, "EUR"],
        ["P01", "E003", as_of_date, 50000, 12000000, "EUR"],
        ["P01", "E004", as_of_date, 40000, 92000000, "SEK"],  # 92m SEK ≈ 8.0m EUR at 0.087
        ["P01", "E005", as_of_date, 30000, 15000000, "EUR"],
        ["P01", "E006", as_of_date, 20000, 5000000, "EUR"],
        ["P01", "E007", as_of_date, 15000, SAS_ONLY_HOLDING_EUR, "EUR"],
        ["P01", "E008", as_of_date, 10000, 6000000, "EUR"],
        ["P01", "E009", as_of_date, 12000, 7000000, "EUR"],
        ["P01", "E010", as_of_date, 22000, 9000000, "EUR"],
        ["P01", "E011", as_of_date, 18000, 8475000, "GBP"],  # 8.475m GBP * 1.18 ≈ 10.0m EUR
        ["P02", "B001", as_of_date, 200, 20000000, "EUR"],
        ["P02", "B002", as_of_date, 220, 22000000, "EUR"],
        ["P02", "B003", as_of_date, 110, 11000000, "EUR"],
        ["P02", "B004", as_of_date, 160, 16000000, "EUR"],
        ["P02", "B005", as_of_date, 30, 3000000, "EUR"],
        ["P02", "B006", as_of_date, 80, 6780000, "GBP"],
        ["P04", "E001", as_of_date, 20000, 5000000, "EUR"],
        ["P04", "E002", as_of_date, 15000, 4000000, "EUR"],
        ["P04", "B001", as_of_date, 50, 5000000, "EUR"],
        ["P04", "E003", as_of_date, 8000, "", "EUR"],  # missing market_value
        ["P01", "E001", "2024-12-31", 90000, 22000000, "EUR"],  # prior year; gold filters it
    ]
    return header, rows


def _loans(as_of_date: str):
    header = ["loan_id", "borrower_issuer_id", "as_of_date", "outstanding_amount", "currency", "loan_purpose"]
    rows = [
        ["L001", "ISS001", as_of_date, 30000000, "EUR", "general_corporate"],
        ["L002", "ISS002", as_of_date, 45000000, "EUR", "capex"],
        ["L003", "ISS003", as_of_date, 20000000, "EUR", "working_capital"],
        ["L004", "ISS008", as_of_date, 8000000, "EUR", "general_corporate"],
        ["L005", "ISS006", as_of_date, 6000000, "EUR", "general_corporate"],
        ["L006", "ISS011", as_of_date, -1000000, "EUR", "working_capital"],  # negative — reject
        ["L007", "ISS012", as_of_date, 5000000, "GBP", "working_capital"],
    ]
    return header, rows


def _company_financials():
    header = ["issuer_id", "fiscal_year", "evic_eur", "total_assets_eur", "revenue_eur"]
    rows = [
        ["ISS001", 2025, 40_000_000_000, 45_000_000_000, 8_000_000_000],
        ["ISS002", 2025, 25_000_000_000, 30_000_000_000, 12_000_000_000],
        ["ISS003", 2025, 80_000_000_000, 400_000_000_000, 15_000_000_000],
        ["ISS004", 2025, 12_000_000_000, 14_000_000_000, 3_000_000_000],
        ["ISS005", 2025, 50_000_000_000, 70_000_000_000, 20_000_000_000],
        ["ISS006", 2025, 4_000_000_000, 5_000_000_000, 1_200_000_000],
        ["ISS007", 2025, 3_500_000_000, 4_000_000_000, 900_000_000],
        ["ISS008", 2025, 0, 2_000_000_000, 500_000_000],  # EVIC = 0
        ["ISS009", 2025, 6_000_000_000, 7_000_000_000, 2_000_000_000],
        ["ISS010", 2025, 1_500_000_000, 1_800_000_000, 400_000_000],
        ["ISS011", 2025, 18_000_000_000, 22_000_000_000, 5_000_000_000],
        ["ISS012", 2025, 30_000_000_000, 200_000_000_000, 6_000_000_000],
    ]
    return header, rows


def _msci_esg():
    header = [
        "provider_entity_id",
        "fiscal_year",
        "scope1_tco2e",
        "scope2_tco2e",
        "scope3_tco2e",
        "emission_data_source",
        "esg_rating",
        "fossil_fuel_flag",
    ]
    rows = [
        ["PROV001", 2025, 500_000, 200_000, 3_000_000, "verified", "A", 0],
        ["PROV002", 2025, 8_000_000, 1_500_000, 20_000_000, "reported", "CCC", 1],
        ["PROV003", 2025, 50_000, 80_000, "", "reported", "AA", 0],  # missing scope 3
        ["PROV004", 2025, 1_200_000, 400_000, 5_000_000, "estimated", "", 1],  # null rating, fossil mining
        ["PROV005", 2025, 15_000_000, 2_000_000, 40_000_000, "reported", "B", 1],
        ["PROV008", 2025, 300_000, 100_000, 800_000, "reported", "BB", 0],
        ["PROV009", 2025, 700_000, 200_000, 2_500_000, "verified", "BBB", 0],
        ["PROV011", 2025, 100_000, 50_000, 400_000, "reported", "A", 0],
        ["PROV012", 2025, 20_000, 30_000, "", "estimated", "AA", 0],  # missing scope 3
    ]
    return header, rows


def _taxonomy_data():
    header = ["issuer_id", "fiscal_year", "taxonomy_eligible_share", "taxonomy_aligned_share"]
    rows = [
        ["ISS001", 2025, 0.40, 0.15],
        ["ISS002", 2025, 0.10, 0.02],
        ["ISS003", 2025, 0.05, 0.01],
        ["ISS004", 2025, 0.20, 0.50],  # aligned > eligible — reject
        ["ISS005", 2025, 0.08, 0.01],
        ["ISS006", 2025, 0.60, 0.20],
        ["ISS007", 2025, 0.30, 0.10],
        ["ISS008", 2025, 0.25, 0.05],
        ["ISS009", 2025, 0.15, 0.04],
        ["ISS010", 2025, 0.35, 0.10],
        ["ISS011", 2025, 0.80, 0.45],
        ["ISS012", 2025, 0.12, 0.03],
    ]
    return header, rows


def _fx_rates(as_of_date: str):
    # SAS DATE9. for the reporting date. Prior-year EUR rate shows the format is table-wide.
    header = ["currency", "as_of_date", "rate_to_eur"]
    rows = [
        ["EUR", "31DEC2025", 1.0],
        ["USD", "31DEC2025", 0.92],
        ["GBP", "31DEC2025", 1.18],
        ["SEK", "31DEC2025", 0.087],
        ["NOK", "31DEC2025", 0.085],
        ["EUR", "31DEC2024", 1.0],
    ]
    # ASSUMPTION: generate always emits DATE9. for 2025-12-31; other as-of dates
    # are not generated in this demo.
    _ = as_of_date
    return header, rows


def _mapping():
    header = ["issuer_id", "provider_entity_id"]
    rows = [
        ["ISS001", "PROV001"],
        ["ISS002", "PROV002"],
        ["ISS003", "PROV003"],
        ["ISS004", "PROV004"],
        ["ISS005", "PROV005"],
        ["ISS008", "PROV008"],
        ["ISS009", "PROV009"],
        ["ISS010", "PROV999"],  # provider does not exist in msci_esg
        ["ISS011", "PROV011"],
        ["ISS012", "PROV012"],
        # ISS006 unmapped
        # ISS007 unmapped in the new crosswalk (legacy SAS still had it)
    ]
    return header, rows


def _seeded_diff_r01_rounding(path: Path) -> None:
    header, rows = _read_dicts(path)
    for row in rows:
        if row.get("financed_emissions_tco2e"):
            value = float(row["financed_emissions_tco2e"])
            row["financed_emissions_tco2e"] = f"{round(value / 10) * 10:.1f}"
    _write_dicts(path, header, rows)


def _seeded_diff_r09_legacy_mapping(path: Path) -> None:
    header, rows = _read_dicts(path)
    by_cause = {row["gap_cause"]: row for row in rows}
    unmapped = by_cause.get("unmapped_issuer")
    mapped = by_cause.get("mapped")
    if not unmapped or not mapped:
        LOGGER.warning("step=generate.baselines R09 missing expected gap_cause rows; skip seeded diff 2")
        return
    # Move ISS007's single 4m EUR holding from unmapped -> mapped.
    unmapped["row_count"] = str(int(float(unmapped["row_count"])) - 1)
    unmapped["market_value_eur"] = str(float(unmapped["market_value_eur"]) - SAS_ONLY_HOLDING_EUR)
    mapped["row_count"] = str(int(float(mapped["row_count"])) + 1)
    mapped["market_value_eur"] = str(float(mapped["market_value_eur"]) + SAS_ONLY_HOLDING_EUR)
    total_mv = sum(float(r["market_value_eur"]) for r in rows)
    for row in rows:
        mv = float(row["market_value_eur"])
        row["share_of_aum_pct"] = f"{(100.0 * mv / total_mv) if total_mv else 0:.6f}"
    _write_dicts(path, header, rows)


def _read_dicts(path: Path) -> tuple[list[str], list[dict]]:
    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return list(reader.fieldnames or []), list(reader)


def _write_dicts(path: Path, header: list[str], rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)
