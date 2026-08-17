"""New output vs frozen SAS baseline, with per-report tolerances.

Business question this module answers: which reports match the legacy SAS
numbers within tolerance, and which rows break cutover?
"""

from __future__ import annotations

from pathlib import Path

from xpankki_esg.config import load_report_config
from xpankki_esg.io_utils import read_csv_rows
from xpankki_esg.logging_utils import get_logger

LOGGER = get_logger(__name__)


def compare_all(cfg: dict, as_of_date: str) -> int:
    """Print a pass/fail table for R01–R09. Return 0 even when two seeded diffs fail.

    A real cutover job would return non-zero on any fail. This demo expects
    R01 and R09 to fail (the two documented SAS-vs-new differences).
    """
    _ = as_of_date
    print(f"{'report':<6} {'status':<8} {'rows_new':>8} {'rows_base':>10} {'breaks':>7}  note")
    print("-" * 78)
    exit_like = 0
    unexpected_fail = False
    for report_id in [f"R{i:02d}" for i in range(1, 10)]:
        status, n_new, n_base, n_break, note = compare_report(cfg, report_id)
        print(f"{report_id:<6} {status:<8} {n_new:8d} {n_base:10d} {n_break:7d}  {note}")
        if status == "FAIL" and report_id not in {"R01", "R09"}:
            unexpected_fail = True
            exit_like = 1
        if status == "FAIL":
            _print_breaking_rows(cfg, report_id)
    if unexpected_fail:
        LOGGER.error("step=recon unexpected FAIL outside seeded diffs R01/R09")
        return 1
    LOGGER.info("step=recon status=done expected_fails=R01,R09")
    return exit_like


def compare_report(cfg: dict, report_id: str) -> tuple[str, int, int, int, str]:
    report_cfg = load_report_config(cfg, report_id)
    new_path = Path(cfg["paths"]["output"]) / f"{report_id}.csv"
    base_path = Path(cfg["paths"]["baselines"]) / f"{report_id}.csv"
    if not new_path.exists() or not base_path.exists():
        return "FAIL", 0, 0, 0, "missing output or baseline file"

    _, new_rows = read_csv_rows(new_path)
    _, base_rows = read_csv_rows(base_path)
    keys = report_cfg["reconciliation"]["key_columns"]
    values = report_cfg["reconciliation"]["value_columns"]
    abs_tol = float(report_cfg["reconciliation"]["absolute_tolerance"])
    rel_tol = float(report_cfg["reconciliation"]["relative_tolerance"])

    new_map = {_key(row, keys): row for row in new_rows}
    base_map = {_key(row, keys): row for row in base_rows}
    all_keys = sorted(set(new_map) | set(base_map))
    breaks = 0
    for key in all_keys:
        if key not in new_map or key not in base_map:
            breaks += 1
            continue
        for col in values:
            if not _within_tolerance(new_map[key].get(col), base_map[key].get(col), abs_tol, rel_tol):
                breaks += 1
                break
    status = "PASS" if breaks == 0 else "FAIL"
    note = "match" if status == "PASS" else "see breaking rows below"
    if report_id == "R01" and status == "FAIL":
        note = "seeded: SAS rounded financed emissions to nearest 10 tCO2e"
    if report_id == "R09" and status == "FAIL":
        note = "seeded: SAS still mapped ISS007"
    return status, len(new_rows), len(base_rows), breaks, note


def _print_breaking_rows(cfg: dict, report_id: str) -> None:
    report_cfg = load_report_config(cfg, report_id)
    _, new_rows = read_csv_rows(Path(cfg["paths"]["output"]) / f"{report_id}.csv")
    _, base_rows = read_csv_rows(Path(cfg["paths"]["baselines"]) / f"{report_id}.csv")
    keys = report_cfg["reconciliation"]["key_columns"]
    values = report_cfg["reconciliation"]["value_columns"]
    abs_tol = float(report_cfg["reconciliation"]["absolute_tolerance"])
    rel_tol = float(report_cfg["reconciliation"]["relative_tolerance"])
    new_map = {_key(row, keys): row for row in new_rows}
    base_map = {_key(row, keys): row for row in base_rows}
    shown = 0
    for key in sorted(set(new_map) | set(base_map)):
        if shown >= 8:
            print("         ... additional breaking rows omitted")
            break
        if key not in new_map:
            print(f"         missing in new: {key}")
            shown += 1
            continue
        if key not in base_map:
            print(f"         missing in baseline: {key}")
            shown += 1
            continue
        diffs = []
        for col in values:
            new_v = new_map[key].get(col)
            old_v = base_map[key].get(col)
            if not _within_tolerance(new_v, old_v, abs_tol, rel_tol):
                diffs.append(f"{col} new={new_v} sas={old_v}")
        if diffs:
            print(f"         {key} | " + "; ".join(diffs))
            shown += 1


def _key(row: dict, keys: list[str]) -> tuple:
    return tuple(str(row.get(k, "")) for k in keys)


def _within_tolerance(new_v, old_v, abs_tol: float, rel_tol: float) -> bool:
    if (new_v in (None, "")) and (old_v in (None, "")):
        return True
    try:
        new_f = float(new_v)
        old_f = float(old_v)
    except (TypeError, ValueError):
        return str(new_v) == str(old_v)
    diff = abs(new_f - old_f)
    threshold = max(abs_tol, rel_tol * max(abs(old_f), 1.0))
    return diff <= threshold
