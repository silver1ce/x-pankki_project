"""Simple structured logging used by every pipeline step.

Business question this module answers: when a step ran, how many rows went
in, how many came out, what was dropped and why, and how long did it take?

Format is one line of key=value pairs so it is readable in a terminal and
still greppable in a Databricks job log. No extra logging libraries.
"""

from __future__ import annotations

import logging
import time
from contextlib import contextmanager


def setup_logging(level: int = logging.INFO) -> None:
    """Configure the root logger once. Safe to call from tests and the CLI."""
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True,
    )


def get_logger(name: str) -> logging.Logger:
    """Return a named logger. Call setup_logging() first from the CLI."""
    return logging.getLogger(name)


def log_step(
    logger: logging.Logger,
    step_name: str,
    input_rows: int,
    output_rows: int,
    rows_dropped: int = 0,
    drop_reason: str = "",
    duration_s: float = 0.0,
) -> None:
    """Emit the standard per-step line every transformation must produce."""
    logger.info(
        "step=%s input_rows=%s output_rows=%s rows_dropped=%s drop_reason=%s duration_s=%.3f",
        step_name,
        input_rows,
        output_rows,
        rows_dropped,
        drop_reason or "-",
        duration_s,
    )


@contextmanager
def timed_step(logger: logging.Logger, step_name: str):
    """Log start/end timestamps around a block. Use with log_step for counts.

    Example::

        with timed_step(logger, "silver.holdings") as timer:
            out = clean_holdings(frame)
            log_step(logger, "silver.holdings", n_in, n_out, n_drop, reason, timer.duration_s)
    """
    timer = _Timer()
    logger.info("step=%s status=start", step_name)
    started = time.perf_counter()
    try:
        yield timer
    finally:
        timer.duration_s = time.perf_counter() - started
        logger.info("step=%s status=end duration_s=%.3f", step_name, timer.duration_s)


class _Timer:
    """Tiny mutable holder so the context manager can pass duration back."""

    duration_s: float = 0.0
